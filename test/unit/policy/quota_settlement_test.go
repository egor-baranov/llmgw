package policy_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"llmgw/gateway"
	"llmgw/policy"
	"llmgw/proxy"
	"llmgw/store"
)

func TestQuotaUsesExecutionIDAndSettlesWithLiveContext(t *testing.T) {
	quotaStore := &settlementQuotaStore{}
	state := quotaTestState("client-request-id", "server-execution-id")
	wrapped := policy.Quota{Store: quotaStore, Reservation: time.Minute}.Wrap(func(ctx context.Context, got *gateway.RequestState) (*gateway.Execution, error) {
		if err := got.ReserveProviderAttempt(ctx, &gateway.Attempt{ID: "attempt-1", Route: state.Candidates[0]}); err != nil {
			return nil, err
		}
		return &gateway.Execution{
			Attempt:  &gateway.Attempt{Route: state.Candidates[0]},
			Finalize: func(context.Context, gateway.Usage, error) error { return nil },
		}, nil
	})
	exec, err := wrapped(context.Background(), state)
	if err != nil {
		t.Fatalf("Wrap() error = %v", err)
	}
	if quotaStore.reserveID != "server-execution-id" {
		t.Fatalf("reservation id = %q, want server execution id", quotaStore.reserveID)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := exec.Settle(canceled, gateway.Usage{InputTokens: 4, OutputTokens: 4, TotalTokens: 8}, nil); err != nil {
		t.Fatalf("Settle() error = %v", err)
	}
	if quotaStore.commitContextErr != nil {
		t.Fatalf("commit context error = %v, want live settlement context", quotaStore.commitContextErr)
	}
}

func TestQuotaSurfacesRefundErrorWithoutCanceledContext(t *testing.T) {
	refundErr := errors.New("redis unavailable")
	quotaStore := &settlementQuotaStore{refundErr: refundErr}
	state := quotaTestState("request-id", "execution-id")
	callErr := errors.New("upstream failed")
	canceled, cancel := context.WithCancel(context.Background())
	wrapped := policy.Quota{Store: quotaStore, Reservation: time.Minute}.Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		cancel()
		return nil, callErr
	})
	_, err := wrapped(canceled, state)
	if !errors.Is(err, callErr) || !errors.Is(err, refundErr) {
		t.Fatalf("Wrap() error = %v, want upstream and refund errors", err)
	}
	if quotaStore.refundContextErr != nil {
		t.Fatalf("refund context error = %v, want live settlement context", quotaStore.refundContextErr)
	}
}

func TestQuotaClassifiesInitialReservationStoreFailure(t *testing.T) {
	backendErr := errors.New("redis password leaked in diagnostic")
	quotaStore := &settlementQuotaStore{reserveErr: backendErr}
	state := quotaTestState("request-id", "execution-id")
	_, err := (policy.Quota{Store: quotaStore, Reservation: time.Minute}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("next handler called after failed quota reservation")
		return nil, nil
	})(context.Background(), state)
	if err == nil {
		t.Fatal("Wrap() error = nil, want quota store outage")
	}
	apiErr := gateway.AsAPIError(err)
	if apiErr.Status != http.StatusServiceUnavailable || apiErr.Code != "quota_store_unavailable" {
		t.Fatalf("api error = %#v, want terminal 503 quota_store_unavailable", apiErr)
	}
	if !errors.Is(err, backendErr) || strings.Contains(apiErr.Message, "password") {
		t.Fatalf("error cause/message = %v/%q, want preserved private cause and sanitized message", err, apiErr.Message)
	}
}

func TestQuotaPreservesInitialReservationDenial(t *testing.T) {
	denial := gateway.RateLimited("quota exceeded: rpm")
	quotaStore := &settlementQuotaStore{reserveErr: denial}
	state := quotaTestState("request-id", "execution-id")
	_, err := (policy.Quota{Store: quotaStore, Reservation: time.Minute}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("next handler called after denied quota reservation")
		return nil, nil
	})(context.Background(), state)
	if err != denial {
		t.Fatalf("Wrap() error = %#v, want original quota denial %#v", err, denial)
	}
}

func TestQuotaBypassesStoreOnlyWhenNoAccountingDimensionIsEnabled(t *testing.T) {
	inactiveStore := &settlementQuotaStore{}
	inactiveState := quotaTestState("request-id", "execution-id")
	inactiveState.Scopes = []gateway.ScopedLimit{{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key"},
		Limits: gateway.LimitSpec{
			BudgetDuration:    gateway.Duration{Duration: time.Hour},
			MaxInputTokens:    100,
			MaxOutputTokens:   100,
			ModelAllowlist:    []string{"alias"},
			ProviderAllowlist: []string{"openai"},
		},
	}}
	called := false
	exec, err := (policy.Quota{Store: inactiveStore}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		called = true
		return &gateway.Execution{}, nil
	})(context.Background(), inactiveState)
	if err != nil || exec == nil || !called {
		t.Fatalf("inactive quota Wrap() = (%#v, %v), called=%t", exec, err, called)
	}
	if inactiveStore.reserveID != "" {
		t.Fatalf("inactive quota reserved ticket %q", inactiveStore.reserveID)
	}

	activeLimits := []gateway.LimitSpec{
		{RPM: 1},
		{TPM: 1},
		{MaxParallel: 1},
		{MaxSpendMicros: 1},
		{SoftSpendMicros: 1},
		{DailyTokens: 1},
		{MonthlyTokens: 1},
	}
	for _, limit := range activeLimits {
		quotaStore := &settlementQuotaStore{}
		state := quotaTestState("request-id", "execution-id")
		state.Scopes[0].Limits = limit
		exec, err := (policy.Quota{Store: quotaStore, Reservation: time.Minute}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
			return &gateway.Execution{Attempt: &gateway.Attempt{Route: state.Candidates[0]}}, nil
		})(context.Background(), state)
		if err != nil {
			t.Fatalf("active limit %#v: Wrap() error = %v", limit, err)
		}
		if quotaStore.reserveID != "execution-id" {
			t.Fatalf("active limit %#v: reservation id = %q", limit, quotaStore.reserveID)
		}
		if err := exec.Settle(context.Background(), gateway.Usage{}, nil); err != nil {
			t.Fatalf("active limit %#v: Settle() error = %v", limit, err)
		}
	}
}

func TestQuotaTopUpAccountsForActualUsageAboveReservation(t *testing.T) {
	quotaStore := &settlementQuotaStore{}
	state := quotaTestState("request-id", "execution-id")
	wrapped := policy.Quota{Store: quotaStore, Reservation: time.Minute}.Wrap(func(ctx context.Context, got *gateway.RequestState) (*gateway.Execution, error) {
		if err := got.ReserveProviderAttempt(ctx, &gateway.Attempt{ID: "attempt-1", Route: state.Candidates[0]}); err != nil {
			return nil, err
		}
		return &gateway.Execution{
			Attempt:  &gateway.Attempt{Route: state.Candidates[0]},
			Finalize: func(context.Context, gateway.Usage, error) error { return nil },
		}, nil
	})
	exec, err := wrapped(context.Background(), state)
	if err != nil {
		t.Fatalf("Wrap() error = %v", err)
	}
	if err := exec.Settle(context.Background(), gateway.Usage{InputTokens: 9, OutputTokens: 7, TotalTokens: 16}, nil); err != nil {
		t.Fatalf("Settle() error = %v", err)
	}
	if quotaStore.topUp.TotalTokens() != 6 {
		t.Fatalf("top-up tokens = %d, want 6", quotaStore.topUp.TotalTokens())
	}
}

func TestQuotaReservationTTLIncludesAllAttempts(t *testing.T) {
	quotaStore := &settlementQuotaStore{}
	state := quotaTestState("request-id", "execution-id")
	state.Candidates[0].Route.Timeout = gateway.Duration{Duration: 2 * time.Second}
	state.Candidates[0].Route.Retries = 2
	wrapped := policy.Quota{Store: quotaStore, Reservation: time.Second}.Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		return &gateway.Execution{Attempt: &gateway.Attempt{Route: state.Candidates[0]}}, nil
	})
	exec, err := wrapped(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if quotaStore.reserveTTL < 16*time.Second {
		t.Fatalf("reservation TTL = %v, want at least retries*timeouts plus settlement buffer", quotaStore.reserveTTL)
	}
	if err := exec.Settle(context.Background(), gateway.Usage{}, errors.New("request failed")); err != nil {
		t.Fatal(err)
	}
}

func TestQuotaRoundsSubMicroSpendUp(t *testing.T) {
	quotaStore := &settlementQuotaStore{}
	state := quotaTestState("request-id", "execution-id")
	state.Candidates[0].Route.Pricing = gateway.Pricing{InputPer1M: 0.15}
	wrapped := policy.Quota{Store: quotaStore, Reservation: time.Minute}.Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		return &gateway.Execution{Attempt: &gateway.Attempt{Route: state.Candidates[0]}}, nil
	})
	exec, err := wrapped(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Settle(context.Background(), gateway.Usage{InputTokens: 1, TotalTokens: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if quotaStore.committed.SpendMicros != 1 {
		t.Fatalf("committed spend = %d micro, want conservative ceil to 1", quotaStore.committed.SpendMicros)
	}
}

func TestQuotaAccountsBillableRetriesAndFallbackWithoutDoubleCounting(t *testing.T) {
	for _, test := range []struct {
		name                string
		failedError         error
		failedRetries       int
		wantFailedCalls     int
		failedCallsBillable bool
	}{
		{
			name:                "two billable failed calls before fallback",
			failedError:         gateway.NewError(http.StatusBadGateway, "upstream_error", "temporary", "temporary").WithDisposition(true, true, true),
			failedRetries:       1,
			wantFailedCalls:     2,
			failedCallsBillable: true,
		},
		{
			name:                "unbilled 429 before fallback",
			failedError:         gateway.WithoutAttemptCharge(gateway.RateLimited("busy")),
			failedRetries:       1,
			wantFailedCalls:     1,
			failedCallsBillable: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			quotaStore := &settlementQuotaStore{}
			provider := &accountingProvider{failedError: test.failedError, estimates: map[string]gateway.Usage{}}
			failedPricing := gateway.Pricing{InputPer1M: 1, OutputPer1M: 2}
			successPricing := gateway.Pricing{InputPer1M: 3, OutputPer1M: 4}
			snapshot := accountingSnapshot(test.failedRetries, failedPricing, successPricing)
			engine := gateway.NewEngine(
				gateway.NewConfigStore(snapshot),
				[]gateway.Provider{provider},
				[]gateway.RequestInterceptor{
					policy.TokenValidation{},
					fixedQuotaScope{},
					policy.Quota{Store: quotaStore, Reservation: time.Minute},
				},
				[]gateway.AttemptInterceptor{&policy.AttemptLimits{}},
			)
			exec, err := engine.Execute(context.Background(), &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "alias",
				RawBody:   []byte(`{"model":"alias","messages":[{"role":"user","content":"ping"}]}`),
				Hints:     gateway.RequestHints{MaxOutputTokens: 5},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := provider.calls["failed"]; got != test.wantFailedCalls {
				t.Fatalf("failed route calls = %d, want %d", got, test.wantFailedCalls)
			}
			failedEstimate := provider.estimates["failed"]
			if quotaStore.reserved.TotalTokens() != 0 || exec.State.Reserved.TotalTokens() == 0 || quotaStore.topUps.TotalTokens() == 0 {
				t.Fatalf("request/attempt reservations = %#v/%#v/%#v, want zero request hold and positive JIT attempt hold", quotaStore.reserved, exec.State.Reserved, quotaStore.topUps)
			}

			finalUsage := gateway.Usage{InputTokens: 2, OutputTokens: 1, TotalTokens: 3}
			if err := exec.Settle(context.Background(), finalUsage, nil); err != nil {
				t.Fatal(err)
			}
			wantCommitted := gateway.ActualUsage{
				InputTokens:  2,
				OutputTokens: 1,
				SpendMicros:  successPricing.SpendMicros(2, 1),
			}
			if test.failedCallsBillable {
				wantCommitted.InputTokens += failedEstimate.InputTokens * int64(test.wantFailedCalls)
				wantCommitted.OutputTokens += failedEstimate.OutputTokens * int64(test.wantFailedCalls)
				wantCommitted.SpendMicros += failedPricing.SpendMicros(failedEstimate.InputTokens, failedEstimate.OutputTokens) * int64(test.wantFailedCalls)
			}
			if quotaStore.committed != wantCommitted {
				t.Fatalf("committed = %#v, want aggregate %#v", quotaStore.committed, wantCommitted)
			}
		})
	}
}

func TestQuotaReusesUnbillableAttemptHoldForFallback(t *testing.T) {
	quotaStore := store.NewMemoryQuotaStore()
	provider := &accountingProvider{
		failedError: gateway.WithoutAttemptCharge(gateway.RateLimited("rejected before billing")),
		estimates:   map[string]gateway.Usage{},
	}
	snapshot := accountingSnapshot(0, gateway.Pricing{}, gateway.Pricing{})
	engine := gateway.NewEngine(
		gateway.NewConfigStore(snapshot),
		[]gateway.Provider{provider},
		[]gateway.RequestInterceptor{
			fixedCandidateEstimate{usage: gateway.Usage{InputTokens: 5, OutputTokens: 5, TotalTokens: 10}},
			fixedQuotaLimit{limit: gateway.LimitSpec{DailyTokens: 15}},
			policy.Quota{Store: quotaStore, Reservation: time.Minute},
		},
		[]gateway.AttemptInterceptor{&policy.AttemptLimits{}},
	)
	exec, err := engine.Execute(context.Background(), &gateway.Request{Operation: gateway.OpChatCompletions, Model: "alias"})
	if err != nil {
		t.Fatalf("fallback was denied by the released unbillable hold: %v", err)
	}
	if provider.calls["failed"] != 1 || provider.calls["success"] != 1 {
		t.Fatalf("provider calls = %#v, want one rejected call and one fallback", provider.calls)
	}
	if err := exec.Settle(context.Background(), exec.Result.Usage, nil); err != nil {
		t.Fatal(err)
	}
}

func TestQuotaReusesUnusedBillableAttemptHoldForFallback(t *testing.T) {
	quotaStore := store.NewMemoryQuotaStore()
	provider := &accountingProvider{
		failedError: gateway.WithAttemptUsage(
			gateway.NewError(http.StatusBadGateway, "upstream_error", "temporary", "temporary").WithDisposition(false, true, true),
			gateway.Usage{InputTokens: 1, TotalTokens: 1},
		),
		estimates: map[string]gateway.Usage{},
	}
	snapshot := accountingSnapshot(0, gateway.Pricing{}, gateway.Pricing{})
	engine := gateway.NewEngine(
		gateway.NewConfigStore(snapshot),
		[]gateway.Provider{provider},
		[]gateway.RequestInterceptor{
			fixedCandidateEstimate{usage: gateway.Usage{InputTokens: 5, OutputTokens: 5, TotalTokens: 10}},
			fixedQuotaLimit{limit: gateway.LimitSpec{DailyTokens: 15}},
			policy.Quota{Store: quotaStore, Reservation: time.Minute},
		},
		[]gateway.AttemptInterceptor{&policy.AttemptLimits{}},
	)
	exec, err := engine.Execute(context.Background(), &gateway.Request{Operation: gateway.OpChatCompletions, Model: "alias"})
	if err != nil {
		t.Fatalf("fallback was denied by unused billable hold: %v", err)
	}
	if provider.calls["failed"] != 1 || provider.calls["success"] != 1 {
		t.Fatalf("provider calls = %#v, want one billed failure and one fallback", provider.calls)
	}
	if err := exec.Settle(context.Background(), exec.Result.Usage, nil); err != nil {
		t.Fatal(err)
	}
	usage, err := quotaStore.GetUsage(context.Background(), gateway.ScopedLimit{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key"}, Limits: gateway.LimitSpec{DailyTokens: 15},
	})
	if err != nil {
		t.Fatal(err)
	}
	if usage.DailyUsedTokens != 4 {
		t.Fatalf("committed tokens = %d, want explicit failed usage 1 plus fallback usage 3", usage.DailyUsedTokens)
	}
}

func TestQuotaFailedAttemptOverageStopsFallback(t *testing.T) {
	quotaStore := store.NewMemoryQuotaStore()
	provider := &accountingProvider{
		failedError: gateway.WithAttemptUsage(
			gateway.NewError(http.StatusBadGateway, "upstream_error", "temporary", "temporary").WithDisposition(false, true, true),
			gateway.Usage{InputTokens: 12, OutputTokens: 8, TotalTokens: 20},
		),
		estimates: map[string]gateway.Usage{},
	}
	engine := gateway.NewEngine(
		gateway.NewConfigStore(accountingSnapshot(0, gateway.Pricing{}, gateway.Pricing{})),
		[]gateway.Provider{provider},
		[]gateway.RequestInterceptor{
			fixedCandidateEstimate{usage: gateway.Usage{InputTokens: 5, OutputTokens: 5, TotalTokens: 10}},
			fixedQuotaLimit{limit: gateway.LimitSpec{DailyTokens: 15}},
			policy.Quota{Store: quotaStore, Reservation: time.Minute},
		},
		[]gateway.AttemptInterceptor{&policy.AttemptLimits{}},
	)
	_, err := engine.Execute(context.Background(), &gateway.Request{Operation: gateway.OpChatCompletions, Model: "alias"})
	if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Status != http.StatusTooManyRequests {
		t.Fatalf("Execute() error = %#v, want terminal quota overage", apiErr)
	}
	if provider.calls["failed"] != 1 || provider.calls["success"] != 0 {
		t.Fatalf("provider calls = %#v, want no fallback after actual usage exceeds quota", provider.calls)
	}
}

func TestQuotaAdmissionFailureDoesNotConsumeRouteRatesOrOpenCircuit(t *testing.T) {
	quotaStore := &failingTopUpQuotaStore{err: errors.New("redis unavailable")}
	rates := &countingRateStore{}
	provider := &accountingProvider{estimates: map[string]gateway.Usage{}}
	snapshot := accountingSnapshot(0, gateway.Pricing{}, gateway.Pricing{})
	delete(snapshot.Routes, "failed")
	route := snapshot.Routes["success"]
	route.Limits.RPM = 10
	route.Limits.TPM = 100
	route.Circuit = gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Minute}}
	breaker := policy.NewBreaker()
	engine := gateway.NewEngine(
		gateway.NewConfigStore(snapshot),
		[]gateway.Provider{provider},
		[]gateway.RequestInterceptor{
			fixedCandidateEstimate{usage: gateway.Usage{InputTokens: 5, OutputTokens: 5, TotalTokens: 10}},
			fixedQuotaLimit{limit: gateway.LimitSpec{DailyTokens: 100}},
			policy.Quota{Store: quotaStore, Reservation: time.Minute},
		},
		[]gateway.AttemptInterceptor{&policy.AttemptLimits{Rates: rates, Breakers: breaker}},
	)
	_, err := engine.Execute(context.Background(), &gateway.Request{Operation: gateway.OpChatCompletions, Model: "alias"})
	if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Code != "quota_store_unavailable" {
		t.Fatalf("Execute() error = %#v, want classified quota store failure", apiErr)
	}
	if provider.calls["success"] != 0 || rates.calls != 0 {
		t.Fatalf("provider/rate calls = %d/%d, want 0/0", provider.calls["success"], rates.calls)
	}
	if allowed, _ := breaker.AllowAttempt(route); !allowed {
		t.Fatal("quota store failure opened the provider circuit")
	}
}

func TestQuotaAbortedStreamFillsMissingOutputButCompletedStreamDoesNot(t *testing.T) {
	for _, test := range []struct {
		name       string
		stream     io.ReadCloser
		abort      bool
		wantOutput int64
		wantSpend  int64
	}{
		{name: "aborted partial usage", stream: &firstChunkStream{chunk: []byte("data: partial\n\n")}, abort: true, wantOutput: 5, wantSpend: 45},
		{name: "completed zero output", stream: io.NopCloser(strings.NewReader("data: done\n\n")), wantOutput: 0, wantSpend: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			quotaStore := &settlementQuotaStore{}
			provider := &streamAccountingProvider{stream: test.stream}
			snapshot := accountingSnapshot(0, gateway.Pricing{}, gateway.Pricing{
				InputPer1M: 1, OutputPer1M: 1, CacheWritePer1M: 5,
				ProviderUnits: map[string]gateway.ProviderUnitPricing{
					"search": {MicrosPerUnit: 7.5, MaxUnitsPerRequest: 4},
				},
			})
			delete(snapshot.Routes, "failed")
			engine := gateway.NewEngine(
				gateway.NewConfigStore(snapshot), []gateway.Provider{provider},
				[]gateway.RequestInterceptor{policy.TokenValidation{}, fixedQuotaScope{}, policy.Quota{Store: quotaStore, Reservation: time.Minute}}, nil,
			)
			exec, err := engine.Execute(context.Background(), &gateway.Request{
				Operation: gateway.OpChatCompletions, Model: "alias", Stream: true,
				RawBody: []byte(`{"model":"alias","stream":true}`),
				Hints: gateway.RequestHints{
					MaxOutputTokens: 5, MayWritePromptCache: true, ProviderUnits: []string{"search"},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			var callErr error
			if test.abort {
				callErr = errors.New("client disconnected")
				_ = exec.Result.RawStream.Close()
			} else {
				_, _ = io.Copy(io.Discard, exec.Result.RawStream)
				_ = exec.Result.RawStream.Close()
			}
			actual := gateway.Usage{InputTokens: 2, TotalTokens: 2}
			if err := exec.Settle(context.Background(), actual, callErr); err != nil {
				t.Fatal(err)
			}
			if quotaStore.committed.InputTokens != 2 || quotaStore.committed.OutputTokens != test.wantOutput ||
				quotaStore.committed.SpendMicros != test.wantSpend {
				t.Fatalf("committed = %#v, want input=2 output=%d spend=%d", quotaStore.committed, test.wantOutput, test.wantSpend)
			}
		})
	}
}

func TestQuotaRefundsProviderCallCanceledBeforeDispatch(t *testing.T) {
	var upstreamCalls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		upstreamCalls.Add(1)
	}))
	defer upstream.Close()

	quotaStore := &settlementQuotaStore{}
	snapshot := accountingSnapshot(0, gateway.Pricing{}, gateway.Pricing{InputPer1M: 1, OutputPer1M: 1})
	delete(snapshot.Routes, "failed")
	snapshot.Routes["success"].BaseURL = upstream.URL
	engine := gateway.NewEngine(
		gateway.NewConfigStore(snapshot), []gateway.Provider{proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())},
		[]gateway.RequestInterceptor{policy.TokenValidation{}, fixedQuotaScope{}, policy.Quota{Store: quotaStore, Reservation: time.Minute}}, nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := engine.Execute(ctx, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "alias",
		RawBody:   []byte(`{"model":"alias","messages":[{"role":"user","content":"ping"}]}`),
		Hints:     gateway.RequestHints{MaxOutputTokens: 5},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if upstreamCalls.Load() != 0 {
		t.Fatalf("upstream calls = %d, want zero", upstreamCalls.Load())
	}
	if quotaStore.commitCalls != 0 || quotaStore.refundCalls != 1 {
		t.Fatalf("settlement calls = commit:%d refund:%d, want 0/1", quotaStore.commitCalls, quotaStore.refundCalls)
	}
}

func TestAbandonedExecutionStopsRenewingAtMaximumAge(t *testing.T) {
	quotaStore := &renewalCountingStore{}
	state := quotaTestState("request-id", "execution-id")
	_, err := (policy.Quota{
		Store: quotaStore, Reservation: time.Minute,
		RenewalInterval: 10 * time.Millisecond, MaxReservationAge: 50 * time.Millisecond,
	}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		return &gateway.Execution{Attempt: &gateway.Attempt{Route: state.Candidates[0]}}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(90 * time.Millisecond)
	count := quotaStore.renewals.Load()
	if count == 0 {
		t.Fatal("renewals = 0, want heartbeat before maximum age")
	}
	time.Sleep(40 * time.Millisecond)
	if got := quotaStore.renewals.Load(); got != count {
		t.Fatalf("renewals continued after maximum age: before=%d after=%d", count, got)
	}
}

func quotaTestState(requestID, executionID string) *gateway.RequestState {
	route := &gateway.Route{Name: "route", Provider: "openai", Model: "model"}
	return &gateway.RequestState{
		Request: &gateway.Request{Meta: gateway.Meta{RequestID: requestID, ExecutionID: executionID}},
		Scopes: []gateway.ScopedLimit{{
			Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key"},
			Limits: gateway.LimitSpec{DailyTokens: 100},
		}},
		Candidates: []gateway.ResolvedRoute{{
			Route:    route,
			Request:  &gateway.Request{},
			Estimate: gateway.Usage{InputTokens: 4, OutputTokens: 6, TotalTokens: 10},
		}},
	}
}

type settlementQuotaStore struct {
	reserveID        string
	reserveTTL       time.Duration
	reserved         gateway.EstimatedUsage
	reserveErr       error
	topUp            gateway.EstimatedUsage
	topUps           gateway.EstimatedUsage
	committed        gateway.ActualUsage
	commitContextErr error
	refundContextErr error
	commitErr        error
	refundErr        error
	commitCalls      int
	refundCalls      int
}

type fixedQuotaScope struct{}

func (fixedQuotaScope) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		state.Scopes = []gateway.ScopedLimit{{
			Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key"}, Limits: gateway.LimitSpec{DailyTokens: 1_000_000},
		}}
		return next(ctx, state)
	}
}

type fixedQuotaLimit struct{ limit gateway.LimitSpec }

func (f fixedQuotaLimit) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		state.Scopes = []gateway.ScopedLimit{{Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key"}, Limits: f.limit}}
		return next(ctx, state)
	}
}

type fixedCandidateEstimate struct{ usage gateway.Usage }

func (f fixedCandidateEstimate) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		candidates, err := state.ResolveCandidates()
		if err != nil {
			return nil, err
		}
		for i := range candidates {
			candidates[i].Estimate = f.usage
		}
		state.ReplaceCandidates(candidates)
		state.Estimate = f.usage
		return next(ctx, state)
	}
}

type failingTopUpQuotaStore struct{ err error }

func (*failingTopUpQuotaStore) Reserve(_ context.Context, requestID string, _ []gateway.ScopedLimit, _ gateway.EstimatedUsage, _ time.Duration) (gateway.QuotaTicket, error) {
	return gateway.QuotaTicket{RequestID: requestID}, nil
}
func (s *failingTopUpQuotaStore) TopUp(context.Context, gateway.QuotaTicket, []gateway.ScopedLimit, gateway.EstimatedUsage, time.Duration) error {
	return s.err
}
func (*failingTopUpQuotaStore) Commit(context.Context, gateway.QuotaTicket, gateway.ActualUsage) error {
	return nil
}
func (*failingTopUpQuotaStore) Refund(context.Context, gateway.QuotaTicket) error { return nil }

type countingRateStore struct{ calls int }

func (s *countingRateStore) Allow(context.Context, string, store.RateLimit, int64) error {
	s.calls++
	return nil
}

func (s *countingRateStore) AllowBatch(context.Context, []store.RateRequest) error {
	s.calls++
	return nil
}

type accountingProvider struct {
	failedError error
	calls       map[string]int
	estimates   map[string]gateway.Usage
}

func (p *accountingProvider) Name() string                    { return "openai" }
func (p *accountingProvider) Supports(gateway.Operation) bool { return true }
func (p *accountingProvider) Invoke(_ context.Context, route gateway.ResolvedRoute, _ *gateway.Request) (*gateway.Result, error) {
	if p.calls == nil {
		p.calls = map[string]int{}
	}
	p.calls[route.Route.Name]++
	p.estimates[route.Route.Name] = route.Estimate
	if route.Route.Name == "failed" {
		return nil, p.failedError
	}
	return &gateway.Result{
		RawBody: []byte(`{"ok":true}`),
		Usage:   gateway.Usage{InputTokens: 2, OutputTokens: 1, TotalTokens: 3},
	}, nil
}

type streamAccountingProvider struct{ stream io.ReadCloser }

func (p *streamAccountingProvider) Name() string                    { return "openai" }
func (p *streamAccountingProvider) Supports(gateway.Operation) bool { return true }
func (p *streamAccountingProvider) Invoke(context.Context, gateway.ResolvedRoute, *gateway.Request) (*gateway.Result, error) {
	return &gateway.Result{
		RawStream: p.stream,
		UsageSnapshot: func() gateway.Usage {
			return gateway.Usage{InputTokens: 2, TotalTokens: 2}
		},
	}, nil
}

type firstChunkStream struct {
	chunk []byte
	sent  bool
}

func (s *firstChunkStream) Read(p []byte) (int, error) {
	if s.sent {
		return 0, nil
	}
	s.sent = true
	return copy(p, s.chunk), nil
}

func (*firstChunkStream) Close() error { return nil }

func accountingSnapshot(failedRetries int, failedPricing, successPricing gateway.Pricing) *gateway.Snapshot {
	capabilities := gateway.Capability{
		Operations: []gateway.Operation{gateway.OpChatCompletions}, Streaming: true, Tokenizer: "cl100k_base",
	}
	return &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"failed": {
			Name: "failed", Provider: "openai", Model: "alias", Priority: 2, Retries: failedRetries,
			Timeout: gateway.Duration{Duration: time.Second}, Pricing: failedPricing, Capabilities: capabilities,
		},
		"success": {
			Name: "success", Provider: "openai", Model: "alias", Priority: 1,
			Timeout: gateway.Duration{Duration: time.Second}, Pricing: successPricing, Capabilities: capabilities,
		},
	}}
}

type renewalCountingStore struct {
	renewals atomic.Int64
}

func (s *renewalCountingStore) Reserve(_ context.Context, requestID string, _ []gateway.ScopedLimit, _ gateway.EstimatedUsage, _ time.Duration) (gateway.QuotaTicket, error) {
	return gateway.QuotaTicket{RequestID: requestID}, nil
}
func (s *renewalCountingStore) TopUp(context.Context, gateway.QuotaTicket, []gateway.ScopedLimit, gateway.EstimatedUsage, time.Duration) error {
	s.renewals.Add(1)
	return nil
}
func (s *renewalCountingStore) Commit(context.Context, gateway.QuotaTicket, gateway.ActualUsage) error {
	return nil
}
func (s *renewalCountingStore) Refund(context.Context, gateway.QuotaTicket) error { return nil }

func (s *settlementQuotaStore) Reserve(_ context.Context, requestID string, _ []gateway.ScopedLimit, usage gateway.EstimatedUsage, ttl time.Duration) (gateway.QuotaTicket, error) {
	s.reserveID = requestID
	s.reserveTTL = ttl
	s.reserved = usage
	return gateway.QuotaTicket{RequestID: requestID}, s.reserveErr
}

func (s *settlementQuotaStore) TopUp(_ context.Context, _ gateway.QuotaTicket, _ []gateway.ScopedLimit, delta gateway.EstimatedUsage, _ time.Duration) error {
	s.topUp = delta
	s.topUps.InputTokens += delta.InputTokens
	s.topUps.ReservedOutputTokens += delta.ReservedOutputTokens
	s.topUps.EstimatedSpendMicros += delta.EstimatedSpendMicros
	return nil
}

func (s *settlementQuotaStore) Commit(ctx context.Context, _ gateway.QuotaTicket, actual gateway.ActualUsage) error {
	s.commitCalls++
	s.commitContextErr = ctx.Err()
	s.committed = actual
	return s.commitErr
}

func (s *settlementQuotaStore) Refund(ctx context.Context, _ gateway.QuotaTicket) error {
	s.refundCalls++
	s.refundContextErr = ctx.Err()
	return s.refundErr
}
