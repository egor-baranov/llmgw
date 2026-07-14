package policy_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llmgw/gateway"
	"llmgw/observer"
	"llmgw/policy"
	"llmgw/proxy"
	"llmgw/store"

	"github.com/golang-jwt/jwt/v5"
)

func TestRequestSizeRejectsRouteLimit(t *testing.T) {
	interceptor := policy.RequestSize{}
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{
			Routes: map[string]*gateway.Route{
				"small": {
					Name:     "small",
					Provider: "openai",
					Model:    "gpt-test",
					Limits:   gateway.LimitConfig{MaxBodyBytes: 16},
					Capabilities: gateway.Capability{
						Operations: []gateway.Operation{gateway.OpChatCompletions},
					},
				},
			},
		},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-test",
			Hints:     gateway.RequestHints{PromptText: "too big"},
			Meta:      gateway.Meta{BodyBytes: 32},
		},
	}

	_, err := interceptor.Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("next handler should not be called")
		return nil, nil
	})(context.Background(), state)
	if err == nil {
		t.Fatal("Wrap() error = nil, want body_too_large")
	}
	apiErr := gateway.AsAPIError(err)
	if apiErr.Status != http.StatusRequestEntityTooLarge || apiErr.Code != "body_too_large" {
		t.Fatalf("api error = %#v, want 413/body_too_large", apiErr)
	}
}

func TestMetadataValidationRejectsUnsafeForwardedIdentityBeforeAttempt(t *testing.T) {
	provider := &invocationCountingProvider{name: "openai"}
	route := &gateway.Route{
		Name:     "primary",
		Provider: "openai",
		Model:    "model",
		Circuit: gateway.CircuitConfig{
			Failures: 1,
			Cooldown: gateway.Duration{Duration: time.Minute},
		},
		Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
	}
	snapshot := &gateway.Snapshot{
		Auth:   gateway.AuthConfig{AllowAnonymous: true},
		Routes: map[string]*gateway.Route{"primary": route},
	}
	breaker := policy.NewBreaker()
	engine := gateway.NewEngine(
		gateway.NewConfigStore(snapshot),
		[]gateway.Provider{provider},
		[]gateway.RequestInterceptor{policy.Auth{}, policy.MetadataValidation{}},
		[]gateway.AttemptInterceptor{&policy.AttemptLimits{Breakers: breaker}, policy.AttemptHeaders{}},
	)
	_, err := engine.Execute(context.Background(), &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "model",
		Meta:      gateway.Meta{User: "bad\r\nvalue"},
	})
	apiErr := gateway.AsAPIError(err)
	if err == nil || apiErr.Status != http.StatusBadRequest || apiErr.Code != "invalid_metadata" || apiErr.Param != "user" {
		t.Fatalf("Execute() error = %#v, want invalid user metadata 400", apiErr)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.calls)
	}
	if allowed, _ := breaker.AllowAttempt(route); !allowed {
		t.Fatal("route breaker opened for a rejected client metadata value")
	}

	state := &gateway.RequestState{Request: &gateway.Request{Meta: gateway.Meta{Project: strings.Repeat("p", 513)}}}
	_, err = (policy.MetadataValidation{}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("next handler called for oversized project")
		return nil, nil
	})(context.Background(), state)
	if apiErr = gateway.AsAPIError(err); err == nil || apiErr.Status != http.StatusBadRequest || apiErr.Param != "project" {
		t.Fatalf("oversized project error = %#v, want project 400", apiErr)
	}
}

func TestBreakerDoesNotCarryOpenStateAcrossRouteReplacement(t *testing.T) {
	breaker := policy.NewBreaker()
	oldRoute := &gateway.Route{
		Name: "primary", Provider: "openai", BaseURL: "https://old.example/v1", UpstreamModel: "old-model",
		Circuit: gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Minute}},
	}
	newRoute := &gateway.Route{
		Name: "primary", Provider: "openai", BaseURL: "https://new.example/v1", UpstreamModel: "new-model",
		Circuit: oldRoute.Circuit,
	}
	allowed, startedAt := breaker.AllowAttempt(oldRoute)
	if !allowed {
		t.Fatal("old route was not initially admitted")
	}
	breaker.FailAttempt(oldRoute, startedAt, errors.New("old endpoint failed"))
	if allowed, _ := breaker.AllowAttempt(oldRoute); allowed {
		t.Fatal("old route breaker remained closed after threshold failure")
	}
	if allowed, _ := breaker.AllowAttempt(newRoute); !allowed {
		t.Fatal("replacement route inherited the old endpoint's open circuit")
	}
}

func TestBreakerIgnoresSuccessFromAttemptOlderThanLatestFailure(t *testing.T) {
	breaker := policy.NewBreaker()
	route := &gateway.Route{
		Name: "primary", Provider: "openai", BaseURL: "https://example.com/v1", UpstreamModel: "model",
		Circuit: gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Minute}},
	}
	allowed, slowStartedAt := breaker.AllowAttempt(route)
	if !allowed {
		t.Fatal("first attempt was not admitted")
	}
	allowed, failedStartedAt := breaker.AllowAttempt(route)
	if !allowed {
		t.Fatal("second attempt was not admitted")
	}
	breaker.FailAttempt(route, failedStartedAt, errors.New("newer attempt failed"))
	if allowed, _ := breaker.AllowAttempt(route); allowed {
		t.Fatal("breaker remained closed after threshold failure")
	}

	breaker.SuccessAttempt(route, slowStartedAt)
	if allowed, _ := breaker.AllowAttempt(route); allowed {
		t.Fatal("older in-flight success closed the newer open circuit")
	}
}

func TestAttemptLimitsProviderConcurrency(t *testing.T) {
	limits := &policy.AttemptLimits{}
	state := &gateway.RequestState{}
	route1 := &gateway.Route{
		Name:     "route-1",
		Provider: "openai",
		Timeout:  gateway.Duration{Duration: time.Second},
		Limits:   gateway.LimitConfig{ProviderConcurrency: 1},
	}
	route2 := &gateway.Route{
		Name:     "route-2",
		Provider: "openai",
		Timeout:  gateway.Duration{Duration: time.Second},
		Limits:   gateway.LimitConfig{ProviderConcurrency: 1},
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	handler := limits.WrapAttempt(func(ctx context.Context, _ *gateway.RequestState, attempt *gateway.Attempt) (*gateway.Result, error) {
		if attempt.Route.Route.Name == "route-1" {
			started <- struct{}{}
			<-release
		}
		return &gateway.Result{}, nil
	})

	done := make(chan error, 1)
	go func() {
		_, err := handler(context.Background(), state, &gateway.Attempt{
			Route:     gateway.ResolvedRoute{Route: route1},
			StartedAt: time.Now(),
		})
		done <- err
	}()
	<-started

	_, err := handler(context.Background(), state, &gateway.Attempt{
		Route:     gateway.ResolvedRoute{Route: route2},
		StartedAt: time.Now(),
	})
	if err == nil {
		t.Fatal("second attempt error = nil, want provider concurrency limit exceeded")
	}
	if got := gateway.AsAPIError(err).Message; !strings.Contains(got, "provider concurrency limit exceeded") {
		t.Fatalf("error message = %q, want provider concurrency limit exceeded", got)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first attempt error = %v, want nil", err)
	}
}

func TestAttemptLimitsAppliesRaisedConcurrencyAfterReload(t *testing.T) {
	limits := &policy.AttemptLimits{}
	route := &gateway.Route{
		Name:     "route-reloaded",
		Provider: "openai",
		Timeout:  gateway.Duration{Duration: time.Second},
		Limits:   gateway.LimitConfig{Concurrency: 1},
	}
	invoke := limits.WrapAttempt(func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
		return &gateway.Result{RawStream: io.NopCloser(strings.NewReader("data: chunk\n\n"))}, nil
	})
	first, err := invoke(context.Background(), &gateway.RequestState{}, &gateway.Attempt{
		ID: "first", Route: gateway.ResolvedRoute{Route: route},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invoke(context.Background(), &gateway.RequestState{}, &gateway.Attempt{
		ID: "blocked", Route: gateway.ResolvedRoute{Route: route},
	}); err == nil {
		t.Fatal("second attempt before reload error = nil, want concurrency rejection")
	}

	// A new immutable config snapshot supplies a new route object with a higher
	// limit but the same route name/limiter key.
	reloaded := *route
	reloaded.Limits.Concurrency = 2
	second, err := invoke(context.Background(), &gateway.RequestState{}, &gateway.Attempt{
		ID: "second", Route: gateway.ResolvedRoute{Route: &reloaded},
	})
	if err != nil {
		t.Fatalf("attempt after raised limit = %v, want success", err)
	}
	_ = second.RawStream.Close()
	_ = first.RawStream.Close()
}

func TestAttemptLimitsUsesDistributedState(t *testing.T) {
	breakerStartedAt := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	dist := &stubState{breakerAllowed: true, breakerStartedAt: breakerStartedAt}
	limits := &policy.AttemptLimits{State: dist}
	state := &gateway.RequestState{
		Request: &gateway.Request{
			Meta: gateway.Meta{RequestID: "req-1"},
		},
	}
	attempt := &gateway.Attempt{
		ID: "attempt-1",
		Route: gateway.ResolvedRoute{
			Route: &gateway.Route{
				Name:     "route-1",
				Provider: "openai",
				Timeout:  gateway.Duration{Duration: time.Second},
				Limits: gateway.LimitConfig{
					Concurrency:         1,
					ProviderConcurrency: 1,
				},
				Circuit: gateway.CircuitConfig{
					Failures: 2,
					Cooldown: gateway.Duration{Duration: time.Second},
				},
			},
		},
		StartedAt: time.Now(),
	}

	_, err := limits.WrapAttempt(func(_ context.Context, _ *gateway.RequestState, got *gateway.Attempt) (*gateway.Result, error) {
		if !got.StartedAt.Equal(breakerStartedAt) {
			t.Fatalf("attempt started_at = %v, want breaker admission time %v", got.StartedAt, breakerStartedAt)
		}
		return &gateway.Result{}, nil
	})(context.Background(), state, attempt)
	if err != nil {
		t.Fatalf("WrapAttempt() error = %v, want nil", err)
	}
	if len(dist.acquireCalls) != 2 {
		t.Fatalf("acquire calls = %d, want 2", len(dist.acquireCalls))
	}
	if dist.acquireCalls[0] != "route:route-1" || dist.acquireCalls[1] != "provider:openai" {
		t.Fatalf("acquire calls = %#v, want route/provider buckets", dist.acquireCalls)
	}
	if len(dist.releaseCalls) != 2 {
		t.Fatalf("release calls = %d, want 2", len(dist.releaseCalls))
	}
	if dist.breakerSuccessCalls != 1 {
		t.Fatalf("breaker success calls = %d, want 1", dist.breakerSuccessCalls)
	}
	if !dist.breakerOutcomeStartedAt.Equal(breakerStartedAt) {
		t.Fatalf("breaker outcome started_at = %v, want %v", dist.breakerOutcomeStartedAt, breakerStartedAt)
	}
	if dist.breakerRetention != 31*time.Second {
		t.Fatalf("breaker evidence retention = %v, want 31s", dist.breakerRetention)
	}
}

func TestAttemptLimitsSupportsLegacyStateContract(t *testing.T) {
	dist := &legacyState{breakerAllowed: true}
	limits := &policy.AttemptLimits{State: dist}
	attempt := &gateway.Attempt{Route: gateway.ResolvedRoute{Route: &gateway.Route{
		Name:     "legacy-route",
		Provider: "openai",
		Timeout:  gateway.Duration{Duration: time.Second},
		Circuit:  gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Second}},
	}}}
	want := errors.New("provider failed")
	_, err := limits.WrapAttempt(func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
		return nil, want
	})(context.Background(), &gateway.RequestState{}, attempt)
	if !errors.Is(err, want) {
		t.Fatalf("WrapAttempt() error = %v, want %v", err, want)
	}
	if dist.breakerAllowCalls != 1 || dist.breakerFailCalls != 1 {
		t.Fatalf("legacy breaker calls = (allow %d, fail %d), want (1, 1)", dist.breakerAllowCalls, dist.breakerFailCalls)
	}
	if dist.breakerFailureClass != "provider_failure" {
		t.Fatalf("legacy breaker failure class = %q, want provider_failure", dist.breakerFailureClass)
	}
}

func TestAttemptLimitsReportsDistributedBreakerUpdateErrors(t *testing.T) {
	updateErr := errors.New("breaker backend unavailable")
	upstreamErr := errors.New("upstream failed")
	tests := []struct {
		name      string
		operation string
		configure func(*stubState)
		next      gateway.AttemptHandler
	}{
		{
			name:      "success update",
			operation: "success",
			configure: func(state *stubState) { state.breakerSuccessErr = updateErr },
			next: func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
				return &gateway.Result{}, nil
			},
		},
		{
			name:      "failure update",
			operation: "failure",
			configure: func(state *stubState) { state.breakerFailErr = updateErr },
			next: func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
				return nil, upstreamErr
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			distributed := &stubState{breakerAllowed: true}
			tt.configure(distributed)
			var observedOperation string
			var observedErr error
			limits := &policy.AttemptLimits{
				State: distributed,
				OnBreakerUpdateError: func(operation string, err error) {
					observedOperation = operation
					observedErr = err
				},
			}
			attempt := &gateway.Attempt{Route: gateway.ResolvedRoute{Route: &gateway.Route{
				Name:     "route-1",
				Provider: "openai",
				Timeout:  gateway.Duration{Duration: time.Second},
				Circuit:  gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Second}},
			}}}
			_, _ = limits.WrapAttempt(tt.next)(context.Background(), &gateway.RequestState{}, attempt)
			if observedOperation != tt.operation || !errors.Is(observedErr, updateErr) {
				t.Fatalf("breaker hook = (%q, %v), want (%q, %v)", observedOperation, observedErr, tt.operation, updateErr)
			}
			if tt.operation == "failure" && distributed.breakerFailureClass != "provider_failure" {
				t.Fatalf("breaker failure class = %q, want non-sensitive provider_failure", distributed.breakerFailureClass)
			}
		})
	}
}

func TestAttemptLimitsDistributedBreakerOpen(t *testing.T) {
	dist := &stubState{breakerAllowed: false}
	limits := &policy.AttemptLimits{State: dist}
	attempt := &gateway.Attempt{
		Route: gateway.ResolvedRoute{
			Route: &gateway.Route{
				Name:     "route-1",
				Provider: "openai",
				Timeout:  gateway.Duration{Duration: time.Second},
			},
		},
		StartedAt: time.Now(),
	}

	_, err := limits.WrapAttempt(func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
		t.Fatal("attempt handler should not be called when breaker is open")
		return nil, nil
	})(context.Background(), &gateway.RequestState{}, attempt)
	if err == nil {
		t.Fatal("WrapAttempt() error = nil, want circuit_open")
	}
	apiErr := gateway.AsAPIError(err)
	if apiErr.Code != "circuit_open" {
		t.Fatalf("api error = %#v, want circuit_open", apiErr)
	}
	if len(dist.acquireCalls) != 0 {
		t.Fatalf("acquire calls = %#v, want none when breaker is open", dist.acquireCalls)
	}
}

func TestAttemptLimitsTreatsDistributedStateFailureAsTerminal(t *testing.T) {
	for _, test := range []struct {
		name     string
		state    *stubState
		wantCode string
	}{
		{
			name:     "breaker store",
			state:    &stubState{breakerErr: errors.New("redis breaker password leaked")},
			wantCode: "breaker_unavailable",
		},
		{
			name:     "concurrency store",
			state:    &stubState{breakerAllowed: true, acquireErr: errors.New("redis limiter password leaked")},
			wantCode: "distributed_limiter_unavailable",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &invocationCountingProvider{name: "openai"}
			snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
				"primary": {
					Name: "primary", Provider: "openai", Model: "alias", Priority: 2,
					Timeout: gateway.Duration{Duration: time.Second},
					Limits:  gateway.LimitConfig{Concurrency: 1},
					Capabilities: gateway.Capability{Operations: []gateway.Operation{
						gateway.OpChatCompletions,
					}},
				},
				"secondary": {
					Name: "secondary", Provider: "openai", Model: "alias", Priority: 1,
					Timeout: gateway.Duration{Duration: time.Second},
					Limits:  gateway.LimitConfig{Concurrency: 1},
					Capabilities: gateway.Capability{Operations: []gateway.Operation{
						gateway.OpChatCompletions,
					}},
				},
			}}
			engine := gateway.NewEngine(
				gateway.NewConfigStore(snapshot),
				[]gateway.Provider{provider},
				nil,
				[]gateway.AttemptInterceptor{&policy.AttemptLimits{State: test.state}},
			)

			_, err := engine.Execute(context.Background(), &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "alias",
			})
			if err == nil {
				t.Fatal("Execute() error = nil, want distributed state outage")
			}
			apiErr := gateway.AsAPIError(err)
			if apiErr.Status != http.StatusServiceUnavailable || apiErr.Code != test.wantCode {
				t.Fatalf("api error = %#v, want terminal 503 %s", apiErr, test.wantCode)
			}
			if strings.Contains(apiErr.Message, "password") {
				t.Fatalf("api message exposed backend error: %q", apiErr.Message)
			}
			cause := test.state.breakerErr
			if cause == nil {
				cause = test.state.acquireErr
			}
			if !errors.Is(err, cause) {
				t.Fatalf("error = %v, want preserved backend cause", err)
			}
			if provider.calls != 0 {
				t.Fatalf("provider calls = %d, want 0", provider.calls)
			}
			if test.state.breakerAllowCalls != 1 || len(test.state.acquireCalls) > 1 {
				t.Fatalf("breaker/acquire calls = %d/%#v, want only the primary route", test.state.breakerAllowCalls, test.state.acquireCalls)
			}
		})
	}
}

func TestAttemptLimitsChecksRPMAndTPM(t *testing.T) {
	rates := &stubRateStore{}
	limits := &policy.AttemptLimits{Rates: rates}
	state := &gateway.RequestState{
		Estimate: gateway.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
	}
	attempt := &gateway.Attempt{
		Route: gateway.ResolvedRoute{
			Route: &gateway.Route{
				Name:     "route-1",
				Provider: "openai",
				Timeout:  gateway.Duration{Duration: time.Second},
				Limits: gateway.LimitConfig{
					RPM: 10,
					TPM: 100,
				},
			},
			Estimate: gateway.Usage{TotalTokens: 23},
		},
		StartedAt: time.Now(),
	}

	_, err := limits.WrapAttempt(func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
		return &gateway.Result{}, nil
	})(context.Background(), state, attempt)
	if err != nil {
		t.Fatalf("WrapAttempt() error = %v, want nil", err)
	}
	if len(rates.calls) != 2 {
		t.Fatalf("rate calls = %d, want 2", len(rates.calls))
	}
	if rates.calls[0].key != "rpm:route-1" || rates.calls[0].n != 1 || rates.calls[0].limit.Rate != 10 {
		t.Fatalf("rpm call = %#v, want route rpm check", rates.calls[0])
	}
	if rates.calls[1].key != "tpm:route-1" || rates.calls[1].n != 23 || rates.calls[1].limit.Rate != 100 {
		t.Fatalf("tpm call = %#v, want route tpm check using attempt estimate", rates.calls[1])
	}
}

func TestAttemptLimitsSupportsLegacyNonBatchRateStore(t *testing.T) {
	rates := &legacyRateStore{}
	limits := &policy.AttemptLimits{Rates: rates}
	attempt := &gateway.Attempt{Route: gateway.ResolvedRoute{
		Route: &gateway.Route{
			Name: "legacy", Provider: "openai", Timeout: gateway.Duration{Duration: time.Second},
			Limits: gateway.LimitConfig{RPM: 10, TPM: 100},
		},
		Estimate: gateway.Usage{TotalTokens: 23},
	}}
	_, err := limits.WrapAttempt(func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
		return &gateway.Result{}, nil
	})(context.Background(), &gateway.RequestState{}, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if len(rates.calls) != 2 || rates.calls[0].key != "rpm:legacy" || rates.calls[1].key != "tpm:legacy" {
		t.Fatalf("legacy rate calls = %#v, want sequential RPM and TPM checks", rates.calls)
	}
}

func TestAttemptLimitsSaturatesOverflowedTokenEstimate(t *testing.T) {
	rates := &stubRateStore{}
	limits := &policy.AttemptLimits{Rates: rates}
	attempt := &gateway.Attempt{Route: gateway.ResolvedRoute{
		Route: &gateway.Route{
			Name: "overflow", Provider: "openai", Timeout: gateway.Duration{Duration: time.Second},
			Limits: gateway.LimitConfig{TPM: math.MaxInt64},
		},
		Estimate: gateway.Usage{InputTokens: 1, OutputTokens: math.MaxInt64, TotalTokens: math.MinInt64},
	}}
	_, err := limits.WrapAttempt(func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
		return &gateway.Result{}, nil
	})(context.Background(), &gateway.RequestState{}, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if len(rates.calls) != 1 || rates.calls[0].n != math.MaxInt64 {
		t.Fatalf("TPM calls = %#v, want saturated positive MaxInt64", rates.calls)
	}
}

func TestAttemptLimitsMapsRouteRateErrors(t *testing.T) {
	rates := &stubRateStore{errByKey: map[string]error{
		"rpm:route-1": gateway.RateLimited("backend said no"),
	}}
	limits := &policy.AttemptLimits{Rates: rates}
	attempt := &gateway.Attempt{
		Route: gateway.ResolvedRoute{
			Route: &gateway.Route{
				Name:     "route-1",
				Provider: "openai",
				Timeout:  gateway.Duration{Duration: time.Second},
				Limits: gateway.LimitConfig{
					RPM: 1,
				},
			},
		},
		StartedAt: time.Now(),
	}

	_, err := limits.WrapAttempt(func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
		t.Fatal("attempt handler should not be called when rpm check fails")
		return nil, nil
	})(context.Background(), &gateway.RequestState{}, attempt)
	if err == nil {
		t.Fatal("WrapAttempt() error = nil, want rate limited")
	}
	apiErr := gateway.AsAPIError(err)
	if apiErr.Status != http.StatusTooManyRequests || apiErr.Message != "rpm limit exceeded" {
		t.Fatalf("api error = %#v, want 429 rpm limit exceeded", apiErr)
	}
}

func TestAttemptLimitsTreatsRateStoreFailureAsTerminalDependencyOutage(t *testing.T) {
	backendErr := errors.New("redis password leaked in diagnostic")
	rates := &stubRateStore{errByKey: map[string]error{"rpm:primary": backendErr}}
	provider := &invocationCountingProvider{name: "openai"}
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"primary": {
			Name: "primary", Provider: "openai", Model: "alias", Priority: 2,
			Timeout: gateway.Duration{Duration: time.Second},
			Limits:  gateway.LimitConfig{RPM: 1},
			Capabilities: gateway.Capability{Operations: []gateway.Operation{
				gateway.OpChatCompletions,
			}},
		},
		"secondary": {
			Name: "secondary", Provider: "openai", Model: "alias", Priority: 1,
			Timeout: gateway.Duration{Duration: time.Second},
			Limits:  gateway.LimitConfig{RPM: 1},
			Capabilities: gateway.Capability{Operations: []gateway.Operation{
				gateway.OpChatCompletions,
			}},
		},
	}}
	engine := gateway.NewEngine(
		gateway.NewConfigStore(snapshot),
		[]gateway.Provider{provider},
		nil,
		[]gateway.AttemptInterceptor{&policy.AttemptLimits{Rates: rates}},
	)

	_, err := engine.Execute(context.Background(), &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "alias",
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want rate store outage")
	}
	apiErr := gateway.AsAPIError(err)
	if apiErr.Status != http.StatusServiceUnavailable || apiErr.Code != "rate_store_unavailable" {
		t.Fatalf("api error = %#v, want terminal 503 rate_store_unavailable", apiErr)
	}
	if !errors.Is(err, backendErr) || strings.Contains(apiErr.Message, "password") {
		t.Fatalf("error cause/message = %v/%q, want preserved private cause and sanitized message", err, apiErr.Message)
	}
	if len(rates.calls) != 1 || rates.calls[0].key != "rpm:primary" {
		t.Fatalf("rate calls = %#v, want only the primary candidate", rates.calls)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.calls)
	}
}

func TestAttemptLimitsAccountsEveryRetry(t *testing.T) {
	rates := &stubRateStore{}
	limits := &policy.AttemptLimits{Rates: rates}
	attempt := &gateway.Attempt{Number: 2, ID: "route-attempt", Route: gateway.ResolvedRoute{Route: &gateway.Route{
		Name:     "route-1",
		Provider: "openai",
		Retries:  2,
		Timeout:  gateway.Duration{Duration: time.Second},
		Limits:   gateway.LimitConfig{RPM: 10, TPM: 100},
	}, Estimate: gateway.Usage{TotalTokens: 7}}}
	upstreamCalls := 0
	var attemptIDs, retryHeaders []string
	hooks := &countingAttemptHooks{}

	providerHandler := func(_ context.Context, _ *gateway.RequestState, got *gateway.Attempt) (*gateway.Result, error) {
		upstreamCalls++
		attemptIDs = append(attemptIDs, got.ID)
		retryHeaders = append(retryHeaders, got.Route.Headers.Get("X-LLMGW-Retry"))
		if upstreamCalls < 3 {
			return nil, gateway.NewError(http.StatusBadGateway, "upstream_error", "temporary", "temporary failure").WithDisposition(true, true, true)
		}
		return &gateway.Result{}, nil
	}
	perCall := policy.AttemptHeaders{}.WrapAttempt(hooks.WrapAttempt(providerHandler))
	_, err := limits.WrapAttempt(perCall)(context.Background(), &gateway.RequestState{Request: &gateway.Request{}}, attempt)
	if err != nil {
		t.Fatalf("WrapAttempt() error = %v, want nil after retry", err)
	}
	if upstreamCalls != 3 || hooks.calls != 3 {
		t.Fatalf("upstream/hook calls = %d/%d, want 3/3", upstreamCalls, hooks.calls)
	}
	if len(rates.calls) != 6 {
		t.Fatalf("rate calls = %d, want rpm+tpm for each of 3 upstream calls", len(rates.calls))
	}
	if strings.Join(retryHeaders, ",") != "0,1,2" || attemptIDs[0] == attemptIDs[1] || attemptIDs[1] == attemptIDs[2] {
		t.Fatalf("retry headers/attempt IDs = %#v/%#v", retryHeaders, attemptIDs)
	}
}

type countingAttemptHooks struct{ calls int }

func (h *countingAttemptHooks) WrapAttempt(next gateway.AttemptHandler) gateway.AttemptHandler {
	return func(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt) (*gateway.Result, error) {
		h.calls++
		return next(ctx, state, attempt)
	}
}

func TestAttemptLimitsDoesNotTripBreakerForRequestErrorsOrCancellation(t *testing.T) {
	for _, test := range []struct {
		name string
		ctx  func() context.Context
		err  func(context.Context) error
	}{
		{
			name: "upstream 400",
			ctx:  context.Background,
			err: func(context.Context) error {
				return gateway.NewError(http.StatusBadRequest, "invalid_request_error", "bad_request", "bad request")
			},
		},
		{
			name: "client cancellation",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			err: func(ctx context.Context) error { return ctx.Err() },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dist := &stubState{breakerAllowed: true}
			limits := &policy.AttemptLimits{State: dist}
			attempt := &gateway.Attempt{Route: gateway.ResolvedRoute{Route: &gateway.Route{
				Name:     "route-1",
				Provider: "openai",
				Timeout:  gateway.Duration{Duration: time.Second},
				Circuit:  gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Second}},
			}}}
			_, _ = limits.WrapAttempt(func(ctx context.Context, _ *gateway.RequestState, _ *gateway.Attempt) (*gateway.Result, error) {
				return nil, test.err(ctx)
			})(test.ctx(), &gateway.RequestState{}, attempt)
			if dist.breakerFailCalls != 0 {
				t.Fatalf("breaker failures = %d, want 0", dist.breakerFailCalls)
			}
		})
	}
}

func TestAttemptLimitsKeepsDelayedSSEContextAndConcurrencyUntilEOF(t *testing.T) {
	started := make(chan struct{}, 1)
	releaseChunk := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case started <- struct{}{}:
		default:
		}
		<-releaseChunk
		_, _ = io.WriteString(w, "data: {\"id\":\"chunk\"}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	limits := &policy.AttemptLimits{}
	route := &gateway.Route{
		Name:     "route-1",
		Provider: "openai",
		BaseURL:  upstream.URL + "/v1",
		Model:    "route-model",
		Timeout:  gateway.Duration{Duration: 2 * time.Second},
		Limits:   gateway.LimitConfig{ProviderConcurrency: 1},
	}
	request := &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Stream:    true,
		RawBody:   json.RawMessage(`{"model":"route-model","stream":true,"messages":[{"role":"user","content":"ping"}]}`),
	}
	invoke := limits.WrapAttempt(func(ctx context.Context, _ *gateway.RequestState, attempt *gateway.Attempt) (*gateway.Result, error) {
		return provider.Invoke(ctx, attempt.Route, request)
	})
	firstAttempt := &gateway.Attempt{ID: "attempt-1", Route: gateway.ResolvedRoute{Route: route}}
	result, err := invoke(context.Background(), &gateway.RequestState{}, firstAttempt)
	if err != nil {
		t.Fatal(err)
	}
	<-started

	_, err = invoke(context.Background(), &gateway.RequestState{}, &gateway.Attempt{ID: "attempt-2", Route: gateway.ResolvedRoute{Route: route}})
	if err == nil || !strings.Contains(gateway.AsAPIError(err).Message, "provider concurrency limit exceeded") {
		t.Fatalf("second attempt error = %v, want provider concurrency limit", err)
	}

	close(releaseChunk)
	body, err := io.ReadAll(result.RawStream)
	if err != nil {
		t.Fatalf("delayed stream read failed (attempt context was canceled too early): %v", err)
	}
	if !strings.Contains(string(body), "data: [DONE]") {
		t.Fatalf("stream body = %q, want delayed SSE data", body)
	}
	_ = result.RawStream.Close()

	third, err := invoke(context.Background(), &gateway.RequestState{}, &gateway.Attempt{ID: "attempt-3", Route: gateway.ResolvedRoute{Route: route}})
	if err != nil {
		t.Fatalf("attempt after EOF = %v, want released concurrency slot", err)
	}
	_, _ = io.ReadAll(third.RawStream)
	_ = third.RawStream.Close()
}

func TestAttemptLimitsStreamCloseIsNotBreakerFailure(t *testing.T) {
	dist := &stubState{breakerAllowed: true}
	limits := &policy.AttemptLimits{State: dist}
	attempt := &gateway.Attempt{ID: "attempt-1", Route: gateway.ResolvedRoute{Route: &gateway.Route{
		Name:     "route-1",
		Provider: "openai",
		Timeout:  gateway.Duration{Duration: time.Second},
		Circuit:  gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Second}},
	}}}
	result, err := limits.WrapAttempt(func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
		return &gateway.Result{RawStream: io.NopCloser(strings.NewReader("data: chunk\n\n"))}, nil
	})(context.Background(), &gateway.RequestState{}, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := result.RawStream.Close(); err != nil {
		t.Fatal(err)
	}
	if dist.breakerFailCalls != 0 {
		t.Fatalf("breaker failures = %d, want 0 for downstream close", dist.breakerFailCalls)
	}
}

func TestAttemptLimitsReleasesUnreadStreamWhenAttemptTimesOut(t *testing.T) {
	limits := &policy.AttemptLimits{}
	obs := observer.New("test")
	obs.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	route := &gateway.Route{
		Name: "timeout-stream", Provider: "openai",
		Timeout: gateway.Duration{Duration: 40 * time.Millisecond},
		Limits:  gateway.LimitConfig{Concurrency: 1},
	}
	providerHandler := func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
		return &gateway.Result{RawStream: io.NopCloser(strings.NewReader("data: chunk\n\n"))}, nil
	}
	invoke := limits.WrapAttempt((observer.AttemptMetrics{Obs: obs}).WrapAttempt(providerHandler))
	first, err := invoke(context.Background(), &gateway.RequestState{}, &gateway.Attempt{ID: "first", Route: gateway.ResolvedRoute{Route: route}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invoke(context.Background(), &gateway.RequestState{}, &gateway.Attempt{ID: "blocked", Route: gateway.ResolvedRoute{Route: route}}); err == nil {
		t.Fatal("second attempt before timeout error = nil, want concurrency rejection")
	}
	time.Sleep(80 * time.Millisecond)
	var metrics bytes.Buffer
	obs.Metrics.Set.WritePrometheus(&metrics)
	if !strings.Contains(metrics.String(), `llmgw_attempts_total{provider="openai",route="timeout-stream",status="error"} 1`) {
		t.Fatalf("timeout attempt metric missing: %s", metrics.String())
	}
	third, err := invoke(context.Background(), &gateway.RequestState{}, &gateway.Attempt{ID: "third", Route: gateway.ResolvedRoute{Route: route}})
	if err != nil {
		t.Fatalf("attempt after stream timeout = %v, want released slot", err)
	}
	_ = first.RawStream.Close()
	_ = third.RawStream.Close()
}

func TestAttemptHeadersSetsGatewayHeaders(t *testing.T) {
	state := &gateway.RequestState{
		Request: &gateway.Request{
			Model: "gemini-2.5-flash",
			Meta: gateway.Meta{
				RequestID: "req-1",
				User:      "alice",
				Project:   "project-1",
			},
		},
	}
	attempt := &gateway.Attempt{
		ID:     "attempt-1",
		Number: 2,
		Route: gateway.ResolvedRoute{
			Route: &gateway.Route{
				Name:     "gemini-primary",
				Provider: "gemini",
				Model:    "gemini-2.5-flash",
			},
		},
	}

	_, err := policy.AttemptHeaders{}.WrapAttempt(func(_ context.Context, _ *gateway.RequestState, attempt *gateway.Attempt) (*gateway.Result, error) {
		headers := attempt.Route.Headers
		if headers.Get("X-LLMGW-Request-ID") != "req-1" {
			t.Fatalf("request id header = %q, want req-1", headers.Get("X-LLMGW-Request-ID"))
		}
		if headers.Get("X-LLMGW-Attempt") != "2" || headers.Get("X-LLMGW-Attempt-ID") != "attempt-1" {
			t.Fatalf("attempt headers = %#v, want attempt metadata", headers)
		}
		if headers.Get("X-LLMGW-Provider") != "gemini" || headers.Get("X-LLMGW-Route") != "gemini-primary" {
			t.Fatalf("route headers = %#v, want provider and route", headers)
		}
		if headers.Get("X-LLMGW-Model") != "gemini-2.5-flash" {
			t.Fatalf("model headers = %#v, want routed model", headers)
		}
		if headers.Get("X-LLMGW-User") != "alice" || headers.Get("X-LLMGW-Project") != "project-1" {
			t.Fatalf("principal headers = %#v, want user and project", headers)
		}
		if headers.Get("x-goog-api-client") != "" {
			t.Fatal("provider-native client headers must be applied by the upstream adapter")
		}
		return &gateway.Result{}, nil
	})(context.Background(), state, attempt)
	if err != nil {
		t.Fatalf("WrapAttempt() error = %v, want nil", err)
	}
}

type countingProvider struct {
	name     string
	usage    gateway.Usage
	calls    int
	lastReq  *gateway.Request
	lastPath string
}

type invocationCountingProvider struct {
	name  string
	calls int
}

func (p *invocationCountingProvider) Name() string                    { return p.name }
func (p *invocationCountingProvider) Supports(gateway.Operation) bool { return true }
func (p *invocationCountingProvider) Invoke(context.Context, gateway.ResolvedRoute, *gateway.Request) (*gateway.Result, error) {
	p.calls++
	return &gateway.Result{StatusCode: http.StatusOK, RawBody: []byte(`{}`)}, nil
}

type stubState struct {
	breakerAllowed          bool
	breakerAllowCalls       int
	breakerErr              error
	breakerStartedAt        time.Time
	breakerOutcomeStartedAt time.Time
	breakerRetention        time.Duration
	acquireErr              error
	acquireCalls            []string
	releaseCalls            []string
	breakerFailCalls        int
	breakerSuccessCalls     int
	breakerFailErr          error
	breakerSuccessErr       error
	breakerFailureClass     string
}

type legacyState struct {
	breakerAllowed      bool
	breakerAllowCalls   int
	breakerFailCalls    int
	breakerFailureClass string
}

var _ store.State = (*legacyState)(nil)

func (*legacyState) AcquireSlot(context.Context, string, string, int64, time.Duration) error {
	return nil
}

func (*legacyState) ReleaseSlot(context.Context, string, string) error { return nil }

func (s *legacyState) BreakerAllow(context.Context, string, time.Time) (bool, error) {
	s.breakerAllowCalls++
	return s.breakerAllowed, nil
}

func (s *legacyState) BreakerFail(_ context.Context, _ string, _ int, _ time.Duration, failureClass string, _ time.Time) error {
	s.breakerFailCalls++
	s.breakerFailureClass = failureClass
	return nil
}

func (*legacyState) BreakerSuccess(context.Context, string, time.Time) error { return nil }

func (s *stubState) AcquireSlot(_ context.Context, bucket, _ string, _ int64, _ time.Duration) error {
	s.acquireCalls = append(s.acquireCalls, bucket)
	return s.acquireErr
}

func (s *stubState) ReleaseSlot(_ context.Context, bucket, _ string) error {
	s.releaseCalls = append(s.releaseCalls, bucket)
	return nil
}

func (s *stubState) BreakerAllowAttempt(_ context.Context, _ string, retention time.Duration) (bool, time.Time, error) {
	s.breakerAllowCalls++
	s.breakerRetention = retention
	startedAt := s.breakerStartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	return s.breakerAllowed, startedAt, s.breakerErr
}

func (s *stubState) BreakerFailAttempt(_ context.Context, _ string, startedAt time.Time, _ int, _ time.Duration, failureClass string) error {
	s.breakerFailCalls++
	s.breakerOutcomeStartedAt = startedAt
	s.breakerFailureClass = failureClass
	return s.breakerFailErr
}

func (s *stubState) BreakerSuccessAttempt(_ context.Context, _ string, startedAt time.Time) error {
	s.breakerSuccessCalls++
	s.breakerOutcomeStartedAt = startedAt
	return s.breakerSuccessErr
}

func (s *stubState) BreakerAllow(ctx context.Context, route string, _ time.Time) (bool, error) {
	allowed, _, err := s.BreakerAllowAttempt(ctx, route, time.Hour)
	return allowed, err
}

func (s *stubState) BreakerFail(ctx context.Context, route string, threshold int, cooldown time.Duration, failureClass string, now time.Time) error {
	return s.BreakerFailAttempt(ctx, route, now, threshold, cooldown, failureClass)
}

func (s *stubState) BreakerSuccess(ctx context.Context, route string, now time.Time) error {
	return s.BreakerSuccessAttempt(ctx, route, now)
}

func (c *countingProvider) Name() string { return c.name }

func (c *countingProvider) CountTokens(_ context.Context, route gateway.ResolvedRoute, req *gateway.Request) (gateway.Usage, error) {
	c.calls++
	c.lastReq = req
	if route.Route != nil {
		c.lastPath = route.Route.Name
	}
	return c.usage, nil
}

func TestTokenValidationPrefersProviderCount(t *testing.T) {
	counter := &countingProvider{
		name:  "openai",
		usage: gateway.Usage{InputTokens: 42, OutputTokens: 12, TotalTokens: 54},
	}
	validation := policy.TokenValidation{
		Counters: map[string]gateway.TokenCounter{
			"openai": counter,
		},
	}

	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{
			Routes: map[string]*gateway.Route{
				"openai-primary": {
					Name:     "openai-primary",
					Provider: "openai",
					Model:    "gpt-4o-mini",
					Capabilities: gateway.Capability{
						Operations:      []gateway.Operation{gateway.OpChatCompletions},
						MaxInputTokens:  1000,
						MaxOutputTokens: 1000,
						Tokenizer:       "o200k_base",
					},
				},
			},
		},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-4o-mini",
			Hints:     gateway.RequestHints{MaxOutputTokens: 12, PromptText: "count this"},
		},
	}

	handler := validation.Wrap(func(_ context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if state.Estimate.TotalTokens != 54 {
			t.Fatalf("estimate = %#v, want provider count", state.Estimate)
		}
		return &gateway.Execution{}, nil
	})
	if _, err := handler(context.Background(), state); err != nil {
		t.Fatalf("Wrap() error = %v, want nil", err)
	}
	if counter.calls != 1 {
		t.Fatalf("CountTokens() calls = %d, want 1", counter.calls)
	}
	if counter.lastReq == nil || counter.lastReq.Model != "gpt-4o-mini" {
		t.Fatalf("counter request = %#v, want resolved upstream model", counter.lastReq)
	}
	if counter.lastPath != "openai-primary" {
		t.Fatalf("counter route = %q, want openai-primary", counter.lastPath)
	}
}

func TestTokenValidationRetainsPromptEstimateForAuxiliaryOnlyProviderCount(t *testing.T) {
	counter := &countingProvider{
		name: "openai",
		usage: gateway.Usage{
			InputDetails:    &gateway.UsageDetails{},
			ProviderDetails: map[string]int64{"web_search_requests": 2},
		},
	}
	validation := policy.TokenValidation{
		Counters: map[string]gateway.TokenCounter{"openai": counter},
	}
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{Routes: map[string]*gateway.Route{
			"openai-primary": {
				Name: "openai-primary", Provider: "openai", Model: "gpt-test",
				Capabilities: gateway.Capability{
					Operations:      []gateway.Operation{gateway.OpChatCompletions},
					MaxOutputTokens: 100,
					Tokenizer:       "cl100k_base",
				},
			},
		}},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-test",
			Hints:     gateway.RequestHints{PromptText: "retain this prompt estimate", MaxOutputTokens: 5},
		},
	}

	_, err := validation.Wrap(func(_ context.Context, got *gateway.RequestState) (*gateway.Execution, error) {
		if got.Estimate.InputTokens <= 0 || got.Estimate.OutputTokens != 5 {
			t.Fatalf("estimate = %#v, want tokenizer input and requested output", got.Estimate)
		}
		if got.Estimate.ProviderDetails["web_search_requests"] != 2 {
			t.Fatalf("provider details = %#v, want auxiliary counter preserved", got.Estimate.ProviderDetails)
		}
		return &gateway.Execution{}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTokenValidationPrefersConfiguredTokenizerOverCheapEstimate(t *testing.T) {
	validation := policy.TokenValidation{}
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{Routes: map[string]*gateway.Route{
			"openai-primary": {
				Name:     "openai-primary",
				Provider: "openai",
				Model:    "gpt-test",
				Capabilities: gateway.Capability{
					Operations: []gateway.Operation{gateway.OpChatCompletions},
					Tokenizer:  "cl100k_base",
				},
			},
		}},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-test",
			RawBody:   json.RawMessage(`{"model":"gpt-test","messages":[{"role":"user","content":"one two three four five six seven eight nine ten"}]}`),
			Hints:     gateway.RequestHints{EstimatedInputTokens: 1, MaxOutputTokens: 1},
		},
	}

	_, err := validation.Wrap(func(_ context.Context, got *gateway.RequestState) (*gateway.Execution, error) {
		if got.Estimate.InputTokens <= 1 {
			t.Fatalf("input estimate = %d, want configured tokenizer result over cheap estimate 1", got.Estimate.InputTokens)
		}
		return &gateway.Execution{}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTokenValidationSaturatesTotalAtMaxInt64(t *testing.T) {
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{Routes: map[string]*gateway.Route{
			"route": {
				Name: "route", Provider: "openai", Model: "gpt-test",
				Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}, Tokenizer: "cl100k_base"},
			},
		}},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-test",
			RawBody:   json.RawMessage(`{"model":"gpt-test","messages":[{"role":"user","content":"x"}]}`),
			Hints:     gateway.RequestHints{MaxOutputTokens: math.MaxInt},
		},
	}
	_, err := (policy.TokenValidation{}).Wrap(func(_ context.Context, got *gateway.RequestState) (*gateway.Execution, error) {
		if got.Estimate.TotalTokens != math.MaxInt64 || got.Estimate.TotalTokens < got.Estimate.InputTokens {
			t.Fatalf("estimate = %#v, want saturated MaxInt64 total", got.Estimate)
		}
		return &gateway.Execution{}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTokenValidationReservesOutputMultiplicityAndRouteDefault(t *testing.T) {
	for _, tt := range []struct {
		name  string
		hints gateway.RequestHints
		cap   int
		want  int64
	}{
		{name: "explicit multiplied output", hints: gateway.RequestHints{MaxOutputTokens: 100, OutputMultiplicity: 10}, cap: 2_000, want: 1_000},
		{name: "omitted max uses route bound", hints: gateway.RequestHints{}, cap: 16_384, want: 16_384},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state := &gateway.RequestState{
				Snapshot: &gateway.Snapshot{Routes: map[string]*gateway.Route{
					"route": {
						Name: "route", Provider: "openai", Model: "gpt-test",
						Capabilities: gateway.Capability{
							Operations: []gateway.Operation{gateway.OpChatCompletions}, Tokenizer: "cl100k_base", MaxOutputTokens: tt.cap,
						},
					},
				}},
				Request: &gateway.Request{
					Operation: gateway.OpChatCompletions, Model: "gpt-test",
					RawBody: json.RawMessage(`{"model":"gpt-test","messages":[{"role":"user","content":"x"}]}`), Hints: tt.hints,
				},
			}
			_, err := (policy.TokenValidation{}).Wrap(func(_ context.Context, got *gateway.RequestState) (*gateway.Execution, error) {
				if got.Estimate.OutputTokens != tt.want {
					t.Fatalf("output estimate = %d, want %d", got.Estimate.OutputTokens, tt.want)
				}
				return &gateway.Execution{}, nil
			})(context.Background(), state)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOutputMultiplicityIsDeniedBeforeUpstreamWhenQuotaIsTooSmall(t *testing.T) {
	quotaStore := store.NewMemoryQuotaStore()
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{Store: gateway.StoreConfig{ReservationTTL: gateway.Duration{Duration: time.Minute}}, Routes: map[string]*gateway.Route{
			"route": {
				Name: "route", Provider: "openai", Model: "gpt-test",
				Capabilities: gateway.Capability{
					Operations: []gateway.Operation{gateway.OpChatCompletions}, Tokenizer: "cl100k_base", MaxOutputTokens: 2_000,
				},
			},
		}},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions, Model: "gpt-test",
			RawBody: json.RawMessage(`{"model":"gpt-test","messages":[{"role":"user","content":"x"}]}`),
			Hints:   gateway.RequestHints{MaxOutputTokens: 100, OutputMultiplicity: 10},
			Meta:    gateway.Meta{ExecutionID: "multiplicity-quota"},
		},
		Scopes: []gateway.ScopedLimit{{
			Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key"}, Limits: gateway.LimitSpec{DailyTokens: 500},
		}},
	}
	nextCalled := false
	handler := (policy.TokenValidation{}).Wrap((policy.Quota{Store: quotaStore}).Wrap(func(ctx context.Context, got *gateway.RequestState) (*gateway.Execution, error) {
		if err := got.ReserveProviderAttempt(ctx, &gateway.Attempt{ID: "attempt-1", Route: got.Candidates[0]}); err != nil {
			return nil, err
		}
		nextCalled = true
		return &gateway.Execution{}, nil
	}))
	_, err := handler(context.Background(), state)
	if gateway.AsAPIError(err).Status != http.StatusTooManyRequests {
		t.Fatalf("error = %v, want quota denial", err)
	}
	if nextCalled {
		t.Fatal("upstream handler ran despite multiplied output exceeding quota")
	}
}

func TestTokenValidationUsesByteUpperBoundWithoutTokenizer(t *testing.T) {
	raw := json.RawMessage(`{"model":"gpt-test","messages":[{"role":"user","content":"你好👋世界"}]}`)
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{Routes: map[string]*gateway.Route{
			"route": {
				Name: "route", Provider: "openai", Model: "gpt-test",
				Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
			},
		}},
		Request: &gateway.Request{Operation: gateway.OpChatCompletions, Model: "gpt-test", RawBody: raw},
	}
	_, err := (policy.TokenValidation{}).Wrap(func(_ context.Context, got *gateway.RequestState) (*gateway.Execution, error) {
		if got.Estimate.InputTokens != int64(len(raw)) {
			t.Fatalf("input estimate = %d, want conservative byte bound %d", got.Estimate.InputTokens, len(raw))
		}
		return &gateway.Execution{}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultimodalSurchargesIncreaseQuotaReservation(t *testing.T) {
	const (
		visionSurcharge = 1200
		audioSurcharge  = 5000
	)
	quotaStore := &settlementQuotaStore{}
	route := &gateway.Route{
		Name: "multimodal", Provider: "openai", Model: "gpt-test",
		Capabilities: gateway.Capability{
			Operations:                []gateway.Operation{gateway.OpResponses},
			VisionInput:               true,
			Audio:                     true,
			MaxInputTokens:            100_000,
			Tokenizer:                 "cl100k_base",
			VisionInputTokenSurcharge: visionSurcharge,
			AudioInputTokenSurcharge:  audioSurcharge,
		},
	}
	request := &gateway.Request{
		Operation: gateway.OpResponses,
		Model:     "gpt-test",
		RawBody:   json.RawMessage(`{"model":"gpt-test","input":[{"role":"user","content":"describe these inputs"}]}`),
		Hints: gateway.RequestHints{
			VisionInputParts: 2,
			AudioInputParts:  2,
			RequiresVision:   true,
			RequiresAudio:    true,
			MaxOutputTokens:  1,
		},
		Meta: gateway.Meta{ExecutionID: "multimodal-reservation"},
	}
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{Routes: map[string]*gateway.Route{"multimodal": route}},
		Request:  request,
		Scopes: []gateway.ScopedLimit{{
			Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key"},
			Limits: gateway.LimitSpec{DailyTokens: 100_000},
		}},
	}
	handler := (policy.TokenValidation{}).Wrap((policy.Quota{Store: quotaStore, Reservation: time.Minute}).Wrap(
		func(ctx context.Context, got *gateway.RequestState) (*gateway.Execution, error) {
			if err := got.ReserveProviderAttempt(ctx, &gateway.Attempt{ID: "attempt-1", Route: got.Candidates[0]}); err != nil {
				return nil, err
			}
			return &gateway.Execution{}, nil
		},
	))
	exec, err := handler(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	wantSurcharge := int64(2*visionSurcharge + 2*audioSurcharge)
	if quotaStore.topUps.InputTokens <= wantSurcharge {
		t.Fatalf("reserved input tokens = %d, want text estimate plus multimodal surcharge > %d", quotaStore.topUps.InputTokens, wantSurcharge)
	}
	if state.Reserved.InputTokens != quotaStore.topUps.InputTokens {
		t.Fatalf("state reservation = %d, store reservation = %d", state.Reserved.InputTokens, quotaStore.topUps.InputTokens)
	}
	if err := exec.Settle(context.Background(), gateway.Usage{}, errors.New("stop test execution")); err != nil {
		t.Fatal(err)
	}
}

func TestMultimodalSurchargesEnforceRouteMaxInputTokens(t *testing.T) {
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{Routes: map[string]*gateway.Route{
			"limited": {
				Name: "limited", Provider: "openai", Model: "gpt-test",
				Capabilities: gateway.Capability{
					Operations:                []gateway.Operation{gateway.OpResponses},
					VisionInput:               true,
					MaxInputTokens:            2000,
					Tokenizer:                 "cl100k_base",
					VisionInputTokenSurcharge: 1200,
				},
			},
		}},
		Request: &gateway.Request{
			Operation: gateway.OpResponses,
			Model:     "gpt-test",
			RawBody:   json.RawMessage(`{"model":"gpt-test","input":"small"}`),
			Hints: gateway.RequestHints{
				VisionInputParts: 2,
				RequiresVision:   true,
			},
		},
	}
	_, err := (policy.TokenValidation{}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("next handler should not run when multimodal estimate exceeds max_input_tokens")
		return nil, nil
	})(context.Background(), state)
	if err == nil {
		t.Fatal("Wrap() error = nil, want max_input_tokens_exceeded")
	}
	apiErr := gateway.AsAPIError(err)
	if apiErr.Code != "max_input_tokens_exceeded" {
		t.Fatalf("error code = %q, want max_input_tokens_exceeded", apiErr.Code)
	}
}

func TestInlineMediaPayloadIsNotTokenizedAsText(t *testing.T) {
	inline := "data:image/png;base64," + strings.Repeat("A", 1<<20)
	raw := json.RawMessage(`{"model":"vision","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"` + inline + `"}},{"type":"text","text":"describe it"}]}]}`)
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{Routes: map[string]*gateway.Route{
			"vision": {
				Name: "vision", Provider: "openai", Model: "vision",
				Capabilities: gateway.Capability{
					Operations: []gateway.Operation{gateway.OpChatCompletions}, Tokenizer: "cl100k_base",
					VisionInput: true, VisionInputTokenSurcharge: 1024, MaxInputTokens: 10_000, MaxOutputTokens: 100,
				},
			},
		}},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions, Model: "vision", RawBody: raw,
			Hints: gateway.RequestHints{RequiresVision: true, VisionInputParts: 1, MaxOutputTokens: 10},
		},
	}
	_, err := (policy.TokenValidation{Projectors: map[string]gateway.TokenProjector{
		"openai": proxy.NewProvider(proxy.OpenAIAdapter(), nil),
	}}).Wrap(func(_ context.Context, got *gateway.RequestState) (*gateway.Execution, error) {
		if got.Estimate.InputTokens < 1024 || got.Estimate.InputTokens >= 10_000 {
			t.Fatalf("inline-media input estimate = %d, want bounded text projection plus surcharge", got.Estimate.InputTokens)
		}
		return &gateway.Execution{}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatalf("TokenValidation rejected a bounded inline image: %v", err)
	}
}

func TestOrdinaryDataPrefixedPromptRemainsTokenized(t *testing.T) {
	text := "data: " + strings.Repeat("ordinary prompt text ", 20_000)
	raw, err := json.Marshal(map[string]any{
		"model":    "text-model",
		"messages": []map[string]any{{"role": "user", "content": text}},
	})
	if err != nil {
		t.Fatal(err)
	}
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{Routes: map[string]*gateway.Route{
			"text": {
				Name: "text", Provider: "openai", Model: "text-model",
				Capabilities: gateway.Capability{
					Operations: []gateway.Operation{gateway.OpChatCompletions}, Tokenizer: "cl100k_base",
					MaxInputTokens: 1000, MaxOutputTokens: 100,
				},
			},
		}},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions, Model: "text-model", RawBody: raw,
			Hints: gateway.RequestHints{MaxOutputTokens: 10},
		},
	}
	_, err = (policy.TokenValidation{}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("large ordinary prompt bypassed max-input validation")
		return nil, nil
	})(context.Background(), state)
	if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Code != "max_input_tokens_exceeded" {
		t.Fatalf("TokenValidation error = %#v, want max_input_tokens_exceeded", apiErr)
	}
}

func TestToolPayloadBinaryNamedFieldsRemainTokenized(t *testing.T) {
	largeValue := strings.Repeat("A", 40_124)
	for _, test := range []struct {
		name     string
		response map[string]any
	}{
		{name: "b64_json", response: map[string]any{"b64_json": largeValue}},
		{name: "source data", response: map[string]any{"source": map[string]any{"data": largeValue}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"model": "gemini-test",
				"contents": []any{map[string]any{"parts": []any{map[string]any{
					"functionResponse": map[string]any{"name": "tool", "response": test.response},
				}}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			state := &gateway.RequestState{
				Snapshot: &gateway.Snapshot{Routes: map[string]*gateway.Route{
					"gemini": {
						Name: "gemini", Provider: "gemini", Model: "gemini-test",
						Capabilities: gateway.Capability{
							Operations: []gateway.Operation{gateway.OpChatCompletions}, Tokenizer: "cl100k_base",
							MaxInputTokens: 1000, MaxOutputTokens: 100,
						},
					},
				}},
				Request: &gateway.Request{
					Provider: "gemini", Operation: gateway.OpChatCompletions, Model: "gemini-test", RawBody: raw,
					Hints: gateway.RequestHints{MaxOutputTokens: 10},
				},
			}
			_, err = (policy.TokenValidation{}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
				t.Fatal("large tool response bypassed max-input validation")
				return nil, nil
			})(context.Background(), state)
			if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Code != "max_input_tokens_exceeded" {
				t.Fatalf("TokenValidation error = %#v, want max_input_tokens_exceeded", apiErr)
			}
		})
	}
}

func TestResolveScopesFiltersByUserAndProvider(t *testing.T) {
	interceptor := policy.ResolveScopes{}
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{
			Quota: gateway.QuotaConfig{
				Profiles: map[string]gateway.LimitSpec{
					"key-default": {
						ProviderAllowlist: []string{"anthropic"},
						MaxInputTokens:    100,
					},
				},
				Keys: map[string]string{
					"key-1": "key-default",
				},
			},
			Routes: map[string]*gateway.Route{
				"openai-primary": {
					Name:     "openai-primary",
					Provider: "openai",
					Model:    "shared-model",
					Capabilities: gateway.Capability{
						Operations:      []gateway.Operation{gateway.OpChatCompletions},
						MaxInputTokens:  1000,
						MaxOutputTokens: 1000,
					},
				},
				"anthropic-primary": {
					Name:     "anthropic-primary",
					Provider: "anthropic",
					Model:    "shared-model",
					Capabilities: gateway.Capability{
						Operations:      []gateway.Operation{gateway.OpChatCompletions},
						MaxInputTokens:  1000,
						MaxOutputTokens: 1000,
					},
				},
			},
		},
		Subject: gateway.Subject{KeyID: "key-1"},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "shared-model",
			Hints:     gateway.RequestHints{MaxOutputTokens: 8, PromptText: "hello"},
		},
	}
	state.ReplaceCandidates([]gateway.ResolvedRoute{
		{
			Route: &gateway.Route{Name: "openai-primary", Provider: "openai"},
			Request: &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "gpt-4o-mini",
				Hints:     gateway.RequestHints{MaxOutputTokens: 8},
			},
			Estimate: gateway.Usage{InputTokens: 10, OutputTokens: 8, TotalTokens: 18},
		},
		{
			Route: &gateway.Route{Name: "anthropic-primary", Provider: "anthropic"},
			Request: &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "claude",
				Hints:     gateway.RequestHints{MaxOutputTokens: 8},
			},
			Estimate: gateway.Usage{InputTokens: 10, OutputTokens: 8, TotalTokens: 18},
		},
	})

	_, err := interceptor.Wrap(func(_ context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if len(state.Scopes) != 1 {
			t.Fatalf("scopes = %d, want 1 key scope", len(state.Scopes))
		}
		if len(state.Candidates) != 1 || state.Candidates[0].Route.Provider != "anthropic" {
			t.Fatalf("candidates = %#v, want anthropic only", state.Candidates)
		}
		return &gateway.Execution{}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatalf("Wrap() error = %v, want nil", err)
	}
}

func TestAuthAcceptsJWTAndResolvesKeyPrincipal(t *testing.T) {
	token := signedJWT(t, "test-secret-that-is-at-least-32-bytes", jwt.MapClaims{
		"iss":         "llmgw-tests",
		"aud":         "gateway",
		"sub":         "principal-1",
		"key_id":      "key-1",
		"permissions": []string{gateway.PermissionManageLimits},
	})
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{
			Auth: gateway.AuthConfig{
				JWT: gateway.JWTConfig{
					Algorithm: "HS256",
					Issuer:    "llmgw-tests",
					Audience:  "gateway",
					Secret:    "test-secret-that-is-at-least-32-bytes",
				},
			},
		},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-4o-mini",
			Meta:      gateway.Meta{Headers: http.Header{"Authorization": []string{"Bearer " + token}}},
		},
	}

	_, err := policy.Auth{}.Wrap(func(_ context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if state.Principal == nil {
			t.Fatal("principal = nil, want resolved principal")
		}
		if state.Subject.KeyID != "key-1" {
			t.Fatalf("subject key = %q, want key-1", state.Subject.KeyID)
		}
		if !state.Principal.HasPermission(gateway.PermissionManageLimits) {
			t.Fatal("principal missing manage_limits permission from JWT claim")
		}
		if state.Request.Meta.User != "" {
			t.Fatalf("meta user = %q, want empty", state.Request.Meta.User)
		}
		return &gateway.Execution{}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatalf("Wrap() error = %v, want nil", err)
	}
}

func TestAuthRequiresExplicitAnonymousMode(t *testing.T) {
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{},
		Request:  &gateway.Request{Meta: gateway.Meta{Headers: make(http.Header)}},
	}
	_, err := policy.Auth{}.Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("next handler should not be called")
		return nil, nil
	})(context.Background(), state)
	if err == nil || gateway.AsAPIError(err).Status != http.StatusUnauthorized {
		t.Fatalf("Auth.Wrap() error = %v, want unauthorized", err)
	}

	state.Snapshot.Auth.AllowAnonymous = true
	if _, err := (policy.Auth{}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		return &gateway.Execution{}, nil
	})(context.Background(), state); err != nil {
		t.Fatalf("Auth.Wrap() explicit anonymous error = %v", err)
	}
}

func TestACLRequiresProjectForRestrictedPrincipal(t *testing.T) {
	state := &gateway.RequestState{
		Snapshot:  &gateway.Snapshot{},
		Principal: &gateway.Principal{Projects: []string{"allowed-project"}},
		Request:   &gateway.Request{Model: "model", Meta: gateway.Meta{}},
	}
	_, err := policy.ACL{}.Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("next handler should not be called")
		return nil, nil
	})(context.Background(), state)
	if err == nil || gateway.AsAPIError(err).Status != http.StatusForbidden {
		t.Fatalf("ACL.Wrap() error = %v, want forbidden for omitted project", err)
	}
}

func TestAuthAcceptsStaticTokenAndResolvesKeyPrincipal(t *testing.T) {
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{
			Auth: gateway.AuthConfig{
				Tokens: map[string]gateway.Principal{
					"static-token": {
						ID:    "principal-1",
						KeyID: "key-1",
					},
				},
			},
		},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-4o-mini",
			Meta: gateway.Meta{
				Headers: http.Header{"Authorization": []string{"Bearer static-token"}},
			},
		},
	}

	_, err := policy.Auth{}.Wrap(func(_ context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if state.Principal == nil || state.Principal.ID != "principal-1" {
			t.Fatalf("principal = %#v, want static principal", state.Principal)
		}
		if state.Subject.KeyID != "key-1" {
			t.Fatalf("subject key = %q, want key-1", state.Subject.KeyID)
		}
		return &gateway.Execution{}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatalf("Wrap() error = %v, want nil", err)
	}
}

func TestAuthLeavesProviderNativeCredentialParsingToIngress(t *testing.T) {
	for _, tt := range []struct {
		provider string
		header   string
	}{
		{provider: "anthropic", header: "x-api-key"},
		{provider: "gemini", header: "x-goog-api-key"},
	} {
		t.Run(tt.provider, func(t *testing.T) {
			state := &gateway.RequestState{
				Snapshot: &gateway.Snapshot{Auth: gateway.AuthConfig{Tokens: map[string]gateway.Principal{
					"gateway-token": {ID: "native-client"},
				}}},
				Request: &gateway.Request{Provider: tt.provider, Meta: gateway.Meta{Headers: http.Header{tt.header: []string{"gateway-token"}}}},
			}
			_, err := (policy.Auth{}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
				return &gateway.Execution{}, nil
			})(context.Background(), state)
			if err == nil || gateway.AsAPIError(err).Status != http.StatusUnauthorized {
				t.Fatalf("direct policy auth error = %v, want ingress-owned credential rejection", err)
			}
		})
	}
}

func TestAuthRejectsInvalidJWTSignature(t *testing.T) {
	token := signedJWT(t, "wrong-secret", jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "principal-1",
		"key_id": "key-1",
	})
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{
			Auth: gateway.AuthConfig{
				JWT: gateway.JWTConfig{
					Algorithm: "HS256",
					Issuer:    "llmgw-tests",
					Audience:  "gateway",
					Secret:    "test-secret-that-is-at-least-32-bytes",
				},
			},
		},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-4o-mini",
			Meta:      gateway.Meta{Headers: http.Header{"Authorization": []string{"Bearer " + token}}},
		},
	}

	_, err := policy.Auth{}.Wrap(func(_ context.Context, _ *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("next handler should not be called")
		return nil, nil
	})(context.Background(), state)
	if err == nil {
		t.Fatal("Wrap() error = nil, want invalid jwt")
	}
	apiErr := gateway.AsAPIError(err)
	if apiErr.Status != http.StatusUnauthorized {
		t.Fatalf("api error = %#v, want 401 unauthorized", apiErr)
	}
}

func TestAuthRejectsMalformedJWTAllowlists(t *testing.T) {
	for _, test := range []struct {
		name   string
		claims jwt.MapClaims
	}{
		{name: "scalar model", claims: jwt.MapClaims{"models": 42}},
		{name: "mixed providers", claims: jwt.MapClaims{"providers": []any{"openai", 42}}},
		{name: "empty projects", claims: jwt.MapClaims{"projects": []any{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.claims["sub"] = "principal-1"
			token := signedJWT(t, "test-secret-that-is-at-least-32-bytes", test.claims)
			_, err := policy.AuthenticatePrincipal(&gateway.Snapshot{Auth: gateway.AuthConfig{JWT: gateway.JWTConfig{
				Algorithm: "HS256", Secret: "test-secret-that-is-at-least-32-bytes",
			}}}, token)
			if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Status != http.StatusUnauthorized {
				t.Fatalf("AuthenticatePrincipal() error = %#v, want malformed claim rejection", apiErr)
			}
		})
	}
}

func TestAuthenticatePrincipalAcceptsCanonicalEdDSAJWT(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"sub": "ed25519-principal",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := policy.AuthenticatePrincipal(&gateway.Snapshot{Auth: gateway.AuthConfig{JWT: gateway.JWTConfig{
		Algorithm: "EdDSA", PublicKey: string(publicPEM),
	}}}, signed)
	if err != nil {
		t.Fatal(err)
	}
	if principal.ID != "ed25519-principal" {
		t.Fatalf("principal = %#v, want EdDSA subject", principal)
	}
}

func TestAuthRejectsJWTWithoutStableIdentity(t *testing.T) {
	token := signedJWT(t, "test-secret-that-is-at-least-32-bytes", jwt.MapClaims{
		"iss": "llmgw-tests",
		"aud": "gateway",
	})
	_, err := policy.AuthenticatePrincipal(&gateway.Snapshot{Auth: gateway.AuthConfig{JWT: gateway.JWTConfig{
		Algorithm: "HS256", Issuer: "llmgw-tests", Audience: "gateway", Secret: "test-secret-that-is-at-least-32-bytes",
	}}}, token)
	if err == nil || gateway.AsAPIError(err).Status != http.StatusUnauthorized {
		t.Fatalf("AuthenticatePrincipal() error = %v, want unauthorized missing identity", err)
	}
}

func TestAuthRejectsJWTWithoutExpiration(t *testing.T) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "principal-1",
		"key_id": "key-1",
		"iat":    time.Now().Unix(),
	})
	rawToken, err := token.SignedString([]byte("test-secret-that-is-at-least-32-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{Auth: gateway.AuthConfig{JWT: gateway.JWTConfig{
			Algorithm: "HS256",
			Issuer:    "llmgw-tests",
			Audience:  "gateway",
			Secret:    "test-secret-that-is-at-least-32-bytes",
		}}},
		Request: &gateway.Request{Meta: gateway.Meta{Headers: http.Header{
			"Authorization": []string{"Bearer " + rawToken},
		}}},
	}
	_, err = policy.Auth{}.Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("next handler should not be called")
		return nil, nil
	})(context.Background(), state)
	if err == nil || gateway.AsAPIError(err).Status != http.StatusUnauthorized {
		t.Fatalf("Auth.Wrap() error = %v, want unauthorized for missing exp", err)
	}
}

func TestResolveScopesUsesJWTKeyClaimForTokenQuota(t *testing.T) {
	token := signedJWT(t, "test-secret-that-is-at-least-32-bytes", jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "principal-1",
		"key_id": "key-1",
	})
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{
			Auth: gateway.AuthConfig{
				JWT: gateway.JWTConfig{
					Algorithm: "HS256",
					Issuer:    "llmgw-tests",
					Audience:  "gateway",
					Secret:    "test-secret-that-is-at-least-32-bytes",
				},
			},
			Quota: gateway.QuotaConfig{
				Profiles: map[string]gateway.LimitSpec{
					"key-default": {
						ProviderAllowlist: []string{"openai"},
					},
				},
				Keys: map[string]string{
					"key-1": "key-default",
				},
			},
		},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-4o-mini",
			Hints:     gateway.RequestHints{PromptText: "hello"},
			Meta:      gateway.Meta{Headers: http.Header{"Authorization": []string{"Bearer " + token}}},
		},
	}
	state.ReplaceCandidates([]gateway.ResolvedRoute{{
		Route: &gateway.Route{Name: "openai-primary", Provider: "openai"},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-4o-mini",
			Hints:     gateway.RequestHints{MaxOutputTokens: 8},
		},
		Estimate: gateway.Usage{InputTokens: 10, OutputTokens: 8, TotalTokens: 18},
	}})

	handler := policy.Auth{}.Wrap(policy.ResolveScopes{}.Wrap(func(_ context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if state.Subject.KeyID != "key-1" {
			t.Fatalf("subject key = %q, want key-1", state.Subject.KeyID)
		}
		if len(state.Scopes) != 1 {
			t.Fatalf("scopes = %d, want 1 key scope", len(state.Scopes))
		}
		if state.Scopes[0].Ref.Kind != gateway.ScopeKey || state.Scopes[0].Ref.ID != "key-1" {
			t.Fatalf("scope = %#v, want key scope for key-1", state.Scopes[0].Ref)
		}
		return &gateway.Execution{}, nil
	}))
	if _, err := handler(context.Background(), state); err != nil {
		t.Fatalf("handler error = %v, want nil", err)
	}
}

func TestResolveScopesPrefersDynamicLimitStore(t *testing.T) {
	limitStore := store.NewMemoryQuotaLimitStore()
	if err := limitStore.Put(context.Background(), "key-1", gateway.LimitSpec{ProviderAllowlist: []string{"anthropic"}}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{
			Quota: gateway.QuotaConfig{
				Profiles: map[string]gateway.LimitSpec{
					"config-limit": {ProviderAllowlist: []string{"openai"}},
				},
				Keys: map[string]string{
					"key-1": "config-limit",
				},
			},
		},
		Subject: gateway.Subject{KeyID: "key-1"},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-4o-mini",
			Hints:     gateway.RequestHints{PromptText: "hello"},
		},
	}
	state.ReplaceCandidates([]gateway.ResolvedRoute{
		{
			Route: &gateway.Route{Name: "openai-primary", Provider: "openai"},
			Request: &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "gpt-4o-mini",
				Hints:     gateway.RequestHints{MaxOutputTokens: 8},
			},
		},
		{
			Route: &gateway.Route{Name: "anthropic-primary", Provider: "anthropic"},
			Request: &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "claude",
				Hints:     gateway.RequestHints{MaxOutputTokens: 8},
			},
		},
	})

	_, err := (policy.ResolveScopes{Limits: limitStore}).Wrap(func(_ context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if len(state.Scopes) != 1 {
			t.Fatalf("scopes = %d, want 1", len(state.Scopes))
		}
		if got := state.Scopes[0].Limits.ProviderAllowlist; len(got) != 1 || got[0] != "anthropic" {
			t.Fatalf("provider allowlist = %#v, want dynamic anthropic override", got)
		}
		if len(state.Candidates) != 1 || state.Candidates[0].Route.Provider != "anthropic" {
			t.Fatalf("candidates = %#v, want anthropic only", state.Candidates)
		}
		return &gateway.Execution{}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatalf("Wrap() error = %v, want nil", err)
	}
}

func TestResolveScopesClassifiesDynamicLimitStoreFailure(t *testing.T) {
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{},
		Subject:  gateway.Subject{KeyID: "key-1"},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "gpt-4o-mini",
		},
	}

	_, err := (policy.ResolveScopes{Limits: &stubQuotaLimitLookup{err: errors.New("boom")}}).Wrap(func(_ context.Context, _ *gateway.RequestState) (*gateway.Execution, error) {
		t.Fatal("next handler should not be called on lookup failure")
		return nil, nil
	})(context.Background(), state)
	if err == nil {
		t.Fatal("Wrap() error = nil, want quota_limit_store_unavailable")
	}
	apiErr := gateway.AsAPIError(err)
	if apiErr.Status != http.StatusServiceUnavailable || apiErr.Code != "quota_limit_store_unavailable" {
		t.Fatalf("api error = %#v, want 503 quota_limit_store_unavailable", apiErr)
	}
	if strings.Contains(apiErr.Message, "boom") {
		t.Fatalf("api message exposed backend failure: %q", apiErr.Message)
	}
}

func signedJWT(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	if _, ok := claims["exp"]; !ok {
		claims["exp"] = time.Now().Add(time.Hour).Unix()
	}
	if _, ok := claims["iat"]; !ok {
		claims["iat"] = time.Now().Unix()
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}
	return signed
}

type stubRateStore struct {
	calls    []rateCall
	errByKey map[string]error
}

type legacyRateStore struct{ calls []rateCall }

func (s *legacyRateStore) Allow(_ context.Context, key string, limit store.RateLimit, n int64) error {
	s.calls = append(s.calls, rateCall{key: key, limit: limit, n: n})
	return nil
}

type rateCall struct {
	key   string
	limit store.RateLimit
	n     int64
}

func (s *stubRateStore) Allow(_ context.Context, key string, limit store.RateLimit, n int64) error {
	s.calls = append(s.calls, rateCall{key: key, limit: limit, n: n})
	if s.errByKey != nil {
		if err, ok := s.errByKey[key]; ok {
			return err
		}
	}
	return nil
}

func (s *stubRateStore) AllowBatch(ctx context.Context, requests []store.RateRequest) error {
	for _, request := range requests {
		if err := s.Allow(ctx, request.Key, request.Limit, request.N); err != nil {
			return err
		}
	}
	return nil
}

type stubQuotaLimitLookup struct {
	err error
}

func (s *stubQuotaLimitLookup) Get(context.Context, string) (gateway.LimitSpec, bool, error) {
	return gateway.LimitSpec{}, false, s.err
}

func (s *stubQuotaLimitLookup) Put(context.Context, string, gateway.LimitSpec) error {
	return s.err
}
