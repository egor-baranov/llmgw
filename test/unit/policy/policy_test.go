package policy_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"llmgw/gateway"
	"llmgw/policy"
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

func TestAttemptLimitsUsesDistributedState(t *testing.T) {
	dist := &stubState{breakerAllowed: true}
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

	_, err := limits.WrapAttempt(func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
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
		if headers.Get("x-goog-api-client") == "" {
			t.Fatal("x-goog-api-client header = empty, want gemini client marker")
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

type stubState struct {
	breakerAllowed      bool
	acquireCalls        []string
	releaseCalls        []string
	breakerFailCalls    int
	breakerSuccessCalls int
}

func (s *stubState) AcquireSlot(_ context.Context, bucket, _ string, _ int64, _ time.Duration) error {
	s.acquireCalls = append(s.acquireCalls, bucket)
	return nil
}

func (s *stubState) ReleaseSlot(_ context.Context, bucket, _ string) error {
	s.releaseCalls = append(s.releaseCalls, bucket)
	return nil
}

func (s *stubState) BreakerAllow(_ context.Context, _ string, _ time.Time) (bool, error) {
	return s.breakerAllowed, nil
}

func (s *stubState) BreakerFail(_ context.Context, _ string, _ int, _ time.Duration, _ string, _ time.Time) error {
	s.breakerFailCalls++
	return nil
}

func (s *stubState) BreakerSuccess(_ context.Context, _ string, _ time.Time) error {
	s.breakerSuccessCalls++
	return nil
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
	token := signedJWT(t, "test-secret", jwt.MapClaims{
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
					Secret:    "test-secret",
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
		if state.Request.Meta.User != "" {
			t.Fatalf("meta user = %q, want empty", state.Request.Meta.User)
		}
		return &gateway.Execution{}, nil
	})(context.Background(), state)
	if err != nil {
		t.Fatalf("Wrap() error = %v, want nil", err)
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
					Secret:    "test-secret",
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

func TestResolveScopesUsesJWTKeyClaimForTokenQuota(t *testing.T) {
	token := signedJWT(t, "test-secret", jwt.MapClaims{
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
					Secret:    "test-secret",
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

func TestResolveScopesPropagatesDynamicLimitLookupFailure(t *testing.T) {
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
		t.Fatal("Wrap() error = nil, want quota_limit_lookup_failed")
	}
	apiErr := gateway.AsAPIError(err)
	if apiErr.Status != http.StatusInternalServerError || apiErr.Code != "quota_limit_lookup_failed" {
		t.Fatalf("api error = %#v, want 500 quota_limit_lookup_failed", apiErr)
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

type stubQuotaLimitLookup struct {
	err error
}

func (s *stubQuotaLimitLookup) Get(context.Context, string) (gateway.LimitSpec, bool, error) {
	return gateway.LimitSpec{}, false, s.err
}

func (s *stubQuotaLimitLookup) Put(context.Context, string, gateway.LimitSpec) error {
	return s.err
}
