package observer_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"llmgw/gateway"
	"llmgw/observer"
	"llmgw/policy"
	"llmgw/store"
)

func TestRequestMetricsBoundsUnknownModelLabels(t *testing.T) {
	obs := observer.New("test")
	obs.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	wrapped := (observer.RequestMetrics{Obs: obs}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		return nil, gateway.Unauthorized("missing bearer token")
	})

	for i := 0; i < 20; i++ {
		state := &gateway.RequestState{
			Snapshot: &gateway.Snapshot{},
			Request: &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     fmt.Sprintf("attacker-model-%d", i),
			},
		}
		if _, err := wrapped(context.Background(), state); err == nil {
			t.Fatal("Wrap() error = nil, want authentication error")
		}
	}

	var out bytes.Buffer
	obs.Metrics.Set.WritePrometheus(&out)
	metrics := out.String()
	if strings.Contains(metrics, "attacker-model-") {
		t.Fatalf("metrics contain unbounded client model label: %s", metrics)
	}
	if !strings.Contains(metrics, `model="unknown"`) {
		t.Fatalf("metrics missing bounded unknown label: %s", metrics)
	}
}

func TestObserverRetainsConfiguredServiceName(t *testing.T) {
	obs := observer.New("gateway-production")
	if obs.ServiceName != "gateway-production" {
		t.Fatalf("service name = %q", obs.ServiceName)
	}
}

func TestRequestMetricsRedactsPrivateErrorCauses(t *testing.T) {
	const secret = "credential-value-must-not-be-logged"
	var logs bytes.Buffer
	obs := observer.New("test")
	obs.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	tracer := &errorRecordingTracer{}
	obs.Tracer = tracer
	wantErr := gateway.NewError(502, "upstream_error", "request_failed", "provider echoed "+secret).
		WithCause(errors.New("dial https://user:" + secret + "@upstream.invalid failed"))
	wrapped := (observer.RequestMetrics{Obs: obs}).Wrap(func(context.Context, *gateway.RequestState) (*gateway.Execution, error) {
		return nil, wantErr
	})
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "untrusted-model",
			Meta:      gateway.Meta{RequestID: "request-1"},
		},
	}

	if _, err := wrapped(context.Background(), state); !errors.Is(err, wantErr) {
		t.Fatalf("Wrap() error = %v, want original API error", err)
	}
	if output := logs.String(); strings.Contains(output, secret) || !strings.Contains(output, `"error":"bad gateway"`) {
		t.Fatalf("log output was not safely redacted: %s", output)
	}
	if tracer.recorded == nil || strings.Contains(tracer.recorded.Error(), secret) || tracer.recorded.Error() != "bad gateway" {
		t.Fatalf("recorded trace error = %v, want redacted bad gateway", tracer.recorded)
	}
	if got := observer.SafeErrorMessage(errors.New("raw " + secret)); got != "internal error" {
		t.Fatalf("SafeErrorMessage() = %q, want internal error", got)
	}
}

func TestMetricsRemainUsableWithoutLogger(t *testing.T) {
	obs := observer.New("test")
	obs.Logger = nil
	tracer := &recordingTracer{}
	obs.Tracer = tracer
	state := &gateway.RequestState{
		Snapshot: &gateway.Snapshot{},
		Request: &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Meta:      gateway.Meta{RequestID: "request-without-logger"},
		},
	}
	attempt := &gateway.Attempt{Route: gateway.ResolvedRoute{Route: &gateway.Route{
		Name: "route", Provider: "openai",
	}}}
	attemptHandler := (observer.AttemptMetrics{Obs: obs}).WrapAttempt(
		func(context.Context, *gateway.RequestState, *gateway.Attempt) (*gateway.Result, error) {
			return &gateway.Result{RawBody: []byte(`{}`)}, nil
		},
	)
	requestHandler := (observer.RequestMetrics{Obs: obs}).Wrap(
		func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
			result, err := attemptHandler(ctx, state, attempt)
			if err != nil {
				return nil, err
			}
			return &gateway.Execution{Result: result}, nil
		},
	)

	exec, err := requestHandler(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Settle(context.Background(), gateway.Usage{}, nil); err != nil {
		t.Fatal(err)
	}
	if tracer.started.Load() != 2 || tracer.ended.Load() != 2 {
		t.Fatalf("tracer counts = started:%d ended:%d, want 2/2", tracer.started.Load(), tracer.ended.Load())
	}
	var out bytes.Buffer
	obs.Metrics.Set.WritePrometheus(&out)
	if metrics := out.String(); !strings.Contains(metrics, "llmgw_requests_total") || !strings.Contains(metrics, "llmgw_attempts_total") {
		t.Fatalf("metrics missing without logger: %s", metrics)
	}
}

func TestMetricsFinishWhenProviderPanics(t *testing.T) {
	obs := observer.New("test")
	obs.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	tracer := &recordingTracer{}
	obs.Tracer = tracer
	snapshot := &gateway.Snapshot{
		Auth:  gateway.AuthConfig{AllowAnonymous: true},
		Store: gateway.StoreConfig{ReservationTTL: gateway.Duration{Duration: time.Minute}},
		Quota: gateway.QuotaConfig{
			Profiles: map[string]gateway.LimitSpec{"test": {DailyTokens: 10_000}},
			Keys:     map[string]string{"anonymous": "test"},
		},
		Routes: map[string]*gateway.Route{
			"panic": {
				Name: "panic", Provider: "openai", Model: "panic-model", Timeout: gateway.Duration{Duration: time.Second},
				Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}, Tokenizer: "cl100k_base", MaxOutputTokens: 100},
			},
		},
	}
	attemptLimits := &policy.AttemptLimits{}
	engine := gateway.NewEngine(
		gateway.NewConfigStore(snapshot), []gateway.Provider{panicProvider{}},
		[]gateway.RequestInterceptor{
			observer.RequestMetrics{Obs: obs}, policy.Auth{}, policy.TokenValidation{}, policy.ResolveScopes{},
			policy.Quota{Store: store.NewMemoryQuotaStore()},
		},
		[]gateway.AttemptInterceptor{attemptLimits, policy.AttemptHeaders{}, observer.AttemptMetrics{Obs: obs}},
	)

	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		_, _ = engine.Execute(context.Background(), &gateway.Request{
			Operation: gateway.OpChatCompletions, Model: "panic-model",
			RawBody: []byte(`{"model":"panic-model","messages":[{"role":"user","content":"ping"}]}`),
			Meta:    gateway.Meta{RequestID: "panic-request"},
		})
	}()
	if !panicked {
		t.Fatal("provider panic was not propagated")
	}

	var out bytes.Buffer
	obs.Metrics.Set.WritePrometheus(&out)
	metrics := out.String()
	for _, want := range []string{
		"llmgw_inflight_requests 0",
		`llmgw_requests_total{operation="chat.completions",model="panic-model",status="error"} 1`,
		`llmgw_attempts_total{provider="openai",route="panic",status="error"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q: %s", want, metrics)
		}
	}
	if tracer.started.Load() != 2 || tracer.ended.Load() != 2 || tracer.errors.Load() != 2 {
		t.Fatalf("tracer counts = started:%d ended:%d errors:%d, want 2/2/2", tracer.started.Load(), tracer.ended.Load(), tracer.errors.Load())
	}
}

type panicProvider struct{}

func (panicProvider) Name() string                    { return "openai" }
func (panicProvider) Supports(gateway.Operation) bool { return true }
func (panicProvider) Invoke(context.Context, gateway.ResolvedRoute, *gateway.Request) (*gateway.Result, error) {
	panic("provider panic")
}

type recordingTracer struct {
	started atomic.Int64
	ended   atomic.Int64
	errors  atomic.Int64
}

type errorRecordingTracer struct{ recorded error }

func (t *errorRecordingTracer) Start(ctx context.Context, _ string, _ ...observer.Attribute) (context.Context, observer.Span) {
	return ctx, errorRecordingSpan{tracer: t}
}

type errorRecordingSpan struct{ tracer *errorRecordingTracer }

func (errorRecordingSpan) End() {}
func (s errorRecordingSpan) RecordError(err error) {
	s.tracer.recorded = err
}

func (t *recordingTracer) Start(ctx context.Context, _ string, _ ...observer.Attribute) (context.Context, observer.Span) {
	t.started.Add(1)
	return ctx, &recordingSpan{tracer: t}
}

type recordingSpan struct{ tracer *recordingTracer }

func (s *recordingSpan) End()              { s.tracer.ended.Add(1) }
func (s *recordingSpan) RecordError(error) { s.tracer.errors.Add(1) }
