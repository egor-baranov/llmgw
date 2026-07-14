package gateway_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llmgw/gateway"
	"llmgw/policy"
	"llmgw/proxy"
)

type fallbackProvider struct {
	streamFailure bool
	calls         []string
}

func TestEngineFallsBackWhenPrimaryUpstreamCredentialsFail(t *testing.T) {
	var primaryCalls, secondaryCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/primary/"):
			primaryCalls++
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"message":"invalid upstream key","type":"authentication_error"}}`)
		case strings.HasPrefix(r.URL.Path, "/secondary/"):
			secondaryCalls++
			_, _ = io.WriteString(w, `{"id":"chat_1","choices":[]}`)
		default:
			t.Fatalf("unexpected upstream path %q", r.URL.Path)
		}
	}))
	defer upstream.Close()
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"primary": {
			Name: "primary", Provider: "openai", Model: "alias", BaseURL: upstream.URL + "/primary/v1", Priority: 100, Weight: 1,
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
		"secondary": {
			Name: "secondary", Provider: "openai", Model: "alias", BaseURL: upstream.URL + "/secondary/v1", Priority: 10, Weight: 1,
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
	}}
	engine := gateway.NewEngine(gateway.NewConfigStore(snapshot), []gateway.Provider{proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())}, nil, nil)
	exec, err := engine.Execute(context.Background(), &gateway.Request{
		Provider: "openai", Operation: gateway.OpChatCompletions, Model: "alias",
		RawBody: []byte(`{"model":"alias","messages":[{"role":"user","content":"ping"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if exec.Attempt.Route.Route.Name != "secondary" || primaryCalls != 1 || secondaryCalls != 1 {
		t.Fatalf("route/calls = %q/%d/%d, want secondary/1/1", exec.Attempt.Route.Route.Name, primaryCalls, secondaryCalls)
	}
}

type emptyPrimaryProvider struct {
	stream bool
	calls  map[string]int
}

func (p *emptyPrimaryProvider) Name() string                    { return "openai" }
func (p *emptyPrimaryProvider) Supports(gateway.Operation) bool { return true }
func (p *emptyPrimaryProvider) Invoke(_ context.Context, route gateway.ResolvedRoute, _ *gateway.Request) (*gateway.Result, error) {
	p.calls[route.Route.Name]++
	if route.Route.Name == "primary" {
		if p.stream {
			return &gateway.Result{RawStream: io.NopCloser(strings.NewReader(""))}, nil
		}
		return &gateway.Result{StatusCode: http.StatusNoContent}, nil
	}
	if p.stream {
		return &gateway.Result{RawStream: io.NopCloser(strings.NewReader("data: ok\n\n"))}, nil
	}
	return &gateway.Result{RawBody: []byte(`{"ok":true}`)}, nil
}

func TestEmptyProviderResultsTripCircuitBeforeFallback(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "unary", true: "stream"}[stream], func(t *testing.T) {
			provider := &emptyPrimaryProvider{stream: stream, calls: map[string]int{}}
			snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
				"primary": {
					Name: "primary", Provider: "openai", Model: "alias", Priority: 100, Weight: 1,
					Timeout:      gateway.Duration{Duration: time.Second},
					Circuit:      gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Minute}},
					Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}, Streaming: stream},
				},
				"secondary": {
					Name: "secondary", Provider: "openai", Model: "alias", Priority: 10, Weight: 1,
					Timeout:      gateway.Duration{Duration: time.Second},
					Circuit:      gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Minute}},
					Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}, Streaming: stream},
				},
			}}
			limits := &policy.AttemptLimits{Breakers: policy.NewBreaker()}
			engine := gateway.NewEngine(gateway.NewConfigStore(snapshot), []gateway.Provider{provider}, nil, []gateway.AttemptInterceptor{limits})
			for i := 0; i < 2; i++ {
				exec, err := engine.Execute(context.Background(), &gateway.Request{
					Provider: "openai", Operation: gateway.OpChatCompletions, Model: "alias", Stream: stream,
				})
				if err != nil {
					t.Fatal(err)
				}
				if exec.Attempt.Route.Route.Name != "secondary" {
					t.Fatalf("selected route = %q, want secondary", exec.Attempt.Route.Route.Name)
				}
				if exec.Result.RawStream != nil {
					_, _ = io.Copy(io.Discard, exec.Result.RawStream)
					_ = exec.Result.RawStream.Close()
				}
			}
			if provider.calls["primary"] != 1 {
				t.Fatalf("primary calls = %d, want 1 after circuit opens", provider.calls["primary"])
			}
		})
	}
}

func (p *fallbackProvider) Name() string { return "openai" }

func (p *fallbackProvider) Supports(gateway.Operation) bool { return true }

func (p *fallbackProvider) Invoke(_ context.Context, route gateway.ResolvedRoute, _ *gateway.Request) (*gateway.Result, error) {
	p.calls = append(p.calls, route.Route.Name)
	if route.Route.Name == "primary" {
		if p.streamFailure {
			return &gateway.Result{RawStream: failingReadCloser{}}, nil
		}
		return nil, gateway.RateLimited("primary saturated")
	}
	return &gateway.Result{RawBody: []byte(`{"ok":true}`)}, nil
}

type failingReadCloser struct{}

func (failingReadCloser) Read([]byte) (int, error) { return 0, errors.New("stream failed") }
func (failingReadCloser) Close() error             { return nil }

type captureExecutionIDs struct {
	ids *[]string
}

func (c captureExecutionIDs) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(_ context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		*c.ids = append(*c.ids, state.Request.Meta.ExecutionID)
		return nil, gateway.Unauthorized("stop before dispatch")
	}
}

func TestEngineGeneratesExecutionIDIndependentOfClientRequestID(t *testing.T) {
	var ids []string
	engine := gateway.NewEngine(
		gateway.NewConfigStore(&gateway.Snapshot{}),
		nil,
		[]gateway.RequestInterceptor{captureExecutionIDs{ids: &ids}},
		nil,
	)
	request := &gateway.Request{Meta: gateway.Meta{RequestID: "client-controlled"}}

	for range 2 {
		if _, err := engine.Execute(context.Background(), request); err == nil {
			t.Fatal("Execute() error = nil, want interceptor stop")
		}
	}
	if len(ids) != 2 || ids[0] == "" || ids[1] == "" || ids[0] == ids[1] {
		t.Fatalf("execution IDs = %#v, want two unique non-empty IDs", ids)
	}
	if request.Meta.ExecutionID != "" {
		t.Fatalf("caller request mutated with execution ID %q", request.Meta.ExecutionID)
	}
}

func TestAPIErrorRedactsUnexpectedAndWrappedInternalErrors(t *testing.T) {
	secret := errors.New("dial redis://admin:secret@example.invalid")
	if got := gateway.AsAPIError(secret); got.Message != "internal server error" || got.Code != "internal_error" {
		t.Fatalf("unexpected error = %#v, want redacted internal error", got)
	}
	if got := gateway.WrapError(503, "server_error", "store_unavailable", secret); got.Message != "service unavailable" {
		t.Fatalf("wrapped error message = %q, want redacted status text", got.Message)
	}
}

func TestEngineFallsBackForRouteRateLimitAndPreByteStreamFailure(t *testing.T) {
	for _, streamFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "rate_limit", true: "stream"}[streamFailure], func(t *testing.T) {
			provider := &fallbackProvider{streamFailure: streamFailure}
			streaming := streamFailure
			snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
				"primary": {
					Name:     "primary",
					Provider: "openai",
					Model:    "alias",
					Priority: 100,
					Weight:   1,
					Capabilities: gateway.Capability{
						Operations: []gateway.Operation{gateway.OpChatCompletions},
						Streaming:  streaming,
					},
				},
				"secondary": {
					Name:     "secondary",
					Provider: "openai",
					Model:    "alias",
					Priority: 10,
					Weight:   1,
					Capabilities: gateway.Capability{
						Operations: []gateway.Operation{gateway.OpChatCompletions},
						Streaming:  streaming,
					},
				},
			}}
			engine := gateway.NewEngine(gateway.NewConfigStore(snapshot), []gateway.Provider{provider}, nil, nil)
			exec, err := engine.Execute(context.Background(), &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "alias",
				Stream:    streaming,
			})
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if exec.Attempt.Route.Route.Name != "secondary" {
				t.Fatalf("selected route = %q, want secondary", exec.Attempt.Route.Route.Name)
			}
			if len(provider.calls) != 2 {
				t.Fatalf("provider calls = %#v, want primary then secondary", provider.calls)
			}
			if exec.Result.RawStream != nil {
				_, _ = io.Copy(io.Discard, exec.Result.RawStream)
				_ = exec.Result.RawStream.Close()
			}
		})
	}
}
