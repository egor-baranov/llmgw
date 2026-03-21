package api_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gwapi "llmgw/api"
	"llmgw/gateway"
	"llmgw/policy"
)

func TestRawStreamingWriteFailureBeforeFirstByteRefundsQuota(t *testing.T) {
	quotaStore := &recordingQuotaStore{}
	engine := gateway.NewEngine(
		gateway.NewConfigStore(streamTestSnapshot(true)),
		[]gateway.Provider{streamTestProvider{
			result: &gateway.Result{
				StatusCode:  http.StatusOK,
				ContentType: "text/event-stream",
				RawStream:   io.NopCloser(strings.NewReader("data: hello\n\n")),
				Usage:       gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2},
			},
		}},
		[]gateway.RequestInterceptor{
			policy.Auth{},
			policy.ResolveScopes{},
			policy.Quota{Store: quotaStore},
		},
		nil,
	)
	srv := gwapi.NewServer(engine, gateway.NewConfigStore(streamTestSnapshot(true)), nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"upstream-model","stream":true,"messages":[{"role":"user","content":"ping"}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer static-token")

	writer := &failingStreamWriter{header: make(http.Header), failAfter: 0}
	srv.Handler().ServeHTTP(writer, req)

	if quotaStore.reserves != 1 {
		t.Fatalf("reserves = %d, want 1", quotaStore.reserves)
	}
	if quotaStore.refunds != 1 {
		t.Fatalf("refunds = %d, want 1", quotaStore.refunds)
	}
	if quotaStore.commits != 0 {
		t.Fatalf("commits = %d, want 0", quotaStore.commits)
	}
}

func TestRawUnaryPassthroughWritesBody(t *testing.T) {
	srv := newStreamingServer(streamTestProvider{
		result: &gateway.Result{
			StatusCode:  http.StatusOK,
			ContentType: "application/json",
			RawBody:     []byte(`{"id":"chat_1","object":"chat.completion","choices":[]}`),
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"upstream-model","messages":[{"role":"user","content":"ping"}]}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"chat.completion"`) {
		t.Fatalf("response missing passthrough body: %s", rr.Body.String())
	}
}

func newStreamingServer(provider gateway.Provider) *gwapi.Server {
	cfg := gateway.NewConfigStore(streamTestSnapshot(false))
	engine := gateway.NewEngine(cfg, []gateway.Provider{provider}, nil, nil)
	return gwapi.NewServer(engine, cfg, nil, nil, nil)
}

func streamTestSnapshot(withAuth bool) *gateway.Snapshot {
	cfg := &gateway.Snapshot{
		Auth: gateway.AuthConfig{
			MaxBodyBytes: 1 << 20,
		},
		Routes: map[string]*gateway.Route{
			"demo": {
				Name:     "demo",
				Provider: "openai",
				Model:    "upstream-model",
				Capabilities: gateway.Capability{
					Operations: []gateway.Operation{
						gateway.OpChatCompletions,
						gateway.OpResponses,
					},
					Streaming: true,
				},
			},
		},
		Quota: gateway.QuotaConfig{
			Profiles: map[string]gateway.LimitSpec{
				"default": {DailyTokens: 1000},
			},
			Keys: map[string]string{
				"key-1": "default",
			},
		},
	}
	if withAuth {
		cfg.Auth.Tokens = map[string]gateway.Principal{
			"static-token": {ID: "principal-1", KeyID: "key-1"},
		}
	}
	return cfg
}

type streamTestProvider struct {
	result *gateway.Result
	err    error
}

func (p streamTestProvider) Name() string { return "openai" }

func (p streamTestProvider) Supports(gateway.Operation) bool { return true }

func (p streamTestProvider) Invoke(context.Context, gateway.ResolvedRoute, *gateway.Request) (*gateway.Result, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.result, nil
}

type recordingQuotaStore struct {
	reserves int
	commits  int
	refunds  int
}

func (s *recordingQuotaStore) Reserve(context.Context, string, []gateway.ScopedLimit, gateway.EstimatedUsage, time.Duration) (gateway.QuotaTicket, error) {
	s.reserves++
	return gateway.QuotaTicket{RequestID: "req-1"}, nil
}

func (s *recordingQuotaStore) TopUp(context.Context, gateway.QuotaTicket, []gateway.ScopedLimit, gateway.EstimatedUsage, time.Duration) error {
	return nil
}

func (s *recordingQuotaStore) Commit(context.Context, gateway.QuotaTicket, gateway.ActualUsage) error {
	s.commits++
	return nil
}

func (s *recordingQuotaStore) Refund(context.Context, gateway.QuotaTicket) error {
	s.refunds++
	return nil
}

type failingStreamWriter struct {
	header    http.Header
	status    int
	writes    int
	failAfter int
}

func (w *failingStreamWriter) Header() http.Header { return w.header }

func (w *failingStreamWriter) WriteHeader(status int) { w.status = status }

func (w *failingStreamWriter) Write(p []byte) (int, error) {
	if w.writes >= w.failAfter {
		return 0, errors.New("write failed")
	}
	w.writes++
	return len(p), nil
}

func (w *failingStreamWriter) Flush() {}
