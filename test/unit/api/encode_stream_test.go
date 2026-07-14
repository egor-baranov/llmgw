package api_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	gwapi "llmgw/api"
	"llmgw/gateway"
	"llmgw/policy"
)

func TestRawStreamingWriteFailureAfterUpstreamPrefetchCommitsQuota(t *testing.T) {
	quotaStore := &recordingQuotaStore{}
	engine := gateway.NewEngine(
		gateway.NewConfigStore(streamTestSnapshot(true)),
		[]gateway.Provider{streamTestProvider{
			result: &gateway.Result{
				StatusCode:  http.StatusOK,
				ContentType: "text/event-stream",
				RawStream:   io.NopCloser(strings.NewReader("data: hello\n\n")),
			},
		}},
		[]gateway.RequestInterceptor{
			policy.Auth{},
			policy.TokenValidation{},
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
	if quotaStore.refunds != 0 {
		t.Fatalf("refunds = %d, want 0", quotaStore.refunds)
	}
	if quotaStore.commits != 1 {
		t.Fatalf("commits = %d, want 1", quotaStore.commits)
	}
	if quotaStore.committed.TotalTokens() == 0 {
		t.Fatalf("committed usage = %#v, want reserved fallback usage", quotaStore.committed)
	}
}

func TestRawStreamingWithoutProviderUsageCommitsReservedEstimate(t *testing.T) {
	quotaStore := &recordingQuotaStore{}
	cfg := gateway.NewConfigStore(streamTestSnapshot(true))
	engine := gateway.NewEngine(cfg, []gateway.Provider{streamTestProvider{result: &gateway.Result{
		StatusCode: http.StatusOK, ContentType: "text/event-stream",
		RawStream: io.NopCloser(strings.NewReader("data: hello\n\n")),
	}}}, []gateway.RequestInterceptor{
		policy.Auth{}, policy.TokenValidation{}, policy.ResolveScopes{}, policy.Quota{Store: quotaStore},
	}, nil)
	srv := gwapi.NewServer(engine, cfg, nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"upstream-model","stream":true,"messages":[{"role":"user","content":"ping"}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer static-token")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || quotaStore.commits != 1 || quotaStore.committed.TotalTokens() == 0 {
		t.Fatalf("status/commits/usage = %d/%d/%#v", rr.Code, quotaStore.commits, quotaStore.committed)
	}
}

func TestSuccessfulStreamFiltersUnsafeUpstreamHeaders(t *testing.T) {
	headers := echoedSensitiveResponseHeaders()
	headers["Content-Length"] = []string{"999"}
	headers["Connection"] = []string{"keep-alive, X-Internal-Hop"}
	headers["Proxy-Connection"] = []string{"keep-alive"}
	headers["X-LLMGW-Route"] = []string{"spoofed-route"}
	headers["X-Internal-Hop"] = []string{"must-not-escape"}
	headers["X-Request-ID"] = []string{"upstream-request"}
	headers["X-RateLimit-Remaining"] = []string{"7"}
	headers["Content-Language"] = []string{"en"}
	headers["X-Upstream-Debug-Header"] = []string{"kept"}
	srv := newStreamingServer(streamTestProvider{result: &gateway.Result{
		StatusCode:  http.StatusOK,
		ContentType: "text/event-stream",
		Headers:     headers,
		RawStream:   io.NopCloser(strings.NewReader("data: [DONE]\n\n")),
	}})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"upstream-model","stream":true,"messages":[{"role":"user","content":"ping"}]}`))
	rr := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	for _, key := range []string{"Content-Length", "Connection", "Proxy-Connection", "X-Internal-Hop", "X-LLMGW-Route"} {
		if value := rr.Header().Get(key); value != "" {
			t.Fatalf("filtered response header %s = %q, want empty", key, value)
		}
	}
	assertSensitiveResponseHeadersFiltered(t, rr.Header())
	if got := rr.Header().Get("X-LLMGW-Upstream-Request-ID"); got != "upstream-request" {
		t.Fatalf("upstream request id = %q, want upstream-request", got)
	}
	if got := rr.Header().Get("X-RateLimit-Remaining"); got != "7" {
		t.Fatalf("rate-limit header = %q, want 7", got)
	}
	if got := rr.Header().Get("X-Upstream-Debug-Header"); got != "kept" {
		t.Fatalf("ordinary upstream header = %q, want kept", got)
	}
	if got := rr.Header().Get("Content-Language"); got != "en" {
		t.Fatalf("safe content metadata = %q, want en", got)
	}
}

func TestProviderErrorPreservesAnthropicRequestID(t *testing.T) {
	providerErr := gateway.NewError(http.StatusTooManyRequests, "upstream_error", "rate_limit_error", "busy")
	gateway.WithResponseHeaders(providerErr, http.Header{"Request-ID": []string{"anthropic-request"}})
	srv := newStreamingServer(streamTestProvider{err: providerErr})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"upstream-model","messages":[{"role":"user","content":"ping"}]}`))
	rr := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rr.Code)
	}
	if got := rr.Header().Get("Request-ID"); got != "anthropic-request" {
		t.Fatalf("native upstream request id = %q, want anthropic-request", got)
	}
	if got := rr.Header().Get("X-LLMGW-Upstream-Request-ID"); got != "anthropic-request" {
		t.Fatalf("gateway upstream request id = %q, want anthropic-request", got)
	}
}

func TestStreamingUnsupportedPrecheckDoesNotCommitProviderResponse(t *testing.T) {
	stream := &closeTrackingReadCloser{Reader: strings.NewReader("data: [DONE]\n\n")}
	srv := newStreamingServer(streamTestProvider{result: &gateway.Result{
		StatusCode:  http.StatusAccepted,
		ContentType: "text/event-stream",
		Headers:     http.Header{"X-Upstream": []string{"should-not-be-copied"}},
		RawStream:   stream,
	}})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"upstream-model","stream":true,"messages":[{"role":"user","content":"ping"}]}`))
	writer := &nonFlushingWriter{header: make(http.Header)}

	srv.Handler().ServeHTTP(writer, req)

	if writer.status != 0 || writer.writes != 0 {
		t.Fatalf("provider response was committed before Flusher check: status/writes = %d/%d", writer.status, writer.writes)
	}
	if got := writer.header.Get("X-Upstream"); got != "" {
		t.Fatalf("provider header copied before Flusher check: %q", got)
	}
	if !stream.closed {
		t.Fatal("prefetched provider stream was not closed after streaming was rejected")
	}
}

func TestRawUnaryPassthroughWritesBody(t *testing.T) {
	headers := echoedSensitiveResponseHeaders()
	headers["X-Request-ID"] = []string{"upstream-request"}
	headers["X-RateLimit-Remaining"] = []string{"9"}
	headers["Content-Language"] = []string{"en"}
	headers["X-Upstream-Debug-Header"] = []string{"kept"}
	srv := newStreamingServer(streamTestProvider{
		result: &gateway.Result{
			StatusCode:  http.StatusOK,
			ContentType: "application/json",
			Headers:     headers,
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
	assertSensitiveResponseHeadersFiltered(t, rr.Header())
	if got := rr.Header().Get("X-LLMGW-Upstream-Request-ID"); got != "upstream-request" {
		t.Fatalf("upstream request id = %q, want upstream-request", got)
	}
	if got := rr.Header().Get("X-RateLimit-Remaining"); got != "9" {
		t.Fatalf("rate-limit header = %q, want 9", got)
	}
	if got := rr.Header().Get("Content-Language"); got != "en" {
		t.Fatalf("safe content metadata = %q, want en", got)
	}
	if got := rr.Header().Get("X-Upstream-Debug-Header"); got != "kept" {
		t.Fatalf("ordinary upstream header = %q, want kept", got)
	}
}

func echoedSensitiveResponseHeaders() http.Header {
	return http.Header{
		"Authorization":             []string{"Bearer upstream-secret"},
		"Proxy-Authorization":       []string{"Basic upstream-secret"},
		"Cookie":                    []string{"session=upstream-secret"},
		"Set-Cookie":                []string{"session=upstream-secret"},
		"Api-Key":                   []string{"upstream-secret"},
		"X-Api-Key":                 []string{"upstream-secret"},
		"X-Goog-Api-Key":            []string{"upstream-secret"},
		"Ocp-Apim-Subscription-Key": []string{"upstream-secret"},
		"X-Auth-Token":              []string{"upstream-secret"},
		"X-Amz-Credential":          []string{"upstream-secret"},
		"X-Amz-Signature":           []string{"upstream-secret"},
		"X-Forwarded-For":           []string{"10.0.0.1"},
		"OpenAI-Organization":       []string{"org-secret"},
		"Traceparent":               []string{"00-secret"},
	}
}

func assertSensitiveResponseHeadersFiltered(t *testing.T, headers http.Header) {
	t.Helper()
	for key := range echoedSensitiveResponseHeaders() {
		if values := headers.Values(key); len(values) != 0 {
			t.Errorf("sensitive response header %s escaped: %q", key, values)
		}
	}
}

func TestProviderRequestPinsConfigurationSnapshotAcrossReload(t *testing.T) {
	oldSnapshot := streamTestSnapshot(true)
	newSnapshot := streamTestSnapshot(true)
	newSnapshot.Auth.Tokens = map[string]gateway.Principal{
		"replacement-token": {ID: "replacement", KeyID: "replacement-key"},
	}
	cfg := gateway.NewConfigStore(oldSnapshot)
	engine := gateway.NewEngine(cfg, []gateway.Provider{streamTestProvider{result: &gateway.Result{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		RawBody:     []byte(`{"id":"chat_1","choices":[]}`),
	}}}, []gateway.RequestInterceptor{policy.Auth{}}, nil)
	srv := gwapi.NewServer(engine, cfg, nil, nil, nil)
	body := newBlockingRequestBody(`{"model":"upstream-model","messages":[{"role":"user","content":"ping"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer static-token")
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.Handler().ServeHTTP(rr, req)
	}()
	<-body.started
	cfg.Swap(newSnapshot)
	close(body.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request did not finish after decoder was released")
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want old pinned snapshot to authorize request; body=%s", rr.Code, rr.Body.String())
	}
}

func TestRawStreamingAppliesDownstreamWriteIdleDeadline(t *testing.T) {
	cfgSnapshot := streamTestSnapshot(false)
	cfgSnapshot.Server.StreamWriteTimeout = gateway.Duration{Duration: 10 * time.Millisecond}
	cfg := gateway.NewConfigStore(cfgSnapshot)
	engine := gateway.NewEngine(cfg, []gateway.Provider{streamTestProvider{result: &gateway.Result{
		StatusCode: http.StatusOK, ContentType: "text/event-stream",
		RawStream: io.NopCloser(strings.NewReader("data: blocked\n\n")),
	}}}, nil, nil)
	srv := gwapi.NewServer(engine, cfg, nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"upstream-model","stream":true,"messages":[{"role":"user","content":"ping"}]}`))
	writer := &deadlineStreamWriter{header: make(http.Header)}
	started := time.Now()
	srv.Handler().ServeHTTP(writer, req)
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("blocked downstream write took %v, want bounded idle timeout", elapsed)
	}
	if !writer.deadlineSet || !writer.deadlineCleared {
		t.Fatalf("write deadline set/cleared = %t/%t, want true/true", writer.deadlineSet, writer.deadlineCleared)
	}
}

func TestRawUnaryAppliesDownstreamWriteIdleDeadline(t *testing.T) {
	cfgSnapshot := streamTestSnapshot(false)
	cfgSnapshot.Server.StreamWriteTimeout = gateway.Duration{Duration: 10 * time.Millisecond}
	cfg := gateway.NewConfigStore(cfgSnapshot)
	engine := gateway.NewEngine(cfg, []gateway.Provider{streamTestProvider{result: &gateway.Result{
		StatusCode: http.StatusOK, ContentType: "application/json", RawBody: []byte(`{"ok":true}`),
	}}}, nil, nil)
	srv := gwapi.NewServer(engine, cfg, nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"upstream-model","messages":[{"role":"user","content":"ping"}]}`))
	writer := &deadlineStreamWriter{header: make(http.Header)}
	started := time.Now()
	srv.Handler().ServeHTTP(writer, req)
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("blocked downstream unary write took %v, want bounded idle timeout", elapsed)
	}
	if !writer.deadlineSet || !writer.deadlineCleared {
		t.Fatalf("write deadline set/cleared = %t/%t, want true/true", writer.deadlineSet, writer.deadlineCleared)
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
			MaxBodyBytes:   1 << 20,
			AllowAnonymous: !withAuth,
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
	reserves  int
	commits   int
	refunds   int
	reserved  gateway.EstimatedUsage
	committed gateway.ActualUsage
}

func (s *recordingQuotaStore) Reserve(_ context.Context, _ string, _ []gateway.ScopedLimit, estimate gateway.EstimatedUsage, _ time.Duration) (gateway.QuotaTicket, error) {
	s.reserves++
	s.reserved = estimate
	return gateway.QuotaTicket{RequestID: "req-1"}, nil
}

func (s *recordingQuotaStore) TopUp(context.Context, gateway.QuotaTicket, []gateway.ScopedLimit, gateway.EstimatedUsage, time.Duration) error {
	return nil
}

func (s *recordingQuotaStore) Commit(_ context.Context, _ gateway.QuotaTicket, actual gateway.ActualUsage) error {
	s.commits++
	s.committed = actual
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

type nonFlushingWriter struct {
	header http.Header
	status int
	writes int
}

func (w *nonFlushingWriter) Header() http.Header { return w.header }

func (w *nonFlushingWriter) WriteHeader(status int) { w.status = status }

func (w *nonFlushingWriter) Write(p []byte) (int, error) {
	w.writes++
	return len(p), nil
}

type closeTrackingReadCloser struct {
	io.Reader
	closed bool
}

func (r *closeTrackingReadCloser) Close() error {
	r.closed = true
	return nil
}

type deadlineStreamWriter struct {
	header          http.Header
	deadline        time.Time
	deadlineSet     bool
	deadlineCleared bool
}

type blockingRequestBody struct {
	reader  *strings.Reader
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingRequestBody(value string) *blockingRequestBody {
	return &blockingRequestBody{
		reader:  strings.NewReader(value),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingRequestBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return b.reader.Read(p)
}

func (b *blockingRequestBody) Close() error { return nil }

func (w *deadlineStreamWriter) Header() http.Header { return w.header }
func (w *deadlineStreamWriter) WriteHeader(int)     {}
func (w *deadlineStreamWriter) Flush()              {}
func (w *deadlineStreamWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	if deadline.IsZero() {
		w.deadlineCleared = true
	} else {
		w.deadlineSet = true
	}
	return nil
}
func (w *deadlineStreamWriter) Write([]byte) (int, error) {
	wait := time.Until(w.deadline)
	if wait > 0 {
		timer := time.NewTimer(wait)
		<-timer.C
	}
	return 0, errors.New("write deadline exceeded")
}
