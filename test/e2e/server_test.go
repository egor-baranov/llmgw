package e2e_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gwapi "llmgw/api"
	"llmgw/gateway"
	"llmgw/observer"
	"llmgw/policy"
	"llmgw/store"

	"github.com/golang-jwt/jwt/v5"
)

var _ gateway.Provider = (*captureProvider)(nil)

type captureProvider struct {
	name     string
	mu       sync.Mutex
	captured []*gateway.Request
	invoke   func(*gateway.Request) (*gateway.Result, error)
}

func (p *captureProvider) Name() string {
	if p.name != "" {
		return p.name
	}
	return "openai"
}

func (p *captureProvider) Supports(gateway.Operation) bool { return true }

func (p *captureProvider) Invoke(_ context.Context, _ gateway.ResolvedRoute, req *gateway.Request) (*gateway.Result, error) {
	clone := req.Clone()
	p.mu.Lock()
	p.captured = append(p.captured, clone)
	p.mu.Unlock()
	if p.invoke != nil {
		return p.invoke(clone)
	}
	return defaultResult(clone), nil
}

func (p *captureProvider) last() *gateway.Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.captured) == 0 {
		return nil
	}
	return p.captured[len(p.captured)-1]
}

func TestPublicEndpoints(t *testing.T) {
	ts := newTestServer(t, &captureProvider{})
	defer ts.Close()

	tests := []struct {
		method string
		path   string
		body   string
		want   string
	}{
		{method: http.MethodGet, path: "/healthz", want: `"ok":true`},
		{method: http.MethodGet, path: "/readyz", want: `"ready":true`},
		{method: http.MethodGet, path: "/v1/models", want: `"id":"upstream-demo"`},
		{method: http.MethodPost, path: "/v1/chat/completions", body: `{"model":"upstream-demo","messages":[{"role":"user","content":"ping"}]}`, want: `"chat.completion"`},
		{method: http.MethodPost, path: "/v1/responses", body: `{"model":"upstream-demo","input":"ping"}`, want: `"response"`},
		{method: http.MethodPost, path: "/v1/completions", body: `{"model":"upstream-demo","prompt":"ping"}`, want: `"text_completion"`},
		{method: http.MethodPost, path: "/v1/embeddings", body: `{"model":"upstream-demo","input":"ping"}`, want: `"embedding"`},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, ts.URL+tt.path, strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
			}
			if !bytes.Contains(body, []byte(tt.want)) {
				t.Fatalf("response %s does not contain %q: %s", tt.path, tt.want, body)
			}
		})
	}
}

func TestSpecAndDocsEndpoints(t *testing.T) {
	ts := newTestServer(t, &captureProvider{})
	defer ts.Close()

	specResp, err := http.Get(ts.URL + "/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer specResp.Body.Close()
	specBody, err := io.ReadAll(specResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if specResp.StatusCode != http.StatusOK {
		t.Fatalf("spec status = %d, want %d: %s", specResp.StatusCode, http.StatusOK, specBody)
	}
	if got := specResp.Header.Get("Content-Type"); !strings.Contains(got, "yaml") {
		t.Fatalf("spec content-type = %q, want yaml", got)
	}
	for _, want := range []string{
		"openapi: 3.0.3",
		"/v1/chat/completions:",
		"/v1/messages:",
		"/v1beta/models/{model}:generateContent:",
		"/v1beta/models/{model}:embedContent:",
		"/v1/models/{model}:generateContent:",
		"/v1/models/{model}:embedContent:",
		"/v1/limits:",
		"/docs:",
	} {
		if !strings.Contains(string(specBody), want) {
			t.Fatalf("spec missing %q: %s", want, specBody)
		}
	}

	jsonResp, err := http.Get(ts.URL + "/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	defer jsonResp.Body.Close()
	jsonBody, err := io.ReadAll(jsonResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if jsonResp.StatusCode != http.StatusOK {
		t.Fatalf("json spec status = %d, want %d: %s", jsonResp.StatusCode, http.StatusOK, jsonBody)
	}
	if got := jsonResp.Header.Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("json spec content-type = %q, want application/json", got)
	}
	for _, want := range []string{
		`"openapi":"3.0.3"`,
		`"/v1/chat/completions"`,
		`"/v1/messages"`,
		`"/v1beta/models/{model}:generateContent"`,
		`"/v1beta/models/{model}:embedContent"`,
		`"/v1/models/{model}:generateContent"`,
		`"/v1/models/{model}:embedContent"`,
		`"/v1/limits"`,
	} {
		if !strings.Contains(string(jsonBody), want) {
			t.Fatalf("json spec missing %q: %s", want, jsonBody)
		}
	}

	noRedirect := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	redirectResp, err := noRedirect.Get(ts.URL + "/docs")
	if err != nil {
		t.Fatal(err)
	}
	defer redirectResp.Body.Close()
	if redirectResp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("docs redirect status = %d, want %d", redirectResp.StatusCode, http.StatusMovedPermanently)
	}
	if got := redirectResp.Header.Get("Location"); got != "/docs/index.html" {
		t.Fatalf("docs redirect location = %q, want /docs/index.html", got)
	}

	docsResp, err := http.Get(ts.URL + "/docs/index.html")
	if err != nil {
		t.Fatal(err)
	}
	defer docsResp.Body.Close()
	docsBody, err := io.ReadAll(docsResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if docsResp.StatusCode != http.StatusOK {
		t.Fatalf("docs status = %d, want %d: %s", docsResp.StatusCode, http.StatusOK, docsBody)
	}
	if got := docsResp.Header.Get("Content-Type"); !strings.Contains(got, "text/html") {
		t.Fatalf("docs content-type = %q, want text/html", got)
	}
	for _, want := range []string{"SwaggerUIBundle", "/openapi.json", "Swagger UI"} {
		if !strings.Contains(string(docsBody), want) {
			t.Fatalf("docs missing %q: %s", want, docsBody)
		}
	}
}

func TestMetricsEndpoint(t *testing.T) {
	cfg := loadTestConfig(t)
	cfgStore := gateway.NewConfigStore(cfg)
	obsrv := observer.New("llmgw")
	engine := gateway.NewEngine(
		cfgStore,
		[]gateway.Provider{&captureProvider{}},
		[]gateway.RequestInterceptor{observer.RequestMetrics{Obs: obsrv}},
		[]gateway.AttemptInterceptor{observer.AttemptMetrics{Obs: obsrv}},
	)
	srv := gwapi.NewServer(engine, cfgStore, obsrv, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{
		"model":"upstream-demo",
		"messages":[{"role":"user","content":"ping"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("chat status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
	}

	metricsResp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metricsResp.Body.Close()
	metricsBody, err := io.ReadAll(metricsResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d: %s", metricsResp.StatusCode, http.StatusOK, metricsBody)
	}
	if got := metricsResp.Header.Get("Content-Type"); !strings.Contains(got, "text/plain") {
		t.Fatalf("metrics content-type = %q, want text/plain", got)
	}
	for _, want := range []string{
		"# TYPE llmgw_requests_total counter",
		`llmgw_requests_total{operation="chat.completions",model="upstream-demo",status="ok"} 1`,
		"# TYPE llmgw_request_duration_seconds histogram",
		"llmgw_inflight_requests 0",
	} {
		if !strings.Contains(string(metricsBody), want) {
			t.Fatalf("metrics missing %q: %s", want, metricsBody)
		}
	}
}

func TestQuotaLimitsEndpointsUseJWTKeyID(t *testing.T) {
	provider := &captureProvider{}
	limitStore := store.NewMemoryQuotaLimitStore()
	quotaStore := store.NewMemoryQuotaStore()
	ts := newQuotaServer(t, provider, limitStore, quotaStore)
	defer ts.Close()

	token := signedQuotaJWT(t, "test-secret", jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "session-1",
		"key_id": "jwt-key-1",
	})

	putReq, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/limits", strings.NewReader(`{
		"rpm": 25,
		"daily_tokens": 1000,
		"max_parallel": 2
	}`))
	if err != nil {
		t.Fatal(err)
	}
	putReq.Header.Set("Authorization", "Bearer "+token)
	putReq.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	defer putResp.Body.Close()
	putBody, err := io.ReadAll(putResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("put quota status = %d, want %d: %s", putResp.StatusCode, http.StatusOK, putBody)
	}
	for _, want := range []string{`"key_id":"jwt-key-1"`, `"source":"dynamic"`, `"rpm":25`, `"daily_tokens":1000`} {
		if !strings.Contains(string(putBody), want) {
			t.Fatalf("put quota response missing %q: %s", want, putBody)
		}
	}

	chatReq, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(`{
		"model":"upstream-demo",
		"messages":[{"role":"user","content":"ping"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	chatReq.Header.Set("Authorization", "Bearer "+token)
	chatResp, err := http.DefaultClient.Do(chatReq)
	if err != nil {
		t.Fatal(err)
	}
	defer chatResp.Body.Close()
	chatBody, err := io.ReadAll(chatResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if chatResp.StatusCode != http.StatusOK {
		t.Fatalf("chat status = %d, want %d: %s", chatResp.StatusCode, http.StatusOK, chatBody)
	}

	getReq, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/limits", nil)
	if err != nil {
		t.Fatal(err)
	}
	getReq.Header.Set("Authorization", "Bearer "+token)
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	defer getResp.Body.Close()
	getBody, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get quota status = %d, want %d: %s", getResp.StatusCode, http.StatusOK, getBody)
	}
	for _, want := range []string{`"key_id":"jwt-key-1"`, `"source":"dynamic"`, `"daily_used_tokens":2`, `"rpm_current":1`} {
		if !strings.Contains(string(getBody), want) {
			t.Fatalf("get quota response missing %q: %s", want, getBody)
		}
	}
}

func TestQuotaLimitsGetFallsBackToConfigForStaticToken(t *testing.T) {
	provider := &captureProvider{}
	quotaStore := store.NewMemoryQuotaStore()
	ts := newConfigQuotaServer(t, provider, quotaStore)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/limits", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer local-dev-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
	}
	for _, want := range []string{`"key_id":"dev-key"`, `"source":"config"`, `"daily_tokens":1000000`} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("response missing %q: %s", want, body)
		}
	}
}

func TestQuotaLimitsRejectMissingBearer(t *testing.T) {
	ts := newQuotaServer(t, &captureProvider{}, store.NewMemoryQuotaLimitStore(), store.NewMemoryQuotaStore())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/limits")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusUnauthorized, body)
	}
	if !strings.Contains(string(body), `"code":"unauthorized"`) {
		t.Fatalf("body = %s, want unauthorized", body)
	}
}

func TestQuotaLimitsRejectJWTWithoutKeyID(t *testing.T) {
	ts := newQuotaServer(t, &captureProvider{}, store.NewMemoryQuotaLimitStore(), store.NewMemoryQuotaStore())
	defer ts.Close()

	token := signedQuotaJWT(t, "test-secret", jwt.MapClaims{
		"iss": "llmgw-tests",
		"aud": "gateway",
	})

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/limits", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusBadRequest, body)
	}
	if !strings.Contains(string(body), `"code":"missing_key_id"`) {
		t.Fatalf("body = %s, want missing_key_id", body)
	}
}

func TestQuotaLimitsPutRejectsInvalidPayload(t *testing.T) {
	ts := newQuotaServer(t, &captureProvider{}, store.NewMemoryQuotaLimitStore(), store.NewMemoryQuotaStore())
	defer ts.Close()

	token := signedQuotaJWT(t, "test-secret", jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "session-1",
		"key_id": "jwt-key-1",
	})

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/limits", strings.NewReader(`{"rpm":-1}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusBadRequest, body)
	}
	if !strings.Contains(string(body), `"code":"invalid_limit"`) {
		t.Fatalf("body = %s, want invalid_limit", body)
	}
}

func TestQuotaLimitsPutRejectsWhenDynamicStoreDisabled(t *testing.T) {
	cfg := loadQuotaConfig(t)
	cfgStore := gateway.NewConfigStore(cfg)
	engine := gateway.NewEngine(
		cfgStore,
		[]gateway.Provider{&captureProvider{}},
		[]gateway.RequestInterceptor{
			policy.Auth{},
		},
		nil,
	)
	srv := gwapi.NewServer(engine, cfgStore, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	token := signedQuotaJWT(t, "test-secret", jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "session-1",
		"key_id": "jwt-key-1",
	})

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/limits", strings.NewReader(`{"rpm":10}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusServiceUnavailable, body)
	}
	if !strings.Contains(string(body), `"code":"quota_limit_store_unavailable"`) {
		t.Fatalf("body = %s, want quota_limit_store_unavailable", body)
	}
}

func TestRequestDecodingPreservesExtras(t *testing.T) {
	provider := &captureProvider{}
	ts := newTestServer(t, provider)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(`{
		"model":"upstream-demo",
		"stream":true,
		"messages":[{"role":"user","content":"hello","vendor_hint":{"x":1}}],
		"metadata":{"project":"p1"},
		"provider_field":{"mode":"fast"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
	}

	env := provider.last()
	if env == nil {
		t.Fatal("provider did not capture a request")
	}
	if env.Model != "upstream-demo" {
		t.Fatalf("model = %q, want upstream-demo", env.Model)
	}
	if !env.Stream {
		t.Fatal("stream = false, want true")
	}
	if got := env.PromptText(); got != "" {
		t.Fatalf("prompt text = %q, want empty for minimal decoder", got)
	}
	if !bytes.Contains(env.RawBody, []byte(`"content":"hello"`)) {
		t.Fatal("raw request body did not preserve message content")
	}
	if !bytes.Contains(env.RawBody, []byte(`"vendor_hint":{"x":1}`)) {
		t.Fatal("raw request body did not preserve vendor_hint")
	}
	if !bytes.Contains(env.RawBody, []byte(`"provider_field":{"mode":"fast"}`)) {
		t.Fatal("top-level provider_field did not survive in raw body")
	}
}

func TestResponsesStringInputDecodesToMessage(t *testing.T) {
	provider := &captureProvider{}
	ts := newTestServer(t, provider)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/responses", strings.NewReader(`{
		"model":"upstream-demo",
		"input":"hello"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
	}

	env := provider.last()
	if env == nil {
		t.Fatal("provider did not capture a request")
	}
	if got := env.PromptText(); got != "" {
		t.Fatalf("prompt text = %q, want empty for minimal decoder", got)
	}
	if !bytes.Contains(env.RawBody, []byte(`"input":"hello"`)) {
		t.Fatal("raw request body did not preserve string input")
	}
}

func TestStreamingEndpoints(t *testing.T) {
	provider := &captureProvider{
		invoke: func(req *gateway.Request) (*gateway.Result, error) {
			switch req.Operation {
			case gateway.OpChatCompletions:
				return rawStreamResult("data: {\"id\":\"chat_1\",\"object\":\"chat.completion.chunk\",\"model\":\"" + req.Model + "\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\ndata: {\"id\":\"chat_1\",\"object\":\"chat.completion.chunk\",\"model\":\"" + req.Model + "\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"), nil
			case gateway.OpResponses:
				return rawStreamResult("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"" + req.Model + "\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"" + req.Model + "\",\"output_text\":\"hello\"}}\n\ndata: [DONE]\n\n"), nil
			case gateway.OpCompletions:
				return rawStreamResult("data: {\"id\":\"cmpl_1\",\"object\":\"text_completion\",\"model\":\"" + req.Model + "\",\"choices\":[{\"index\":0,\"text\":\"hello\"}]}\n\ndata: {\"id\":\"cmpl_1\",\"object\":\"text_completion\",\"model\":\"" + req.Model + "\",\"choices\":[{\"index\":0,\"text\":\"\",\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"), nil
			default:
				return defaultResult(req), nil
			}
		},
	}
	ts := newTestServer(t, provider)
	defer ts.Close()

	tests := []struct {
		name      string
		path      string
		body      string
		wantBody  []string
		wantEvent string
	}{
		{
			name:      "chat",
			path:      "/v1/chat/completions",
			body:      `{"model":"upstream-demo","stream":true,"messages":[{"role":"user","content":"ping"}]}`,
			wantBody:  []string{`"chat.completion.chunk"`, `hello`, `[DONE]`},
			wantEvent: "",
		},
		{
			name:      "responses",
			path:      "/v1/responses",
			body:      `{"model":"upstream-demo","stream":true,"input":"ping"}`,
			wantBody:  []string{`response.output_text.delta`, `response.completed`, `hello`, `[DONE]`},
			wantEvent: "response.created",
		},
		{
			name:      "completions",
			path:      "/v1/completions",
			body:      `{"model":"upstream-demo","stream":true,"prompt":"ping"}`,
			wantBody:  []string{`"text_completion"`, `hello`, `[DONE]`},
			wantEvent: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, ts.URL+tt.path, strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
			}
			if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
				t.Fatalf("content-type = %q, want text/event-stream", got)
			}
			if tt.wantEvent != "" {
				found := false
				if err := readSSEFrames(bytes.NewReader(body), func(frame sseFrame) error {
					if frame.Event == tt.wantEvent {
						found = true
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if !found {
					t.Fatalf("stream missing event %q: %s", tt.wantEvent, body)
				}
			}
			for _, want := range tt.wantBody {
				if !strings.Contains(string(body), want) {
					t.Fatalf("stream body missing %q: %s", want, body)
				}
			}
		})
	}
}

func TestProviderNativeAnthropicEndpoint(t *testing.T) {
	provider := &captureProvider{name: "anthropic"}
	ts := newTestServer(t, provider)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/messages", strings.NewReader(`{
		"model":"upstream-anthropic",
		"messages":[{"role":"user","content":"ping"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
	}
	for _, want := range []string{`"type":"message"`, `"role":"assistant"`, `"pong"`} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("response missing %q: %s", want, body)
		}
	}

	env := provider.last()
	if env == nil {
		t.Fatal("provider did not capture a request")
	}
	if env.Provider != "anthropic" {
		t.Fatalf("provider = %q, want anthropic", env.Provider)
	}
	if env.Operation != gateway.OpChatCompletions {
		t.Fatalf("operation = %q, want %q", env.Operation, gateway.OpChatCompletions)
	}
	if env.Model != "upstream-anthropic" {
		t.Fatalf("model = %q, want upstream-anthropic", env.Model)
	}
	if got := env.PromptText(); got != "" {
		t.Fatalf("prompt text = %q, want empty for minimal decoder", got)
	}
	if !bytes.Contains(env.RawBody, []byte(`"content":"ping"`)) {
		t.Fatal("raw request body did not preserve anthropic message content")
	}
}

func TestProviderNativeGeminiEndpoints(t *testing.T) {
	provider := &captureProvider{name: "gemini"}
	ts := newTestServer(t, provider)
	defer ts.Close()

	generateReq, err := http.NewRequest(http.MethodPost, ts.URL+"/v1beta/models/upstream-gemini:generateContent", strings.NewReader(`{
		"contents":[{"role":"user","parts":[{"text":"ping"}]}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	generateReq.Header.Set("Content-Type", "application/json")
	generateResp, err := http.DefaultClient.Do(generateReq)
	if err != nil {
		t.Fatal(err)
	}
	defer generateResp.Body.Close()
	generateBody, err := io.ReadAll(generateResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if generateResp.StatusCode != http.StatusOK {
		t.Fatalf("generate status = %d, want %d: %s", generateResp.StatusCode, http.StatusOK, generateBody)
	}
	for _, want := range []string{`"responseId":"resp_1"`, `"candidates"`, `"text":"pong"`} {
		if !strings.Contains(string(generateBody), want) {
			t.Fatalf("generate response missing %q: %s", want, generateBody)
		}
	}

	env := provider.last()
	if env == nil {
		t.Fatal("provider did not capture generate request")
	}
	if env.Provider != "gemini" {
		t.Fatalf("provider = %q, want gemini", env.Provider)
	}
	if env.Model != "upstream-gemini" {
		t.Fatalf("model = %q, want upstream-gemini", env.Model)
	}
	if got := env.PromptText(); got != "" {
		t.Fatalf("prompt text = %q, want empty for minimal decoder", got)
	}
	if !bytes.Contains(env.RawBody, []byte(`"text":"ping"`)) {
		t.Fatal("raw request body did not preserve gemini text content")
	}

	embedReq, err := http.NewRequest(http.MethodPost, ts.URL+"/v1beta/models/upstream-gemini-embed:embedContent", strings.NewReader(`{
		"content":{"parts":[{"text":"embed me"}]}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	embedReq.Header.Set("Content-Type", "application/json")
	embedResp, err := http.DefaultClient.Do(embedReq)
	if err != nil {
		t.Fatal(err)
	}
	defer embedResp.Body.Close()
	embedBody, err := io.ReadAll(embedResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if embedResp.StatusCode != http.StatusOK {
		t.Fatalf("embed status = %d, want %d: %s", embedResp.StatusCode, http.StatusOK, embedBody)
	}
	for _, want := range []string{`"embeddings"`, `"values":[0.1,0.2]`} {
		if !strings.Contains(string(embedBody), want) {
			t.Fatalf("embed response missing %q: %s", want, embedBody)
		}
	}

	env = provider.last()
	if env == nil {
		t.Fatal("provider did not capture embed request")
	}
	if env.Operation != gateway.OpEmbeddings {
		t.Fatalf("operation = %q, want %q", env.Operation, gateway.OpEmbeddings)
	}
	if env.Model != "upstream-gemini-embed" {
		t.Fatalf("model = %q, want upstream-gemini-embed", env.Model)
	}

	// v1 path variant should also route and path model should take precedence.
	generateV1Req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/models/upstream-gemini:generateContent", strings.NewReader(`{
		"model":"wrong-model",
		"contents":[{"role":"user","parts":[{"text":"ping-v1"}]}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	generateV1Req.Header.Set("Content-Type", "application/json")
	generateV1Resp, err := http.DefaultClient.Do(generateV1Req)
	if err != nil {
		t.Fatal(err)
	}
	defer generateV1Resp.Body.Close()
	generateV1Body, err := io.ReadAll(generateV1Resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if generateV1Resp.StatusCode != http.StatusOK {
		t.Fatalf("v1 generate status = %d, want %d: %s", generateV1Resp.StatusCode, http.StatusOK, generateV1Body)
	}
	env = provider.last()
	if env == nil {
		t.Fatal("provider did not capture v1 generate request")
	}
	if env.Model != "upstream-gemini" {
		t.Fatalf("model = %q, want path model upstream-gemini", env.Model)
	}

	embedV1Req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/models/upstream-gemini-embed-v1:embedContent", strings.NewReader(`{
		"model":"wrong-embed-model",
		"content":{"parts":[{"text":"embed-v1"}]}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	embedV1Req.Header.Set("Content-Type", "application/json")
	embedV1Resp, err := http.DefaultClient.Do(embedV1Req)
	if err != nil {
		t.Fatal(err)
	}
	defer embedV1Resp.Body.Close()
	embedV1Body, err := io.ReadAll(embedV1Resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if embedV1Resp.StatusCode != http.StatusOK {
		t.Fatalf("v1 embed status = %d, want %d: %s", embedV1Resp.StatusCode, http.StatusOK, embedV1Body)
	}
	env = provider.last()
	if env == nil {
		t.Fatal("provider did not capture v1 embed request")
	}
	if env.Operation != gateway.OpEmbeddings {
		t.Fatalf("operation = %q, want %q", env.Operation, gateway.OpEmbeddings)
	}
	if env.Model != "upstream-gemini-embed-v1" {
		t.Fatalf("model = %q, want path model upstream-gemini-embed-v1", env.Model)
	}
}

func TestProviderNativeGeminiInvalidPath(t *testing.T) {
	provider := &captureProvider{name: "gemini"}
	ts := newTestServer(t, provider)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1beta/models/upstream-gemini:unknownOp", strings.NewReader(`{
		"contents":[{"role":"user","parts":[{"text":"ping"}]}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusNotFound, body)
	}
	if !strings.Contains(string(body), `"code":"not_found"`) {
		t.Fatalf("body = %s, want not_found", body)
	}
	if provider.last() != nil {
		t.Fatal("provider should not be invoked for unknown gemini operation")
	}
}

func TestProviderNativeRoutingFiltersByProvider(t *testing.T) {
	openaiProvider := &captureProvider{name: "openai"}
	anthropicProvider := &captureProvider{name: "anthropic"}
	ts := newTestServer(t, openaiProvider, anthropicProvider)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/messages", strings.NewReader(`{
		"model":"shared-model",
		"messages":[{"role":"user","content":"ping"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
	}
	if openaiProvider.last() != nil {
		t.Fatal("openai provider should not receive anthropic-native request")
	}
	if anthropicProvider.last() == nil {
		t.Fatal("anthropic provider did not receive anthropic-native request")
	}
}

func TestProviderNativeStreamingPassesThroughAnthropic(t *testing.T) {
	provider := &captureProvider{
		name: "anthropic",
		invoke: func(req *gateway.Request) (*gateway.Result, error) {
			if !req.Stream {
				t.Fatal("expected anthropic stream request")
			}
			return &gateway.Result{
				StatusCode:  http.StatusOK,
				ContentType: "text/event-stream",
				RawStream:   io.NopCloser(strings.NewReader("event: message_start\ndata: {\"type\":\"message_start\"}\n\n")),
				Usage:       gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2},
			}, nil
		},
	}
	ts := newTestServer(t, provider)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/messages", strings.NewReader(`{
		"model":"upstream-anthropic",
		"stream":true,
		"messages":[{"role":"user","content":"ping"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusOK, body)
	}
	if !strings.Contains(string(body), "message_start") {
		t.Fatalf("body = %s, want passthrough stream payload", body)
	}
	if provider.last() == nil {
		t.Fatal("provider should be invoked for anthropic native streaming")
	}
}

func TestHTTPTransport(t *testing.T) {
	provider := &captureProvider{
		invoke: func(req *gateway.Request) (*gateway.Result, error) {
			if req.Operation == gateway.OpChatCompletions && req.Stream {
				return rawStreamResult("data: {\"id\":\"chat_1\",\"object\":\"chat.completion.chunk\",\"model\":\"" + req.Model + "\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n"), nil
			}
			return defaultResult(req), nil
		},
	}
	baseURL, shutdown := newHTTPServer(t, provider)
	defer shutdown()

	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"ok":true`) {
		t.Fatalf("healthz response = %d %s", resp.StatusCode, body)
	}

	chatResp, err := http.Post(baseURL+"/v1/chat/completions", "application/json", strings.NewReader(`{
		"model":"upstream-demo",
		"messages":[{"role":"user","content":"ping"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer chatResp.Body.Close()
	chatBody, err := io.ReadAll(chatResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if chatResp.StatusCode != http.StatusOK || !strings.Contains(string(chatBody), `"chat.completion"`) {
		t.Fatalf("chat response = %d %s", chatResp.StatusCode, chatBody)
	}

	streamResp, err := http.Post(baseURL+"/v1/chat/completions", "application/json", strings.NewReader(`{
		"model":"upstream-demo",
		"stream":true,
		"messages":[{"role":"user","content":"ping"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer streamResp.Body.Close()
	streamBody, err := io.ReadAll(streamResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if streamResp.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d: %s", streamResp.StatusCode, streamBody)
	}
	if got := streamResp.Header.Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("stream content-type = %q, want text/event-stream", got)
	}
	if !strings.Contains(string(streamBody), "hello") || !strings.Contains(string(streamBody), "[DONE]") {
		t.Fatalf("stream body = %s", streamBody)
	}
}

func newTestServer(t *testing.T, providers ...gateway.Provider) *httptest.Server {
	t.Helper()
	cfg := loadTestConfig(t)
	cfgStore := gateway.NewConfigStore(cfg)
	engine := gateway.NewEngine(cfgStore, providers, nil, nil)
	srv := gwapi.NewServer(engine, cfgStore, nil, store.NewMemoryQuotaLimitStore(), store.NewMemoryQuotaStore())
	return httptest.NewServer(srv.Handler())
}

func newQuotaServer(t *testing.T, provider gateway.Provider, limitStore store.QuotaLimitStore, quotaStore store.QuotaStore) *httptest.Server {
	t.Helper()
	cfg := loadQuotaConfig(t)
	cfgStore := gateway.NewConfigStore(cfg)
	engine := gateway.NewEngine(
		cfgStore,
		[]gateway.Provider{provider},
		[]gateway.RequestInterceptor{
			policy.Auth{},
			policy.TokenValidation{},
			policy.ResolveScopes{Limits: limitStore},
			policy.Quota{Store: quotaStore},
		},
		nil,
	)
	usageStore, _ := quotaStore.(store.QuotaUsageStore)
	srv := gwapi.NewServer(engine, cfgStore, nil, limitStore, usageStore)
	return httptest.NewServer(srv.Handler())
}

func newConfigQuotaServer(t *testing.T, provider gateway.Provider, quotaStore store.QuotaStore) *httptest.Server {
	t.Helper()
	cfg := loadConfigQuotaConfig(t)
	cfgStore := gateway.NewConfigStore(cfg)
	engine := gateway.NewEngine(
		cfgStore,
		[]gateway.Provider{provider},
		[]gateway.RequestInterceptor{
			policy.Auth{},
			policy.TokenValidation{},
			policy.ResolveScopes{},
			policy.Quota{Store: quotaStore},
		},
		nil,
	)
	usageStore, _ := quotaStore.(store.QuotaUsageStore)
	srv := gwapi.NewServer(engine, cfgStore, nil, nil, usageStore)
	return httptest.NewServer(srv.Handler())
}

func newHTTPServer(t *testing.T, provider gateway.Provider) (string, func()) {
	t.Helper()
	cfg := loadTestConfig(t)
	cfgStore := gateway.NewConfigStore(cfg)
	engine := gateway.NewEngine(cfgStore, []gateway.Provider{provider}, nil, nil)
	srv := gwapi.NewServer(engine, cfgStore, nil, store.NewMemoryQuotaLimitStore(), store.NewMemoryQuotaStore())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: srv.Handler()}
	errCh := make(chan error, 1)
	go func() {
		errCh <- httpServer.Serve(ln)
	}()
	return "http://" + ln.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-errCh; err != nil && err != http.ErrServerClosed && !strings.Contains(err.Error(), "use of closed network connection") {
			t.Fatal(err)
		}
	}
}

func loadTestConfig(t *testing.T) *gateway.Snapshot {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.example.yaml")
	content := `
server:
  listen: ":0"
auth:
  max_body_bytes: 1048576
store:
  mode: memory
routes:
  demo-route:
    provider: openai
    model: upstream-demo
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions, responses, completions, embeddings]
      streaming: true
      tool_calling: true
      structured_output: true
      reasoning: true
  anthropic-route:
    provider: anthropic
    model: upstream-anthropic
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      streaming: true
      tool_calling: true
      structured_output: true
      reasoning: true
  gemini-route:
    provider: gemini
    model: upstream-gemini
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      tool_calling: true
      structured_output: true
      reasoning: true
  gemini-embed-route:
    provider: gemini
    model: upstream-gemini-embed
    base_url: http://example.invalid/v1
    capabilities:
      operations: [embeddings]
  gemini-embed-v1-route:
    provider: gemini
    model: upstream-gemini-embed-v1
    base_url: http://example.invalid/v1
    capabilities:
      operations: [embeddings]
  shared-openai-route:
    provider: openai
    model: shared-model
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      tool_calling: true
      structured_output: true
      reasoning: true
  shared-anthropic-route:
    provider: anthropic
    model: shared-model
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      tool_calling: true
      structured_output: true
      reasoning: true
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := gateway.LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func loadQuotaConfig(t *testing.T) *gateway.Snapshot {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.example.yaml")
	content := `
server:
  listen: ":0"
auth:
  max_body_bytes: 1048576
  jwt:
    algorithm: HS256
    issuer: llmgw-tests
    audience: gateway
    secret: test-secret
    claims:
      principal: sub
      key_id: key_id
store:
  mode: memory
routes:
  demo-route:
    provider: openai
    model: upstream-demo
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      streaming: true
      tool_calling: true
      structured_output: true
      reasoning: true
    pricing:
      input_per_1m: 1.0
      output_per_1m: 1.0
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := gateway.LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func loadConfigQuotaConfig(t *testing.T) *gateway.Snapshot {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.example.yaml")
	content := `
server:
  listen: ":0"
auth:
  max_body_bytes: 1048576
  tokens:
    local-dev-token:
      id: dev
      key_id: dev-key
store:
  mode: memory
quota:
  profiles:
    dev-token:
      rpm: 60
      daily_tokens: 1000000
  keys:
    dev-key: dev-token
routes:
  demo-route:
    provider: openai
    model: upstream-demo
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      streaming: true
      tool_calling: true
      structured_output: true
      reasoning: true
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := gateway.LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func defaultResult(req *gateway.Request) *gateway.Result {
	switch req.Operation {
	case gateway.OpChatCompletions:
		if req.Provider == "anthropic" {
			return rawJSONResult(`{"id":"msg_1","type":"message","role":"assistant","model":"`+req.Model+`","content":[{"type":"text","text":"pong"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`, gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2})
		}
		if req.Provider == "gemini" {
			return rawJSONResult(`{"responseId":"resp_1","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"pong"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}`, gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2})
		}
		return rawJSONResult(`{"id":"chat_1","object":"chat.completion","model":"`+req.Model+`","created":1,"choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2})
	case gateway.OpResponses:
		return rawJSONResult(`{"id":"resp_1","object":"response","model":"`+req.Model+`","created":1,"status":"completed","output_text":"pong","usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2})
	case gateway.OpCompletions:
		return rawJSONResult(`{"id":"cmpl_1","object":"text_completion","model":"`+req.Model+`","created":1,"choices":[{"index":0,"text":"pong","finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2})
	case gateway.OpEmbeddings:
		if req.Provider == "gemini" {
			return rawJSONResult(`{"embeddings":[{"values":[0.1,0.2]}]}`, gateway.Usage{InputTokens: 1, TotalTokens: 1})
		}
		return rawJSONResult(`{"object":"list","model":"`+req.Model+`","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":1,"total_tokens":1}}`, gateway.Usage{InputTokens: 1, TotalTokens: 1})
	default:
		return &gateway.Result{}
	}
}

func rawJSONResult(body string, usage gateway.Usage) *gateway.Result {
	return &gateway.Result{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		RawBody:     []byte(body),
		Usage:       usage,
	}
}

func rawStreamResult(body string) *gateway.Result {
	return &gateway.Result{
		StatusCode:  http.StatusOK,
		ContentType: "text/event-stream",
		RawStream:   io.NopCloser(strings.NewReader(body)),
		Usage:       gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2},
	}
}

func signedQuotaJWT(t *testing.T, secret string, claims jwt.MapClaims) string {
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
