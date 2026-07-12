package performance

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	gwapi "llmgw/api"
	"llmgw/gateway"
	"llmgw/observer"
	"llmgw/policy"
	"llmgw/store"
)

const (
	benchmarkKeyID        = "benchmark-key"
	benchmarkLimitProfile = "benchmark-enforced"
	benchmarkToken        = "benchmark-token"
)

var (
	chatResponseBody = []byte(`{"id":"chat_bench","object":"chat.completion","model":"benchmark-model","choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	responsesBody    = []byte(`{"id":"resp_bench","object":"response","model":"benchmark-model","status":"completed","output_text":"pong","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
	completionsBody  = []byte(`{"id":"cmpl_bench","object":"text_completion","model":"benchmark-model","choices":[{"index":0,"text":"pong","finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	embeddingsBody   = []byte(`{"object":"list","model":"benchmark-model","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":1,"total_tokens":1}}`)
)

type benchmarkEndpoint struct {
	name   string
	method string
	path   string
	body   []byte
}

var benchmarkEndpoints = []benchmarkEndpoint{
	{name: "healthz", method: http.MethodGet, path: "/healthz"},
	{name: "readyz", method: http.MethodGet, path: "/readyz"},
	{name: "models", method: http.MethodGet, path: "/v1/models"},
	{
		name:   "chat_completions",
		method: http.MethodPost,
		path:   "/v1/chat/completions",
		body:   []byte(`{"model":"benchmark-model","messages":[{"role":"user","content":"ping"}]}`),
	},
	{
		name:   "responses",
		method: http.MethodPost,
		path:   "/v1/responses",
		body:   []byte(`{"model":"benchmark-model","input":"ping"}`),
	},
	{
		name:   "completions",
		method: http.MethodPost,
		path:   "/v1/completions",
		body:   []byte(`{"model":"benchmark-model","prompt":"ping"}`),
	},
	{
		name:   "embeddings",
		method: http.MethodPost,
		path:   "/v1/embeddings",
		body:   []byte(`{"model":"benchmark-model","input":"ping"}`),
	},
}

type benchmarkProvider struct{}

func (benchmarkProvider) Name() string { return "openai" }

func (benchmarkProvider) Supports(op gateway.Operation) bool {
	switch op {
	case gateway.OpChatCompletions, gateway.OpResponses, gateway.OpCompletions, gateway.OpEmbeddings:
		return true
	default:
		return false
	}
}

func (benchmarkProvider) Invoke(_ context.Context, _ gateway.ResolvedRoute, req *gateway.Request) (*gateway.Result, error) {
	body, usage := benchmarkResponse(req.Operation)
	return &gateway.Result{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		RawBody:     body,
		Usage:       usage,
	}, nil
}

func BenchmarkHTTP(b *testing.B) {
	server, client := newBenchmarkServer(b)

	for _, endpoint := range benchmarkEndpoints {
		endpoint := endpoint
		b.Run(endpoint.name, func(b *testing.B) {
			benchmarkHTTPEndpoints(b, client, server.URL, []benchmarkEndpoint{endpoint})
		})
	}

	b.Run("mixed_inference", func(b *testing.B) {
		benchmarkHTTPEndpoints(b, client, server.URL, benchmarkEndpoints[3:])
	})
}

func TestBenchmarkHarness(t *testing.T) {
	server, client := newBenchmarkServer(t)
	for _, endpoint := range benchmarkEndpoints {
		if err := invokeEndpoint(client, server.URL, endpoint); err != nil {
			t.Fatalf("%s: %v", endpoint.name, err)
		}
	}
}

func benchmarkHTTPEndpoints(b *testing.B, client *http.Client, baseURL string, endpoints []benchmarkEndpoint) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	var once sync.Once
	var firstErr error
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			endpoint := endpoints[idx%len(endpoints)]
			idx++
			if err := invokeEndpoint(client, baseURL, endpoint); err != nil {
				once.Do(func() { firstErr = err })
				return
			}
		}
	})

	elapsed := b.Elapsed()
	b.StopTimer()
	if firstErr != nil {
		b.Fatal(firstErr)
	}
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "req/s")
	}
}

func invokeEndpoint(client *http.Client, baseURL string, endpoint benchmarkEndpoint) error {
	req, err := http.NewRequest(endpoint.method, baseURL+endpoint.path, bytes.NewReader(endpoint.body))
	if err != nil {
		return err
	}
	if len(endpoint.body) > 0 {
		req.Header.Set("Authorization", "Bearer "+benchmarkToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-LLMGW-User", "benchmark-user")
		req.Header.Set("X-Project-ID", "benchmark-project")
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("%s %s returned %s: %s", endpoint.method, endpoint.path, resp.Status, body)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

func newBenchmarkServer(tb testing.TB) (*httptest.Server, *http.Client) {
	tb.Helper()
	cfg, err := gateway.LoadConfigFile("testdata/config.yaml")
	if err != nil {
		tb.Fatal(err)
	}
	cfgStore := gateway.NewConfigStore(cfg)
	obsrv := observer.New("llmgw-benchmark")
	// Keep production-equivalent JSON log formatting in the measured path without
	// making terminal or disk throughput part of the result.
	obsrv.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	quotaStore := store.NewMemoryQuotaStore()
	limitStore := store.NewMemoryQuotaLimitStore()
	limits, ok := cfg.Quota.Profiles[benchmarkLimitProfile]
	if !ok {
		tb.Fatalf("benchmark quota profile %q is not configured", benchmarkLimitProfile)
	}
	validateBenchmarkConfig(tb, cfg, limits)
	if err := limitStore.Put(context.Background(), benchmarkKeyID, limits); err != nil {
		tb.Fatalf("configure benchmark key limits: %v", err)
	}
	attemptLimits := &policy.AttemptLimits{
		Rates:    store.NewMemoryRateStore(),
		Breakers: policy.NewBreaker(),
	}
	engine := gateway.NewEngine(
		cfgStore,
		[]gateway.Provider{benchmarkProvider{}},
		[]gateway.RequestInterceptor{
			observer.RequestMetrics{Obs: obsrv},
			policy.Auth{},
			policy.RequireUser{},
			policy.RequestSize{},
			policy.TokenValidation{},
			policy.ACL{},
			policy.ResolveScopes{Limits: limitStore},
			policy.Quota{Store: quotaStore, Obs: obsrv},
		},
		[]gateway.AttemptInterceptor{
			policy.AttemptHeaders{},
			observer.AttemptMetrics{Obs: obsrv},
			attemptLimits,
		},
	)
	apiServer := gwapi.NewServer(engine, cfgStore, obsrv, limitStore, quotaStore)
	server := httptest.NewServer(apiServer.Handler())
	transport := &http.Transport{
		DisableCompression:    true,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   1024,
		IdleConnTimeout:       30 * time.Second,
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: 5 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}
	tb.Cleanup(func() {
		transport.CloseIdleConnections()
		server.Close()
	})
	return server, client
}

func validateBenchmarkConfig(tb testing.TB, cfg *gateway.Snapshot, limits gateway.LimitSpec) {
	tb.Helper()
	if !cfg.Auth.RequireUser || !cfg.Auth.RequireProject {
		tb.Fatal("benchmark must require both user and project")
	}
	if limits.RPM <= 0 || limits.TPM <= 0 || limits.MaxParallel <= 0 ||
		limits.MaxSpendMicros <= 0 || limits.DailyTokens <= 0 || limits.MonthlyTokens <= 0 ||
		limits.MaxInputTokens <= 0 || limits.MaxOutputTokens <= 0 ||
		len(limits.ModelAllowlist) == 0 || len(limits.ProviderAllowlist) == 0 {
		tb.Fatal("benchmark key quota and ACL limits must all be active")
	}
	route := cfg.Routes["benchmark-route"]
	if route == nil || route.Limits.RPM <= 0 || route.Limits.TPM <= 0 ||
		route.Limits.Concurrency <= 0 || route.Limits.ProviderConcurrency <= 0 ||
		route.Limits.MaxBodyBytes <= 0 || route.Pricing.InputPer1M <= 0 || route.Pricing.OutputPer1M <= 0 {
		tb.Fatal("benchmark route limits and pricing must all be active")
	}
}

func benchmarkResponse(op gateway.Operation) ([]byte, gateway.Usage) {
	switch op {
	case gateway.OpChatCompletions:
		return chatResponseBody, gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}
	case gateway.OpResponses:
		return responsesBody, gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}
	case gateway.OpCompletions:
		return completionsBody, gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}
	case gateway.OpEmbeddings:
		return embeddingsBody, gateway.Usage{InputTokens: 1, TotalTokens: 1}
	default:
		return nil, gateway.Usage{}
	}
}
