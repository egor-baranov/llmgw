package proxy_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"llmgw/gateway"
	"llmgw/policy"
	"llmgw/proxy"
	"llmgw/store"
)

func decodeJSON[T any](t *testing.T, raw json.RawMessage) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	return out
}

func TestOpenAIProxyInvokeUnary(t *testing.T) {
	var body map[string]json.RawMessage
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q, want /v1/chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("authorization = %q, want Bearer test-key", got)
		}
		if got := r.Header.Get("X-Static"); got != "static" {
			t.Fatalf("X-Static = %q, want static", got)
		}
		if got := r.Header.Get("X-LLMGW-Route"); got != "openai-primary" {
			t.Fatalf("X-LLMGW-Route = %q, want openai-primary", got)
		}
		if got := r.Header.Get("X-Request-ID"); got != "req_openai" {
			t.Fatalf("X-Request-ID = %q, want req_openai", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chat_1","object":"chat.completion","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`))
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
		Route: &gateway.Route{
			Name:          "openai-primary",
			Provider:      "openai",
			BaseURL:       upstream.URL + "/v1",
			Model:         "public-alias",
			UpstreamModel: "route-model",
			APIKey:        "test-key",
			Headers:       map[string]string{"X-Static": "static"},
		},
		Headers: http.Header{"X-LLMGW-Route": []string{"openai-primary"}},
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "public-model",
		RawBody:   json.RawMessage(`{"model":"public-model","messages":[{"role":"user","content":"ping"}],"temperature":0.2}`),
		Meta:      gateway.Meta{RequestID: "req_openai"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := decodeJSON[string](t, body["model"]); got != "route-model" {
		t.Fatalf("model = %q, want route-model", got)
	}
	if got := decodeJSON[float64](t, body["temperature"]); got != 0.2 {
		t.Fatalf("temperature = %v, want 0.2", got)
	}
	if result.ContentType != "application/json" {
		t.Fatalf("content-type = %q, want application/json", result.ContentType)
	}
	if !strings.Contains(string(result.RawBody), `"chat_1"`) {
		t.Fatalf("raw body = %s, want upstream payload", result.RawBody)
	}
	if result.Usage.TotalTokens != 5 {
		t.Fatalf("usage = %#v, want total_tokens=5", result.Usage)
	}
}

func TestOpenAIProxyRetainsFallbackTokensForMalformedUsage(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chat_usage","choices":[],"usage":{"prompt_tokens":-2,"completion_tokens":-3,"total_tokens":-5},"output":[{"type":"web_search_call"},{"type":"web_search_call"}]}`)
	}))
	defer upstream.Close()

	estimate := gateway.Usage{
		InputTokens: 10, OutputTokens: 20, TotalTokens: 30,
		ProviderDetails: map[string]int64{"web_search_requests": 4},
	}
	result, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
		Route: &gateway.Route{
			Provider: "openai",
			BaseURL:  upstream.URL + "/v1",
			Model:    "model",
		},
		Estimate: estimate,
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		RawBody:   json.RawMessage(`{"model":"model","messages":[]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Usage.InputTokens != estimate.InputTokens || result.Usage.OutputTokens != estimate.OutputTokens || result.Usage.TotalTokens != estimate.TotalTokens {
		t.Fatalf("usage = %#v, want fallback token estimate %#v", result.Usage, estimate)
	}
	if got := result.Usage.ProviderDetails["web_search_requests"]; got != 2 {
		t.Fatalf("web-search usage = %d, want exact reported count 2", got)
	}
}

func TestOpenAIProxyPreservesCacheUsageWhenFallingBackTokens(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chat_usage","choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"prompt_tokens_details":{"cached_tokens":5}}}`)
	}))
	defer upstream.Close()

	estimate := gateway.Usage{InputTokens: 10, OutputTokens: 20, TotalTokens: 30, CacheWriteTokens: 10}
	result, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
		Route:    &gateway.Route{Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "model"},
		Estimate: estimate,
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		RawBody:   json.RawMessage(`{"model":"model","messages":[]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Usage.InputTokens != 10 || result.Usage.OutputTokens != 20 || result.Usage.CacheReadTokens != 5 || result.Usage.CacheWriteTokens != 10 {
		t.Fatalf("usage = %#v, want fallback tokens with reported cache reads", result.Usage)
	}
}

func TestOpenAIProxyRetainsFallbackForMalformedCacheUsage(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chat_usage","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12,"prompt_tokens_details":{"cached_tokens":-1}}}`)
	}))
	defer upstream.Close()

	estimate := gateway.Usage{
		InputTokens: 10, OutputTokens: 20, TotalTokens: 30, CacheReadTokens: 5,
		InputDetails: &gateway.UsageDetails{CachedTokens: 5},
	}
	result, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
		Route:    &gateway.Route{Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "model"},
		Estimate: estimate,
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		RawBody:   json.RawMessage(`{"model":"model","messages":[]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Usage.InputTokens != 10 || result.Usage.OutputTokens != 2 || result.Usage.CacheReadTokens != 5 ||
		result.Usage.InputDetails == nil || result.Usage.InputDetails.CachedTokens != 5 {
		t.Fatalf("usage = %#v, want valid core usage with fallback cache reads", result.Usage)
	}
}

func TestBuiltInProvidersRejectNonArraySuccessCollections(t *testing.T) {
	tests := []struct {
		name             string
		adapter          proxy.Adapter
		operation        gateway.Operation
		responseTemplate string
	}{
		{
			name:             "OpenAI chat choices",
			adapter:          proxy.OpenAIAdapter(),
			operation:        gateway.OpChatCompletions,
			responseTemplate: `{"id":"chat_invalid","choices":VALUE}`,
		},
		{
			name:             "OpenAI response output",
			adapter:          proxy.OpenAIAdapter(),
			operation:        gateway.OpResponses,
			responseTemplate: `{"id":"response_invalid","output":VALUE}`,
		},
		{
			name:             "OpenAI embedding data",
			adapter:          proxy.OpenAIAdapter(),
			operation:        gateway.OpEmbeddings,
			responseTemplate: `{"data":VALUE}`,
		},
		{
			name:             "Anthropic content",
			adapter:          proxy.AnthropicAdapter(),
			operation:        gateway.OpChatCompletions,
			responseTemplate: `{"id":"message_invalid","content":VALUE}`,
		},
	}
	invalidValues := []struct {
		name  string
		value string
	}{
		{name: "null", value: "null"},
		{name: "object", value: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, invalid := range invalidValues {
				t.Run(invalid.name, func(t *testing.T) {
					response := strings.Replace(tt.responseTemplate, "VALUE", invalid.value, 1)
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, response)
					}))
					defer upstream.Close()

					_, err := proxy.NewProvider(tt.adapter, upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
						Route: &gateway.Route{
							Provider: tt.adapter.Name,
							BaseURL:  upstream.URL + "/v1",
							Model:    "model",
						},
					}, &gateway.Request{
						Operation: tt.operation,
						RawBody:   json.RawMessage(`{"model":"model","messages":[]}`),
					})
					if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Code != "invalid_upstream_response" {
						t.Fatalf("Invoke() error = %#v, want invalid_upstream_response", apiErr)
					}
				})
			}
		})
	}
}

func TestProxyMaxInt64ResponseLimitDoesNotOverflow(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"chat_limit","choices":[]}`)
	}))
	defer upstream.Close()

	result, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "model",
		Limits: gateway.LimitConfig{MaxResponseBytes: math.MaxInt64},
	}}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		RawBody:   json.RawMessage(`{"model":"model","messages":[]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result.RawBody), `"chat_limit"`) {
		t.Fatalf("body = %s, want complete upstream response", result.RawBody)
	}
}

func TestEmbeddingResponseCanUseConfiguredLargerUnaryLimit(t *testing.T) {
	payload := `{"object":"list","data":[],"padding":"` + strings.Repeat("x", 17<<20) + `"}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, payload)
	}))
	defer upstream.Close()
	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "embedding-model",
		Limits: gateway.LimitConfig{MaxResponseBytes: 20 << 20},
	}}, &gateway.Request{
		Operation: gateway.OpEmbeddings,
		RawBody:   json.RawMessage(`{"model":"embedding-model","input":["hello"]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RawBody) != len(payload) {
		t.Fatalf("response bytes = %d, want %d", len(result.RawBody), len(payload))
	}
}

func TestOpenAIResponsesUsageUsesInputOutputFields(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"resp_1","output":[],"usage":{"input_tokens":7,"output_tokens":4,"total_tokens":11}}`)
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai",
		BaseURL:  upstream.URL + "/v1",
		Model:    "route-model",
	}}, &gateway.Request{
		Operation: gateway.OpResponses,
		RawBody:   json.RawMessage(`{"model":"public-model","input":"ping"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Usage.InputTokens != 7 || result.Usage.OutputTokens != 4 || result.Usage.TotalTokens != 11 {
		t.Fatalf("usage = %#v, want input=7 output=4 total=11", result.Usage)
	}
}

func TestOpenAIUsageUsesConservativeMaximumAcrossAliases(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"resp_alias_usage","output":[],"usage":{"input_tokens":5,"prompt_tokens":10,"output_tokens":2,"completion_tokens":4,"total_tokens":14}}`)
	}))
	defer upstream.Close()

	result, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "route-model",
	}}, &gateway.Request{
		Operation: gateway.OpResponses,
		RawBody:   json.RawMessage(`{"model":"public-model","input":"ping"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Usage.InputTokens != 10 || result.Usage.OutputTokens != 4 || result.Usage.TotalTokens != 14 {
		t.Fatalf("usage = %#v, want conservative maxima input=10 output=4 total=14", result.Usage)
	}
}

func TestOpenAIChatUsageReadsPromptAndCompletionDetails(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"chat_1","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"prompt_tokens_details":{"cached_tokens":7,"audio_tokens":2},"completion_tokens_details":{"reasoning_tokens":3,"audio_tokens":1}}}`)
	}))
	defer upstream.Close()

	result, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "route-model",
	}}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"model":"route-model","messages":[{"role":"user","content":"ping"}]}`)})
	if err != nil {
		t.Fatal(err)
	}
	if result.Usage.CacheReadTokens != 7 || result.Usage.InputDetails == nil || result.Usage.InputDetails.AudioTokens != 2 {
		t.Fatalf("input usage details = %#v", result.Usage)
	}
	if result.Usage.OutputDetails == nil || result.Usage.OutputDetails.ReasoningTokens != 3 || result.Usage.OutputDetails.AudioTokens != 1 {
		t.Fatalf("output usage details = %#v", result.Usage.OutputDetails)
	}
}

func TestOpenAIResponsesToChatBridgeConvertsRequestAndResponse(t *testing.T) {
	var upstreamBody map[string]json.RawMessage
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q, want chat completions bridge", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&upstreamBody); err != nil {
			t.Fatal(err)
		}
		payload := `{"id":"chatcmpl-bridge","object":"chat.completion","created":123,"model":"upstream-model","choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"prompt_tokens_details":{"cached_tokens":2},"completion_tokens_details":{"reasoning_tokens":1}}}`
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.Header().Set("ETag", `"upstream-representation"`)
		_, _ = io.WriteString(w, payload)
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
		Route:      &gateway.Route{Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "public-alias", UpstreamModel: "upstream-model"},
		BridgeFrom: gateway.OpResponses,
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		RawBody: json.RawMessage(`{
			"model":"public-alias","instructions":"be concise","input":"ping","max_output_tokens":42,
			"parallel_tool_calls":false,"store":false,
			"text":{"format":{"type":"json_schema","name":"answer","schema":{"type":"object"}}}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeJSON[string](t, upstreamBody["model"]); got != "upstream-model" {
		t.Fatalf("upstream model = %q, want upstream-model", got)
	}
	if got := decodeJSON[int](t, upstreamBody["max_completion_tokens"]); got != 42 {
		t.Fatalf("max completion tokens = %d, want 42", got)
	}
	if got := decodeJSON[bool](t, upstreamBody["parallel_tool_calls"]); got {
		t.Fatal("parallel_tool_calls = true, want explicit false preserved")
	}
	if got := decodeJSON[bool](t, upstreamBody["store"]); got {
		t.Fatal("store = true, want explicit false preserved")
	}
	var messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(upstreamBody["messages"], &messages); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Role != "system" || messages[1].Content != "ping" {
		t.Fatalf("bridged messages = %#v, want system instructions and user input", messages)
	}
	var response struct {
		Object     string `json:"object"`
		Model      string `json:"model"`
		OutputText string `json:"output_text"`
		Usage      struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
			InputDetails struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"input_tokens_details"`
			OutputDetails struct {
				ReasoningTokens int64 `json:"reasoning_tokens"`
			} `json:"output_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(result.RawBody, &response); err != nil {
		t.Fatal(err)
	}
	if response.Object != "response" || response.Model != "public-alias" || response.OutputText != "pong" {
		t.Fatalf("bridged response = %#v", response)
	}
	if response.Usage.InputTokens != 5 || response.Usage.OutputTokens != 2 || response.Usage.InputDetails.CachedTokens != 2 || response.Usage.OutputDetails.ReasoningTokens != 1 {
		t.Fatalf("bridged response usage = %#v", response.Usage)
	}
	if result.Usage.TotalTokens != 7 {
		t.Fatalf("settlement usage = %#v, want upstream usage", result.Usage)
	}
	if got := result.Headers.Get("Content-Length"); got != "" {
		t.Fatalf("bridged Content-Length = %q, want removed", got)
	}
	if got := result.Headers.Get("ETag"); got != "" {
		t.Fatalf("bridged ETag = %q, want removed", got)
	}
}

func TestOpenAIResponsesToChatBridgePreservesImageDetail(t *testing.T) {
	var upstreamBody struct {
		Messages []struct {
			Role    string `json:"role"`
			Content []struct {
				Type     string `json:"type"`
				ImageURL struct {
					URL    string `json:"url"`
					Detail string `json:"detail"`
				} `json:"image_url"`
			} `json:"content"`
		} `json:"messages"`
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&upstreamBody); err != nil {
			t.Fatal(err)
		}
		_, _ = io.WriteString(w, `{"id":"chatcmpl-image","created":1,"model":"upstream-model","choices":[{"index":0,"message":{"role":"assistant","content":"described"},"finish_reason":"stop"}]}`)
	}))
	defer upstream.Close()

	_, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
		Route:      &gateway.Route{Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "alias", UpstreamModel: "upstream-model"},
		BridgeFrom: gateway.OpResponses,
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		RawBody: json.RawMessage(`{
			"model":"alias",
			"input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.test/image.png","detail":"high"}]}]
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(upstreamBody.Messages) != 1 || upstreamBody.Messages[0].Role != "user" || len(upstreamBody.Messages[0].Content) != 1 {
		t.Fatalf("bridged messages = %#v", upstreamBody.Messages)
	}
	image := upstreamBody.Messages[0].Content[0]
	if image.Type != "image_url" || image.ImageURL.URL != "https://example.test/image.png" || image.ImageURL.Detail != "high" {
		t.Fatalf("bridged image = %#v, want URL and detail preserved", image)
	}
}

func TestUnsupportedBridgeFieldsFallBackToNativeRoute(t *testing.T) {
	var calls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/v1/responses" {
			t.Fatalf("native fallback path = %q, want /v1/responses", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"id":"resp_native","object":"response","model":"upstream-native","output":[]}`)
	}))
	defer upstream.Close()

	capChat := gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}, Tokenizer: "cl100k_base"}
	capResponses := gateway.Capability{Operations: []gateway.Operation{gateway.OpResponses}, Tokenizer: "cl100k_base"}
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"bridge-first": {
			Name: "bridge-first", Provider: "openai", Model: "alias", UpstreamModel: "upstream-chat",
			BaseURL: upstream.URL + "/v1", Priority: 100, Capabilities: capChat,
		},
		"native-fallback": {
			Name: "native-fallback", Provider: "openai", Model: "alias", UpstreamModel: "upstream-native",
			BaseURL: upstream.URL + "/v1", Priority: 90, Capabilities: capResponses,
		},
	}}
	engine := gateway.NewEngine(gateway.NewConfigStore(snapshot), []gateway.Provider{proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())}, nil, nil)
	exec, err := engine.Execute(context.Background(), &gateway.Request{
		Provider: "openai", Operation: gateway.OpResponses, Model: "alias",
		RawBody: json.RawMessage(`{"model":"alias","input":"continue","previous_response_id":"resp_previous"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || exec.Attempt.Route.Route.Name != "native-fallback" {
		t.Fatalf("calls/route = %d/%q, want one native fallback call", calls, exec.Attempt.Route.Route.Name)
	}
}

func TestBridgePreflightExcludesUnattemptableCandidateBeforeQuotaReservation(t *testing.T) {
	var bridgeCalls, nativeCalls int
	bridge := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { bridgeCalls++ }))
	defer bridge.Close()
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nativeCalls++
		_, _ = io.WriteString(w, `{"id":"resp_native","object":"response","status":"completed","output":[],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}`)
	}))
	defer native.Close()
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"chat-bridge": {
			Name: "chat-bridge", Provider: "openai", BaseURL: bridge.URL + "/v1", Model: "alias", Priority: 2,
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
		"native": {
			Name: "native", Provider: "openai", BaseURL: native.URL + "/v1", Model: "alias", Priority: 1,
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpResponses}},
		},
	}}
	provider := proxy.NewProvider(proxy.OpenAIAdapter(), native.Client())
	quota := store.NewMemoryQuotaStore()
	engine := gateway.NewEngine(
		gateway.NewConfigStore(snapshot),
		[]gateway.Provider{provider},
		[]gateway.RequestInterceptor{
			gateway.NewCandidatePreflight([]gateway.Provider{provider}),
			fixedAttemptEstimate{},
			fixedQuotaScope{limit: gateway.LimitSpec{DailyTokens: 15}},
			policy.Quota{Store: quota, Reservation: time.Minute},
		},
		nil,
	)
	exec, err := engine.Execute(context.Background(), &gateway.Request{
		Provider: "openai", Operation: gateway.OpResponses, Model: "alias",
		RawBody: json.RawMessage(`{"model":"alias","input":"continue","previous_response_id":"resp_previous"}`),
	})
	if err != nil {
		t.Fatalf("Execute() denied a request that fits the native candidate quota: %v", err)
	}
	if bridgeCalls != 0 || nativeCalls != 1 || exec.Attempt.Route.Route.Name != "native" {
		t.Fatalf("bridge/native/route = %d/%d/%q, want 0/1/native", bridgeCalls, nativeCalls, exec.Attempt.Route.Route.Name)
	}
	if err := exec.Settle(context.Background(), exec.Result.Usage, nil); err != nil {
		t.Fatal(err)
	}
}

func TestOpenAICompletionsToChatBridge(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		var messages []struct {
			Content string `json:"content"`
		}
		_ = json.Unmarshal(body["messages"], &messages)
		if len(messages) != 1 || messages[0].Content != "complete this" {
			t.Fatalf("messages = %#v, want legacy prompt converted", messages)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-legacy","created":1,"system_fingerprint":"fp_bridge","service_tier":"priority","choices":[{"index":0,"message":{"role":"assistant","content":" done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
		Route:      &gateway.Route{Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "public-alias", UpstreamModel: "upstream-model"},
		BridgeFrom: gateway.OpCompletions,
	}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"model":"public-alias","prompt":"complete this","max_tokens":12}`)})
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Object            string `json:"object"`
		SystemFingerprint string `json:"system_fingerprint"`
		ServiceTier       string `json:"service_tier"`
		Choices           []struct {
			Text string `json:"text"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(result.RawBody, &response); err != nil {
		t.Fatal(err)
	}
	if response.Object != "text_completion" || len(response.Choices) != 1 || response.Choices[0].Text != " done" {
		t.Fatalf("bridged completion response = %#v", response)
	}
	if response.SystemFingerprint != "fp_bridge" || response.ServiceTier != "priority" {
		t.Fatalf("bridged response metadata = %#v", response)
	}
	if result.ContentType != "application/json" {
		t.Fatalf("bridged content type = %q, want application/json", result.ContentType)
	}
}

func TestOpenAICompletionsToChatBridgeStreamsLegacyChunks(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q, want chat completions bridge", r.URL.Path)
		}
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if !decodeJSON[bool](t, body["stream"]) {
			t.Fatal("upstream stream = false, want true")
		}
		var streamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		}
		if err := json.Unmarshal(body["stream_options"], &streamOptions); err != nil || !streamOptions.IncludeUsage {
			t.Fatalf("upstream stream_options = %s, want include_usage=true", body["stream_options"])
		}
		payload :=
			"data: {\"id\":\"chatcmpl-stream\",\"object\":\"chat.completion.chunk\",\r\n" +
				"data: \"created\":1,\"model\":\"upstream-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" hello\"},\"finish_reason\":null}]}\r\n\r\n" +
				"data: {\"id\":\"chatcmpl-stream\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"upstream-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\r\n\r\n" +
				"data: {\"id\":\"chatcmpl-stream\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"upstream-model\",\"system_fingerprint\":\"fp_stream\",\"service_tier\":\"priority\",\"obfuscation\":\"padding\",\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\r\n\r\n" +
				"data: [DONE]\r\n\r\n"
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = io.WriteString(w, payload)
	}))
	defer upstream.Close()

	result, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
		Route:      &gateway.Route{Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "public-alias", UpstreamModel: "upstream-model"},
		BridgeFrom: gateway.OpCompletions,
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Stream:    true,
		RawBody:   json.RawMessage(`{"model":"public-alias","prompt":"complete this","stream":true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer result.RawStream.Close()
	if got := result.Headers.Get("Content-Length"); got != "" {
		t.Fatalf("Content-Length = %q, want stripped after stream transformation", got)
	}
	data, err := io.ReadAll(result.RawStream)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{
		`"object":"text_completion"`, `"model":"public-alias"`, `"text":" hello"`,
		`"choices":[]`, `"system_fingerprint":"fp_stream"`, `"service_tier":"priority"`,
		`"obfuscation":"padding"`, "data: [DONE]", "\r\n\r\n",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("bridged stream missing %q: %s", want, text)
		}
	}
	usage := result.FinalUsage()
	if usage.InputTokens != 2 || usage.OutputTokens != 1 || usage.TotalTokens != 3 {
		t.Fatalf("usage = %#v, want 2/1/3", usage)
	}
}

func TestOpenAICompletionStreamBridgePreservesResponseCapErrors(t *testing.T) {
	tests := []struct {
		name        string
		model       string
		limit       int64
		payload     string
		wantPartial bool
	}{
		{
			name:    "source event exceeds cap",
			model:   "public-alias",
			limit:   64,
			payload: "data: {\"id\":\"chatcmpl-cap\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"" + strings.Repeat("x", 256) + "\"}}]}\n\n",
		},
		{
			name:        "transformed event exceeds cap",
			model:       strings.Repeat("m", 128),
			limit:       80,
			payload:     "data: {\"id\":\"x\",\"choices\":[]}\n\ndata: [DONE]\n\n",
			wantPartial: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tt.payload)
			}))
			defer upstream.Close()

			result, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
				Route: &gateway.Route{
					Provider: "openai", BaseURL: upstream.URL + "/v1", Model: tt.model,
					Limits: gateway.LimitConfig{MaxResponseBytes: tt.limit},
				},
				BridgeFrom: gateway.OpCompletions,
			}, &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Stream:    true,
				RawBody:   json.RawMessage(`{"model":"public-alias","prompt":"complete this","stream":true}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer result.RawStream.Close()
			body, readErr := io.ReadAll(result.RawStream)
			apiErr := gateway.AsAPIError(readErr)
			if readErr == nil || apiErr.Code != "response_too_large" || apiErr.CircuitFailure {
				t.Fatalf("stream error = %#v, want non-circuit response_too_large", apiErr)
			}
			if tt.wantPartial && int64(len(body)) != tt.limit {
				t.Fatalf("transformed bytes before cap = %d, want %d", len(body), tt.limit)
			}
			if !tt.wantPartial && len(body) != 0 {
				t.Fatalf("source-cap bytes = %d, want no partial transformed event", len(body))
			}
		})
	}
}

func TestOpenAICompletionStreamSemanticIncompatibilityFallsBackWithoutOpeningCircuit(t *testing.T) {
	tests := []struct {
		name  string
		delta string
	}{
		{name: "refusal", delta: `{"refusal":"cannot comply"}`},
		{name: "tool call", delta: `{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`},
		{name: "empty legacy function call", delta: `{"function_call":{}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bridgeCalls, nativeCalls int
			bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				bridgeCalls++
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, `data: {"id":"chatcmpl-incompatible","choices":[{"index":0,"delta":`+tt.delta+`,"finish_reason":"stop"}]}`+"\n\ndata: [DONE]\n\n")
			}))
			defer bridge.Close()
			native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				nativeCalls++
				if r.URL.Path != "/v1/completions" {
					t.Fatalf("native fallback path = %q, want /v1/completions", r.URL.Path)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"id\":\"cmpl-native\",\"object\":\"text_completion\",\"choices\":[{\"index\":0,\"text\":\"native\",\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
			}))
			defer native.Close()

			chatRoute := &gateway.Route{
				Name: "chat-bridge", Provider: "openai", BaseURL: bridge.URL + "/v1", Model: "alias", Priority: 2,
				Timeout:      gateway.Duration{Duration: time.Second},
				Circuit:      gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Minute}},
				Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}, Streaming: true},
			}
			nativeRoute := &gateway.Route{
				Name: "native", Provider: "openai", BaseURL: native.URL + "/v1", Model: "alias", Priority: 1,
				Timeout:      gateway.Duration{Duration: time.Second},
				Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpCompletions}, Streaming: true},
			}
			breaker := policy.NewBreaker()
			engine := gateway.NewEngine(
				gateway.NewConfigStore(&gateway.Snapshot{Routes: map[string]*gateway.Route{
					chatRoute.Name: chatRoute, nativeRoute.Name: nativeRoute,
				}}),
				[]gateway.Provider{proxy.NewProvider(proxy.OpenAIAdapter(), http.DefaultClient)},
				nil,
				[]gateway.AttemptInterceptor{&policy.AttemptLimits{Breakers: breaker}},
			)
			exec, err := engine.Execute(context.Background(), &gateway.Request{
				Provider: "openai", Operation: gateway.OpCompletions, Model: "alias", Stream: true,
				RawBody: json.RawMessage(`{"model":"alias","prompt":"complete this","stream":true}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer exec.Result.RawStream.Close()
			body, err := io.ReadAll(exec.Result.RawStream)
			if err != nil {
				t.Fatal(err)
			}
			if exec.Attempt.Route.Route.Name != nativeRoute.Name || !strings.Contains(string(body), `"text":"native"`) {
				t.Fatalf("selected route/body = %q/%s, want native fallback", exec.Attempt.Route.Route.Name, body)
			}
			if bridgeCalls != 1 || nativeCalls != 1 {
				t.Fatalf("bridge/native calls = %d/%d, want 1/1", bridgeCalls, nativeCalls)
			}
			if allowed, _ := breaker.AllowAttempt(chatRoute); !allowed {
				t.Fatal("semantic bridge incompatibility opened the chat route circuit")
			}
		})
	}
}

func TestOpenAICompletionResponseBridgeFallsBackWithoutLosingUnsupportedOutput(t *testing.T) {
	tests := []struct {
		name    string
		message string
	}{
		{
			name:    "tool call",
			message: `{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`,
		},
		{
			name:    "refusal",
			message: `{"role":"assistant","content":null,"refusal":"cannot comply"}`,
		},
		{
			name:    "legacy function call",
			message: `{"role":"assistant","content":null,"function_call":{"name":"lookup","arguments":"{}"}}`,
		},
		{
			name:    "malformed empty legacy function call",
			message: `{"role":"assistant","content":null,"function_call":{}}`,
		},
		{
			name:    "malformed false legacy function call",
			message: `{"role":"assistant","content":null,"function_call":false}`,
		},
		{
			name:    "malformed array legacy function call",
			message: `{"role":"assistant","content":null,"function_call":[]}`,
		},
		{
			name:    "non-text content",
			message: `{"role":"assistant","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/image.png"}}]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bridgeCalls, nativeCalls int
			bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				bridgeCalls++
				_, _ = io.WriteString(w, `{"id":"chatcmpl-bridge","created":1,"choices":[{"index":0,"message":`+tt.message+`,"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}`)
			}))
			defer bridge.Close()
			native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				nativeCalls++
				_, _ = io.WriteString(w, `{"id":"cmpl-native","object":"text_completion","created":1,"model":"alias","choices":[{"index":0,"text":"native","finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
			}))
			defer native.Close()

			snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
				"chat-bridge": {
					Name: "chat-bridge", Provider: "openai", BaseURL: bridge.URL + "/v1", Model: "alias", Priority: 2,
					Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
				},
				"native": {
					Name: "native", Provider: "openai", BaseURL: native.URL + "/v1", Model: "alias", Priority: 1,
					Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpCompletions}},
				},
			}}
			engine := gateway.NewEngine(gateway.NewConfigStore(snapshot), []gateway.Provider{proxy.NewProvider(proxy.OpenAIAdapter(), bridge.Client())}, nil, nil)
			exec, err := engine.Execute(context.Background(), &gateway.Request{
				Operation: gateway.OpCompletions,
				Model:     "alias",
				RawBody:   json.RawMessage(`{"model":"alias","prompt":"hello"}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			if bridgeCalls != 1 || nativeCalls != 1 || exec.Attempt.Route.Route.Name != "native" {
				t.Fatalf("bridge/native/route = %d/%d/%q, want 1/1/native", bridgeCalls, nativeCalls, exec.Attempt.Route.Route.Name)
			}
			if usage := exec.State.TotalAttemptUsage(); usage.InputTokens != 7 || usage.OutputTokens != 2 {
				t.Fatalf("attempt usage = %#v, want billed bridge plus native usage", usage)
			}
		})
	}
}

func TestOpenAIBridgesRejectLossyOrUnknownFieldsBeforeCallingUpstream(t *testing.T) {
	var calls int
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer upstream.Close()
	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	tests := []struct {
		name string
		from gateway.Operation
		body string
	}{
		{name: "previous response", from: gateway.OpResponses, body: `{"model":"alias","input":"continue","previous_response_id":"resp_1"}`},
		{name: "background", from: gateway.OpResponses, body: `{"model":"alias","input":"work","background":true}`},
		{name: "reasoning summary", from: gateway.OpResponses, body: `{"model":"alias","input":"think","reasoning":{"effort":"medium","summary":"auto"}}`},
		{name: "unknown responses field", from: gateway.OpResponses, body: `{"model":"alias","input":"x","future_option":false}`},
		{name: "legacy logprobs zero", from: gateway.OpCompletions, body: `{"model":"alias","prompt":"x","logprobs":0}`},
		{name: "invalid legacy echo", from: gateway.OpCompletions, body: `{"model":"alias","prompt":"x","echo":"false"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
				Route:      &gateway.Route{Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "alias"},
				BridgeFrom: tt.from,
			}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(tt.body)})
			if err == nil || gateway.AsAPIError(err).Code != "unsupported_operation" {
				t.Fatalf("Invoke() error = %v, want unsupported_operation", err)
			}
		})
	}
	if calls != 0 {
		t.Fatalf("upstream calls = %d, want 0 for rejected bridges", calls)
	}
}

func TestAnthropicProxyInvokeUnary(t *testing.T) {
	var body map[string]json.RawMessage
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Fatalf("path = %q, want /v1/messages", r.URL.Path)
		}
		if got := r.Header.Get("X-Api-Key"); got != "test-key" {
			t.Fatalf("X-Api-Key = %q, want test-key", got)
		}
		if got := r.Header.Get("anthropic-version"); got != "2024-01-01" {
			t.Fatalf("anthropic-version = %q, want 2024-01-01", got)
		}
		if got := r.Header.Get("anthropic-beta"); got != "prompt-caching-2024-07-31" {
			t.Fatalf("anthropic-beta = %q, want client feature header", got)
		}
		if got := r.Header.Get("X-Request-ID"); got != "req_anthropic" {
			t.Fatalf("X-Request-ID = %q, want req_anthropic", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"msg_1","content":[{"type":"text","text":"pong"}],"usage":{"input_tokens":7,"output_tokens":4}}`))
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.AnthropicAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
		Route: &gateway.Route{
			Provider: "anthropic",
			BaseURL:  upstream.URL + "/v1",
			Model:    "claude-route",
			APIKey:   "test-key",
			Headers:  map[string]string{"anthropic-version": "2024-01-01"},
		},
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "public-model",
		RawBody:   json.RawMessage(`{"model":"public-model","messages":[{"role":"user","content":"ping"}]}`),
		Meta: gateway.Meta{RequestID: "req_anthropic", Headers: http.Header{
			"Anthropic-Beta":    []string{"prompt-caching-2024-07-31"},
			"Anthropic-Version": []string{"2023-06-01"},
			"X-Api-Key":         []string{"gateway-token-must-not-be-forwarded"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := decodeJSON[string](t, body["model"]); got != "claude-route" {
		t.Fatalf("model = %q, want claude-route", got)
	}
	if result.Usage.InputTokens != 7 || result.Usage.OutputTokens != 4 || result.Usage.TotalTokens != 11 {
		t.Fatalf("usage = %#v, want input=7 output=4 total=11", result.Usage)
	}
}

func TestAnthropicUsageIncludesCacheAndServerToolCharges(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"msg_cached","type":"message","content":[],"usage":{"input_tokens":10,"output_tokens":20,"cache_read_input_tokens":1000,"cache_creation_input_tokens":500,"server_tool_use":{"web_search_requests":3}}}`)
	}))
	defer upstream.Close()
	provider := proxy.NewProvider(proxy.AnthropicAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
		Route: &gateway.Route{Provider: "anthropic", BaseURL: upstream.URL + "/v1", Model: "claude"},
	}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"model":"claude","messages":[]}`)})
	if err != nil {
		t.Fatal(err)
	}
	usage := result.Usage
	if usage.InputTokens != 1510 || usage.OutputTokens != 20 || usage.TotalTokens != 1530 {
		t.Fatalf("usage = %#v, want cache tokens included in billable input", usage)
	}
	if usage.CacheReadTokens != 1000 || usage.CacheWriteTokens != 500 || usage.ProviderDetails["web_search_requests"] != 3 {
		t.Fatalf("usage details = %#v, want cache and web-search counters", usage)
	}
}

func TestAnthropicMalformedInputUsageRetainsFallbackEstimate(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"msg_invalid_usage","type":"message","content":[],"usage":{"input_tokens":-5,"output_tokens":2}}`)
	}))
	defer upstream.Close()

	estimate := gateway.Usage{InputTokens: 10, OutputTokens: 20, TotalTokens: 30}
	result, err := proxy.NewProvider(proxy.AnthropicAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
		Route:    &gateway.Route{Provider: "anthropic", BaseURL: upstream.URL + "/v1", Model: "claude"},
		Estimate: estimate,
	}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"model":"claude","messages":[]}`)})
	if err != nil {
		t.Fatal(err)
	}
	if result.Usage.InputTokens != estimate.InputTokens || result.Usage.OutputTokens != 2 || result.Usage.TotalTokens != 12 {
		t.Fatalf("usage = %#v, want fallback input with reported output", result.Usage)
	}
}

func TestAnthropicMalformedProviderUsageRetainsFallbackReservation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"msg_invalid_usage","type":"message","content":[],"usage":{"input_tokens":10,"output_tokens":2,"server_tool_use":{"web_search_requests":-1}}}`)
	}))
	defer upstream.Close()

	result, err := proxy.NewProvider(proxy.AnthropicAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
		Route: &gateway.Route{Provider: "anthropic", BaseURL: upstream.URL + "/v1", Model: "claude"},
		Estimate: gateway.Usage{
			InputTokens: 10, OutputTokens: 20, TotalTokens: 30,
			ProviderDetails: map[string]int64{"web_search_requests": 4},
		},
	}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"model":"claude","messages":[]}`)})
	if err != nil {
		t.Fatal(err)
	}
	if result.Usage.InputTokens != 10 || result.Usage.OutputTokens != 2 || result.Usage.ProviderDetails["web_search_requests"] != 4 {
		t.Fatalf("usage = %#v, want valid core usage with fallback provider units", result.Usage)
	}
}

func TestGeminiProxyInvokeUnary(t *testing.T) {
	var body map[string]json.RawMessage
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1beta/models/gemini-route:generateContent" {
			t.Fatalf("path = %q, want /v1beta/models/gemini-route:generateContent", r.URL.Path)
		}
		if got := r.URL.Query().Get("key"); got != "" {
			t.Fatalf("key query = %q, want API key kept out of the URL", got)
		}
		if got := r.Header.Get("x-goog-api-key"); got != "test-key" {
			t.Fatalf("x-goog-api-key = %q, want test-key", got)
		}
		if got := r.Header.Get("x-goog-api-client"); got == "" {
			t.Fatal("x-goog-api-client is empty")
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"responseId":"resp_1","candidates":[],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":2,"totalTokenCount":7,"cachedContentTokenCount":1,"toolUsePromptTokenCount":1,"thoughtsTokenCount":1}}`))
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.GeminiAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
		Route: &gateway.Route{
			Provider:   "gemini",
			BaseURL:    upstream.URL,
			APIVersion: "v1beta",
			Model:      "gemini-route",
			APIKey:     "test-key",
		},
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "public-model",
		RawBody:   json.RawMessage(`{"model":"public-model","contents":[{"role":"user","parts":[{"text":"ping"}]}]}`),
		Meta:      gateway.Meta{RequestID: "req_gemini"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := body["model"]; ok {
		t.Fatalf("body = %s, model should be stripped for gemini proxy", mustMarshalJSON(t, body))
	}
	if result.Usage.InputTokens != 5 || result.Usage.OutputTokens != 2 || result.Usage.TotalTokens != 7 {
		t.Fatalf("usage = %#v, want input=5 output=2 total=7", result.Usage)
	}
	if result.Usage.CacheReadTokens != 1 {
		t.Fatalf("usage = %#v, want cache_read_tokens=1", result.Usage)
	}
	if result.Usage.InputDetails == nil || result.Usage.InputDetails.ToolTokens != 1 {
		t.Fatalf("usage input details = %#v, want tool tokens", result.Usage.InputDetails)
	}
	if result.Usage.OutputDetails == nil || result.Usage.OutputDetails.ReasoningTokens != 1 {
		t.Fatalf("usage output details = %#v, want reasoning tokens", result.Usage.OutputDetails)
	}
}

func TestGeminiProxyValidatesUnaryResponseShape(t *testing.T) {
	tests := []struct {
		name      string
		operation gateway.Operation
		response  string
		wantError bool
	}{
		{name: "candidates array", operation: gateway.OpChatCompletions, response: `{"candidates":[]}`},
		{name: "documented blocked prompt", operation: gateway.OpChatCompletions, response: `{"promptFeedback":{"blockReason":"SAFETY","blockReasonMessage":"blocked"}}`},
		{name: "null candidates", operation: gateway.OpChatCompletions, response: `{"candidates":null}`, wantError: true},
		{name: "null prompt feedback", operation: gateway.OpChatCompletions, response: `{"promptFeedback":null}`, wantError: true},
		{name: "unblocked prompt feedback without candidates", operation: gateway.OpChatCompletions, response: `{"promptFeedback":{"blockReasonMessage":"not blocked"}}`, wantError: true},
		{name: "null candidate", operation: gateway.OpChatCompletions, response: `{"candidates":[null]}`, wantError: true},
		{name: "embedding vector", operation: gateway.OpEmbeddings, response: `{"embedding":{"values":[0.1,-0.2,3]}}`},
		{name: "null embedding", operation: gateway.OpEmbeddings, response: `{"embedding":null}`, wantError: true},
		{name: "missing embedding values", operation: gateway.OpEmbeddings, response: `{"embedding":{}}`, wantError: true},
		{name: "null embedding values", operation: gateway.OpEmbeddings, response: `{"embedding":{"values":null}}`, wantError: true},
		{name: "empty embedding vector", operation: gateway.OpEmbeddings, response: `{"embedding":{"values":[]}}`, wantError: true},
		{name: "non-numeric embedding value", operation: gateway.OpEmbeddings, response: `{"embedding":{"values":[null]}}`, wantError: true},
		{name: "batch shape on unary operation", operation: gateway.OpEmbeddings, response: `{"embeddings":[{"values":[0.1]}]}`, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tt.response)
			}))
			defer upstream.Close()

			requestBody := json.RawMessage(`{"contents":[{"parts":[{"text":"ping"}]}]}`)
			if tt.operation == gateway.OpEmbeddings {
				requestBody = json.RawMessage(`{"content":{"parts":[{"text":"ping"}]}}`)
			}
			_, err := proxy.NewProvider(proxy.GeminiAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
				Provider: "gemini", BaseURL: upstream.URL, APIVersion: "v1beta", Model: "gemini-test",
			}}, &gateway.Request{Operation: tt.operation, RawBody: requestBody})
			if tt.wantError {
				if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Code != "invalid_upstream_response" {
					t.Fatalf("Invoke() error = %#v, want invalid_upstream_response", apiErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGeminiSearchUsageFollowsModelBillingUnit(t *testing.T) {
	tests := []struct {
		name            string
		routeModel      string
		responseVersion string
		queries         []string
		want            int64
	}{
		{
			name:            "publisher-qualified Gemini 2.5 is one grounded prompt",
			routeModel:      "public-alias",
			responseVersion: "publishers/google/models/gemini-2.5-flash-001",
			queries:         []string{"first search", "second search"},
			want:            1,
		},
		{
			name:            "response model version selects Gemini 3 query billing",
			routeModel:      "public-alias",
			responseVersion: "gemini-3.1-flash",
			queries:         []string{"first search", "second search", "first search", "   "},
			want:            2,
		},
		{
			name:       "unknown model conservatively retains query count",
			routeModel: "custom-gemini-2.5-route",
			queries:    []string{"first search", "second search"},
			want:       2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"modelVersion": tt.responseVersion,
					"candidates": []any{map[string]any{
						"groundingMetadata": map[string]any{"webSearchQueries": tt.queries},
					}},
				})
			}))
			defer upstream.Close()

			result, err := proxy.NewProvider(proxy.GeminiAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
				Route: &gateway.Route{Provider: "gemini", BaseURL: upstream.URL, APIVersion: "v1beta", Model: tt.routeModel},
			}, &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     tt.routeModel,
				RawBody:   json.RawMessage(`{"contents":[{"parts":[{"text":"ping"}]}]}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := result.Usage.ProviderDetails["google_search_requests"]; got != tt.want {
				t.Fatalf("google search billing units = %d, want %d (usage=%#v)", got, tt.want, result.Usage)
			}
		})
	}
}

func TestGeminiProxyPreservesIngressAPIVersion(t *testing.T) {
	for _, version := range []string{"v1", "v1beta"} {
		t.Run(version, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				want := "/" + version + "/models/gemini-route:generateContent"
				if r.URL.Path != want {
					t.Fatalf("path = %q, want %q", r.URL.Path, want)
				}
				_, _ = io.WriteString(w, `{"candidates":[],"usageMetadata":{"totalTokenCount":1}}`)
			}))
			defer upstream.Close()
			provider := proxy.NewProvider(proxy.GeminiAdapter(), upstream.Client())
			_, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
				Provider: "gemini", BaseURL: upstream.URL, Model: "gemini-route", APIVersion: "route-default-must-not-override-ingress",
			}}, &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Hints:     gateway.RequestHints{APIVersion: version},
				RawBody:   json.RawMessage(`{"contents":[]}`),
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGeminiProxyInvokeStream(t *testing.T) {
	wantBody := "data: {\"candidates\":[{\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":4,\"candidatesTokenCount\":2,\"totalTokenCount\":6}}\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1beta/models/gemini-real:streamGenerateContent" {
			t.Fatalf("path = %q, want streamGenerateContent", r.URL.Path)
		}
		if r.URL.Query().Get("alt") != "sse" {
			t.Fatalf("alt = %q, want sse", r.URL.Query().Get("alt"))
		}
		if r.Header.Get("x-goog-api-key") != "test-key" {
			t.Fatal("missing Gemini header credential")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, wantBody)
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.GeminiAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "gemini", BaseURL: upstream.URL, APIVersion: "v1beta", Model: "public-gemini", UpstreamModel: "gemini-real", APIKey: "test-key",
	}}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Stream:    true,
		RawBody:   json.RawMessage(`{"model":"public-gemini","contents":[{"parts":[{"text":"ping"}]}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(result.RawStream)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != wantBody {
		t.Fatalf("body = %q, want raw SSE passthrough", body)
	}
	if usage := result.FinalUsage(); usage.InputTokens != 4 || usage.OutputTokens != 2 || usage.TotalTokens != 6 {
		t.Fatalf("usage = %#v, want Gemini stream usage", usage)
	}
}

func TestGeminiStreamRequiresEveryRequestedCandidateToFinish(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantError bool
	}{
		{
			name:      "one of two candidates is missing",
			body:      "data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n",
			wantError: true,
		},
		{
			name: "both candidates finish",
			body: "data: {\"candidates\":[{\"index\":0,\"finishReason\":\"STOP\"}]}\n\n" +
				"data: {\"candidates\":[{\"index\":1,\"finishReason\":\"MAX_TOKENS\"}]}\n\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tt.body)
			}))
			defer upstream.Close()

			result, err := proxy.NewProvider(proxy.GeminiAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
				Provider: "gemini", BaseURL: upstream.URL, APIVersion: "v1beta", Model: "gemini-2.5-flash",
			}}, &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Stream:    true,
				Hints:     gateway.RequestHints{OutputMultiplicity: 2},
				RawBody:   json.RawMessage(`{"contents":[{"parts":[{"text":"ping"}]}],"generationConfig":{"candidateCount":2}}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer result.RawStream.Close()
			body, readErr := io.ReadAll(result.RawStream)
			if string(body) != tt.body {
				t.Fatalf("stream body = %q, want %q", body, tt.body)
			}
			if tt.wantError {
				if apiErr := gateway.AsAPIError(readErr); readErr == nil || apiErr.Code != "truncated_stream" {
					t.Fatalf("stream read error = %#v, want truncated_stream", apiErr)
				}
				return
			}
			if readErr != nil {
				t.Fatalf("complete stream read error = %v", readErr)
			}
		})
	}
}

func TestOpenAIProxyInvokeStream(t *testing.T) {
	wantBody := "data: {\"id\":\"chat_1\"}\n\ndata: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\ndata: [DONE]\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q, want /v1/chat/completions", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(wantBody))
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
		Route: &gateway.Route{
			Provider: "openai",
			BaseURL:  upstream.URL + "/v1",
			Model:    "route-model",
		},
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "public-model",
		Stream:    true,
		RawBody:   json.RawMessage(`{"model":"public-model","stream":true,"messages":[{"role":"user","content":"ping"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RawStream == nil {
		t.Fatal("RawStream is nil")
	}
	defer result.RawStream.Close()

	body, err := io.ReadAll(result.RawStream)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != wantBody {
		t.Fatalf("stream body = %q, want byte-for-byte passthrough %q", body, wantBody)
	}
	if !strings.Contains(result.ContentType, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream", result.ContentType)
	}
	if usage := result.FinalUsage(); usage.InputTokens != 5 || usage.OutputTokens != 3 || usage.TotalTokens != 8 {
		t.Fatalf("final usage = %#v, want input=5 output=3 total=8", usage)
	}
}

func TestProviderStreamsRejectEOFWithoutTerminalEvent(t *testing.T) {
	tests := []struct {
		name       string
		provider   string
		newAdapter func() proxy.Adapter
		body       string
		rawBody    json.RawMessage
	}{
		{
			name:       "openai",
			provider:   "openai",
			newAdapter: proxy.OpenAIAdapter,
			body:       "data: {\"id\":\"chat_1\",\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
			rawBody:    json.RawMessage(`{"model":"model","stream":true,"messages":[{"role":"user","content":"ping"}]}`),
		},
		{
			name:       "anthropic",
			provider:   "anthropic",
			newAdapter: proxy.AnthropicAdapter,
			body:       "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n",
			rawBody:    json.RawMessage(`{"model":"model","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"ping"}]}`),
		},
		{
			name:       "gemini",
			provider:   "gemini",
			newAdapter: proxy.GeminiAdapter,
			body:       "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"partial\"}]}}]}\n\n",
			rawBody:    json.RawMessage(`{"contents":[{"parts":[{"text":"ping"}]}]}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tt.body)
			}))
			defer upstream.Close()

			route := &gateway.Route{Provider: tt.provider, BaseURL: upstream.URL, Model: "model"}
			if tt.provider == "openai" || tt.provider == "anthropic" {
				route.BaseURL += "/v1"
			}
			if tt.provider == "gemini" {
				route.APIVersion = "v1beta"
			}
			result, err := proxy.NewProvider(tt.newAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{Route: route}, &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "model",
				Stream:    true,
				RawBody:   tt.rawBody,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer result.RawStream.Close()

			body, err := io.ReadAll(result.RawStream)
			if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Code != "truncated_stream" {
				t.Fatalf("stream read error = %#v, want truncated_stream", apiErr)
			}
			if string(body) != tt.body {
				t.Fatalf("partial stream body = %q, want byte-for-byte passthrough %q", body, tt.body)
			}
		})
	}
}

func TestProviderBoundsStreamingResponseBody(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: "+strings.Repeat("x", 256)+"\n\n")
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "route-model",
		Limits: gateway.LimitConfig{MaxResponseBytes: 64},
	}}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Stream:    true,
		RawBody:   json.RawMessage(`{"model":"route-model","stream":true,"messages":[{"role":"user","content":"ping"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(result.RawStream)
	if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Code != "response_too_large" {
		t.Fatalf("stream read error = %#v, want response_too_large", apiErr)
	}
	if len(body) != 64 {
		t.Fatalf("stream bytes before rejection = %d, want configured limit 64", len(body))
	}
}

func TestResponseLimitErrorsDoNotOpenCircuit(t *testing.T) {
	var calls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, strings.Repeat("x", 256))
	}))
	defer upstream.Close()

	route := &gateway.Route{
		Name:     "bounded",
		Provider: "openai",
		BaseURL:  upstream.URL + "/v1",
		Model:    "alias",
		Timeout:  gateway.Duration{Duration: time.Second},
		Limits:   gateway.LimitConfig{MaxResponseBytes: 64},
		Circuit: gateway.CircuitConfig{
			Failures: 1,
			Cooldown: gateway.Duration{Duration: time.Minute},
		},
		Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
	}
	breaker := policy.NewBreaker()
	engine := gateway.NewEngine(
		gateway.NewConfigStore(&gateway.Snapshot{Routes: map[string]*gateway.Route{"bounded": route}}),
		[]gateway.Provider{proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())},
		nil,
		[]gateway.AttemptInterceptor{&policy.AttemptLimits{Breakers: breaker}},
	)

	for attempt := 1; attempt <= 3; attempt++ {
		_, err := engine.Execute(context.Background(), &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "alias",
			RawBody:   json.RawMessage(`{"model":"alias","messages":[{"role":"user","content":"ping"}]}`),
		})
		if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Code != "response_too_large" {
			t.Fatalf("attempt %d error = %#v, want response_too_large", attempt, apiErr)
		}
		if allowed, _ := breaker.AllowAttempt(route); !allowed {
			t.Fatalf("route circuit opened after response-cap error on attempt %d", attempt)
		}
		if calls != attempt {
			t.Fatalf("upstream calls after attempt %d = %d, want %d", attempt, calls, attempt)
		}
	}
}

func TestOversizedStreamFallsBackBeforeClientBytes(t *testing.T) {
	var oversizedCalls, fallbackCalls int
	oversized := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		oversizedCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: "+strings.Repeat("x", 256)+"\n\n")
	}))
	defer oversized.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: fallback\n\ndata: [DONE]\n\n")
	}))
	defer fallback.Close()

	capability := gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}, Streaming: true}
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"oversized": {
			Name: "oversized", Provider: "openai", BaseURL: oversized.URL + "/v1", Model: "alias", Priority: 2,
			Capabilities: capability, Limits: gateway.LimitConfig{MaxResponseBytes: 64},
		},
		"fallback": {
			Name: "fallback", Provider: "openai", BaseURL: fallback.URL + "/v1", Model: "alias", Priority: 1,
			Capabilities: capability,
		},
	}}
	engine := gateway.NewEngine(gateway.NewConfigStore(snapshot), []gateway.Provider{proxy.NewProvider(proxy.OpenAIAdapter(), http.DefaultClient)}, nil, nil)
	exec, err := engine.Execute(context.Background(), &gateway.Request{
		Operation: gateway.OpChatCompletions, Model: "alias", Stream: true,
		RawBody: json.RawMessage(`{"model":"alias","stream":true,"messages":[{"role":"user","content":"ping"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer exec.Result.RawStream.Close()
	body, err := io.ReadAll(exec.Result.RawStream)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "data: fallback\n\ndata: [DONE]\n\n" || exec.Attempt.Route.Route.Name != "fallback" {
		t.Fatalf("selected route/body = %q/%q, want clean fallback", exec.Attempt.Route.Route.Name, body)
	}
	if oversizedCalls != 1 || fallbackCalls != 1 {
		t.Fatalf("upstream calls = %d/%d, want 1/1", oversizedCalls, fallbackCalls)
	}
}

func TestOpenAIResponsesStreamExtractsNestedUsage(t *testing.T) {
	wantBody := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":9,\"output_tokens\":2,\"total_tokens\":11}}}\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, wantBody)
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai",
		BaseURL:  upstream.URL + "/v1",
		Model:    "route-model",
	}}, &gateway.Request{
		Operation: gateway.OpResponses,
		Stream:    true,
		RawBody:   json.RawMessage(`{"model":"public-model","stream":true,"input":"ping"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(result.RawStream)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != wantBody {
		t.Fatalf("stream body = %q, want %q", body, wantBody)
	}
	if usage := result.FinalUsage(); usage.InputTokens != 9 || usage.OutputTokens != 2 || usage.TotalTokens != 11 {
		t.Fatalf("final usage = %#v, want input=9 output=2 total=11", usage)
	}
}

func TestStreamWithoutUsageUsesRouteTokenizerEstimate(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"id\":\"chat_1\"}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
		Route:    &gateway.Route{Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "route-model"},
		Estimate: gateway.Usage{InputTokens: 31, OutputTokens: 7, TotalTokens: 38},
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Stream:    true,
		Hints:     gateway.RequestHints{EstimatedInputTokens: 1, MaxOutputTokens: 2},
		RawBody:   json.RawMessage(`{"model":"public-model","stream":true,"messages":[{"role":"user","content":"ping"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(result.RawStream); err != nil {
		t.Fatal(err)
	}
	if usage := result.FinalUsage(); usage.InputTokens != 31 || usage.OutputTokens != 7 || usage.TotalTokens != 38 {
		t.Fatalf("fallback usage = %#v, want route tokenizer estimate", usage)
	}
}

func TestFallbackUsageSaturatesTokenTotals(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"chat_saturated","choices":[]}`)
	}))
	defer upstream.Close()
	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())

	tests := []struct {
		name     string
		estimate gateway.Usage
		hints    gateway.RequestHints
	}{
		{
			name:     "route estimate",
			estimate: gateway.Usage{InputTokens: math.MaxInt64, OutputTokens: 1},
		},
		{
			name:  "request hints",
			hints: gateway.RequestHints{EstimatedInputTokens: math.MaxInt64, MaxOutputTokens: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
				Route:    &gateway.Route{Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "route-model"},
				Estimate: tt.estimate,
			}, &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Hints:     tt.hints,
				RawBody:   json.RawMessage(`{"model":"public-model","messages":[]}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Usage.InputTokens != math.MaxInt64 || result.Usage.OutputTokens != 1 || result.Usage.TotalTokens != math.MaxInt64 {
				t.Fatalf("fallback usage = %#v, want saturated total", result.Usage)
			}
		})
	}
}

func TestAnthropicStreamMergesStartAndDeltaUsage(t *testing.T) {
	wantBody := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":12,\"output_tokens\":1}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":6}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, wantBody)
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.AnthropicAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "anthropic",
		BaseURL:  upstream.URL + "/v1",
		Model:    "claude-route",
	}}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Stream:    true,
		RawBody:   json.RawMessage(`{"model":"public-model","stream":true,"messages":[{"role":"user","content":"ping"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(result.RawStream); err != nil {
		t.Fatal(err)
	}
	if usage := result.FinalUsage(); usage.InputTokens != 12 || usage.OutputTokens != 6 || usage.TotalTokens != 18 {
		t.Fatalf("final usage = %#v, want input=12 output=6 total=18", usage)
	}
}

func TestProviderRefusesRedirectWithoutForwardingCredentials(t *testing.T) {
	var leaked string
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		leaked = r.Header.Get("Authorization") + r.Header.Get("x-api-key") + r.Header.Get("x-goog-api-key")
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), redirect.Client())
	_, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai",
		BaseURL:  redirect.URL,
		Model:    "route-model",
		APIKey:   "secret-key",
	}}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"model":"x"}`)})
	if err == nil {
		t.Fatal("Invoke() error = nil, want redirect rejected")
	}
	if apiErr := gateway.AsAPIError(err); apiErr.Status != http.StatusBadGateway || apiErr.Code != "unexpected_upstream_status" {
		t.Fatalf("redirect error = %#v, want sanitized 502", apiErr)
	}
	if leaked != "" {
		t.Fatalf("credentials reached redirect target: %q", leaked)
	}
}

func TestProviderRejectsInformationalFinalStatus(t *testing.T) {
	provider := proxy.NewProvider(proxy.OpenAIAdapter(), &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 199,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("informational")),
			Request:    request,
		}, nil
	})})
	_, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai", BaseURL: "https://upstream.invalid/v1", Model: "route-model",
	}}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"model":"route-model"}`)})
	if apiErr := gateway.AsAPIError(err); err == nil || apiErr.Status != http.StatusBadGateway || apiErr.Code != "unexpected_upstream_status" {
		t.Fatalf("informational status error = %#v, want sanitized 502", apiErr)
	}
}

func TestProviderSanitizesTransportErrors(t *testing.T) {
	provider := proxy.NewProvider(proxy.OpenAIAdapter(), &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial https://secret.example/?token=credential-value failed")
	})})
	_, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai",
		BaseURL:  "https://secret.example/?token=credential-value",
		Model:    "route-model",
	}}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"model":"x"}`)})
	if err == nil {
		t.Fatal("Invoke() error = nil, want transport failure")
	}
	if strings.Contains(err.Error(), "credential-value") || strings.Contains(err.Error(), "secret.example") {
		t.Fatalf("transport error leaked request target: %q", err)
	}
}

func TestProviderHandlesStreamingErrorBodyReadFailure(t *testing.T) {
	provider := proxy.NewProvider(proxy.OpenAIAdapter(), &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       failingReadCloser{err: errors.New("secret upstream read detail")},
			Request:    request,
		}, nil
	})})
	_, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai", BaseURL: "https://upstream.invalid/v1", Model: "route-model",
	}}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Stream:    true,
		RawBody:   json.RawMessage(`{"model":"route-model","stream":true,"messages":[]}`),
	})
	apiErr := gateway.AsAPIError(err)
	if err == nil || apiErr.Status != http.StatusBadGateway || apiErr.Code != "read_failed" || !apiErr.Retryable || !apiErr.Fallback || !apiErr.CircuitFailure {
		t.Fatalf("stream error body read error = %#v, want retryable/fallback read_failed", apiErr)
	}
	if strings.Contains(err.Error(), "secret upstream") {
		t.Fatalf("stream error body read failure leaked cause: %q", err)
	}
}

func TestProviderBoundsUnaryResponseBody(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, strings.Repeat("x", (16<<20)+1))
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	_, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai",
		BaseURL:  upstream.URL + "/v1",
		Model:    "route-model",
	}}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"model":"x"}`)})
	if err == nil {
		t.Fatal("Invoke() error = nil, want oversized response rejected")
	}
	if apiErr := gateway.AsAPIError(err); apiErr.Code != "response_too_large" {
		t.Fatalf("api error = %#v, want response_too_large", apiErr)
	}
}

func TestProxy429WithoutUsageDoesNotChargeFailedAttempt(t *testing.T) {
	limited := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"busy","type":"rate_limit_error"}}`)
	}))
	defer limited.Close()
	success := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"chat_1","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
	}))
	defer success.Close()

	routeCapabilities := gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}}
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"limited": {
			Name: "limited", Provider: "openai", BaseURL: limited.URL + "/v1", Model: "alias", Priority: 2,
			Capabilities: routeCapabilities,
		},
		"success": {
			Name: "success", Provider: "openai", BaseURL: success.URL + "/v1", Model: "alias", Priority: 1,
			Capabilities: routeCapabilities,
		},
	}}
	engine := gateway.NewEngine(
		gateway.NewConfigStore(snapshot), []gateway.Provider{proxy.NewProvider(proxy.OpenAIAdapter(), limited.Client())},
		[]gateway.RequestInterceptor{fixedAttemptEstimate{}}, nil,
	)
	exec, err := engine.Execute(context.Background(), &gateway.Request{
		Operation: gateway.OpChatCompletions, Model: "alias",
		RawBody: json.RawMessage(`{"model":"alias","messages":[{"role":"user","content":"ping"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := exec.State.TotalAttemptUsage(); got.InputTokens != 2 || got.OutputTokens != 1 {
		t.Fatalf("attempt usage = %#v, want only successful fallback usage", got)
	}
}

func TestProxyPreservesSafeUpstreamErrorHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-Request-ID", "upstream-request")
		w.Header().Set("Request-ID", "anthropic-request")
		w.Header().Set("Set-Cookie", "secret=value")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"busy"}}`)
	}))
	defer upstream.Close()
	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	_, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "model",
	}}, &gateway.Request{Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"model":"model","messages":[]}`)})
	apiErr := gateway.AsAPIError(err)
	if err == nil || apiErr.Headers.Get("Retry-After") != "7" || apiErr.Headers.Get("X-RateLimit-Remaining") != "0" || apiErr.Headers.Get("X-Request-ID") != "upstream-request" || apiErr.Headers.Get("Request-ID") != "anthropic-request" {
		t.Fatalf("error headers = %#v, want safe retry/rate/request headers", apiErr.Headers)
	}
	if apiErr.Headers.Get("Set-Cookie") != "" {
		t.Fatalf("unsafe Set-Cookie header was retained: %#v", apiErr.Headers)
	}
}

func TestProxyStripsHeadersEchoedFromOutboundRequest(t *testing.T) {
	for _, stream := range []bool{false, true} {
		name := "unary"
		if stream {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("X-Tenant-Cred"); got != "configured-secret" {
					t.Fatalf("configured request header = %q, want configured-secret", got)
				}
				w.Header().Set("X-Tenant-Cred", r.Header.Get("X-Tenant-Cred"))
				w.Header().Set("Cache-Control", "no-store")
				w.Header().Set("X-Request-ID", r.Header.Get("X-Request-ID"))
				w.Header().Set("X-Provider-Region", "eu-test-1")
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: [DONE]\n\n")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"chat_echo","choices":[]}`)
			}))
			defer upstream.Close()

			result, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
				Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "model",
				Headers: map[string]string{"X-Tenant-Cred": "configured-secret", "Cache-Control": "max-age=0"},
			}}, &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Stream:    stream,
				Meta:      gateway.Meta{RequestID: "upstream-request"},
				RawBody:   json.RawMessage(`{"model":"model","messages":[]}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			if stream {
				defer result.RawStream.Close()
				if _, err := io.ReadAll(result.RawStream); err != nil {
					t.Fatal(err)
				}
			}
			if got := result.Headers.Get("X-Tenant-Cred"); got != "" {
				t.Fatalf("echoed configured credential escaped in response headers: %q", got)
			}
			if got := result.Headers.Get("Cache-Control"); got != "no-store" {
				t.Fatalf("response-semantic overlap = %q, want no-store", got)
			}
			if got := result.Headers.Get("X-Request-ID"); got != "upstream-request" {
				t.Fatalf("response request ID = %q, want upstream-request", got)
			}
			if got := result.Headers.Get("X-Provider-Region"); got != "eu-test-1" {
				t.Fatalf("ordinary provider metadata = %q, want eu-test-1", got)
			}
		})
	}
}

type fixedAttemptEstimate struct{}

func (fixedAttemptEstimate) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		candidates, err := state.ResolveCandidates()
		if err != nil {
			return nil, err
		}
		for i := range candidates {
			candidates[i].Estimate = gateway.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15}
		}
		state.ReplaceCandidates(candidates)
		return next(ctx, state)
	}
}

type fixedQuotaScope struct{ limit gateway.LimitSpec }

func (f fixedQuotaScope) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		state.Scopes = []gateway.ScopedLimit{{
			Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "test-key"},
			Limits: f.limit,
		}}
		return next(ctx, state)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type failingReadCloser struct {
	err error
}

func (r failingReadCloser) Read([]byte) (int, error) { return 0, r.err }

func (failingReadCloser) Close() error { return nil }

func TestGeminiBuildEffectiveUsesRequestHints(t *testing.T) {
	provider := proxy.NewProvider(proxy.GeminiAdapter(), nil)
	effective, err := provider.BuildEffective(gateway.ResolvedRoute{
		Route: &gateway.Route{
			Provider:   "gemini",
			BaseURL:    "https://example.invalid",
			APIVersion: "v1beta",
			Model:      "gemini-route",
		},
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "public-model",
		Hints:     gateway.RequestHints{MaxOutputTokens: 64},
		RawBody:   json.RawMessage(`{"model":"public-model","contents":[{"role":"user","parts":[{"text":"ping"}]}],"generationConfig":{"maxOutputTokens":64}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if effective.MaxOutputTokens != 64 {
		t.Fatalf("max output tokens = %d, want 64 from request hints", effective.MaxOutputTokens)
	}
}

func mustMarshalJSON(t *testing.T, value any) string {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return string(body)
}
