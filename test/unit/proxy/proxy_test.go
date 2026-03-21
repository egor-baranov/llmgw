package proxy_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"llmgw/gateway"
	"llmgw/proxy"
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

	provider := proxy.New("openai", upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{
		Route: &gateway.Route{
			Name:     "openai-primary",
			Provider: "openai",
			BaseURL:  upstream.URL + "/v1",
			Model:    "route-model",
			APIKey:   "test-key",
			Headers:  map[string]string{"X-Static": "static"},
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

	provider := proxy.New("anthropic", upstream.Client())
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
		Meta:      gateway.Meta{RequestID: "req_anthropic"},
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

func TestGeminiProxyInvokeUnary(t *testing.T) {
	var body map[string]json.RawMessage
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1beta/models/gemini-route:generateContent" {
			t.Fatalf("path = %q, want /v1beta/models/gemini-route:generateContent", r.URL.Path)
		}
		if got := r.URL.Query().Get("key"); got != "test-key" {
			t.Fatalf("key query = %q, want test-key", got)
		}
		if got := r.Header.Get("x-goog-api-client"); got == "" {
			t.Fatal("x-goog-api-client is empty")
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"responseId":"resp_1","usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":2,"totalTokenCount":7,"cachedContentTokenCount":1,"toolUsePromptTokenCount":1,"thoughtsTokenCount":1}}`))
	}))
	defer upstream.Close()

	provider := proxy.New("gemini", upstream.Client())
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

func TestOpenAIProxyInvokeStream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q, want /v1/chat/completions", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"id\":\"chat_1\"}\n\ndata: [DONE]\n\n"))
	}))
	defer upstream.Close()

	provider := proxy.New("openai", upstream.Client())
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
	if !strings.Contains(string(body), "data: [DONE]") {
		t.Fatalf("stream body = %s, want passthrough SSE payload", body)
	}
	if !strings.Contains(result.ContentType, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream", result.ContentType)
	}
	if result.Usage.TotalTokens == 0 {
		t.Fatalf("usage = %#v, want fallback estimate", result.Usage)
	}
}

func TestGeminiBuildEffectiveUsesRequestHints(t *testing.T) {
	provider := proxy.New("gemini", nil)
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
