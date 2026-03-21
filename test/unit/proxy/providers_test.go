package proxy_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"llmgw/gateway"
	proxyproviders "llmgw/proxy/providers"
)

func TestOpenAIRequestDecodesMinimalMetadata(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{
		"model":"gpt-4o-mini",
		"stream":true,
		"user":"alice",
		"metadata":{"project":"proj-1"},
		"max_tokens":"120",
		"messages":[{"role":"user","content":"ping"}]
	}`))
	req.Header.Set("Content-Type", "application/json")

	env, err := proxyproviders.OpenAIRequest(req, gateway.OpChatCompletions, 1<<20)
	if err != nil {
		t.Fatalf("OpenAIRequest() error = %v, want nil", err)
	}
	if env.Provider != "openai" {
		t.Fatalf("provider = %q, want openai", env.Provider)
	}
	if env.Operation != gateway.OpChatCompletions {
		t.Fatalf("operation = %q, want chat.completions", env.Operation)
	}
	if env.Model != "gpt-4o-mini" {
		t.Fatalf("model = %q, want gpt-4o-mini", env.Model)
	}
	if !env.Stream {
		t.Fatal("stream = false, want true")
	}
	if env.Hints.User != "alice" {
		t.Fatalf("user = %q, want alice", env.Hints.User)
	}
	if env.Hints.Metadata["project"] != "proj-1" {
		t.Fatalf("metadata project = %q, want proj-1", env.Hints.Metadata["project"])
	}
	if env.Hints.MaxOutputTokens != 120 {
		t.Fatalf("max output tokens = %d, want 120", env.Hints.MaxOutputTokens)
	}
	if env.Hints.EstimatedInputTokens <= 0 {
		t.Fatalf("estimated input tokens = %d, want > 0", env.Hints.EstimatedInputTokens)
	}
	if env.Hints.PromptText != "" {
		t.Fatalf("prompt text = %q, want empty for minimal decoder", env.Hints.PromptText)
	}
}

func TestAnthropicRequestAllowsModelQueryFallback(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/messages?model=claude-3-7-sonnet", strings.NewReader(`{
		"messages":[{"role":"user","content":"ping"}],
		"max_tokens":64
	}`))

	env, err := proxyproviders.AnthropicRequest(req, 1<<20)
	if err != nil {
		t.Fatalf("AnthropicRequest() error = %v, want nil", err)
	}
	if env.Model != "claude-3-7-sonnet" {
		t.Fatalf("model = %q, want query fallback model", env.Model)
	}
	if env.Hints.MaxOutputTokens != 64 {
		t.Fatalf("max output tokens = %d, want 64", env.Hints.MaxOutputTokens)
	}
}

func TestGeminiGenerateRequestReadsNestedMaxOutputTokens(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1beta/models/gemini-2.5-flash:generateContent?model=gemini-2.5-flash", strings.NewReader(`{
		"generationConfig":{"maxOutputTokens":77},
		"contents":[{"role":"user","parts":[{"text":"ping"}]}]
	}`))

	env, err := proxyproviders.GeminiGenerateRequest(req, 1<<20)
	if err != nil {
		t.Fatalf("GeminiGenerateRequest() error = %v, want nil", err)
	}
	if env.Hints.MaxOutputTokens != 77 {
		t.Fatalf("max output tokens = %d, want 77", env.Hints.MaxOutputTokens)
	}
	if env.Model != "gemini-2.5-flash" {
		t.Fatalf("model = %q, want query fallback model", env.Model)
	}
}

func TestGeminiEmbeddingRequestAllowsModelQueryFallback(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1beta/models/gemini-2.5-flash:embedContent?model=gemini-2.5-flash", strings.NewReader(`{
		"content":{"parts":[{"text":"x"}]}
	}`))

	env, err := proxyproviders.GeminiEmbeddingRequest(req, 1<<20)
	if err != nil {
		t.Fatalf("GeminiEmbeddingRequest() error = %v, want nil", err)
	}
	if env.Model != "gemini-2.5-flash" {
		t.Fatalf("model = %q, want query fallback model", env.Model)
	}
}

func TestGeminiEmbeddingRequestRequiresModel(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1beta/models/gemini-2.5-flash:embedContent", strings.NewReader(`{"content":{"parts":[{"text":"x"}]}}`))

	_, err := proxyproviders.GeminiEmbeddingRequest(req, 1<<20)
	if err == nil {
		t.Fatal("GeminiEmbeddingRequest() error = nil, want missing model")
	}
	apiErr := gateway.AsAPIError(err)
	if apiErr.Code != "missing_model" {
		t.Fatalf("api error code = %q, want missing_model", apiErr.Code)
	}
}
