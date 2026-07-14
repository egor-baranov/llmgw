package proxy_test

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"llmgw/gateway"
	proxyproviders "llmgw/proxy/providers"
)

func TestProviderDecoderHandlesMaxInt64BodyLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{
		"model":"gpt-4o-mini","messages":[{"role":"user","content":"ping"}]
	}`))
	env, err := proxyproviders.OpenAIRequest(req, gateway.OpChatCompletions, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if env.Model != "gpt-4o-mini" {
		t.Fatalf("model = %q, want gpt-4o-mini", env.Model)
	}
}

func TestOpenAIRequestDecodesMinimalMetadata(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{
		"model":"gpt-4o-mini",
		"stream":true,
		"user":"alice",
		"metadata":{"project":"proj-1"},
		"max_tokens":120,
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

func TestOpenAIRequestReadsMaxCompletionTokens(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{
		"model":"gpt-4o-mini",
		"max_tokens":64,
		"max_completion_tokens":321,
		"messages":[{"role":"user","content":"ping"}]
	}`))
	env, err := proxyproviders.OpenAIRequest(req, gateway.OpChatCompletions, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if env.Hints.MaxOutputTokens != 321 {
		t.Fatalf("max output tokens = %d, want max_completion_tokens value 321", env.Hints.MaxOutputTokens)
	}
}

func TestOpenAIRequestUsesLargestMaxTokenAlias(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{
		"model":"gpt-4o-mini",
		"max_completion_tokens":1,
		"max_tokens":100000,
		"messages":[{"role":"user","content":"ping"}]
	}`))
	env, err := proxyproviders.OpenAIRequest(req, gateway.OpChatCompletions, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if env.Hints.MaxOutputTokens != 100000 {
		t.Fatalf("max output tokens = %d, want largest supplied alias 100000", env.Hints.MaxOutputTokens)
	}
}

func TestOpenAIRequestDecodesOutputMultiplicity(t *testing.T) {
	tests := []struct {
		name string
		op   gateway.Operation
		body string
		want int64
	}{
		{name: "chat n", op: gateway.OpChatCompletions, body: `{"model":"gpt-test","n":7,"messages":[{}]}`, want: 7},
		{name: "completion best_of", op: gateway.OpCompletions, body: `{"model":"gpt-test","n":2,"best_of":5,"prompt":"x"}`, want: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := proxyproviders.OpenAIRequest(httptest.NewRequest("POST", "/", strings.NewReader(tt.body)), tt.op, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if env.Hints.OutputMultiplicity != tt.want {
				t.Fatalf("output multiplicity = %d, want %d", env.Hints.OutputMultiplicity, tt.want)
			}
		})
	}
}

func TestOpenAIRequestRejectsInvalidOutputMultiplicity(t *testing.T) {
	for _, body := range []string{
		`{"model":"gpt-test","n":0,"messages":[{}]}`,
		`{"model":"gpt-test","n":"many","messages":[{}]}`,
	} {
		_, err := proxyproviders.OpenAIRequest(httptest.NewRequest("POST", "/", strings.NewReader(body)), gateway.OpChatCompletions, 1<<20)
		if gateway.AsAPIError(err).Code != "invalid_output_multiplicity" {
			t.Fatalf("error = %v, want invalid_output_multiplicity", err)
		}
	}
}

func TestProviderDecodersRejectNonBooleanStream(t *testing.T) {
	tests := []struct {
		name   string
		decode func(*http.Request) (*gateway.Request, error)
		body   string
	}{
		{
			name: "openai",
			decode: func(r *http.Request) (*gateway.Request, error) {
				return proxyproviders.OpenAIRequest(r, gateway.OpChatCompletions, 1<<20)
			},
			body: `{"model":"gpt-test","stream":"true","messages":[{}]}`,
		},
		{
			name: "anthropic",
			decode: func(r *http.Request) (*gateway.Request, error) {
				return proxyproviders.AnthropicRequest(r, 1<<20)
			},
			body: `{"model":"claude","stream":1,"max_tokens":10,"messages":[{}]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.decode(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body)))
			apiErr := gateway.AsAPIError(err)
			if err == nil || apiErr.Status != http.StatusBadRequest || apiErr.Code != "invalid_stream" || apiErr.Param != "stream" {
				t.Fatalf("decode error = %#v, want invalid_stream 400", apiErr)
			}
		})
	}
}

func TestGeminiDecoderDetectsEmptyHostedSearchDeclarations(t *testing.T) {
	for _, field := range []string{"googleSearch", "google_search", "googleSearchRetrieval", "google_search_retrieval"} {
		t.Run(field, func(t *testing.T) {
			body := `{"contents":[{}],"tools":[{"` + field + `":{}}]}`
			req := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini:generateContent?model=gemini", strings.NewReader(body))
			decoded, err := proxyproviders.GeminiGenerateRequest(req, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if len(decoded.Hints.ProviderUnits) != 1 || decoded.Hints.ProviderUnits[0] != "google_search_requests" {
				t.Fatalf("provider units = %#v, want google_search_requests", decoded.Hints.ProviderUnits)
			}
		})
	}
}

func TestDecodersDetectOnlyBillableCacheAndSearchControls(t *testing.T) {
	search, err := proxyproviders.OpenAIRequest(
		httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt","messages":[{}],"web_search_options":{}}`)),
		gateway.OpChatCompletions,
		1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(search.Hints.ProviderUnits) != 1 || search.Hints.ProviderUnits[0] != "web_search_requests" {
		t.Fatalf("web search provider units = %#v", search.Hints.ProviderUnits)
	}
	if !search.Hints.RequiresTools {
		t.Fatal("web_search_options did not require a tool-capable route")
	}

	for _, tt := range []struct {
		name string
		body string
		want bool
	}{
		{
			name: "real ephemeral cache control",
			body: `{"model":"claude","max_tokens":10,"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]}`,
			want: true,
		},
		{
			name: "automatic top-level cache control",
			body: `{"model":"claude","max_tokens":10,"messages":[{"role":"user","content":"hello"}],"cache_control":{"type":"ephemeral"}}`,
			want: true,
		},
		{
			name: "tool schema property only",
			body: `{"model":"claude","max_tokens":10,"messages":[{}],"tools":[{"name":"f","input_schema":{"type":"object","properties":{"cache_control":{"type":"string"}},"examples":[{"cache_control":{"type":"ephemeral"}}]}}]}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := proxyproviders.AnthropicRequest(httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(tt.body)), 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.Hints.MayWritePromptCache != tt.want {
				t.Fatalf("MayWritePromptCache = %t, want %t", decoded.Hints.MayWritePromptCache, tt.want)
			}
		})
	}
}

func TestProviderDecodersRejectInvalidRequiredCoreShapes(t *testing.T) {
	tests := []struct {
		name   string
		decode func(*http.Request) (*gateway.Request, error)
		body   string
		param  string
	}{
		{name: "chat messages scalar", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.OpenAIRequest(r, gateway.OpChatCompletions, 1<<20)
		}, body: `{"model":"gpt","messages":0}`, param: "messages"},
		{name: "chat messages empty", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.OpenAIRequest(r, gateway.OpChatCompletions, 1<<20)
		}, body: `{"model":"gpt","messages":[]}`, param: "messages"},
		{name: "responses input boolean", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.OpenAIRequest(r, gateway.OpResponses, 1<<20)
		}, body: `{"model":"gpt","input":true}`, param: "input"},
		{name: "completion prompt object", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.OpenAIRequest(r, gateway.OpCompletions, 1<<20)
		}, body: `{"model":"gpt","prompt":{}}`, param: "prompt"},
		{name: "embedding input object", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.OpenAIRequest(r, gateway.OpEmbeddings, 1<<20)
		}, body: `{"model":"embed","input":{}}`, param: "input"},
		{name: "anthropic messages object", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.AnthropicRequest(r, 1<<20)
		}, body: `{"model":"claude","max_tokens":10,"messages":{}}`, param: "messages"},
		{name: "anthropic max tokens string", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.AnthropicRequest(r, 1<<20)
		}, body: `{"model":"claude","max_tokens":"10","messages":[{}]}`, param: "max_tokens"},
		{name: "anthropic max tokens zero", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.AnthropicRequest(r, 1<<20)
		}, body: `{"model":"claude","max_tokens":0,"messages":[{}]}`, param: "max_tokens"},
		{name: "gemini contents object", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.GeminiGenerateRequest(r, 1<<20)
		}, body: `{"contents":{}}`, param: "contents"},
		{name: "gemini embedding content string", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.GeminiEmbeddingRequest(r, 1<<20)
		}, body: `{"content":"text"}`, param: "content"},
		{name: "openai max tokens string", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.OpenAIRequest(r, gateway.OpChatCompletions, 1<<20)
		}, body: `{"model":"gpt","messages":[{}],"max_tokens":"10"}`, param: "max_tokens"},
		{name: "openai max output zero", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.OpenAIRequest(r, gateway.OpResponses, 1<<20)
		}, body: `{"model":"gpt","input":"x","max_output_tokens":0}`, param: "max_output_tokens"},
		{name: "gemini max output negative", decode: func(r *http.Request) (*gateway.Request, error) {
			return proxyproviders.GeminiGenerateRequest(r, 1<<20)
		}, body: `{"contents":[{}],"generationConfig":{"maxOutputTokens":-1}}`, param: "generationConfig.maxOutputTokens"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/?model=gemini", strings.NewReader(tt.body))
			_, err := tt.decode(req)
			apiErr := gateway.AsAPIError(err)
			if err == nil || apiErr.Status != http.StatusBadRequest || apiErr.Param != tt.param {
				t.Fatalf("decode error = %#v, want parameter %q 400", apiErr, tt.param)
			}
		})
	}
}

func TestOpenAIRequestDetectsRequiredCapabilities(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{
		"model":"gpt-test",
		"tools":[{"type":"function","name":"lookup"}],
		"text":{"format":{"type":"json_schema","name":"answer","schema":{"type":"object"}}},
		"reasoning":{"effort":"medium"},
		"modalities":["text","audio"],
		"input":[{"role":"user","content":[{"type":"input_text","text":"describe"},{"type":"input_image","image_url":"https://example.test/image.png"}]}]
	}`))
	env, err := proxyproviders.OpenAIRequest(req, gateway.OpResponses, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	hints := env.Hints
	if !hints.RequiresTools || !hints.RequiresStructuredOutput || !hints.RequiresVision || !hints.RequiresAudio || !hints.RequiresReasoning {
		t.Fatalf("required capabilities = %#v, want all feature flags", hints)
	}
	if hints.VisionInputParts != 1 || hints.AudioInputParts != 0 {
		t.Fatalf("multimodal input counts = vision:%d audio:%d, want 1/0; output audio modality is not an input part", hints.VisionInputParts, hints.AudioInputParts)
	}
}

func TestOpenAIRequestCountsDistinctRemoteMultimodalParts(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{
		"model":"gpt-test",
		"input":[{"role":"user","content":[
			{"type":"input_image","image_url":"https://example.test/one.png"},
			{"type":"input_image","image_url":{"url":"https://example.test/two.png"}},
			{"type":"input_audio","audio_url":"https://example.test/one.mp3"},
			{"type":"input_audio","input_audio":{"url":"https://example.test/two.mp3"}}
		]}]
	}`))
	env, err := proxyproviders.OpenAIRequest(req, gateway.OpResponses, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if env.Hints.VisionInputParts != 2 || env.Hints.AudioInputParts != 2 {
		t.Fatalf("multimodal input counts = vision:%d audio:%d, want 2/2", env.Hints.VisionInputParts, env.Hints.AudioInputParts)
	}
}

func TestOpenAIRequestDetectsToolHistoryWithoutToolDeclaration(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{
		"model":"gpt-test",
		"input":[{"type":"function_call_output","call_id":"call_1","output":"done"}]
	}`))
	env, err := proxyproviders.OpenAIRequest(req, gateway.OpResponses, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !env.Hints.RequiresTools {
		t.Fatalf("required capabilities = %#v, want tool history detected", env.Hints)
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

func TestAnthropicRequestReadsMetadataUserID(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{
		"model":"claude-test","max_tokens":64,
		"metadata":{"user_id":"anthropic-user"},
		"messages":[{"role":"user","content":"ping"}]
	}`))
	env, err := proxyproviders.AnthropicRequest(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if env.Hints.User != "anthropic-user" {
		t.Fatalf("user = %q, want metadata.user_id", env.Hints.User)
	}
}

func TestAnthropicRequestDetectsToolHistory(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{
		"model":"claude-test","max_tokens":64,
		"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool_1","content":"done"}]}]
	}`))
	env, err := proxyproviders.AnthropicRequest(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !env.Hints.RequiresTools {
		t.Fatalf("required capabilities = %#v, want Anthropic tool history detected", env.Hints)
	}
}

func TestAnthropicRequestCountsNestedSourceOnce(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{
		"model":"claude-test","max_tokens":64,
		"messages":[{"role":"user","content":[{"type":"image","source":{"type":"url","media_type":"image/png","url":"https://example.test/image.png"}}]}]
	}`))
	env, err := proxyproviders.AnthropicRequest(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if env.Hints.VisionInputParts != 1 {
		t.Fatalf("vision input parts = %d, want nested source counted once", env.Hints.VisionInputParts)
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

func TestGeminiGenerateRequestDetectsStreamingEndpoint(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1beta/models/gemini-2.5-flash:streamGenerateContent?model=gemini-2.5-flash&alt=sse", strings.NewReader(`{
		"contents":[{"role":"user","parts":[{"text":"ping"}]}]
	}`))
	env, err := proxyproviders.GeminiGenerateRequest(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !env.Stream {
		t.Fatal("stream = false, want true for streamGenerateContent")
	}
}

func TestGeminiGenerateRequestDoesNotPromoteUnaryAltSSE(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1beta/models/gemini-2.5-flash:generateContent?model=gemini-2.5-flash&alt=sse", strings.NewReader(`{
		"contents":[{"role":"user","parts":[{"text":"ping"}]}]
	}`))
	env, err := proxyproviders.GeminiGenerateRequest(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if env.Stream {
		t.Fatal("stream = true, want unary generateContent to remain non-streaming")
	}
}

func TestGeminiGenerateRequestDecodesCandidateCount(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1beta/models/gemini-test:generateContent?model=gemini-test", strings.NewReader(`{
		"generationConfig":{"candidateCount":4},"contents":[{"parts":[{"text":"ping"}]}]
	}`))
	env, err := proxyproviders.GeminiGenerateRequest(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if env.Hints.OutputMultiplicity != 4 {
		t.Fatalf("output multiplicity = %d, want 4", env.Hints.OutputMultiplicity)
	}
}

func TestGeminiGenerateRequestDetectsFunctionResponseHistory(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1beta/models/gemini-test:generateContent?model=gemini-test", strings.NewReader(`{
		"contents":[{"role":"user","parts":[{"functionResponse":{"name":"lookup","response":{"value":"done"}}}]}]
	}`))
	env, err := proxyproviders.GeminiGenerateRequest(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !env.Hints.RequiresTools {
		t.Fatalf("required capabilities = %#v, want Gemini function response detected", env.Hints)
	}
}

func TestGeminiGenerateRequestCountsMultimodalPartsByMIMEType(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1beta/models/gemini-test:generateContent?model=gemini-test", strings.NewReader(`{
		"contents":[{"role":"user","parts":[
			{"fileData":{"mimeType":"image/png","fileUri":"https://example.test/one.png"}},
			{"inlineData":{"mimeType":"image/jpeg","data":"abc"}},
			{"fileData":{"mimeType":"audio/mpeg","fileUri":"https://example.test/one.mp3"}},
			{"inlineData":{"mimeType":"audio/wav","data":"def"}}
		]}]
	}`))
	env, err := proxyproviders.GeminiGenerateRequest(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if env.Hints.VisionInputParts != 2 || env.Hints.AudioInputParts != 2 {
		t.Fatalf("multimodal input counts = vision:%d audio:%d, want 2/2", env.Hints.VisionInputParts, env.Hints.AudioInputParts)
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
