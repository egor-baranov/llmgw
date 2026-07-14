package proxy_test

import (
	"bytes"
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

var (
	_ gateway.Provider               = (*proxy.Provider)(nil)
	_ gateway.ProviderRouteValidator = (*proxy.Provider)(nil)
	_ gateway.ProviderBridgePlanner  = (*proxy.Provider)(nil)
	_ gateway.TokenProjector         = (*proxy.Provider)(nil)
)

func TestExplicitAdapterRequiresNoCoreProviderSwitch(t *testing.T) {
	const unaryResponse = `{"ok":true,"tokens":3}`
	const streamResponse = "data: {\"tokens\":7}\n\ndata: {\"done\":true}\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/invoke" {
			t.Fatalf("path = %q, want /invoke", r.URL.Path)
		}
		if r.Header.Get("X-Fake-Auth") != "configured" || r.Header.Get("X-Fake-Feature") != "forwarded" {
			t.Fatalf("adapter headers = %#v", r.Header)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		var request struct {
			Stream bool `json:"stream"`
		}
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		if request.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, streamResponse)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, unaryResponse)
	}))
	defer upstream.Close()

	extractUsage := func(_ gateway.ResolvedRoute, _ *gateway.Request, data []byte) gateway.Usage {
		var event struct {
			Tokens int64 `json:"tokens"`
		}
		_ = json.Unmarshal(data, &event)
		return gateway.Usage{InputTokens: event.Tokens, TotalTokens: event.Tokens}
	}
	operations := map[gateway.Operation]proxy.OperationAdapter{
		gateway.OpChatCompletions: {
			Prepare: func(_ gateway.ResolvedRoute, request *gateway.Request) (proxy.PreparedRequest, error) {
				return proxy.PreparedRequest{URL: upstream.URL + "/invoke", Body: append([]byte(nil), request.RawBody...)}, nil
			},
			ValidateResponse: func(data []byte) error {
				if !bytes.Equal(data, []byte(unaryResponse)) {
					t.Fatalf("validated response = %s", data)
				}
				return nil
			},
		},
	}
	adapter := proxy.Adapter{
		Name:       "fake-provider",
		Operations: operations,
		ApplyAuth: func(request *http.Request, _ *gateway.Route) error {
			request.Header.Set("X-Fake-Auth", "configured")
			return nil
		},
		ForwardHeaders: func(dst, src http.Header) {
			dst.Set("X-Fake-Feature", src.Get("X-Fake-Feature"))
		},
		ExtractUsage: extractUsage,
		Stream: proxy.StreamCodec{
			Usage: extractUsage,
			Terminal: func(_ gateway.ResolvedRoute, _ *gateway.Request) proxy.StreamTerminal {
				return func(data []byte) bool {
					var event struct {
						Done bool `json:"done"`
					}
					return json.Unmarshal(data, &event) == nil && event.Done
				}
			},
		},
	}
	provider := proxy.NewProvider(adapter, upstream.Client())
	// NewProvider owns an immutable operation registry snapshot.
	delete(operations, gateway.OpChatCompletions)

	if provider.Name() != "fake-provider" || !provider.Supports(gateway.OpChatCompletions) || provider.Supports(gateway.OpResponses) {
		t.Fatalf("provider contract name/support = %q/%t/%t", provider.Name(), provider.Supports(gateway.OpChatCompletions), provider.Supports(gateway.OpResponses))
	}
	route := gateway.ResolvedRoute{Route: &gateway.Route{Provider: provider.Name(), Model: "fake-model"}}
	meta := gateway.Meta{Headers: http.Header{"X-Fake-Feature": []string{"forwarded"}}}

	unary, err := provider.Invoke(context.Background(), route, &gateway.Request{
		Meta: meta, Operation: gateway.OpChatCompletions, RawBody: json.RawMessage(`{"stream":false}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(unary.RawBody, []byte(unaryResponse)) || unary.Usage.InputTokens != 3 {
		t.Fatalf("unary body/usage = %s/%#v", unary.RawBody, unary.Usage)
	}

	stream, err := provider.Invoke(context.Background(), route, &gateway.Request{
		Meta: meta, Operation: gateway.OpChatCompletions, Stream: true, RawBody: json.RawMessage(`{"stream":true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(stream.RawStream)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != streamResponse {
		t.Fatalf("stream = %q, want byte-for-byte passthrough", data)
	}
	if usage := stream.FinalUsage(); usage.InputTokens != 7 || usage.TotalTokens != 7 {
		t.Fatalf("stream usage = %#v, want adapter-observed usage", usage)
	}
}

func TestAdapterDelegatesRouteValidationAndBridgePlanning(t *testing.T) {
	validated := false
	provider := proxy.NewProvider(proxy.Adapter{
		Name: "custom",
		Operations: map[gateway.Operation]proxy.OperationAdapter{
			gateway.OpChatCompletions: {
				Prepare: func(gateway.ResolvedRoute, *gateway.Request) (proxy.PreparedRequest, error) {
					return proxy.PreparedRequest{}, nil
				},
			},
		},
		ValidateRoute: func(route *gateway.Route) error {
			validated = route.Name == "route"
			return nil
		},
		PlanBridge: func(*gateway.Route, *gateway.Request) (gateway.Operation, string, bool) {
			return gateway.OpChatCompletions, "", true
		},
	}, nil)
	route := &gateway.Route{Name: "route", Provider: "custom"}
	if err := provider.ValidateRoute(route); err != nil || !validated {
		t.Fatalf("ValidateRoute() = %v, delegated=%t", err, validated)
	}
	target, reason, ok := provider.PlanBridge(route, &gateway.Request{Operation: gateway.OpCompletions})
	if !ok || reason != "" || target != gateway.OpChatCompletions {
		t.Fatalf("PlanBridge() = %q/%q/%t", target, reason, ok)
	}
	if err := provider.ValidateRoute(&gateway.Route{Name: "wrong", Provider: "other"}); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("provider mismatch error = %v", err)
	}
}

func TestBuiltInAdaptersProjectInlineMediaWithoutProviderSwitches(t *testing.T) {
	tests := []struct {
		name       string
		newAdapter func() proxy.Adapter
		raw        json.RawMessage
		want       string
	}{
		{
			name:       "openai",
			newAdapter: proxy.OpenAIAdapter,
			raw:        json.RawMessage(`{"messages":[{"content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,QUJDRA=="}}]}]}`),
			want:       "[binary image]",
		},
		{
			name:       "anthropic",
			newAdapter: proxy.AnthropicAdapter,
			raw:        json.RawMessage(`{"messages":[{"content":[{"type":"tool_result","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"QUJDRA=="}}]}]}]}`),
			want:       "[binary image]",
		},
		{
			name:       "gemini",
			newAdapter: proxy.GeminiAdapter,
			raw:        json.RawMessage(`{"contents":[{"parts":[{"inlineData":{"mimeType":"image/png","data":"QUJDRA=="}}]}]}`),
			want:       "[binary media]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := proxy.NewProvider(tt.newAdapter(), nil)
			projected := provider.ProjectTokenText(tt.raw)
			if !strings.Contains(projected, tt.want) || strings.Contains(projected, "QUJDRA==") {
				t.Fatalf("projected text = %s", projected)
			}
		})
	}
}

func TestBuiltInAdapterContracts(t *testing.T) {
	tests := []struct {
		name       string
		newAdapter func() proxy.Adapter
		supported  []gateway.Operation
		denied     []gateway.Operation
	}{
		{
			name:       "openai",
			newAdapter: proxy.OpenAIAdapter,
			supported: []gateway.Operation{
				gateway.OpChatCompletions, gateway.OpResponses, gateway.OpCompletions, gateway.OpEmbeddings,
			},
		},
		{name: "anthropic", newAdapter: proxy.AnthropicAdapter, supported: []gateway.Operation{gateway.OpChatCompletions}, denied: []gateway.Operation{gateway.OpEmbeddings}},
		{name: "gemini", newAdapter: proxy.GeminiAdapter, supported: []gateway.Operation{gateway.OpChatCompletions, gateway.OpEmbeddings}, denied: []gateway.Operation{gateway.OpResponses}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := proxy.NewProvider(tt.newAdapter(), nil)
			if provider.Name() != tt.name {
				t.Fatalf("Name() = %q", provider.Name())
			}
			for _, operation := range tt.supported {
				if !provider.Supports(operation) {
					t.Errorf("Supports(%q) = false", operation)
				}
			}
			for _, operation := range tt.denied {
				if provider.Supports(operation) {
					t.Errorf("Supports(%q) = true", operation)
				}
			}
		})
	}
	openAI := proxy.NewProvider(proxy.OpenAIAdapter(), nil)
	chatOnly := &gateway.Route{
		Name: "chat", Provider: "openai",
		Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
	}
	if target, reason, ok := openAI.PlanBridge(chatOnly, &gateway.Request{Operation: gateway.OpCompletions}); !ok || reason != "" || target != gateway.OpChatCompletions {
		t.Fatalf("completion bridge = %q/%q/%t", target, reason, ok)
	}
	if _, reason, ok := openAI.PlanBridge(chatOnly, &gateway.Request{Operation: gateway.OpResponses, Stream: true}); ok || !strings.Contains(reason, "streaming") {
		t.Fatalf("streaming responses bridge = %q/%t", reason, ok)
	}
	if _, _, ok := proxy.NewProvider(proxy.AnthropicAdapter(), nil).PlanBridge(chatOnly, &gateway.Request{Operation: gateway.OpCompletions}); ok {
		t.Fatal("anthropic unexpectedly planned an OpenAI bridge")
	}

	if err := proxy.NewProvider(proxy.GeminiAdapter(), nil).ValidateRoute(&gateway.Route{Name: "gemini", Provider: "gemini", Backend: "gemini"}); err != nil {
		t.Fatalf("native Gemini backend rejected: %v", err)
	}
	for _, tt := range []struct {
		route      *gateway.Route
		newAdapter func() proxy.Adapter
	}{
		{route: &gateway.Route{Name: "vertex", Provider: "gemini", Backend: "vertex"}, newAdapter: proxy.GeminiAdapter},
		{route: &gateway.Route{Name: "vertex-project", Provider: "gemini", Project: "project"}, newAdapter: proxy.GeminiAdapter},
		{route: &gateway.Route{Name: "openai-backend", Provider: "openai", Backend: "custom"}, newAdapter: proxy.OpenAIAdapter},
	} {
		provider := proxy.NewProvider(tt.newAdapter(), nil)
		if err := provider.ValidateRoute(tt.route); err == nil {
			t.Errorf("ValidateRoute(%s) accepted unsupported route semantics", tt.route.Name)
		}
	}
}

func TestLegacyNameBasedProviderConstructor(t *testing.T) {
	tests := []struct {
		name      string
		operation gateway.Operation
		supported bool
	}{
		{name: "openai", operation: gateway.OpResponses, supported: true},
		{name: "anthropic", operation: gateway.OpChatCompletions, supported: true},
		{name: "gemini", operation: gateway.OpEmbeddings, supported: true},
		{name: "unknown", operation: gateway.OpChatCompletions, supported: false},
	}
	for _, tt := range tests {
		provider := proxy.New(tt.name, nil)
		if provider.Name() != tt.name || provider.Supports(tt.operation) != tt.supported {
			t.Fatalf("New(%q) = name %q supported %t, want supported %t", tt.name, provider.Name(), provider.Supports(tt.operation), tt.supported)
		}
	}
}

func TestBuiltInAdaptersParseOnlyTheirProviderErrorEnvelope(t *testing.T) {
	tests := []struct {
		name       string
		newAdapter func() proxy.Adapter
		body       string
		wantType   string
		wantCode   string
	}{
		{name: "openai", newAdapter: proxy.OpenAIAdapter, body: `{"error":{"message":"bad OpenAI request","type":"invalid_request_error","code":"bad_parameter"}}`, wantType: "invalid_request_error", wantCode: "bad_parameter"},
		{name: "anthropic", newAdapter: proxy.AnthropicAdapter, body: `{"error":{"message":"bad Anthropic request","type":"invalid_request_error","status":"SHOULD_NOT_WIN"}}`, wantType: "invalid_request_error", wantCode: "upstream_error"},
		{name: "gemini", newAdapter: proxy.GeminiAdapter, body: `{"error":{"message":"bad Gemini request","type":"SHOULD_NOT_WIN","status":"INVALID_ARGUMENT"}}`, wantType: "upstream_error", wantCode: "invalid_argument"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer upstream.Close()
			route := &gateway.Route{Name: tt.name, Provider: tt.name, BaseURL: upstream.URL, Model: "model", UpstreamModel: "model"}
			if tt.name != "gemini" {
				route.BaseURL += "/v1"
			}
			_, err := proxy.NewProvider(tt.newAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{Route: route}, &gateway.Request{
				Operation: gateway.OpChatCompletions,
				RawBody:   json.RawMessage(`{"model":"model","messages":[]}`),
			})
			apiErr := gateway.AsAPIError(err)
			if err == nil || apiErr.Type != tt.wantType || apiErr.Code != tt.wantCode {
				t.Fatalf("error = %#v, want type/code %q/%q", apiErr, tt.wantType, tt.wantCode)
			}
		})
	}
}
