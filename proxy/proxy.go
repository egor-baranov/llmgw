package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"llmgw/gateway"
)

type Provider struct {
	name   string
	client *http.Client
}

var supportedOperationsByProvider = map[string]map[gateway.Operation]struct{}{
	"openai": {
		gateway.OpChatCompletions: {},
		gateway.OpResponses:       {},
		gateway.OpCompletions:     {},
		gateway.OpEmbeddings:      {},
	},
	"anthropic": {
		gateway.OpChatCompletions: {},
	},
	"gemini": {
		gateway.OpChatCompletions: {},
		gateway.OpEmbeddings:      {},
	},
}

func New(name string, client *http.Client) *Provider {
	if client == nil {
		client = http.DefaultClient
	}
	return &Provider{name: name, client: client}
}

func (p *Provider) Name() string { return p.name }

func (p *Provider) Supports(op gateway.Operation) bool {
	supported, ok := supportedOperationsByProvider[p.name]
	if !ok {
		return false
	}
	_, ok = supported[op]
	return ok
}

func (p *Provider) BuildEffective(_ gateway.ResolvedRoute, req *gateway.Request) (*gateway.EffectiveParams, error) {
	effective := &gateway.EffectiveParams{}
	if req != nil {
		effective.MaxOutputTokens = req.RequestedMaxOutputTokens()
	}
	return effective, nil
}

func (p *Provider) Invoke(ctx context.Context, route gateway.ResolvedRoute, req *gateway.Request) (*gateway.Result, error) {
	body, targetURL, err := p.prepare(route, req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, gateway.NewError(http.StatusBadGateway, "upstream_error", "request_failed", err.Error())
	}
	httpReq.Header.Set("Content-Type", "application/json")
	applyRequestHeaders(httpReq, route, req.Meta)
	if err := applyProviderAuth(httpReq, route.Route); err != nil {
		return nil, err
	}
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, gateway.NewError(http.StatusBadGateway, "upstream_error", "request_failed", err.Error())
	}
	if req.Stream {
		if resp.StatusCode >= 300 {
			defer resp.Body.Close()
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			return nil, readUpstreamError(resp.StatusCode, body)
		}
		return &gateway.Result{
			StatusCode:  resp.StatusCode,
			Headers:     cloneHeaders(resp.Header),
			ContentType: firstNonEmpty(resp.Header.Get("Content-Type"), "text/event-stream"),
			RawStream:   resp.Body,
			Usage:       fallbackUsage(req, route.Route.Capabilities.Tokenizer),
		}, nil
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, gateway.NewError(http.StatusBadGateway, "upstream_error", "read_failed", err.Error())
	}
	if resp.StatusCode >= 300 {
		return nil, readUpstreamError(resp.StatusCode, data)
	}
	usage := extractUsage(p.name, data)
	if usage.IsZero() {
		usage = fallbackUsage(req, route.Route.Capabilities.Tokenizer)
	}
	return &gateway.Result{
		StatusCode:  resp.StatusCode,
		Headers:     cloneHeaders(resp.Header),
		ContentType: firstNonEmpty(resp.Header.Get("Content-Type"), "application/json"),
		RawBody:     data,
		Usage:       usage,
	}, nil
}

func (p *Provider) prepare(route gateway.ResolvedRoute, req *gateway.Request) ([]byte, string, error) {
	switch p.name {
	case "openai":
		return prepareOpenAI(route, req)
	case "anthropic":
		return prepareAnthropic(route, req)
	case "gemini":
		return prepareGemini(route, req)
	default:
		return nil, "", gateway.UnsupportedOperation("unsupported provider proxy")
	}
}

func prepareOpenAI(route gateway.ResolvedRoute, req *gateway.Request) ([]byte, string, error) {
	body, err := patchBody(req.RawBody, map[string]any{"model": route.Route.Model})
	if err != nil {
		return nil, "", gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error())
	}
	return body, joinURL(route.Route.BaseURL, openAIPath(req.Operation)), nil
}

func prepareAnthropic(route gateway.ResolvedRoute, req *gateway.Request) ([]byte, string, error) {
	body, err := patchBody(req.RawBody, map[string]any{"model": route.Route.Model})
	if err != nil {
		return nil, "", gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error())
	}
	return body, joinURL(route.Route.BaseURL, "messages"), nil
}

func prepareGemini(route gateway.ResolvedRoute, req *gateway.Request) ([]byte, string, error) {
	if strings.EqualFold(route.Route.Backend, "vertex") || route.Route.Project != "" || route.Route.Location != "" {
		return nil, "", gateway.UnsupportedOperation("vertex gemini proxy mode is not supported")
	}
	body, err := patchBody(req.RawBody, map[string]any{"model": nil})
	if err != nil {
		return nil, "", gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error())
	}
	target, err := geminiURL(route.Route, req.Operation)
	if err != nil {
		return nil, "", err
	}
	return body, target, nil
}

func openAIPath(op gateway.Operation) string {
	switch op {
	case gateway.OpChatCompletions:
		return "chat/completions"
	case gateway.OpResponses:
		return "responses"
	case gateway.OpCompletions:
		return "completions"
	case gateway.OpEmbeddings:
		return "embeddings"
	default:
		return ""
	}
}

func geminiURL(route *gateway.Route, op gateway.Operation) (string, error) {
	if route == nil {
		return "", gateway.UnsupportedOperation("missing route")
	}
	version := firstNonEmpty(route.APIVersion, "v1beta")
	var suffix string
	switch op {
	case gateway.OpChatCompletions:
		suffix = ":generateContent"
	case gateway.OpEmbeddings:
		suffix = ":embedContent"
	default:
		return "", gateway.UnsupportedOperation("gemini proxy only supports chat and embeddings")
	}
	base := strings.TrimRight(route.BaseURL, "/")
	target := fmt.Sprintf("%s/%s/models/%s%s", base, strings.TrimLeft(version, "/"), url.PathEscape(route.Model), suffix)
	if route.APIKey != "" {
		values := url.Values{}
		values.Set("key", route.APIKey)
		target += "?" + values.Encode()
	}
	return target, nil
}

func applyRequestHeaders(req *http.Request, resolved gateway.ResolvedRoute, meta gateway.Meta) {
	if meta.RequestID != "" {
		req.Header.Set("X-Request-ID", meta.RequestID)
	}
	for key, value := range resolved.Route.Headers {
		req.Header.Set(key, value)
	}
	for key, values := range resolved.Headers {
		req.Header.Del(key)
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
}

func applyProviderAuth(req *http.Request, route *gateway.Route) error {
	if route == nil {
		return gateway.UnsupportedOperation("missing route")
	}
	switch route.Provider {
	case "openai":
		if route.APIKey != "" {
			req.Header.Set("Authorization", "Bearer "+route.APIKey)
		}
	case "anthropic":
		if route.APIKey != "" {
			req.Header.Set("x-api-key", route.APIKey)
		}
		if req.Header.Get("anthropic-version") == "" {
			req.Header.Set("anthropic-version", "2023-06-01")
		}
	case "gemini":
		if route.APIKey != "" {
			req.Header.Set("x-goog-api-key", route.APIKey)
		}
		if req.Header.Get("x-goog-api-client") == "" {
			req.Header.Set("x-goog-api-client", "llmgw/1.0 gateway/aggregator")
		}
	default:
		return gateway.UnsupportedOperation("unsupported provider proxy")
	}
	return nil
}

func readUpstreamError(status int, body []byte) error {
	var openAI struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &openAI); err == nil && openAI.Error.Message != "" {
		return gateway.NewError(status, firstNonEmpty(openAI.Error.Type, "upstream_error"), openAI.Error.Code, openAI.Error.Message)
	}
	var anthropic struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &anthropic); err == nil && anthropic.Error.Message != "" {
		return gateway.NewError(status, firstNonEmpty(anthropic.Error.Type, "upstream_error"), "upstream_error", anthropic.Error.Message)
	}
	var gemini struct {
		Error struct {
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &gemini); err == nil && gemini.Error.Message != "" {
		return gateway.NewError(status, "upstream_error", strings.ToLower(gemini.Error.Status), gemini.Error.Message)
	}
	return gateway.NewError(status, "upstream_error", "upstream_error", fmt.Sprintf("upstream returned %d", status))
}

func extractUsage(provider string, body []byte) gateway.Usage {
	switch provider {
	case "openai":
		var wire struct {
			Usage struct {
				PromptTokens     int64 `json:"prompt_tokens"`
				CompletionTokens int64 `json:"completion_tokens"`
				TotalTokens      int64 `json:"total_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(body, &wire) == nil {
			return gateway.Usage{
				InputTokens:  wire.Usage.PromptTokens,
				OutputTokens: wire.Usage.CompletionTokens,
				TotalTokens:  wire.Usage.TotalTokens,
			}
		}
	case "anthropic":
		var wire struct {
			Usage struct {
				InputTokens  int64 `json:"input_tokens"`
				OutputTokens int64 `json:"output_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(body, &wire) == nil {
			return gateway.Usage{
				InputTokens:  wire.Usage.InputTokens,
				OutputTokens: wire.Usage.OutputTokens,
				TotalTokens:  wire.Usage.InputTokens + wire.Usage.OutputTokens,
			}
		}
	case "gemini":
		var wire struct {
			Usage struct {
				PromptTokenCount        int64 `json:"promptTokenCount"`
				CandidatesTokenCount    int64 `json:"candidatesTokenCount"`
				TotalTokenCount         int64 `json:"totalTokenCount"`
				CachedContentTokenCount int64 `json:"cachedContentTokenCount"`
				ToolUsePromptTokenCount int64 `json:"toolUsePromptTokenCount"`
				ThoughtsTokenCount      int64 `json:"thoughtsTokenCount"`
			} `json:"usageMetadata"`
		}
		if json.Unmarshal(body, &wire) == nil {
			usage := gateway.Usage{
				InputTokens:     wire.Usage.PromptTokenCount,
				OutputTokens:    wire.Usage.CandidatesTokenCount,
				TotalTokens:     wire.Usage.TotalTokenCount,
				CacheReadTokens: wire.Usage.CachedContentTokenCount,
			}
			if wire.Usage.ToolUsePromptTokenCount > 0 {
				usage.InputDetails = &gateway.UsageDetails{ToolTokens: wire.Usage.ToolUsePromptTokenCount}
			}
			if wire.Usage.ThoughtsTokenCount > 0 {
				if usage.OutputDetails == nil {
					usage.OutputDetails = &gateway.UsageDetails{}
				}
				usage.OutputDetails.ReasoningTokens = wire.Usage.ThoughtsTokenCount
			}
			return usage
		}
	}
	return gateway.Usage{}
}

func fallbackUsage(req *gateway.Request, tokenizer string) gateway.Usage {
	text := strings.TrimSpace(req.PromptText())
	in := req.Hints.EstimatedInputTokens
	if in == 0 && req.Meta.BodyBytes > 0 {
		in = req.Meta.BodyBytes / 4
		if in == 0 {
			in = 1
		}
	}
	if in == 0 && text != "" {
		in = int64(len([]rune(text))/4 + 1)
	}
	if in == 0 {
		in = 1
	}
	out := int64(req.RequestedMaxOutputTokens())
	if out == 0 && req.Operation != gateway.OpEmbeddings {
		out = 256
	}
	if req.Operation == gateway.OpEmbeddings {
		out = 0
	}
	return gateway.Usage{
		InputTokens:  in,
		OutputTokens: out,
		TotalTokens:  in + out,
	}
}

func patchBody(body []byte, overrides map[string]any) ([]byte, error) {
	raw := map[string]json.RawMessage{}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &raw); err != nil {
			return nil, err
		}
	}
	for key, value := range overrides {
		if value == nil {
			delete(raw, key)
			continue
		}
		payload, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		raw[key] = payload
	}
	return json.Marshal(raw)
}

func joinURL(base, path string) string {
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/")
}

func cloneHeaders(in http.Header) http.Header {
	if in == nil {
		return nil
	}
	return in.Clone()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
