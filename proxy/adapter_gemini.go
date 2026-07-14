package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"llmgw/gateway"
)

// GeminiAdapter implements the public Gemini generateContent/embedContent
// protocol. Vertex project/location routing intentionally remains unsupported.
func GeminiAdapter() Adapter {
	return Adapter{
		Name: "gemini",
		Operations: map[gateway.Operation]OperationAdapter{
			gateway.OpChatCompletions: geminiOperation(":generateContent", ":streamGenerateContent", validateGeminiGenerateResponse),
			gateway.OpEmbeddings:      geminiOperation(":embedContent", "", validateGeminiEmbeddingResponse),
		},
		ApplyAuth:        applyGeminiAuth,
		ParseError:       parseGeminiError,
		ExtractUsage:     extractGeminiUsage,
		ValidateRoute:    validateGeminiRoute,
		ProjectTokenText: projectGeminiTokenText,
		Stream: StreamCodec{
			Usage:    geminiStreamUsage,
			Terminal: geminiStreamTerminal,
		},
	}
}

func geminiOperation(unarySuffix, streamSuffix string, validate func([]byte) error) OperationAdapter {
	return OperationAdapter{
		Prepare: func(route gateway.ResolvedRoute, req *gateway.Request) (PreparedRequest, error) {
			if route.Route == nil {
				return PreparedRequest{}, gateway.UnsupportedOperation("missing route")
			}
			if err := validateGeminiRoute(route.Route); err != nil {
				return PreparedRequest{}, err
			}
			body, err := patchBody(req.RawBody, map[string]any{"model": nil})
			if err != nil {
				return PreparedRequest{}, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error())
			}
			suffix := unarySuffix
			if req.Stream {
				if streamSuffix == "" {
					return PreparedRequest{}, gateway.UnsupportedOperation("gemini operation does not support streaming")
				}
				suffix = streamSuffix
			}
			version := firstNonEmpty(req.Hints.APIVersion, route.Route.APIVersion, "v1beta")
			target := fmt.Sprintf("%s/%s/models/%s%s",
				strings.TrimRight(route.Route.BaseURL, "/"),
				strings.TrimLeft(version, "/"),
				url.PathEscape(upstreamModel(route.Route)), suffix,
			)
			if req.Stream {
				target += "?" + url.Values{"alt": []string{"sse"}}.Encode()
			}
			return PreparedRequest{Body: body, URL: target}, nil
		},
		ValidateResponse: validate,
	}
}

func applyGeminiAuth(req *http.Request, route *gateway.Route) error {
	if route == nil {
		return gateway.UnsupportedOperation("missing route")
	}
	if route.APIKey != "" {
		req.Header.Set("x-goog-api-key", route.APIKey)
	}
	if req.Header.Get("x-goog-api-client") == "" {
		req.Header.Set("x-goog-api-client", "llmgw/1.0 gateway/aggregator")
	}
	return nil
}

func validateGeminiRoute(route *gateway.Route) error {
	if route == nil {
		return gateway.UnsupportedOperation("missing route")
	}
	if route.Backend != "" && !strings.EqualFold(route.Backend, "gemini") {
		return unsupportedRouteBackend(route)
	}
	if route.Project != "" || route.Location != "" {
		return unsupportedProjectLocation(route)
	}
	return nil
}

func validateGeminiGenerateResponse(data []byte) error {
	object, err := decodeResponseObject(data)
	if err != nil {
		return err
	}
	if !validGeminiCandidates(object["candidates"]) && !validGeminiBlockedPrompt(object["promptFeedback"]) {
		return invalidUpstreamResponse("response does not match the provider operation")
	}
	return nil
}

func validateGeminiEmbeddingResponse(data []byte) error {
	object, err := decodeResponseObject(data)
	if err != nil {
		return err
	}
	var embedding struct {
		Values json.RawMessage `json:"values"`
	}
	raw := bytes.TrimSpace(object["embedding"])
	if len(raw) == 0 || raw[0] != '{' || json.Unmarshal(raw, &embedding) != nil {
		return invalidUpstreamResponse("response does not match the provider operation")
	}
	valuesRaw := bytes.TrimSpace(embedding.Values)
	if len(valuesRaw) == 0 || valuesRaw[0] != '[' {
		return invalidUpstreamResponse("response does not match the provider operation")
	}
	var values []json.RawMessage
	if json.Unmarshal(valuesRaw, &values) != nil || len(values) == 0 {
		return invalidUpstreamResponse("response does not match the provider operation")
	}
	for _, value := range values {
		var decoded any
		if json.Unmarshal(value, &decoded) != nil {
			return invalidUpstreamResponse("response does not match the provider operation")
		}
		if _, ok := decoded.(float64); !ok {
			return invalidUpstreamResponse("response does not match the provider operation")
		}
	}
	return nil
}

func validGeminiCandidates(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '[' {
		return false
	}
	var candidates []json.RawMessage
	if json.Unmarshal(raw, &candidates) != nil {
		return false
	}
	for _, candidate := range candidates {
		candidate = bytes.TrimSpace(candidate)
		if len(candidate) == 0 || candidate[0] != '{' {
			return false
		}
	}
	return true
}

func validGeminiBlockedPrompt(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return false
	}
	var feedback struct {
		BlockReason string `json:"blockReason"`
	}
	return json.Unmarshal(raw, &feedback) == nil && strings.TrimSpace(feedback.BlockReason) != ""
}

func parseGeminiError(status int, body []byte) error {
	var wire struct {
		Error struct {
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &wire) == nil && wire.Error.Message != "" {
		return withUpstreamDisposition(gateway.NewError(status, "upstream_error", strings.ToLower(wire.Error.Status), wire.Error.Message), status)
	}
	return genericUpstreamError(status)
}

func extractGeminiUsage(route gateway.ResolvedRoute, req *gateway.Request, body []byte) gateway.Usage {
	var wire struct {
		ModelVersion string `json:"modelVersion"`
		Usage        struct {
			PromptTokenCount        int64 `json:"promptTokenCount"`
			CandidatesTokenCount    int64 `json:"candidatesTokenCount"`
			TotalTokenCount         int64 `json:"totalTokenCount"`
			CachedContentTokenCount int64 `json:"cachedContentTokenCount"`
			ToolUsePromptTokenCount int64 `json:"toolUsePromptTokenCount"`
			ThoughtsTokenCount      int64 `json:"thoughtsTokenCount"`
		} `json:"usageMetadata"`
		Candidates []struct {
			GroundingMetadata struct {
				WebSearchQueries []string `json:"webSearchQueries"`
			} `json:"groundingMetadata"`
		} `json:"candidates"`
	}
	if json.Unmarshal(body, &wire) != nil {
		return gateway.Usage{}
	}
	usage := gateway.Usage{
		InputTokens:     wire.Usage.PromptTokenCount,
		OutputTokens:    wire.Usage.CandidatesTokenCount,
		TotalTokens:     wire.Usage.TotalTokenCount,
		CacheReadTokens: wire.Usage.CachedContentTokenCount,
	}
	if wire.Usage.ToolUsePromptTokenCount != 0 {
		usage.InputDetails = &gateway.UsageDetails{ToolTokens: wire.Usage.ToolUsePromptTokenCount}
	}
	if wire.Usage.ThoughtsTokenCount != 0 {
		usage.OutputDetails = &gateway.UsageDetails{ReasoningTokens: wire.Usage.ThoughtsTokenCount}
	}
	queries := make(map[string]struct{})
	for _, candidate := range wire.Candidates {
		for _, query := range candidate.GroundingMetadata.WebSearchQueries {
			if query = strings.TrimSpace(query); query != "" {
				queries[query] = struct{}{}
			}
		}
	}
	searchRequests := int64(len(queries))
	if searchRequests > 0 {
		model := strings.TrimSpace(wire.ModelVersion)
		if model == "" && route.Route != nil {
			model = upstreamModel(route.Route)
		}
		if model == "" && req != nil {
			model = req.Model
		}
		// Gemini 3 is billed per non-empty unique search query. Gemini 2.5
		// and older are billed once per grounded prompt even when the model
		// executes several queries. Unknown/future model families retain the
		// query count as the conservative no-undercount fallback.
		if major, known := geminiModelMajor(model); known && major < 3 {
			searchRequests = 1
		}
		usage.ProviderDetails = map[string]int64{"google_search_requests": searchRequests}
	}
	return usage
}

func geminiModelMajor(model string) (int, bool) {
	model = strings.ToLower(strings.TrimSpace(model))
	model = strings.TrimRight(model, "/")
	if index := strings.LastIndexByte(model, '/'); index >= 0 {
		model = model[index+1:]
	}
	if !strings.HasPrefix(model, "gemini-") {
		return 0, false
	}
	version := strings.TrimPrefix(model, "gemini-")
	end := 0
	for end < len(version) && version[end] >= '0' && version[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}
	major, err := strconv.Atoi(version[:end])
	return major, err == nil && major > 0
}

func geminiStreamUsage(route gateway.ResolvedRoute, req *gateway.Request, data []byte) gateway.Usage {
	return nestedStreamUsage(func(body []byte) gateway.Usage {
		return extractGeminiUsage(route, req, body)
	}, data)
}

func geminiStreamTerminal(_ gateway.ResolvedRoute, req *gateway.Request) StreamTerminal {
	expected := int64(1)
	if req != nil && req.Hints.OutputMultiplicity > expected {
		expected = req.Hints.OutputMultiplicity
	}
	finished := make(map[int64]struct{})
	return func(data []byte) bool {
		data = bytes.TrimSpace(data)
		var event struct {
			Candidates []struct {
				FinishReason string `json:"finishReason"`
				Index        *int64 `json:"index"`
			} `json:"candidates"`
			PromptFeedback struct {
				BlockReason string `json:"blockReason"`
			} `json:"promptFeedback"`
		}
		if json.Unmarshal(data, &event) != nil {
			return false
		}
		if event.PromptFeedback.BlockReason != "" {
			return true
		}
		for position, candidate := range event.Candidates {
			if candidate.FinishReason == "" {
				continue
			}
			index := int64(position)
			if candidate.Index != nil {
				index = *candidate.Index
			}
			if index >= 0 && index < expected {
				finished[index] = struct{}{}
			}
		}
		return int64(len(finished)) == expected
	}
}
