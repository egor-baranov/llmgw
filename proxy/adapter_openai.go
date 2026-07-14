package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"llmgw/gateway"
)

// OpenAIAdapter covers OpenAI and OpenAI-compatible upstreams, including the
// Responses/legacy-completions compatibility bridges.
func OpenAIAdapter() Adapter {
	return Adapter{
		Name: "openai",
		Operations: map[gateway.Operation]OperationAdapter{
			gateway.OpChatCompletions: openAIOperation("chat/completions", validateOpenAIChoices),
			gateway.OpResponses:       openAIOperation("responses", validateOpenAIResponses),
			gateway.OpCompletions:     openAIOperation("completions", validateOpenAIChoices),
			gateway.OpEmbeddings:      openAIOperation("embeddings", validateOpenAIEmbeddings),
		},
		ApplyAuth:         applyOpenAIAuth,
		ForwardHeaders:    forwardOpenAIHeaders,
		ParseError:        parseOpenAIError,
		ExtractUsage:      extractOpenAIUsage,
		Preflight:         preflightOpenAIBridge,
		TransformResponse: transformOpenAIResponse,
		ValidateRoute:     validateOpenAIRoute,
		PlanBridge:        planOpenAIBridge,
		ProjectTokenText:  projectOpenAITokenText,
		Stream: StreamCodec{
			Usage:     openAIStreamUsage,
			Terminal:  openAIStreamTerminal,
			Transform: transformOpenAIStream,
		},
	}
}

func planOpenAIBridge(route *gateway.Route, req *gateway.Request) (gateway.Operation, string, bool) {
	if route == nil || req == nil || !route.Capabilities.Supports(gateway.OpChatCompletions) {
		return "", "route does not support requested operation", false
	}
	if req.Operation != gateway.OpResponses && req.Operation != gateway.OpCompletions {
		return "", "route does not support requested operation", false
	}
	if req.Stream && req.Operation == gateway.OpResponses {
		return "", "compatibility bridge does not support streaming", false
	}
	return gateway.OpChatCompletions, "", true
}

func openAIOperation(path string, validate func([]byte) error) OperationAdapter {
	return OperationAdapter{
		Prepare: func(route gateway.ResolvedRoute, req *gateway.Request) (PreparedRequest, error) {
			if route.Route == nil {
				return PreparedRequest{}, gateway.UnsupportedOperation("missing route")
			}
			body := []byte(req.RawBody)
			var err error
			if route.BridgeFrom != "" {
				body, err = bridgeOpenAIRequest(route.BridgeFrom, req.RawBody)
				if err != nil {
					return PreparedRequest{}, gateway.AllowFallback(err)
				}
			}
			body, err = patchBody(body, map[string]any{"model": upstreamModel(route.Route)})
			if err != nil {
				return PreparedRequest{}, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error())
			}
			return PreparedRequest{Body: body, URL: joinURL(route.Route.BaseURL, path)}, nil
		},
		ValidateResponse: validate,
	}
}

func preflightOpenAIBridge(route gateway.ResolvedRoute, req *gateway.Request) error {
	if route.BridgeFrom == "" || req == nil {
		return nil
	}
	_, err := bridgeOpenAIRequest(route.BridgeFrom, req.RawBody)
	if err != nil {
		return gateway.AllowFallback(err)
	}
	return nil
}

func transformOpenAIResponse(route gateway.ResolvedRoute, _ *gateway.Request, data []byte) ([]byte, bool, error) {
	if route.BridgeFrom == "" {
		return data, false, nil
	}
	converted, err := bridgeOpenAIResponse(route.BridgeFrom, route.Route.Model, data)
	if err != nil {
		return nil, false, err
	}
	return converted, true, nil
}

func transformOpenAIStream(route gateway.ResolvedRoute, _ *gateway.Request, stream io.ReadCloser) (io.ReadCloser, bool, error) {
	if route.BridgeFrom != gateway.OpCompletions {
		return stream, false, nil
	}
	return newCompletionBridgeReadCloser(stream, route.Route.Model), true, nil
}

func applyOpenAIAuth(req *http.Request, route *gateway.Route) error {
	if route == nil {
		return gateway.UnsupportedOperation("missing route")
	}
	if route.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+route.APIKey)
	}
	return nil
}

func forwardOpenAIHeaders(dst, src http.Header) {
	forwardSelectedHeaders(dst, src, "OpenAI-Beta")
}

func validateOpenAIRoute(route *gateway.Route) error {
	if route == nil {
		return gateway.UnsupportedOperation("missing route")
	}
	if route.Backend != "" {
		return unsupportedRouteBackend(route)
	}
	if route.Project != "" || route.Location != "" {
		return unsupportedProjectLocation(route)
	}
	return nil
}

func validateOpenAIChoices(data []byte) error {
	object, err := decodeResponseObject(data)
	if err != nil {
		return err
	}
	if !responseHasID(object) || !responseHasArray(object, "choices") {
		return invalidUpstreamResponse("response does not match the provider operation")
	}
	return nil
}

func validateOpenAIResponses(data []byte) error {
	object, err := decodeResponseObject(data)
	if err != nil {
		return err
	}
	if !responseHasID(object) || !responseHasArray(object, "output") {
		return invalidUpstreamResponse("response does not match the provider operation")
	}
	return nil
}

func validateOpenAIEmbeddings(data []byte) error {
	object, err := decodeResponseObject(data)
	if err != nil {
		return err
	}
	if !responseHasArray(object, "data") {
		return invalidUpstreamResponse("response does not match the provider operation")
	}
	return nil
}

func parseOpenAIError(status int, body []byte) error {
	var wire struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &wire) == nil && wire.Error.Message != "" {
		return withUpstreamDisposition(gateway.NewError(status, firstNonEmpty(wire.Error.Type, "upstream_error"), wire.Error.Code, wire.Error.Message), status)
	}
	return genericUpstreamError(status)
}

func extractOpenAIUsage(_ gateway.ResolvedRoute, _ *gateway.Request, body []byte) gateway.Usage {
	var wire struct {
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			InputTokens      int64 `json:"input_tokens"`
			OutputTokens     int64 `json:"output_tokens"`
			TotalTokens      int64 `json:"total_tokens"`
			InputDetails     struct {
				CachedTokens int64 `json:"cached_tokens"`
				AudioTokens  int64 `json:"audio_tokens"`
			} `json:"input_tokens_details"`
			OutputDetails struct {
				ReasoningTokens int64 `json:"reasoning_tokens"`
				AudioTokens     int64 `json:"audio_tokens"`
			} `json:"output_tokens_details"`
			PromptDetails struct {
				CachedTokens int64 `json:"cached_tokens"`
				AudioTokens  int64 `json:"audio_tokens"`
			} `json:"prompt_tokens_details"`
			CompletionDetails struct {
				ReasoningTokens int64 `json:"reasoning_tokens"`
				AudioTokens     int64 `json:"audio_tokens"`
			} `json:"completion_tokens_details"`
		} `json:"usage"`
		Output []struct {
			Type string `json:"type"`
		} `json:"output"`
	}
	if json.Unmarshal(body, &wire) != nil {
		return gateway.Usage{}
	}
	usage := gateway.Usage{
		InputTokens:  usageAliasMaximum(wire.Usage.InputTokens, wire.Usage.PromptTokens),
		OutputTokens: usageAliasMaximum(wire.Usage.OutputTokens, wire.Usage.CompletionTokens),
		TotalTokens:  wire.Usage.TotalTokens,
	}
	if usage.TotalTokens == 0 {
		usage.TotalTokens = saturatingProxyAdd(usage.InputTokens, usage.OutputTokens)
	}
	cachedTokens := usageAliasMaximum(wire.Usage.InputDetails.CachedTokens, wire.Usage.PromptDetails.CachedTokens)
	inputAudioTokens := usageAliasMaximum(wire.Usage.InputDetails.AudioTokens, wire.Usage.PromptDetails.AudioTokens)
	if cachedTokens != 0 || inputAudioTokens != 0 {
		usage.CacheReadTokens = cachedTokens
		usage.InputDetails = &gateway.UsageDetails{CachedTokens: cachedTokens, AudioTokens: inputAudioTokens}
	}
	reasoningTokens := usageAliasMaximum(wire.Usage.OutputDetails.ReasoningTokens, wire.Usage.CompletionDetails.ReasoningTokens)
	outputAudioTokens := usageAliasMaximum(wire.Usage.OutputDetails.AudioTokens, wire.Usage.CompletionDetails.AudioTokens)
	if reasoningTokens != 0 || outputAudioTokens != 0 {
		usage.OutputDetails = &gateway.UsageDetails{ReasoningTokens: reasoningTokens, AudioTokens: outputAudioTokens}
	}
	for _, item := range wire.Output {
		unit := openAIProviderUnit(item.Type)
		if unit == "" {
			continue
		}
		if usage.ProviderDetails == nil {
			usage.ProviderDetails = make(map[string]int64)
		}
		usage.ProviderDetails[unit]++
	}
	return usage
}

// OpenAI exposes the same usage counters under endpoint-specific aliases. A
// negative value on either alias is malformed and must survive reconciliation.
func usageAliasMaximum(first, second int64) int64 {
	if first < 0 || second < 0 {
		return -1
	}
	return max(first, second)
}

func openAIStreamUsage(route gateway.ResolvedRoute, req *gateway.Request, data []byte) gateway.Usage {
	return nestedStreamUsage(func(body []byte) gateway.Usage {
		return extractOpenAIUsage(route, req, body)
	}, data)
}

func openAIStreamTerminal(_ gateway.ResolvedRoute, _ *gateway.Request) StreamTerminal {
	return func(data []byte) bool {
		data = bytes.TrimSpace(data)
		if bytes.Equal(data, []byte("[DONE]")) {
			return true
		}
		var event struct {
			Type string `json:"type"`
		}
		return json.Unmarshal(data, &event) == nil &&
			(event.Type == "response.completed" || event.Type == "response.incomplete" || event.Type == "response.failed")
	}
}

func openAIProviderUnit(outputType string) string {
	switch strings.ToLower(strings.TrimSpace(outputType)) {
	case "web_search_call":
		return "web_search_requests"
	case "file_search_call":
		return "file_search_requests"
	case "computer_call", "computer_use_call":
		return "computer_use_requests"
	case "code_interpreter_call":
		return "code_interpreter_requests"
	case "image_generation_call":
		return "image_generation_requests"
	default:
		return ""
	}
}
