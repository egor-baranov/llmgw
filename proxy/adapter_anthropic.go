package proxy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"llmgw/gateway"
)

// AnthropicAdapter implements the native Messages protocol.
func AnthropicAdapter() Adapter {
	operation := OperationAdapter{
		Prepare:          prepareAnthropicRequest,
		ValidateResponse: validateAnthropicResponse,
	}
	return Adapter{
		Name: "anthropic",
		Operations: map[gateway.Operation]OperationAdapter{
			gateway.OpChatCompletions: operation,
		},
		ApplyAuth:        applyAnthropicAuth,
		ForwardHeaders:   forwardAnthropicHeaders,
		ParseError:       parseAnthropicError,
		ExtractUsage:     extractAnthropicUsage,
		ValidateRoute:    validateAnthropicRoute,
		ProjectTokenText: projectAnthropicTokenText,
		Stream: StreamCodec{
			Usage:    anthropicStreamUsage,
			Terminal: anthropicStreamTerminal,
		},
	}
}

func prepareAnthropicRequest(route gateway.ResolvedRoute, req *gateway.Request) (PreparedRequest, error) {
	if route.Route == nil {
		return PreparedRequest{}, gateway.UnsupportedOperation("missing route")
	}
	body, err := patchBody(req.RawBody, map[string]any{"model": upstreamModel(route.Route)})
	if err != nil {
		return PreparedRequest{}, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error())
	}
	return PreparedRequest{Body: body, URL: joinURL(route.Route.BaseURL, "messages")}, nil
}

func applyAnthropicAuth(req *http.Request, route *gateway.Route) error {
	if route == nil {
		return gateway.UnsupportedOperation("missing route")
	}
	if route.APIKey != "" {
		req.Header.Set("x-api-key", route.APIKey)
	}
	if req.Header.Get("anthropic-version") == "" {
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	return nil
}

func forwardAnthropicHeaders(dst, src http.Header) {
	forwardSelectedHeaders(dst, src, "Anthropic-Version", "Anthropic-Beta")
}

func validateAnthropicRoute(route *gateway.Route) error {
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

func validateAnthropicResponse(data []byte) error {
	object, err := decodeResponseObject(data)
	if err != nil {
		return err
	}
	if !responseHasID(object) || !responseHasArray(object, "content") {
		return invalidUpstreamResponse("response does not match the provider operation")
	}
	return nil
}

func parseAnthropicError(status int, body []byte) error {
	var wire struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &wire) == nil && wire.Error.Message != "" {
		return withUpstreamDisposition(gateway.NewError(status, firstNonEmpty(wire.Error.Type, "upstream_error"), "upstream_error", wire.Error.Message), status)
	}
	return genericUpstreamError(status)
}

func extractAnthropicUsage(_ gateway.ResolvedRoute, _ *gateway.Request, body []byte) gateway.Usage {
	var wire struct {
		Usage struct {
			InputTokens      int64 `json:"input_tokens"`
			OutputTokens     int64 `json:"output_tokens"`
			CacheReadTokens  int64 `json:"cache_read_input_tokens"`
			CacheWriteTokens int64 `json:"cache_creation_input_tokens"`
			ServerToolUse    struct {
				WebSearchRequests int64 `json:"web_search_requests"`
			} `json:"server_tool_use"`
		} `json:"usage"`
	}
	if json.Unmarshal(body, &wire) != nil {
		return gateway.Usage{}
	}
	// Anthropic reports cache read/creation tokens separately from input_tokens.
	// Include them in the billable input dimension for quota and spend accuracy.
	// Keep malformed negatives visible so reconciliation can retain the safe
	// pre-call estimate instead of accepting a partially clamped provider total.
	billableInput := int64(-1)
	if wire.Usage.InputTokens >= 0 && wire.Usage.CacheReadTokens >= 0 && wire.Usage.CacheWriteTokens >= 0 {
		billableInput = saturatingProxyAdd(wire.Usage.InputTokens, wire.Usage.CacheReadTokens)
		billableInput = saturatingProxyAdd(billableInput, wire.Usage.CacheWriteTokens)
	}
	usage := gateway.Usage{
		InputTokens:      billableInput,
		OutputTokens:     wire.Usage.OutputTokens,
		TotalTokens:      saturatingProxyAdd(billableInput, wire.Usage.OutputTokens),
		CacheReadTokens:  wire.Usage.CacheReadTokens,
		CacheWriteTokens: wire.Usage.CacheWriteTokens,
	}
	if wire.Usage.ServerToolUse.WebSearchRequests != 0 {
		usage.ProviderDetails = map[string]int64{"web_search_requests": wire.Usage.ServerToolUse.WebSearchRequests}
	}
	return usage
}

func anthropicStreamUsage(route gateway.ResolvedRoute, req *gateway.Request, data []byte) gateway.Usage {
	return nestedStreamUsage(func(body []byte) gateway.Usage {
		return extractAnthropicUsage(route, req, body)
	}, data)
}

func anthropicStreamTerminal(_ gateway.ResolvedRoute, _ *gateway.Request) StreamTerminal {
	return func(data []byte) bool {
		data = bytes.TrimSpace(data)
		var event struct {
			Type string `json:"type"`
		}
		return json.Unmarshal(data, &event) == nil && strings.EqualFold(event.Type, "message_stop")
	}
}
