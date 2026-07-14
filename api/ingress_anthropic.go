package api

import (
	"net/http"
	"strings"

	"llmgw/gateway"
	proxyproviders "llmgw/proxy/providers"
)

// AnthropicIngress returns the Anthropic Messages API descriptor.
func AnthropicIngress() Ingress {
	return Ingress{
		Provider:     "anthropic",
		Authenticate: bearerIngressAuthenticator("x-api-key"),
		WriteError:   writeAnthropicError,
		MatchPath: func(path string) bool {
			return strings.HasPrefix(path, "/v1/messages")
		},
		Routes: []IngressRoute{{
			Pattern:   "/v1/messages",
			Method:    http.MethodPost,
			Operation: gateway.OpChatCompletions,
			Decoder:   proxyproviders.AnthropicRequest,
		}},
	}
}

func writeAnthropicError(w http.ResponseWriter, err error) {
	apiErr := gateway.AsAPIError(err)
	copyProviderErrorHeaders(w.Header(), apiErr.Headers)
	_ = writeJSON(w, apiErr.Status, map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    anthropicErrorType(apiErr.Status),
			"message": apiErr.Message,
		},
	})
}

func anthropicErrorType(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return "authentication_error"
	case http.StatusForbidden:
		return "permission_error"
	case http.StatusNotFound:
		return "not_found_error"
	case http.StatusRequestEntityTooLarge:
		return "request_too_large"
	case http.StatusTooManyRequests:
		return "rate_limit_error"
	case http.StatusServiceUnavailable, 529:
		return "overloaded_error"
	default:
		if status >= http.StatusBadRequest && status < http.StatusInternalServerError {
			return "invalid_request_error"
		}
		return "api_error"
	}
}
