package api

import (
	"net/http"
	"net/url"
	"strings"

	"llmgw/gateway"
	proxyproviders "llmgw/proxy/providers"
)

// GeminiIngress returns the v1 and v1beta Gemini model-operation descriptors.
func GeminiIngress() Ingress {
	return Ingress{
		Provider:     "gemini",
		Authenticate: bearerIngressAuthenticator("x-goog-api-key"),
		WriteError:   writeGeminiError,
		MatchPath: func(path string) bool {
			return strings.HasPrefix(path, "/v1beta/") || strings.HasPrefix(path, "/v1/models/")
		},
		Routes: []IngressRoute{
			geminiRoute("/v1beta/models/"),
			geminiRoute("/v1/models/"),
		},
	}
}

func geminiRoute(prefix string) IngressRoute {
	return IngressRoute{
		Pattern:   prefix,
		Method:    http.MethodPost,
		Operation: gateway.OpChatCompletions,
		Resolve: func(r *http.Request) (gateway.Operation, RequestDecoder, *http.Request, error) {
			model, op, ok := parseGeminiOperation(r.URL.Path, prefix)
			if !ok {
				return gateway.OpChatCompletions, nil, r, gateway.NewError(http.StatusNotFound, "invalid_request_error", "not_found", "endpoint not found")
			}
			request := withModelQuery(r, model)
			apiVersion := strings.Trim(prefix, "/")
			var decoder RequestDecoder
			switch op {
			case gateway.OpChatCompletions:
				decoder = func(r *http.Request, maxBytes int64) (*gateway.Request, error) {
					out, err := proxyproviders.GeminiGenerateRequest(r, maxBytes)
					if err != nil {
						return nil, err
					}
					out.Model = model
					out.Hints.APIVersion = apiVersion
					return out, nil
				}
			case gateway.OpEmbeddings:
				decoder = func(r *http.Request, maxBytes int64) (*gateway.Request, error) {
					out, err := proxyproviders.GeminiEmbeddingRequest(r, maxBytes)
					if err != nil {
						return nil, err
					}
					out.Model = model
					out.Hints.APIVersion = apiVersion
					return out, nil
				}
			default:
				return op, nil, request, gateway.UnsupportedOperation("gemini operation is not supported")
			}
			return op, decoder, request, nil
		},
	}
}

func parseGeminiOperation(path, prefix string) (model string, op gateway.Operation, ok bool) {
	rest, found := strings.CutPrefix(path, prefix)
	if !found || rest == "" {
		return "", "", false
	}
	switch {
	case strings.HasSuffix(rest, ":streamGenerateContent"):
		model = strings.TrimSuffix(rest, ":streamGenerateContent")
		op = gateway.OpChatCompletions
	case strings.HasSuffix(rest, ":generateContent"):
		model = strings.TrimSuffix(rest, ":generateContent")
		op = gateway.OpChatCompletions
	case strings.HasSuffix(rest, ":embedContent"):
		model = strings.TrimSuffix(rest, ":embedContent")
		op = gateway.OpEmbeddings
	default:
		return "", "", false
	}
	model, err := url.PathUnescape(strings.TrimSpace(model))
	if err != nil || model == "" || strings.Contains(model, "/") {
		return "", "", false
	}
	return model, op, true
}

func withModelQuery(r *http.Request, model string) *http.Request {
	cloned := r.Clone(r.Context())
	urlCopy := *r.URL
	query := cloned.URL.Query()
	if strings.TrimSpace(query.Get("model")) == "" {
		query.Set("model", model)
	}
	urlCopy.RawQuery = query.Encode()
	cloned.URL = &urlCopy
	return cloned
}

func writeGeminiError(w http.ResponseWriter, err error) {
	apiErr := gateway.AsAPIError(err)
	copyProviderErrorHeaders(w.Header(), apiErr.Headers)
	_ = writeJSON(w, apiErr.Status, map[string]any{
		"error": map[string]any{
			"code":    apiErr.Status,
			"message": apiErr.Message,
			"status":  googleErrorStatus(apiErr.Status),
		},
	})
}

func googleErrorStatus(status int) string {
	switch status {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return "INVALID_ARGUMENT"
	case http.StatusUnauthorized:
		return "UNAUTHENTICATED"
	case http.StatusForbidden:
		return "PERMISSION_DENIED"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "ABORTED"
	case http.StatusTooManyRequests:
		return "RESOURCE_EXHAUSTED"
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return "DEADLINE_EXCEEDED"
	case http.StatusServiceUnavailable, http.StatusBadGateway:
		return "UNAVAILABLE"
	default:
		return "INTERNAL"
	}
}
