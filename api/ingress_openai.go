package api

import (
	"net/http"

	"llmgw/gateway"
	proxyproviders "llmgw/proxy/providers"
)

// OpenAIIngress returns the OpenAI-compatible northbound API descriptor.
func OpenAIIngress() Ingress {
	return Ingress{
		Provider:     "openai",
		Authenticate: bearerIngressAuthenticator(),
		WriteError:   writeOpenAIError,
		Fallback:     true,
		Routes: []IngressRoute{
			openAIRoute("/v1/chat/completions", gateway.OpChatCompletions),
			openAIRoute("/v1/responses", gateway.OpResponses),
			openAIRoute("/v1/completions", gateway.OpCompletions),
			openAIRoute("/v1/embeddings", gateway.OpEmbeddings),
		},
	}
}

func openAIRoute(pattern string, op gateway.Operation) IngressRoute {
	return IngressRoute{
		Pattern:   pattern,
		Method:    http.MethodPost,
		Operation: op,
		Decoder: func(r *http.Request, maxBytes int64) (*gateway.Request, error) {
			return proxyproviders.OpenAIRequest(r, op, maxBytes)
		},
	}
}
