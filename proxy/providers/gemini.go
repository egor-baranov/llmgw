package providers

import (
	"net/http"

	"llmgw/gateway"
	"llmgw/proxy"
)

func GeminiGenerateRequest(r *http.Request, maxBytes int64) (*gateway.Request, error) {
	return proxy.DecodeMinimalRequest(r, maxBytes, proxy.RequestDecodeSpec{
		Provider:        "gemini",
		Operation:       gateway.OpChatCompletions,
		RequireModel:    true,
		AllowQueryModel: true,
		MaxOutputPaths: [][]string{
			{"generationConfig", "maxOutputTokens"},
			{"generation_config", "max_output_tokens"},
		},
	})
}

func GeminiEmbeddingRequest(r *http.Request, maxBytes int64) (*gateway.Request, error) {
	return proxy.DecodeMinimalRequest(r, maxBytes, proxy.RequestDecodeSpec{
		Provider:        "gemini",
		Operation:       gateway.OpEmbeddings,
		RequireModel:    true,
		AllowQueryModel: true,
	})
}
