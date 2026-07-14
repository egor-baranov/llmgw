package providers

import (
	"net/http"
	"strings"

	"llmgw/gateway"
	"llmgw/proxy"
)

func GeminiGenerateRequest(r *http.Request, maxBytes int64) (*gateway.Request, error) {
	request, err := proxy.DecodeMinimalRequest(r, maxBytes, proxy.RequestDecodeSpec{
		Provider:        "gemini",
		Operation:       gateway.OpChatCompletions,
		RequireModel:    true,
		AllowQueryModel: true,
		MaxOutputPaths: [][]string{
			{"generationConfig", "maxOutputTokens"},
			{"generation_config", "max_output_tokens"},
		},
		OutputMultiplicityPaths: [][]string{
			{"generationConfig", "candidateCount"},
			{"generation_config", "candidate_count"},
		},
		RequiredArrayPaths:  [][]string{{"contents"}},
		DetectProviderUnits: geminiProviderUnits,
	})
	if err != nil {
		return nil, err
	}
	// Streaming is selected by the Gemini RPC method. The alt query parameter
	// controls the representation of streamGenerateContent; it must not turn the
	// unary generateContent method into a different upstream operation.
	request.Stream = strings.HasSuffix(r.URL.Path, ":streamGenerateContent")
	return request, nil
}

func GeminiEmbeddingRequest(r *http.Request, maxBytes int64) (*gateway.Request, error) {
	return proxy.DecodeMinimalRequest(r, maxBytes, proxy.RequestDecodeSpec{
		Provider:            "gemini",
		Operation:           gateway.OpEmbeddings,
		RequireModel:        true,
		AllowQueryModel:     true,
		RequiredObjectPaths: [][]string{{"content"}},
	})
}
