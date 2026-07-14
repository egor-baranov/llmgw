package providers

import (
	"net/http"

	"llmgw/gateway"
	"llmgw/proxy"
)

func OpenAIRequest(r *http.Request, op gateway.Operation, maxBytes int64) (*gateway.Request, error) {
	spec := proxy.RequestDecodeSpec{
		Provider:     "openai",
		Operation:    op,
		RequireModel: true,
		StreamPaths:  [][]string{{"stream"}},
		MetadataPaths: [][]string{
			{"metadata"},
		},
		UserPaths: [][]string{
			{"user"},
		},
		DetectProviderUnits: openAIProviderUnits,
	}
	switch op {
	case gateway.OpChatCompletions:
		spec.RequiredArrayPaths = [][]string{{"messages"}}
		spec.MaxOutputPaths = [][]string{{"max_completion_tokens"}, {"max_output_tokens"}, {"max_tokens"}}
		spec.OutputMultiplicityPaths = [][]string{{"n"}}
	case gateway.OpResponses:
		spec.ResponsesInputPaths = [][]string{{"input"}}
		spec.MaxOutputPaths = [][]string{{"max_output_tokens"}}
	case gateway.OpCompletions:
		spec.TextInputPaths = [][]string{{"prompt"}}
		spec.MaxOutputPaths = [][]string{{"max_tokens"}}
		spec.OutputMultiplicityPaths = [][]string{{"n"}, {"best_of"}}
	case gateway.OpEmbeddings:
		spec.TextInputPaths = [][]string{{"input"}}
	}
	return proxy.DecodeMinimalRequest(r, maxBytes, spec)
}
