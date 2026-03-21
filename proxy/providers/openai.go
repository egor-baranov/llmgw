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
	}
	switch op {
	case gateway.OpChatCompletions:
		spec.MaxOutputPaths = [][]string{{"max_output_tokens"}, {"max_tokens"}}
	case gateway.OpResponses:
		spec.MaxOutputPaths = [][]string{{"max_output_tokens"}}
	case gateway.OpCompletions:
		spec.MaxOutputPaths = [][]string{{"max_tokens"}}
	}
	return proxy.DecodeMinimalRequest(r, maxBytes, spec)
}
