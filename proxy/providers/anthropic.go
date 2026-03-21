package providers

import (
	"net/http"

	"llmgw/gateway"
	"llmgw/proxy"
)

func AnthropicRequest(r *http.Request, maxBytes int64) (*gateway.Request, error) {
	return proxy.DecodeMinimalRequest(r, maxBytes, proxy.RequestDecodeSpec{
		Provider:        "anthropic",
		Operation:       gateway.OpChatCompletions,
		RequireModel:    true,
		AllowQueryModel: true,
		StreamPaths: [][]string{
			{"stream"},
		},
		MaxOutputPaths: [][]string{
			{"max_tokens"},
		},
		MetadataPaths: [][]string{
			{"metadata"},
		},
	})
}
