package gateway_test

import (
	"context"
	"testing"

	"llmgw/gateway"
)

type benchProvider struct{}

func (benchProvider) Name() string { return "openai" }

func (benchProvider) Supports(op gateway.Operation) bool { return op == gateway.OpChatCompletions }

func (benchProvider) Invoke(_ context.Context, _ gateway.ResolvedRoute, req *gateway.Request) (*gateway.Result, error) {
	return &gateway.Result{
		Model:       req.Model,
		ContentType: "application/json",
		RawBody:     []byte(`{"id":"chat_1","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}]}`),
		Usage:       gateway.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2},
	}, nil
}

func BenchmarkChatHotPath(b *testing.B) {
	cfg := &gateway.Snapshot{
		Auth: gateway.AuthConfig{MaxBodyBytes: 1 << 20},
		Routes: map[string]*gateway.Route{
			"demo": {
				Name:     "demo",
				Provider: "openai",
				Model:    "gpt-4o-mini",
				Capabilities: gateway.Capability{
					Operations:       []gateway.Operation{gateway.OpChatCompletions},
					Streaming:        true,
					ToolCalling:      true,
					StructuredOutput: true,
					Reasoning:        true,
				},
			},
		},
	}
	engine := gateway.NewEngine(gateway.NewConfigStore(cfg), []gateway.Provider{benchProvider{}}, nil, nil)
	req := &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "gpt-4o-mini",
		Hints:     gateway.RequestHints{PromptText: "ping"},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		exec, err := engine.Execute(context.Background(), req)
		if err != nil {
			b.Fatal(err)
		}
		_ = exec.Settle(context.Background(), exec.Result.Usage, nil)
	}
}
