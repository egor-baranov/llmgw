package gateway_test

import (
	"context"
	"fmt"
	"os"
	"strings"
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
	cfg := benchmarkRouterSnapshot(b, 1)
	engine := gateway.NewEngine(gateway.NewConfigStore(cfg), []gateway.Provider{benchProvider{}}, nil, nil)
	req := &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "target-model",
		Hints:     gateway.RequestHints{PromptText: "ping"},
	}
	b.ReportAllocs()
	for b.Loop() {
		exec, err := engine.Execute(context.Background(), req)
		if err != nil {
			b.Fatal(err)
		}
		_ = exec.Settle(context.Background(), exec.Result.Usage, nil)
	}
}

func BenchmarkRouterResolveByModelIndex(b *testing.B) {
	for _, routeCount := range []int{1, 100, 10_000} {
		cfg := benchmarkRouterSnapshot(b, routeCount)
		snapshots := []struct {
			name     string
			snapshot *gateway.Snapshot
		}{
			{name: "indexed", snapshot: cfg},
			{name: "unindexed", snapshot: &gateway.Snapshot{Routes: cfg.Routes}},
		}
		for _, candidate := range snapshots {
			b.Run(fmt.Sprintf("%d/%s", routeCount, candidate.name), func(b *testing.B) {
				router := gateway.NewRouter(benchProvider{})
				request := &gateway.Request{
					Operation: gateway.OpChatCompletions,
					Model:     "target-model",
					Meta:      gateway.Meta{ExecutionID: "benchmark"},
				}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := router.Resolve(candidate.snapshot, request); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func benchmarkRouterSnapshot(b *testing.B, routeCount int) *gateway.Snapshot {
	b.Helper()
	var config strings.Builder
	config.WriteString("auth:\n  allow_anonymous: true\nstore:\n  mode: memory\nroutes:\n")
	for route := range routeCount {
		model := fmt.Sprintf("model-%d", route)
		if route == 0 {
			model = "target-model"
		}
		_, _ = fmt.Fprintf(&config, "  route-%d:\n    provider: openai\n    model: %s\n    base_url: https://example.invalid/v1\n    capabilities:\n      operations: [chat.completions]\n      max_output_tokens: 4096\n      tokenizer: cl100k_base\n", route, model)
	}
	path := b.TempDir() + "/config.yaml"
	if err := os.WriteFile(path, []byte(config.String()), 0o600); err != nil {
		b.Fatal(err)
	}
	snapshot, err := gateway.LoadConfigFile(path)
	if err != nil {
		b.Fatal(err)
	}
	return snapshot
}
