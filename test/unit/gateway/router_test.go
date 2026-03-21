package gateway_test

import (
	"strings"
	"testing"

	"llmgw/gateway"
)

func TestRouterResolveFiltersByProviderAndCapability(t *testing.T) {
	snapshot := &gateway.Snapshot{
		Routes: map[string]*gateway.Route{
			"openai": {
				Name:     "openai",
				Provider: "openai",
				Model:    "shared-model",
				Weight:   1,
				Priority: 10,
				Capabilities: gateway.Capability{
					Operations:       []gateway.Operation{gateway.OpResponses},
					Streaming:        true,
					ToolCalling:      true,
					StructuredOutput: true,
					Reasoning:        true,
				},
			},
			"anthropic": {
				Name:     "anthropic",
				Provider: "anthropic",
				Model:    "shared-model",
				Weight:   1,
				Priority: 5,
				Capabilities: gateway.Capability{
					Operations:       []gateway.Operation{gateway.OpResponses},
					Streaming:        true,
					ToolCalling:      true,
					StructuredOutput: true,
					Reasoning:        true,
				},
			},
		},
	}

	router := gateway.NewRouter()
	env := &gateway.Request{
		Provider:  "openai",
		Operation: gateway.OpResponses,
		Model:     "shared-model",
		Stream:    true,
		Hints:     gateway.RequestHints{PromptText: "hello"},
	}

	candidates, err := router.Resolve(snapshot, env)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(candidates))
	}
	if candidates[0].Route.Name != "openai" {
		t.Fatalf("first candidate = %#v, want openai", candidates[0])
	}
	if candidates[0].Request.Model != "shared-model" {
		t.Fatalf("candidate model = %q, want shared-model", candidates[0].Request.Model)
	}
}

func TestRouterRejectsUnsupportedOperationWithoutBridge(t *testing.T) {
	snapshot := &gateway.Snapshot{
		Routes: map[string]*gateway.Route{
			"chat-only": {
				Name:     "chat-only",
				Provider: "openai",
				Model:    "gpt-chat",
				Capabilities: gateway.Capability{
					Operations: []gateway.Operation{gateway.OpChatCompletions},
					Streaming:  true,
				},
			},
		},
	}

	router := gateway.NewRouter()
	_, err := router.Resolve(snapshot, &gateway.Request{
		Provider:  "openai",
		Operation: gateway.OpCompletions,
		Model:     "gpt-chat",
		Hints:     gateway.RequestHints{PromptText: "hello"},
	})
	if err == nil {
		t.Fatal("Resolve() error = nil, want unsupported operation")
	}
	if !strings.Contains(err.Error(), "requested operation") {
		t.Fatalf("error = %v, want requested operation", err)
	}
}
