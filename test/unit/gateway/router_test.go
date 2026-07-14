package gateway_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"

	"llmgw/gateway"
)

func TestWrapErrorPreservesCauseWithoutExposingIt(t *testing.T) {
	cause := errors.New("redis password leaked in diagnostic")
	err := gateway.WrapError(503, "server_error", "dependency_unavailable", cause)
	if !errors.Is(err, cause) {
		t.Fatal("WrapError() did not preserve the internal cause")
	}
	if strings.Contains(err.Error(), "password") {
		t.Fatalf("public error exposed internal cause: %q", err.Error())
	}
}

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

func TestIndexedRouterMatchesUnindexedResolution(t *testing.T) {
	routes := map[string]*gateway.Route{
		"second": {
			Name: "second", Provider: "openai", Model: "shared", Priority: 5, Weight: 2,
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
		"first": {
			Name: "first", Provider: "openai", Model: "shared", Priority: 5, Weight: 1,
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
		"other-model": {
			Name: "other-model", Provider: "openai", Model: "other",
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
	}
	unindexed := &gateway.Snapshot{Routes: routes}
	indexed := gateway.NewConfigStore(unindexed).Load()
	router := gateway.NewRouter()
	request := &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "shared",
		Meta:      gateway.Meta{ExecutionID: "stable-test-seed"},
	}

	indexedCandidates, err := router.Resolve(indexed, request)
	if err != nil {
		t.Fatal(err)
	}
	unindexedCandidates, err := router.Resolve(unindexed, request)
	if err != nil {
		t.Fatal(err)
	}
	indexedNames := make([]string, len(indexedCandidates))
	unindexedNames := make([]string, len(unindexedCandidates))
	for i := range indexedCandidates {
		indexedNames[i] = indexedCandidates[i].Route.Name
		unindexedNames[i] = unindexedCandidates[i].Route.Name
	}
	if !reflect.DeepEqual(indexedNames, unindexedNames) {
		t.Fatalf("indexed routes = %v, unindexed routes = %v", indexedNames, unindexedNames)
	}

	unsupported := request.Clone()
	unsupported.Hints.RequiresVision = true
	_, indexedErr := router.Resolve(indexed, unsupported)
	_, unindexedErr := router.Resolve(unindexed, unsupported)
	indexedAPIError := gateway.AsAPIError(indexedErr)
	unindexedAPIError := gateway.AsAPIError(unindexedErr)
	if indexedAPIError.Status != unindexedAPIError.Status || indexedAPIError.Type != unindexedAPIError.Type ||
		indexedAPIError.Code != unindexedAPIError.Code || indexedAPIError.Message != unindexedAPIError.Message {
		t.Fatalf("indexed error = %#v, unindexed error = %#v", indexedAPIError, unindexedAPIError)
	}
}

func TestRouterReturnsRequestLocalRouteCopy(t *testing.T) {
	original := &gateway.Route{
		Name: "local", Provider: "openai", Model: "alias", Priority: 7,
		Headers: map[string]string{"X-Original": "true"},
		Capabilities: gateway.Capability{
			Operations: []gateway.Operation{gateway.OpChatCompletions},
		},
		Pricing: gateway.Pricing{ProviderUnits: map[string]gateway.ProviderUnitPricing{
			"search": {MicrosPerUnit: 2, MaxUnitsPerRequest: 1},
		}},
	}
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{"local": original}}

	candidates, err := gateway.NewRouter().Resolve(snapshot, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "alias",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(candidates))
	}
	candidate := candidates[0].Route
	candidate.Priority = 99
	candidate.Headers["X-Original"] = "mutated"
	candidate.Capabilities.Operations[0] = gateway.OpEmbeddings
	candidate.Pricing.ProviderUnits["search"] = gateway.ProviderUnitPricing{MicrosPerUnit: 100}

	if original.Priority != 7 || original.Headers["X-Original"] != "true" {
		t.Fatalf("snapshot scalar/map mutated through resolved route: %#v", original)
	}
	if original.Capabilities.Operations[0] != gateway.OpChatCompletions {
		t.Fatalf("snapshot operations mutated through resolved route: %#v", original.Capabilities.Operations)
	}
	if original.Pricing.ProviderUnits["search"].MicrosPerUnit != 2 {
		t.Fatalf("snapshot pricing mutated through resolved route: %#v", original.Pricing.ProviderUnits)
	}
}

func TestRouterWeightedSelectionDoesNotOverflow(t *testing.T) {
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"a": {
			Name: "a", Provider: "openai", Model: "alias", Weight: math.MaxInt,
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
		"b": {
			Name: "b", Provider: "openai", Model: "alias", Weight: math.MaxInt,
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
	}}
	router := gateway.NewRouter()
	firstRoutes := map[string]bool{}
	for i := range 128 {
		candidates, err := router.Resolve(snapshot, &gateway.Request{
			Operation: gateway.OpChatCompletions,
			Model:     "alias",
			Meta:      gateway.Meta{RequestID: fmt.Sprintf("request-%d", i)},
		})
		if err != nil {
			t.Fatal(err)
		}
		firstRoutes[candidates[0].Route.Name] = true
	}
	if !firstRoutes["a"] || !firstRoutes["b"] {
		t.Fatalf("overflowed weights did not select both equal candidates: %#v", firstRoutes)
	}
}

func TestRouterIsolatesBridgePlannerRouteMutation(t *testing.T) {
	original := &gateway.Route{
		Name: "chat-only", Provider: "mutating-bridge", Model: "alias", Priority: 7,
		Headers: map[string]string{"X-Original": "true"},
		Capabilities: gateway.Capability{
			Operations: []gateway.Operation{gateway.OpChatCompletions},
		},
	}
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{"chat-only": original}}
	candidates, err := gateway.NewRouter(mutatingBridgeProvider{}).Resolve(snapshot, &gateway.Request{
		Operation: gateway.OpResponses,
		Model:     "alias",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].Route.Priority != 7 || candidates[0].Route.Headers["X-Original"] != "true" {
		t.Fatalf("candidate was mutated by bridge planner: %#v", candidates)
	}
	if candidates[0].Route.Capabilities.Operations[0] != gateway.OpChatCompletions {
		t.Fatalf("candidate operations were mutated by bridge planner: %#v", candidates[0].Route.Capabilities.Operations)
	}
	if original.Priority != 7 || original.Headers["X-Original"] != "true" || original.Capabilities.Operations[0] != gateway.OpChatCompletions {
		t.Fatalf("snapshot route was mutated by bridge planner: %#v", original)
	}
}

func TestEngineIsolatesConcurrentProviderRouteMutation(t *testing.T) {
	original := &gateway.Route{
		Name: "mutating", Provider: "route-mutator", Model: "alias", Priority: 7,
		Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
	}
	engine := gateway.NewEngine(
		gateway.NewConfigStore(&gateway.Snapshot{Routes: map[string]*gateway.Route{"mutating": original}}),
		[]gateway.Provider{routeMutatingProvider{}},
		nil,
		nil,
	)

	const requests = 64
	errorsByRequest := make(chan error, requests)
	var wg sync.WaitGroup
	for range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := engine.Execute(context.Background(), &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "alias",
			})
			errorsByRequest <- err
		}()
	}
	wg.Wait()
	close(errorsByRequest)
	for err := range errorsByRequest {
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
	}
	if original.Priority != 7 {
		t.Fatalf("snapshot priority = %d, want 7", original.Priority)
	}
}

func TestEngineFallsBackWhenCustomProviderReturnsErrorStatus(t *testing.T) {
	provider := &statusResultProvider{}
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"unavailable": {
			Name: "unavailable", Provider: "custom", Model: "alias", Priority: 2,
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
		"healthy": {
			Name: "healthy", Provider: "custom", Model: "alias", Priority: 1,
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
	}}
	engine := gateway.NewEngine(gateway.NewConfigStore(snapshot), []gateway.Provider{provider}, nil, nil)
	exec, err := engine.Execute(context.Background(), &gateway.Request{Operation: gateway.OpChatCompletions, Model: "alias"})
	if err != nil {
		t.Fatal(err)
	}
	if provider.calls != 2 || exec.Attempt.Route.Route.Name != "healthy" {
		t.Fatalf("calls/route = %d/%q, want fallback to healthy", provider.calls, exec.Attempt.Route.Route.Name)
	}
}

func TestAttemptAccountingChargesProviderTotalRemainder(t *testing.T) {
	provider := usageResultProvider{usage: gateway.Usage{
		InputTokens: 10, OutputTokens: 20, TotalTokens: 100,
		ProviderDetails: map[string]int64{"web_search_requests": 2},
	}}
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"route": {
			Name: "route", Provider: "usage", Model: "alias",
			Pricing: gateway.Pricing{
				InputPer1M: 1, OutputPer1M: 1,
				ProviderUnits: map[string]gateway.ProviderUnitPricing{
					"web_search_requests": {MicrosPerUnit: 7.5, MaxUnitsPerRequest: 4},
				},
			},
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
	}}
	engine := gateway.NewEngine(
		gateway.NewConfigStore(snapshot),
		[]gateway.Provider{provider},
		[]gateway.RequestInterceptor{fixedUsageEstimate{usage: gateway.Usage{
			InputTokens: 10, OutputTokens: 20, TotalTokens: 30,
			ProviderDetails: map[string]int64{"web_search_requests": 4},
		}}},
		nil,
	)
	exec, err := engine.Execute(context.Background(), &gateway.Request{Operation: gateway.OpChatCompletions, Model: "alias"})
	if err != nil {
		t.Fatal(err)
	}
	actual := exec.State.TotalAttemptUsage()
	if actual.InputTokens != 10 || actual.OutputTokens != 90 || actual.TotalTokens() != 100 || actual.SpendMicros != 115 {
		t.Fatalf("attempt charge = %#v, want total remainder and provider units billed", actual)
	}
}

func TestAttemptAccountingRetainsEstimateForNonPositiveReportedTokens(t *testing.T) {
	estimate := gateway.Usage{
		InputTokens: 10, OutputTokens: 20, TotalTokens: 30,
		ProviderDetails: map[string]int64{"web_search_requests": 4},
	}
	tests := []struct {
		name       string
		reported   gateway.Usage
		wantInput  int64
		wantOutput int64
		wantSpend  int64
	}{
		{
			name: "negative usage",
			reported: gateway.Usage{
				InputTokens: -10, OutputTokens: -20, TotalTokens: -30,
				CacheReadTokens: -1, CacheWriteTokens: -1,
				ProviderDetails: map[string]int64{"web_search_requests": -1},
			},
			wantInput: 10, wantOutput: 20, wantSpend: 60,
		},
		{
			name: "provider units without token usage",
			reported: gateway.Usage{
				ProviderDetails: map[string]int64{"web_search_requests": 2, "invalid": -1},
			},
			wantInput: 10, wantOutput: 20, wantSpend: 45,
		},
		{
			name: "empty detail envelopes",
			reported: gateway.Usage{
				InputDetails:  &gateway.UsageDetails{},
				OutputDetails: &gateway.UsageDetails{},
			},
			wantInput: 10, wantOutput: 20, wantSpend: 60,
		},
		{
			name: "mixed valid and malformed dimensions",
			reported: gateway.Usage{
				InputTokens: 1, OutputTokens: -20,
			},
			wantInput: 1, wantOutput: 20, wantSpend: 51,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
				"route": {
					Name: "route", Provider: "usage", Model: "alias",
					Pricing: gateway.Pricing{
						InputPer1M: 1, OutputPer1M: 1,
						ProviderUnits: map[string]gateway.ProviderUnitPricing{
							"web_search_requests": {MicrosPerUnit: 7.5, MaxUnitsPerRequest: 4},
						},
					},
					Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
				},
			}}
			engine := gateway.NewEngine(
				gateway.NewConfigStore(snapshot),
				[]gateway.Provider{usageResultProvider{usage: tt.reported}},
				[]gateway.RequestInterceptor{fixedUsageEstimate{usage: estimate}},
				nil,
			)
			execution, err := engine.Execute(context.Background(), &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "alias",
			})
			if err != nil {
				t.Fatal(err)
			}
			actual := execution.State.TotalAttemptUsage()
			if actual.InputTokens != tt.wantInput || actual.OutputTokens != tt.wantOutput || actual.SpendMicros != tt.wantSpend {
				t.Fatalf("attempt charge = %#v, want input=%d output=%d spend=%d", actual, tt.wantInput, tt.wantOutput, tt.wantSpend)
			}
		})
	}
}

func TestPricingUsesCacheSpecificRates(t *testing.T) {
	pricing := gateway.Pricing{InputPer1M: 1, OutputPer1M: 2, CacheReadPer1M: 0.2, CacheWritePer1M: 0.3}
	usage := gateway.Usage{
		InputTokens: 1510, OutputTokens: 10, TotalTokens: 1520,
		CacheReadTokens: 1000, CacheWriteTokens: 500,
	}
	if got := pricing.SpendMicrosForUsage(usage); got != 380 {
		t.Fatalf("SpendMicrosForUsage() = %d, want 380", got)
	}
}

type statusResultProvider struct{ calls int }

type usageResultProvider struct{ usage gateway.Usage }

type fixedUsageEstimate struct{ usage gateway.Usage }

type bridgePlanningProvider struct{}

type invalidBridgePlanningProvider struct {
	target gateway.Operation
}

type mutatingBridgeProvider struct{}

type routeMutatingProvider struct{}

func (mutatingBridgeProvider) Name() string { return "mutating-bridge" }
func (mutatingBridgeProvider) Supports(gateway.Operation) bool {
	return true
}
func (mutatingBridgeProvider) Invoke(context.Context, gateway.ResolvedRoute, *gateway.Request) (*gateway.Result, error) {
	return nil, errors.New("not used")
}
func (mutatingBridgeProvider) PlanBridge(route *gateway.Route, _ *gateway.Request) (gateway.Operation, string, bool) {
	route.Priority = 99
	route.Headers["X-Original"] = "mutated"
	route.Capabilities.Operations[0] = gateway.OpEmbeddings
	return gateway.OpChatCompletions, "", true
}

func (routeMutatingProvider) Name() string                    { return "route-mutator" }
func (routeMutatingProvider) Supports(gateway.Operation) bool { return true }
func (routeMutatingProvider) Invoke(_ context.Context, route gateway.ResolvedRoute, _ *gateway.Request) (*gateway.Result, error) {
	route.Route.Priority++
	return &gateway.Result{StatusCode: 200, RawBody: []byte(`{}`)}, nil
}

func (bridgePlanningProvider) Name() string { return "openai" }
func (bridgePlanningProvider) Supports(operation gateway.Operation) bool {
	return operation == gateway.OpChatCompletions
}
func (bridgePlanningProvider) Invoke(context.Context, gateway.ResolvedRoute, *gateway.Request) (*gateway.Result, error) {
	return nil, errors.New("not used")
}
func (bridgePlanningProvider) PlanBridge(route *gateway.Route, request *gateway.Request) (gateway.Operation, string, bool) {
	if route == nil || request == nil || !route.Capabilities.Supports(gateway.OpChatCompletions) {
		return "", "route does not support requested operation", false
	}
	if request.Operation != gateway.OpResponses && request.Operation != gateway.OpCompletions {
		return "", "route does not support requested operation", false
	}
	if request.Stream && request.Operation == gateway.OpResponses {
		return "", "compatibility bridge does not support streaming", false
	}
	return gateway.OpChatCompletions, "", true
}

func (p invalidBridgePlanningProvider) Name() string { return "openai" }
func (p invalidBridgePlanningProvider) Supports(gateway.Operation) bool {
	return true
}
func (p invalidBridgePlanningProvider) Invoke(context.Context, gateway.ResolvedRoute, *gateway.Request) (*gateway.Result, error) {
	return nil, errors.New("not used")
}
func (p invalidBridgePlanningProvider) PlanBridge(*gateway.Route, *gateway.Request) (gateway.Operation, string, bool) {
	return p.target, "", true
}

func (f fixedUsageEstimate) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		candidates, err := state.ResolveCandidates()
		if err != nil {
			return nil, err
		}
		for i := range candidates {
			candidates[i].Estimate = f.usage
		}
		state.ReplaceCandidates(candidates)
		return next(ctx, state)
	}
}

func (p usageResultProvider) Name() string                    { return "usage" }
func (p usageResultProvider) Supports(gateway.Operation) bool { return true }
func (p usageResultProvider) Invoke(context.Context, gateway.ResolvedRoute, *gateway.Request) (*gateway.Result, error) {
	return &gateway.Result{StatusCode: 200, RawBody: []byte(`{}`), Usage: p.usage}, nil
}

func (p *statusResultProvider) Name() string                    { return "custom" }
func (p *statusResultProvider) Supports(gateway.Operation) bool { return true }
func (p *statusResultProvider) Invoke(_ context.Context, route gateway.ResolvedRoute, _ *gateway.Request) (*gateway.Result, error) {
	p.calls++
	if route.Route.Name == "unavailable" {
		return &gateway.Result{StatusCode: 503, RawBody: []byte(`{"error":"unavailable"}`)}, nil
	}
	return &gateway.Result{StatusCode: 200, RawBody: []byte(`{"ok":true}`)}, nil
}

func TestRouterBuildsNonStreamingCompatibilityBridge(t *testing.T) {
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

	router := gateway.NewRouter(bridgePlanningProvider{})
	candidates, err := router.Resolve(snapshot, &gateway.Request{
		Provider:  "openai",
		Operation: gateway.OpCompletions,
		Model:     "gpt-chat",
		Hints:     gateway.RequestHints{PromptText: "hello"},
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(candidates) != 1 || candidates[0].BridgeFrom != gateway.OpCompletions {
		t.Fatalf("candidates = %#v, want completions bridge", candidates)
	}
	if candidates[0].Request.Operation != gateway.OpChatCompletions {
		t.Fatalf("bridged operation = %q, want chat", candidates[0].Request.Operation)
	}

	streaming, err := router.Resolve(snapshot, &gateway.Request{
		Provider:  "openai",
		Operation: gateway.OpCompletions,
		Model:     "gpt-chat",
		Stream:    true,
	})
	if err != nil || len(streaming) != 1 || streaming[0].BridgeFrom != gateway.OpCompletions {
		t.Fatalf("streaming completion bridge = %#v, %v", streaming, err)
	}

	_, err = router.Resolve(snapshot, &gateway.Request{
		Provider:  "openai",
		Operation: gateway.OpResponses,
		Model:     "gpt-chat",
		Stream:    true,
	})
	if err == nil || !strings.Contains(err.Error(), "bridge") {
		t.Fatalf("streaming responses bridge error = %v, want explicit unsupported bridge", err)
	}
}

func TestRouterRejectsInvalidProviderBridgePlan(t *testing.T) {
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"chat-only": {
			Name: "chat-only", Provider: "openai", Model: "alias",
			Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}},
		},
	}}
	for _, target := range []gateway.Operation{gateway.OpEmbeddings, gateway.Operation("unknown.operation")} {
		_, err := gateway.NewRouter(invalidBridgePlanningProvider{target: target}).Resolve(snapshot, &gateway.Request{
			Operation: gateway.OpResponses,
			Model:     "alias",
		})
		if err == nil || !strings.Contains(err.Error(), "invalid compatibility bridge plan") {
			t.Fatalf("bridge target %q error = %v, want invalid plan", target, err)
		}
	}
}

func TestRouterFiltersRequiredFeaturesAndUsesUpstreamModel(t *testing.T) {
	snapshot := &gateway.Snapshot{Routes: map[string]*gateway.Route{
		"incapable": {
			Name:          "incapable",
			Provider:      "openai",
			Model:         "public-alias",
			UpstreamModel: "wrong-upstream",
			Priority:      100,
			Weight:        1,
			Capabilities: gateway.Capability{
				Operations: []gateway.Operation{gateway.OpChatCompletions},
			},
		},
		"capable": {
			Name:          "capable",
			Provider:      "openai",
			Model:         "public-alias",
			UpstreamModel: "real-upstream",
			Priority:      10,
			Weight:        1,
			Capabilities: gateway.Capability{
				Operations:       []gateway.Operation{gateway.OpChatCompletions},
				ToolCalling:      true,
				StructuredOutput: true,
				VisionInput:      true,
				Audio:            true,
				Reasoning:        true,
			},
		},
	}}

	candidates, err := gateway.NewRouter().Resolve(snapshot, &gateway.Request{
		Provider:  "openai",
		Operation: gateway.OpChatCompletions,
		Model:     "public-alias",
		Hints: gateway.RequestHints{
			RequiresTools:            true,
			RequiresStructuredOutput: true,
			RequiresVision:           true,
			RequiresAudio:            true,
			RequiresReasoning:        true,
		},
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(candidates) != 1 || candidates[0].Route.Name != "capable" {
		t.Fatalf("candidates = %#v, want only capable route", candidates)
	}
	if candidates[0].Request.Model != "real-upstream" {
		t.Fatalf("resolved model = %q, want real-upstream", candidates[0].Request.Model)
	}
}
