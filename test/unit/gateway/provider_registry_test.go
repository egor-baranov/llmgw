package gateway_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"llmgw/gateway"
)

func TestCustomProviderRequiresNoGatewaySwitch(t *testing.T) {
	configText := strings.Replace(minimalAnonymousConfig, "provider: openai", "provider: acme", 1)
	snapshot, err := loadConfigText(t, configText)
	if err != nil {
		t.Fatalf("custom provider config did not load structurally: %v", err)
	}
	provider := &registryProvider{name: "acme", supported: map[gateway.Operation]bool{gateway.OpChatCompletions: true}}
	if err := gateway.ValidateProviders(snapshot, []gateway.Provider{provider}); err != nil {
		t.Fatalf("ValidateProviders() rejected registered custom provider: %v", err)
	}
	engine := gateway.NewEngine(gateway.NewConfigStore(snapshot), []gateway.Provider{provider}, nil, nil)
	execution, err := engine.Execute(context.Background(), &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Model:     "gpt-4o-mini",
	})
	if err != nil {
		t.Fatalf("custom provider execution failed: %v", err)
	}
	if execution.Result.Provider != "acme" || provider.calls != 1 || !provider.validated {
		t.Fatalf("custom provider result/calls/validation = %q/%d/%t", execution.Result.Provider, provider.calls, provider.validated)
	}
}

func TestValidateProvidersRejectsInvalidRegistryBindings(t *testing.T) {
	snapshot, err := loadConfigText(t, strings.Replace(minimalAnonymousConfig, "provider: openai", "provider: acme", 1))
	if err != nil {
		t.Fatal(err)
	}
	var typedNil *registryProvider
	tests := []struct {
		name      string
		providers []gateway.Provider
		want      string
	}{
		{name: "missing", want: "unregistered provider"},
		{name: "nil", providers: []gateway.Provider{nil}, want: "is nil"},
		{name: "typed nil", providers: []gateway.Provider{typedNil}, want: "is nil"},
		{
			name: "duplicate",
			providers: []gateway.Provider{
				&registryProvider{name: "acme", supported: map[gateway.Operation]bool{gateway.OpChatCompletions: true}},
				&registryProvider{name: "acme", supported: map[gateway.Operation]bool{gateway.OpChatCompletions: true}},
			},
			want: "more than once",
		},
		{
			name:      "noncanonical name",
			providers: []gateway.Provider{&registryProvider{name: "Acme"}},
			want:      "must be lowercase",
		},
		{
			name:      "unsupported operation",
			providers: []gateway.Provider{&registryProvider{name: "acme", supported: map[gateway.Operation]bool{gateway.OpEmbeddings: true}}},
			want:      "does not support operation",
		},
		{
			name: "provider-native route validation",
			providers: []gateway.Provider{&registryProvider{
				name:      "acme",
				supported: map[gateway.Operation]bool{gateway.OpChatCompletions: true},
				routeErr:  errors.New("unsupported backend"),
			}},
			want: "unsupported backend",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := gateway.ValidateProviders(snapshot, tt.providers)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateProviders() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestValidateProvidersIsolatesRouteValidationFromSnapshot(t *testing.T) {
	snapshot, err := loadConfigText(t, strings.Replace(minimalAnonymousConfig, "provider: openai", "provider: acme", 1))
	if err != nil {
		t.Fatal(err)
	}
	route := snapshot.Routes["demo"]
	route.Headers = map[string]string{"X-Route": "original"}
	route.Pricing.ProviderUnits = map[string]gateway.ProviderUnitPricing{
		"tool_calls": {MicrosPerUnit: 2, MaxUnitsPerRequest: 3},
	}

	provider := &registryProvider{
		name:      "acme",
		supported: map[gateway.Operation]bool{gateway.OpChatCompletions: true},
		validate: func(candidate *gateway.Route) error {
			candidate.Provider = "mutated"
			candidate.Headers["X-Route"] = "mutated"
			candidate.Capabilities.Operations[0] = gateway.OpEmbeddings
			candidate.Pricing.ProviderUnits["tool_calls"] = gateway.ProviderUnitPricing{MicrosPerUnit: 99}
			return nil
		},
	}
	if err := gateway.ValidateProviders(snapshot, []gateway.Provider{provider}); err != nil {
		t.Fatal(err)
	}
	if route.Provider != "acme" || route.Headers["X-Route"] != "original" ||
		route.Capabilities.Operations[0] != gateway.OpChatCompletions ||
		route.Pricing.ProviderUnits["tool_calls"].MicrosPerUnit != 2 {
		t.Fatalf("provider route validation mutated live snapshot: %#v", route)
	}
}

func TestEngineUsesRegisteredProviderBridgePlanner(t *testing.T) {
	snapshot, err := loadConfigText(t, strings.Replace(minimalAnonymousConfig, "provider: openai", "provider: acme", 1))
	if err != nil {
		t.Fatal(err)
	}
	provider := &bridgingRegistryProvider{}
	if err := gateway.ValidateProviders(snapshot, []gateway.Provider{provider}); err != nil {
		t.Fatal(err)
	}
	engine := gateway.NewEngine(gateway.NewConfigStore(snapshot), []gateway.Provider{provider}, nil, nil)
	execution, err := engine.Execute(context.Background(), &gateway.Request{
		Operation: gateway.OpCompletions,
		Model:     "gpt-4o-mini",
	})
	if err != nil {
		t.Fatal(err)
	}
	if execution.Attempt.Route.BridgeFrom != gateway.OpCompletions {
		t.Fatalf("BridgeFrom = %q, want %q", execution.Attempt.Route.BridgeFrom, gateway.OpCompletions)
	}
	if provider.operation != gateway.OpChatCompletions {
		t.Fatalf("provider operation = %q, want %q", provider.operation, gateway.OpChatCompletions)
	}
}

type registryProvider struct {
	name      string
	supported map[gateway.Operation]bool
	routeErr  error
	validate  func(*gateway.Route) error
	validated bool
	calls     int
}

func (p *registryProvider) Name() string { return p.name }

func (p *registryProvider) Supports(operation gateway.Operation) bool {
	return p.supported[operation]
}

func (p *registryProvider) ValidateRoute(route *gateway.Route) error {
	p.validated = true
	if p.validate != nil {
		return p.validate(route)
	}
	return p.routeErr
}

func (p *registryProvider) Invoke(context.Context, gateway.ResolvedRoute, *gateway.Request) (*gateway.Result, error) {
	p.calls++
	return &gateway.Result{StatusCode: 200, RawBody: []byte(`{"ok":true}`)}, nil
}

type bridgingRegistryProvider struct {
	operation gateway.Operation
}

func (*bridgingRegistryProvider) Name() string { return "acme" }

func (*bridgingRegistryProvider) Supports(operation gateway.Operation) bool {
	return operation == gateway.OpChatCompletions
}

func (*bridgingRegistryProvider) PlanBridge(_ *gateway.Route, request *gateway.Request) (gateway.Operation, string, bool) {
	if request != nil && request.Operation == gateway.OpCompletions {
		return gateway.OpChatCompletions, "", true
	}
	return "", "not bridgeable", false
}

func (p *bridgingRegistryProvider) Invoke(_ context.Context, _ gateway.ResolvedRoute, request *gateway.Request) (*gateway.Result, error) {
	p.operation = request.Operation
	return &gateway.Result{StatusCode: 200, RawBody: []byte(`{"ok":true}`)}, nil
}
