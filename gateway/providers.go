package gateway

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// ProviderRouteValidator lets an adapter enforce provider-native route
// settings without teaching configuration loading about provider names.
// Implementations must treat the supplied route as read-only.
type ProviderRouteValidator interface {
	ValidateRoute(*Route) error
}

// ProviderBridgePlanner describes a provider-native compatibility bridge for a
// route that does not advertise the requested operation directly. The router
// remains provider-agnostic; adapters own which operations can be bridged and
// whether a particular request shape (for example streaming) is eligible.
type ProviderBridgePlanner interface {
	PlanBridge(route *Route, request *Request) (target Operation, reason string, ok bool)
}

// ValidateProviders binds a structurally valid configuration snapshot to the
// compile-time provider registry assembled by the application. Keeping this
// check outside YAML decoding means adding a provider does not require another
// central provider-name switch in the gateway package.
func ValidateProviders(snapshot *Snapshot, providers []Provider) error {
	if snapshot == nil {
		return fmt.Errorf("provider validation requires a configuration snapshot")
	}
	registry := make(map[string]Provider, len(providers))
	for index, provider := range providers {
		if nilProvider(provider) {
			return fmt.Errorf("provider registry entry %d is nil", index)
		}
		name := provider.Name()
		if err := validateProviderName(name); err != nil {
			return fmt.Errorf("provider registry entry %d: %w", index, err)
		}
		if _, duplicate := registry[name]; duplicate {
			return fmt.Errorf("provider %q is registered more than once", name)
		}
		registry[name] = provider
	}

	routeNames := make([]string, 0, len(snapshot.Routes))
	for name := range snapshot.Routes {
		routeNames = append(routeNames, name)
	}
	sort.Strings(routeNames)
	for _, name := range routeNames {
		route := snapshot.Routes[name]
		if route == nil {
			return fmt.Errorf("route %s must not be null", name)
		}
		provider, ok := registry[route.Provider]
		if !ok {
			return fmt.Errorf("route %s references unregistered provider %q", name, route.Provider)
		}
		for _, operation := range route.Capabilities.Operations {
			if !provider.Supports(operation) {
				return fmt.Errorf("route %s provider %s does not support operation %q", name, route.Provider, operation)
			}
		}
		if validator, ok := provider.(ProviderRouteValidator); ok {
			if err := validator.ValidateRoute(cloneRoute(route)); err != nil {
				return fmt.Errorf("route %s is invalid for provider %s: %w", name, route.Provider, err)
			}
		}
	}
	return nil
}

func validateProviderName(name string) error {
	if name == "" {
		return fmt.Errorf("provider name must not be empty")
	}
	if name != strings.TrimSpace(name) || name != strings.ToLower(name) {
		return fmt.Errorf("provider name %q must be lowercase without surrounding whitespace", name)
	}
	if len(name) > 128 {
		return fmt.Errorf("provider name %q exceeds 128 bytes", name)
	}
	for i := 0; i < len(name); i++ {
		value := name[i]
		if value >= 'a' && value <= 'z' || value >= '0' && value <= '9' {
			continue
		}
		switch value {
		case '.', '_', '-':
			continue
		default:
			return fmt.Errorf("provider name %q contains an invalid character", name)
		}
	}
	return nil
}

func nilProvider(provider Provider) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func providerRegistry(providers []Provider) map[string]Provider {
	registry := make(map[string]Provider, len(providers))
	for _, provider := range providers {
		if nilProvider(provider) {
			continue
		}
		name := provider.Name()
		if name == "" {
			continue
		}
		registry[name] = provider
	}
	return registry
}

func cloneRoute(route *Route) *Route {
	if route == nil {
		return nil
	}
	clone := *route
	clone.Headers = cloneStringMap(route.Headers)
	clone.Capabilities.Operations = append([]Operation(nil), route.Capabilities.Operations...)
	if len(route.Pricing.ProviderUnits) > 0 {
		clone.Pricing.ProviderUnits = make(map[string]ProviderUnitPricing, len(route.Pricing.ProviderUnits))
		for name, pricing := range route.Pricing.ProviderUnits {
			clone.Pricing.ProviderUnits[name] = pricing
		}
	}
	return &clone
}
