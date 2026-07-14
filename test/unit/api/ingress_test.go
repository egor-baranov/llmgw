package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gwapi "llmgw/api"
	"llmgw/gateway"
)

func TestCustomIngressOwnsRoutesAuthenticationErrorsAndPathMatching(t *testing.T) {
	var authCalls, decodeCalls, errorWrites, matchCalls int
	custom := gwapi.Ingress{
		Provider: "custom",
		Routes: []gwapi.IngressRoute{{
			Pattern:   "/custom/invoke",
			Method:    http.MethodPost,
			Operation: gateway.OpChatCompletions,
			Decoder: func(*http.Request, int64) (*gateway.Request, error) {
				decodeCalls++
				return nil, nil
			},
		}},
		Authenticate: func(*gateway.Snapshot, *http.Request) (*gateway.Principal, error) {
			authCalls++
			return nil, gateway.Unauthorized("custom credential required")
		},
		WriteError: func(w http.ResponseWriter, err error) {
			errorWrites++
			apiErr := gateway.AsAPIError(err)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Custom-Envelope", "true")
			w.WriteHeader(apiErr.Status)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"custom_error": map[string]any{"code": apiErr.Code},
			})
		},
		MatchPath: func(path string) bool {
			matchCalls++
			return strings.HasPrefix(path, "/custom/")
		},
	}

	srv := gwapi.NewServerWithIngresses(
		nil,
		gateway.NewConfigStore(&gateway.Snapshot{}),
		nil,
		nil,
		nil,
		custom,
	)
	handler := srv.Handler()

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/custom/invoke", nil))
	if unauthorized.Code != http.StatusUnauthorized || unauthorized.Header().Get("X-Custom-Envelope") != "true" || !strings.Contains(unauthorized.Body.String(), `"code":"unauthorized"`) {
		t.Fatalf("custom auth response = %d %s, headers=%v", unauthorized.Code, unauthorized.Body.String(), unauthorized.Header())
	}
	if authCalls != 1 || decodeCalls != 0 {
		t.Fatalf("auth calls = %d, decoder calls = %d; want authentication before decoding", authCalls, decodeCalls)
	}

	method := httptest.NewRecorder()
	handler.ServeHTTP(method, httptest.NewRequest(http.MethodGet, "/custom/invoke", nil))
	if method.Code != http.StatusMethodNotAllowed || method.Header().Get("Allow") != http.MethodPost || method.Header().Get("X-Custom-Envelope") != "true" {
		t.Fatalf("custom method response = %d %s, headers=%v", method.Code, method.Body.String(), method.Header())
	}
	if authCalls != 1 {
		t.Fatalf("auth calls after wrong method = %d, want 1", authCalls)
	}

	notFound := httptest.NewRecorder()
	handler.ServeHTTP(notFound, httptest.NewRequest(http.MethodGet, "/custom/missing", nil))
	if notFound.Code != http.StatusNotFound || notFound.Header().Get("X-Custom-Envelope") != "true" || !strings.Contains(notFound.Body.String(), `"code":"not_found"`) {
		t.Fatalf("custom not-found response = %d %s, headers=%v", notFound.Code, notFound.Body.String(), notFound.Header())
	}
	if matchCalls == 0 || errorWrites != 3 {
		t.Fatalf("path matches = %d, error writes = %d; want matcher used and three custom envelopes", matchCalls, errorWrites)
	}
}

func TestDefaultNativeIngressCredentials(t *testing.T) {
	cfg := &gateway.Snapshot{Auth: gateway.AuthConfig{Tokens: map[string]gateway.Principal{
		"gateway-token": {ID: "principal"},
	}}}
	for _, tt := range []struct {
		name    string
		ingress gwapi.Ingress
		header  string
	}{
		{name: "anthropic", ingress: gwapi.AnthropicIngress(), header: "x-api-key"},
		{name: "gemini", ingress: gwapi.GeminiIngress(), header: "x-goog-api-key"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set(tt.header, "gateway-token")
			principal, err := tt.ingress.Authenticate(cfg, req)
			if err != nil {
				t.Fatal(err)
			}
			if principal == nil || principal.ID != "principal" {
				t.Fatalf("principal = %#v, want principal", principal)
			}
		})
	}
}

func TestBuiltInProtocolBindingsAreUniqueAndAligned(t *testing.T) {
	ingresses := gwapi.DefaultIngresses()
	providers := gwapi.DefaultProviders(nil)
	if len(ingresses) != len(providers) || len(ingresses) == 0 {
		t.Fatalf("ingress/provider counts = %d/%d, want equal non-zero registries", len(ingresses), len(providers))
	}

	seen := make(map[string]struct{}, len(providers))
	for index, provider := range providers {
		if provider == nil {
			t.Fatalf("provider %d is nil", index)
		}
		name := strings.TrimSpace(provider.Name())
		if name == "" || ingresses[index].Provider != name {
			t.Fatalf("binding %d ingress/provider = %q/%q, want matching non-empty names", index, ingresses[index].Provider, name)
		}
		if _, duplicate := seen[name]; duplicate {
			t.Fatalf("duplicate built-in provider %q", name)
		}
		seen[name] = struct{}{}
		for _, route := range ingresses[index].Routes {
			if route.Operation != "" && !provider.Supports(route.Operation) {
				t.Errorf("provider %q does not support ingress operation %q", name, route.Operation)
			}
		}
	}

	// Factories must not expose mutable descriptors shared by multiple servers.
	ingresses[0].Provider = "mutated"
	providers = gwapi.DefaultProviders(nil)
	if fresh := gwapi.DefaultIngresses(); fresh[0].Provider == "mutated" || fresh[0].Provider != providers[0].Name() {
		t.Fatalf("default protocol factories returned shared or misaligned descriptors: %#v/%q", fresh[0], providers[0].Name())
	}
}
