package api_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	gwapi "llmgw/api"
	"llmgw/gateway"
	"llmgw/observer"
	"llmgw/store"
)

func TestUnknownPathsUseProviderErrorEnvelopesAndIngressMetrics(t *testing.T) {
	cfg := loadModelsConfig(t)
	obs := observer.New("test")
	srv := gwapi.NewServer(nil, gateway.NewConfigStore(cfg), obs, nil, nil)
	tests := []struct {
		path string
		want string
	}{
		{path: "/v1/unknown", want: `"code":"not_found"`},
		{path: "/v1/messages/unknown", want: `"type":"not_found_error"`},
		{path: "/v1beta/unknown", want: `"status":"NOT_FOUND"`},
	}
	for _, tt := range tests {
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, tt.path, nil))
		if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), tt.want) {
			t.Fatalf("%s response = %d %s, want provider JSON 404 containing %s", tt.path, rr.Code, rr.Body.String(), tt.want)
		}
		if contentType := rr.Header().Get("Content-Type"); !strings.Contains(contentType, "application/json") {
			t.Fatalf("%s content type = %q, want JSON", tt.path, contentType)
		}
		if rr.Header().Get("X-Request-ID") == "" {
			t.Fatalf("%s response is missing X-Request-ID", tt.path)
		}
	}
	var metrics bytes.Buffer
	obs.Metrics.Set.WritePrometheus(&metrics)
	if !strings.Contains(metrics.String(), `llmgw_requests_total{operation="unknown",model="unknown",status="error"} 3`) {
		t.Fatalf("unknown-path ingress metric missing: %s", metrics.String())
	}
}

func TestProviderIngressAuthenticatesBeforeReadingBody(t *testing.T) {
	cfg := loadModelsConfig(t)
	srv := gwapi.NewServer(nil, gateway.NewConfigStore(cfg), nil, nil, nil)
	tests := []struct {
		path string
		want string
	}{
		{path: "/v1/chat/completions", want: `"code":"unauthorized"`},
		{path: "/v1/messages", want: `"type":"authentication_error"`},
		{path: "/v1beta/models/gemini:generateContent", want: `"status":"UNAUTHENTICATED"`},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			body := &unreadBody{}
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			req.Body = body
			rr := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusUnauthorized || !strings.Contains(rr.Body.String(), tt.want) {
				t.Fatalf("response = %d %s, want provider-native 401 containing %s", rr.Code, rr.Body.String(), tt.want)
			}
			if body.reads != 0 {
				t.Fatalf("body reads = %d, want 0 before authentication", body.reads)
			}
		})
	}
}

type unreadBody struct{ reads int }

func (b *unreadBody) Read([]byte) (int, error) {
	b.reads++
	return 0, errors.New("body must not be read")
}

func (*unreadBody) Close() error { return nil }

var _ io.ReadCloser = (*unreadBody)(nil)

func TestModelsRequiresAuthAndFiltersPrincipalACLs(t *testing.T) {
	cfg := loadModelsConfig(t)
	srv := gwapi.NewServer(nil, gateway.NewConfigStore(cfg), nil, nil, nil)

	unauthorized := httptest.NewRecorder()
	srv.Handler().ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer restricted-token")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("authenticated status = %d, want %d: %s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var response struct {
		Data []gateway.ModelDescriptor `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Data) != 2 || response.Data[0].ID != "anthropic-only" || response.Data[1].ID != "shared" {
		t.Fatalf("filtered models = %#v, want anthropic-only and shared", response.Data)
	}
}

func TestModelsAllowsAnonymousOnlyWhenExplicit(t *testing.T) {
	cfg := loadModelsConfig(t)
	cfg.Auth.AllowAnonymous = true
	srv := gwapi.NewServer(nil, gateway.NewConfigStore(cfg), nil, nil, nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rr.Code, http.StatusOK, rr.Body.String())
	}
}

func TestModelsIntersectsDynamicQuotaAllowlists(t *testing.T) {
	cfg := loadModelsConfig(t)
	limits := store.NewMemoryQuotaLimitStore()
	if err := limits.Put(t.Context(), "restricted", gateway.LimitSpec{
		ModelAllowlist:    []string{"shared"},
		ProviderAllowlist: []string{"anthropic"},
	}); err != nil {
		t.Fatal(err)
	}
	srv := gwapi.NewServer(nil, gateway.NewConfigStore(cfg), nil, limits, nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer restricted-token")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	var response struct {
		Data []gateway.ModelDescriptor `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Data) != 1 || response.Data[0].ID != "shared" {
		t.Fatalf("quota-filtered models = %#v, want only shared", response.Data)
	}
}

func loadModelsConfig(t *testing.T) *gateway.Snapshot {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := `
auth:
  tokens:
    restricted-token:
      id: restricted
      models: [openai-only, anthropic-only, shared]
      providers: [anthropic]
routes:
  openai-only:
    provider: openai
    model: openai-only
    base_url: https://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      max_output_tokens: 4096
      tokenizer: cl100k_base
  anthropic-only:
    provider: anthropic
    model: anthropic-only
    base_url: https://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      max_output_tokens: 4096
      tokenizer: cl100k_base
  shared-openai:
    provider: openai
    model: shared
    base_url: https://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      max_output_tokens: 4096
      tokenizer: cl100k_base
  shared-anthropic:
    provider: anthropic
    model: shared
    base_url: https://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      max_output_tokens: 4096
      tokenizer: cl100k_base
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := gateway.LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
