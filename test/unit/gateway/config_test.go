package gateway_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"llmgw/gateway"
)

func TestConfigStoreReloadAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.example.yaml")
	writeConfig := func(model string) {
		t.Helper()
		content := `
server:
  listen: ":0"
auth:
  max_body_bytes: 1024
store:
  mode: memory
routes:
  demo:
    provider: openai
    model: ` + model + `
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
`
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeConfig("model-a")
	cfg, err := gateway.LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	store := gateway.NewConfigStore(cfg)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if store.Load() == nil {
					t.Error("Load() returned nil")
				}
			}
		}()
	}

	writeConfig("model-b")
	if _, err := store.Reload(path); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if got := store.Load().Models()[0].ID; got != "model-b" {
		t.Fatalf("model after reload = %q, want model-b", got)
	}
}

func TestLoadConfigFileResolvesJWTSecretEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.example.yaml")
	if err := os.Setenv("LLMGW_TEST_JWT_SECRET", "jwt-secret"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Unsetenv("LLMGW_TEST_JWT_SECRET") })
	content := `
server:
  listen: ":0"
auth:
  jwt:
    algorithm: hs256
    secret_env: LLMGW_TEST_JWT_SECRET
store:
  mode: memory
routes:
  demo:
    provider: openai
    model: gpt-4o-mini
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := gateway.LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Auth.JWT.Secret != "jwt-secret" {
		t.Fatalf("jwt secret = %q, want jwt-secret", cfg.Auth.JWT.Secret)
	}
	if cfg.Auth.JWT.Algorithm != "HS256" {
		t.Fatalf("jwt algorithm = %q, want HS256", cfg.Auth.JWT.Algorithm)
	}
}

func TestLoadConfigFileResolvesStoreDefaultsAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.example.yaml")
	if err := os.Setenv("LLMGW_TEST_POSTGRES_DSN", "postgres://user:pass@localhost/db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Unsetenv("LLMGW_TEST_POSTGRES_DSN") })
	content := `
server:
  listen: ":0"
store:
  mode: memory
  postgres_dsn_env: LLMGW_TEST_POSTGRES_DSN
routes:
  demo:
    provider: openai
    model: gpt-4o-mini
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := gateway.LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store.PostgresDSN != "postgres://user:pass@localhost/db" {
		t.Fatalf("postgres dsn = %q, want env value", cfg.Store.PostgresDSN)
	}
	if cfg.Store.QuotaTable != "quota_limits" {
		t.Fatalf("quota table = %q, want quota_limits", cfg.Store.QuotaTable)
	}
	if cfg.Store.LimitCacheTTL.Duration != 30*time.Second {
		t.Fatalf("limit cache ttl = %v, want 30s", cfg.Store.LimitCacheTTL.Duration)
	}
}

func TestResolveQuotaScopesUsesKeyProfiles(t *testing.T) {
	cfg := &gateway.Snapshot{
		Quota: gateway.QuotaConfig{
			Profiles: map[string]gateway.LimitSpec{
				"dev": {RPM: 60, DailyTokens: 1000},
			},
			Keys: map[string]string{
				"key-1": "dev",
			},
		},
	}
	scopes := cfg.ResolveQuotaScopes(gateway.Subject{KeyID: "key-1"})
	if len(scopes) != 1 {
		t.Fatalf("len(scopes) = %d, want 1", len(scopes))
	}
	if scopes[0].Ref.Kind != gateway.ScopeKey || scopes[0].Ref.ID != "key-1" {
		t.Fatalf("scope ref = %#v, want key/key-1", scopes[0].Ref)
	}
	if scopes[0].Limits.RPM != 60 || scopes[0].Limits.DailyTokens != 1000 {
		t.Fatalf("scope limits = %#v, want configured profile", scopes[0].Limits)
	}
}

func TestJWTConfigNormalizeSetsDefaultClaims(t *testing.T) {
	got := (gateway.JWTConfig{Secret: "secret"}).Normalize()
	if got.Algorithm != "HS256" {
		t.Fatalf("algorithm = %q, want HS256", got.Algorithm)
	}
	if got.Claims.Principal != "sub" || got.Claims.Name != "name" || got.Claims.KeyID != "key_id" {
		t.Fatalf("claims = %#v, want default principal/name/key_id mappings", got.Claims)
	}
	if got.Claims.Providers != "providers" || got.Claims.Models != "models" || got.Claims.Projects != "projects" {
		t.Fatalf("claims = %#v, want default ACL mappings", got.Claims)
	}
}

func TestDurationJSONRoundTrip(t *testing.T) {
	type payload struct {
		Duration gateway.Duration `json:"duration"`
	}
	in := payload{Duration: gateway.Duration{Duration: 45 * time.Second}}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if string(data) != `{"duration":"45s"}` {
		t.Fatalf("Marshal() = %s, want duration string", data)
	}
	var out payload
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if out.Duration.Duration != 45*time.Second {
		t.Fatalf("duration = %v, want 45s", out.Duration.Duration)
	}
}
