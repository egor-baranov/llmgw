package gateway_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
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
  allow_anonymous: true
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
      max_output_tokens: 4096
      tokenizer: cl100k_base
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
	if got := store.Load().Routes["demo"].UpstreamModel; got != "model-b" {
		t.Fatalf("default upstream model = %q, want public model model-b", got)
	}
}

func TestConfigStoreRebuildsCatalogWhenPublishingCopiedSnapshot(t *testing.T) {
	original, err := loadConfigText(t, minimalAnonymousConfig)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		publish func(*gateway.Snapshot) *gateway.ConfigStore
	}{
		{name: "constructor", publish: gateway.NewConfigStore},
		{name: "swap", publish: func(candidate *gateway.Snapshot) *gateway.ConfigStore {
			store := gateway.NewConfigStore(original)
			store.Swap(candidate)
			return store
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replacement := *original
			replacementRoute := *original.Routes["demo"]
			replacementRoute.Name = "replacement"
			replacementRoute.Model = "replacement-model"
			replacementRoute.UpstreamModel = "replacement-upstream-model"
			replacement.Routes = map[string]*gateway.Route{"replacement": &replacementRoute}

			published := tt.publish(&replacement).Load()
			if models := published.Models(); len(models) != 1 || models[0].ID != "replacement-model" {
				t.Fatalf("published models = %#v, want only replacement-model", models)
			}
			if !published.HasModel("replacement-model") || published.HasModel("gpt-4o-mini") {
				t.Fatalf("published model lookup retained stale catalog: %#v", published.Models())
			}
			candidates, err := gateway.NewRouter().Resolve(published, &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "replacement-model",
			})
			if err != nil || len(candidates) != 1 || candidates[0].Route.Name != "replacement" {
				t.Fatalf("Resolve() = %#v, %v; want replacement route", candidates, err)
			}
			if _, err := gateway.NewRouter().Resolve(published, &gateway.Request{
				Operation: gateway.OpChatCompletions,
				Model:     "gpt-4o-mini",
			}); gateway.AsAPIError(err).Code != "model_not_found" {
				t.Fatalf("stale-model Resolve() error = %#v, want model_not_found", gateway.AsAPIError(err))
			}

			// Publishing owns the route map used by its private index.
			replacement.Routes["replacement"].Model = "mutated-after-publication"
			if !published.HasModel("replacement-model") || published.Routes["replacement"].Model != "replacement-model" {
				t.Fatal("published route catalog changed through the caller-owned snapshot")
			}
		})
	}
}

func TestLoadConfigFileResolvesJWTSecretEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.example.yaml")
	t.Setenv("LLMGW_TEST_JWT_SECRET", "test-secret-that-is-at-least-32-bytes")
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
      max_output_tokens: 4096
      tokenizer: cl100k_base
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := gateway.LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Auth.JWT.Secret != "test-secret-that-is-at-least-32-bytes" {
		t.Fatalf("jwt secret = %q, want configured value", cfg.Auth.JWT.Secret)
	}
	if cfg.Auth.JWT.Algorithm != "HS256" {
		t.Fatalf("jwt algorithm = %q, want HS256", cfg.Auth.JWT.Algorithm)
	}
}

func TestLoadConfigFileResolvesStoreDefaultsAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.example.yaml")
	t.Setenv("LLMGW_TEST_POSTGRES_DSN", "postgres://user:pass@localhost/db")
	content := `
server:
  listen: ":0"
auth:
  allow_anonymous: true
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
      max_output_tokens: 4096
      tokenizer: cl100k_base
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
	if got.Claims.Permissions != "permissions" {
		t.Fatalf("permission claim = %q, want permissions", got.Claims.Permissions)
	}
}

func TestLoadConfigFileRejectsInvalidJWTVerificationKeys(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	weakRSAKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	ecdsaKey, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		algorithm string
		key       string
		want      string
	}{
		{name: "malformed PEM", algorithm: "RS256", key: "not-a-public-key", want: "invalid jwt public key PEM"},
		{name: "weak RSA key", algorithm: "RS256", key: publicKeyPEM(t, &weakRSAKey.PublicKey), want: "at least 2048 bits"},
		{name: "wrong key type", algorithm: "ES256", key: publicKeyPEM(t, &rsaKey.PublicKey), want: "incompatible"},
		{name: "wrong ECDSA curve", algorithm: "ES256", key: publicKeyPEM(t, &ecdsaKey.PublicKey), want: "requires curve P-256"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jwtConfig := "  jwt:\n    algorithm: " + tt.algorithm + "\n    public_key: |\n" + indentYAML(tt.key, 6)
			content := strings.Replace(minimalAnonymousConfig, "  allow_anonymous: true", "  allow_anonymous: true\n"+jwtConfig, 1)
			_, err := loadConfigText(t, content)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadConfigFile() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestLoadConfigFileRejectsShortJWTSecret(t *testing.T) {
	content := strings.Replace(minimalAnonymousConfig, "  allow_anonymous: true", `  allow_anonymous: true
  jwt:
    algorithm: HS256
    secret: too-short`, 1)
	_, err := loadConfigText(t, content)
	if err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
		t.Fatalf("LoadConfigFile() error = %v, want minimum HMAC key strength error", err)
	}
}

func publicKeyPEM(t *testing.T, key any) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func indentYAML(value string, spaces int) string {
	prefix := strings.Repeat(" ", spaces)
	lines := strings.Split(strings.TrimSuffix(value, "\n"), "\n")
	return prefix + strings.Join(lines, "\n"+prefix) + "\n"
}

func TestLoadConfigFileResolvesStaticTokenEnv(t *testing.T) {
	t.Setenv("LLMGW_TEST_BEARER_TOKEN", "generated-test-token")
	cfg, err := loadConfigText(t, strings.Replace(minimalAnonymousConfig, "allow_anonymous: true", `allow_anonymous: false
  token_envs:
    LLMGW_TEST_BEARER_TOKEN:
      id: env-principal
      key_id: env-key
      permissions: [manage_limits]`, 1))
	if err != nil {
		t.Fatal(err)
	}
	principal, ok := cfg.Auth.Tokens["generated-test-token"]
	if !ok || principal.ID != "env-principal" || !principal.HasPermission(gateway.PermissionManageLimits) {
		t.Fatalf("resolved principal = %#v, want env-backed limits manager", principal)
	}
}

func TestLoadConfigFilePreservesPublicAndUpstreamModels(t *testing.T) {
	content := strings.Replace(minimalAnonymousConfig, "    base_url:", "    upstream_model: provider-model\n    base_url:", 1)
	cfg, err := loadConfigText(t, content)
	if err != nil {
		t.Fatal(err)
	}
	route := cfg.Routes["demo"]
	if route.Model != "gpt-4o-mini" || route.UpstreamModel != "provider-model" {
		t.Fatalf("route models = public %q upstream %q", route.Model, route.UpstreamModel)
	}
	if got := cfg.Models(); len(got) != 1 || got[0].ID != "gpt-4o-mini" {
		t.Fatalf("public model catalog = %#v, want public alias", got)
	}
	if !cfg.HasModel("gpt-4o-mini") || cfg.HasModel("provider-model") {
		t.Fatalf("HasModel() did not preserve the public-only catalog")
	}
	models := cfg.Models()
	models[0].ID = "mutated-copy"
	if !cfg.HasModel("gpt-4o-mini") {
		t.Fatal("mutating Models() result changed the immutable model catalog")
	}
}

func TestLoadConfigFileRequiresAndResolvesConfiguredRouteEnvironment(t *testing.T) {
	const envName = "LLMGW_TEST_ROUTE_API_KEY"
	_ = os.Unsetenv(envName)
	content := strings.Replace(minimalAnonymousConfig, "    base_url:", "    api_key_env: "+envName+"\n    base_url:", 1)
	if _, err := loadConfigText(t, content); err == nil || !strings.Contains(err.Error(), envName) {
		t.Fatalf("missing route env error = %v, want %s", err, envName)
	}
	t.Setenv(envName, "resolved-route-key")
	cfg, err := loadConfigText(t, content)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Routes["demo"].APIKey; got != "resolved-route-key" {
		t.Fatalf("resolved API key = %q", got)
	}
	withLiteral := strings.Replace(content, "    api_key_env:", "    api_key: inline-key\n    api_key_env:", 1)
	if _, err := loadConfigText(t, withLiteral); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("literal+env error = %v, want mutually exclusive", err)
	}
}

func TestLoadConfigFileRejectsInconsistentProviderConcurrency(t *testing.T) {
	content := strings.Replace(minimalAnonymousConfig, "    capabilities:", "    limits:\n      provider_concurrency: 1\n    capabilities:", 1) + `
  second:
    provider: openai
    model: second-model
    base_url: http://example.invalid/v1
    limits:
      provider_concurrency: 2
    capabilities:
      operations: [chat.completions]
      max_output_tokens: 4096
      tokenizer: cl100k_base
`
	if _, err := loadConfigText(t, content); err == nil || !strings.Contains(err.Error(), "consistent limits.provider_concurrency") {
		t.Fatalf("provider concurrency error = %v", err)
	}
}

func TestExampleConfigRequiresRuntimeBearerToken(t *testing.T) {
	path := filepath.Join("..", "..", "..", "config", "config.example.yaml")
	previous, existed := os.LookupEnv("LLMGW_BEARER_TOKEN")
	if err := os.Unsetenv("LLMGW_BEARER_TOKEN"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if existed {
			_ = os.Setenv("LLMGW_BEARER_TOKEN", previous)
		} else {
			_ = os.Unsetenv("LLMGW_BEARER_TOKEN")
		}
	})

	if _, err := gateway.LoadConfigFile(path); err == nil || !strings.Contains(err.Error(), "LLMGW_BEARER_TOKEN") {
		t.Fatalf("LoadConfigFile() error = %v, want required runtime bearer token error", err)
	}
	if err := os.Setenv("LLMGW_BEARER_TOKEN", "generated-test-token"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	t.Setenv("ANTHROPIC_API_KEY", "test-anthropic-key")
	t.Setenv("GEMINI_API_KEY", "test-gemini-key")
	cfg, err := gateway.LoadConfigFile(path)
	if err != nil {
		t.Fatalf("LoadConfigFile() with bearer token error = %v", err)
	}
	if _, ok := cfg.Auth.Tokens["generated-test-token"]; !ok {
		t.Fatal("example config did not resolve LLMGW_BEARER_TOKEN")
	}
}

func TestLoadConfigFileRequiresExplicitAuthenticationMode(t *testing.T) {
	_, err := loadConfigText(t, strings.Replace(minimalAnonymousConfig, "  allow_anonymous: true\n", "", 1))
	if err == nil || !strings.Contains(err.Error(), "authentication is not configured") {
		t.Fatalf("LoadConfigFile() error = %v, want fail-closed authentication error", err)
	}
}

func TestLoadConfigFileRejectsUnusableWhitespacePaddedBearerToken(t *testing.T) {
	content := strings.Replace(minimalAnonymousConfig, "  allow_anonymous: true", `  allow_anonymous: false
  tokens:
    " padded-token ":
      id: principal`, 1)
	_, err := loadConfigText(t, content)
	if err == nil || !strings.Contains(err.Error(), "leading or trailing whitespace") {
		t.Fatalf("LoadConfigFile() error = %v, want whitespace-padded bearer token rejection", err)
	}
}

func TestLoadConfigFileValidatesProviderReferencesAgainstConfiguredRoutes(t *testing.T) {
	custom := strings.Replace(minimalAnonymousConfig, "provider: openai", "provider: acme", 1)
	custom = strings.Replace(custom, "  allow_anonymous: true", `  allow_anonymous: false
  tokens:
    test-token:
      id: custom-client
      providers: [acme]`, 1)
	if _, err := loadConfigText(t, custom); err != nil {
		t.Fatalf("custom provider reference was rejected: %v", err)
	}

	unknownPrincipal := strings.Replace(custom, "providers: [acme]", "providers: [other]", 1)
	if _, err := loadConfigText(t, unknownPrincipal); err == nil || !strings.Contains(err.Error(), "no configured route") {
		t.Fatalf("unknown principal provider error = %v", err)
	}

	unknownQuota := custom + `
quota:
  profiles:
    restricted:
      provider_allowlist: [other]
  keys:
    custom-client: restricted
`
	if _, err := loadConfigText(t, unknownQuota); err == nil || !strings.Contains(err.Error(), "no configured route") {
		t.Fatalf("unknown quota provider error = %v", err)
	}
}

func TestLoadConfigFileRejectsMissingCredentialEnvironment(t *testing.T) {
	for _, name := range []string{"LLMGW_TEST_MISSING_TOKEN", "LLMGW_TEST_MISSING_JWT_SECRET", "LLMGW_TEST_MISSING_JWT_PUBLIC_KEY"} {
		_ = os.Unsetenv(name)
	}
	tests := []struct {
		name string
		auth string
		want string
	}{
		{
			name: "static token",
			auth: `  token_envs:
    LLMGW_TEST_MISSING_TOKEN:
      id: principal`,
			want: "LLMGW_TEST_MISSING_TOKEN",
		},
		{
			name: "jwt secret",
			auth: `  jwt:
    algorithm: HS256
    secret_env: LLMGW_TEST_MISSING_JWT_SECRET`,
			want: "LLMGW_TEST_MISSING_JWT_SECRET",
		},
		{
			name: "jwt public key",
			auth: `  jwt:
    algorithm: RS256
    public_key_env: LLMGW_TEST_MISSING_JWT_PUBLIC_KEY`,
			want: "LLMGW_TEST_MISSING_JWT_PUBLIC_KEY",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := strings.Replace(minimalAnonymousConfig, "  allow_anonymous: true", tt.auth, 1)
			_, err := loadConfigText(t, content)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadConfigFile() error = %v, want missing %s error", err, tt.want)
			}
		})
	}
}

func TestLoadConfigFileUsesStrictYAMLAndValidation(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"unknown field", strings.Replace(minimalAnonymousConfig, "  allow_anonymous: true", "  allow_anonymous: true\n  unknown_auth_option: true", 1), "field unknown_auth_option not found"},
		{"null route", strings.Replace(minimalAnonymousConfig, `  demo:
    provider: openai
    model: gpt-4o-mini
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      max_output_tokens: 4096
      tokenizer: cl100k_base`, "  demo: null", 1), "must not be null"},
		{"unsupported operation", strings.Replace(minimalAnonymousConfig, "chat.completions", "images", 1), "unsupported operation"},
		{"missing tokenizer", strings.Replace(minimalAnonymousConfig, "      tokenizer: cl100k_base\n", "", 1), "capabilities.tokenizer is required"},
		{"unknown tokenizer", strings.Replace(minimalAnonymousConfig, "cl100k_base", "typo-tokenizer", 1), "unsupported capabilities.tokenizer"},
		{"missing generative output bound", strings.Replace(minimalAnonymousConfig, "      max_output_tokens: 4096\n", "", 1), "capabilities.max_output_tokens must be greater than zero"},
		{"invalid URL", strings.Replace(minimalAnonymousConfig, "http://example.invalid/v1", "file:///tmp/upstream", 1), "scheme must be http or https"},
		{"URL query", strings.Replace(minimalAnonymousConfig, "http://example.invalid/v1", "https://example.invalid/v1?key=secret", 1), "must not contain a query string"},
		{"URL fragment", strings.Replace(minimalAnonymousConfig, "http://example.invalid/v1", "https://example.invalid/v1#fragment", 1), "must not contain a fragment"},
		{"negative retries", strings.Replace(minimalAnonymousConfig, "    capabilities:", "    retries: -1\n    capabilities:", 1), "retries must be greater than or equal to zero"},
		{"excessive retries", strings.Replace(minimalAnonymousConfig, "    capabilities:", "    retries: 11\n    capabilities:", 1), "retries must not exceed 10"},
		{"excessive weight", strings.Replace(minimalAnonymousConfig, "    capabilities:", "    weight: 1000001\n    capabilities:", 1), "weight must not exceed 1000000"},
		{"negative duration", strings.Replace(minimalAnonymousConfig, "    capabilities:", "    timeout: -1s\n    capabilities:", 1), "timeout must be greater than or equal to zero"},
		{"negative limit", strings.Replace(minimalAnonymousConfig, "    capabilities:", "    limits:\n      rpm: -1\n    capabilities:", 1), "limits.rpm must be greater than or equal to zero"},
		{"unsafe distributed limit", strings.Replace(minimalAnonymousConfig, "    capabilities:", "    limits:\n      rpm: 100000000000000\n    capabilities:", 1), "limits.rpm must not exceed"},
		{"negative multimodal surcharge", strings.Replace(minimalAnonymousConfig, "      tokenizer: cl100k_base", "      vision_input: true\n      vision_input_token_surcharge: -1\n      tokenizer: cl100k_base", 1), "multimodal token surcharges must be greater than or equal to zero"},
		{"vision surcharge without capability", strings.Replace(minimalAnonymousConfig, "      tokenizer: cl100k_base", "      vision_input_token_surcharge: 100\n      tokenizer: cl100k_base", 1), "vision_input_token_surcharge requires vision_input"},
		{"audio surcharge without capability", strings.Replace(minimalAnonymousConfig, "      tokenizer: cl100k_base", "      audio_input_token_surcharge: 100\n      tokenizer: cl100k_base", 1), "audio_input_token_surcharge requires audio"},
		{"multiple documents", minimalAnonymousConfig + "\n---\n{}\n", "multiple YAML documents"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfigText(t, tt.content)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadConfigFile() error = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestLoadConfigFileResolvesSecureRedisURL(t *testing.T) {
	t.Setenv("LLMGW_TEST_REDIS_URL", "rediss://gateway:secret@redis.example.invalid:6380/2")
	content := strings.Replace(minimalAnonymousConfig, "store:\n  mode: memory", `store:
  mode: redis
  redis_url_env: LLMGW_TEST_REDIS_URL
  redis_namespace: tenant-a`, 1)
	cfg, err := loadConfigText(t, content)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store.RedisURL != "rediss://gateway:secret@redis.example.invalid:6380/2" {
		t.Fatalf("resolved Redis URL = %q", cfg.Store.RedisURL)
	}
	if cfg.Store.RedisNamespace != "tenant-a" {
		t.Fatalf("Redis namespace = %q, want tenant-a", cfg.Store.RedisNamespace)
	}
}

func TestLoadConfigFilePreservesLegacyRedisKeyLayoutByDefault(t *testing.T) {
	content := strings.Replace(minimalAnonymousConfig, "store:\n  mode: memory", `store:
  mode: redis
  redis_addr: localhost:6379`, 1)
	cfg, err := loadConfigText(t, content)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store.RedisNamespace != "" {
		t.Fatalf("Redis namespace = %q, want empty legacy layout", cfg.Store.RedisNamespace)
	}
}

func TestLoadConfigFileRejectsAmbiguousOrInvalidRedisConfiguration(t *testing.T) {
	tests := []struct {
		name  string
		store string
		want  string
	}{
		{
			name: "URL and address",
			store: `store:
  mode: redis
  redis_url: redis://localhost:6379/0
  redis_addr: localhost:6379`,
			want: "cannot be combined",
		},
		{
			name: "insecure URL scheme typo",
			store: `store:
  mode: redis
  redis_url: https://redis.example.invalid`,
			want: "scheme must be redis or rediss",
		},
		{
			name: "invalid database path",
			store: `store:
  mode: redis
  redis_url: redis://localhost/not-a-db`,
			want: "invalid database number",
		},
		{
			name: "unknown URL option",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?unknown=true`,
			want: "unexpected option",
		},
		{
			name: "disabled socket timeout",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?read_timeout=0`,
			want: "must not disable timeouts",
		},
		{
			name: "excessive socket timeout",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?dial_timeout=1m`,
			want: "must not exceed 30s",
		},
		{
			name: "negative URL database",
			store: `store:
  mode: redis
  redis_url: redis://localhost/-1`,
			want: "database must be greater than or equal to zero",
		},
		{
			name: "invalid RESP protocol",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?protocol=1`,
			want: "protocol must be 2 or 3",
		},
		{
			name: "excessive retries",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?max_retries=11`,
			want: "max_retries must be between -1 and 10",
		},
		{
			name: "excessive retry backoff",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?max_retry_backoff=6s`,
			want: "max_retry_backoff must not exceed 5s",
		},
		{
			name: "negative pool size",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?pool_size=-1`,
			want: "pool_size must be greater than or equal to zero",
		},
		{
			name: "excessive pool size",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?pool_size=4097`,
			want: "pool_size must not exceed 4096",
		},
		{
			name: "minimum idle exceeds maximum idle",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?pool_size=4&min_idle_conns=3&max_idle_conns=2`,
			want: "min_idle_conns must not exceed max_idle_conns",
		},
		{
			name: "minimum idle exceeds pool",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?pool_size=2&min_idle_conns=3`,
			want: "min_idle_conns must not exceed pool_size",
		},
		{
			name: "maximum idle exceeds pool",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?pool_size=2&max_idle_conns=3`,
			want: "max_idle_conns must not exceed pool_size",
		},
		{
			name: "pool exceeds maximum active",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?pool_size=4&max_active_conns=3`,
			want: "pool_size must not exceed max_active_conns",
		},
		{
			name: "retry backoffs inverted",
			store: `store:
  mode: redis
  redis_url: redis://localhost/0?min_retry_backoff=1s&max_retry_backoff=500ms`,
			want: "min_retry_backoff must not exceed max_retry_backoff",
		},
		{
			name: "missing URL environment variable",
			store: `store:
  mode: redis
  redis_url_env: LLMGW_TEST_MISSING_REDIS_URL`,
			want: "is not set or is empty",
		},
		{
			name: "invalid namespace",
			store: `store:
  mode: redis
  redis_addr: localhost:6379
  redis_namespace: "tenant one"`,
			want: "redis_namespace",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := strings.Replace(minimalAnonymousConfig, "store:\n  mode: memory", tt.store, 1)
			_, err := loadConfigText(t, content)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadConfigFile() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestLoadConfigFileRedactsRedisCredentialsFromParseErrors(t *testing.T) {
	const secret = "distinctive-redis-password"
	storeConfig := `store:
  mode: redis
  redis_url: "redis://gateway:` + secret + `%zz@localhost/0"`
	content := strings.Replace(minimalAnonymousConfig, "store:\n  mode: memory", storeConfig, 1)
	_, err := loadConfigText(t, content)
	if err == nil {
		t.Fatal("malformed Redis URL was accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("Redis config error leaked credentials: %v", err)
	}
	if !strings.Contains(err.Error(), "invalid URL escape") {
		t.Fatalf("Redis config error = %v, want sanitized reason", err)
	}
}

func TestLoadConfigFileValidatesRouteMetadataHeaders(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{
			name: "header name with space",
			content: strings.Replace(minimalAnonymousConfig, "    capabilities:", `    headers:
      "Bad Header": value
    capabilities:`, 1),
			want: "invalid header name",
		},
		{
			name: "header name with non-ASCII byte",
			content: strings.Replace(minimalAnonymousConfig, "    capabilities:", `    headers:
      "X-Café": value
    capabilities:`, 1),
			want: "invalid header name",
		},
		{
			name:    "header value with control byte",
			content: strings.Replace(minimalAnonymousConfig, "    capabilities:", "    headers:\n      X-Test: \"bad\\rvalue\"\n    capabilities:", 1),
			want:    "invalid control characters",
		},
		{
			name: "case-insensitive duplicate header",
			content: strings.Replace(minimalAnonymousConfig, "    capabilities:", `    headers:
      Authorization: first
      authorization: second
    capabilities:`, 1),
			want: "more than once with different casing",
		},
		{
			name: "connection header",
			content: strings.Replace(minimalAnonymousConfig, "    capabilities:", `    headers:
      Connection: keep-alive
    capabilities:`, 1),
			want: "managed by the HTTP transport",
		},
		{
			name: "content length header",
			content: strings.Replace(minimalAnonymousConfig, "    capabilities:", `    headers:
      Content-Length: "1"
    capabilities:`, 1),
			want: "managed by the HTTP transport",
		},
		{
			name: "host header",
			content: strings.Replace(minimalAnonymousConfig, "    capabilities:", `    headers:
      Host: other.example
    capabilities:`, 1),
			want: "managed by the HTTP transport",
		},
		{
			name:    "route name with control byte",
			content: strings.Replace(minimalAnonymousConfig, "  demo:", "  \"bad\\nroute\":", 1),
			want:    "route name",
		},
		{
			name:    "public model with control byte",
			content: strings.Replace(minimalAnonymousConfig, "model: gpt-4o-mini", "model: \"gpt-4o\\rmini\"", 1),
			want:    "model contains invalid HTTP header value bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfigText(t, tt.content)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadConfigFile() error = %v, want error containing %q", err, tt.want)
			}
		})
	}

	valid := strings.Replace(minimalAnonymousConfig, "    capabilities:", `    headers:
      x-custom_trace: value
    capabilities:`, 1)
	cfg, err := loadConfigText(t, valid)
	if err != nil {
		t.Fatalf("LoadConfigFile() rejected valid RFC token header name: %v", err)
	}
	if got := cfg.Routes["demo"].Headers; len(got) != 1 || got["X-Custom_trace"] != "value" {
		t.Fatalf("canonical route headers = %#v", got)
	}
}

func TestLoadConfigFileDefaultsMultimodalTokenSurcharges(t *testing.T) {
	content := strings.Replace(minimalAnonymousConfig, "      operations: [chat.completions]", "      operations: [chat.completions]\n      vision_input: true\n      audio: true", 1)
	cfg, err := loadConfigText(t, content)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := cfg.Routes["demo"].Capabilities
	if capabilities.VisionInputTokenSurcharge != gateway.DefaultVisionInputTokenSurcharge {
		t.Fatalf("vision surcharge = %d, want default %d", capabilities.VisionInputTokenSurcharge, gateway.DefaultVisionInputTokenSurcharge)
	}
	if capabilities.AudioInputTokenSurcharge != gateway.DefaultAudioInputTokenSurcharge {
		t.Fatalf("audio surcharge = %d, want default %d", capabilities.AudioInputTokenSurcharge, gateway.DefaultAudioInputTokenSurcharge)
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

const minimalAnonymousConfig = `
server:
  listen: ":0"
auth:
  allow_anonymous: true
store:
  mode: memory
routes:
  demo:
    provider: openai
    model: gpt-4o-mini
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      max_output_tokens: 4096
      tokenizer: cl100k_base
`

func loadConfigText(t *testing.T, content string) (*gateway.Snapshot, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return gateway.LoadConfigFile(path)
}
