package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
	"gopkg.in/yaml.v3"
)

type Snapshot struct {
	Server        ServerConfig        `yaml:"server"`
	Auth          AuthConfig          `yaml:"auth"`
	Store         StoreConfig         `yaml:"store"`
	Quota         QuotaConfig         `yaml:"quota"`
	Routes        map[string]*Route   `yaml:"routes"`
	Telemetry     TelemetryConfig     `yaml:"telemetry"`
	LoadedAt      time.Time           `yaml:"-"`
	models        []ModelDescriptor   `yaml:"-"`
	modelSet      map[string]struct{} `yaml:"-"`
	routesByModel map[string][]*Route `yaml:"-"`
}

type ServerConfig struct {
	Listen             string   `yaml:"listen"`
	ReadTimeout        Duration `yaml:"read_timeout"`
	WriteTimeout       Duration `yaml:"write_timeout"`
	StreamWriteTimeout Duration `yaml:"stream_write_timeout"`
	IdleTimeout        Duration `yaml:"idle_timeout"`
	ShutdownTimeout    Duration `yaml:"shutdown_timeout"`
}

type AuthConfig struct {
	RequireUser    bool                 `yaml:"require_user"`
	RequireProject bool                 `yaml:"require_project"`
	AllowAnonymous bool                 `yaml:"allow_anonymous"`
	MaxBodyBytes   int64                `yaml:"max_body_bytes"`
	Tokens         map[string]Principal `yaml:"tokens"`
	TokenEnvs      map[string]Principal `yaml:"token_envs"`
	JWT            JWTConfig            `yaml:"jwt"`
}

const (
	PermissionManageLimits   = "manage_limits"
	PermissionViewOperations = "view_operations"
)

type Principal struct {
	ID          string   `yaml:"id"`
	Name        string   `yaml:"name"`
	KeyID       string   `yaml:"key_id"`
	Providers   []string `yaml:"providers"`
	Models      []string `yaml:"models"`
	Projects    []string `yaml:"projects"`
	Permissions []string `yaml:"permissions"`
	QuotaKey    string   `yaml:"quota_key"`
}

type JWTConfig struct {
	Algorithm       string          `yaml:"algorithm"`
	Issuer          string          `yaml:"issuer"`
	Audience        string          `yaml:"audience"`
	Secret          string          `yaml:"secret"`
	SecretEnv       string          `yaml:"secret_env"`
	PublicKey       string          `yaml:"public_key"`
	PublicKeyEnv    string          `yaml:"public_key_env"`
	Claims          JWTClaimMapping `yaml:"claims"`
	verificationKey any             `yaml:"-"`
}

type JWTClaimMapping struct {
	Principal   string `yaml:"principal"`
	Name        string `yaml:"name"`
	KeyID       string `yaml:"key_id"`
	Providers   string `yaml:"providers"`
	Models      string `yaml:"models"`
	Projects    string `yaml:"projects"`
	Permissions string `yaml:"permissions"`
}

type StoreConfig struct {
	Mode             string   `yaml:"mode"`
	RedisURL         string   `yaml:"redis_url"`
	RedisURLEnv      string   `yaml:"redis_url_env"`
	RedisNamespace   string   `yaml:"redis_namespace"`
	RedisAddr        string   `yaml:"redis_addr"`
	RedisPasswordEnv string   `yaml:"redis_password_env"`
	RedisDB          int      `yaml:"redis_db"`
	PostgresDSN      string   `yaml:"postgres_dsn"`
	PostgresDSNEnv   string   `yaml:"postgres_dsn_env"`
	QuotaTable       string   `yaml:"quota_table"`
	LimitCacheTTL    Duration `yaml:"limit_cache_ttl"`
	ReservationTTL   Duration `yaml:"reservation_ttl"`
	StartupTimeout   Duration `yaml:"startup_timeout"`
}

type TelemetryConfig struct {
	ServiceName string `yaml:"service_name"`
}

type QuotaConfig struct {
	Profiles map[string]LimitSpec `yaml:"profiles"`
	Keys     map[string]string    `yaml:"keys"`
}

type LimitSpec struct {
	RPM               int64    `yaml:"rpm" json:"rpm,omitempty"`
	TPM               int64    `yaml:"tpm" json:"tpm,omitempty"`
	MaxParallel       int64    `yaml:"max_parallel" json:"max_parallel,omitempty"`
	MaxSpendMicros    int64    `yaml:"max_spend_micros" json:"max_spend_micros,omitempty"`
	SoftSpendMicros   int64    `yaml:"soft_spend_micros" json:"soft_spend_micros,omitempty"`
	DailyTokens       int64    `yaml:"daily_tokens" json:"daily_tokens,omitempty"`
	MonthlyTokens     int64    `yaml:"monthly_tokens" json:"monthly_tokens,omitempty"`
	BudgetDuration    Duration `yaml:"budget_duration" json:"budget_duration,omitempty"`
	MaxInputTokens    int64    `yaml:"max_input_tokens" json:"max_input_tokens,omitempty"`
	MaxOutputTokens   int64    `yaml:"max_output_tokens" json:"max_output_tokens,omitempty"`
	ModelAllowlist    []string `yaml:"model_allowlist" json:"model_allowlist,omitempty"`
	ProviderAllowlist []string `yaml:"provider_allowlist" json:"provider_allowlist,omitempty"`
}

// MaximumQuotaValue keeps distributed accounting values exactly representable
// through Redis Lua's 14-significant-digit CJSON round trips.
const MaximumQuotaValue int64 = 99_999_999_999_999

type Route struct {
	Name          string            `yaml:"-"`
	Provider      string            `yaml:"provider"`
	Backend       string            `yaml:"backend"`
	Model         string            `yaml:"model"`
	UpstreamModel string            `yaml:"upstream_model"`
	BaseURL       string            `yaml:"base_url"`
	APIKey        string            `yaml:"api_key"`
	APIKeyEnv     string            `yaml:"api_key_env"`
	Project       string            `yaml:"project"`
	ProjectEnv    string            `yaml:"project_env"`
	Location      string            `yaml:"location"`
	LocationEnv   string            `yaml:"location_env"`
	APIVersion    string            `yaml:"api_version"`
	Headers       map[string]string `yaml:"headers"`
	Priority      int               `yaml:"priority"`
	Weight        int               `yaml:"weight"`
	Retries       int               `yaml:"retries"`
	Timeout       Duration          `yaml:"timeout"`
	Limits        LimitConfig       `yaml:"limits"`
	Circuit       CircuitConfig     `yaml:"circuit"`
	Capabilities  Capability        `yaml:"capabilities"`
	Pricing       Pricing           `yaml:"pricing"`
}

const (
	// MaximumRouteRetries bounds request amplification and keeps retry/timeout
	// arithmetic safe across configuration reloads.
	MaximumRouteRetries = 10
	// MaximumRouteWeight is intentionally generous while keeping configured
	// weighted groups far from integer overflow.
	MaximumRouteWeight = 1_000_000
)

type LimitConfig struct {
	RPM                 int64 `yaml:"rpm"`
	TPM                 int64 `yaml:"tpm"`
	Concurrency         int64 `yaml:"concurrency"`
	ProviderConcurrency int64 `yaml:"provider_concurrency"`
	MaxBodyBytes        int64 `yaml:"max_body_bytes"`
	MaxResponseBytes    int64 `yaml:"max_response_bytes"`
}

type CircuitConfig struct {
	Failures int      `yaml:"failures"`
	Cooldown Duration `yaml:"cooldown"`
}

type Pricing struct {
	InputPer1M      float64                        `yaml:"input_per_1m"`
	OutputPer1M     float64                        `yaml:"output_per_1m"`
	CacheReadPer1M  float64                        `yaml:"cache_read_per_1m"`
	CacheWritePer1M float64                        `yaml:"cache_write_per_1m"`
	ProviderUnits   map[string]ProviderUnitPricing `yaml:"provider_units"`
}

type ProviderUnitPricing struct {
	MicrosPerUnit      float64 `yaml:"micros_per_unit"`
	MaxUnitsPerRequest int64   `yaml:"max_units_per_request"`
}

type Capability struct {
	Operations                []Operation `yaml:"operations"`
	Streaming                 bool        `yaml:"streaming"`
	ToolCalling               bool        `yaml:"tool_calling"`
	StructuredOutput          bool        `yaml:"structured_output"`
	VisionInput               bool        `yaml:"vision_input"`
	Audio                     bool        `yaml:"audio"`
	Reasoning                 bool        `yaml:"reasoning"`
	MaxInputTokens            int         `yaml:"max_input_tokens"`
	MaxOutputTokens           int         `yaml:"max_output_tokens"`
	Tokenizer                 string      `yaml:"tokenizer"`
	VisionInputTokenSurcharge int         `yaml:"vision_input_token_surcharge"`
	AudioInputTokenSurcharge  int         `yaml:"audio_input_token_surcharge"`
}

const (
	DefaultVisionInputTokenSurcharge = 1024
	DefaultAudioInputTokenSurcharge  = 8192
)

type ModelDescriptor struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	OwnedBy string `json:"owned_by"`
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Value == "" {
		return nil
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	if d.Duration == 0 {
		return json.Marshal("")
	}
	return json.Marshal(d.Duration.String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		d.Duration = 0
		return nil
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	if text == "" {
		d.Duration = 0
		return nil
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

type ConfigStore struct {
	current atomic.Pointer[Snapshot]
}

func LoadConfigFile(path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Snapshot
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode config: multiple YAML documents are not supported")
		}
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if err := cfg.finalize(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func NewConfigStore(cfg *Snapshot) *ConfigStore {
	store := &ConfigStore{}
	store.Swap(cfg)
	return store
}

func (s *ConfigStore) Load() *Snapshot {
	return s.current.Load()
}

func (s *ConfigStore) Swap(cfg *Snapshot) {
	s.current.Store(snapshotForPublication(cfg))
}

// Reload applies structural configuration validation and atomically swaps the
// snapshot. Applications with a runtime provider registry must additionally
// call ValidateProviders before accepting a reload; app.Service.Reload does so.
func (s *ConfigStore) Reload(path string) (*Snapshot, error) {
	cfg, err := LoadConfigFile(path)
	if err != nil {
		return nil, err
	}
	s.Swap(cfg)
	return cfg, nil
}

func (s *Snapshot) Models() []ModelDescriptor {
	out := make([]ModelDescriptor, len(s.models))
	copy(out, s.models)
	return out
}

// HasModel checks the immutable catalog without allocating a defensive copy.
func (s *Snapshot) HasModel(model string) bool {
	if s == nil {
		return false
	}
	if s.modelSet != nil {
		_, ok := s.modelSet[model]
		return ok
	}
	for _, descriptor := range s.models {
		if descriptor.ID == model {
			return true
		}
	}
	return false
}

// snapshotForPublication keeps the derived catalog consistent with Routes and
// prevents callers from mutating the published route map after an atomic swap.
func snapshotForPublication(source *Snapshot) *Snapshot {
	if source == nil {
		return nil
	}
	published := *source
	published.Routes = make(map[string]*Route, len(source.Routes))
	for name, route := range source.Routes {
		published.Routes[name] = cloneRoute(route)
	}
	published.rebuildRouteCatalog()
	return &published
}

func (s *Snapshot) rebuildRouteCatalog() {
	modelSet := make(map[string]struct{})
	routesByModel := make(map[string][]*Route)
	for _, route := range s.Routes {
		if route == nil {
			continue
		}
		modelSet[route.Model] = struct{}{}
		routesByModel[route.Model] = append(routesByModel[route.Model], route)
	}

	models := make([]ModelDescriptor, 0, len(modelSet))
	for model := range modelSet {
		models = append(models, ModelDescriptor{ID: model, Object: "model", OwnedBy: "llmgw"})
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	for model := range routesByModel {
		sort.Slice(routesByModel[model], func(i, j int) bool {
			return routesByModel[model][i].Name < routesByModel[model][j].Name
		})
	}
	s.models = models
	s.modelSet = modelSet
	s.routesByModel = routesByModel
}

func (s *Snapshot) finalize() error {
	if s == nil {
		return fmt.Errorf("configuration is nil")
	}
	if err := s.resolveAndValidateAuth(); err != nil {
		return err
	}
	if err := validateDurationsAndLimits(s); err != nil {
		return err
	}
	if s.Server.Listen == "" {
		s.Server.Listen = ":8080"
	}
	if s.Server.ShutdownTimeout.Duration == 0 {
		s.Server.ShutdownTimeout.Duration = 10 * time.Second
	}
	if s.Server.ReadTimeout.Duration == 0 {
		s.Server.ReadTimeout.Duration = 15 * time.Second
	}
	if s.Server.IdleTimeout.Duration == 0 {
		s.Server.IdleTimeout.Duration = 60 * time.Second
	}
	if s.Server.StreamWriteTimeout.Duration == 0 {
		s.Server.StreamWriteTimeout.Duration = 30 * time.Second
	}
	if s.Auth.MaxBodyBytes == 0 {
		s.Auth.MaxBodyBytes = 4 << 20
	}
	if s.Store.Mode == "" {
		s.Store.Mode = "memory"
	}
	s.Store.Mode = strings.ToLower(strings.TrimSpace(s.Store.Mode))
	if s.Store.Mode != "memory" && s.Store.Mode != "redis" {
		return fmt.Errorf("store.mode must be one of memory or redis, got %q", s.Store.Mode)
	}
	s.Store.RedisURL = strings.TrimSpace(s.Store.RedisURL)
	s.Store.RedisURLEnv = strings.TrimSpace(s.Store.RedisURLEnv)
	s.Store.RedisNamespace = strings.TrimSpace(s.Store.RedisNamespace)
	s.Store.RedisAddr = strings.TrimSpace(s.Store.RedisAddr)
	s.Store.RedisPasswordEnv = strings.TrimSpace(s.Store.RedisPasswordEnv)
	if s.Store.RedisURL != "" && s.Store.RedisURLEnv != "" {
		return fmt.Errorf("store.redis_url and store.redis_url_env are mutually exclusive")
	}
	if s.Store.Mode == "redis" {
		if s.Store.RedisURL == "" && s.Store.RedisURLEnv != "" {
			value, ok := os.LookupEnv(s.Store.RedisURLEnv)
			if !ok || strings.TrimSpace(value) == "" {
				return fmt.Errorf("store.redis_url_env %q is not set or is empty", s.Store.RedisURLEnv)
			}
			s.Store.RedisURL = strings.TrimSpace(value)
		}
		hasURL := s.Store.RedisURL != ""
		hasAddressOptions := s.Store.RedisAddr != "" || s.Store.RedisPasswordEnv != "" || s.Store.RedisDB != 0
		if hasURL && hasAddressOptions {
			return fmt.Errorf("store.redis_url cannot be combined with redis_addr, redis_password_env, or redis_db")
		}
		if !hasURL && s.Store.RedisAddr == "" {
			return fmt.Errorf("store.redis_addr or store.redis_url is required when store.mode is redis")
		}
		if hasURL {
			if err := validateRedisURL(s.Store.RedisURL); err != nil {
				return fmt.Errorf("store.redis_url is invalid: %w", err)
			}
		} else if s.Store.RedisPasswordEnv != "" {
			value, ok := os.LookupEnv(s.Store.RedisPasswordEnv)
			if !ok || strings.TrimSpace(value) == "" {
				return fmt.Errorf("store.redis_password_env %q is not set or is empty", s.Store.RedisPasswordEnv)
			}
		}
		if s.Store.RedisNamespace != "" && !validRedisNamespace(s.Store.RedisNamespace) {
			return fmt.Errorf("store.redis_namespace must contain only letters, digits, '.', '_', or '-' and be at most 128 bytes")
		}
	}
	if s.Store.PostgresDSN != "" && s.Store.PostgresDSNEnv != "" {
		return fmt.Errorf("store.postgres_dsn and store.postgres_dsn_env are mutually exclusive")
	}
	if s.Store.PostgresDSN == "" && s.Store.PostgresDSNEnv != "" {
		value, ok := os.LookupEnv(s.Store.PostgresDSNEnv)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("store.postgres_dsn_env %q is not set or is empty", s.Store.PostgresDSNEnv)
		}
		s.Store.PostgresDSN = value
	}
	if s.Store.QuotaTable == "" {
		s.Store.QuotaTable = "quota_limits"
	}
	if s.Store.LimitCacheTTL.Duration == 0 {
		s.Store.LimitCacheTTL.Duration = 30 * time.Second
	}
	if s.Store.ReservationTTL.Duration == 0 {
		s.Store.ReservationTTL.Duration = 10 * time.Minute
	}
	if s.Store.StartupTimeout.Duration == 0 {
		s.Store.StartupTimeout.Duration = 10 * time.Second
	}
	if len(s.Routes) == 0 {
		return fmt.Errorf("no routes configured")
	}
	providerConcurrency := make(map[string]int64)
	for name, route := range s.Routes {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("route name must not be empty")
		}
		if route == nil {
			return fmt.Errorf("route %s must not be null", name)
		}
		route.Name = name
		route.Provider = strings.ToLower(strings.TrimSpace(route.Provider))
		route.Backend = strings.ToLower(strings.TrimSpace(route.Backend))
		route.Model = strings.TrimSpace(route.Model)
		route.UpstreamModel = strings.TrimSpace(route.UpstreamModel)
		route.Capabilities.Tokenizer = strings.ToLower(strings.TrimSpace(route.Capabilities.Tokenizer))
		if route.Capabilities.VisionInput && route.Capabilities.VisionInputTokenSurcharge == 0 {
			route.Capabilities.VisionInputTokenSurcharge = DefaultVisionInputTokenSurcharge
		}
		if route.Capabilities.Audio && route.Capabilities.AudioInputTokenSurcharge == 0 {
			route.Capabilities.AudioInputTokenSurcharge = DefaultAudioInputTokenSurcharge
		}
		if route.UpstreamModel == "" {
			route.UpstreamModel = route.Model
		}
		route.BaseURL = strings.TrimSpace(route.BaseURL)
		if err := resolveRouteEnvironment(name, route); err != nil {
			return err
		}
		normalizedHeaders, err := normalizeRouteHeaders(name, route.Headers)
		if err != nil {
			return err
		}
		route.Headers = normalizedHeaders
		if err := validateRoute(name, route); err != nil {
			return err
		}
		if configured, ok := providerConcurrency[route.Provider]; ok && configured != route.Limits.ProviderConcurrency {
			return fmt.Errorf("routes for provider %s must use one consistent limits.provider_concurrency value (got %d and %d)", route.Provider, configured, route.Limits.ProviderConcurrency)
		}
		providerConcurrency[route.Provider] = route.Limits.ProviderConcurrency
		if route.Weight == 0 {
			route.Weight = 1
		}
		if route.Timeout.Duration == 0 {
			route.Timeout.Duration = 30 * time.Second
		}
		if route.Circuit.Failures == 0 {
			route.Circuit.Failures = 3
		}
		if route.Circuit.Cooldown.Duration == 0 {
			route.Circuit.Cooldown.Duration = 30 * time.Second
		}
	}
	for name, profile := range s.Quota.Profiles {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("quota profile name must not be empty")
		}
		if err := validateLimitSpec("quota.profiles."+name, profile); err != nil {
			return err
		}
	}
	if err := validateConfiguredProviderReferences(s); err != nil {
		return err
	}
	for keyID, profile := range s.Quota.Keys {
		if strings.TrimSpace(keyID) == "" {
			return fmt.Errorf("quota key id must not be empty")
		}
		if _, ok := s.Quota.Profiles[profile]; !ok {
			return fmt.Errorf("quota.keys.%s references unknown profile %q", keyID, profile)
		}
	}
	s.rebuildRouteCatalog()
	s.LoadedAt = time.Now()
	return nil
}

func resolveRouteEnvironment(name string, route *Route) error {
	if route == nil {
		return fmt.Errorf("route %s must not be null", name)
	}
	fields := []struct {
		label   string
		literal *string
		envName *string
	}{
		{label: "api_key", literal: &route.APIKey, envName: &route.APIKeyEnv},
		{label: "project", literal: &route.Project, envName: &route.ProjectEnv},
		{label: "location", literal: &route.Location, envName: &route.LocationEnv},
	}
	for _, field := range fields {
		*field.literal = strings.TrimSpace(*field.literal)
		*field.envName = strings.TrimSpace(*field.envName)
		if *field.literal != "" && *field.envName != "" {
			return fmt.Errorf("route %s %s and %s_env are mutually exclusive", name, field.label, field.label)
		}
		if *field.envName == "" {
			continue
		}
		value, ok := os.LookupEnv(*field.envName)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("route %s %s_env %q is not set or is empty", name, field.label, *field.envName)
		}
		*field.literal = strings.TrimSpace(value)
	}
	return nil
}

func (s *Snapshot) resolveAndValidateAuth() error {
	if s.Auth.MaxBodyBytes < 0 {
		return fmt.Errorf("auth.max_body_bytes must be greater than or equal to zero")
	}
	if s.Auth.Tokens == nil {
		s.Auth.Tokens = make(map[string]Principal, len(s.Auth.TokenEnvs))
	}
	for token, principal := range s.Auth.Tokens {
		trimmed := strings.TrimSpace(token)
		if trimmed == "" {
			return fmt.Errorf("auth.tokens contains an empty bearer token")
		}
		if token != trimmed {
			return fmt.Errorf("auth.tokens bearer tokens must not contain leading or trailing whitespace")
		}
		if err := validatePrincipal("auth.tokens", principal); err != nil {
			return err
		}
	}
	for envName, principal := range s.Auth.TokenEnvs {
		if strings.TrimSpace(envName) == "" {
			return fmt.Errorf("auth.token_envs contains an empty environment variable name")
		}
		if err := validatePrincipal("auth.token_envs."+envName, principal); err != nil {
			return err
		}
		value, ok := os.LookupEnv(envName)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("auth.token_envs environment variable %q is not set or is empty", envName)
		}
		token := strings.TrimSpace(value)
		if _, exists := s.Auth.Tokens[token]; exists {
			return fmt.Errorf("auth.token_envs environment variable %q resolves to a duplicate bearer token", envName)
		}
		s.Auth.Tokens[token] = principal
	}
	if err := resolveAndValidateJWT(&s.Auth.JWT); err != nil {
		return err
	}
	if !s.Auth.AllowAnonymous && len(s.Auth.Tokens) == 0 && !s.Auth.JWT.Enabled() {
		return fmt.Errorf("authentication is not configured; set auth.allow_anonymous to true explicitly or configure a bearer token/JWT")
	}
	return nil
}

func resolveAndValidateJWT(jwt *JWTConfig) error {
	if jwt == nil {
		return nil
	}
	if jwt.Secret != "" && jwt.SecretEnv != "" {
		return fmt.Errorf("auth.jwt.secret and auth.jwt.secret_env are mutually exclusive")
	}
	if jwt.PublicKey != "" && jwt.PublicKeyEnv != "" {
		return fmt.Errorf("auth.jwt.public_key and auth.jwt.public_key_env are mutually exclusive")
	}
	if jwt.SecretEnv != "" {
		value, ok := os.LookupEnv(jwt.SecretEnv)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("auth.jwt.secret_env %q is not set or is empty", jwt.SecretEnv)
		}
		jwt.Secret = value
	}
	if jwt.PublicKeyEnv != "" {
		value, ok := os.LookupEnv(jwt.PublicKeyEnv)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("auth.jwt.public_key_env %q is not set or is empty", jwt.PublicKeyEnv)
		}
		jwt.PublicKey = value
	}
	configured := jwt.Algorithm != "" || jwt.Issuer != "" || jwt.Audience != "" || jwt.Secret != "" || jwt.PublicKey != ""
	if !configured {
		return nil
	}
	jwt.finalize()
	switch jwt.Algorithm {
	case "HS256", "HS384", "HS512":
		if strings.TrimSpace(jwt.Secret) == "" {
			return fmt.Errorf("auth.jwt.secret or auth.jwt.secret_env is required for %s", jwt.Algorithm)
		}
		if jwt.PublicKey != "" {
			return fmt.Errorf("auth.jwt.public_key cannot be used with %s", jwt.Algorithm)
		}
	case "RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "EdDSA":
		if strings.TrimSpace(jwt.PublicKey) == "" {
			return fmt.Errorf("auth.jwt.public_key or auth.jwt.public_key_env is required for %s", jwt.Algorithm)
		}
		if jwt.Secret != "" {
			return fmt.Errorf("auth.jwt.secret cannot be used with %s", jwt.Algorithm)
		}
	default:
		return fmt.Errorf("auth.jwt.algorithm %q is not supported", jwt.Algorithm)
	}
	key, err := jwt.VerificationKey()
	if err != nil {
		return fmt.Errorf("auth.jwt verification key is invalid: %w", err)
	}
	jwt.verificationKey = key
	return nil
}

func validatePrincipal(path string, principal Principal) error {
	if firstNonEmptyString(strings.TrimSpace(principal.ID), strings.TrimSpace(principal.KeyID), strings.TrimSpace(principal.QuotaKey)) == "" {
		return fmt.Errorf("%s principal must configure at least one of id, key_id, or quota_key", path)
	}
	for _, provider := range principal.Providers {
		if err := validateProviderName(provider); err != nil {
			return fmt.Errorf("%s principal has invalid provider reference: %w", path, err)
		}
	}
	for _, permission := range principal.Permissions {
		if permission != PermissionManageLimits && permission != PermissionViewOperations {
			return fmt.Errorf("%s principal references unsupported permission %q", path, permission)
		}
	}
	return nil
}

func validateConfiguredProviderReferences(snapshot *Snapshot) error {
	configured := make(map[string]struct{}, len(snapshot.Routes))
	for _, route := range snapshot.Routes {
		if route != nil {
			configured[route.Provider] = struct{}{}
		}
	}
	validate := func(path string, providers []string) error {
		for _, provider := range providers {
			if _, ok := configured[provider]; !ok {
				return fmt.Errorf("%s references provider %q, which has no configured route", path, provider)
			}
		}
		return nil
	}
	for _, principal := range snapshot.Auth.Tokens {
		if err := validate("auth.tokens principal", principal.Providers); err != nil {
			return err
		}
	}
	for envName, principal := range snapshot.Auth.TokenEnvs {
		if err := validate("auth.token_envs."+envName, principal.Providers); err != nil {
			return err
		}
	}
	for name, profile := range snapshot.Quota.Profiles {
		if err := validate("quota.profiles."+name+".provider_allowlist", profile.ProviderAllowlist); err != nil {
			return err
		}
	}
	return nil
}

func validateDurationsAndLimits(s *Snapshot) error {
	durations := []struct {
		name  string
		value time.Duration
	}{
		{"server.read_timeout", s.Server.ReadTimeout.Duration},
		{"server.write_timeout", s.Server.WriteTimeout.Duration},
		{"server.stream_write_timeout", s.Server.StreamWriteTimeout.Duration},
		{"server.idle_timeout", s.Server.IdleTimeout.Duration},
		{"server.shutdown_timeout", s.Server.ShutdownTimeout.Duration},
		{"store.limit_cache_ttl", s.Store.LimitCacheTTL.Duration},
		{"store.reservation_ttl", s.Store.ReservationTTL.Duration},
		{"store.startup_timeout", s.Store.StartupTimeout.Duration},
	}
	for _, duration := range durations {
		if duration.value < 0 {
			return fmt.Errorf("%s must be greater than or equal to zero", duration.name)
		}
	}
	if s.Store.RedisDB < 0 {
		return fmt.Errorf("store.redis_db must be greater than or equal to zero")
	}
	return nil
}

func validateRoute(name string, route *Route) error {
	if !validHTTPHeaderValue(name) {
		return fmt.Errorf("route name %q contains invalid HTTP header value bytes", name)
	}
	if route.Provider == "" {
		return fmt.Errorf("route %s missing provider", name)
	}
	if err := validateProviderName(route.Provider); err != nil {
		return fmt.Errorf("route %s: %w", name, err)
	}
	if route.Backend != "" && (!validHTTPHeaderValue(route.Backend) || len(route.Backend) > 128) {
		return fmt.Errorf("route %s backend contains invalid bytes or exceeds 128 bytes", name)
	}
	if route.Model == "" {
		return fmt.Errorf("route %s missing model", name)
	}
	if !validHTTPHeaderValue(route.Model) {
		return fmt.Errorf("route %s model contains invalid HTTP header value bytes", name)
	}
	for header, value := range route.Headers {
		if !validHTTPHeaderName(header) {
			return fmt.Errorf("route %s has invalid header name %q", name, header)
		}
		if !validHTTPHeaderValue(value) {
			return fmt.Errorf("route %s header %q contains invalid control characters", name, header)
		}
	}
	if route.UpstreamModel == "" {
		return fmt.Errorf("route %s missing upstream model", name)
	}
	if err := validateBaseURL(route.BaseURL); err != nil {
		return fmt.Errorf("route %s base_url: %w", name, err)
	}
	if route.Priority < 0 {
		return fmt.Errorf("route %s priority must be greater than or equal to zero", name)
	}
	if route.Weight < 0 {
		return fmt.Errorf("route %s weight must be greater than or equal to zero", name)
	}
	if route.Weight > MaximumRouteWeight {
		return fmt.Errorf("route %s weight must not exceed %d", name, MaximumRouteWeight)
	}
	if route.Retries < 0 {
		return fmt.Errorf("route %s retries must be greater than or equal to zero", name)
	}
	if route.Retries > MaximumRouteRetries {
		return fmt.Errorf("route %s retries must not exceed %d", name, MaximumRouteRetries)
	}
	if route.Timeout.Duration < 0 {
		return fmt.Errorf("route %s timeout must be greater than or equal to zero", name)
	}
	if route.Circuit.Failures < 0 {
		return fmt.Errorf("route %s circuit.failures must be greater than or equal to zero", name)
	}
	if route.Circuit.Cooldown.Duration < 0 {
		return fmt.Errorf("route %s circuit.cooldown must be greater than or equal to zero", name)
	}
	for field, value := range map[string]int64{
		"limits.rpm":                  route.Limits.RPM,
		"limits.tpm":                  route.Limits.TPM,
		"limits.concurrency":          route.Limits.Concurrency,
		"limits.provider_concurrency": route.Limits.ProviderConcurrency,
		"limits.max_body_bytes":       route.Limits.MaxBodyBytes,
		"limits.max_response_bytes":   route.Limits.MaxResponseBytes,
	} {
		if value < 0 {
			return fmt.Errorf("route %s %s must be greater than or equal to zero", name, field)
		}
		if value > MaximumQuotaValue {
			return fmt.Errorf("route %s %s must not exceed %d", name, field, MaximumQuotaValue)
		}
	}
	if route.Capabilities.MaxInputTokens < 0 || route.Capabilities.MaxOutputTokens < 0 {
		return fmt.Errorf("route %s capability token limits must be greater than or equal to zero", name)
	}
	if int64(route.Capabilities.MaxInputTokens) > MaximumQuotaValue || int64(route.Capabilities.MaxOutputTokens) > MaximumQuotaValue {
		return fmt.Errorf("route %s capability token limits must not exceed %d", name, MaximumQuotaValue)
	}
	if route.Capabilities.VisionInputTokenSurcharge < 0 || route.Capabilities.AudioInputTokenSurcharge < 0 {
		return fmt.Errorf("route %s capability multimodal token surcharges must be greater than or equal to zero", name)
	}
	if !route.Capabilities.VisionInput && route.Capabilities.VisionInputTokenSurcharge != 0 {
		return fmt.Errorf("route %s capabilities.vision_input_token_surcharge requires vision_input", name)
	}
	if !route.Capabilities.Audio && route.Capabilities.AudioInputTokenSurcharge != 0 {
		return fmt.Errorf("route %s capabilities.audio_input_token_surcharge requires audio", name)
	}
	if len(route.Capabilities.Operations) == 0 {
		return fmt.Errorf("route %s must declare at least one capability operation", name)
	}
	if routeSupportsGeneratedOutput(route.Capabilities.Operations) && route.Capabilities.MaxOutputTokens <= 0 {
		return fmt.Errorf("route %s capabilities.max_output_tokens must be greater than zero for generative operations", name)
	}
	if _, ok := supportedTokenizerFamilies[route.Capabilities.Tokenizer]; !ok {
		if route.Capabilities.Tokenizer == "" {
			return fmt.Errorf("route %s capabilities.tokenizer is required", name)
		}
		return fmt.Errorf("route %s uses unsupported capabilities.tokenizer %q", name, route.Capabilities.Tokenizer)
	}
	seen := make(map[Operation]struct{}, len(route.Capabilities.Operations))
	for _, operation := range route.Capabilities.Operations {
		if !isKnownOperation(operation) {
			return fmt.Errorf("route %s declares unsupported operation %q", name, operation)
		}
		if _, duplicate := seen[operation]; duplicate {
			return fmt.Errorf("route %s declares operation %q more than once", name, operation)
		}
		seen[operation] = struct{}{}
	}
	for field, value := range map[string]float64{
		"pricing.input_per_1m":       route.Pricing.InputPer1M,
		"pricing.output_per_1m":      route.Pricing.OutputPer1M,
		"pricing.cache_read_per_1m":  route.Pricing.CacheReadPer1M,
		"pricing.cache_write_per_1m": route.Pricing.CacheWritePer1M,
	} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("route %s %s must be a finite value greater than or equal to zero", name, field)
		}
	}
	for unit, pricing := range route.Pricing.ProviderUnits {
		if strings.TrimSpace(unit) == "" {
			return fmt.Errorf("route %s pricing.provider_units contains an empty unit name", name)
		}
		if pricing.MicrosPerUnit < 0 || math.IsNaN(pricing.MicrosPerUnit) || math.IsInf(pricing.MicrosPerUnit, 0) {
			return fmt.Errorf("route %s pricing.provider_units.%s.micros_per_unit must be a finite value greater than or equal to zero", name, unit)
		}
		if pricing.MaxUnitsPerRequest < 0 {
			return fmt.Errorf("route %s pricing.provider_units.%s.max_units_per_request must be greater than or equal to zero", name, unit)
		}
		if pricing.MaxUnitsPerRequest > MaximumQuotaValue {
			return fmt.Errorf("route %s pricing.provider_units.%s.max_units_per_request must not exceed %d", name, unit, MaximumQuotaValue)
		}
	}
	return nil
}

func normalizeRouteHeaders(routeName string, headers map[string]string) (map[string]string, error) {
	if len(headers) == 0 {
		return nil, nil
	}
	normalized := make(map[string]string, len(headers))
	for header, value := range headers {
		if !validHTTPHeaderName(header) {
			return nil, fmt.Errorf("route %s has invalid header name %q", routeName, header)
		}
		if !validHTTPHeaderValue(value) {
			return nil, fmt.Errorf("route %s header %q contains invalid control characters", routeName, header)
		}
		canonical := http.CanonicalHeaderKey(header)
		if _, duplicate := normalized[canonical]; duplicate {
			return nil, fmt.Errorf("route %s configures header %q more than once with different casing", routeName, canonical)
		}
		if forbiddenRouteHeader(canonical) {
			return nil, fmt.Errorf("route %s header %q is managed by the HTTP transport and cannot be configured", routeName, canonical)
		}
		normalized[canonical] = value
	}
	return normalized, nil
}

func forbiddenRouteHeader(header string) bool {
	switch http.CanonicalHeaderKey(header) {
	case "Connection", "Content-Length", "Host", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	default:
		return false
	}
}

// HTTP field names use the RFC 9110 token grammar. CanonicalMIMEHeaderKey is
// intentionally not a validator: it returns invalid input unchanged.
func validHTTPHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !validHTTPTokenByte(name[i]) {
			return false
		}
	}
	return true
}

func validHTTPTokenByte(value byte) bool {
	if value >= '0' && value <= '9' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' {
		return true
	}
	switch value {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	default:
		return false
	}
}

// HTTP field values permit visible ASCII, horizontal tabs, and obs-text, but
// reject other controls. Route names and public model aliases are validated
// with the same rule because attempt metadata forwards them as headers.
func validHTTPHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] == 0x7f || value[i] < 0x20 && value[i] != '\t' {
			return false
		}
	}
	return true
}

func validateRedisURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return safeRedisURLParseError(err)
	}
	if parsed.Scheme != "redis" && parsed.Scheme != "rediss" {
		return fmt.Errorf("scheme must be redis or rediss")
	}
	if parsed.Hostname() == "" {
		return fmt.Errorf("host is required")
	}
	if parsed.Fragment != "" {
		return fmt.Errorf("fragment is not allowed")
	}
	options, err := redis.ParseURL(raw)
	if err != nil {
		return safeRedisURLParseError(err)
	}
	if err := normalizeAndValidateRedisOptions(options); err != nil {
		return err
	}
	return nil
}

func safeRedisURLParseError(err error) error {
	var parsed *url.Error
	if errors.As(err, &parsed) && parsed.Err != nil {
		return fmt.Errorf("parse Redis URL: %v", parsed.Err)
	}
	return fmt.Errorf("parse Redis URL: %v", err)
}

const (
	maximumRedisTimeout         = 30 * time.Second
	maximumRedisRetryBackoff    = 5 * time.Second
	maximumRedisRetries         = 10
	defaultRedisPoolSize        = 64
	defaultRedisMaxActiveConns  = 128
	maximumRedisPoolConnections = 4096
)

func normalizeAndValidateRedisOptions(options *redis.Options) error {
	if options == nil {
		return fmt.Errorf("options are missing")
	}
	if options.DB < 0 {
		return fmt.Errorf("database must be greater than or equal to zero")
	}
	if options.Protocol != 0 && options.Protocol != 2 && options.Protocol != 3 {
		return fmt.Errorf("protocol must be 2 or 3")
	}
	if options.MaxRetries < -1 || options.MaxRetries > maximumRedisRetries {
		return fmt.Errorf("max_retries must be between -1 and %d", maximumRedisRetries)
	}
	for _, field := range []struct {
		name  string
		value time.Duration
	}{
		{name: "dial_timeout", value: options.DialTimeout},
		{name: "read_timeout", value: options.ReadTimeout},
		{name: "write_timeout", value: options.WriteTimeout},
		{name: "pool_timeout", value: options.PoolTimeout},
	} {
		name, value := field.name, field.value
		if value < 0 {
			return fmt.Errorf("%s must not disable timeouts", name)
		}
		if value > maximumRedisTimeout {
			return fmt.Errorf("%s must not exceed %s", name, maximumRedisTimeout)
		}
	}
	for _, field := range []struct {
		name  string
		value time.Duration
	}{
		{name: "min_retry_backoff", value: options.MinRetryBackoff},
		{name: "max_retry_backoff", value: options.MaxRetryBackoff},
	} {
		if field.value < -1 {
			return fmt.Errorf("%s must be -1 or greater", field.name)
		}
		if field.value > maximumRedisRetryBackoff {
			return fmt.Errorf("%s must not exceed %s", field.name, maximumRedisRetryBackoff)
		}
	}
	for _, field := range []struct {
		name  string
		value int
	}{
		{name: "pool_size", value: options.PoolSize},
		{name: "min_idle_conns", value: options.MinIdleConns},
		{name: "max_idle_conns", value: options.MaxIdleConns},
		{name: "max_active_conns", value: options.MaxActiveConns},
	} {
		if field.value < 0 {
			return fmt.Errorf("%s must be greater than or equal to zero", field.name)
		}
		if field.value > maximumRedisPoolConnections {
			return fmt.Errorf("%s must not exceed %d", field.name, maximumRedisPoolConnections)
		}
	}
	if options.PoolSize == 0 {
		options.PoolSize = defaultRedisPoolSize
	}
	if options.MaxIdleConns == 0 {
		options.MaxIdleConns = options.PoolSize
	}
	if options.MaxActiveConns == 0 {
		options.MaxActiveConns = options.PoolSize * 2
		if options.MaxActiveConns < defaultRedisMaxActiveConns {
			options.MaxActiveConns = defaultRedisMaxActiveConns
		}
		if options.MaxActiveConns > maximumRedisPoolConnections {
			options.MaxActiveConns = maximumRedisPoolConnections
		}
	}
	if options.MinIdleConns > options.PoolSize {
		return fmt.Errorf("min_idle_conns must not exceed pool_size")
	}
	if options.MaxIdleConns > options.PoolSize {
		return fmt.Errorf("max_idle_conns must not exceed pool_size")
	}
	if options.MinIdleConns > options.MaxIdleConns {
		return fmt.Errorf("min_idle_conns must not exceed max_idle_conns")
	}
	if options.PoolSize > options.MaxActiveConns {
		return fmt.Errorf("pool_size must not exceed max_active_conns")
	}
	if options.MinIdleConns > options.MaxActiveConns {
		return fmt.Errorf("min_idle_conns must not exceed max_active_conns")
	}
	if options.MaxIdleConns > options.MaxActiveConns {
		return fmt.Errorf("max_idle_conns must not exceed max_active_conns")
	}
	effectiveMinBackoff := options.MinRetryBackoff
	if effectiveMinBackoff == 0 {
		effectiveMinBackoff = 8 * time.Millisecond
	} else if effectiveMinBackoff == -1 {
		effectiveMinBackoff = 0
	}
	effectiveMaxBackoff := options.MaxRetryBackoff
	if effectiveMaxBackoff == 0 {
		effectiveMaxBackoff = 512 * time.Millisecond
	} else if effectiveMaxBackoff == -1 {
		effectiveMaxBackoff = 0
	}
	if effectiveMinBackoff > 0 && effectiveMaxBackoff > 0 && effectiveMinBackoff > effectiveMaxBackoff {
		return fmt.Errorf("min_retry_backoff must not exceed max_retry_backoff")
	}
	return nil
}

func validRedisNamespace(namespace string) bool {
	if namespace == "" || len(namespace) > 128 {
		return false
	}
	for i := 0; i < len(namespace); i++ {
		value := namespace[i]
		if value >= '0' && value <= '9' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' {
			continue
		}
		switch value {
		case '.', '_', '-':
			continue
		default:
			return false
		}
	}
	return true
}

func routeSupportsGeneratedOutput(operations []Operation) bool {
	for _, operation := range operations {
		switch operation {
		case OpChatCompletions, OpResponses, OpCompletions:
			return true
		}
	}
	return false
}

var supportedTokenizerFamilies = map[string]struct{}{
	"o200k_base":  {},
	"cl100k_base": {},
	"p50k_base":   {},
	"p50k_edit":   {},
	"r50k_base":   {},
	"claude":      {},
	"gemini":      {},
}

func validateBaseURL(raw string) error {
	if raw == "" {
		return fmt.Errorf("is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("is invalid: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}
	if parsed.Host == "" {
		return fmt.Errorf("must be an absolute URL with a host")
	}
	if parsed.User != nil {
		return fmt.Errorf("must not contain user information")
	}
	if parsed.RawQuery != "" {
		return fmt.Errorf("must not contain a query string")
	}
	if parsed.Fragment != "" {
		return fmt.Errorf("must not contain a fragment")
	}
	return nil
}

func isKnownOperation(operation Operation) bool {
	switch operation {
	case OpChatCompletions, OpResponses, OpCompletions, OpEmbeddings:
		return true
	default:
		return false
	}
}

func validateLimitSpec(path string, limit LimitSpec) error {
	for field, value := range map[string]int64{
		"rpm":               limit.RPM,
		"tpm":               limit.TPM,
		"max_parallel":      limit.MaxParallel,
		"max_spend_micros":  limit.MaxSpendMicros,
		"soft_spend_micros": limit.SoftSpendMicros,
		"daily_tokens":      limit.DailyTokens,
		"monthly_tokens":    limit.MonthlyTokens,
		"max_input_tokens":  limit.MaxInputTokens,
		"max_output_tokens": limit.MaxOutputTokens,
	} {
		if value < 0 {
			return fmt.Errorf("%s.%s must be greater than or equal to zero", path, field)
		}
		if value > MaximumQuotaValue {
			return fmt.Errorf("%s.%s must not exceed %d", path, field, MaximumQuotaValue)
		}
	}
	if limit.BudgetDuration.Duration < 0 {
		return fmt.Errorf("%s.budget_duration must be greater than or equal to zero", path)
	}
	if limit.MaxSpendMicros > 0 && limit.SoftSpendMicros > limit.MaxSpendMicros {
		return fmt.Errorf("%s.soft_spend_micros must not exceed max_spend_micros", path)
	}
	for _, provider := range limit.ProviderAllowlist {
		if err := validateProviderName(provider); err != nil {
			return fmt.Errorf("%s.provider_allowlist contains an invalid provider: %w", path, err)
		}
	}
	return nil
}

// ValidateLimitSpec applies the same semantic checks used for file-backed
// configuration to dynamically supplied quota limits.
func ValidateLimitSpec(limit LimitSpec) error {
	return validateLimitSpec("limits", limit)
}

func (c Capability) Supports(op Operation) bool {
	return lo.Contains(c.Operations, op)
}

func (p Principal) Subject() Subject {
	return Subject{
		KeyID: firstNonEmptyString(p.KeyID, p.QuotaKey, p.ID),
	}
}

func (p Principal) HasPermission(permission string) bool {
	return lo.Contains(p.Permissions, permission)
}

func (j JWTConfig) Enabled() bool {
	return j.Secret != "" || j.PublicKey != ""
}

func (j JWTConfig) Normalize() JWTConfig {
	j.finalize()
	return j
}

func (j *JWTConfig) finalize() {
	if j == nil {
		return
	}
	if j.Algorithm == "" {
		switch {
		case j.Secret != "":
			j.Algorithm = "HS256"
		case j.PublicKey != "":
			j.Algorithm = "RS256"
		}
	}
	if j.Claims.Principal == "" {
		j.Claims.Principal = "sub"
	}
	if j.Claims.Name == "" {
		j.Claims.Name = "name"
	}
	if j.Claims.KeyID == "" {
		j.Claims.KeyID = "key_id"
	}
	if j.Claims.Providers == "" {
		j.Claims.Providers = "providers"
	}
	if j.Claims.Models == "" {
		j.Claims.Models = "models"
	}
	if j.Claims.Projects == "" {
		j.Claims.Projects = "projects"
	}
	if j.Claims.Permissions == "" {
		j.Claims.Permissions = "permissions"
	}
	normalized := strings.ToUpper(j.Algorithm)
	if normalized == "EDDSA" {
		j.Algorithm = "EdDSA"
	} else {
		j.Algorithm = normalized
	}
}

func (s *Snapshot) ResolveQuotaScopes(subject Subject) []ScopedLimit {
	if s == nil {
		return nil
	}
	out := make([]ScopedLimit, 0, 1)
	if subject.KeyID != "" {
		if limit, ok := s.lookupQuotaLimit(s.Quota.Keys[subject.KeyID]); ok {
			out = append(out, ScopedLimit{Ref: ScopeRef{Kind: ScopeKey, ID: subject.KeyID}, Limits: limit})
		}
	}
	return out
}

func (s *Snapshot) lookupQuotaLimit(name string) (LimitSpec, bool) {
	if name == "" || s == nil || s.Quota.Profiles == nil {
		return LimitSpec{}, false
	}
	limit, ok := s.Quota.Profiles[name]
	return limit, ok
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
