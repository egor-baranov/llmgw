package gateway

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/samber/lo"
	"gopkg.in/yaml.v3"
)

type Snapshot struct {
	Server    ServerConfig      `yaml:"server"`
	Auth      AuthConfig        `yaml:"auth"`
	Store     StoreConfig       `yaml:"store"`
	Quota     QuotaConfig       `yaml:"quota"`
	Routes    map[string]*Route `yaml:"routes"`
	Telemetry TelemetryConfig   `yaml:"telemetry"`
	LoadedAt  time.Time         `yaml:"-"`
	models    []ModelDescriptor `yaml:"-"`
}

type ServerConfig struct {
	Listen          string   `yaml:"listen"`
	ReadTimeout     Duration `yaml:"read_timeout"`
	WriteTimeout    Duration `yaml:"write_timeout"`
	IdleTimeout     Duration `yaml:"idle_timeout"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
}

type AuthConfig struct {
	RequireUser    bool                 `yaml:"require_user"`
	RequireProject bool                 `yaml:"require_project"`
	MaxBodyBytes   int64                `yaml:"max_body_bytes"`
	Tokens         map[string]Principal `yaml:"tokens"`
	JWT            JWTConfig            `yaml:"jwt"`
}

type Principal struct {
	ID        string   `yaml:"id"`
	Name      string   `yaml:"name"`
	KeyID     string   `yaml:"key_id"`
	Providers []string `yaml:"providers"`
	Models    []string `yaml:"models"`
	Projects  []string `yaml:"projects"`
	QuotaKey  string   `yaml:"quota_key"`
}

type JWTConfig struct {
	Algorithm    string          `yaml:"algorithm"`
	Issuer       string          `yaml:"issuer"`
	Audience     string          `yaml:"audience"`
	Secret       string          `yaml:"secret"`
	SecretEnv    string          `yaml:"secret_env"`
	PublicKey    string          `yaml:"public_key"`
	PublicKeyEnv string          `yaml:"public_key_env"`
	Claims       JWTClaimMapping `yaml:"claims"`
}

type JWTClaimMapping struct {
	Principal string `yaml:"principal"`
	Name      string `yaml:"name"`
	KeyID     string `yaml:"key_id"`
	Providers string `yaml:"providers"`
	Models    string `yaml:"models"`
	Projects  string `yaml:"projects"`
}

type StoreConfig struct {
	Mode             string   `yaml:"mode"`
	RedisAddr        string   `yaml:"redis_addr"`
	RedisPasswordEnv string   `yaml:"redis_password_env"`
	RedisDB          int      `yaml:"redis_db"`
	PostgresDSN      string   `yaml:"postgres_dsn"`
	PostgresDSNEnv   string   `yaml:"postgres_dsn_env"`
	QuotaTable       string   `yaml:"quota_table"`
	LimitCacheTTL    Duration `yaml:"limit_cache_ttl"`
	ReservationTTL   Duration `yaml:"reservation_ttl"`
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

type Route struct {
	Name         string            `yaml:"-"`
	Provider     string            `yaml:"provider"`
	Backend      string            `yaml:"backend"`
	Model        string            `yaml:"model"`
	BaseURL      string            `yaml:"base_url"`
	APIKey       string            `yaml:"api_key"`
	APIKeyEnv    string            `yaml:"api_key_env"`
	Project      string            `yaml:"project"`
	ProjectEnv   string            `yaml:"project_env"`
	Location     string            `yaml:"location"`
	LocationEnv  string            `yaml:"location_env"`
	APIVersion   string            `yaml:"api_version"`
	Headers      map[string]string `yaml:"headers"`
	Priority     int               `yaml:"priority"`
	Weight       int               `yaml:"weight"`
	Retries      int               `yaml:"retries"`
	Timeout      Duration          `yaml:"timeout"`
	Limits       LimitConfig       `yaml:"limits"`
	Circuit      CircuitConfig     `yaml:"circuit"`
	Capabilities Capability        `yaml:"capabilities"`
	Pricing      Pricing           `yaml:"pricing"`
}

type LimitConfig struct {
	RPM                 int64 `yaml:"rpm"`
	TPM                 int64 `yaml:"tpm"`
	Concurrency         int64 `yaml:"concurrency"`
	ProviderConcurrency int64 `yaml:"provider_concurrency"`
	MaxBodyBytes        int64 `yaml:"max_body_bytes"`
}

type CircuitConfig struct {
	Failures int      `yaml:"failures"`
	Cooldown Duration `yaml:"cooldown"`
}

type Pricing struct {
	InputPer1M  float64 `yaml:"input_per_1m"`
	OutputPer1M float64 `yaml:"output_per_1m"`
}

type Capability struct {
	Operations       []Operation `yaml:"operations"`
	Streaming        bool        `yaml:"streaming"`
	ToolCalling      bool        `yaml:"tool_calling"`
	StructuredOutput bool        `yaml:"structured_output"`
	VisionInput      bool        `yaml:"vision_input"`
	Audio            bool        `yaml:"audio"`
	Reasoning        bool        `yaml:"reasoning"`
	MaxInputTokens   int         `yaml:"max_input_tokens"`
	MaxOutputTokens  int         `yaml:"max_output_tokens"`
	Tokenizer        string      `yaml:"tokenizer"`
}

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
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.finalize(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func NewConfigStore(cfg *Snapshot) *ConfigStore {
	store := &ConfigStore{}
	if cfg != nil {
		store.current.Store(cfg)
	}
	return store
}

func (s *ConfigStore) Load() *Snapshot {
	return s.current.Load()
}

func (s *ConfigStore) Swap(cfg *Snapshot) {
	s.current.Store(cfg)
}

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

func (s *Snapshot) finalize() error {
	if s.Server.Listen == "" {
		s.Server.Listen = ":8080"
	}
	if s.Server.ShutdownTimeout.Duration == 0 {
		s.Server.ShutdownTimeout.Duration = 10 * time.Second
	}
	if s.Auth.MaxBodyBytes == 0 {
		s.Auth.MaxBodyBytes = 4 << 20
	}
	if s.Auth.JWT.Secret == "" && s.Auth.JWT.SecretEnv != "" {
		s.Auth.JWT.Secret = os.Getenv(s.Auth.JWT.SecretEnv)
	}
	if s.Auth.JWT.PublicKey == "" && s.Auth.JWT.PublicKeyEnv != "" {
		s.Auth.JWT.PublicKey = os.Getenv(s.Auth.JWT.PublicKeyEnv)
	}
	s.Auth.JWT.finalize()
	if s.Store.Mode == "" {
		s.Store.Mode = "memory"
	}
	if s.Store.PostgresDSN == "" && s.Store.PostgresDSNEnv != "" {
		s.Store.PostgresDSN = os.Getenv(s.Store.PostgresDSNEnv)
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
	for name, route := range s.Routes {
		route.Name = name
		if route.Weight <= 0 {
			route.Weight = 1
		}
		if route.Timeout.Duration == 0 {
			route.Timeout.Duration = 30 * time.Second
		}
		if route.Circuit.Failures <= 0 {
			route.Circuit.Failures = 3
		}
		if route.Circuit.Cooldown.Duration == 0 {
			route.Circuit.Cooldown.Duration = 30 * time.Second
		}
		if route.APIKey == "" && route.APIKeyEnv != "" {
			route.APIKey = os.Getenv(route.APIKeyEnv)
		}
		if route.Project == "" && route.ProjectEnv != "" {
			route.Project = os.Getenv(route.ProjectEnv)
		}
		if route.Location == "" && route.LocationEnv != "" {
			route.Location = os.Getenv(route.LocationEnv)
		}
		if route.Provider == "" {
			return fmt.Errorf("route %s missing provider", name)
		}
		if route.Model == "" {
			return fmt.Errorf("route %s missing model", name)
		}
	}
	modelNames := lo.Uniq(lo.Map(lo.Values(s.Routes), func(route *Route, _ int) string {
		if route == nil {
			return ""
		}
		return route.Model
	}))
	modelNames = lo.Filter(modelNames, func(model string, _ int) bool { return model != "" })
	if len(modelNames) == 0 {
		return fmt.Errorf("no route models configured")
	}
	sort.Strings(modelNames)
	models := lo.Map(modelNames, func(model string, _ int) ModelDescriptor {
		return ModelDescriptor{ID: model, Object: "model", OwnedBy: "llmgw"}
	})
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	s.models = models
	s.LoadedAt = time.Now()
	return nil
}

func (c Capability) Supports(op Operation) bool {
	return lo.Contains(c.Operations, op)
}

func (p Principal) Subject() Subject {
	return Subject{
		KeyID: firstNonEmptyString(p.KeyID, p.QuotaKey, p.ID),
	}
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
	j.Algorithm = strings.ToUpper(j.Algorithm)
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
