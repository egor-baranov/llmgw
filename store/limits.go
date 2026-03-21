package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"time"

	"llmgw/gateway"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

type QuotaLimitStore interface {
	Get(ctx context.Context, keyID string) (gateway.LimitSpec, bool, error)
	Put(ctx context.Context, keyID string, limit gateway.LimitSpec) error
}

type QuotaUsageStore interface {
	GetUsage(ctx context.Context, scope gateway.ScopedLimit) (gateway.QuotaUsage, error)
}

type MemoryQuotaLimitStore struct {
	mu     sync.RWMutex
	limits map[string]gateway.LimitSpec
}

type CachedQuotaLimitStore struct {
	source QuotaLimitStore
	ttl    time.Duration
	mu     sync.RWMutex
	cache  map[string]cachedQuotaLimit
}

type cachedQuotaLimit struct {
	limit   gateway.LimitSpec
	found   bool
	expires time.Time
}

type PostgresQuotaLimitStore struct {
	db    *sql.DB
	table string
}

var quotaTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func NewMemoryQuotaLimitStore() *MemoryQuotaLimitStore {
	return &MemoryQuotaLimitStore{limits: map[string]gateway.LimitSpec{}}
}

func NewCachedQuotaLimitStore(source QuotaLimitStore, ttl time.Duration) *CachedQuotaLimitStore {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &CachedQuotaLimitStore{
		source: source,
		ttl:    ttl,
		cache:  map[string]cachedQuotaLimit{},
	}
}

func NewPostgresQuotaLimitStore(ctx context.Context, dsn, table string) (*PostgresQuotaLimitStore, error) {
	if dsn == "" {
		return nil, fmt.Errorf("postgres dsn is required")
	}
	if table == "" {
		table = "quota_limits"
	}
	if !quotaTableName.MatchString(table) {
		return nil, fmt.Errorf("invalid quota table name %q", table)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	store := &PostgresQuotaLimitStore{db: db, table: table}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *MemoryQuotaLimitStore) Get(_ context.Context, keyID string) (gateway.LimitSpec, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	limit, ok := s.limits[keyID]
	return limit, ok, nil
}

func (s *MemoryQuotaLimitStore) Put(_ context.Context, keyID string, limit gateway.LimitSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits[keyID] = limit
	return nil
}

func (s *CachedQuotaLimitStore) Get(ctx context.Context, keyID string) (gateway.LimitSpec, bool, error) {
	if keyID == "" {
		return gateway.LimitSpec{}, false, nil
	}
	now := time.Now()
	s.mu.RLock()
	entry, ok := s.cache[keyID]
	s.mu.RUnlock()
	if ok && now.Before(entry.expires) {
		return entry.limit, entry.found, nil
	}
	if s.source == nil {
		return gateway.LimitSpec{}, false, nil
	}
	limit, found, err := s.source.Get(ctx, keyID)
	if err != nil {
		return gateway.LimitSpec{}, false, err
	}
	s.mu.Lock()
	s.cache[keyID] = cachedQuotaLimit{
		limit:   limit,
		found:   found,
		expires: now.Add(s.ttl),
	}
	s.mu.Unlock()
	return limit, found, nil
}

func (s *CachedQuotaLimitStore) Put(ctx context.Context, keyID string, limit gateway.LimitSpec) error {
	if s.source == nil {
		return nil
	}
	if err := s.source.Put(ctx, keyID, limit); err != nil {
		return err
	}
	s.mu.Lock()
	s.cache[keyID] = cachedQuotaLimit{
		limit:   limit,
		found:   true,
		expires: time.Now().Add(s.ttl),
	}
	s.mu.Unlock()
	return nil
}

func (s *CachedQuotaLimitStore) Close() error {
	if closer, ok := s.source.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func (s *PostgresQuotaLimitStore) Get(ctx context.Context, keyID string) (gateway.LimitSpec, bool, error) {
	row := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT rpm, tpm, max_parallel, max_spend_micros, soft_spend_micros,
		       daily_tokens, monthly_tokens, budget_duration, max_input_tokens, max_output_tokens,
		       model_allowlist, provider_allowlist
		FROM %s
		WHERE key_id = $1
	`, s.table), keyID)
	var limit gateway.LimitSpec
	var budget sql.NullString
	var models []byte
	var providers []byte
	err := row.Scan(
		&limit.RPM,
		&limit.TPM,
		&limit.MaxParallel,
		&limit.MaxSpendMicros,
		&limit.SoftSpendMicros,
		&limit.DailyTokens,
		&limit.MonthlyTokens,
		&budget,
		&limit.MaxInputTokens,
		&limit.MaxOutputTokens,
		&models,
		&providers,
	)
	if err == sql.ErrNoRows {
		return gateway.LimitSpec{}, false, nil
	}
	if err != nil {
		return gateway.LimitSpec{}, false, err
	}
	if budget.Valid && budget.String != "" {
		if parsed, parseErr := time.ParseDuration(budget.String); parseErr == nil {
			limit.BudgetDuration.Duration = parsed
		} else {
			return gateway.LimitSpec{}, false, parseErr
		}
	}
	if len(models) > 0 {
		if err := json.Unmarshal(models, &limit.ModelAllowlist); err != nil {
			return gateway.LimitSpec{}, false, err
		}
	}
	if len(providers) > 0 {
		if err := json.Unmarshal(providers, &limit.ProviderAllowlist); err != nil {
			return gateway.LimitSpec{}, false, err
		}
	}
	return limit, true, nil
}

func (s *PostgresQuotaLimitStore) Put(ctx context.Context, keyID string, limit gateway.LimitSpec) error {
	models, err := json.Marshal(limit.ModelAllowlist)
	if err != nil {
		return err
	}
	providers, err := json.Marshal(limit.ProviderAllowlist)
	if err != nil {
		return err
	}
	budget := ""
	if limit.BudgetDuration.Duration > 0 {
		budget = limit.BudgetDuration.Duration.String()
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (
			key_id, rpm, tpm, max_parallel, max_spend_micros, soft_spend_micros,
			daily_tokens, monthly_tokens, budget_duration, max_input_tokens, max_output_tokens,
			model_allowlist, provider_allowlist, updated_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,NOW())
		ON CONFLICT (key_id) DO UPDATE SET
			rpm = EXCLUDED.rpm,
			tpm = EXCLUDED.tpm,
			max_parallel = EXCLUDED.max_parallel,
			max_spend_micros = EXCLUDED.max_spend_micros,
			soft_spend_micros = EXCLUDED.soft_spend_micros,
			daily_tokens = EXCLUDED.daily_tokens,
			monthly_tokens = EXCLUDED.monthly_tokens,
			budget_duration = EXCLUDED.budget_duration,
			max_input_tokens = EXCLUDED.max_input_tokens,
			max_output_tokens = EXCLUDED.max_output_tokens,
			model_allowlist = EXCLUDED.model_allowlist,
			provider_allowlist = EXCLUDED.provider_allowlist,
			updated_at = NOW()
	`, s.table),
		keyID,
		limit.RPM,
		limit.TPM,
		limit.MaxParallel,
		limit.MaxSpendMicros,
		limit.SoftSpendMicros,
		limit.DailyTokens,
		limit.MonthlyTokens,
		budget,
		limit.MaxInputTokens,
		limit.MaxOutputTokens,
		string(models),
		string(providers),
	)
	return err
}

func (s *PostgresQuotaLimitStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *PostgresQuotaLimitStore) ensureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			key_id TEXT PRIMARY KEY,
			rpm BIGINT NOT NULL DEFAULT 0,
			tpm BIGINT NOT NULL DEFAULT 0,
			max_parallel BIGINT NOT NULL DEFAULT 0,
			max_spend_micros BIGINT NOT NULL DEFAULT 0,
			soft_spend_micros BIGINT NOT NULL DEFAULT 0,
			daily_tokens BIGINT NOT NULL DEFAULT 0,
			monthly_tokens BIGINT NOT NULL DEFAULT 0,
			budget_duration TEXT NOT NULL DEFAULT '',
			max_input_tokens BIGINT NOT NULL DEFAULT 0,
			max_output_tokens BIGINT NOT NULL DEFAULT 0,
			model_allowlist JSONB NOT NULL DEFAULT '[]'::jsonb,
			provider_allowlist JSONB NOT NULL DEFAULT '[]'::jsonb,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, s.table))
	return err
}

func (s *MemoryQuotaStore) GetUsage(_ context.Context, scope gateway.ScopedLimit) (gateway.QuotaUsage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	keys := quotaKeysForScope(scope.Ref, scope.Limits, now)
	return gateway.QuotaUsage{
		ActiveRequests:  s.values[keys.active],
		RPMCurrent:      s.counterValue(keys.rpm, now),
		TPMCurrent:      s.counterValue(keys.tpm, now),
		SpendUsedMicros: s.values[keys.spendUsed],
		SpendHeldMicros: s.values[keys.spendHeld],
		DailyUsedTokens: s.values[keys.dayUsed],
		DailyHeldTokens: s.values[keys.dayHeld],
		MonthUsedTokens: s.values[keys.monthUsed],
		MonthHeldTokens: s.values[keys.monthHeld],
	}, nil
}

func (s *RedisStore) GetUsage(ctx context.Context, scope gateway.ScopedLimit) (gateway.QuotaUsage, error) {
	keys := quotaKeysForScope(scope.Ref, scope.Limits, time.Now())
	pipe := s.client.Pipeline()
	active := pipe.Get(ctx, keys.active)
	rpm := pipe.Get(ctx, keys.rpm)
	tpm := pipe.Get(ctx, keys.tpm)
	spendUsed := pipe.Get(ctx, keys.spendUsed)
	spendHeld := pipe.Get(ctx, keys.spendHeld)
	dayUsed := pipe.Get(ctx, keys.dayUsed)
	dayHeld := pipe.Get(ctx, keys.dayHeld)
	monthUsed := pipe.Get(ctx, keys.monthUsed)
	monthHeld := pipe.Get(ctx, keys.monthHeld)
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return gateway.QuotaUsage{}, err
	}
	return gateway.QuotaUsage{
		ActiveRequests:  redisInt64(active),
		RPMCurrent:      redisInt64(rpm),
		TPMCurrent:      redisInt64(tpm),
		SpendUsedMicros: redisInt64(spendUsed),
		SpendHeldMicros: redisInt64(spendHeld),
		DailyUsedTokens: redisInt64(dayUsed),
		DailyHeldTokens: redisInt64(dayHeld),
		MonthUsedTokens: redisInt64(monthUsed),
		MonthHeldTokens: redisInt64(monthHeld),
	}, nil
}

func redisInt64(cmd *redis.StringCmd) int64 {
	if cmd == nil {
		return 0
	}
	value, err := cmd.Int64()
	if err != nil && err != redis.Nil {
		return 0
	}
	return value
}
