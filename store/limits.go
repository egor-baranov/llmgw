package store

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"llmgw/gateway"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type QuotaLimitStore interface {
	Get(ctx context.Context, keyID string) (gateway.LimitSpec, bool, error)
	Put(ctx context.Context, keyID string, limit gateway.LimitSpec) error
}

type QuotaUsageStore interface {
	GetUsage(ctx context.Context, scope gateway.ScopedLimit) (gateway.QuotaUsage, error)
}

// QuotaLimitSnapshotSource is the durable source used to build an in-memory
// quota-limit snapshot. LoadAll is deliberately separate from Get so the
// inference path never needs to query the durable store one key at a time.
type QuotaLimitSnapshotSource interface {
	QuotaLimitStore
	LoadAll(ctx context.Context) (map[string]gateway.LimitSpec, error)
}

type MemoryQuotaLimitStore struct {
	mu     sync.RWMutex
	limits map[string]gateway.LimitSpec
}

type CachedQuotaLimitStore struct {
	source   QuotaLimitStore
	ttl      time.Duration
	capacity int
	mu       sync.Mutex
	cache    map[string]*list.Element
	lru      *list.List
	// writeGeneration keeps an older in-flight source read from replacing a
	// cache value published by a successful Put.
	writeGeneration uint64
}

type cachedQuotaLimit struct {
	key     string
	limit   gateway.LimitSpec
	found   bool
	expires time.Time
}

type quotaLimitSnapshot struct {
	limits map[string]gateway.LimitSpec
}

// RefreshingQuotaLimitStore serves reads from an immutable in-memory snapshot.
// Durable reads only happen during startup and background/manual refreshes.
type RefreshingQuotaLimitStore struct {
	source QuotaLimitSnapshotSource
	ttl    time.Duration

	snapshot         atomic.Pointer[quotaLimitSnapshot]
	closed           atomic.Bool
	updateMu         sync.Mutex
	healthMu         sync.RWMutex
	lastErr          error
	errorHook        func(error)
	healthGeneration uint64
	healthChanged    chan struct{}
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	close            sync.Once
	closeErr         error
}

type PostgresQuotaLimitStore struct {
	db    *sql.DB
	table string
}

var quotaTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

var ErrQuotaLimitStoreClosed = errors.New("quota limit store is closed")

const (
	defaultQuotaLimitCacheCapacity = 4096
	defaultPostgresMaxOpenConns    = 8
	defaultPostgresMaxIdleConns    = 4
	defaultPostgresConnMaxLifetime = 30 * time.Minute
	defaultPostgresConnMaxIdleTime = 5 * time.Minute
)

type postgresPoolConfig struct {
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
}

func defaultPostgresPoolConfig() postgresPoolConfig {
	return postgresPoolConfig{
		maxOpenConns:    defaultPostgresMaxOpenConns,
		maxIdleConns:    defaultPostgresMaxIdleConns,
		connMaxLifetime: defaultPostgresConnMaxLifetime,
		connMaxIdleTime: defaultPostgresConnMaxIdleTime,
	}
}

func configurePostgresPool(db *sql.DB, config postgresPoolConfig) {
	db.SetMaxOpenConns(config.maxOpenConns)
	db.SetMaxIdleConns(config.maxIdleConns)
	db.SetConnMaxLifetime(config.connMaxLifetime)
	db.SetConnMaxIdleTime(config.connMaxIdleTime)
}

func NewMemoryQuotaLimitStore() *MemoryQuotaLimitStore {
	return &MemoryQuotaLimitStore{limits: map[string]gateway.LimitSpec{}}
}

func NewCachedQuotaLimitStore(source QuotaLimitStore, ttl time.Duration) *CachedQuotaLimitStore {
	return NewCachedQuotaLimitStoreWithCapacity(source, ttl, defaultQuotaLimitCacheCapacity)
}

func NewCachedQuotaLimitStoreWithCapacity(source QuotaLimitStore, ttl time.Duration, capacity int) *CachedQuotaLimitStore {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if capacity <= 0 {
		capacity = defaultQuotaLimitCacheCapacity
	}
	return &CachedQuotaLimitStore{
		source:   source,
		ttl:      ttl,
		capacity: capacity,
		cache:    make(map[string]*list.Element, capacity),
		lru:      list.New(),
	}
}

func NewRefreshingQuotaLimitStore(ctx context.Context, source QuotaLimitSnapshotSource, ttl time.Duration) (*RefreshingQuotaLimitStore, error) {
	if source == nil {
		return nil, fmt.Errorf("quota limit snapshot source is required")
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	limits, err := source.LoadAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("load quota limit snapshot: %w", err)
	}
	if err := validateQuotaLimitMap(limits); err != nil {
		return nil, fmt.Errorf("validate quota limit snapshot: %w", err)
	}
	refreshCtx, cancel := context.WithCancel(context.Background())
	s := &RefreshingQuotaLimitStore{
		source:        source,
		ttl:           ttl,
		healthChanged: make(chan struct{}, 1),
		cancel:        cancel,
	}
	s.snapshot.Store(newQuotaLimitSnapshot(limits))
	s.wg.Add(1)
	go s.refreshLoop(refreshCtx)
	go s.refreshErrorHookLoop(refreshCtx)
	return s, nil
}

func NewPostgresRefreshingQuotaLimitStore(ctx context.Context, dsn, table string, ttl time.Duration) (*RefreshingQuotaLimitStore, error) {
	source, err := NewPostgresQuotaLimitStore(ctx, dsn, table)
	if err != nil {
		return nil, err
	}
	snapshot, err := NewRefreshingQuotaLimitStore(ctx, source, ttl)
	if err != nil {
		_ = source.Close()
		return nil, err
	}
	return snapshot, nil
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
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	configurePostgresPool(db, defaultPostgresPoolConfig())
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

func (s *MemoryQuotaLimitStore) Get(ctx context.Context, keyID string) (gateway.LimitSpec, bool, error) {
	if err := ctx.Err(); err != nil {
		return gateway.LimitSpec{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return gateway.LimitSpec{}, false, err
	}
	limit, ok := s.limits[keyID]
	return cloneLimitSpec(limit), ok, nil
}

func (s *MemoryQuotaLimitStore) Put(ctx context.Context, keyID string, limit gateway.LimitSpec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateQuotaLimit(keyID, limit); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s.limits[keyID] = cloneLimitSpec(limit)
	return nil
}

func (s *CachedQuotaLimitStore) Get(ctx context.Context, keyID string) (gateway.LimitSpec, bool, error) {
	if keyID == "" {
		return gateway.LimitSpec{}, false, nil
	}
	now := time.Now()
	s.mu.Lock()
	if elem, ok := s.cache[keyID]; ok {
		entry := elem.Value.(cachedQuotaLimit)
		if now.Before(entry.expires) {
			s.lru.MoveToFront(elem)
			s.mu.Unlock()
			return cloneLimitSpec(entry.limit), entry.found, nil
		}
		s.removeCacheElementLocked(elem)
	}
	writeGeneration := s.writeGeneration
	s.mu.Unlock()
	if s.source == nil {
		return gateway.LimitSpec{}, false, nil
	}
	limit, found, err := s.source.Get(ctx, keyID)
	if err != nil {
		return gateway.LimitSpec{}, false, err
	}
	limit = cloneLimitSpec(limit)
	s.mu.Lock()
	if writeGeneration == s.writeGeneration {
		now = time.Now()
		s.pruneExpiredLocked(now)
		s.putCacheEntryLocked(cachedQuotaLimit{
			key:     keyID,
			limit:   limit,
			found:   found,
			expires: now.Add(s.ttl),
		})
	}
	s.mu.Unlock()
	return cloneLimitSpec(limit), found, nil
}

func (s *CachedQuotaLimitStore) Put(ctx context.Context, keyID string, limit gateway.LimitSpec) error {
	if err := validateQuotaLimit(keyID, limit); err != nil {
		return err
	}
	if s.source == nil {
		return nil
	}
	if err := s.source.Put(ctx, keyID, limit); err != nil {
		return err
	}
	limit = cloneLimitSpec(limit)
	s.mu.Lock()
	now := time.Now()
	s.pruneExpiredLocked(now)
	s.writeGeneration++
	s.putCacheEntryLocked(cachedQuotaLimit{
		key:     keyID,
		limit:   limit,
		found:   true,
		expires: now.Add(s.ttl),
	})
	s.mu.Unlock()
	return nil
}

func (s *CachedQuotaLimitStore) putCacheEntryLocked(entry cachedQuotaLimit) {
	if elem, ok := s.cache[entry.key]; ok {
		elem.Value = entry
		s.lru.MoveToFront(elem)
		return
	}
	elem := s.lru.PushFront(entry)
	s.cache[entry.key] = elem
	for len(s.cache) > s.capacity {
		s.removeCacheElementLocked(s.lru.Back())
	}
}

func (s *CachedQuotaLimitStore) pruneExpiredLocked(now time.Time) {
	for elem := s.lru.Back(); elem != nil; {
		previous := elem.Prev()
		if !now.Before(elem.Value.(cachedQuotaLimit).expires) {
			s.removeCacheElementLocked(elem)
		}
		elem = previous
	}
}

func (s *CachedQuotaLimitStore) removeCacheElementLocked(elem *list.Element) {
	if elem == nil {
		return
	}
	delete(s.cache, elem.Value.(cachedQuotaLimit).key)
	s.lru.Remove(elem)
}

func (s *CachedQuotaLimitStore) Close() error {
	if closer, ok := s.source.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func (s *CachedQuotaLimitStore) Ping(ctx context.Context) error {
	if s == nil || s.source == nil {
		return nil
	}
	if checker, ok := s.source.(HealthChecker); ok {
		return checker.Ping(ctx)
	}
	return nil
}

func (s *RefreshingQuotaLimitStore) Get(_ context.Context, keyID string) (gateway.LimitSpec, bool, error) {
	if s == nil || s.closed.Load() {
		return gateway.LimitSpec{}, false, ErrQuotaLimitStoreClosed
	}
	if keyID == "" {
		return gateway.LimitSpec{}, false, nil
	}
	snapshot := s.snapshot.Load()
	if snapshot == nil {
		return gateway.LimitSpec{}, false, nil
	}
	limit, found := snapshot.limits[keyID]
	return cloneLimitSpec(limit), found, nil
}

func (s *RefreshingQuotaLimitStore) Put(ctx context.Context, keyID string, limit gateway.LimitSpec) error {
	if s == nil || s.closed.Load() {
		return ErrQuotaLimitStoreClosed
	}
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	if s.closed.Load() {
		return ErrQuotaLimitStoreClosed
	}
	if err := validateQuotaLimit(keyID, limit); err != nil {
		return err
	}
	if err := s.source.Put(ctx, keyID, limit); err != nil {
		return err
	}
	current := s.snapshot.Load()
	limits := make(map[string]gateway.LimitSpec, 1)
	if current != nil {
		limits = cloneLimitMap(current.limits)
	}
	limits[keyID] = cloneLimitSpec(limit)
	s.snapshot.Store(&quotaLimitSnapshot{limits: limits})
	return nil
}

// Refresh atomically replaces the current snapshot after a successful full
// source load. A failed refresh leaves the last known-good snapshot intact.
func (s *RefreshingQuotaLimitStore) Refresh(ctx context.Context) error {
	if s == nil || s.closed.Load() {
		return ErrQuotaLimitStoreClosed
	}
	s.updateMu.Lock()
	if s.closed.Load() {
		s.updateMu.Unlock()
		return ErrQuotaLimitStoreClosed
	}
	limits, err := s.source.LoadAll(ctx)
	if err == nil {
		err = validateQuotaLimitMap(limits)
	}
	if err == nil {
		s.snapshot.Store(newQuotaLimitSnapshot(limits))
	}
	s.updateMu.Unlock()

	// User callbacks run on the notifier goroutine, after updateMu is released.
	// This lets hooks safely inspect or update the store without lock inversion.
	s.healthMu.Lock()
	s.lastErr = err
	s.healthGeneration++
	s.healthMu.Unlock()
	s.signalRefreshErrorHook()
	return err
}

func (s *RefreshingQuotaLimitStore) refreshLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.ttl)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.Refresh(ctx)
		}
	}
}

func (s *RefreshingQuotaLimitStore) Close() error {
	if s == nil {
		return nil
	}
	s.close.Do(func() {
		s.closed.Store(true)
		s.healthMu.Lock()
		s.errorHook = nil
		s.healthGeneration++
		s.healthMu.Unlock()
		if s.cancel != nil {
			s.cancel()
		}
		s.wg.Wait()
		s.updateMu.Lock()
		defer s.updateMu.Unlock()
		if closer, ok := s.source.(interface{ Close() error }); ok {
			s.closeErr = closer.Close()
		}
	})
	return s.closeErr
}

func (s *RefreshingQuotaLimitStore) Ping(ctx context.Context) error {
	if s == nil || s.closed.Load() {
		return ErrQuotaLimitStoreClosed
	}
	if s.snapshot.Load() == nil {
		return fmt.Errorf("quota limit snapshot is not loaded")
	}
	return nil
}

// LastRefreshError reports control-plane staleness without making a valid
// last-known snapshot a data-plane readiness failure.
func (s *RefreshingQuotaLimitStore) LastRefreshError() error {
	if s == nil {
		return ErrQuotaLimitStoreClosed
	}
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	return s.lastErr
}

// SetRefreshErrorHook replaces the refresh-health callback. Notifications are
// serialized and delivered asynchronously; use LastRefreshError for a
// synchronous read of the current state.
func (s *RefreshingQuotaLimitStore) SetRefreshErrorHook(hook func(error)) {
	if s == nil || s.closed.Load() {
		return
	}
	s.healthMu.Lock()
	if s.closed.Load() {
		s.healthMu.Unlock()
		return
	}
	s.errorHook = hook
	s.healthGeneration++
	s.healthMu.Unlock()
	s.signalRefreshErrorHook()
}

func (s *RefreshingQuotaLimitStore) signalRefreshErrorHook() {
	select {
	case s.healthChanged <- struct{}{}:
	default:
	}
}

func (s *RefreshingQuotaLimitStore) refreshErrorHookLoop(ctx context.Context) {
	// This goroutine is intentionally not part of wg: hooks are allowed to call
	// Close, which waits for the refresh loop. Cancellation prevents new hook
	// calls; an already-running user callback is allowed to finish naturally.
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.healthChanged:
			s.notifyRefreshErrorHook(ctx)
		}
	}
}

func (s *RefreshingQuotaLimitStore) notifyRefreshErrorHook(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		s.healthMu.RLock()
		hook := s.errorHook
		err := s.lastErr
		generation := s.healthGeneration
		s.healthMu.RUnlock()
		if hook == nil {
			return
		}
		hook(err)
		// The callback runs without healthMu and may block while a refresh
		// publishes newer state. Recheck before considering this notification
		// complete so an older callback cannot be the final observation.
		s.healthMu.RLock()
		currentGeneration := s.healthGeneration
		s.healthMu.RUnlock()
		if currentGeneration == generation {
			return
		}
		// The pending signal represents the newer generation that this loop is
		// about to deliver. Consume it to avoid a duplicate callback afterward.
		select {
		case <-s.healthChanged:
		default:
		}
	}
}

func newQuotaLimitSnapshot(limits map[string]gateway.LimitSpec) *quotaLimitSnapshot {
	return &quotaLimitSnapshot{limits: cloneLimitMap(limits)}
}

func cloneLimitMap(limits map[string]gateway.LimitSpec) map[string]gateway.LimitSpec {
	clone := make(map[string]gateway.LimitSpec, len(limits))
	for keyID, limit := range limits {
		clone[keyID] = cloneLimitSpec(limit)
	}
	return clone
}

func cloneLimitSpec(limit gateway.LimitSpec) gateway.LimitSpec {
	limit.ModelAllowlist = append([]string(nil), limit.ModelAllowlist...)
	limit.ProviderAllowlist = append([]string(nil), limit.ProviderAllowlist...)
	return limit
}

func validateQuotaLimit(keyID string, limit gateway.LimitSpec) error {
	if strings.TrimSpace(keyID) == "" {
		return fmt.Errorf("quota limit key id must not be empty")
	}
	if err := gateway.ValidateLimitSpec(limit); err != nil {
		return fmt.Errorf("quota limit %q is invalid: %w", keyID, err)
	}
	return nil
}

func validateQuotaLimitMap(limits map[string]gateway.LimitSpec) error {
	for keyID, limit := range limits {
		if err := validateQuotaLimit(keyID, limit); err != nil {
			return err
		}
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
	limit, err := scanQuotaLimit(row)
	if err == sql.ErrNoRows {
		return gateway.LimitSpec{}, false, nil
	}
	if err != nil {
		return gateway.LimitSpec{}, false, err
	}
	if err := validateQuotaLimit(keyID, limit); err != nil {
		return gateway.LimitSpec{}, false, err
	}
	return limit, true, nil
}

func (s *PostgresQuotaLimitStore) LoadAll(ctx context.Context) (map[string]gateway.LimitSpec, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT key_id, rpm, tpm, max_parallel, max_spend_micros, soft_spend_micros,
		       daily_tokens, monthly_tokens, budget_duration, max_input_tokens, max_output_tokens,
		       model_allowlist, provider_allowlist
		FROM %s
	`, s.table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	limits := make(map[string]gateway.LimitSpec)
	for rows.Next() {
		var keyID string
		limit, err := scanQuotaLimitWithKey(rows, &keyID)
		if err != nil {
			return nil, err
		}
		if err := validateQuotaLimit(keyID, limit); err != nil {
			return nil, err
		}
		limits[keyID] = limit
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return limits, nil
}

type quotaLimitScanner interface {
	Scan(dest ...any) error
}

func scanQuotaLimit(scanner quotaLimitScanner) (gateway.LimitSpec, error) {
	return scanQuotaLimitWithKey(scanner, nil)
}

func scanQuotaLimitWithKey(scanner quotaLimitScanner, keyID *string) (gateway.LimitSpec, error) {
	var limit gateway.LimitSpec
	var budget sql.NullString
	var models []byte
	var providers []byte
	dest := make([]any, 0, 13)
	if keyID != nil {
		dest = append(dest, keyID)
	}
	dest = append(dest,
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
	if err := scanner.Scan(dest...); err != nil {
		return gateway.LimitSpec{}, err
	}
	if budget.Valid && budget.String != "" {
		if parsed, parseErr := time.ParseDuration(budget.String); parseErr == nil {
			limit.BudgetDuration.Duration = parsed
		} else {
			return gateway.LimitSpec{}, parseErr
		}
	}
	if len(models) > 0 {
		if err := json.Unmarshal(models, &limit.ModelAllowlist); err != nil {
			return gateway.LimitSpec{}, err
		}
	}
	if len(providers) > 0 {
		if err := json.Unmarshal(providers, &limit.ProviderAllowlist); err != nil {
			return gateway.LimitSpec{}, err
		}
	}
	return limit, nil
}

func (s *PostgresQuotaLimitStore) Put(ctx context.Context, keyID string, limit gateway.LimitSpec) error {
	if err := validateQuotaLimit(keyID, limit); err != nil {
		return err
	}
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

func (s *PostgresQuotaLimitStore) Ping(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres quota limit store is not configured")
	}
	return s.db.PingContext(ctx)
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

func (s *MemoryQuotaStore) GetUsage(ctx context.Context, scope gateway.ScopedLimit) (gateway.QuotaUsage, error) {
	if err := ctx.Err(); err != nil {
		return gateway.QuotaUsage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return gateway.QuotaUsage{}, err
	}
	now := time.Now()
	s.cleanupExpired(now)
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
	now, err := s.redisNow(ctx)
	if err != nil {
		return gateway.QuotaUsage{}, err
	}
	keys := quotaKeysForScope(scope.Ref, scope.Limits, now)
	leaseSet, leaseData := redisLeaseKeys(scope.Ref)
	leaseSet, leaseData = s.key(leaseSet), s.key(leaseData)
	if _, err := redisQuotaPruneScript.Run(ctx, s.client, []string{leaseSet, leaseData}).Result(); err != nil {
		return gateway.QuotaUsage{}, err
	}
	pipe := s.client.Pipeline()
	active := queuedRedisGet(pipe, ctx, s.key(keys.active))
	rpm := queuedRedisGet(pipe, ctx, s.key(keys.rpm))
	tpm := queuedRedisGet(pipe, ctx, s.key(keys.tpm))
	spendUsed := queuedRedisGet(pipe, ctx, s.key(keys.spendUsed))
	spendHeld := queuedRedisGet(pipe, ctx, s.key(keys.spendHeld))
	dayUsed := queuedRedisGet(pipe, ctx, s.key(keys.dayUsed))
	dayHeld := queuedRedisGet(pipe, ctx, s.key(keys.dayHeld))
	monthUsed := queuedRedisGet(pipe, ctx, s.key(keys.monthUsed))
	monthHeld := queuedRedisGet(pipe, ctx, s.key(keys.monthHeld))
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return gateway.QuotaUsage{}, err
	}
	usage := gateway.QuotaUsage{}
	values := []struct {
		name string
		cmd  *redis.StringCmd
		dst  *int64
	}{
		{name: "active_requests", cmd: active, dst: &usage.ActiveRequests},
		{name: "rpm_current", cmd: rpm, dst: &usage.RPMCurrent},
		{name: "tpm_current", cmd: tpm, dst: &usage.TPMCurrent},
		{name: "spend_used_micros", cmd: spendUsed, dst: &usage.SpendUsedMicros},
		{name: "spend_held_micros", cmd: spendHeld, dst: &usage.SpendHeldMicros},
		{name: "daily_used_tokens", cmd: dayUsed, dst: &usage.DailyUsedTokens},
		{name: "daily_held_tokens", cmd: dayHeld, dst: &usage.DailyHeldTokens},
		{name: "month_used_tokens", cmd: monthUsed, dst: &usage.MonthUsedTokens},
		{name: "month_held_tokens", cmd: monthHeld, dst: &usage.MonthHeldTokens},
	}
	for _, value := range values {
		parsed, err := redisInt64(value.cmd)
		if err != nil {
			return gateway.QuotaUsage{}, fmt.Errorf("decode Redis quota usage %s: %w", value.name, err)
		}
		*value.dst = nonnegative(parsed)
	}
	return usage, nil
}

func queuedRedisGet(pipe redis.Pipeliner, ctx context.Context, key string) *redis.StringCmd {
	if key == "" {
		return nil
	}
	return pipe.Get(ctx, key)
}

func nonnegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func redisInt64(cmd *redis.StringCmd) (int64, error) {
	if cmd == nil {
		return 0, nil
	}
	value, err := cmd.Int64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return value, nil
}
