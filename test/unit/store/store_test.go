package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"llmgw/gateway"
	"llmgw/store"
)

func TestMemoryQuotaReserveCommitRefund(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	scopes := []gateway.ScopedLimit{{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-1"},
		Limits: gateway.LimitSpec{DailyTokens: 1000},
	}}
	first, err := quota.Reserve(context.Background(), "req-1", scopes, gateway.EstimatedUsage{InputTokens: 40, ReservedOutputTokens: 60}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := quota.Commit(context.Background(), first, gateway.ActualUsage{InputTokens: 40, OutputTokens: 40}); err != nil {
		t.Fatal(err)
	}
	second, err := quota.Reserve(context.Background(), "req-2", scopes, gateway.EstimatedUsage{InputTokens: 460, ReservedOutputTokens: 460}, time.Minute)
	if err != nil {
		t.Fatalf("second reserve error = %v, want nil", err)
	}
	if err := quota.Refund(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if _, err := quota.Reserve(context.Background(), "req-3", scopes, gateway.EstimatedUsage{InputTokens: 500, ReservedOutputTokens: 501}, time.Minute); err == nil {
		t.Fatal("Reserve() error = nil, want quota exceeded")
	}
}

func TestMemoryCounterStoreConcurrentAdd(t *testing.T) {
	counter := store.NewMemoryCounterStore()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if _, err := counter.Add(context.Background(), "rpm:test", 1, time.Minute); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	value, err := counter.Add(context.Background(), "rpm:test", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if value != 320 {
		t.Fatalf("counter value = %d, want 320", value)
	}
}

func TestMemoryRateStoreAllowAndReject(t *testing.T) {
	rates := store.NewMemoryRateStore()
	limit := store.RateLimit{Rate: 2, Burst: 2, Period: time.Minute}

	if err := rates.Allow(context.Background(), "rpm:test", limit, 1); err != nil {
		t.Fatalf("first Allow() error = %v, want nil", err)
	}
	if err := rates.Allow(context.Background(), "rpm:test", limit, 1); err != nil {
		t.Fatalf("second Allow() error = %v, want nil", err)
	}
	if err := rates.Allow(context.Background(), "rpm:test", limit, 1); err == nil {
		t.Fatal("third Allow() error = nil, want rate limited")
	}

	if err := rates.Allow(context.Background(), "rpm:other", limit, 1); err != nil {
		t.Fatalf("other key Allow() error = %v, want nil", err)
	}
}

func TestMemoryQuotaLimitStorePutGet(t *testing.T) {
	limits := store.NewMemoryQuotaLimitStore()
	want := gateway.LimitSpec{
		RPM:             12,
		TPM:             345,
		MaxParallel:     2,
		DailyTokens:     1000,
		MaxInputTokens:  100,
		MaxOutputTokens: 50,
	}
	if err := limits.Put(context.Background(), "key-1", want); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	got, ok, err := limits.Get(context.Background(), "key-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if got.RPM != want.RPM || got.TPM != want.TPM || got.MaxParallel != want.MaxParallel || got.DailyTokens != want.DailyTokens {
		t.Fatalf("Get() = %#v, want %#v", got, want)
	}
}

func TestCachedQuotaLimitStoreCachesAndRefreshes(t *testing.T) {
	source := &stubQuotaLimitStore{
		limits: map[string]gateway.LimitSpec{
			"key-1": {RPM: 10},
		},
	}
	cached := store.NewCachedQuotaLimitStore(source, 20*time.Millisecond)

	first, ok, err := cached.Get(context.Background(), "key-1")
	if err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	if !ok || first.RPM != 10 {
		t.Fatalf("first Get() = (%#v, %t), want RPM 10 true", first, ok)
	}
	source.mu.Lock()
	source.limits["key-1"] = gateway.LimitSpec{RPM: 20}
	source.mu.Unlock()

	second, ok, err := cached.Get(context.Background(), "key-1")
	if err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	if !ok || second.RPM != 10 {
		t.Fatalf("second Get() = (%#v, %t), want cached RPM 10 true", second, ok)
	}
	if source.gets != 1 {
		t.Fatalf("source gets = %d, want 1 while cache is warm", source.gets)
	}

	time.Sleep(25 * time.Millisecond)
	third, ok, err := cached.Get(context.Background(), "key-1")
	if err != nil {
		t.Fatalf("third Get() error = %v", err)
	}
	if !ok || third.RPM != 20 {
		t.Fatalf("third Get() = (%#v, %t), want refreshed RPM 20 true", third, ok)
	}
	if source.gets < 2 {
		t.Fatalf("source gets = %d, want cache refresh after ttl", source.gets)
	}
}

func TestMemoryQuotaStoreGetUsage(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	scope := gateway.ScopedLimit{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-usage"},
		Limits: gateway.LimitSpec{
			DailyTokens: 1000,
		},
	}
	ticket, err := quota.Reserve(context.Background(), "req-usage", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens:          10,
		ReservedOutputTokens: 5,
		EstimatedSpendMicros: 100,
	}, time.Minute)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	usage, err := quota.GetUsage(context.Background(), scope)
	if err != nil {
		t.Fatalf("GetUsage() after reserve error = %v", err)
	}
	if usage.ActiveRequests != 1 || usage.RPMCurrent != 1 || usage.TPMCurrent != 15 || usage.DailyHeldTokens != 15 {
		t.Fatalf("usage after reserve = %#v", usage)
	}

	if err := quota.Commit(context.Background(), ticket, gateway.ActualUsage{
		InputTokens:  10,
		OutputTokens: 4,
		SpendMicros:  80,
	}); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	usage, err = quota.GetUsage(context.Background(), scope)
	if err != nil {
		t.Fatalf("GetUsage() after commit error = %v", err)
	}
	if usage.ActiveRequests != 0 || usage.DailyHeldTokens != 0 || usage.DailyUsedTokens != 14 || usage.SpendUsedMicros != 80 {
		t.Fatalf("usage after commit = %#v", usage)
	}
}

func TestMemoryQuotaStoreTopUpUpdatesHeldUsage(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	scope := gateway.ScopedLimit{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-topup"},
		Limits: gateway.LimitSpec{
			DailyTokens: 20,
		},
	}
	ticket, err := quota.Reserve(context.Background(), "req-topup", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens:          4,
		ReservedOutputTokens: 4,
	}, time.Minute)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if err := quota.TopUp(context.Background(), ticket, []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens:          3,
		ReservedOutputTokens: 2,
	}, time.Minute); err != nil {
		t.Fatalf("TopUp() error = %v", err)
	}
	usage, err := quota.GetUsage(context.Background(), scope)
	if err != nil {
		t.Fatalf("GetUsage() error = %v", err)
	}
	if usage.ActiveRequests != 1 || usage.DailyHeldTokens != 13 || usage.TPMCurrent != 13 {
		t.Fatalf("usage after topup = %#v, want 13 held tokens and active request", usage)
	}
	if err := quota.TopUp(context.Background(), ticket, []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens:          4,
		ReservedOutputTokens: 4,
	}, time.Minute); err == nil {
		t.Fatal("TopUp() error = nil, want daily token quota exceeded")
	}
}

func TestCachedQuotaLimitStorePropagatesSourceError(t *testing.T) {
	cached := store.NewCachedQuotaLimitStore(&stubQuotaLimitStore{err: errors.New("boom")}, time.Minute)
	if _, _, err := cached.Get(context.Background(), "key-1"); err == nil {
		t.Fatal("Get() error = nil, want source error")
	}
}

func TestCachedQuotaLimitStorePutWarmsCache(t *testing.T) {
	source := &stubQuotaLimitStore{}
	cached := store.NewCachedQuotaLimitStore(source, time.Minute)
	if err := cached.Put(context.Background(), "key-1", gateway.LimitSpec{RPM: 33}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	source.mu.Lock()
	source.limits["key-1"] = gateway.LimitSpec{RPM: 99}
	source.mu.Unlock()

	got, ok, err := cached.Get(context.Background(), "key-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok || got.RPM != 33 {
		t.Fatalf("Get() = (%#v, %t), want cached RPM 33 true", got, ok)
	}
	if source.gets != 0 {
		t.Fatalf("source gets = %d, want 0 because put should warm cache", source.gets)
	}
}

func TestNewPostgresQuotaLimitStoreValidatesInput(t *testing.T) {
	if _, err := store.NewPostgresQuotaLimitStore(context.Background(), "", "quota_limits"); err == nil {
		t.Fatal("NewPostgresQuotaLimitStore() error = nil, want missing dsn error")
	}
	if _, err := store.NewPostgresQuotaLimitStore(context.Background(), "postgres://user@localhost/db", "invalid-name"); err == nil {
		t.Fatal("NewPostgresQuotaLimitStore() error = nil, want invalid table error")
	}
}

type stubQuotaLimitStore struct {
	mu     sync.Mutex
	limits map[string]gateway.LimitSpec
	gets   int
	err    error
}

func (s *stubQuotaLimitStore) Get(_ context.Context, keyID string) (gateway.LimitSpec, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if s.err != nil {
		return gateway.LimitSpec{}, false, s.err
	}
	limit, ok := s.limits[keyID]
	return limit, ok, nil
}

func (s *stubQuotaLimitStore) Put(_ context.Context, keyID string, limit gateway.LimitSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.limits == nil {
		s.limits = map[string]gateway.LimitSpec{}
	}
	s.limits[keyID] = limit
	return s.err
}
