package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestMemoryCounterStorePrunesExpiredChurn(t *testing.T) {
	counters := NewMemoryCounterStore()
	for i := 0; i < memoryCounterPruneEvery*3; i++ {
		if _, err := counters.Add(context.Background(), fmt.Sprintf("expired-%d", i), 1, -time.Second); err != nil {
			t.Fatal(err)
		}
	}
	// Each periodic sweep runs before the current entry is installed, so only
	// entries created since the last sweep can remain.
	if got := len(counters.entries); got > memoryCounterPruneEvery {
		t.Fatalf("expired counter state = %d entries, want <= %d", got, memoryCounterPruneEvery)
	}
}

func TestMemoryRateStorePruningIsPeriodicAndBounded(t *testing.T) {
	rates := NewMemoryRateStore()
	now := time.Now()
	for i := 0; i < memoryRatePruneLimit*3; i++ {
		rates.limiters[fmt.Sprintf("expired-%d", i)] = &memoryRateEntry{
			lastSeen: now.Add(-memoryRateEntryIdleTTL - time.Second),
		}
	}

	initial := len(rates.limiters)
	for i := 0; i < memoryRatePruneEvery-1; i++ {
		rates.pruneLocked(now)
	}
	if got := len(rates.limiters); got != initial {
		t.Fatalf("rate state before periodic prune = %d entries, want %d", got, initial)
	}

	rates.pruneLocked(now)
	if got, want := len(rates.limiters), initial-memoryRatePruneLimit; got != want {
		t.Fatalf("rate state after bounded prune = %d entries, want %d", got, want)
	}
	rates.pruneLocked(now)
	if got, want := len(rates.limiters), initial-memoryRatePruneLimit; got != want {
		t.Fatalf("rate state after non-periodic call = %d entries, want %d", got, want)
	}
}

func TestMemoryRateStorePruningPreservesDepletedLongWindow(t *testing.T) {
	rates := NewMemoryRateStore()
	now := time.Now()
	limit := RateLimit{Rate: 1, Burst: 1, Period: time.Hour}
	limiter := rates.getLimiterLocked("hourly", limit, now)
	if !limiter.AllowN(now, 1) {
		t.Fatal("initial hourly rate debit was rejected")
	}
	rates.limiters["hourly"].lastSeen = now.Add(-memoryRateEntryIdleTTL - time.Second)

	rates.operations = memoryRatePruneEvery - 1
	rates.pruneLocked(now)
	if rates.limiters["hourly"] == nil {
		t.Fatal("pruning discarded a depleted long-window limiter")
	}

	rates.operations = memoryRatePruneEvery - 1
	rates.pruneLocked(now.Add(time.Hour))
	if rates.limiters["hourly"] != nil {
		t.Fatal("pruning retained a fully replenished idle limiter")
	}
}

func TestDefaultPostgresPoolIsBounded(t *testing.T) {
	config := defaultPostgresPoolConfig()
	if config.maxOpenConns != defaultPostgresMaxOpenConns ||
		config.maxIdleConns != defaultPostgresMaxIdleConns ||
		config.connMaxLifetime != defaultPostgresConnMaxLifetime ||
		config.connMaxIdleTime != defaultPostgresConnMaxIdleTime {
		t.Fatalf("pool config = %#v, want repository defaults", config)
	}
	if config.maxOpenConns <= 0 || config.maxIdleConns < 0 || config.maxIdleConns > config.maxOpenConns ||
		config.connMaxLifetime <= 0 || config.connMaxIdleTime <= 0 {
		t.Fatalf("pool config is not safely bounded: %#v", config)
	}

	db, err := sql.Open("postgres", "postgres://unused@localhost/unused")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	configurePostgresPool(db, config)
	if got := db.Stats().MaxOpenConnections; got != defaultPostgresMaxOpenConns {
		t.Fatalf("max open connections = %d, want %d", got, defaultPostgresMaxOpenConns)
	}
}
