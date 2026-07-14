//go:build integration

package store

import (
	"context"
	"errors"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"llmgw/gateway"
)

// TestRedisRealServerIntegration is opt-in locally and runs in CI against an
// actual Redis service. Miniredis remains useful for deterministic clock tests,
// but it cannot validate Redis ACL enforcement or exact Lua command behavior.
func TestRedisRealServerIntegration(t *testing.T) {
	rawURL := strings.TrimSpace(os.Getenv("LLMGW_TEST_REDIS_URL"))
	if rawURL == "" {
		t.Skip("LLMGW_TEST_REDIS_URL is not configured")
	}

	namespace := "llmgw-integration-" + randomToken()
	state, err := NewRedisStoreFromURL(rawURL, namespace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	cleanupRedisIntegrationKeys(t, state, namespace)
	t.Cleanup(func() { cleanupRedisIntegrationKeys(t, state, namespace) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := state.ValidateStartup(ctx); err != nil {
		t.Fatalf("ValidateStartup() error = %v", err)
	}

	t.Run("scripts and accounting", func(t *testing.T) {
		if value, err := state.Add(ctx, "counter", 2, time.Minute); err != nil || value != 2 {
			t.Fatalf("Add() = (%d, %v), want (2, nil)", value, err)
		}

		rateLimit := RateLimit{Rate: 3, Burst: 3, Period: time.Minute}
		if err := state.AllowBatch(ctx, []RateRequest{
			{Key: "rpm:real", Limit: rateLimit, N: 1},
			{Key: "tpm:real", Limit: rateLimit, N: 2},
		}); err != nil {
			t.Fatalf("AllowBatch() error = %v", err)
		}

		if err := state.AcquireSlot(ctx, "real", "first", 1, time.Minute); err != nil {
			t.Fatalf("AcquireSlot(first) error = %v", err)
		}
		if err := state.AcquireSlot(ctx, "real", "second", 1, time.Minute); err == nil {
			t.Fatal("AcquireSlot(second) error = nil, want limit rejection")
		}
		if err := state.ReleaseSlot(ctx, "real", "first"); err != nil {
			t.Fatalf("ReleaseSlot(first) error = %v", err)
		}
		if err := state.AcquireSlot(ctx, "real", "second", 1, time.Minute); err != nil {
			t.Fatalf("AcquireSlot(second after release) error = %v", err)
		}
		if err := state.ReleaseSlot(ctx, "real", "second"); err != nil {
			t.Fatalf("ReleaseSlot(second) error = %v", err)
		}

		allowed, startedAt, err := state.BreakerAllowAttempt(ctx, "real", time.Hour)
		if err != nil || !allowed {
			t.Fatalf("BreakerAllow() = (%t, %v), want allowed", allowed, err)
		}
		if err := state.BreakerFailAttempt(ctx, "real", startedAt, 1, 50*time.Millisecond, "probe"); err != nil {
			t.Fatalf("BreakerFail() error = %v", err)
		}
		allowed, _, err = state.BreakerAllowAttempt(ctx, "real", time.Hour)
		if err != nil || allowed {
			t.Fatalf("BreakerAllow(open) = (%t, %v), want rejected", allowed, err)
		}
		time.Sleep(75 * time.Millisecond)
		allowed, startedAt, err = state.BreakerAllowAttempt(ctx, "real", time.Hour)
		if err != nil || !allowed {
			t.Fatalf("BreakerAllow(after cooldown) = (%t, %v), want allowed", allowed, err)
		}
		if err := state.BreakerSuccessAttempt(ctx, "real", startedAt); err != nil {
			t.Fatalf("BreakerSuccess() error = %v", err)
		}

		scope := gateway.ScopedLimit{
			Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "real"},
			Limits: gateway.LimitSpec{
				RPM:            20,
				TPM:            100,
				MaxParallel:    2,
				MaxSpendMicros: 1000,
				DailyTokens:    100,
				MonthlyTokens:  100,
				BudgetDuration: gateway.Duration{Duration: time.Hour},
			},
		}
		ticket, err := state.Reserve(ctx, "real-commit", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
			InputTokens: 2, ReservedOutputTokens: 3, EstimatedSpendMicros: 10,
		}, time.Minute)
		if err != nil {
			t.Fatalf("Reserve(commit) error = %v", err)
		}
		if err := state.TopUp(ctx, ticket, []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
			ReservedOutputTokens: 1, EstimatedSpendMicros: 2,
		}, time.Minute); err != nil {
			t.Fatalf("TopUp() error = %v", err)
		}
		if err := state.Commit(ctx, ticket, gateway.ActualUsage{InputTokens: 2, OutputTokens: 2, SpendMicros: 8}); err != nil {
			t.Fatalf("Commit() error = %v", err)
		}
		refund, err := state.Reserve(ctx, "real-refund", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 1}, time.Minute)
		if err != nil {
			t.Fatalf("Reserve(refund) error = %v", err)
		}
		if err := state.Refund(ctx, refund); err != nil {
			t.Fatalf("Refund() error = %v", err)
		}
		usage, err := state.GetUsage(ctx, scope)
		if err != nil {
			t.Fatalf("GetUsage() error = %v", err)
		}
		if usage.ActiveRequests != 0 || usage.RPMCurrent != 2 || usage.TPMCurrent != 4 ||
			usage.SpendUsedMicros != 8 || usage.SpendHeldMicros != 0 ||
			usage.DailyUsedTokens != 4 || usage.DailyHeldTokens != 0 ||
			usage.MonthUsedTokens != 4 || usage.MonthHeldTokens != 0 {
			t.Fatalf("GetUsage() = %#v, want settled commit with no live holds", usage)
		}
	})

	t.Run("numeric boundary mutations retain tickets", func(t *testing.T) {
		scope := gateway.ScopedLimit{
			Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "real-boundary"},
			Limits: gateway.LimitSpec{
				MaxSpendMicros: gateway.MaximumQuotaValue,
				DailyTokens:    gateway.MaximumQuotaValue,
				BudgetDuration: gateway.Duration{Duration: time.Hour},
			},
		}
		exact, err := state.Reserve(ctx, "real-boundary-exact", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
			InputTokens:          gateway.MaximumQuotaValue - 1,
			EstimatedSpendMicros: gateway.MaximumQuotaValue - 1,
		}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.TopUp(ctx, exact, nil, gateway.EstimatedUsage{InputTokens: 1, EstimatedSpendMicros: 1}, time.Minute); err != nil {
			t.Fatalf("TopUp(to exact maximum) error = %v", err)
		}
		usage, err := state.GetUsage(ctx, scope)
		if err != nil {
			t.Fatal(err)
		}
		if usage.DailyHeldTokens != gateway.MaximumQuotaValue || usage.SpendHeldMicros != gateway.MaximumQuotaValue {
			t.Fatalf("exact maximum usage = %#v", usage)
		}
		if err := state.Refund(ctx, exact); err != nil {
			t.Fatal(err)
		}

		now, err := state.redisNow(ctx)
		if err != nil {
			t.Fatal(err)
		}
		keys := quotaKeysForScope(scope.Ref, scope.Limits, now)
		topUpTicket, err := state.Reserve(ctx, "real-boundary-top-up", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
			InputTokens: 1, EstimatedSpendMicros: 1,
		}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.client.Set(ctx, state.key(keys.spendHeld), strconv.FormatInt(math.MaxInt64, 10), time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if err := state.TopUp(ctx, topUpTicket, nil, gateway.EstimatedUsage{EstimatedSpendMicros: 1}, time.Minute); !errors.Is(err, ErrQuotaAccountingCapacity) {
			t.Fatalf("TopUp(over capacity) error = %v", err)
		}
		if exists, err := state.client.Exists(ctx, state.ticketKey(topUpTicket.RequestID)).Result(); err != nil || exists != 1 {
			t.Fatalf("TopUp ticket retention = %d, %v", exists, err)
		}
		if err := state.client.Set(ctx, state.key(keys.spendHeld), 1, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if err := state.Refund(ctx, topUpTicket); err != nil {
			t.Fatal(err)
		}

		settleTicket, err := state.Reserve(ctx, "real-boundary-settle", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
			InputTokens: 1, EstimatedSpendMicros: 1,
		}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.client.Set(ctx, state.key(keys.spendUsed), strconv.FormatInt(math.MaxInt64, 10), time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
		if err := state.Commit(ctx, settleTicket, gateway.ActualUsage{InputTokens: 1, SpendMicros: 1}); !errors.Is(err, ErrQuotaAccountingCapacity) {
			t.Fatalf("Commit(over capacity) error = %v", err)
		}
		if exists, err := state.client.Exists(ctx, state.ticketKey(settleTicket.RequestID)).Result(); err != nil || exists != 1 {
			t.Fatalf("settlement ticket retention = %d, %v", exists, err)
		}
		if err := state.client.Set(ctx, state.key(keys.spendUsed), 0, time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
		if err := state.Commit(ctx, settleTicket, gateway.ActualUsage{InputTokens: 1, SpendMicros: 1}); err != nil {
			t.Fatalf("Commit(after repair) error = %v", err)
		}
	})

	t.Run("command granular ACL", func(t *testing.T) {
		testRedisStartupACL(t, ctx, state, namespace)
	})

	keys, err := redisIntegrationKeys(ctx, state, namespace)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, namespace+":") && !strings.HasPrefix(key, "rate:"+namespace+":") {
			t.Fatalf("integration state escaped namespace: %q", key)
		}
	}
}

func testRedisStartupACL(t *testing.T, ctx context.Context, admin *RedisStore, namespace string) {
	t.Helper()
	username := "llmgw_probe_" + randomToken()
	password := "p" + randomToken()
	commands := []any{
		"reset", "on", ">" + password, "~" + namespace + ":*",
		"+auth", "+hello", "+client", "+select", "+eval", "+evalsha",
		"+ping", "+time", "+set", "+get", "+exists", "+incrby", "+pttl", "+pexpireat",
		"+hset", "+hget", "+hdel", "+pexpire",
		"+zadd", "+zscore", "+zcard", "+zrangebyscore", "+zrevrange", "+zrem", "+zremrangebyscore",
		"+del",
	}
	args := append([]any{"ACL", "SETUSER", username}, commands...)
	if err := admin.client.Do(ctx, args...).Err(); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "noperm") || strings.Contains(strings.ToLower(err.Error()), "unknown command") {
			t.Skipf("Redis endpoint does not permit ACL integration setup: %v", err)
		}
		t.Fatalf("ACL SETUSER error = %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := admin.client.Do(cleanupCtx, "ACL", "DELUSER", username).Err(); err != nil {
			t.Errorf("ACL DELUSER error = %v", err)
		}
	})

	options := *admin.client.Options()
	options.Username = username
	options.Password = password
	restricted := newRedisStore(&options, namespace)
	t.Cleanup(func() { _ = restricted.Close() })
	if err := restricted.ValidateStartup(ctx); err != nil {
		t.Fatalf("ValidateStartup() with exact ACL error = %v", err)
	}
	if err := admin.client.Do(ctx, "ACL", "SETUSER", username, "-incrby").Err(); err != nil {
		t.Fatalf("ACL SETUSER remove INCRBY error = %v", err)
	}
	if err := restricted.ValidateStartup(ctx); err == nil {
		t.Fatal("ValidateStartup() accepted an ACL missing required INCRBY permission")
	}
}

func cleanupRedisIntegrationKeys(t *testing.T, state *RedisStore, namespace string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	keys, err := redisIntegrationKeys(ctx, state, namespace)
	if err != nil {
		t.Errorf("scan integration keys: %v", err)
		return
	}
	if len(keys) > 0 {
		if err := state.client.Del(ctx, keys...).Err(); err != nil {
			t.Errorf("delete integration keys: %v", err)
		}
	}
}

func redisIntegrationKeys(ctx context.Context, state *RedisStore, namespace string) ([]string, error) {
	var (
		cursor uint64
		out    []string
	)
	for {
		keys, next, err := state.client.Scan(ctx, cursor, "*"+namespace+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
		cursor = next
		if cursor == 0 {
			return out, nil
		}
	}
}
