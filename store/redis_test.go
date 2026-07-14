package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"llmgw/gateway"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisStoreFromURLConfiguresTLSAndACL(t *testing.T) {
	state, err := NewRedisStoreFromURL("rediss://gateway:secret@redis.example.invalid:6380/2", "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	options := state.client.Options()
	if options.Addr != "redis.example.invalid:6380" || options.Username != "gateway" || options.Password != "secret" || options.DB != 2 {
		t.Fatalf("Redis options = %#v", options)
	}
	if options.TLSConfig == nil {
		t.Fatal("rediss URL did not enable TLS")
	}
	if !options.ContextTimeoutEnabled {
		t.Fatal("Redis client does not honor command context deadlines")
	}
	if options.PoolSize != defaultRedisPoolSize || options.MaxIdleConns != defaultRedisPoolSize || options.MaxActiveConns != defaultRedisMaxActiveConns {
		t.Fatalf("Redis pool defaults = size %d/idle %d/active %d, want %d/%d/%d",
			options.PoolSize, options.MaxIdleConns, options.MaxActiveConns,
			defaultRedisPoolSize, defaultRedisPoolSize, defaultRedisMaxActiveConns)
	}
	if state.namespace != "tenant-a" {
		t.Fatalf("namespace = %q, want tenant-a", state.namespace)
	}
	if _, err := NewRedisStoreFromURL("https://redis.example.invalid", "tenant-a"); err == nil {
		t.Fatal("non-Redis URL was accepted")
	}
}

func TestRedisStoreFromURLRejectsUnsafeOptionsWithoutPanicking(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{name: "disabled read timeout", rawURL: "redis://localhost/0?read_timeout=0", want: "read_timeout"},
		{name: "excessive dial timeout", rawURL: "redis://localhost/0?dial_timeout=1m", want: "dial_timeout"},
		{name: "negative database", rawURL: "redis://localhost/-1", want: "database"},
		{name: "invalid protocol", rawURL: "redis://localhost/0?protocol=1", want: "protocol"},
		{name: "negative retries", rawURL: "redis://localhost/0?max_retries=-2", want: "max_retries"},
		{name: "excessive retries", rawURL: "redis://localhost/0?max_retries=11", want: "max_retries"},
		{name: "excessive retry backoff", rawURL: "redis://localhost/0?max_retry_backoff=6s", want: "max_retry_backoff"},
		{name: "negative pool size", rawURL: "redis://localhost/0?pool_size=-1", want: "pool_size"},
		{name: "excessive pool size", rawURL: "redis://localhost/0?pool_size=4097", want: "pool_size"},
		{name: "negative minimum idle", rawURL: "redis://localhost/0?min_idle_conns=-1", want: "min_idle_conns"},
		{name: "negative maximum idle", rawURL: "redis://localhost/0?max_idle_conns=-1", want: "max_idle_conns"},
		{name: "negative maximum active", rawURL: "redis://localhost/0?max_active_conns=-1", want: "max_active_conns"},
		{name: "minimum idle exceeds maximum idle", rawURL: "redis://localhost/0?pool_size=4&min_idle_conns=3&max_idle_conns=2", want: "min_idle_conns"},
		{name: "minimum idle exceeds pool", rawURL: "redis://localhost/0?pool_size=2&min_idle_conns=3", want: "min_idle_conns"},
		{name: "maximum idle exceeds pool", rawURL: "redis://localhost/0?pool_size=2&max_idle_conns=3", want: "max_idle_conns"},
		{name: "pool exceeds maximum active", rawURL: "redis://localhost/0?pool_size=4&max_active_conns=3", want: "pool_size"},
		{name: "retry backoffs inverted", rawURL: "redis://localhost/0?min_retry_backoff=1s&max_retry_backoff=500ms", want: "min_retry_backoff"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("NewRedisStoreFromURL(%q) panicked: %v", tt.rawURL, recovered)
				}
			}()
			state, err := NewRedisStoreFromURL(tt.rawURL, "tenant-a")
			if state != nil {
				_ = state.Close()
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tt.want) {
				t.Fatalf("NewRedisStoreFromURL(%q) error = %v, want %q", tt.rawURL, err, tt.want)
			}
		})
	}
}

func TestRedisStoreFromURLRedactsCredentialsFromParseErrors(t *testing.T) {
	const secret = "distinctive-redis-password"
	_, err := NewRedisStoreFromURL("redis://gateway:"+secret+"%zz@localhost/0", "tenant-a")
	if err == nil {
		t.Fatal("malformed Redis URL was accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("Redis parse error leaked credentials: %v", err)
	}
	if !strings.Contains(err.Error(), "invalid URL escape") {
		t.Fatalf("Redis parse error = %v, want sanitized reason", err)
	}
}

func TestRedisLegacyConstructorUsesBoundedPoolDefaults(t *testing.T) {
	state := NewRedisStore("localhost:6379", "", 0)
	t.Cleanup(func() { _ = state.Close() })
	options := state.client.Options()
	if options.PoolSize != defaultRedisPoolSize || options.MaxIdleConns != defaultRedisPoolSize || options.MaxActiveConns != defaultRedisMaxActiveConns {
		t.Fatalf("legacy Redis pool defaults = size %d/idle %d/active %d, want %d/%d/%d",
			options.PoolSize, options.MaxIdleConns, options.MaxActiveConns,
			defaultRedisPoolSize, defaultRedisPoolSize, defaultRedisMaxActiveConns)
	}
}

func TestRedisStartupCapabilityProbe(t *testing.T) {
	server := miniredis.RunT(t)
	state := NewRedisStoreWithNamespace(server.Addr(), "", 0, "tenant-a")
	defer state.Close()
	if err := state.ValidateStartup(context.Background()); err != nil {
		t.Fatalf("ValidateStartup() error = %v", err)
	}
	for _, key := range server.Keys() {
		if strings.Contains(key, "startup-probe") {
			t.Fatalf("startup capability probe leaked key %q", key)
		}
	}
}

func TestRedisNamespacesIsolateRateCounterAndQuotaState(t *testing.T) {
	server := miniredis.RunT(t)
	first := NewRedisStoreWithNamespace(server.Addr(), "", 0, "tenant-a")
	second := NewRedisStoreWithNamespace(server.Addr(), "", 0, "tenant-b")
	defer first.Close()
	defer second.Close()
	ctx := context.Background()
	limit := RateLimit{Rate: 1, Burst: 1, Period: time.Minute}
	if err := first.Allow(ctx, "route", limit, 1); err != nil {
		t.Fatal(err)
	}
	if err := second.Allow(ctx, "route", limit, 1); err != nil {
		t.Fatalf("second namespace shared rate capacity: %v", err)
	}
	if err := first.Allow(ctx, "route", limit, 1); err == nil {
		t.Fatal("first namespace unexpectedly retained rate capacity")
	}
	if value, err := first.Add(ctx, "counter", 1, time.Minute); err != nil || value != 1 {
		t.Fatalf("first Add() = %d, %v", value, err)
	}
	if value, err := second.Add(ctx, "counter", 10, time.Minute); err != nil || value != 10 {
		t.Fatalf("second Add() = %d, %v", value, err)
	}

	scope := gateway.ScopedLimit{Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "shared"}, Limits: gateway.LimitSpec{DailyTokens: 100}}
	firstTicket, err := first.Reserve(ctx, "same-request", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 3}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	secondTicket, err := second.Reserve(ctx, "same-request", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 5}, time.Minute)
	if err != nil {
		t.Fatalf("second namespace shared reservation ID: %v", err)
	}
	if err := first.Commit(ctx, firstTicket, gateway.ActualUsage{InputTokens: 3}); err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(ctx, secondTicket, gateway.ActualUsage{InputTokens: 5}); err != nil {
		t.Fatal(err)
	}
	firstUsage, err := first.GetUsage(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	secondUsage, err := second.GetUsage(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if firstUsage.DailyUsedTokens != 3 || secondUsage.DailyUsedTokens != 5 {
		t.Fatalf("namespaced usage = %d/%d, want 3/5", firstUsage.DailyUsedTokens, secondUsage.DailyUsedTokens)
	}
	for _, key := range server.Keys() {
		if !strings.HasPrefix(key, "tenant-a:") && !strings.HasPrefix(key, "tenant-b:") &&
			!strings.HasPrefix(key, "rate:tenant-a:") && !strings.HasPrefix(key, "rate:tenant-b:") {
			t.Fatalf("un-namespaced Redis key %q", key)
		}
	}
}

func TestRedisAttemptSlotPrunesExpiredMembers(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Now()
	server.SetTime(now)
	state := NewRedisStore(server.Addr(), "", 0)
	ctx := context.Background()
	if err := state.AcquireSlot(ctx, "route", "first", 1, time.Second); err != nil {
		t.Fatalf("first AcquireSlot() error = %v", err)
	}
	if err := state.AcquireSlot(ctx, "route", "second", 1, time.Second); err == nil {
		t.Fatal("second AcquireSlot() error = nil, want concurrency rejection")
	}
	advanceRedisTime(server, &now, 2*time.Second)
	if err := state.AcquireSlot(ctx, "route", "second", 1, time.Second); err != nil {
		t.Fatalf("AcquireSlot() after lease expiry error = %v", err)
	}
}

func TestRedisBreakerIgnoresSuccessFromAttemptOlderThanLatestFailure(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	server.SetTime(now)
	state := NewRedisStore(server.Addr(), "", 0)
	t.Cleanup(func() { _ = state.Close() })
	ctx := context.Background()

	allowed, slowStartedAt, err := state.BreakerAllowAttempt(ctx, "route", time.Hour)
	if err != nil || !allowed {
		t.Fatalf("first BreakerAllow() = (%t, %v), want allowed", allowed, err)
	}
	if slowStartedAt.UnixMilli() != now.UnixMilli() {
		t.Fatalf("breaker admission time = %v, want Redis time %v", slowStartedAt, now)
	}
	advanceRedisTime(server, &now, time.Second)
	allowed, failedStartedAt, err := state.BreakerAllowAttempt(ctx, "route", time.Hour)
	if err != nil || !allowed {
		t.Fatalf("second BreakerAllow() = (%t, %v), want allowed", allowed, err)
	}
	advanceRedisTime(server, &now, time.Second)
	if err := state.BreakerFailAttempt(ctx, "route", failedStartedAt, 1, time.Minute, "newer attempt failed"); err != nil {
		t.Fatal(err)
	}
	if allowed, _, err := state.BreakerAllowAttempt(ctx, "route", time.Hour); err != nil || allowed {
		t.Fatalf("BreakerAllow() after failure = (%t, %v), want open", allowed, err)
	}

	if err := state.BreakerSuccessAttempt(ctx, "route", slowStartedAt); err != nil {
		t.Fatal(err)
	}
	if allowed, _, err := state.BreakerAllowAttempt(ctx, "route", time.Hour); err != nil || allowed {
		t.Fatalf("BreakerAllow() after stale success = (%t, %v), want still open", allowed, err)
	}
}

func TestRedisBreakerAcceptsSuccessFromNewerAdmissionAfterOlderFailure(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	server.SetTime(now)
	state := NewRedisStore(server.Addr(), "", 0)
	t.Cleanup(func() { _ = state.Close() })
	ctx := context.Background()

	_, olderStartedAt, err := state.BreakerAllowAttempt(ctx, "route", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, time.Second)
	_, newerStartedAt, err := state.BreakerAllowAttempt(ctx, "route", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, time.Second)
	if err := state.BreakerFailAttempt(ctx, "route", olderStartedAt, 1, time.Minute, "older failure"); err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, time.Second)
	if err := state.BreakerSuccessAttempt(ctx, "route", newerStartedAt); err != nil {
		t.Fatal(err)
	}
	if allowed, _, err := state.BreakerAllowAttempt(ctx, "route", time.Hour); err != nil || !allowed {
		t.Fatalf("BreakerAllow() after newer success = (%t, %v), want closed", allowed, err)
	}
	fields, err := state.client.HGetAll(ctx, state.breakerKey("route")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if fields["last_failure_us"] != fmt.Sprint(olderStartedAt.UnixMicro()) ||
		fields["last_success_us"] != fmt.Sprint(newerStartedAt.UnixMicro()) {
		t.Fatalf("breaker ordering fields = %#v, want admission timestamps", fields)
	}
}

func TestRedisBreakerAdmissionTimestampsAreMonotonic(t *testing.T) {
	server := miniredis.RunT(t)
	server.SetTime(time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC))
	state := NewRedisStore(server.Addr(), "", 0)
	t.Cleanup(func() { _ = state.Close() })
	ctx := context.Background()

	_, first, err := state.BreakerAllowAttempt(ctx, "route", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := state.BreakerAllowAttempt(ctx, "route", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, third, err := state.BreakerAllowAttempt(ctx, "route", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !second.After(first) || !third.After(second) {
		t.Fatalf("admission timestamps = (%v, %v, %v), want strictly increasing", first, second, third)
	}
}

func TestRedisBreakerRetainsEvidenceForLongestAdmittedAttempt(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	server.SetTime(now)
	state := NewRedisStore(server.Addr(), "", 0)
	t.Cleanup(func() { _ = state.Close() })
	ctx := context.Background()

	_, olderStartedAt, err := state.BreakerAllowAttempt(ctx, "route", 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, time.Minute)
	_, newerStartedAt, err := state.BreakerAllowAttempt(ctx, "route", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.BreakerSuccessAttempt(ctx, "route", newerStartedAt); err != nil {
		t.Fatal(err)
	}
	if ttl := server.TTL(state.breakerKey("route")); ttl < 90*time.Minute {
		t.Fatalf("breaker evidence TTL = %v, want retention from longest admitted attempt", ttl)
	}

	// Move beyond the default one-hour breaker TTL while the older admitted
	// call can still be running. Its late failure must remain stale relative to
	// the newer success.
	advanceRedisTime(server, &now, 61*time.Minute)
	if !server.Exists(state.breakerKey("route")) {
		t.Fatal("breaker evidence expired while an older admitted call could still finish")
	}
	if err := state.BreakerFailAttempt(ctx, "route", olderStartedAt, 1, time.Hour, "older failure"); err != nil {
		t.Fatal(err)
	}
	if allowed, _, err := state.BreakerAllowAttempt(ctx, "route", time.Hour); err != nil || !allowed {
		t.Fatalf("BreakerAllowAttempt() after stale failure = (%t, %v), want closed circuit", allowed, err)
	}
}

func TestRedisBreakerDoesNotPersistFailureMessage(t *testing.T) {
	server := miniredis.RunT(t)
	state := NewRedisStore(server.Addr(), "", 0)
	t.Cleanup(func() { _ = state.Close() })
	ctx := context.Background()

	_, startedAt, err := state.BreakerAllowAttempt(ctx, "route", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	const sensitive = "provider echoed secret prompt: customer-token-123"
	if err := state.client.HSet(ctx, state.breakerKey("route"), "last_error", sensitive).Err(); err != nil {
		t.Fatal(err)
	}
	if err := state.BreakerFailAttempt(ctx, "route", startedAt, 1, time.Minute, sensitive); err != nil {
		t.Fatal(err)
	}
	fields, err := state.client.HGetAll(ctx, state.breakerKey("route")).Result()
	if err != nil {
		t.Fatal(err)
	}
	for field, value := range fields {
		if strings.Contains(field, sensitive) || strings.Contains(value, sensitive) {
			t.Fatalf("breaker state persisted sensitive failure text in %q=%q", field, value)
		}
	}

	_, successStartedAt, err := state.BreakerAllowAttempt(ctx, "successful-route", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.client.HSet(ctx, state.breakerKey("successful-route"), "last_error", sensitive).Err(); err != nil {
		t.Fatal(err)
	}
	if err := state.BreakerSuccessAttempt(ctx, "successful-route", successStartedAt); err != nil {
		t.Fatal(err)
	}
	if exists, err := state.client.HExists(ctx, state.breakerKey("successful-route"), "last_error").Result(); err != nil || exists {
		t.Fatalf("legacy last_error after success = (exists %t, error %v), want removed", exists, err)
	}
}

func TestRedisBreakerLateFailureDoesNotExtendOpenCooldown(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	server.SetTime(now)
	state := NewRedisStore(server.Addr(), "", 0)
	t.Cleanup(func() { _ = state.Close() })
	ctx := context.Background()

	_, slowStartedAt, err := state.BreakerAllowAttempt(ctx, "route", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, time.Second)
	_, failedStartedAt, err := state.BreakerAllowAttempt(ctx, "route", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, time.Second)
	if err := state.BreakerFailAttempt(ctx, "route", failedStartedAt, 1, 10*time.Second, "opened"); err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, 3*time.Second)
	if err := state.BreakerFailAttempt(ctx, "route", slowStartedAt, 1, 10*time.Second, "late in-flight failure"); err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, 8*time.Second)
	if allowed, _, err := state.BreakerAllowAttempt(ctx, "route", time.Hour); err != nil || !allowed {
		t.Fatalf("BreakerAllow() after original cooldown = (%t, %v), want allowed without extension", allowed, err)
	}
}

func TestRedisRateStoreCanceledContextDoesNotConsumeCapacity(t *testing.T) {
	server := miniredis.RunT(t)
	rates := NewRedisStore(server.Addr(), "", 0)
	limit := RateLimit{Rate: 1, Burst: 1, Period: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rates.Allow(ctx, "canceled-rate", limit, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Allow() error = %v, want context.Canceled", err)
	}
	if err := rates.Allow(context.Background(), "canceled-rate", limit, 1); err != nil {
		t.Fatalf("valid Allow() after cancellation consumed capacity: %v", err)
	}
}

func TestRedisRateStoreBatchRejectsAtomically(t *testing.T) {
	server := miniredis.RunT(t)
	rates := NewRedisStore(server.Addr(), "", 0)
	rpm := RateLimit{Rate: 1, Burst: 1, Period: time.Minute}
	tpm := RateLimit{Rate: 5, Burst: 5, Period: time.Minute}
	requests := []RateRequest{
		{Key: "rpm:route", Limit: rpm, N: 1},
		{Key: "tpm:route", Limit: tpm, N: 10},
	}
	if err := rates.AllowBatch(context.Background(), requests); err == nil {
		t.Fatal("oversized TPM batch was admitted")
	}
	requests[1].N = 1
	if err := rates.AllowBatch(context.Background(), requests); err != nil {
		t.Fatalf("rejected batch consumed RPM capacity: %v", err)
	}
}

func TestRedisRateStoreAggregatesDuplicateBatchKeys(t *testing.T) {
	server := miniredis.RunT(t)
	rates := NewRedisStore(server.Addr(), "", 0)
	defer rates.Close()
	limit := RateLimit{Rate: 2, Burst: 2, Period: time.Minute}
	if err := rates.AllowBatch(context.Background(), []RateRequest{
		{Key: "shared", Limit: limit, N: 1},
		{Key: "shared", Limit: limit, N: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rates.Allow(context.Background(), "shared", limit, 1); err == nil {
		t.Fatal("duplicate batch undercharged shared Redis bucket")
	}
	if err := rates.AllowBatch(context.Background(), []RateRequest{
		{Key: "mismatch", Limit: limit, N: 1},
		{Key: "mismatch", Limit: RateLimit{Rate: 3, Burst: 3, Period: time.Minute}, N: 1},
	}); err == nil {
		t.Fatal("inconsistent duplicate limits were accepted")
	}
}

func TestRedisQuotaReservePrunesOneExpiredLeaseWithoutDroppingLiveLease(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Now()
	server.SetTime(now)
	quota := NewRedisStore(server.Addr(), "", 0)
	ctx := context.Background()
	scope := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-expiry"},
		Limits: gateway.LimitSpec{MaxParallel: 2, DailyTokens: 100},
	}
	usage := gateway.EstimatedUsage{InputTokens: 4, ReservedOutputTokens: 1}
	if _, err := quota.Reserve(ctx, "first", []gateway.ScopedLimit{scope}, usage, time.Second); err != nil {
		t.Fatalf("first Reserve() error = %v", err)
	}
	advanceRedisTime(server, &now, 500*time.Millisecond)
	second, err := quota.Reserve(ctx, "second", []gateway.ScopedLimit{scope}, usage, 5*time.Second)
	if err != nil {
		t.Fatalf("second Reserve() error = %v", err)
	}
	advanceRedisTime(server, &now, 600*time.Millisecond)
	third, err := quota.Reserve(ctx, "third", []gateway.ScopedLimit{scope}, usage, 5*time.Second)
	if err != nil {
		t.Fatalf("third Reserve() after first lease expiry error = %v", err)
	}
	current, err := quota.GetUsage(ctx, scope)
	if err != nil {
		t.Fatalf("GetUsage() error = %v", err)
	}
	if current.ActiveRequests != 2 || current.DailyHeldTokens != 10 {
		t.Fatalf("usage after pruning = %#v, want two live reservations", current)
	}
	if err := quota.Refund(ctx, second); err != nil {
		t.Fatalf("Refund(second) error = %v", err)
	}
	if err := quota.Refund(ctx, third); err != nil {
		t.Fatalf("Refund(third) error = %v", err)
	}
}

func TestRedisQuotaSettlementRejectsPrunedLeaseWithoutTouchingLiveReservation(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Now()
	server.SetTime(now)
	quota := NewRedisStore(server.Addr(), "", 0)
	t.Cleanup(func() { _ = quota.Close() })
	ctx := context.Background()
	scope := gateway.ScopedLimit{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "settle-pruned-lease"},
		Limits: gateway.LimitSpec{
			MaxParallel: 2,
			TPM:         100,
			DailyTokens: 100,
		},
	}
	estimate := gateway.EstimatedUsage{InputTokens: 3, ReservedOutputTokens: 2}
	expired, err := quota.Reserve(ctx, "expired-settlement", []gateway.ScopedLimit{scope}, estimate, time.Second)
	if err != nil {
		t.Fatalf("Reserve(expired) error = %v", err)
	}
	// Reserve reads Redis time before running the Lua script, so the lease score
	// can expire just before the ticket's relative TTL. Extend that deterministic
	// gap here instead of relying on network timing.
	server.SetTTL(quota.ticketKey(expired.RequestID), 5*time.Second)

	advanceRedisTime(server, &now, 500*time.Millisecond)
	live, err := quota.Reserve(ctx, "live-settlement", []gateway.ScopedLimit{scope}, estimate, 5*time.Second)
	if err != nil {
		t.Fatalf("Reserve(live) error = %v", err)
	}
	advanceRedisTime(server, &now, 600*time.Millisecond)

	before, err := quota.GetUsage(ctx, scope)
	if err != nil {
		t.Fatalf("GetUsage(before settlement) error = %v", err)
	}
	if before.ActiveRequests != 1 || before.TPMCurrent != estimate.TotalTokens() || before.DailyHeldTokens != estimate.TotalTokens() {
		t.Fatalf("usage after expired lease prune = %#v, want only live reservation", before)
	}
	if !server.Exists(quota.ticketKey(expired.RequestID)) {
		t.Fatal("expired reservation ticket did not survive the simulated ticket/lease gap")
	}

	if err := quota.Refund(ctx, expired); !errors.Is(err, ErrQuotaReservationNotFound) {
		t.Fatalf("Refund(pruned lease) error = %v, want ErrQuotaReservationNotFound", err)
	}
	after, err := quota.GetUsage(ctx, scope)
	if err != nil {
		t.Fatalf("GetUsage(after settlement) error = %v", err)
	}
	if after != before {
		t.Fatalf("usage after rejected stale settlement = %#v, want unchanged %#v", after, before)
	}
	if err := quota.Refund(ctx, live); err != nil {
		t.Fatalf("Refund(live) error = %v", err)
	}
}

func TestRedisQuotaBucketsUseRedisClock(t *testing.T) {
	server := miniredis.RunT(t)
	redisTime := time.Date(2040, time.February, 3, 4, 5, 6, 0, time.UTC)
	server.SetTime(redisTime)
	quota := NewRedisStoreWithNamespace(server.Addr(), "", 0, "tenant-a")
	defer quota.Close()
	ctx := context.Background()
	scope := gateway.ScopedLimit{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "clocked"},
		Limits: gateway.LimitSpec{
			RPM: 10, TPM: 100, MaxSpendMicros: 1000, DailyTokens: 100, MonthlyTokens: 100,
			BudgetDuration: gateway.Duration{Duration: 24 * time.Hour},
		},
	}
	ticket, err := quota.Reserve(ctx, "clocked-request", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 2, EstimatedSpendMicros: 3}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := quota.Commit(ctx, ticket, gateway.ActualUsage{InputTokens: 2, SpendMicros: 3}); err != nil {
		t.Fatal(err)
	}
	usage, err := quota.GetUsage(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if usage.RPMCurrent != 1 || usage.TPMCurrent != 2 || usage.DailyUsedTokens != 2 || usage.MonthUsedTokens != 2 || usage.SpendUsedMicros != 3 {
		t.Fatalf("usage selected with Redis clock = %#v", usage)
	}
	joined := strings.Join(server.Keys(), "\n")
	for _, want := range []string{"204002030405", "20400203", "204002"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("Redis-clock bucket %q missing from keys:\n%s", want, joined)
		}
	}
}

func TestRedisGetUsageRejectsCorruptCounter(t *testing.T) {
	server := miniredis.RunT(t)
	quota := NewRedisStore(server.Addr(), "", 0)
	t.Cleanup(func() { _ = quota.Close() })
	ctx := context.Background()
	scope := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "corrupt"},
		Limits: gateway.LimitSpec{TPM: 100},
	}
	now, err := quota.redisNow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys := quotaKeysForScope(scope.Ref, scope.Limits, now)
	if err := quota.client.Set(ctx, quota.key(keys.tpm), "not-an-integer", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := quota.GetUsage(ctx, scope); err == nil || !strings.Contains(err.Error(), "tpm_current") {
		t.Fatalf("GetUsage() error = %v, want corrupt TPM counter error", err)
	}
}

func TestSpendBudgetKeysUseUnixAlignedPreciseBuckets(t *testing.T) {
	ref := gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "budgeted"}
	limit := gateway.LimitSpec{
		MaxSpendMicros: 100,
		BudgetDuration: gateway.Duration{Duration: time.Hour},
	}
	beforeBoundary := time.Unix(0, int64(time.Hour)-1).UTC()
	afterBoundary := time.Unix(0, int64(time.Hour)).UTC()
	before := quotaKeysForScope(ref, limit, beforeBoundary)
	after := quotaKeysForScope(ref, limit, afterBoundary)
	if before.spendHeld == after.spendHeld {
		t.Fatalf("spend hold key crossed budget boundary: %q", before.spendHeld)
	}
	if !strings.HasSuffix(before.spendUsed, budgetBucket(time.Hour, beforeBoundary)) ||
		!strings.HasSuffix(before.spendHeld, budgetBucket(time.Hour, beforeBoundary)) {
		t.Fatalf("used/held keys are not in the same budget bucket: %#v", before)
	}

	period := 720 * time.Hour
	now := time.Unix(0, int64(period)+123).UTC()
	wantStart := time.Unix(0, int64(period)).UTC()
	if got := budgetBucketStart(period, now); !got.Equal(wantStart) {
		t.Fatalf("720h bucket start = %v, want Unix-aligned %v", got, wantStart)
	}
	if first, second := budgetBucket(100*time.Millisecond, time.Unix(0, 99_999_999)), budgetBucket(100*time.Millisecond, time.Unix(0, 100_000_000)); first == second {
		t.Fatalf("sub-second budget buckets collapsed to %q", first)
	}

	maxDurationLimit := gateway.LimitSpec{
		MaxSpendMicros: 1,
		BudgetDuration: gateway.Duration{Duration: time.Duration(math.MaxInt64)},
	}
	expires := quotaExpiriesForLimit(maxDurationLimit, time.Now())
	if expires.spendUsed.IsZero() {
		t.Fatal("maximum duration produced no spend expiry")
	}
}

func TestRedisQuotaStoreSeparatesSpendBudgetsByDuration(t *testing.T) {
	server := miniredis.RunT(t)
	server.SetTime(time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC))
	quota := NewRedisStore(server.Addr(), "", 0)
	t.Cleanup(func() { _ = quota.Close() })
	ctx := context.Background()
	ref := gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "budget-duration"}
	firstScope := gateway.ScopedLimit{
		Ref: ref,
		Limits: gateway.LimitSpec{
			MaxSpendMicros: 1_000,
			BudgetDuration: gateway.Duration{Duration: time.Duration(math.MaxInt64)},
		},
	}
	secondScope := gateway.ScopedLimit{
		Ref: ref,
		Limits: gateway.LimitSpec{
			MaxSpendMicros: 100,
			BudgetDuration: gateway.Duration{Duration: time.Duration(math.MaxInt64) - time.Hour},
		},
	}

	first, err := quota.Reserve(ctx, "first-budget", []gateway.ScopedLimit{firstScope}, gateway.EstimatedUsage{
		EstimatedSpendMicros: 80,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := quota.Commit(ctx, first, gateway.ActualUsage{SpendMicros: 80}); err != nil {
		t.Fatal(err)
	}

	second, err := quota.Reserve(ctx, "second-budget", []gateway.ScopedLimit{secondScope}, gateway.EstimatedUsage{
		EstimatedSpendMicros: 30,
	}, time.Minute)
	if err != nil {
		t.Fatalf("Reserve() after budget-duration change error = %v", err)
	}
	if err := quota.Commit(ctx, second, gateway.ActualUsage{SpendMicros: 30}); err != nil {
		t.Fatal(err)
	}

	firstUsage, err := quota.GetUsage(ctx, firstScope)
	if err != nil {
		t.Fatal(err)
	}
	secondUsage, err := quota.GetUsage(ctx, secondScope)
	if err != nil {
		t.Fatal(err)
	}
	if firstUsage.SpendUsedMicros != 80 || secondUsage.SpendUsedMicros != 30 {
		t.Fatalf("spend usage by duration = %d/%d, want 80/30", firstUsage.SpendUsedMicros, secondUsage.SpendUsedMicros)
	}
}

func advanceRedisTime(server *miniredis.Miniredis, now *time.Time, delta time.Duration) {
	server.FastForward(delta)
	*now = now.Add(delta)
	server.SetTime(*now)
}

func TestRedisQuotaSettlementAtomicallyConsumesTicket(t *testing.T) {
	server := miniredis.RunT(t)
	quota := NewRedisStore(server.Addr(), "", 0)
	ctx := context.Background()
	scope := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-idempotent"},
		Limits: gateway.LimitSpec{DailyTokens: 1000, MaxSpendMicros: 1000},
	}
	ticket, err := quota.Reserve(ctx, "request", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens:          5,
		ReservedOutputTokens: 5,
	}, time.Minute)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if err := quota.TopUp(ctx, ticket, []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 2}, time.Minute); err != nil {
		t.Fatalf("TopUp() error = %v", err)
	}
	actual := gateway.ActualUsage{InputTokens: 7, OutputTokens: 5, SpendMicros: 12}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- quota.Commit(ctx, ticket, actual)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Commit() error = %v", err)
		}
	}
	current, err := quota.GetUsage(ctx, scope)
	if err != nil {
		t.Fatalf("GetUsage() error = %v", err)
	}
	if current.ActiveRequests != 0 || current.DailyHeldTokens != 0 || current.DailyUsedTokens != 12 || current.SpendUsedMicros != 12 {
		t.Fatalf("usage after concurrent commits = %#v", current)
	}
}

func TestRedisQuotaAccountingBoundaryAndTicketRetention(t *testing.T) {
	server := miniredis.RunT(t)
	quota := NewRedisStore(server.Addr(), "", 0)
	t.Cleanup(func() { _ = quota.Close() })
	ctx := context.Background()
	scope := gateway.ScopedLimit{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "numeric-boundary"},
		Limits: gateway.LimitSpec{
			MaxSpendMicros: gateway.MaximumQuotaValue,
			DailyTokens:    gateway.MaximumQuotaValue,
			BudgetDuration: gateway.Duration{Duration: time.Hour},
		},
	}

	ticket, err := quota.Reserve(ctx, "exact-boundary", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens:          gateway.MaximumQuotaValue - 1,
		EstimatedSpendMicros: gateway.MaximumQuotaValue - 1,
	}, time.Minute)
	if err != nil {
		t.Fatalf("Reserve(max-1) error = %v", err)
	}
	if err := quota.TopUp(ctx, ticket, nil, gateway.EstimatedUsage{
		InputTokens:          1,
		EstimatedSpendMicros: 1,
	}, time.Minute); err != nil {
		t.Fatalf("TopUp(to max) error = %v", err)
	}
	usage, err := quota.GetUsage(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if usage.DailyHeldTokens != gateway.MaximumQuotaValue || usage.SpendHeldMicros != gateway.MaximumQuotaValue {
		t.Fatalf("usage at exact boundary = %#v", usage)
	}
	if err := quota.Refund(ctx, ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := quota.Reserve(ctx, "too-large", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens: gateway.MaximumQuotaValue, ReservedOutputTokens: 1,
	}, time.Minute); !errors.Is(err, ErrQuotaAccountingCapacity) {
		t.Fatalf("Reserve(over max) error = %v, want ErrQuotaAccountingCapacity", err)
	}

	overflowTicket, err := quota.Reserve(ctx, "top-up-overflow", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens: 1, EstimatedSpendMicros: 1,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now, err := quota.redisNow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys := quotaKeysForScope(scope.Ref, scope.Limits, now)
	if err := quota.client.Set(ctx, quota.key(keys.spendHeld), fmt.Sprint(math.MaxInt64), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := quota.TopUp(ctx, overflowTicket, nil, gateway.EstimatedUsage{EstimatedSpendMicros: 1}, time.Minute); !errors.Is(err, ErrQuotaAccountingCapacity) {
		t.Fatalf("TopUp(over capacity) error = %v, want ErrQuotaAccountingCapacity", err)
	}
	if exists, err := quota.client.Exists(ctx, quota.ticketKey(overflowTicket.RequestID)).Result(); err != nil || exists != 1 {
		t.Fatalf("ticket after failed TopUp exists = %d, %v", exists, err)
	}
	if err := quota.client.Set(ctx, quota.key(keys.spendHeld), 1, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := quota.Refund(ctx, overflowTicket); err != nil {
		t.Fatal(err)
	}

	settleTicket, err := quota.Reserve(ctx, "settle-overflow", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens: 1, EstimatedSpendMicros: 1,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := quota.client.Set(ctx, quota.key(keys.spendUsed), fmt.Sprint(math.MaxInt64), time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if err := quota.Commit(ctx, settleTicket, gateway.ActualUsage{InputTokens: 1, SpendMicros: 1}); !errors.Is(err, ErrQuotaAccountingCapacity) {
		t.Fatalf("Commit(over capacity) error = %v, want ErrQuotaAccountingCapacity", err)
	}
	if exists, err := quota.client.Exists(ctx, quota.ticketKey(settleTicket.RequestID)).Result(); err != nil || exists != 1 {
		t.Fatalf("ticket after failed Commit exists = %d, %v", exists, err)
	}
	if err := quota.client.Set(ctx, quota.key(keys.spendUsed), 0, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if err := quota.Commit(ctx, settleTicket, gateway.ActualUsage{InputTokens: 1, SpendMicros: 1}); err != nil {
		t.Fatalf("Commit(after repair) error = %v", err)
	}
	if exists, err := quota.client.Exists(ctx, quota.ticketKey(settleTicket.RequestID)).Result(); err != nil || exists != 0 {
		t.Fatalf("ticket after successful Commit exists = %d, %v", exists, err)
	}
}

func TestRedisQuotaTPMReconcilesWorstCaseReservation(t *testing.T) {
	server := miniredis.RunT(t)
	quota := NewRedisStore(server.Addr(), "", 0)
	ctx := context.Background()
	scope := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "redis-tpm-reconcile"},
		Limits: gateway.LimitSpec{TPM: 250},
	}
	first, err := quota.Reserve(ctx, "first-tpm", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 200}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := quota.Commit(ctx, first, gateway.ActualUsage{InputTokens: 100}); err != nil {
		t.Fatal(err)
	}
	usage, err := quota.GetUsage(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if usage.TPMCurrent != 100 {
		t.Fatalf("TPM after settlement = %d, want actual 100", usage.TPMCurrent)
	}
	second, err := quota.Reserve(ctx, "second-tpm", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 100}, time.Minute)
	if err != nil {
		t.Fatalf("second reservation was denied by unused fallback estimate: %v", err)
	}
	if err := quota.Refund(ctx, second); err != nil {
		t.Fatal(err)
	}
	usage, err = quota.GetUsage(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if usage.TPMCurrent != 100 {
		t.Fatalf("TPM after refund = %d, want only committed actual 100", usage.TPMCurrent)
	}
}

func TestRedisQuotaExpiredLeaseReleasesTPMHold(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Now()
	server.SetTime(now)
	quota := NewRedisStore(server.Addr(), "", 0)
	ctx := context.Background()
	scope := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "redis-tpm-expiry"},
		Limits: gateway.LimitSpec{TPM: 10},
	}
	if _, err := quota.Reserve(ctx, "expired-tpm", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 10}, time.Second); err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, 2*time.Second)
	second, err := quota.Reserve(ctx, "after-expiry-tpm", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 10}, time.Second)
	if err != nil {
		t.Fatalf("reservation after lease expiry retained stale TPM: %v", err)
	}
	if err := quota.Refund(ctx, second); err != nil {
		t.Fatal(err)
	}
}

func TestRedisQuotaStoreRejectsReservationIDConflict(t *testing.T) {
	server := miniredis.RunT(t)
	quota := NewRedisStore(server.Addr(), "", 0)
	ctx := context.Background()
	scope := gateway.ScopedLimit{Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-a"}}
	estimate := gateway.EstimatedUsage{InputTokens: 2, ReservedOutputTokens: 3}
	if _, err := quota.Reserve(ctx, "shared-id", []gateway.ScopedLimit{scope}, estimate, time.Minute); err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if _, err := quota.Reserve(ctx, "shared-id", []gateway.ScopedLimit{scope}, estimate, time.Minute); err != nil {
		t.Fatalf("idempotent Reserve() error = %v", err)
	}
	if _, err := quota.Reserve(ctx, "shared-id", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 99}, time.Minute); !errors.Is(err, ErrQuotaReservationConflict) {
		t.Fatalf("conflicting Reserve() error = %v, want ErrQuotaReservationConflict", err)
	}
}

func TestRedisQuotaRenewalAndMissingSettlement(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Now()
	server.SetTime(now)
	quota := NewRedisStore(server.Addr(), "", 0)
	ctx := context.Background()
	scope := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-renew"},
		Limits: gateway.LimitSpec{MaxParallel: 1, DailyTokens: 10},
	}
	ticket, err := quota.Reserve(ctx, "renew", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 1}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, 750*time.Millisecond)
	if err := quota.TopUp(ctx, ticket, []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{}, time.Second); err != nil {
		t.Fatal(err)
	}
	advanceRedisTime(server, &now, 500*time.Millisecond)
	usage, err := quota.GetUsage(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if usage.ActiveRequests != 1 || usage.DailyHeldTokens != 1 {
		t.Fatalf("usage after renewal = %#v, want live hold", usage)
	}
	advanceRedisTime(server, &now, 600*time.Millisecond)
	if err := quota.TopUp(ctx, ticket, []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{}, time.Second); !errors.Is(err, ErrQuotaReservationNotFound) {
		t.Fatalf("TopUp(expired) error = %v, want ErrQuotaReservationNotFound", err)
	}
	if err := quota.Commit(ctx, ticket, gateway.ActualUsage{InputTokens: 1}); !errors.Is(err, ErrQuotaReservationNotFound) {
		t.Fatalf("Commit(expired) error = %v, want ErrQuotaReservationNotFound", err)
	}
}

func TestMemorySettlementReplayWindowIsBounded(t *testing.T) {
	quota := NewMemoryQuotaStore()
	ctx := context.Background()
	for i := 0; i < maxMemorySettlements+1024; i++ {
		requestID := fmt.Sprintf("request-%d", i)
		ticket, err := quota.Reserve(ctx, requestID, nil, gateway.EstimatedUsage{}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := quota.Commit(ctx, ticket, gateway.ActualUsage{}); err != nil {
			t.Fatal(err)
		}
	}
	if len(quota.settlements) > maxMemorySettlements || quota.settledOrder.Len() > maxMemorySettlements {
		t.Fatalf("settlement replay state = map %d/list %d, want <= %d", len(quota.settlements), quota.settledOrder.Len(), maxMemorySettlements)
	}
}

func TestRedisDisabledQuotaDimensionsDoNotUseEmptyOrPersistentKeys(t *testing.T) {
	server := miniredis.RunT(t)
	quota := NewRedisStore(server.Addr(), "", 0)
	ctx := context.Background()
	server.Set("", "tenant-data")
	zeroScope := gateway.ScopedLimit{Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "zero"}}
	usage, err := quota.GetUsage(ctx, zeroScope)
	if err != nil {
		t.Fatal(err)
	}
	if usage != (gateway.QuotaUsage{}) {
		t.Fatalf("zero-limit usage = %#v, want zero", usage)
	}
	if value, err := server.Get(""); err != nil || value != "tenant-data" {
		t.Fatalf("unrelated empty-key value = %q, %v; GetUsage must not mutate it", value, err)
	}

	daily := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "daily-only"},
		Limits: gateway.LimitSpec{DailyTokens: 100},
	}
	ticket, err := quota.Reserve(ctx, "daily", []gateway.ScopedLimit{daily}, gateway.EstimatedUsage{InputTokens: 2}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := quota.Commit(ctx, ticket, gateway.ActualUsage{InputTokens: 2, SpendMicros: 9}); err != nil {
		t.Fatal(err)
	}
	for _, key := range server.Keys() {
		if strings.Contains(key, "used:spend") || strings.Contains(key, "used:month") {
			t.Fatalf("disabled quota dimension created persistent key %q", key)
		}
	}
	dayKey := quotaKeysForScope(daily.Ref, daily.Limits, time.Now()).dayUsed
	if ttl := server.TTL(dayKey); ttl <= 0 {
		t.Fatalf("daily used key TTL = %v, want bounded expiry", ttl)
	}
}

func TestRedisCounterAndLifecycle(t *testing.T) {
	server := miniredis.RunT(t)
	state := NewRedisStore(server.Addr(), "", 0)
	ctx := context.Background()
	if err := state.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if value, err := state.Add(ctx, "counter", -10, time.Minute); err != nil || value != 0 {
		t.Fatalf("Add(-10) = (%d, %v), want (0, nil)", value, err)
	}
	if err := state.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := state.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := state.Ping(ctx); err == nil {
		t.Fatal("Ping() after Close() error = nil")
	}
}
