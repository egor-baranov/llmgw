package store_test

import (
	"context"
	"errors"
	"fmt"
	"math"
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

func TestMemoryQuotaEmptyReservationIDDoesNotHoldCapacity(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	scope := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-empty-id"},
		Limits: gateway.LimitSpec{MaxParallel: 1},
	}
	ticket, err := quota.Reserve(context.Background(), "", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ticket.RequestID != "" {
		t.Fatalf("empty reservation ticket = %#v, want no-op ticket", ticket)
	}
	normal, err := quota.Reserve(context.Background(), "normal", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{}, time.Minute)
	if err != nil {
		t.Fatalf("normal reservation was blocked by empty ID hold: %v", err)
	}
	if err := quota.Refund(context.Background(), normal); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryQuotaTPMReconcilesWorstCaseReservation(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	ctx := context.Background()
	scope := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-tpm-reconcile"},
		Limits: gateway.LimitSpec{TPM: 250},
	}
	first, err := quota.Reserve(ctx, "first", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 200}, time.Minute)
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
	second, err := quota.Reserve(ctx, "second", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 100}, time.Minute)
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

func TestMemoryStoresHonorCanceledContextsWithoutMutatingState(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	counter := store.NewMemoryCounterStore()
	if _, err := counter.Add(canceled, "counter", 1, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled counter Add() error = %v", err)
	}
	if value, err := counter.Add(context.Background(), "counter", 1, time.Minute); err != nil || value != 1 {
		t.Fatalf("counter after canceled Add() = (%d, %v), want (1, nil)", value, err)
	}

	quota := store.NewMemoryQuotaStore()
	scope := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "canceled"},
		Limits: gateway.LimitSpec{MaxParallel: 1, DailyTokens: 100},
	}
	if _, err := quota.Reserve(canceled, "canceled", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 1}, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Reserve() error = %v", err)
	}
	ticket, err := quota.Reserve(context.Background(), "live", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 1}, time.Minute)
	if err != nil {
		t.Fatalf("Reserve() after cancellation error = %v", err)
	}
	if err := quota.TopUp(canceled, ticket, nil, gateway.EstimatedUsage{InputTokens: 10}, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled TopUp() error = %v", err)
	}
	if err := quota.Commit(canceled, ticket, gateway.ActualUsage{InputTokens: 10}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Commit() error = %v", err)
	}
	if err := quota.Refund(canceled, ticket); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Refund() error = %v", err)
	}
	if _, err := quota.GetUsage(canceled, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled GetUsage() error = %v", err)
	}
	usage, err := quota.GetUsage(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if usage.ActiveRequests != 1 || usage.DailyHeldTokens != 1 {
		t.Fatalf("usage after canceled mutations = %#v, want original hold", usage)
	}
	if err := quota.Refund(context.Background(), ticket); err != nil {
		t.Fatal(err)
	}

	limits := store.NewMemoryQuotaLimitStore()
	if err := limits.Put(canceled, "key", gateway.LimitSpec{RPM: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled limit Put() error = %v", err)
	}
	if _, _, err := limits.Get(canceled, "key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled limit Get() error = %v", err)
	}
	if _, found, err := limits.Get(context.Background(), "key"); err != nil || found {
		t.Fatalf("limit after canceled Put() = (found %t, %v), want false, nil", found, err)
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

func TestMemoryRateStoreCanceledContextDoesNotConsumeCapacity(t *testing.T) {
	rates := store.NewMemoryRateStore()
	limit := store.RateLimit{Rate: 1, Burst: 1, Period: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rates.Allow(ctx, "canceled", limit, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Allow() error = %v, want context.Canceled", err)
	}
	if err := rates.Allow(context.Background(), "canceled", limit, 1); err != nil {
		t.Fatalf("valid Allow() after cancellation consumed capacity: %v", err)
	}
}

func TestMemoryRateStoreBatchRejectsAtomically(t *testing.T) {
	rates := store.NewMemoryRateStore()
	rpm := store.RateLimit{Rate: 1, Burst: 1, Period: time.Minute}
	tpm := store.RateLimit{Rate: 5, Burst: 5, Period: time.Minute}
	requests := []store.RateRequest{
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

func TestMemoryRateStoreAggregatesDuplicateBatchKeys(t *testing.T) {
	rates := store.NewMemoryRateStore()
	limit := store.RateLimit{Rate: 2, Burst: 2, Period: time.Minute}
	if err := rates.AllowBatch(context.Background(), []store.RateRequest{
		{Key: "shared", Limit: limit, N: 1},
		{Key: "shared", Limit: limit, N: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rates.Allow(context.Background(), "shared", limit, 1); err == nil {
		t.Fatal("duplicate batch undercharged shared bucket")
	}
}

func TestMemoryQuotaLimitStorePutGet(t *testing.T) {
	limits := store.NewMemoryQuotaLimitStore()
	want := gateway.LimitSpec{
		RPM:               12,
		TPM:               345,
		MaxParallel:       2,
		DailyTokens:       1000,
		MaxInputTokens:    100,
		MaxOutputTokens:   50,
		ModelAllowlist:    []string{"model-a"},
		ProviderAllowlist: []string{"openai"},
	}
	if err := limits.Put(context.Background(), "key-1", want); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	want.ModelAllowlist[0] = "mutated-after-put"
	want.ProviderAllowlist[0] = "mutated-after-put"
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
	if got.ModelAllowlist[0] != "model-a" || got.ProviderAllowlist[0] != "openai" {
		t.Fatalf("stored allowlists were aliased: %#v", got)
	}
	got.ModelAllowlist[0] = "mutated-after-get"
	again, _, err := limits.Get(context.Background(), "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if again.ModelAllowlist[0] != "model-a" {
		t.Fatalf("returned allowlist was aliased: %#v", again.ModelAllowlist)
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

func TestCachedQuotaLimitStoreBoundsHighCardinalityEntries(t *testing.T) {
	source := &stubQuotaLimitStore{
		limits: map[string]gateway.LimitSpec{"oldest": {RPM: 10}},
	}
	cached := store.NewCachedQuotaLimitStoreWithCapacity(source, time.Minute, 8)
	if _, _, err := cached.Get(context.Background(), "oldest"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if _, found, err := cached.Get(context.Background(), fmt.Sprintf("absent-%d", i)); err != nil || found {
			t.Fatalf("absent lookup %d = (found %t, err %v), want false nil", i, found, err)
		}
	}
	before := source.getCount()
	if _, found, err := cached.Get(context.Background(), "oldest"); err != nil || !found {
		t.Fatalf("oldest relookup = (found %t, err %v), want true nil", found, err)
	}
	if got := source.getCount(); got != before+1 {
		t.Fatalf("source gets after oldest relookup = %d, want %d; oldest entry should have been evicted", got, before+1)
	}
}

func TestCachedQuotaLimitStorePrunesExpiredNegativeEntry(t *testing.T) {
	source := &stubQuotaLimitStore{}
	cached := store.NewCachedQuotaLimitStoreWithCapacity(source, 10*time.Millisecond, 8)
	if _, found, err := cached.Get(context.Background(), "new-key"); err != nil || found {
		t.Fatalf("initial Get() = (found %t, err %v), want false nil", found, err)
	}
	source.setLimit("new-key", gateway.LimitSpec{RPM: 22})
	if _, found, err := cached.Get(context.Background(), "new-key"); err != nil || found {
		t.Fatalf("warm negative Get() = (found %t, err %v), want false nil", found, err)
	}
	time.Sleep(15 * time.Millisecond)
	got, found, err := cached.Get(context.Background(), "new-key")
	if err != nil || !found || got.RPM != 22 {
		t.Fatalf("expired negative Get() = (%#v, %t, %v), want RPM 22 true nil", got, found, err)
	}
}

func TestCachedQuotaLimitStoreConcurrentPutWinsOverInflightMiss(t *testing.T) {
	source := newBlockingGetQuotaLimitStore(gateway.LimitSpec{RPM: 10})
	defer source.release()
	cached := store.NewCachedQuotaLimitStore(source, time.Minute)

	type getResult struct {
		limit gateway.LimitSpec
		found bool
		err   error
	}
	result := make(chan getResult, 1)
	go func() {
		limit, found, err := cached.Get(context.Background(), "key-1")
		result <- getResult{limit: limit, found: found, err: err}
	}()

	select {
	case <-source.getStarted:
	case <-time.After(time.Second):
		t.Fatal("source Get() did not start")
	}
	if err := cached.Put(context.Background(), "key-1", gateway.LimitSpec{RPM: 20}); err != nil {
		t.Fatalf("concurrent Put() error = %v", err)
	}
	source.release()

	var stale getResult
	select {
	case stale = <-result:
	case <-time.After(time.Second):
		t.Fatal("in-flight Get() did not finish")
	}
	if stale.err != nil || !stale.found || stale.limit.RPM != 10 {
		t.Fatalf("in-flight Get() = (%#v, %t, %v), want its pre-Put snapshot", stale.limit, stale.found, stale.err)
	}

	got, found, err := cached.Get(context.Background(), "key-1")
	if err != nil || !found || got.RPM != 20 {
		t.Fatalf("cached Get() after concurrent Put = (%#v, %t, %v), want RPM 20 true nil", got, found, err)
	}
	if calls := source.getCount(); calls != 1 {
		t.Fatalf("source Get() calls = %d, want 1; stale miss should not evict the Put value", calls)
	}
}

func TestCachedQuotaLimitStoreClosesItsSource(t *testing.T) {
	source := &stubQuotaLimitStore{}
	cached := store.NewCachedQuotaLimitStore(source, time.Minute)
	if err := cached.Close(); err != nil {
		t.Fatal(err)
	}
	if got := source.closeCount(); got != 1 {
		t.Fatalf("source close calls = %d, want 1", got)
	}
}

func TestRefreshingQuotaLimitStoreInferenceReadsUseSnapshot(t *testing.T) {
	source := &stubQuotaLimitStore{
		limits: map[string]gateway.LimitSpec{"known": {RPM: 10}},
	}
	limits, err := store.NewRefreshingQuotaLimitStore(context.Background(), source, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer limits.Close()
	for i := 0; i < 10_000; i++ {
		keyID := fmt.Sprintf("absent-%d", i)
		if _, found, err := limits.Get(context.Background(), keyID); err != nil || found {
			t.Fatalf("Get(%q) = (found %t, err %v), want false nil", keyID, found, err)
		}
	}
	got, found, err := limits.Get(context.Background(), "known")
	if err != nil || !found || got.RPM != 10 {
		t.Fatalf("Get(known) = (%#v, %t, %v), want RPM 10 true nil", got, found, err)
	}
	gets, loads, _ := source.counts()
	if gets != 0 || loads != 1 {
		t.Fatalf("durable source calls after inference reads = gets %d, loads %d; want 0 per-key gets and one startup load", gets, loads)
	}
}

func TestRefreshingQuotaLimitStoreRefreshAndPutUpdateSnapshot(t *testing.T) {
	source := &stubQuotaLimitStore{
		limits: map[string]gateway.LimitSpec{"key-1": {RPM: 10}},
	}
	limits, err := store.NewRefreshingQuotaLimitStore(context.Background(), source, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer limits.Close()

	source.setLimit("key-1", gateway.LimitSpec{RPM: 20})
	if err := limits.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	got, found, err := limits.Get(context.Background(), "key-1")
	if err != nil || !found || got.RPM != 20 {
		t.Fatalf("Get() after refresh = (%#v, %t, %v), want RPM 20 true nil", got, found, err)
	}

	if err := limits.Put(context.Background(), "key-2", gateway.LimitSpec{TPM: 99}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	got, found, err = limits.Get(context.Background(), "key-2")
	if err != nil || !found || got.TPM != 99 {
		t.Fatalf("Get() after Put = (%#v, %t, %v), want TPM 99 true nil", got, found, err)
	}
	gets, loads, puts := source.counts()
	if gets != 0 || loads != 2 || puts != 1 {
		t.Fatalf("durable source calls = gets %d, loads %d, puts %d; want 0, 2, 1", gets, loads, puts)
	}
}

func TestRefreshingQuotaLimitStorePeriodicallyRefreshes(t *testing.T) {
	source := &stubQuotaLimitStore{limits: map[string]gateway.LimitSpec{"key-1": {RPM: 1}}}
	limits, err := store.NewRefreshingQuotaLimitStore(context.Background(), source, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer limits.Close()
	source.setLimit("key-1", gateway.LimitSpec{RPM: 2})
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		got, found, getErr := limits.Get(context.Background(), "key-1")
		if getErr != nil {
			t.Fatal(getErr)
		}
		if found && got.RPM == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic refresh did not publish the updated source limit")
		}
		time.Sleep(time.Millisecond)
	}
	gets, loads, _ := source.counts()
	if gets != 0 || loads < 2 {
		t.Fatalf("durable source calls = gets %d, loads %d; want no per-key gets and at least two loads", gets, loads)
	}
}

func TestRefreshingQuotaLimitStoreFailedRefreshPreservesSnapshotAndHealth(t *testing.T) {
	source := &stubQuotaLimitStore{limits: map[string]gateway.LimitSpec{"key-1": {RPM: 7}}}
	limits, err := store.NewRefreshingQuotaLimitStore(context.Background(), source, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer limits.Close()
	want := errors.New("refresh failed")
	source.setLoadError(want)
	if err := limits.Refresh(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Refresh() error = %v, want %v", err, want)
	}
	got, found, err := limits.Get(context.Background(), "key-1")
	if err != nil || !found || got.RPM != 7 {
		t.Fatalf("Get() after failed refresh = (%#v, %t, %v), want last-known RPM 7 true nil", got, found, err)
	}
	if err := limits.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() after failed refresh = %v, want last-known snapshot ready", err)
	}
	if err := limits.LastRefreshError(); !errors.Is(err, want) {
		t.Fatalf("LastRefreshError() = %v, want %v", err, want)
	}
	source.setLoadError(nil)
	if err := limits.Refresh(context.Background()); err != nil {
		t.Fatalf("recovery Refresh() error = %v", err)
	}
	if err := limits.LastRefreshError(); err != nil {
		t.Fatalf("LastRefreshError() after recovery = %v, want nil", err)
	}
}

func TestRefreshingQuotaLimitStoreHookConvergesAfterConcurrentErrorUpdate(t *testing.T) {
	source := &stubQuotaLimitStore{limits: map[string]gateway.LimitSpec{"key-1": {RPM: 7}}}
	limits, err := store.NewRefreshingQuotaLimitStore(context.Background(), source, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer limits.Close()

	firstHookStarted := make(chan struct{})
	releaseFirstHook := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirstHook) }) }
	defer release()
	var observedMu sync.Mutex
	var observed []error
	observedCurrent := make(chan struct{})
	var observedCurrentOnce sync.Once
	want := errors.New("refresh failed while initial notification was blocked")
	hookCalls := 0
	hook := func(err error) {
		// Hooks must run outside healthMu so observers can inspect current health.
		_ = limits.LastRefreshError()
		observedMu.Lock()
		hookCalls++
		first := hookCalls == 1
		observedMu.Unlock()
		if first {
			close(firstHookStarted)
			<-releaseFirstHook
		}
		observedMu.Lock()
		observed = append(observed, err)
		observedMu.Unlock()
		if errors.Is(err, want) {
			observedCurrentOnce.Do(func() { close(observedCurrent) })
		}
	}

	limits.SetRefreshErrorHook(hook)
	select {
	case <-firstHookStarted:
	case <-time.After(time.Second):
		t.Fatal("initial hook notification did not start")
	}

	source.setLoadError(want)
	if err := limits.Refresh(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Refresh() error = %v, want %v", err, want)
	}
	release()
	select {
	case <-observedCurrent:
	case <-time.After(time.Second):
		t.Fatal("refresh error hook did not converge after concurrent refresh")
	}

	observedMu.Lock()
	defer observedMu.Unlock()
	if len(observed) < 2 {
		t.Fatalf("hook notifications = %d, want stale notification followed by the current generation", len(observed))
	}
	if err := observed[len(observed)-1]; !errors.Is(err, want) {
		t.Fatalf("last hook notification = %v, want current refresh error %v", err, want)
	}
}

func TestRefreshingQuotaLimitStoreHookCanUpdateStore(t *testing.T) {
	source := &stubQuotaLimitStore{limits: map[string]gateway.LimitSpec{"key-1": {RPM: 7}}}
	limits, err := store.NewRefreshingQuotaLimitStore(context.Background(), source, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer limits.Close()

	want := errors.New("refresh failed")
	updated := make(chan error, 1)
	var updateOnce sync.Once
	limits.SetRefreshErrorHook(func(err error) {
		if !errors.Is(err, want) {
			return
		}
		updateOnce.Do(func() {
			updated <- limits.Put(context.Background(), "key-2", gateway.LimitSpec{TPM: 99})
		})
	})
	source.setLoadError(want)
	if err := limits.Refresh(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Refresh() error = %v, want %v", err, want)
	}

	select {
	case err := <-updated:
		if err != nil {
			t.Fatalf("Put() from refresh error hook = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Put() from refresh error hook deadlocked")
	}
	got, found, err := limits.Get(context.Background(), "key-2")
	if err != nil || !found || got.TPM != 99 {
		t.Fatalf("Get() after hook Put = (%#v, %t, %v), want TPM 99 true nil", got, found, err)
	}
}

func TestRefreshingQuotaLimitStoreHookCanCloseFromRefreshLoop(t *testing.T) {
	source := &stubQuotaLimitStore{limits: map[string]gateway.LimitSpec{"key-1": {RPM: 7}}}
	limits, err := store.NewRefreshingQuotaLimitStore(context.Background(), source, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer limits.Close()

	want := errors.New("periodic refresh failed")
	closed := make(chan error, 1)
	var closeOnce sync.Once
	limits.SetRefreshErrorHook(func(err error) {
		if !errors.Is(err, want) {
			return
		}
		closeOnce.Do(func() { closed <- limits.Close() })
	})
	source.setLoadError(want)

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() from refresh error hook = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() from periodic refresh error hook deadlocked")
	}
	if got := source.closeCount(); got != 1 {
		t.Fatalf("source Close calls = %d, want 1", got)
	}
}

func TestRefreshingQuotaLimitStoreRejectsInvalidLoadedLimits(t *testing.T) {
	invalidAtStartup := &stubQuotaLimitStore{limits: map[string]gateway.LimitSpec{"bad": {RPM: -1}}}
	if limits, err := store.NewRefreshingQuotaLimitStore(context.Background(), invalidAtStartup, time.Hour); err == nil {
		_ = limits.Close()
		t.Fatal("NewRefreshingQuotaLimitStore() error = nil, want invalid snapshot rejection")
	}

	source := &stubQuotaLimitStore{limits: map[string]gateway.LimitSpec{"key-1": {RPM: 7}}}
	limits, err := store.NewRefreshingQuotaLimitStore(context.Background(), source, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer limits.Close()
	source.setLimit("key-1", gateway.LimitSpec{RPM: -1})
	if err := limits.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh() error = nil, want invalid durable row rejection")
	}
	got, found, err := limits.Get(context.Background(), "key-1")
	if err != nil || !found || got.RPM != 7 {
		t.Fatalf("Get() after invalid refresh = (%#v, %t, %v), want last-known RPM 7", got, found, err)
	}
	if err := limits.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() after invalid refresh = %v, want last-known snapshot ready", err)
	}
	if err := limits.LastRefreshError(); err == nil {
		t.Fatal("LastRefreshError() after invalid refresh = nil, want surfaced refresh error")
	}
	if err := limits.Put(context.Background(), "key-2", gateway.LimitSpec{TPM: -1}); err == nil {
		t.Fatal("Put() error = nil, want invalid limit rejection")
	}
}

func TestRefreshingQuotaLimitStoreCloseIsIdempotentAndRejectsUse(t *testing.T) {
	source := &stubQuotaLimitStore{}
	limits, err := store.NewRefreshingQuotaLimitStore(context.Background(), source, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := limits.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := limits.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if got := source.closeCount(); got != 1 {
		t.Fatalf("source Close calls = %d, want 1", got)
	}
	if _, _, err := limits.Get(context.Background(), "key"); !errors.Is(err, store.ErrQuotaLimitStoreClosed) {
		t.Fatalf("Get() after Close error = %v, want ErrQuotaLimitStoreClosed", err)
	}
	if err := limits.Put(context.Background(), "key", gateway.LimitSpec{}); !errors.Is(err, store.ErrQuotaLimitStoreClosed) {
		t.Fatalf("Put() after Close error = %v, want ErrQuotaLimitStoreClosed", err)
	}
	if err := limits.Ping(context.Background()); !errors.Is(err, store.ErrQuotaLimitStoreClosed) {
		t.Fatalf("Ping() after Close error = %v, want ErrQuotaLimitStoreClosed", err)
	}
}

func TestMemoryQuotaStoreGetUsage(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	scope := gateway.ScopedLimit{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-usage"},
		Limits: gateway.LimitSpec{
			RPM:            100,
			TPM:            10_000,
			MaxParallel:    10,
			MaxSpendMicros: 10_000,
			DailyTokens:    1000,
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

func TestMemoryQuotaStoreSeparatesSpendBudgetsByDuration(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	ctx := context.Background()
	ref := gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-budget-duration"}
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

func TestMemoryQuotaStoreTopUpUpdatesHeldUsage(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	scope := gateway.ScopedLimit{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-topup"},
		Limits: gateway.LimitSpec{
			TPM:         100,
			MaxParallel: 10,
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

func TestMemoryQuotaStoreExpiredReservationReleasesHolds(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	scope := gateway.ScopedLimit{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-expiry"},
		Limits: gateway.LimitSpec{
			MaxParallel:    1,
			DailyTokens:    10,
			MaxSpendMicros: 100,
		},
	}
	first, err := quota.Reserve(context.Background(), "req-expired", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens:          3,
		ReservedOutputTokens: 2,
		EstimatedSpendMicros: 50,
	}, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if _, err := quota.Reserve(context.Background(), "req-blocked", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 1}, time.Minute); err == nil {
		t.Fatal("Reserve() before expiry error = nil, want max parallel rejection")
	}
	time.Sleep(30 * time.Millisecond)

	usage, err := quota.GetUsage(context.Background(), scope)
	if err != nil {
		t.Fatalf("GetUsage() error = %v", err)
	}
	if usage.ActiveRequests != 0 || usage.DailyHeldTokens != 0 || usage.SpendHeldMicros != 0 {
		t.Fatalf("usage after expiry = %#v, want released holds", usage)
	}
	if err := quota.TopUp(context.Background(), first, []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 1}, time.Minute); !errors.Is(err, store.ErrQuotaReservationNotFound) {
		t.Fatalf("TopUp(expired) error = %v, want ErrQuotaReservationNotFound", err)
	}
	second, err := quota.Reserve(context.Background(), "req-after-expiry", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens:          4,
		ReservedOutputTokens: 4,
		EstimatedSpendMicros: 80,
	}, time.Minute)
	if err != nil {
		t.Fatalf("Reserve() after expiry error = %v", err)
	}
	// A first settlement after expiry is observable and must not release the
	// unrelated live hold. Only duplicate settlement of a completed ticket is
	// idempotent.
	if err := quota.Refund(context.Background(), first); !errors.Is(err, store.ErrQuotaReservationNotFound) {
		t.Fatalf("Refund(expired) error = %v, want ErrQuotaReservationNotFound", err)
	}
	usage, err = quota.GetUsage(context.Background(), scope)
	if err != nil {
		t.Fatalf("GetUsage() after stale refund error = %v", err)
	}
	if usage.ActiveRequests != 1 || usage.DailyHeldTokens != 8 || usage.SpendHeldMicros != 80 {
		t.Fatalf("usage after stale refund = %#v, want second hold intact", usage)
	}
	if err := quota.Refund(context.Background(), second); err != nil {
		t.Fatalf("Refund(second) error = %v", err)
	}
}

func TestMemoryQuotaStoreZeroTopUpRenewsReservation(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	scope := gateway.ScopedLimit{
		Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-renew"},
		Limits: gateway.LimitSpec{MaxParallel: 1, DailyTokens: 10},
	}
	ticket, err := quota.Reserve(context.Background(), "req-renew", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 1}, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := quota.TopUp(context.Background(), ticket, []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{}, 40*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	usage, err := quota.GetUsage(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if usage.ActiveRequests != 1 || usage.DailyHeldTokens != 1 {
		t.Fatalf("usage after renewal = %#v, want live hold", usage)
	}
	time.Sleep(30 * time.Millisecond)
	usage, err = quota.GetUsage(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if usage.ActiveRequests != 0 || usage.DailyHeldTokens != 0 {
		t.Fatalf("usage after renewed lease expiry = %#v, want released hold", usage)
	}
}

func TestMemoryQuotaStoreSettlementIsIdempotentAndNonnegative(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	scope := gateway.ScopedLimit{
		Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-idempotent"},
		Limits: gateway.LimitSpec{
			MaxSpendMicros: 100,
			DailyTokens:    100,
		},
	}
	ticket, err := quota.Reserve(context.Background(), "req-idempotent", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{
		InputTokens:          3,
		ReservedOutputTokens: 2,
	}, time.Minute)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	actual := gateway.ActualUsage{InputTokens: 2, OutputTokens: 2, SpendMicros: 10}
	if err := quota.Commit(context.Background(), ticket, actual); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if err := quota.Commit(context.Background(), ticket, actual); err != nil {
		t.Fatalf("duplicate Commit() error = %v", err)
	}
	if err := quota.Refund(context.Background(), ticket); err != nil {
		t.Fatalf("Refund() after commit error = %v", err)
	}
	usage, err := quota.GetUsage(context.Background(), scope)
	if err != nil {
		t.Fatalf("GetUsage() error = %v", err)
	}
	if usage.ActiveRequests != 0 || usage.DailyHeldTokens != 0 || usage.DailyUsedTokens != 4 || usage.SpendUsedMicros != 10 {
		t.Fatalf("usage after duplicate settlement = %#v", usage)
	}
}

func TestMemoryQuotaStoreRejectsReservationIDConflict(t *testing.T) {
	quota := store.NewMemoryQuotaStore()
	scope := gateway.ScopedLimit{Ref: gateway.ScopeRef{Kind: gateway.ScopeKey, ID: "key-a"}}
	estimate := gateway.EstimatedUsage{InputTokens: 2, ReservedOutputTokens: 3}
	if _, err := quota.Reserve(context.Background(), "shared-id", []gateway.ScopedLimit{scope}, estimate, time.Minute); err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if _, err := quota.Reserve(context.Background(), "shared-id", []gateway.ScopedLimit{scope}, estimate, time.Minute); err != nil {
		t.Fatalf("idempotent Reserve() error = %v", err)
	}
	if _, err := quota.Reserve(context.Background(), "shared-id", []gateway.ScopedLimit{scope}, gateway.EstimatedUsage{InputTokens: 99}, time.Minute); !errors.Is(err, store.ErrQuotaReservationConflict) {
		t.Fatalf("conflicting Reserve() error = %v, want ErrQuotaReservationConflict", err)
	}
}

func TestMemoryCounterStoreClampsAtZero(t *testing.T) {
	counter := store.NewMemoryCounterStore()
	if value, err := counter.Add(context.Background(), "counter", -10, time.Minute); err != nil || value != 0 {
		t.Fatalf("Add(-10) = (%d, %v), want (0, nil)", value, err)
	}
}

func TestCachedQuotaLimitStorePropagatesSourceError(t *testing.T) {
	cached := store.NewCachedQuotaLimitStore(&stubQuotaLimitStore{err: errors.New("boom")}, time.Minute)
	if _, _, err := cached.Get(context.Background(), "key-1"); err == nil {
		t.Fatal("Get() error = nil, want source error")
	}
}

func TestCachedQuotaLimitStoreDelegatesHealthCheck(t *testing.T) {
	want := errors.New("database unavailable")
	source := &stubQuotaLimitStore{pingErr: want}
	cached := store.NewCachedQuotaLimitStore(source, time.Minute)
	if err := cached.Ping(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Ping() error = %v, want %v", err, want)
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
	mu      sync.Mutex
	limits  map[string]gateway.LimitSpec
	gets    int
	loads   int
	puts    int
	closes  int
	err     error
	loadErr error
	pingErr error
}

func (s *stubQuotaLimitStore) Ping(context.Context) error {
	return s.pingErr
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

func (s *stubQuotaLimitStore) LoadAll(context.Context) (map[string]gateway.LimitSpec, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	limits := make(map[string]gateway.LimitSpec, len(s.limits))
	for keyID, limit := range s.limits {
		limits[keyID] = limit
	}
	return limits, nil
}

func (s *stubQuotaLimitStore) Put(_ context.Context, keyID string, limit gateway.LimitSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	if s.limits == nil {
		s.limits = map[string]gateway.LimitSpec{}
	}
	s.limits[keyID] = limit
	return s.err
}

func (s *stubQuotaLimitStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	return nil
}

func (s *stubQuotaLimitStore) setLimit(keyID string, limit gateway.LimitSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.limits == nil {
		s.limits = map[string]gateway.LimitSpec{}
	}
	s.limits[keyID] = limit
}

func (s *stubQuotaLimitStore) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

func (s *stubQuotaLimitStore) setLoadError(err error) {
	s.mu.Lock()
	s.loadErr = err
	s.mu.Unlock()
}

func (s *stubQuotaLimitStore) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

func (s *stubQuotaLimitStore) counts() (gets, loads, puts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets, s.loads, s.puts
}

type blockingGetQuotaLimitStore struct {
	mu          sync.Mutex
	limit       gateway.LimitSpec
	gets        int
	getStarted  chan struct{}
	releaseGet  chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingGetQuotaLimitStore(limit gateway.LimitSpec) *blockingGetQuotaLimitStore {
	return &blockingGetQuotaLimitStore{
		limit:      limit,
		getStarted: make(chan struct{}),
		releaseGet: make(chan struct{}),
	}
}

func (s *blockingGetQuotaLimitStore) Get(ctx context.Context, _ string) (gateway.LimitSpec, bool, error) {
	s.mu.Lock()
	limit := s.limit
	s.gets++
	s.mu.Unlock()
	s.startedOnce.Do(func() { close(s.getStarted) })
	select {
	case <-s.releaseGet:
		return limit, true, nil
	case <-ctx.Done():
		return gateway.LimitSpec{}, false, ctx.Err()
	}
}

func (s *blockingGetQuotaLimitStore) Put(_ context.Context, _ string, limit gateway.LimitSpec) error {
	s.mu.Lock()
	s.limit = limit
	s.mu.Unlock()
	return nil
}

func (s *blockingGetQuotaLimitStore) release() {
	s.releaseOnce.Do(func() { close(s.releaseGet) })
}

func (s *blockingGetQuotaLimitStore) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}
