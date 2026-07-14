package store

import (
	"container/list"
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"sync"
	"time"

	"llmgw/gateway"
)

var (
	ErrQuotaReservationNotFound = errors.New("quota reservation not found or expired")
	ErrQuotaReservationConflict = errors.New("quota reservation ID is already used by a different request")
	ErrQuotaAccountingCapacity  = errors.New("quota accounting exceeds the maximum supported value")
)

type QuotaStore interface {
	Reserve(ctx context.Context, requestID string, scopes []gateway.ScopedLimit, estimate gateway.EstimatedUsage, ttl time.Duration) (gateway.QuotaTicket, error)
	TopUp(ctx context.Context, ticket gateway.QuotaTicket, scopes []gateway.ScopedLimit, delta gateway.EstimatedUsage, ttl time.Duration) error
	Commit(ctx context.Context, ticket gateway.QuotaTicket, actual gateway.ActualUsage) error
	Refund(ctx context.Context, ticket gateway.QuotaTicket) error
}

type MemoryCounterStore struct {
	mu         sync.Mutex
	entries    map[string]counterEntry
	operations uint64
}

type counterEntry struct {
	Value   int64
	Expires time.Time
}

type MemoryQuotaStore struct {
	mu           sync.Mutex
	values       map[string]int64
	valueExpires map[string]time.Time
	counters     map[string]counterEntry
	reservations map[string]memoryReservation
	settlements  map[string]memorySettlement
	settledOrder *list.List
}

type memorySettlement struct {
	expires time.Time
	expired bool
	element *list.Element
}

const (
	quotaSettlementReplayWindow = time.Minute
	maxMemorySettlements        = 65_536
	memoryCounterPruneEvery     = 256
)

type memoryReservation struct {
	ticket  gateway.QuotaTicket
	held    gateway.EstimatedUsage
	scopes  []memoryScopeReservation
	expires time.Time
}

type memoryScopeReservation struct {
	ref     gateway.ScopeRef
	limit   gateway.LimitSpec
	keys    quotaKeys
	expires quotaExpiries
}

type quotaExpiries struct {
	spendUsed time.Time
	dayUsed   time.Time
	monthUsed time.Time
}

type quotaKeys struct {
	active    string
	rpm       string
	tpm       string
	spendUsed string
	spendHeld string
	dayUsed   string
	dayHeld   string
	monthUsed string
	monthHeld string
}

func NewMemoryCounterStore() *MemoryCounterStore {
	return &MemoryCounterStore{entries: map[string]counterEntry{}}
}

func NewMemoryQuotaStore() *MemoryQuotaStore {
	return &MemoryQuotaStore{
		values:       map[string]int64{},
		valueExpires: map[string]time.Time{},
		counters:     map[string]counterEntry{},
		reservations: map[string]memoryReservation{},
		settlements:  map[string]memorySettlement{},
		settledOrder: list.New(),
	}
}

func (s *MemoryCounterStore) Add(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	now := time.Now()
	s.operations++
	if s.operations%memoryCounterPruneEvery == 0 {
		s.pruneExpired(now)
	}
	entry := s.entries[key]
	if entry.Expires.Before(now) {
		entry.Value = 0
	}
	entry.Value = saturatingAddNonnegative(entry.Value, delta)
	entry.Expires = now.Add(ttl)
	s.entries[key] = entry
	return entry.Value, nil
}

func (s *MemoryCounterStore) pruneExpired(now time.Time) {
	for key, entry := range s.entries {
		if !entry.Expires.IsZero() && !now.Before(entry.Expires) {
			delete(s.entries, key)
		}
	}
}

func (s *MemoryQuotaStore) Reserve(ctx context.Context, requestID string, scopes []gateway.ScopedLimit, estimate gateway.EstimatedUsage, ttl time.Duration) (gateway.QuotaTicket, error) {
	if err := ctx.Err(); err != nil {
		return gateway.QuotaTicket{}, err
	}
	if requestID == "" {
		return gateway.QuotaTicket{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return gateway.QuotaTicket{}, err
	}
	now := time.Now()
	s.cleanupExpired(now)
	estimate = normalizeEstimatedUsage(estimate)
	ttl = normalizeReservationTTL(ttl)
	resolved := resolveScopeReservations(scopes, now)
	if _, exists := s.settlements[requestID]; exists {
		return gateway.QuotaTicket{}, ErrQuotaReservationConflict
	}
	if existing, ok := s.reservations[requestID]; ok {
		if existing.held != estimate || !sameReservationScopes(existing.scopes, resolved) {
			return gateway.QuotaTicket{}, ErrQuotaReservationConflict
		}
		return existing.ticket, nil
	}
	for _, scope := range resolved {
		if err := s.checkScope(scope, estimate, now, false); err != nil {
			return gateway.QuotaTicket{}, err
		}
	}
	for _, scope := range resolved {
		s.applyScopeHold(scope, estimate, now, false)
	}
	ticket := gateway.QuotaTicket{
		RequestID: requestID,
		Scopes:    scopedRefs(resolved),
	}
	s.reservations[requestID] = memoryReservation{
		ticket:  ticket,
		held:    estimate,
		scopes:  resolved,
		expires: now.Add(ttl),
	}
	return ticket, nil
}

func sameReservationScopes(left, right []memoryScopeReservation) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].ref != right[i].ref || !reflect.DeepEqual(left[i].limit, right[i].limit) {
			return false
		}
	}
	return true
}

func (s *MemoryQuotaStore) TopUp(ctx context.Context, ticket gateway.QuotaTicket, _ []gateway.ScopedLimit, delta gateway.EstimatedUsage, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	delta = normalizeEstimatedUsage(delta)
	if ticket.RequestID == "" {
		return nil
	}
	now := time.Now()
	s.cleanupExpired(now)
	res, ok := s.reservations[ticket.RequestID]
	if !ok {
		return ErrQuotaReservationNotFound
	}
	if delta.TotalTokens() == 0 && delta.EstimatedSpendMicros == 0 {
		res.expires = now.Add(normalizeReservationTTL(ttl))
		s.reservations[ticket.RequestID] = res
		return nil
	}
	for _, scope := range res.scopes {
		if err := s.checkScope(scope, delta, now, true); err != nil {
			return err
		}
	}
	for _, scope := range res.scopes {
		s.applyScopeHold(scope, delta, now, true)
	}
	res.held.InputTokens = saturatingAddNonnegative(res.held.InputTokens, delta.InputTokens)
	res.held.ReservedOutputTokens = saturatingAddNonnegative(res.held.ReservedOutputTokens, delta.ReservedOutputTokens)
	res.held.EstimatedSpendMicros = saturatingAddNonnegative(res.held.EstimatedSpendMicros, delta.EstimatedSpendMicros)
	res.expires = now.Add(normalizeReservationTTL(ttl))
	s.reservations[ticket.RequestID] = res
	return nil
}

func (s *MemoryQuotaStore) Commit(ctx context.Context, ticket gateway.QuotaTicket, actual gateway.ActualUsage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if ticket.RequestID == "" {
		return nil
	}
	now := time.Now()
	s.cleanupExpired(now)
	res, ok := s.reservations[ticket.RequestID]
	if !ok {
		if settlement, exists := s.settlements[ticket.RequestID]; exists && !settlement.expired {
			return nil
		}
		return ErrQuotaReservationNotFound
	}
	actual = normalizeActualUsage(actual)
	s.releaseHold(res, actual.TotalTokens(), now)
	for _, scope := range res.scopes {
		s.addValue(scope.keys.spendUsed, actual.SpendMicros, scope.expires.spendUsed)
		s.addValue(scope.keys.dayUsed, actual.TotalTokens(), scope.expires.dayUsed)
		s.addValue(scope.keys.monthUsed, actual.TotalTokens(), scope.expires.monthUsed)
	}
	delete(s.reservations, ticket.RequestID)
	s.recordSettlement(ticket.RequestID, false, now)
	return nil
}

func (s *MemoryQuotaStore) Refund(ctx context.Context, ticket gateway.QuotaTicket) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if ticket.RequestID == "" {
		return nil
	}
	now := time.Now()
	s.cleanupExpired(now)
	res, ok := s.reservations[ticket.RequestID]
	if !ok {
		if settlement, exists := s.settlements[ticket.RequestID]; exists && !settlement.expired {
			return nil
		}
		return ErrQuotaReservationNotFound
	}
	s.releaseHold(res, 0, now)
	delete(s.reservations, ticket.RequestID)
	s.recordSettlement(ticket.RequestID, false, now)
	return nil
}

func (s *MemoryQuotaStore) cleanupExpired(now time.Time) {
	for requestID, reservation := range s.reservations {
		if reservation.expires.IsZero() || now.Before(reservation.expires) {
			continue
		}
		s.releaseHold(reservation, 0, now)
		delete(s.reservations, requestID)
		s.recordSettlement(requestID, true, now)
	}
	s.cleanupSettlements(now)
	for key, counter := range s.counters {
		if !counter.Expires.IsZero() && !now.Before(counter.Expires) {
			delete(s.counters, key)
		}
	}
	for key, expires := range s.valueExpires {
		if !expires.IsZero() && !now.Before(expires) {
			delete(s.values, key)
			delete(s.valueExpires, key)
		}
	}
}

func (s *MemoryQuotaStore) recordSettlement(requestID string, expired bool, now time.Time) {
	if requestID == "" {
		return
	}
	if existing, ok := s.settlements[requestID]; ok && existing.element != nil {
		s.settledOrder.Remove(existing.element)
	}
	element := s.settledOrder.PushBack(requestID)
	s.settlements[requestID] = memorySettlement{
		expires: now.Add(quotaSettlementReplayWindow),
		expired: expired,
		element: element,
	}
	s.cleanupSettlements(now)
	for len(s.settlements) > maxMemorySettlements {
		s.removeOldestSettlement()
	}
}

func (s *MemoryQuotaStore) cleanupSettlements(now time.Time) {
	for {
		front := s.settledOrder.Front()
		if front == nil {
			return
		}
		requestID, _ := front.Value.(string)
		settlement, ok := s.settlements[requestID]
		if ok && now.Before(settlement.expires) {
			return
		}
		s.settledOrder.Remove(front)
		if ok && settlement.element == front {
			delete(s.settlements, requestID)
		}
	}
}

func (s *MemoryQuotaStore) removeOldestSettlement() {
	front := s.settledOrder.Front()
	if front == nil {
		return
	}
	requestID, _ := front.Value.(string)
	s.settledOrder.Remove(front)
	if settlement, ok := s.settlements[requestID]; ok && settlement.element == front {
		delete(s.settlements, requestID)
	}
}

func (s *MemoryQuotaStore) releaseHold(res memoryReservation, actualTPM int64, now time.Time) {
	heldTokens := res.held.TotalTokens()
	for _, scope := range res.scopes {
		s.reconcileCounter(scope.keys.tpm, heldTokens, actualTPM, now)
		s.decrValue(scope.keys.active, 1)
		s.decrValue(scope.keys.spendHeld, res.held.EstimatedSpendMicros)
		s.decrValue(scope.keys.dayHeld, heldTokens)
		s.decrValue(scope.keys.monthHeld, heldTokens)
	}
}

func (s *MemoryQuotaStore) reconcileCounter(key string, held, actual int64, now time.Time) {
	if key == "" {
		return
	}
	entry, ok := s.counters[key]
	if !ok || (!entry.Expires.IsZero() && !now.Before(entry.Expires)) {
		delete(s.counters, key)
		return
	}
	held = max(held, 0)
	actual = max(actual, 0)
	if actual >= held {
		entry.Value = saturatingAddNonnegative(entry.Value, actual-held)
	} else {
		entry.Value = saturatingAddNonnegative(entry.Value, -(held - actual))
	}
	if entry.Value == 0 {
		delete(s.counters, key)
		return
	}
	s.counters[key] = entry
}

func (s *MemoryQuotaStore) checkScope(scope memoryScopeReservation, estimate gateway.EstimatedUsage, now time.Time, topUp bool) error {
	if !topUp && exceedsLimit(scope.limit.MaxParallel, s.values[scope.keys.active], 1) {
		return gateway.RateLimited("quota exceeded: max parallel")
	}
	if !topUp && exceedsLimit(scope.limit.RPM, s.counterValue(scope.keys.rpm, now), 1) {
		return gateway.RateLimited("quota exceeded: rpm")
	}
	if exceedsLimit(scope.limit.TPM, s.counterValue(scope.keys.tpm, now), estimate.TotalTokens()) {
		return gateway.RateLimited("quota exceeded: tpm")
	}
	if exceedsLimit(scope.limit.MaxSpendMicros, s.values[scope.keys.spendUsed], s.values[scope.keys.spendHeld], estimate.EstimatedSpendMicros) {
		return gateway.RateLimited("quota exceeded: spend")
	}
	if exceedsLimit(scope.limit.DailyTokens, s.values[scope.keys.dayUsed], s.values[scope.keys.dayHeld], estimate.TotalTokens()) {
		return gateway.RateLimited("quota exceeded: daily tokens")
	}
	if exceedsLimit(scope.limit.MonthlyTokens, s.values[scope.keys.monthUsed], s.values[scope.keys.monthHeld], estimate.TotalTokens()) {
		return gateway.RateLimited("quota exceeded: monthly tokens")
	}
	return nil
}

func (s *MemoryQuotaStore) applyScopeHold(scope memoryScopeReservation, estimate gateway.EstimatedUsage, now time.Time, topUp bool) {
	if !topUp {
		s.addValue(scope.keys.active, 1, time.Time{})
		s.addCounter(scope.keys.rpm, 1, timeUntilNextMinute(now))
	}
	s.addCounter(scope.keys.tpm, estimate.TotalTokens(), timeUntilNextMinute(now))
	s.addValue(scope.keys.spendHeld, estimate.EstimatedSpendMicros, time.Time{})
	s.addValue(scope.keys.dayHeld, estimate.TotalTokens(), time.Time{})
	s.addValue(scope.keys.monthHeld, estimate.TotalTokens(), time.Time{})
}

func (s *MemoryQuotaStore) counterValue(key string, now time.Time) int64 {
	entry := s.counters[key]
	if entry.Expires.Before(now) {
		return 0
	}
	return entry.Value
}

func (s *MemoryQuotaStore) addCounter(key string, delta int64, ttl time.Duration) {
	if key == "" || delta == 0 {
		return
	}
	now := time.Now()
	entry := s.counters[key]
	if entry.Expires.Before(now) {
		entry.Value = 0
	}
	entry.Value = saturatingAddNonnegative(entry.Value, delta)
	entry.Expires = now.Add(ttl)
	s.counters[key] = entry
}

func (s *MemoryQuotaStore) addValue(key string, delta int64, expires time.Time) {
	if key == "" || delta == 0 {
		return
	}
	s.values[key] = saturatingAddNonnegative(s.values[key], delta)
	if !expires.IsZero() {
		if now := time.Now(); !expires.After(now) {
			expires = now.Add(quotaBucketExpiryGrace)
		}
		s.valueExpires[key] = expires
	}
}

func (s *MemoryQuotaStore) decrValue(key string, delta int64) {
	if key == "" || delta == 0 {
		return
	}
	s.values[key] -= delta
	if s.values[key] <= 0 {
		delete(s.values, key)
		delete(s.valueExpires, key)
	}
}

func normalizeEstimatedUsage(usage gateway.EstimatedUsage) gateway.EstimatedUsage {
	if usage.InputTokens < 0 {
		usage.InputTokens = 0
	}
	if usage.ReservedOutputTokens < 0 {
		usage.ReservedOutputTokens = 0
	}
	if usage.EstimatedSpendMicros < 0 {
		usage.EstimatedSpendMicros = 0
	}
	if usage.InputTokens > math.MaxInt64-usage.ReservedOutputTokens {
		usage.InputTokens = math.MaxInt64
		usage.ReservedOutputTokens = 0
	}
	return usage
}

func normalizeActualUsage(usage gateway.ActualUsage) gateway.ActualUsage {
	if usage.InputTokens < 0 {
		usage.InputTokens = 0
	}
	if usage.OutputTokens < 0 {
		usage.OutputTokens = 0
	}
	if usage.SpendMicros < 0 {
		usage.SpendMicros = 0
	}
	if usage.InputTokens > math.MaxInt64-usage.OutputTokens {
		usage.InputTokens = math.MaxInt64
		usage.OutputTokens = 0
	}
	return usage
}

func saturatingAddNonnegative(current, delta int64) int64 {
	if current < 0 {
		current = 0
	}
	if delta < 0 {
		if delta == math.MinInt64 || -delta >= current {
			return 0
		}
		return current + delta
	}
	if current > math.MaxInt64-delta {
		return math.MaxInt64
	}
	return current + delta
}

func exceedsLimit(limit int64, values ...int64) bool {
	if limit <= 0 {
		return false
	}
	total := int64(0)
	for _, value := range values {
		if value < 0 {
			value = 0
		}
		if value > limit-total {
			return true
		}
		total += value
	}
	return false
}

func resolveScopeReservations(scopes []gateway.ScopedLimit, now time.Time) []memoryScopeReservation {
	out := make([]memoryScopeReservation, 0, len(scopes))
	for _, scope := range scopes {
		out = append(out, memoryScopeReservation{
			ref:     scope.Ref,
			limit:   scope.Limits,
			keys:    quotaKeysForScope(scope.Ref, scope.Limits, now),
			expires: quotaExpiriesForLimit(scope.Limits, now),
		})
	}
	return out
}

func scopedRefs(scopes []memoryScopeReservation) []gateway.ScopeRef {
	out := make([]gateway.ScopeRef, 0, len(scopes))
	for _, scope := range scopes {
		out = append(out, scope.ref)
	}
	return out
}

func quotaKeysForScope(ref gateway.ScopeRef, limit gateway.LimitSpec, now time.Time) quotaKeys {
	prefix := string(ref.Kind) + ":" + ref.ID
	keys := quotaKeys{}
	if limit.MaxParallel > 0 {
		keys.active = "quota:" + prefix + ":active"
	}
	if limit.RPM > 0 {
		keys.rpm = "quota:" + prefix + ":rpm:" + now.UTC().Format("200601021504")
	}
	if limit.TPM > 0 {
		keys.tpm = "quota:" + prefix + ":tpm:" + now.UTC().Format("200601021504")
	}
	if limit.MaxSpendMicros > 0 || limit.SoftSpendMicros > 0 {
		bucket := budgetBucket(limit.BudgetDuration.Duration, now)
		keys.spendUsed = "quota:" + prefix + ":used:spend:" + bucket
		keys.spendHeld = "quota:" + prefix + ":held:spend:" + bucket
	}
	if limit.DailyTokens > 0 {
		keys.dayUsed = "quota:" + prefix + ":used:day:" + now.UTC().Format("20060102")
		keys.dayHeld = "quota:" + prefix + ":held:day:" + now.UTC().Format("20060102")
	}
	if limit.MonthlyTokens > 0 {
		keys.monthUsed = "quota:" + prefix + ":used:month:" + now.UTC().Format("200601")
		keys.monthHeld = "quota:" + prefix + ":held:month:" + now.UTC().Format("200601")
	}
	return keys
}

const quotaBucketExpiryGrace = time.Hour

func quotaExpiriesForLimit(limit gateway.LimitSpec, now time.Time) quotaExpiries {
	utc := now.UTC()
	expires := quotaExpiries{}
	if limit.BudgetDuration.Duration > 0 {
		expires.spendUsed = budgetBucketStart(limit.BudgetDuration.Duration, utc).
			Add(limit.BudgetDuration.Duration).
			Add(quotaBucketExpiryGrace)
	}
	if limit.DailyTokens > 0 {
		nextDay := time.Date(utc.Year(), utc.Month(), utc.Day()+1, 0, 0, 0, 0, time.UTC)
		expires.dayUsed = nextDay.Add(quotaBucketExpiryGrace)
	}
	if limit.MonthlyTokens > 0 {
		nextMonth := time.Date(utc.Year(), utc.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		expires.monthUsed = nextMonth.Add(quotaBucketExpiryGrace)
	}
	return expires
}

func budgetBucket(period time.Duration, now time.Time) string {
	if period <= 0 {
		return "forever"
	}
	return strconv.FormatInt(int64(period), 10) + ":" +
		budgetBucketStart(period, now).Format(time.RFC3339Nano)
}

// budgetBucketStart aligns fixed spend windows to the Unix epoch. time.Truncate
// aligns to Go's zero time instead, which shifts durations that do not evenly
// divide the epoch offset (for example 720-hour windows).
func budgetBucketStart(period time.Duration, now time.Time) time.Time {
	if period <= 0 {
		return time.Time{}
	}
	unixNanos := now.UTC().UnixNano()
	remainder := unixNanos % int64(period)
	if remainder < 0 {
		remainder += int64(period)
	}
	return time.Unix(0, unixNanos-remainder).UTC()
}

func timeUntilNextMinute(now time.Time) time.Duration {
	return now.UTC().Truncate(time.Minute).Add(time.Minute).Sub(now.UTC())
}
