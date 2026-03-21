package store

import (
	"context"
	"sync"
	"time"

	"llmgw/gateway"
)

type QuotaStore interface {
	Reserve(ctx context.Context, requestID string, scopes []gateway.ScopedLimit, estimate gateway.EstimatedUsage, ttl time.Duration) (gateway.QuotaTicket, error)
	TopUp(ctx context.Context, ticket gateway.QuotaTicket, scopes []gateway.ScopedLimit, delta gateway.EstimatedUsage, ttl time.Duration) error
	Commit(ctx context.Context, ticket gateway.QuotaTicket, actual gateway.ActualUsage) error
	Refund(ctx context.Context, ticket gateway.QuotaTicket) error
}

type MemoryCounterStore struct {
	mu      sync.Mutex
	entries map[string]counterEntry
}

type counterEntry struct {
	Value   int64
	Expires time.Time
}

type MemoryQuotaStore struct {
	mu           sync.Mutex
	values       map[string]int64
	counters     map[string]counterEntry
	reservations map[string]memoryReservation
}

type memoryReservation struct {
	ticket gateway.QuotaTicket
	held   gateway.EstimatedUsage
	scopes []memoryScopeReservation
	active bool
}

type memoryScopeReservation struct {
	ref   gateway.ScopeRef
	limit gateway.LimitSpec
	keys  quotaKeys
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
		counters:     map[string]counterEntry{},
		reservations: map[string]memoryReservation{},
	}
}

func (s *MemoryCounterStore) Add(_ context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	entry := s.entries[key]
	if entry.Expires.Before(now) {
		entry.Value = 0
	}
	entry.Value += delta
	entry.Expires = now.Add(ttl)
	s.entries[key] = entry
	return entry.Value, nil
}

func (s *MemoryQuotaStore) Reserve(_ context.Context, requestID string, scopes []gateway.ScopedLimit, estimate gateway.EstimatedUsage, _ time.Duration) (gateway.QuotaTicket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.reservations[requestID]; ok {
		return existing.ticket, nil
	}
	now := time.Now()
	resolved := resolveScopeReservations(scopes, now)
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
		ticket: ticket,
		held:   estimate,
		scopes: resolved,
		active: true,
	}
	return ticket, nil
}

func (s *MemoryQuotaStore) TopUp(_ context.Context, ticket gateway.QuotaTicket, scopes []gateway.ScopedLimit, delta gateway.EstimatedUsage, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ticket.RequestID == "" || delta.TotalTokens() == 0 && delta.EstimatedSpendMicros == 0 {
		return nil
	}
	res, ok := s.reservations[ticket.RequestID]
	if !ok {
		return nil
	}
	now := time.Now()
	for _, scope := range resolveScopeReservations(scopes, now) {
		if err := s.checkScope(scope, delta, now, true); err != nil {
			return err
		}
	}
	for _, scope := range res.scopes {
		s.applyScopeHold(scope, delta, now, true)
	}
	res.held.InputTokens += delta.InputTokens
	res.held.ReservedOutputTokens += delta.ReservedOutputTokens
	res.held.EstimatedSpendMicros += delta.EstimatedSpendMicros
	s.reservations[ticket.RequestID] = res
	return nil
}

func (s *MemoryQuotaStore) Commit(_ context.Context, ticket gateway.QuotaTicket, actual gateway.ActualUsage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ticket.RequestID == "" {
		return nil
	}
	res, ok := s.reservations[ticket.RequestID]
	if !ok {
		return nil
	}
	for _, scope := range res.scopes {
		s.decrValue(scope.keys.active, 1)
		s.decrValue(scope.keys.spendHeld, res.held.EstimatedSpendMicros)
		s.decrValue(scope.keys.dayHeld, res.held.TotalTokens())
		s.decrValue(scope.keys.monthHeld, res.held.TotalTokens())
		s.values[scope.keys.spendUsed] += actual.SpendMicros
		s.values[scope.keys.dayUsed] += actual.TotalTokens()
		s.values[scope.keys.monthUsed] += actual.TotalTokens()
	}
	delete(s.reservations, ticket.RequestID)
	return nil
}

func (s *MemoryQuotaStore) Refund(_ context.Context, ticket gateway.QuotaTicket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, ok := s.reservations[ticket.RequestID]
	if !ok {
		return nil
	}
	for _, scope := range res.scopes {
		s.decrValue(scope.keys.active, 1)
		s.decrValue(scope.keys.spendHeld, res.held.EstimatedSpendMicros)
		s.decrValue(scope.keys.dayHeld, res.held.TotalTokens())
		s.decrValue(scope.keys.monthHeld, res.held.TotalTokens())
	}
	delete(s.reservations, ticket.RequestID)
	return nil
}

func (s *MemoryQuotaStore) checkScope(scope memoryScopeReservation, estimate gateway.EstimatedUsage, now time.Time, topUp bool) error {
	if scope.limit.MaxParallel > 0 && !topUp && s.values[scope.keys.active]+1 > scope.limit.MaxParallel {
		return gateway.RateLimited("quota exceeded: max parallel")
	}
	if scope.limit.RPM > 0 && !topUp && s.counterValue(scope.keys.rpm, now)+1 > scope.limit.RPM {
		return gateway.RateLimited("quota exceeded: rpm")
	}
	if scope.limit.TPM > 0 && s.counterValue(scope.keys.tpm, now)+estimate.TotalTokens() > scope.limit.TPM {
		return gateway.RateLimited("quota exceeded: tpm")
	}
	if scope.limit.MaxSpendMicros > 0 && s.values[scope.keys.spendUsed]+s.values[scope.keys.spendHeld]+estimate.EstimatedSpendMicros > scope.limit.MaxSpendMicros {
		return gateway.RateLimited("quota exceeded: spend")
	}
	if scope.limit.DailyTokens > 0 && s.values[scope.keys.dayUsed]+s.values[scope.keys.dayHeld]+estimate.TotalTokens() > scope.limit.DailyTokens {
		return gateway.RateLimited("quota exceeded: daily tokens")
	}
	if scope.limit.MonthlyTokens > 0 && s.values[scope.keys.monthUsed]+s.values[scope.keys.monthHeld]+estimate.TotalTokens() > scope.limit.MonthlyTokens {
		return gateway.RateLimited("quota exceeded: monthly tokens")
	}
	return nil
}

func (s *MemoryQuotaStore) applyScopeHold(scope memoryScopeReservation, estimate gateway.EstimatedUsage, now time.Time, topUp bool) {
	if !topUp {
		s.values[scope.keys.active]++
		s.addCounter(scope.keys.rpm, 1, timeUntilNextMinute(now))
	}
	s.addCounter(scope.keys.tpm, estimate.TotalTokens(), timeUntilNextMinute(now))
	s.values[scope.keys.spendHeld] += estimate.EstimatedSpendMicros
	s.values[scope.keys.dayHeld] += estimate.TotalTokens()
	s.values[scope.keys.monthHeld] += estimate.TotalTokens()
}

func (s *MemoryQuotaStore) counterValue(key string, now time.Time) int64 {
	entry := s.counters[key]
	if entry.Expires.Before(now) {
		return 0
	}
	return entry.Value
}

func (s *MemoryQuotaStore) addCounter(key string, delta int64, ttl time.Duration) {
	now := time.Now()
	entry := s.counters[key]
	if entry.Expires.Before(now) {
		entry.Value = 0
	}
	entry.Value += delta
	entry.Expires = now.Add(ttl)
	s.counters[key] = entry
}

func (s *MemoryQuotaStore) decrValue(key string, delta int64) {
	if key == "" || delta == 0 {
		return
	}
	s.values[key] -= delta
	if s.values[key] < 0 {
		s.values[key] = 0
	}
}

func resolveScopeReservations(scopes []gateway.ScopedLimit, now time.Time) []memoryScopeReservation {
	out := make([]memoryScopeReservation, 0, len(scopes))
	for _, scope := range scopes {
		out = append(out, memoryScopeReservation{
			ref:   scope.Ref,
			limit: scope.Limits,
			keys:  quotaKeysForScope(scope.Ref, scope.Limits, now),
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
	return quotaKeys{
		active:    "quota:" + prefix + ":active",
		rpm:       "quota:" + prefix + ":rpm:" + now.UTC().Format("200601021504"),
		tpm:       "quota:" + prefix + ":tpm:" + now.UTC().Format("200601021504"),
		spendUsed: "quota:" + prefix + ":used:spend:" + budgetBucket(limit.BudgetDuration.Duration, now),
		spendHeld: "quota:" + prefix + ":held:spend",
		dayUsed:   "quota:" + prefix + ":used:day:" + now.UTC().Format("20060102"),
		dayHeld:   "quota:" + prefix + ":held:day:" + now.UTC().Format("20060102"),
		monthUsed: "quota:" + prefix + ":used:month:" + now.UTC().Format("200601"),
		monthHeld: "quota:" + prefix + ":held:month:" + now.UTC().Format("200601"),
	}
}

func budgetBucket(period time.Duration, now time.Time) string {
	if period <= 0 {
		return "forever"
	}
	return now.UTC().Truncate(period).Format(time.RFC3339)
}

func timeUntilNextMinute(now time.Time) time.Duration {
	return time.Until(now.UTC().Truncate(time.Minute).Add(time.Minute))
}
