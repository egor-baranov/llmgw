package store

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"llmgw/gateway"

	xrate "golang.org/x/time/rate"
)

type RateLimit struct {
	Rate   int64
	Burst  int64
	Period time.Duration
}

type RateStore interface {
	Allow(ctx context.Context, key string, limit RateLimit, n int64) error
}

// BatchRateStore can apply several rate debits atomically. Attempt policy uses
// this capability when available; RateStore remains source-compatible with
// custom stores that predate batch admission.
type BatchRateStore interface {
	RateStore
	// AllowBatch must apply every active debit atomically or leave every
	// bucket unchanged.
	AllowBatch(ctx context.Context, requests []RateRequest) error
}

// RateRequest is one independently configured rate-limit debit. RateStore
// implementations either apply every batch debit or leave every bucket
// unchanged. The attempt policy uses this for route RPM+TPM admission so a TPM
// rejection cannot consume RPM capacity.
type RateRequest struct {
	Key   string
	Limit RateLimit
	N     int64
}

// BatchRateLimitError identifies the bucket that rejected an atomic batch.
type BatchRateLimitError struct {
	Key string
}

func (e *BatchRateLimitError) Error() string { return "rate limit exceeded" }

func (e *BatchRateLimitError) Unwrap() error {
	return gateway.RateLimited("rate limit exceeded")
}

type MemoryRateStore struct {
	mu         sync.Mutex
	limiters   map[string]*memoryRateEntry
	operations uint64
}

type memoryRateEntry struct {
	limiter  *xrate.Limiter
	lastSeen time.Time
}

const (
	memoryRateEntryIdleTTL = 10 * time.Minute
	memoryRatePruneEvery   = 16
	memoryRatePruneLimit   = 64
)

func NewMemoryRateStore() *MemoryRateStore {
	return &MemoryRateStore{limiters: map[string]*memoryRateEntry{}}
}

func (s *MemoryRateStore) Allow(ctx context.Context, key string, limit RateLimit, n int64) error {
	return s.AllowBatch(ctx, []RateRequest{{Key: key, Limit: limit, N: n}})
}

func (s *MemoryRateStore) AllowBatch(ctx context.Context, requests []RateRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	requests, err := normalizeRateRequests(requests)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	s.pruneLocked(now)
	reservations := make([]*xrate.Reservation, 0, len(requests))
	rollback := func() {
		for i := len(reservations) - 1; i >= 0; i-- {
			reservations[i].CancelAt(now)
		}
	}
	for _, request := range requests {
		if request.Limit.Rate <= 0 || request.N <= 0 {
			continue
		}
		limiter := s.getLimiterLocked(request.Key, request.Limit, now)
		reservation := limiter.ReserveN(now, int(request.N))
		if !reservation.OK() || reservation.DelayFrom(now) > 0 {
			if reservation.OK() {
				reservation.CancelAt(now)
			}
			rollback()
			return &BatchRateLimitError{Key: request.Key}
		}
		reservations = append(reservations, reservation)
	}
	if err := ctx.Err(); err != nil {
		rollback()
		return err
	}
	return nil
}

func normalizeRateRequests(requests []RateRequest) ([]RateRequest, error) {
	normalized := make([]RateRequest, 0, len(requests))
	indices := make(map[string]int, len(requests))
	for _, request := range requests {
		if request.Limit.Rate <= 0 || request.N <= 0 {
			continue
		}
		if index, exists := indices[request.Key]; exists {
			if normalized[index].Limit != request.Limit {
				return nil, fmt.Errorf("duplicate rate key %q has inconsistent limits", request.Key)
			}
			if normalized[index].N > math.MaxInt64-request.N {
				normalized[index].N = math.MaxInt64
			} else {
				normalized[index].N += request.N
			}
			continue
		}
		indices[request.Key] = len(normalized)
		normalized = append(normalized, request)
	}
	return normalized, nil
}

func (s *MemoryRateStore) pruneLocked(now time.Time) {
	s.operations++
	if s.operations%memoryRatePruneEvery != 0 {
		return
	}
	inspected := 0
	for candidate, entry := range s.limiters {
		if inspected == memoryRatePruneLimit {
			break
		}
		inspected++
		if now.Sub(entry.lastSeen) > memoryRateEntryIdleTTL &&
			(entry.limiter == nil || entry.limiter.TokensAt(now) >= float64(entry.limiter.Burst())) {
			delete(s.limiters, candidate)
		}
	}
}

func (s *MemoryRateStore) getLimiterLocked(key string, limit RateLimit, now time.Time) *xrate.Limiter {
	entry := s.limiters[key]
	if entry == nil || limitChanged(entry.limiter, limit) {
		entry = &memoryRateEntry{limiter: xrate.NewLimiter(ratePerSecond(limit), burstSize(limit))}
		s.limiters[key] = entry
	}
	entry.lastSeen = now
	return entry.limiter
}

func ratePerSecond(limit RateLimit) xrate.Limit {
	period := limit.Period
	if period <= 0 {
		period = time.Minute
	}
	return xrate.Limit(float64(limit.Rate) / period.Seconds())
}

func burstSize(limit RateLimit) int {
	if limit.Burst > 0 {
		return int(limit.Burst)
	}
	if limit.Rate > 0 {
		return int(limit.Rate)
	}
	return 1
}

func limitChanged(current *xrate.Limiter, limit RateLimit) bool {
	return current == nil || current.Limit() != ratePerSecond(limit) || current.Burst() != burstSize(limit)
}
