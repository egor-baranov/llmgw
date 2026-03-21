package store

import (
	"context"
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

type MemoryRateStore struct {
	mu       sync.Mutex
	limiters map[string]*memoryRateEntry
}

type memoryRateEntry struct {
	limiter  *xrate.Limiter
	lastSeen time.Time
}

func NewMemoryRateStore() *MemoryRateStore {
	return &MemoryRateStore{limiters: map[string]*memoryRateEntry{}}
}

func (s *MemoryRateStore) Allow(ctx context.Context, key string, limit RateLimit, n int64) error {
	if limit.Rate <= 0 || n <= 0 {
		return nil
	}
	limiter := s.getLimiter(key, limit)
	if !limiter.AllowN(time.Now(), int(n)) {
		return gateway.RateLimited("rate limit exceeded")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (s *MemoryRateStore) getLimiter(key string, limit RateLimit) *xrate.Limiter {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for candidate, entry := range s.limiters {
		if now.Sub(entry.lastSeen) > 10*time.Minute {
			delete(s.limiters, candidate)
		}
	}

	entry := s.limiters[key]
	if entry == nil || limitChanged(entry.limiter, limit) {
		entry = &memoryRateEntry{limiter: xrate.NewLimiter(per(limit), burst(limit))}
		s.limiters[key] = entry
	}
	entry.lastSeen = now
	return entry.limiter
}

func per(limit RateLimit) xrate.Limit {
	period := limit.Period
	if period <= 0 {
		period = time.Minute
	}
	return xrate.Limit(float64(limit.Rate) / period.Seconds())
}

func burst(limit RateLimit) int {
	if limit.Burst > 0 {
		return int(limit.Burst)
	}
	if limit.Rate > 0 {
		return int(limit.Rate)
	}
	return 1
}

func limitChanged(current *xrate.Limiter, limit RateLimit) bool {
	return current == nil || current.Limit() != per(limit) || current.Burst() != burst(limit)
}
