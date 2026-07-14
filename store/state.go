package store

import (
	"context"
	"time"
)

// HealthChecker reports whether an external store dependency is reachable.
// In-memory stores do not need to implement it.
type HealthChecker interface {
	Ping(ctx context.Context) error
}

// StartupValidator performs a deeper one-time capability check than the
// lightweight recurring health probe. External state stores can use it to
// verify write/script permissions before the data plane starts accepting
// traffic.
type StartupValidator interface {
	ValidateStartup(ctx context.Context) error
}

// SlotState provides shared cross-instance concurrency admission.
type SlotState interface {
	AcquireSlot(ctx context.Context, bucket, token string, limit int64, ttl time.Duration) error
	ReleaseSlot(ctx context.Context, bucket, token string) error
}

// State is the original shared-state contract. It remains stable for custom
// stores; implementations should provide atomic acquire/release and breaker
// updates. New stores can additionally implement OrderedBreakerState so stale
// in-flight completions cannot overwrite newer breaker evidence.
type State interface {
	SlotState
	BreakerAllow(ctx context.Context, route string, now time.Time) (bool, error)
	BreakerFail(ctx context.Context, route string, threshold int, cooldown time.Duration, failureClass string, now time.Time) error
	BreakerSuccess(ctx context.Context, route string, now time.Time) error
}

// OrderedBreakerState records the admission time for each provider attempt.
// Callers must pass that timestamp back with the outcome. Distributed stores
// should source it from their own clock. failureClass must be a bounded,
// non-sensitive classification rather than raw provider error text.
type OrderedBreakerState interface {
	State
	BreakerAllowAttempt(ctx context.Context, route string, retention time.Duration) (allowed bool, startedAt time.Time, err error)
	BreakerFailAttempt(ctx context.Context, route string, startedAt time.Time, threshold int, cooldown time.Duration, failureClass string) error
	BreakerSuccessAttempt(ctx context.Context, route string, startedAt time.Time) error
}
