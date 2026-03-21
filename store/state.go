package store

import (
	"context"
	"time"
)

// State provides shared cross-instance state for attempt-level controls.
// Implementations should provide atomic semantics for acquire/release and breaker updates.
type State interface {
	AcquireSlot(ctx context.Context, bucket, token string, limit int64, ttl time.Duration) error
	ReleaseSlot(ctx context.Context, bucket, token string) error
	BreakerAllow(ctx context.Context, route string, now time.Time) (bool, error)
	BreakerFail(ctx context.Context, route string, threshold int, cooldown time.Duration, message string, now time.Time) error
	BreakerSuccess(ctx context.Context, route string, now time.Time) error
}
