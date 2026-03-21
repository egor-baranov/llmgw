package policy

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"llmgw/gateway"
	"llmgw/store"
)

type AttemptHeaders struct{}

func (AttemptHeaders) WrapAttempt(next gateway.AttemptHandler) gateway.AttemptHandler {
	return func(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt) (*gateway.Result, error) {
		attempt.Route.SetHeader("X-LLMGW-Request-ID", state.Request.Meta.RequestID)
		attempt.Route.SetHeader("X-LLMGW-Attempt", strconv.Itoa(attempt.Number))
		attempt.Route.SetHeader("X-LLMGW-Attempt-ID", attempt.ID)
		attempt.Route.SetHeader("X-LLMGW-Provider", attempt.Route.Route.Provider)
		attempt.Route.SetHeader("X-LLMGW-Route", attempt.Route.Route.Name)
		attempt.Route.SetHeader("X-LLMGW-Model", attempt.Route.Route.Model)
		if attempt.Route.Route.Provider == "gemini" {
			attempt.Route.SetHeader("x-goog-api-client", "llmgw/1.0 gateway/aggregator")
		}
		if state.Request.Meta.User != "" {
			attempt.Route.SetHeader("X-LLMGW-User", state.Request.Meta.User)
		}
		if state.Request.Meta.Project != "" {
			attempt.Route.SetHeader("X-LLMGW-Project", state.Request.Meta.Project)
		}
		return next(ctx, state, attempt)
	}
}

type AttemptLimits struct {
	Rates    store.RateStore
	Breakers *Breaker
	State    store.State
	limiters sync.Map
}

func (l *AttemptLimits) WrapAttempt(next gateway.AttemptHandler) gateway.AttemptHandler {
	return func(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt) (*gateway.Result, error) {
		route := attempt.Route.Route
		allowed, err := l.breakerAllow(ctx, route)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, gateway.NewError(http.StatusServiceUnavailable, "server_error", "circuit_open", "route circuit is open")
		}
		release, err := l.acquire(ctx, state, attempt, route)
		if err != nil {
			return nil, err
		}
		defer release()
		if err := l.checkRates(ctx, state, attempt, route); err != nil {
			return nil, err
		}
		tries := route.Retries + 1
		var lastErr error
		for i := 0; i < tries; i++ {
			timeoutCtx, cancel := context.WithTimeout(ctx, route.Timeout.Duration)
			result, err := next(timeoutCtx, state, attempt)
			cancel()
			if err == nil {
				l.breakerSuccess(ctx, route)
				return result, nil
			}
			lastErr = err
			if !retryable(err) || i == tries-1 {
				l.breakerFail(ctx, route, err)
				return nil, err
			}
		}
		return nil, lastErr
	}
}

func (l *AttemptLimits) acquire(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt, route *gateway.Route) (func(), error) {
	if l.State != nil {
		token := attempt.ID
		if token == "" && state != nil && state.Request != nil {
			token = state.Request.Meta.RequestID + ":" + route.Name
		}
		if token == "" {
			token = route.Name + ":" + strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		ttl := distributedSlotTTL(route)
		releaseRoute, err := l.acquireDistributedLimiter(ctx, "route:"+route.Name, token, route.Limits.Concurrency, ttl, "concurrency limit exceeded")
		if err != nil {
			return nil, err
		}
		releaseProvider, err := l.acquireDistributedLimiter(ctx, "provider:"+route.Provider, token, route.Limits.ProviderConcurrency, ttl, "provider concurrency limit exceeded")
		if err != nil {
			releaseRoute()
			return nil, err
		}
		return func() {
			releaseProvider()
			releaseRoute()
		}, nil
	}
	releaseRoute, err := l.acquireLimiter("route:"+route.Name, route.Limits.Concurrency, "concurrency limit exceeded")
	if err != nil {
		return nil, err
	}
	releaseProvider, err := l.acquireLimiter("provider:"+route.Provider, route.Limits.ProviderConcurrency, "provider concurrency limit exceeded")
	if err != nil {
		releaseRoute()
		return nil, err
	}
	return func() {
		releaseProvider()
		releaseRoute()
	}, nil
}

func (l *AttemptLimits) acquireDistributedLimiter(ctx context.Context, key, token string, limit int64, ttl time.Duration, message string) (func(), error) {
	if limit <= 0 {
		return func() {}, nil
	}
	if err := l.State.AcquireSlot(ctx, key, token, limit, ttl); err != nil {
		if gateway.AsAPIError(err).Status == http.StatusTooManyRequests {
			return nil, gateway.RateLimited(message)
		}
		return nil, gateway.WrapError(http.StatusServiceUnavailable, "server_error", "distributed_limiter_unavailable", err)
	}
	return func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = l.State.ReleaseSlot(releaseCtx, key, token)
	}, nil
}

func (l *AttemptLimits) acquireLimiter(key string, limit int64, message string) (func(), error) {
	if limit <= 0 {
		return func() {}, nil
	}
	ptr, _ := l.limiters.LoadOrStore(key, &limiter{})
	entry := ptr.(*limiter)
	if !entry.acquire(limit) {
		return nil, gateway.RateLimited(message)
	}
	return entry.release, nil
}

func (l *AttemptLimits) checkRates(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt, route *gateway.Route) error {
	if l.Rates == nil {
		return nil
	}
	if route.Limits.RPM > 0 {
		if err := l.Rates.Allow(ctx, "rpm:"+route.Name, store.RateLimit{
			Rate:   route.Limits.RPM,
			Burst:  route.Limits.RPM,
			Period: time.Minute,
		}, 1); err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return err
			}
			if apiErr := gateway.AsAPIError(err); apiErr.Status != http.StatusTooManyRequests {
				return err
			}
			return gateway.RateLimited("rpm limit exceeded")
		}
	}
	if route.Limits.TPM > 0 {
		delta := attempt.Route.Estimate.TotalTokens
		if delta == 0 {
			delta = state.Estimate.TotalTokens
		}
		if delta == 0 {
			delta = attempt.Route.Estimate.InputTokens + attempt.Route.Estimate.OutputTokens
		}
		if delta == 0 {
			delta = state.Estimate.InputTokens + state.Estimate.OutputTokens
		}
		if delta == 0 {
			delta = 1
		}
		if err := l.Rates.Allow(ctx, "tpm:"+route.Name, store.RateLimit{
			Rate:   route.Limits.TPM,
			Burst:  route.Limits.TPM,
			Period: time.Minute,
		}, delta); err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return err
			}
			if apiErr := gateway.AsAPIError(err); apiErr.Status != http.StatusTooManyRequests {
				return err
			}
			return gateway.RateLimited("tpm limit exceeded")
		}
	}
	return nil
}

func (l *AttemptLimits) breakerAllow(ctx context.Context, route *gateway.Route) (bool, error) {
	if l.State != nil {
		allowed, err := l.State.BreakerAllow(ctx, route.Name, time.Now())
		if err != nil {
			return false, gateway.WrapError(http.StatusServiceUnavailable, "server_error", "breaker_unavailable", err)
		}
		return allowed, nil
	}
	if l.Breakers != nil {
		return l.Breakers.Allow(route), nil
	}
	return true, nil
}

func (l *AttemptLimits) breakerSuccess(ctx context.Context, route *gateway.Route) {
	if l.State != nil {
		_ = l.State.BreakerSuccess(ctx, route.Name, time.Now())
		return
	}
	if l.Breakers != nil {
		l.Breakers.Success(route)
	}
}

func (l *AttemptLimits) breakerFail(ctx context.Context, route *gateway.Route, err error) {
	if l.State != nil {
		message := ""
		if err != nil {
			message = err.Error()
		}
		_ = l.State.BreakerFail(ctx, route.Name, route.Circuit.Failures, route.Circuit.Cooldown.Duration, message, time.Now())
		return
	}
	if l.Breakers != nil {
		l.Breakers.Fail(route, err)
	}
}

func distributedSlotTTL(route *gateway.Route) time.Duration {
	if route == nil {
		return 30 * time.Second
	}
	timeout := route.Timeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	tries := route.Retries + 1
	if tries < 1 {
		tries = 1
	}
	return timeout*time.Duration(tries) + 2*time.Second
}

type Breaker struct {
	mu     sync.Mutex
	states map[string]breakerState
}

type breakerState struct {
	Failures    int
	OpenUntil   time.Time
	LastFailure time.Time
	LastSuccess time.Time
	LastError   string
}

func NewBreaker() *Breaker {
	return &Breaker{states: map[string]breakerState{}}
}

func (b *Breaker) Allow(route *gateway.Route) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.states[route.Name]
	return state.OpenUntil.Before(time.Now())
}

func (b *Breaker) Fail(route *gateway.Route, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.states[route.Name]
	state.Failures++
	state.LastFailure = time.Now()
	if err != nil {
		state.LastError = err.Error()
	}
	if state.Failures >= route.Circuit.Failures {
		state.OpenUntil = time.Now().Add(route.Circuit.Cooldown.Duration)
		state.Failures = 0
	}
	b.states[route.Name] = state
}

func (b *Breaker) Success(route *gateway.Route) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.states[route.Name]
	state.Failures = 0
	state.OpenUntil = time.Time{}
	state.LastSuccess = time.Now()
	state.LastError = ""
	b.states[route.Name] = state
}

type limiter struct {
	mu      sync.Mutex
	current int64
	limit   int64
}

func (l *limiter) acquire(limit int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limit == 0 || limit < l.limit {
		l.limit = limit
	}
	if l.current >= l.limit {
		return false
	}
	l.current++
	return true
}

func (l *limiter) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.current > 0 {
		l.current--
	}
}

func retryable(err error) bool {
	if err == nil {
		return false
	}
	if apiErr, ok := err.(*gateway.APIError); ok {
		return apiErr.Status >= 500
	}
	return true
}
