package policy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"llmgw/gateway"
	"llmgw/store"
)

type AttemptHeaders struct{}

func (AttemptHeaders) WrapAttempt(next gateway.AttemptHandler) gateway.AttemptHandler {
	return func(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt) (*gateway.Result, error) {
		attempt.Route.SetHeader("X-LLMGW-Request-ID", state.Request.Meta.RequestID)
		attempt.Route.SetHeader("X-LLMGW-Attempt", strconv.Itoa(attempt.Number))
		attempt.Route.SetHeader("X-LLMGW-Retry", strconv.Itoa(attempt.Retry))
		attempt.Route.SetHeader("X-LLMGW-Attempt-ID", attempt.ID)
		attempt.Route.SetHeader("X-LLMGW-Provider", attempt.Route.Route.Provider)
		attempt.Route.SetHeader("X-LLMGW-Route", attempt.Route.Route.Name)
		attempt.Route.SetHeader("X-LLMGW-Model", attempt.Route.Route.Model)
		if state.Request.Meta.User != "" && safeForwardedIdentity(state.Request.Meta.User) {
			attempt.Route.SetHeader("X-LLMGW-User", state.Request.Meta.User)
		}
		if state.Request.Meta.Project != "" && safeForwardedIdentity(state.Request.Meta.Project) {
			attempt.Route.SetHeader("X-LLMGW-Project", state.Request.Meta.Project)
		}
		return next(ctx, state, attempt)
	}
}

type AttemptLimits struct {
	Rates                store.RateStore
	Breakers             *Breaker
	State                store.State
	OnBreakerUpdateError func(operation string, err error)

	limiters          sync.Map
	limiterOperations atomic.Uint64
	now               func() time.Time
}

const (
	localPolicyStateIdleTTL = 10 * time.Minute
	localPolicyPruneEvery   = 256
)

func (l *AttemptLimits) WrapAttempt(next gateway.AttemptHandler) gateway.AttemptHandler {
	return func(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt) (*gateway.Result, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		route := attempt.Route.Route
		allowed, breakerStartedAt, err := l.breakerAllow(ctx, route)
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
		streamOwnsLifecycle := false
		defer func() {
			if !streamOwnsLifecycle {
				release()
			}
		}()
		tries := routeAttemptCount(route)
		var lastErr error
		for i := 0; i < tries; i++ {
			if i > 0 {
				allowed, breakerStartedAt, err = l.breakerAllow(ctx, route)
				if err != nil {
					return nil, err
				}
				if !allowed {
					return nil, gateway.NewError(http.StatusServiceUnavailable, "server_error", "circuit_open", "route circuit is open")
				}
			}
			callAttempt := *attempt
			callAttempt.Retry = i
			callAttempt.ID = attempt.ID + ":retry:" + strconv.Itoa(i)
			callAttempt.StartedAt = breakerStartedAt
			// Authorize this real provider call before consuming shared route
			// rate capacity. The engine repeats this idempotently at its final
			// dispatch gate for custom attempt pipelines.
			if err := state.ReserveProviderAttempt(ctx, &callAttempt); err != nil {
				return nil, err
			}
			// RPM and TPM account for actual upstream calls, including retries.
			if err := l.checkRates(ctx, state, &callAttempt, route); err != nil {
				state.ReleaseProviderAttemptReservation(callAttempt.ID)
				return nil, err
			}
			timeoutCtx, cancel := context.WithTimeout(ctx, route.Timeout.Duration)
			result, err := next(timeoutCtx, state, &callAttempt)
			if err == nil {
				if result != nil && result.RawStream != nil {
					stream := &attemptLifecycleStream{
						ReadCloser: result.RawStream,
						ctx:        timeoutCtx,
						finish: func(outcome streamOutcome, streamErr error) {
							cancel()
							release()
							switch outcome {
							case streamSucceeded:
								l.breakerSuccessDetached(ctx, route, callAttempt.StartedAt)
							case streamFailed:
								if circuitFailure(streamErr, nil) {
									l.breakerFailDetached(ctx, route, callAttempt.StartedAt, streamErr)
								}
							}
						},
					}
					stream.watchContext()
					result.RawStream = stream
					streamOwnsLifecycle = true
					return result, nil
				}
				cancel()
				l.breakerSuccess(ctx, route, callAttempt.StartedAt)
				return result, nil
			}
			callCtxErr := timeoutCtx.Err()
			cancel()
			lastErr = err
			if gateway.AttemptUnbillable(err) {
				state.ReleaseProviderAttemptReservation(callAttempt.ID)
			}
			if circuitFailure(err, callCtxErr) {
				l.breakerFail(ctx, route, callAttempt.StartedAt, err)
			}
			if !retryable(err, callCtxErr) || i == tries-1 {
				return nil, err
			}
		}
		return nil, lastErr
	}
}

type streamOutcome uint8

const (
	streamStopped streamOutcome = iota
	streamSucceeded
	streamFailed
)

type attemptLifecycleStream struct {
	io.ReadCloser
	ctx       context.Context
	finish    func(streamOutcome, error)
	once      sync.Once
	closeOnce sync.Once
	closeErr  error
	sawBytes  atomic.Bool
}

func (s *attemptLifecycleStream) watchContext() {
	go func() {
		<-s.ctx.Done()
		err := s.closeUnderlying()
		if errors.Is(s.ctx.Err(), context.DeadlineExceeded) {
			s.complete(streamFailed, firstStreamError(err, context.DeadlineExceeded))
			return
		}
		s.complete(streamStopped, err)
	}()
}

func (s *attemptLifecycleStream) Read(p []byte) (int, error) {
	n, err := s.ReadCloser.Read(p)
	if n > 0 {
		s.sawBytes.Store(true)
	}
	if err == nil {
		return n, nil
	}
	if errors.Is(err, io.EOF) {
		if s.sawBytes.Load() {
			s.complete(streamSucceeded, nil)
		} else {
			s.complete(streamFailed, io.ErrUnexpectedEOF)
		}
		return n, err
	}
	if errors.Is(s.ctx.Err(), context.Canceled) {
		s.complete(streamStopped, err)
	} else {
		s.complete(streamFailed, err)
	}
	return n, err
}

func (s *attemptLifecycleStream) Close() error {
	err := s.closeUnderlying()
	// Closing before EOF normally means the downstream client stopped reading;
	// release capacity without treating that client action as a route failure.
	s.complete(streamStopped, err)
	return err
}

func (s *attemptLifecycleStream) closeUnderlying() error {
	s.closeOnce.Do(func() { s.closeErr = s.ReadCloser.Close() })
	return s.closeErr
}

func firstStreamError(values ...error) error {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return io.ErrUnexpectedEOF
}

func (s *attemptLifecycleStream) complete(outcome streamOutcome, err error) {
	s.once.Do(func() {
		if s.finish != nil {
			s.finish(outcome, err)
		}
	})
}

func (l *AttemptLimits) acquire(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt, route *gateway.Route) (func(), error) {
	if l.State != nil {
		token := attempt.ID
		if token == "" && state != nil && state.Request != nil {
			token = state.Request.Meta.ExecutionID
			if token == "" {
				token = state.Request.Meta.RequestID
			}
			if token != "" {
				token += ":" + route.Name
			}
		}
		if token == "" {
			token = route.Name + ":" + strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		ttl := routeAttemptStateTTL(route)
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
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil, err
		}
		if gateway.AsAPIError(err).Status == http.StatusTooManyRequests {
			return nil, gateway.RateLimited(message)
		}
		return nil, gateway.NewError(
			http.StatusServiceUnavailable,
			"server_error",
			"distributed_limiter_unavailable",
			"concurrency limit service is temporarily unavailable",
		).WithCause(err).WithDisposition(false, false, false)
	}
	renewCtx, stopRenewal := context.WithCancel(context.Background())
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		interval := ttl / 3
		if interval < time.Second {
			interval = time.Second
		}
		if interval > 10*time.Second {
			interval = 10 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				renewCallCtx, cancel := context.WithTimeout(renewCtx, 2*time.Second)
				_ = l.State.AcquireSlot(renewCallCtx, key, token, limit, ttl)
				cancel()
			}
		}
	}()
	return func() {
		stopRenewal()
		<-renewDone
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = l.State.ReleaseSlot(releaseCtx, key, token)
	}, nil
}

func (l *AttemptLimits) acquireLimiter(key string, limit int64, message string) (func(), error) {
	if limit <= 0 {
		return func() {}, nil
	}
	now := l.currentTime()
	if l.limiterOperations.Add(1)%localPolicyPruneEvery == 0 {
		l.pruneLimiters(now)
	}
	for {
		value, _ := l.limiters.LoadOrStore(key, &limiter{})
		entry := value.(*limiter)
		acquired, retired := entry.acquire(limit, now)
		if retired {
			l.limiters.CompareAndDelete(key, entry)
			continue
		}
		if !acquired {
			return nil, gateway.RateLimited(message)
		}
		return func() { entry.release(l.currentTime()) }, nil
	}
}

func (l *AttemptLimits) currentTime() time.Time {
	if l != nil && l.now != nil {
		return l.now()
	}
	return time.Now()
}

func (l *AttemptLimits) pruneLimiters(now time.Time) {
	cutoff := now.Add(-localPolicyStateIdleTTL)
	l.limiters.Range(func(key, value any) bool {
		entry := value.(*limiter)
		if entry.retireIfIdle(cutoff) {
			l.limiters.CompareAndDelete(key, entry)
		}
		return true
	})
}

func (l *AttemptLimits) checkRates(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt, route *gateway.Route) error {
	if l.Rates == nil {
		return nil
	}
	requests := make([]store.RateRequest, 0, 2)
	if route.Limits.RPM > 0 {
		requests = append(requests, store.RateRequest{Key: "rpm:" + route.Name, Limit: store.RateLimit{
			Rate:   route.Limits.RPM,
			Burst:  route.Limits.RPM,
			Period: time.Minute,
		}, N: 1})
	}
	if route.Limits.TPM > 0 {
		delta := safeUsageTotal(attempt.Route.Estimate)
		if delta <= 0 && state != nil {
			delta = safeUsageTotal(state.Estimate)
		}
		if delta <= 0 {
			delta = 1
		}
		requests = append(requests, store.RateRequest{Key: "tpm:" + route.Name, Limit: store.RateLimit{
			Rate:   route.Limits.TPM,
			Burst:  route.Limits.TPM,
			Period: time.Minute,
		}, N: delta})
	}
	if len(requests) == 0 {
		return nil
	}
	batchStore, supportsBatch := l.Rates.(store.BatchRateStore)
	if !supportsBatch {
		for _, request := range requests {
			if err := l.Rates.Allow(ctx, request.Key, request.Limit, request.N); err != nil {
				return rateAdmissionError(ctx, err, rateKind(request.Key))
			}
		}
		return nil
	}
	if err := batchStore.AllowBatch(ctx, requests); err != nil {
		kind := "route"
		if len(requests) == 1 {
			kind = rateKind(requests[0].Key)
		}
		var rejected *store.BatchRateLimitError
		if errors.As(err, &rejected) {
			kind = rateKind(rejected.Key)
		}
		return rateAdmissionError(ctx, err, kind)
	}
	return nil
}

func rateKind(key string) string {
	if strings.HasPrefix(key, "rpm:") {
		return "rpm"
	}
	if strings.HasPrefix(key, "tpm:") {
		return "tpm"
	}
	return "route"
}

func rateAdmissionError(ctx context.Context, err error, kind string) error {
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return err
	}
	if apiErr := gateway.AsAPIError(err); apiErr.Status == http.StatusTooManyRequests {
		return gateway.RateLimited(kind + " limit exceeded")
	}
	// A shared limiter outage is not candidate-local. Failing terminally avoids
	// amplifying the outage by trying every route while still failing closed.
	return gateway.NewError(
		http.StatusServiceUnavailable,
		"server_error",
		"rate_store_unavailable",
		"rate limit service is temporarily unavailable",
	).WithCause(err).WithDisposition(false, false, false)
}

func safeUsageTotal(usage gateway.Usage) int64 {
	combined := saturatingAdd(usage.InputTokens, usage.OutputTokens)
	if usage.TotalTokens > combined {
		return usage.TotalTokens
	}
	return combined
}

func (l *AttemptLimits) breakerAllow(ctx context.Context, route *gateway.Route) (bool, time.Time, error) {
	if l.State != nil {
		key := routeBreakerKey(route)
		startedAt := l.currentTime()
		var (
			allowed bool
			err     error
		)
		if ordered, ok := l.State.(store.OrderedBreakerState); ok {
			allowed, startedAt, err = ordered.BreakerAllowAttempt(ctx, key, routeAttemptStateTTL(route))
		} else {
			allowed, err = l.State.BreakerAllow(ctx, key, startedAt)
		}
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return false, time.Time{}, err
			}
			return false, time.Time{}, gateway.NewError(
				http.StatusServiceUnavailable,
				"server_error",
				"breaker_unavailable",
				"circuit breaker service is temporarily unavailable",
			).WithCause(err).WithDisposition(false, false, false)
		}
		return allowed, startedAt, nil
	}
	if l.Breakers != nil {
		allowed, startedAt := l.Breakers.AllowAttempt(route)
		return allowed, startedAt, nil
	}
	return true, l.currentTime(), nil
}

func (l *AttemptLimits) breakerSuccess(ctx context.Context, route *gateway.Route, startedAt time.Time) {
	if l.State != nil {
		key := routeBreakerKey(route)
		var err error
		if ordered, ok := l.State.(store.OrderedBreakerState); ok {
			err = ordered.BreakerSuccessAttempt(ctx, key, startedAt)
		} else {
			err = l.State.BreakerSuccess(ctx, key, l.currentTime())
		}
		if err != nil && l.OnBreakerUpdateError != nil {
			l.OnBreakerUpdateError("success", err)
		}
		return
	}
	if l.Breakers != nil {
		l.Breakers.SuccessAttempt(route, startedAt)
	}
}

func (l *AttemptLimits) breakerFail(ctx context.Context, route *gateway.Route, startedAt time.Time, err error) {
	if l.State != nil {
		key := routeBreakerKey(route)
		var updateErr error
		if ordered, ok := l.State.(store.OrderedBreakerState); ok {
			updateErr = ordered.BreakerFailAttempt(ctx, key, startedAt, route.Circuit.Failures, route.Circuit.Cooldown.Duration, "provider_failure")
		} else {
			updateErr = l.State.BreakerFail(ctx, key, route.Circuit.Failures, route.Circuit.Cooldown.Duration, "provider_failure", l.currentTime())
		}
		if updateErr != nil && l.OnBreakerUpdateError != nil {
			l.OnBreakerUpdateError("failure", updateErr)
		}
		return
	}
	if l.Breakers != nil {
		l.Breakers.FailAttempt(route, startedAt, err)
	}
}

func (l *AttemptLimits) breakerSuccessDetached(ctx context.Context, route *gateway.Route, startedAt time.Time) {
	detached, cancel := detachedAttemptContext(ctx)
	defer cancel()
	l.breakerSuccess(detached, route, startedAt)
}

func (l *AttemptLimits) breakerFailDetached(ctx context.Context, route *gateway.Route, startedAt time.Time, err error) {
	detached, cancel := detachedAttemptContext(ctx)
	defer cancel()
	l.breakerFail(detached, route, startedAt, err)
}

func detachedAttemptContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
}

// routeAttemptStateTTL keeps distributed leases and ordering evidence alive
// for every configured retry plus a cleanup buffer.
func routeAttemptStateTTL(route *gateway.Route) time.Duration {
	if route == nil {
		return 30 * time.Second
	}
	timeout := route.Timeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	tries := routeAttemptCount(route)
	const releaseBuffer = 30 * time.Second
	if timeout > (time.Duration(math.MaxInt64)-releaseBuffer)/time.Duration(tries) {
		return time.Duration(math.MaxInt64)
	}
	return timeout*time.Duration(tries) + releaseBuffer
}

func routeAttemptCount(route *gateway.Route) int {
	if route == nil || route.Retries <= 0 {
		return 1
	}
	retries := min(route.Retries, gateway.MaximumRouteRetries)
	return retries + 1
}

type Breaker struct {
	mu         sync.Mutex
	states     map[string]breakerState
	operations uint64
	now        func() time.Time
}

type breakerState struct {
	Failures      int
	OpenUntil     time.Time
	RetainUntil   time.Time
	LastAdmission time.Time
	LastFailure   time.Time
	LastSuccess   time.Time
}

func NewBreaker() *Breaker {
	return &Breaker{states: map[string]breakerState{}, now: time.Now}
}

func routeBreakerKey(route *gateway.Route) string {
	if route == nil {
		return "unknown"
	}
	identity := strings.Join([]string{
		route.Provider,
		route.Backend,
		route.BaseURL,
		route.UpstreamModel,
		route.Model,
		route.APIVersion,
	}, "\x00")
	sum := sha256.Sum256([]byte(identity))
	return route.Name + ":" + hex.EncodeToString(sum[:8])
}

// Deprecated: use AllowAttempt and pass its timestamp to the matching outcome.
func (b *Breaker) Allow(route *gateway.Route) bool {
	allowed, _ := b.AllowAttempt(route)
	return allowed
}

// AllowAttempt returns the local breaker's admission time. The same value must
// accompany the eventual outcome so a completion from an older in-flight call
// cannot erase newer failure/success evidence.
func (b *Breaker) AllowAttempt(route *gateway.Route) (bool, time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.currentTime()
	key := routeBreakerKey(route)
	state := b.states[key]
	if state.OpenUntil.After(now) {
		return false, now
	}
	startedAt := now
	if !state.LastAdmission.IsZero() && !startedAt.After(state.LastAdmission) {
		startedAt = state.LastAdmission.Add(time.Nanosecond)
	}
	state.LastAdmission = startedAt
	// Keep newer success/failure evidence for at least the maximum lifetime of
	// an admitted call. Otherwise idle-state pruning could forget that evidence
	// while a long-running older call is still in flight, allowing its stale
	// completion to corrupt the circuit state.
	retainUntil := now.Add(routeAttemptStateTTL(route))
	if retainUntil.After(state.RetainUntil) {
		state.RetainUntil = retainUntil
	}
	b.states[key] = state
	return true, startedAt
}

// Deprecated: use FailAttempt with the timestamp returned by AllowAttempt.
func (b *Breaker) Fail(route *gateway.Route, err error) {
	b.FailAttempt(route, b.currentTime(), err)
}

func (b *Breaker) FailAttempt(route *gateway.Route, startedAt time.Time, _ error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.currentTime()
	b.pruneStatesLocked(now)
	if startedAt.IsZero() {
		startedAt = now
	}
	key := routeBreakerKey(route)
	state := b.states[key]
	// A failure from a call admitted before a newer success is stale evidence.
	if !state.LastSuccess.IsZero() && !startedAt.After(state.LastSuccess) {
		return
	}
	if startedAt.After(state.LastFailure) {
		state.LastFailure = startedAt
	}
	// Calls already in flight when the circuit opened may still fail. Record the
	// newer evidence for stale-success protection, but do not keep extending the
	// current cooldown. This matches the Redis implementation.
	if state.OpenUntil.After(now) {
		b.states[key] = state
		return
	}
	threshold := route.Circuit.Failures
	if threshold <= 0 {
		threshold = 1
	}
	cooldown := route.Circuit.Cooldown.Duration
	if cooldown <= 0 {
		cooldown = time.Second
	}
	state.Failures++
	if state.Failures >= threshold {
		state.OpenUntil = now.Add(cooldown)
		state.Failures = 0
	}
	b.states[key] = state
}

// Deprecated: use SuccessAttempt with the timestamp returned by AllowAttempt.
func (b *Breaker) Success(route *gateway.Route) {
	b.SuccessAttempt(route, b.currentTime())
}

func (b *Breaker) SuccessAttempt(route *gateway.Route, startedAt time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.currentTime()
	b.pruneStatesLocked(now)
	if startedAt.IsZero() {
		startedAt = now
	}
	key := routeBreakerKey(route)
	state := b.states[key]
	// Ignore a success from a call admitted before the latest failure. A success
	// from a newer admission is newer evidence even if it completes while the
	// older failure's cooldown is still active.
	if !state.LastFailure.IsZero() && !startedAt.After(state.LastFailure) {
		return
	}
	state.Failures = 0
	state.OpenUntil = time.Time{}
	if startedAt.After(state.LastSuccess) {
		state.LastSuccess = startedAt
	}
	b.states[key] = state
}

func (b *Breaker) currentTime() time.Time {
	if b != nil && b.now != nil {
		return b.now()
	}
	return time.Now()
}

func (b *Breaker) pruneStatesLocked(now time.Time) {
	if b.states == nil {
		b.states = make(map[string]breakerState)
	}
	b.operations++
	if b.operations%localPolicyPruneEvery != 0 {
		return
	}
	cutoff := now.Add(-localPolicyStateIdleTTL)
	for key, state := range b.states {
		if state.OpenUntil.After(now) || state.RetainUntil.After(now) {
			continue
		}
		lastSeen := state.LastFailure
		if state.LastSuccess.After(lastSeen) {
			lastSeen = state.LastSuccess
		}
		if lastSeen.IsZero() || !lastSeen.After(cutoff) {
			delete(b.states, key)
		}
	}
}

type limiter struct {
	mu       sync.Mutex
	current  int64
	limit    int64
	lastSeen time.Time
	retired  bool
}

func (l *limiter) acquire(limit int64, now time.Time) (acquired, retired bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.retired {
		return false, true
	}
	// Config snapshots are hot-swappable. Always apply the current snapshot's
	// limit so raising a limit does not require a process restart. If a reload
	// lowers the limit below current usage, new acquisitions remain blocked
	// until enough existing calls release their slots.
	l.limit = limit
	l.lastSeen = now
	if l.current >= l.limit {
		return false, false
	}
	l.current++
	return true, false
}

func (l *limiter) release(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.current > 0 {
		l.current--
	}
	l.lastSeen = now
}

func (l *limiter) retireIfIdle(cutoff time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.current != 0 || l.lastSeen.After(cutoff) {
		return false
	}
	l.retired = true
	return true
}

func retryable(err, callCtxErr error) bool {
	if err == nil {
		return false
	}
	if errors.Is(callCtxErr, context.Canceled) {
		return false
	}
	if errors.Is(callCtxErr, context.DeadlineExceeded) {
		return true
	}
	var apiErr *gateway.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Retryable
	}
	return true
}

func circuitFailure(err, callCtxErr error) bool {
	if err == nil || errors.Is(callCtxErr, context.Canceled) {
		return false
	}
	if errors.Is(callCtxErr, context.DeadlineExceeded) {
		return true
	}
	var apiErr *gateway.APIError
	if errors.As(err, &apiErr) {
		return apiErr.CircuitFailure
	}
	return !errors.Is(err, context.Canceled)
}
