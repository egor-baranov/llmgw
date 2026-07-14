package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Provider interface {
	Name() string
	Supports(op Operation) bool
	Invoke(ctx context.Context, route ResolvedRoute, req *Request) (*Result, error)
}

// ProviderPreflighter detects deterministic, candidate-local incompatibility
// before quota is reserved or a provider attempt is started.
type ProviderPreflighter interface {
	Preflight(route ResolvedRoute, req *Request) error
}

type APIError struct {
	Status         int         `json:"-"`
	Type           string      `json:"type"`
	Code           string      `json:"code,omitempty"`
	Param          string      `json:"param,omitempty"`
	Message        string      `json:"message"`
	Retryable      bool        `json:"-"`
	Fallback       bool        `json:"-"`
	CircuitFailure bool        `json:"-"`
	Headers        http.Header `json:"-"`
	cause          error
	dispositionSet bool
}

func WithResponseHeaders(err error, headers http.Header) error {
	if err == nil || len(headers) == 0 {
		return err
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		apiErr.Headers = headers.Clone()
	}
	return err
}

func (e *APIError) Error() string {
	return e.Message
}

func (e *APIError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func NewError(status int, typ, code, message string) *APIError {
	return &APIError{Status: status, Type: typ, Code: code, Message: message}
}

func (e *APIError) WithDisposition(retryable, fallback, circuitFailure bool) *APIError {
	if e == nil {
		return nil
	}
	e.Retryable = retryable
	e.Fallback = fallback
	e.CircuitFailure = circuitFailure
	e.dispositionSet = true
	return e
}

func (e *APIError) WithCause(err error) *APIError {
	if e != nil {
		e.cause = err
	}
	return e
}

// AllowFallback marks a candidate-local error so routing can try another
// compatible route without changing the public status returned when all
// candidates fail.
func AllowFallback(err error) error {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		apiErr.Fallback = true
		apiErr.dispositionSet = true
	}
	return err
}

// DisallowFallback marks an error as terminal for this request even when its
// HTTP status would ordinarily be eligible for another route.
func DisallowFallback(err error) error {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		apiErr.Fallback = false
		apiErr.dispositionSet = true
	}
	return err
}

func WrapError(status int, typ, code string, err error) *APIError {
	if err == nil {
		return nil
	}
	message := http.StatusText(status)
	if message == "" {
		message = "request failed"
	}
	return &APIError{Status: status, Type: typ, Code: code, Message: strings.ToLower(message), cause: err}
}

func AsAPIError(err error) *APIError {
	if err == nil {
		return nil
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return &APIError{
		Status:  http.StatusInternalServerError,
		Type:    "server_error",
		Code:    "internal_error",
		Message: "internal server error",
	}
}

func UnsupportedOperation(message string) *APIError {
	return NewError(http.StatusBadRequest, "invalid_request_error", "unsupported_operation", message)
}

func ModelNotFound(model string) *APIError {
	return NewError(http.StatusNotFound, "invalid_request_error", "model_not_found", fmt.Sprintf("model %q is not configured", model))
}

func RateLimited(message string) *APIError {
	return NewError(http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded", message).
		WithDisposition(false, true, false)
}

func Unauthorized(message string) *APIError {
	return NewError(http.StatusUnauthorized, "invalid_request_error", "unauthorized", message)
}

func Forbidden(message string) *APIError {
	return NewError(http.StatusForbidden, "permission_error", "forbidden", message)
}

type TokenCounter interface {
	Name() string
	CountTokens(ctx context.Context, route ResolvedRoute, req *Request) (Usage, error)
}

type EffectiveParams struct {
	MaxOutputTokens int
}

type EffectiveParamBuilder interface {
	BuildEffective(route ResolvedRoute, req *Request) (*EffectiveParams, error)
}

type ResolvedRoute struct {
	Route      *Route
	Request    *Request
	BridgeFrom Operation
	Headers    http.Header
	Effective  *EffectiveParams
	Estimate   Usage
}

type Router struct {
	providers map[string]Provider
}

func NewRouter(providers ...Provider) *Router {
	return &Router{providers: providerRegistry(providers)}
}

func (r *Router) Resolve(snapshot *Snapshot, env *Request) ([]ResolvedRoute, error) {
	var candidates []ResolvedRoute
	var unsupported []string
	routes := snapshot.routesByModel[env.Model]
	if snapshot.routesByModel == nil {
		// Hand-built snapshots remain supported by package consumers and tests.
		routes = make([]*Route, 0, len(snapshot.Routes))
		for _, route := range snapshot.Routes {
			if route != nil && route.Model == env.Model {
				routes = append(routes, route)
			}
		}
		sort.Slice(routes, func(i, j int) bool { return routes[i].Name < routes[j].Name })
	}
	for _, route := range routes {
		prepared, reason, ok := r.prepareRoute(route, env)
		if ok {
			candidates = append(candidates, prepared)
			continue
		}
		if reason != "" {
			unsupported = append(unsupported, fmt.Sprintf("%s: %s", route.Name, reason))
		}
	}
	if len(candidates) == 0 && len(unsupported) == 0 {
		return nil, ModelNotFound(env.Model)
	}
	if len(candidates) == 0 {
		if len(unsupported) > 0 {
			return nil, UnsupportedOperation(strings.Join(unsupported, "; "))
		}
		return nil, UnsupportedOperation("no configured route can satisfy this request")
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Route.Priority != candidates[j].Route.Priority {
			return candidates[i].Route.Priority > candidates[j].Route.Priority
		}
		return candidates[i].Route.Name < candidates[j].Route.Name
	})
	ordered := make([]ResolvedRoute, 0, len(candidates))
	for start := 0; start < len(candidates); {
		end := start + 1
		for end < len(candidates) && candidates[end].Route.Priority == candidates[start].Route.Priority {
			end++
		}
		seed := env.Meta.ExecutionID
		if seed == "" {
			// Router-only callers do not have the engine's private execution ID.
			// The engine path always uses ExecutionID so clients cannot steer
			// weighted routing by choosing X-Request-ID values.
			seed = env.Meta.RequestID
		}
		ordered = append(ordered, r.rotateWeighted(routeGroupKey(env), candidates[start:end], seed)...)
		start = end
	}
	return ordered, nil
}

func (r *Router) rotateWeighted(alias string, group []ResolvedRoute, requestID string) []ResolvedRoute {
	if len(group) <= 1 {
		return group
	}
	maxUint64 := ^uint64(0)
	total := uint64(0)
	for _, candidate := range group {
		if candidate.Route.Weight <= 0 {
			continue
		}
		weight := uint64(candidate.Route.Weight)
		if total > maxUint64-weight {
			total = maxUint64
			break
		}
		total += weight
	}
	if total == 0 {
		return group
	}
	seed := alias + ":" + requestID
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(seed))
	target := hash.Sum64() % total
	pick := 0
	offset := uint64(0)
	for i, candidate := range group {
		if candidate.Route.Weight <= 0 {
			continue
		}
		weight := uint64(candidate.Route.Weight)
		if offset > maxUint64-weight {
			offset = maxUint64
		} else {
			offset += weight
		}
		if target < offset {
			pick = i
			break
		}
	}
	out := make([]ResolvedRoute, 0, len(group))
	out = append(out, group[pick:]...)
	out = append(out, group[:pick]...)
	return out
}

func routeGroupKey(env *Request) string {
	if env == nil {
		return ""
	}
	if env.Provider == "" {
		return env.Model
	}
	return env.Provider + ":" + env.Model
}

func (r *Router) prepareRoute(route *Route, env *Request) (ResolvedRoute, string, bool) {
	// Configuration snapshots are immutable and shared by every in-flight
	// request. Give the request its own deep copy before provider planners,
	// interceptors, or adapters can observe (and potentially mutate) the route.
	route = cloneRoute(route)
	if env.Provider != "" && route.Provider != env.Provider {
		return ResolvedRoute{}, "route provider does not match request provider", false
	}
	prepared := env.Clone()
	prepared.Model = route.UpstreamModel
	if prepared.Model == "" {
		prepared.Model = route.Model
	}
	resolved := ResolvedRoute{Route: route, Request: prepared}
	if !route.Capabilities.Supports(env.Operation) {
		provider := r.providers[route.Provider]
		planner, supportsBridge := provider.(ProviderBridgePlanner)
		if !supportsBridge {
			return ResolvedRoute{}, "route does not support requested operation", false
		}
		target, reason, ok := planner.PlanBridge(cloneRoute(route), env.Clone())
		if !ok {
			if reason == "" {
				reason = "route does not support requested operation"
			}
			return ResolvedRoute{}, reason, false
		}
		if !isKnownOperation(target) || !route.Capabilities.Supports(target) || !provider.Supports(target) {
			return ResolvedRoute{}, "provider returned an invalid compatibility bridge plan", false
		}
		resolved.BridgeFrom = env.Operation
		prepared.Operation = target
	}
	if env.Stream && !route.Capabilities.Streaming {
		return ResolvedRoute{}, "route does not support streaming", false
	}
	if env.Hints.RequiresTools && !route.Capabilities.ToolCalling {
		return ResolvedRoute{}, "route does not support tool calling", false
	}
	if env.Hints.RequiresStructuredOutput && !route.Capabilities.StructuredOutput {
		return ResolvedRoute{}, "route does not support structured output", false
	}
	if env.Hints.RequiresVision && !route.Capabilities.VisionInput {
		return ResolvedRoute{}, "route does not support vision input", false
	}
	if env.Hints.RequiresAudio && !route.Capabilities.Audio {
		return ResolvedRoute{}, "route does not support audio", false
	}
	if env.Hints.RequiresReasoning && !route.Capabilities.Reasoning {
		return ResolvedRoute{}, "route does not support reasoning parameters", false
	}
	if max := env.RequestedMaxOutputTokens(); max > 0 && route.Capabilities.MaxOutputTokens > 0 && max > route.Capabilities.MaxOutputTokens {
		return ResolvedRoute{}, "requested output tokens exceed route maximum", false
	}
	return resolved, "", true
}

func (r *ResolvedRoute) SetHeader(key, value string) {
	if r == nil {
		return
	}
	if r.Headers == nil {
		r.Headers = make(http.Header)
	}
	r.Headers.Set(key, value)
}

type RequestState struct {
	Snapshot    *Snapshot
	Request     *Request
	Principal   *Principal
	Subject     Subject
	Estimate    Usage
	Candidates  []ResolvedRoute
	Scopes      []ScopedLimit
	Quota       *QuotaTicket
	Reserved    EstimatedUsage
	StartedAt   time.Time
	usage       AttemptUsageAccumulator
	reserveMu   sync.Mutex
	reserveFn   func(context.Context, *Attempt) error
	releaseFn   func(string)
	reconcileFn func(context.Context, string, ActualUsage) error
	resolveFn   func() ([]ResolvedRoute, error)
	resolved    bool
}

func (s *RequestState) SetAttemptReservation(fn func(context.Context, *Attempt) error) {
	if s == nil {
		return
	}
	s.reserveMu.Lock()
	s.reserveFn = fn
	s.reserveMu.Unlock()
}

func (s *RequestState) SetAttemptReservationRelease(fn func(string)) {
	if s == nil {
		return
	}
	s.reserveMu.Lock()
	s.releaseFn = fn
	s.reserveMu.Unlock()
}

func (s *RequestState) SetAttemptReservationReconcile(fn func(context.Context, string, ActualUsage) error) {
	if s == nil {
		return
	}
	s.reserveMu.Lock()
	s.reconcileFn = fn
	s.reserveMu.Unlock()
}

// ReserveProviderAttempt is exposed for custom dispatchers that use the
// request/attempt interceptor contracts without Engine's built-in dispatcher.
func (s *RequestState) ReserveProviderAttempt(ctx context.Context, attempt *Attempt) error {
	if s == nil {
		return nil
	}
	s.reserveMu.Lock()
	fn := s.reserveFn
	s.reserveMu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(ctx, attempt)
}

func (s *RequestState) ReleaseProviderAttemptReservation(attemptID string) {
	if s == nil || attemptID == "" {
		return
	}
	s.reserveMu.Lock()
	fn := s.releaseFn
	s.reserveMu.Unlock()
	if fn != nil {
		fn(attemptID)
	}
}

// ReconcileProviderAttemptReservation makes the unused part of a completed,
// billable failed attempt available to a later retry or fallback. Aborted
// streams deliberately do not call this method because their reported usage
// can be only a partial snapshot.
func (s *RequestState) ReconcileProviderAttemptReservation(ctx context.Context, attemptID string) error {
	if s == nil || attemptID == "" {
		return nil
	}
	actual, ok := s.usage.charge(attemptID)
	if !ok {
		return nil
	}
	s.reserveMu.Lock()
	fn := s.reconcileFn
	s.reserveMu.Unlock()
	if fn != nil {
		return fn(ctx, attemptID, actual)
	}
	return nil
}

type Attempt struct {
	Number    int
	Retry     int
	ID        string
	Route     ResolvedRoute
	StartedAt time.Time
}

type FinalizeFunc func(ctx context.Context, actual Usage, callErr error) error

type Execution struct {
	State    *RequestState
	Attempt  *Attempt
	Result   *Result
	Finalize FinalizeFunc
	mu       sync.Mutex
	stage    ExecutionStage
}

type ExecutionStage string

const (
	ExecutionReserved   ExecutionStage = "reserved"
	ExecutionDispatched ExecutionStage = "dispatched"
	ExecutionFirstByte  ExecutionStage = "first_byte"
	ExecutionCompleted  ExecutionStage = "completed"
	ExecutionFailed     ExecutionStage = "failed"
	ExecutionAborted    ExecutionStage = "aborted"
)

type RequestHandler func(ctx context.Context, state *RequestState) (*Execution, error)
type AttemptHandler func(ctx context.Context, state *RequestState, attempt *Attempt) (*Result, error)

type RequestInterceptor interface {
	Wrap(next RequestHandler) RequestHandler
}

type AttemptInterceptor interface {
	WrapAttempt(next AttemptHandler) AttemptHandler
}

type CandidatePreflight struct {
	providers map[string]Provider
}

func NewCandidatePreflight(providers []Provider) CandidatePreflight {
	return CandidatePreflight{providers: providerRegistry(providers)}
}

func (p CandidatePreflight) Wrap(next RequestHandler) RequestHandler {
	return func(ctx context.Context, state *RequestState) (*Execution, error) {
		candidates, err := state.ResolveCandidates()
		if err != nil {
			return nil, err
		}
		filtered := make([]ResolvedRoute, 0, len(candidates))
		var lastErr error
		for _, candidate := range candidates {
			if candidate.Route == nil {
				lastErr = NewError(http.StatusInternalServerError, "server_error", "invalid_route", "resolved route is missing configuration")
				continue
			}
			provider := p.providers[candidate.Route.Provider]
			preflight, ok := provider.(ProviderPreflighter)
			if !ok {
				filtered = append(filtered, candidate)
				continue
			}
			if err := preflight.Preflight(candidate, candidate.Request); err != nil {
				lastErr = err
				if fallbackEligible(err) {
					continue
				}
				return nil, err
			}
			filtered = append(filtered, candidate)
		}
		if len(filtered) == 0 {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, UnsupportedOperation("no resolved route passed provider preflight")
		}
		state.ReplaceCandidates(filtered)
		return next(ctx, state)
	}
}

type Engine struct {
	cfg                 *ConfigStore
	router              *Router
	providers           map[string]Provider
	requestInterceptors []RequestInterceptor
	attemptInterceptors []AttemptInterceptor
}

var executionSequence atomic.Uint64

func NewEngine(cfg *ConfigStore, providers []Provider, requestInterceptors []RequestInterceptor, attemptInterceptors []AttemptInterceptor) *Engine {
	return &Engine{
		cfg:                 cfg,
		router:              NewRouter(providers...),
		providers:           providerRegistry(providers),
		requestInterceptors: requestInterceptors,
		attemptInterceptors: attemptInterceptors,
	}
}

func (e *Engine) Execute(ctx context.Context, req *Request) (*Execution, error) {
	snapshot := e.cfg.Load()
	return e.ExecuteWithSnapshot(ctx, snapshot, req)
}

// ExecuteWithSnapshot pins routing and every request-scope policy decision to
// the same immutable configuration snapshot used by ingress decoding.
func (e *Engine) ExecuteWithSnapshot(ctx context.Context, snapshot *Snapshot, req *Request) (*Execution, error) {
	if req == nil {
		return nil, NewError(http.StatusBadRequest, "invalid_request_error", "invalid_request", "request is required")
	}
	if snapshot == nil {
		return nil, NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded")
	}
	request := req.Clone()
	request.Meta.ExecutionID = newExecutionID()
	state := &RequestState{
		Snapshot:  snapshot,
		Request:   request,
		StartedAt: time.Now(),
	}
	state.resolveFn = func() ([]ResolvedRoute, error) {
		return e.router.Resolve(snapshot, request)
	}
	handler := e.dispatch
	for i := len(e.requestInterceptors) - 1; i >= 0; i-- {
		handler = e.requestInterceptors[i].Wrap(handler)
	}
	return handler(ctx, state)
}

func (e *Engine) dispatch(ctx context.Context, state *RequestState) (*Execution, error) {
	candidates, err := state.ResolveCandidates()
	if err != nil {
		return nil, err
	}
	var lastErr error
	for idx, route := range candidates {
		provider, ok := e.providers[route.Route.Provider]
		if !ok {
			lastErr = NewError(http.StatusBadGateway, "server_error", "unknown_provider", "route provider is not registered")
			continue
		}
		if !provider.Supports(route.Request.Operation) {
			lastErr = UnsupportedOperation("provider does not support requested operation")
			continue
		}
		startedAt := time.Now()
		attempt := &Attempt{
			Number:    idx + 1,
			ID:        state.Request.Meta.ExecutionID + ":" + strconv.Itoa(idx+1),
			Route:     route,
			StartedAt: startedAt,
		}
		result, err := e.invoke(ctx, state, attempt, provider)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !fallbackEligible(err) {
				return nil, err
			}
			continue
		}
		exec := &Execution{
			State:    state,
			Attempt:  attempt,
			Result:   result,
			Finalize: func(context.Context, Usage, error) error { return nil },
			stage:    ExecutionReserved,
		}
		exec.MarkDispatched()
		if result.RawStream != nil {
			// invoke has already prefetched an upstream byte. Mark the request as
			// chargeable before downstream delivery so an immediate client write
			// failure cannot refund completed upstream work.
			exec.MarkFirstByte()
		}
		return exec, nil
	}
	if lastErr == nil {
		lastErr = NewError(http.StatusBadGateway, "server_error", "no_route", "no route could complete the request")
	}
	return nil, lastErr
}

func (e *Engine) invoke(ctx context.Context, state *RequestState, attempt *Attempt, provider Provider) (*Result, error) {
	handler := func(ctx context.Context, state *RequestState, attempt *Attempt) (*Result, error) {
		// Quota top-ups happen at the last possible point before an actual
		// provider call. Attempt-scope circuit, concurrency, and rate failures
		// therefore cannot cause false hard-quota denials for healthy fallbacks.
		if err := state.ReserveProviderAttempt(ctx, attempt); err != nil {
			return nil, err
		}
		state.beginAttemptUsage(attempt)
		defer func() {
			if panicValue := recover(); panicValue != nil {
				state.usage.complete(attempt.ID, Usage{}, true)
				panic(panicValue)
			}
		}()
		result, err := provider.Invoke(ctx, attempt.Route, attempt.Route.Request)
		if err != nil {
			usage, known, unbillable := attemptErrorUsage(err)
			if unbillable {
				state.usage.cancel(attempt.ID)
				state.ReleaseProviderAttemptReservation(attempt.ID)
			} else {
				state.usage.complete(attempt.ID, usage, false)
				if known {
					if reconcileErr := state.ReconcileProviderAttemptReservation(ctx, attempt.ID); reconcileErr != nil {
						return nil, reconcileErr
					}
				}
			}
			return nil, err
		}
		if result != nil {
			result.AttemptID = attempt.ID
			if result.RawStream != nil {
				result.RawStream = &attemptUsageReadCloser{
					ReadCloser: result.RawStream,
					usage:      result.FinalUsage,
					complete: func(usage Usage, failed bool) {
						state.usage.complete(attempt.ID, usage, failed)
					},
				}
			}
		}
		validated, err := validateProviderResult(ctx, result)
		if err != nil {
			usage := Usage{}
			if result != nil {
				usage = result.FinalUsage()
			}
			state.usage.complete(attempt.ID, usage, false)
			if !usage.IsZero() {
				if reconcileErr := state.ReconcileProviderAttemptReservation(ctx, attempt.ID); reconcileErr != nil {
					return nil, reconcileErr
				}
			}
			return nil, err
		}
		if validated.RawStream == nil {
			state.usage.complete(attempt.ID, validated.FinalUsage(), false)
		}
		return validated, nil
	}
	for i := len(e.attemptInterceptors) - 1; i >= 0; i-- {
		handler = e.attemptInterceptors[i].WrapAttempt(handler)
	}
	result, err := handler(ctx, state, attempt)
	if err != nil {
		return nil, err
	}
	// Attempt interceptors are allowed to short-circuit the provider handler.
	// Keep a defensive check here, while normal provider results are validated
	// inside the interceptor lifecycle so metrics and breakers see failures.
	if result == nil {
		return nil, NewError(http.StatusInternalServerError, "server_error", "invalid_attempt_result", "attempt interceptor returned no result")
	}
	result.Provider = provider.Name()
	result.Route = attempt.Route.Route.Name
	if result.Model == "" {
		result.Model = attempt.Route.Route.Model
	}
	return result, nil
}

type attemptUsageReadCloser struct {
	io.ReadCloser
	usage    func() Usage
	complete func(Usage, bool)
	once     sync.Once
	sawEOF   atomic.Bool
}

func (r *attemptUsageReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if errors.Is(err, io.EOF) {
		r.sawEOF.Store(true)
		r.finish(false)
	} else if err != nil {
		r.finish(true)
	}
	return n, err
}

func (r *attemptUsageReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.finish(!r.sawEOF.Load())
	return err
}

func (r *attemptUsageReadCloser) finish(failed bool) {
	r.once.Do(func() {
		usage := Usage{}
		if r.usage != nil {
			usage = r.usage()
		}
		if r.complete != nil {
			r.complete(usage, failed)
		}
	})
}

func (s *RequestState) beginAttemptUsage(attempt *Attempt) {
	if s == nil || attempt == nil {
		return
	}
	s.usage.begin(attempt.ID, attempt.Route)
}

// CompleteResultUsage reconciles a final stream result that a caller settles
// without first reading or closing the stream.
func (s *RequestState) CompleteResultUsage(result *Result, usage Usage, failed bool) {
	if s == nil || result == nil || result.AttemptID == "" {
		return
	}
	s.usage.complete(result.AttemptID, usage, failed)
}

// TotalAttemptUsage returns the request's aggregate, de-duplicated provider
// usage across every retry and fallback.
func (s *RequestState) TotalAttemptUsage() ActualUsage {
	if s == nil {
		return ActualUsage{}
	}
	return s.usage.total()
}

func validateProviderResult(ctx context.Context, result *Result) (*Result, error) {
	if result == nil {
		return nil, NewError(http.StatusBadGateway, "upstream_error", "empty_result", "provider returned no result").
			WithDisposition(false, true, true)
	}
	if result.StatusCode == 0 {
		result.StatusCode = http.StatusOK
	}
	if result.StatusCode < http.StatusOK || result.StatusCode >= http.StatusMultipleChoices {
		if result.RawStream != nil {
			_ = result.RawStream.Close()
		}
		return nil, providerStatusError(result.StatusCode)
	}
	if result.RawStream == nil && len(result.RawBody) == 0 {
		return nil, NewError(http.StatusBadGateway, "upstream_error", "empty_result", "provider returned no response body").
			WithDisposition(false, true, true)
	}
	if err := prefetchStream(ctx, result); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && !apiErr.dispositionSet {
			apiErr.WithDisposition(false, true, true)
		}
		return nil, err
	}
	return result, nil
}

func providerStatusError(status int) error {
	switch {
	case status < http.StatusOK || (status >= http.StatusMultipleChoices && status < http.StatusBadRequest) || status > 599:
		return NewError(http.StatusBadGateway, "upstream_error", "unexpected_upstream_status", "upstream returned an unexpected HTTP status").
			WithDisposition(false, true, true)
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return NewError(http.StatusBadGateway, "upstream_error", "upstream_authentication_failed", "upstream route authentication failed").
			WithDisposition(false, true, true)
	case status >= http.StatusInternalServerError:
		return NewError(status, "upstream_error", "upstream_status", fmt.Sprintf("upstream returned status %d", status)).
			WithDisposition(true, true, true)
	case status == http.StatusTooManyRequests:
		return NewError(status, "rate_limit_error", "upstream_rate_limit", "upstream rate limit exceeded").
			WithDisposition(false, true, false)
	default:
		return NewError(status, "upstream_error", "upstream_status", "upstream rejected the request")
	}
}

func fallbackEligible(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return true
	}
	if apiErr.dispositionSet {
		return apiErr.Fallback
	}
	if apiErr.Status >= http.StatusInternalServerError {
		return true
	}
	if apiErr.Fallback {
		return true
	}
	switch apiErr.Status {
	case http.StatusRequestTimeout, http.StatusConflict, http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}

func prefetchStream(ctx context.Context, result *Result) error {
	if result == nil || result.RawStream == nil {
		return nil
	}
	buf := make([]byte, 32*1024)
	var n int
	var readErr error
	for emptyReads := 0; emptyReads < 8; emptyReads++ {
		n, readErr = result.RawStream.Read(buf)
		if n > 0 || readErr != nil {
			break
		}
		if err := ctx.Err(); err != nil {
			_ = result.RawStream.Close()
			return err
		}
	}
	if n == 0 && readErr == nil {
		_ = result.RawStream.Close()
		return NewError(http.StatusBadGateway, "upstream_error", "stream_no_progress", "provider stream made no progress before the first byte")
	}
	if n == 0 && readErr != nil {
		_ = result.RawStream.Close()
		var apiErr *APIError
		if errors.As(readErr, &apiErr) {
			return readErr
		}
		code := "stream_read_failed"
		message := "provider stream failed before the first byte"
		if errors.Is(readErr, io.EOF) {
			code = "empty_stream"
			message = "provider returned an empty stream"
		}
		return NewError(http.StatusBadGateway, "upstream_error", code, message)
	}
	if n > 0 && readErr != nil && !errors.Is(readErr, io.EOF) {
		_ = result.RawStream.Close()
		var apiErr *APIError
		if errors.As(readErr, &apiErr) {
			return readErr
		}
		return NewError(http.StatusBadGateway, "upstream_error", "stream_read_failed", "provider stream failed before the first byte")
	}
	if n == 0 {
		return nil
	}
	result.RawStream = &prefetchedReadCloser{
		Reader:     bytes.NewReader(append([]byte(nil), buf[:n]...)),
		Tail:       result.RawStream,
		PendingErr: readErr,
	}
	return nil
}

type prefetchedReadCloser struct {
	Reader     *bytes.Reader
	Tail       io.ReadCloser
	PendingErr error
}

func (r *prefetchedReadCloser) Read(p []byte) (int, error) {
	if r.Reader != nil && r.Reader.Len() > 0 {
		return r.Reader.Read(p)
	}
	if r.PendingErr != nil {
		err := r.PendingErr
		r.PendingErr = nil
		return 0, err
	}
	return r.Tail.Read(p)
}

func (r *prefetchedReadCloser) Close() error {
	if r == nil || r.Tail == nil {
		return nil
	}
	return r.Tail.Close()
}

func newExecutionID() string {
	var random [16]byte
	if _, err := rand.Read(random[:]); err == nil {
		return "exec_" + hex.EncodeToString(random[:])
	}
	return fmt.Sprintf("exec_%d_%d", time.Now().UnixNano(), executionSequence.Add(1))
}

func (x *Execution) Settle(ctx context.Context, actual Usage, callErr error) error {
	if x == nil || x.Finalize == nil {
		return nil
	}
	if callErr == nil {
		x.setStage(ExecutionCompleted)
	} else if x.Stage() == ExecutionFirstByte {
		x.setStage(ExecutionAborted)
	} else {
		x.setStage(ExecutionFailed)
	}
	return x.Finalize(ctx, actual, callErr)
}

func (s *RequestState) ResolveCandidates() ([]ResolvedRoute, error) {
	if s == nil {
		return nil, nil
	}
	if s.resolved {
		return s.Candidates, nil
	}
	if s.resolveFn == nil {
		if s.Snapshot != nil && s.Request != nil {
			candidates, err := NewRouter().Resolve(s.Snapshot, s.Request)
			if err != nil {
				return nil, err
			}
			s.Candidates = candidates
		}
		s.resolved = true
		return s.Candidates, nil
	}
	candidates, err := s.resolveFn()
	if err != nil {
		return nil, err
	}
	s.Candidates = candidates
	s.resolved = true
	return s.Candidates, nil
}

func (s *RequestState) ReplaceCandidates(candidates []ResolvedRoute) {
	if s == nil {
		return
	}
	s.Candidates = candidates
	s.resolved = true
}

func (x *Execution) MarkDispatched() {
	x.setStage(ExecutionDispatched)
}

func (x *Execution) MarkFirstByte() {
	x.setStage(ExecutionFirstByte)
}

func (x *Execution) Stage() ExecutionStage {
	if x == nil {
		return ""
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.stage
}

func (x *Execution) setStage(stage ExecutionStage) {
	if x == nil {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	x.stage = stage
}
