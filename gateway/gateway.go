package gateway

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
)

type Provider interface {
	Name() string
	Supports(op Operation) bool
	Invoke(ctx context.Context, route ResolvedRoute, req *Request) (*Result, error)
}

type APIError struct {
	Status  int    `json:"-"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
	Param   string `json:"param,omitempty"`
	Message string `json:"message"`
}

func (e *APIError) Error() string {
	return e.Message
}

func NewError(status int, typ, code, message string) *APIError {
	return &APIError{Status: status, Type: typ, Code: code, Message: message}
}

func WrapError(status int, typ, code string, err error) *APIError {
	if err == nil {
		return nil
	}
	return &APIError{Status: status, Type: typ, Code: code, Message: err.Error()}
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
		Message: err.Error(),
	}
}

func UnsupportedOperation(message string) *APIError {
	return NewError(http.StatusBadRequest, "invalid_request_error", "unsupported_operation", message)
}

func ModelNotFound(model string) *APIError {
	return NewError(http.StatusNotFound, "invalid_request_error", "model_not_found", fmt.Sprintf("model %q is not configured", model))
}

func RateLimited(message string) *APIError {
	return NewError(http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded", message)
}

func Unauthorized(message string) *APIError {
	return NewError(http.StatusUnauthorized, "invalid_request_error", "unauthorized", message)
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
	Route     *Route
	Request   *Request
	Headers   http.Header
	Effective *EffectiveParams
	Estimate  Usage
}

type Router struct{}

func NewRouter() *Router {
	return &Router{}
}

func (r *Router) Resolve(snapshot *Snapshot, env *Request) ([]ResolvedRoute, error) {
	var candidates []ResolvedRoute
	var unsupported []string
	for _, route := range snapshot.Routes {
		if route == nil || route.Model != env.Model {
			continue
		}
		prepared, reason, ok := prepareRoute(route, env)
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
		ordered = append(ordered, r.rotateWeighted(routeGroupKey(env), candidates[start:end], env.Meta.RequestID)...)
		start = end
	}
	return ordered, nil
}

func (r *Router) rotateWeighted(alias string, group []ResolvedRoute, requestID string) []ResolvedRoute {
	if len(group) <= 1 {
		return group
	}
	total := 0
	for _, candidate := range group {
		total += candidate.Route.Weight
	}
	if total <= 0 {
		return group
	}
	seed := alias + ":" + requestID
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(seed))
	target := int(hash.Sum64() % uint64(total))
	pick := 0
	offset := 0
	for i, candidate := range group {
		offset += candidate.Route.Weight
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

func prepareRoute(route *Route, env *Request) (ResolvedRoute, string, bool) {
	if env.Provider != "" && route.Provider != env.Provider {
		return ResolvedRoute{}, "route provider does not match request provider", false
	}
	prepared := env.Clone()
	prepared.Model = route.Model
	resolved := ResolvedRoute{Route: route, Request: prepared}
	if !route.Capabilities.Supports(env.Operation) {
		return ResolvedRoute{}, "route does not support requested operation", false
	}
	if env.Stream && !route.Capabilities.Streaming {
		return ResolvedRoute{}, "route does not support streaming", false
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
	Snapshot   *Snapshot
	Request    *Request
	Principal  *Principal
	Subject    Subject
	Estimate   Usage
	Candidates []ResolvedRoute
	Scopes     []ScopedLimit
	Quota      *QuotaTicket
	Reserved   EstimatedUsage
	StartedAt  time.Time
	resolveFn  func() ([]ResolvedRoute, error)
	resolved   bool
}

type Attempt struct {
	Number    int
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

type Engine struct {
	cfg                 *ConfigStore
	router              *Router
	providers           map[string]Provider
	requestInterceptors []RequestInterceptor
	attemptInterceptors []AttemptInterceptor
}

func NewEngine(cfg *ConfigStore, providers []Provider, requestInterceptors []RequestInterceptor, attemptInterceptors []AttemptInterceptor) *Engine {
	return &Engine{
		cfg:                 cfg,
		router:              NewRouter(),
		providers:           lo.SliceToMap(providers, func(provider Provider) (string, Provider) { return provider.Name(), provider }),
		requestInterceptors: requestInterceptors,
		attemptInterceptors: attemptInterceptors,
	}
}

func (e *Engine) Execute(ctx context.Context, req *Request) (*Execution, error) {
	snapshot := e.cfg.Load()
	if snapshot == nil {
		return nil, NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded")
	}
	state := &RequestState{
		Snapshot:  snapshot,
		Request:   req,
		StartedAt: time.Now(),
	}
	state.resolveFn = func() ([]ResolvedRoute, error) {
		return e.router.Resolve(snapshot, req)
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
		attempt := &Attempt{
			Number:    idx + 1,
			ID:        state.Request.Meta.RequestID + ":" + route.Route.Name + ":" + time.Now().Format("150405.000000000"),
			Route:     route,
			StartedAt: time.Now(),
		}
		result, err := e.invoke(ctx, state, attempt, provider)
		if err != nil {
			lastErr = err
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.Status < 500 {
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
		return exec, nil
	}
	if lastErr == nil {
		lastErr = NewError(http.StatusBadGateway, "server_error", "no_route", "no route could complete the request")
	}
	return nil, lastErr
}

func (e *Engine) invoke(ctx context.Context, state *RequestState, attempt *Attempt, provider Provider) (*Result, error) {
	handler := func(ctx context.Context, state *RequestState, attempt *Attempt) (*Result, error) {
		return provider.Invoke(ctx, attempt.Route, attempt.Route.Request)
	}
	for i := len(e.attemptInterceptors) - 1; i >= 0; i-- {
		handler = e.attemptInterceptors[i].WrapAttempt(handler)
	}
	result, err := handler(ctx, state, attempt)
	if err != nil {
		return nil, err
	}
	if result.StatusCode == 0 {
		result.StatusCode = http.StatusOK
	}
	result.Provider = provider.Name()
	result.Route = attempt.Route.Route.Name
	if result.Model == "" {
		result.Model = attempt.Route.Route.Model
	}
	return result, nil
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
