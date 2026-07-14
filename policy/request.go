package policy

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"llmgw/gateway"
	"llmgw/store"

	"github.com/samber/lo"
)

type Auth struct{}

type authenticatedPrincipalContextKey struct{}

func WithAuthenticatedPrincipal(ctx context.Context, principal *gateway.Principal) context.Context {
	if principal == nil {
		return ctx
	}
	copy := *principal
	return context.WithValue(ctx, authenticatedPrincipalContextKey{}, &copy)
}

func authenticatedPrincipalFromContext(ctx context.Context) *gateway.Principal {
	principal, _ := ctx.Value(authenticatedPrincipalContextKey{}).(*gateway.Principal)
	return principal
}

func (Auth) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if principal := authenticatedPrincipalFromContext(ctx); principal != nil {
			enrichMeta(state)
			if err := bindPrincipal(state, principal); err != nil {
				return nil, err
			}
			return next(ctx, state)
		}
		if len(state.Snapshot.Auth.Tokens) == 0 && !state.Snapshot.Auth.JWT.Enabled() {
			if !state.Snapshot.Auth.AllowAnonymous {
				return nil, gateway.Unauthorized("authentication is not configured")
			}
			enrichMeta(state)
			return next(ctx, state)
		}
		token, presented := gatewayToken(state.Request)
		if !presented {
			if state.Snapshot.Auth.AllowAnonymous {
				enrichMeta(state)
				return next(ctx, state)
			}
			return nil, gateway.Unauthorized("missing bearer token")
		}
		principal, err := AuthenticatePrincipal(state.Snapshot, token)
		if err != nil {
			return nil, err
		}
		enrichMeta(state)
		if err := bindPrincipal(state, principal); err != nil {
			return nil, err
		}
		return next(ctx, state)
	}
}

func gatewayToken(req *gateway.Request) (string, bool) {
	if req == nil {
		return "", false
	}
	headers := req.Meta.Headers
	if auth := strings.TrimSpace(headerValue(headers, "Authorization")); auth != "" {
		if !strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			return "", true
		}
		return strings.TrimSpace(auth[len("Bearer "):]), true
	}
	return "", false
}

func headerValue(headers http.Header, name string) string {
	if value := headers.Get(name); value != "" {
		return value
	}
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

type RequireUser struct{}

func (RequireUser) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if state.Snapshot.Auth.RequireUser && state.Request.Meta.User == "" {
			return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "missing_user", "user is required")
		}
		if state.Snapshot.Auth.RequireProject && state.Request.Meta.Project == "" {
			return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "missing_project", "project is required")
		}
		return next(ctx, state)
	}
}

const maxForwardedIdentityBytes = 512

// MetadataValidation rejects client-controlled identity metadata before quota
// reservation or route attempts. These values are forwarded as outbound
// headers, so accepting controls or unbounded values would turn a bad request
// into a local transport failure and incorrectly penalize provider health.
type MetadataValidation struct{}

func (MetadataValidation) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if state == nil || state.Request == nil {
			return next(ctx, state)
		}
		if err := validateForwardedIdentity("user", state.Request.Meta.User); err != nil {
			return nil, err
		}
		if err := validateForwardedIdentity("project", state.Request.Meta.Project); err != nil {
			return nil, err
		}
		return next(ctx, state)
	}
}

func validateForwardedIdentity(field, value string) error {
	if len(value) > maxForwardedIdentityBytes {
		err := gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_metadata", field+" exceeds the maximum length")
		err.Param = field
		return err
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] == 0x7f {
			err := gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_metadata", field+" contains invalid control characters")
			err.Param = field
			return err
		}
	}
	return nil
}

func safeForwardedIdentity(value string) bool {
	return validateForwardedIdentity("metadata", value) == nil
}

type RequestSize struct{}

func (RequestSize) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		candidates, err := state.ResolveCandidates()
		if err != nil {
			return nil, err
		}
		filtered := lo.Filter(candidates, func(candidate gateway.ResolvedRoute, _ int) bool {
			maxBody := candidate.Route.Limits.MaxBodyBytes
			return maxBody == 0 || state.Request.Meta.BodyBytes <= maxBody
		})
		if len(filtered) == 0 {
			return nil, gateway.NewError(http.StatusRequestEntityTooLarge, "invalid_request_error", "body_too_large", "request body exceeds route maximum")
		}
		state.ReplaceCandidates(filtered)
		return next(ctx, state)
	}
}

type ACL struct{}

func (ACL) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if state.Principal == nil {
			return next(ctx, state)
		}
		if len(state.Principal.Models) > 0 && !lo.Contains(state.Principal.Models, state.Request.Model) {
			return nil, gateway.Forbidden("model not allowed for this token")
		}
		if len(state.Principal.Projects) > 0 {
			if state.Request.Meta.Project == "" {
				return nil, gateway.Forbidden("project is required for this token")
			}
			if !lo.Contains(state.Principal.Projects, state.Request.Meta.Project) {
				return nil, gateway.Forbidden("project not allowed for this token")
			}
		}
		if len(state.Principal.Providers) > 0 {
			candidates, err := state.ResolveCandidates()
			if err != nil {
				return nil, err
			}
			filtered := lo.Filter(candidates, func(candidate gateway.ResolvedRoute, _ int) bool {
				return lo.Contains(state.Principal.Providers, candidate.Route.Provider)
			})
			if len(filtered) == 0 {
				return nil, gateway.Forbidden("provider not allowed for this token")
			}
			state.ReplaceCandidates(filtered)
		}
		return next(ctx, state)
	}
}

type ResolveScopes struct {
	Limits store.QuotaLimitStore
}

func (r ResolveScopes) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if state.Subject.KeyID == "" {
			state.Subject = subjectForState(state)
		}
		scopes, err := r.resolve(ctx, state)
		if err != nil {
			return nil, err
		}
		if len(scopes) == 0 {
			return next(ctx, state)
		}
		candidates, err := state.ResolveCandidates()
		if err != nil {
			return nil, err
		}
		filtered := lo.Filter(candidates, func(candidate gateway.ResolvedRoute, _ int) bool {
			return candidateAllowedByScopes(state, candidate, scopes) && candidateHasRequiredUnitPricing(state.Request, candidate, scopes)
		})
		if len(filtered) == 0 {
			if hardSpendEnabled(scopes) && len(state.Request.Hints.ProviderUnits) > 0 {
				return nil, gateway.UnsupportedOperation("hard spend quotas require pricing and a positive per-request reservation bound for every hosted provider tool")
			}
			return nil, gateway.Forbidden("request is not allowed by quota scope policy")
		}
		state.ReplaceCandidates(filtered)
		state.Scopes = scopes
		state.Estimate = maxEstimate(filtered)
		return next(ctx, state)
	}
}

func hardSpendEnabled(scopes []gateway.ScopedLimit) bool {
	return lo.SomeBy(scopes, func(scope gateway.ScopedLimit) bool { return scope.Limits.MaxSpendMicros > 0 })
}

func candidateHasRequiredUnitPricing(req *gateway.Request, candidate gateway.ResolvedRoute, scopes []gateway.ScopedLimit) bool {
	if req == nil || len(req.Hints.ProviderUnits) == 0 || !hardSpendEnabled(scopes) || candidate.Route == nil {
		return true
	}
	for _, unit := range req.Hints.ProviderUnits {
		pricing, ok := candidate.Route.Pricing.ProviderUnits[unit]
		if !ok || pricing.MicrosPerUnit <= 0 || pricing.MaxUnitsPerRequest <= 0 {
			return false
		}
	}
	return true
}

func (r ResolveScopes) resolve(ctx context.Context, state *gateway.RequestState) ([]gateway.ScopedLimit, error) {
	if state == nil || state.Subject.KeyID == "" {
		return nil, nil
	}
	if r.Limits != nil {
		limit, ok, err := r.Limits.Get(ctx, state.Subject.KeyID)
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return nil, err
			}
			return nil, gateway.NewError(
				http.StatusServiceUnavailable,
				"server_error",
				"quota_limit_store_unavailable",
				"quota limit service is temporarily unavailable",
			).WithCause(err).WithDisposition(false, false, false)
		}
		if ok {
			return []gateway.ScopedLimit{{
				Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: state.Subject.KeyID},
				Limits: limit,
			}}, nil
		}
	}
	return state.Snapshot.ResolveQuotaScopes(state.Subject), nil
}

func enrichMeta(state *gateway.RequestState) {
	headers := state.Request.Meta.Headers
	if state.Request.Meta.User == "" {
		state.Request.Meta.User = firstNonEmpty(headers.Get("X-LLMGW-User"), headers.Get("OpenAI-User"), state.Request.UserValue())
	}
	if state.Request.Meta.Project == "" {
		state.Request.Meta.Project = firstNonEmpty(headers.Get("X-LLMGW-Project"), headers.Get("X-Project-ID"), headers.Get("OpenAI-Project"), metadataProject(state.Request))
	}
}

func metadataProject(req *gateway.Request) string {
	if req == nil {
		return ""
	}
	if metadata := req.Metadata(); metadata != nil {
		return metadata["project"]
	}
	return ""
}

func subjectForState(state *gateway.RequestState) gateway.Subject {
	subject := state.Subject
	if subject.KeyID == "" {
		subject.KeyID = firstNonEmpty(state.Request.Meta.Principal, "anonymous")
	}
	return subject
}

func bindPrincipal(state *gateway.RequestState, principal *gateway.Principal) error {
	if state == nil || principal == nil {
		return nil
	}
	state.Principal = principal
	state.Request.Meta.Principal = firstNonEmpty(principal.ID, principal.KeyID)
	state.Subject = principal.Subject()
	return nil
}

func candidateAllowedByScopes(state *gateway.RequestState, candidate gateway.ResolvedRoute, scopes []gateway.ScopedLimit) bool {
	for _, scope := range scopes {
		if len(scope.Limits.ModelAllowlist) > 0 && !lo.Contains(scope.Limits.ModelAllowlist, state.Request.Model) {
			return false
		}
		if len(scope.Limits.ProviderAllowlist) > 0 && !lo.Contains(scope.Limits.ProviderAllowlist, candidate.Route.Provider) {
			return false
		}
		if scope.Limits.MaxInputTokens > 0 && candidate.Estimate.InputTokens > scope.Limits.MaxInputTokens {
			return false
		}
		requested := candidate.Request.RequestedMaxOutputTokens()
		if candidate.Effective != nil && candidate.Effective.MaxOutputTokens > 0 {
			requested = candidate.Effective.MaxOutputTokens
		}
		if requested == 0 {
			requested = int(candidate.Estimate.OutputTokens)
		}
		if scope.Limits.MaxOutputTokens > 0 && int64(requested) > scope.Limits.MaxOutputTokens {
			return false
		}
	}
	return true
}

func firstNonEmpty(values ...string) string {
	return lo.FindOrElse(values, "", func(value string) bool { return value != "" })
}
