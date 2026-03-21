package policy

import (
	"context"
	"net/http"
	"strings"

	"llmgw/gateway"
	"llmgw/store"

	"github.com/samber/lo"
)

type Auth struct{}

func (Auth) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if len(state.Snapshot.Auth.Tokens) == 0 && !state.Snapshot.Auth.JWT.Enabled() {
			enrichMeta(state)
			return next(ctx, state)
		}
		auth := state.Request.Meta.Headers.Get("Authorization")
		if !strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			return nil, gateway.Unauthorized("missing bearer token")
		}
		token := strings.TrimSpace(auth[len("Bearer "):])
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
			return nil, gateway.Unauthorized("model not allowed for this token")
		}
		if len(state.Principal.Projects) > 0 && state.Request.Meta.Project != "" && !lo.Contains(state.Principal.Projects, state.Request.Meta.Project) {
			return nil, gateway.Unauthorized("project not allowed for this token")
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
				return nil, gateway.Unauthorized("provider not allowed for this token")
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
			return candidateAllowedByScopes(state, candidate, scopes)
		})
		if len(filtered) == 0 {
			return nil, gateway.Unauthorized("request is not allowed by quota scope policy")
		}
		state.ReplaceCandidates(filtered)
		state.Scopes = scopes
		state.Estimate = maxEstimate(filtered)
		return next(ctx, state)
	}
}

func (r ResolveScopes) resolve(ctx context.Context, state *gateway.RequestState) ([]gateway.ScopedLimit, error) {
	if state == nil || state.Subject.KeyID == "" {
		return nil, nil
	}
	if r.Limits != nil {
		limit, ok, err := r.Limits.Get(ctx, state.Subject.KeyID)
		if err != nil {
			return nil, gateway.WrapError(http.StatusInternalServerError, "server_error", "quota_limit_lookup_failed", err)
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
		state.Request.Meta.Project = firstNonEmpty(headers.Get("X-Project-ID"), headers.Get("OpenAI-Project"), metadataProject(state.Request))
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
