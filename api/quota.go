package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"llmgw/gateway"
)

type limitsResponse struct {
	KeyID            string              `json:"key_id"`
	Source           string              `json:"source"`
	Limits           gateway.LimitSpec   `json:"limits"`
	Usage            *gateway.QuotaUsage `json:"usage,omitempty"`
	UsageUnavailable bool                `json:"usage_unavailable,omitempty"`
}

func (s *Server) limits() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.getLimits(w, r)
		case http.MethodPut:
			s.putLimits(w, r)
		default:
			w.Header().Set("Allow", "GET, PUT")
			writeError(w, gateway.NewError(http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "method not allowed"))
		}
	}
}

func (s *Server) getLimits(w http.ResponseWriter, r *http.Request) {
	cfg, _, subject, err := s.authenticateLimitsRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}
	scope, source, found, err := s.resolveLimitsScope(r.Context(), cfg, subject)
	if err != nil {
		s.logInternalError(r, "quota_limit_lookup_failed", err)
		writeError(w, err)
		return
	}
	if !found {
		writeError(w, gateway.NewError(http.StatusNotFound, "invalid_request_error", "quota_limits_not_found", "no quota limits configured for this token"))
		return
	}
	resp := limitsResponse{
		KeyID:  subject.KeyID,
		Source: source,
		Limits: scope.Limits,
	}
	if s.QuotaUsage != nil {
		usage, err := s.QuotaUsage.GetUsage(r.Context(), scope)
		if err != nil {
			wrapped := gateway.WrapError(http.StatusInternalServerError, "server_error", "quota_usage_lookup_failed", err)
			s.logInternalError(r, "quota_usage_lookup_failed", wrapped)
			resp.UsageUnavailable = true
		} else {
			resp.Usage = &usage
		}
	}
	_ = writeJSON(w, http.StatusOK, resp)
}

func (s *Server) putLimits(w http.ResponseWriter, r *http.Request) {
	if s.Limits == nil {
		writeError(w, gateway.NewError(http.StatusServiceUnavailable, "server_error", "quota_limit_store_unavailable", "dynamic quota limit store is not configured"))
		return
	}
	cfg, principal, subject, err := s.authenticateLimitsRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if !principal.HasPermission(gateway.PermissionManageLimits) {
		writeError(w, gateway.NewError(http.StatusForbidden, "invalid_request_error", "permission_denied", "manage_limits permission is required"))
		return
	}
	defer r.Body.Close()
	maxBytes := cfg.Auth.MaxBodyBytes
	if maxBytes <= 0 {
		maxBytes = 4 << 20
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes))
	decoder.DisallowUnknownFields()
	var limit gateway.LimitSpec
	if err := decoder.Decode(&limit); err != nil {
		writeError(w, quotaJSONError(err))
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = errors.New("request body must contain exactly one JSON object")
		}
		writeError(w, quotaJSONError(err))
		return
	}
	if err := validateLimitSpec(cfg, limit); err != nil {
		writeError(w, err)
		return
	}
	if err := s.Limits.Put(r.Context(), subject.KeyID, limit); err != nil {
		wrapped := quotaLimitStoreError(r.Context(), err)
		s.logInternalError(r, "quota_limit_write_failed", wrapped)
		writeError(w, wrapped)
		return
	}
	resp := limitsResponse{
		KeyID:  subject.KeyID,
		Source: "dynamic",
		Limits: limit,
	}
	if s.QuotaUsage != nil {
		usage, err := s.QuotaUsage.GetUsage(r.Context(), gateway.ScopedLimit{
			Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: subject.KeyID},
			Limits: limit,
		})
		if err != nil {
			wrapped := gateway.WrapError(http.StatusInternalServerError, "server_error", "quota_usage_lookup_failed", err)
			s.logInternalError(r, "quota_usage_lookup_failed", wrapped)
			resp.UsageUnavailable = true
		} else {
			resp.Usage = &usage
		}
	}
	_ = writeJSON(w, http.StatusOK, resp)
}

func quotaJSONError(err error) error {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		return gateway.NewError(http.StatusRequestEntityTooLarge, "invalid_request_error", "body_too_large", "request body exceeds configured maximum")
	}
	return gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error())
}

func (s *Server) authenticateLimitsRequest(r *http.Request) (*gateway.Snapshot, *gateway.Principal, gateway.Subject, error) {
	cfg := s.Config.Load()
	if cfg == nil {
		return nil, nil, gateway.Subject{}, gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded")
	}
	principal, err := authenticateBearerRequest(cfg, r, false)
	if err != nil {
		return nil, nil, gateway.Subject{}, err
	}
	subject := principal.Subject()
	if subject.KeyID == "" {
		return nil, nil, gateway.Subject{}, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "missing_key_id", "authenticated token did not resolve to a quota key id")
	}
	return cfg, principal, subject, nil
}

func (s *Server) resolveLimitsScope(ctx context.Context, cfg *gateway.Snapshot, subject gateway.Subject) (gateway.ScopedLimit, string, bool, error) {
	if s.Limits != nil {
		limit, ok, err := s.Limits.Get(ctx, subject.KeyID)
		if err != nil {
			return gateway.ScopedLimit{}, "", false, quotaLimitStoreError(ctx, err)
		}
		if ok {
			return gateway.ScopedLimit{
				Ref:    gateway.ScopeRef{Kind: gateway.ScopeKey, ID: subject.KeyID},
				Limits: limit,
			}, "dynamic", true, nil
		}
	}
	scopes := cfg.ResolveQuotaScopes(subject)
	if len(scopes) == 0 {
		return gateway.ScopedLimit{}, "", false, nil
	}
	return scopes[0], "config", true, nil
}

func quotaLimitStoreError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return err
	}
	return gateway.NewError(
		http.StatusServiceUnavailable,
		"server_error",
		"quota_limit_store_unavailable",
		"quota limit service is temporarily unavailable",
	).WithCause(err).WithDisposition(false, false, false)
}

func validateLimitSpec(cfg *gateway.Snapshot, limit gateway.LimitSpec) error {
	if err := gateway.ValidateLimitSpec(limit); err != nil {
		return gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_limit", err.Error())
	}
	knownProviders := make(map[string]struct{})
	if cfg != nil {
		for _, route := range cfg.Routes {
			if route != nil && route.Provider != "" {
				knownProviders[route.Provider] = struct{}{}
			}
		}
	}
	for _, provider := range limit.ProviderAllowlist {
		if _, ok := knownProviders[provider]; !ok {
			return gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_limit", fmt.Sprintf("provider_allowlist contains unknown provider %q", provider))
		}
	}
	return nil
}
