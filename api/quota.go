package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"llmgw/gateway"
	"llmgw/policy"
)

type limitsResponse struct {
	KeyID  string              `json:"key_id"`
	Source string              `json:"source"`
	Limits gateway.LimitSpec   `json:"limits"`
	Usage  *gateway.QuotaUsage `json:"usage,omitempty"`
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
	cfg, subject, err := s.authenticateLimitsRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}
	scope, source, found, err := s.resolveLimitsScope(r.Context(), cfg, subject)
	if err != nil {
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
			writeError(w, gateway.WrapError(http.StatusInternalServerError, "server_error", "quota_usage_lookup_failed", err))
			return
		}
		resp.Usage = &usage
	}
	_ = writeJSON(w, http.StatusOK, resp)
}

func (s *Server) putLimits(w http.ResponseWriter, r *http.Request) {
	if s.Limits == nil {
		writeError(w, gateway.NewError(http.StatusServiceUnavailable, "server_error", "quota_limit_store_unavailable", "dynamic quota limit store is not configured"))
		return
	}
	_, subject, err := s.authenticateLimitsRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}
	defer r.Body.Close()
	var limit gateway.LimitSpec
	if err := json.NewDecoder(r.Body).Decode(&limit); err != nil {
		writeError(w, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error()))
		return
	}
	if err := validateLimitSpec(limit); err != nil {
		writeError(w, err)
		return
	}
	if err := s.Limits.Put(r.Context(), subject.KeyID, limit); err != nil {
		writeError(w, gateway.WrapError(http.StatusInternalServerError, "server_error", "quota_limit_write_failed", err))
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
			writeError(w, gateway.WrapError(http.StatusInternalServerError, "server_error", "quota_usage_lookup_failed", err))
			return
		}
		resp.Usage = &usage
	}
	_ = writeJSON(w, http.StatusOK, resp)
}

func (s *Server) authenticateLimitsRequest(r *http.Request) (*gateway.Snapshot, gateway.Subject, error) {
	cfg := s.Config.Load()
	if cfg == nil {
		return nil, gateway.Subject{}, gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded")
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return nil, gateway.Subject{}, gateway.Unauthorized("missing bearer token")
	}
	principal, err := policy.AuthenticatePrincipal(cfg, strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, gateway.Subject{}, err
	}
	subject := principal.Subject()
	if subject.KeyID == "" {
		return nil, gateway.Subject{}, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "missing_key_id", "authenticated token did not resolve to a quota key id")
	}
	return cfg, subject, nil
}

func (s *Server) resolveLimitsScope(ctx context.Context, cfg *gateway.Snapshot, subject gateway.Subject) (gateway.ScopedLimit, string, bool, error) {
	if s.Limits != nil {
		limit, ok, err := s.Limits.Get(ctx, subject.KeyID)
		if err != nil {
			return gateway.ScopedLimit{}, "", false, gateway.WrapError(http.StatusInternalServerError, "server_error", "quota_limit_lookup_failed", err)
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

func validateLimitSpec(limit gateway.LimitSpec) error {
	for _, field := range []struct {
		value int64
		name  string
	}{
		{limit.RPM, "rpm"},
		{limit.TPM, "tpm"},
		{limit.MaxParallel, "max_parallel"},
		{limit.MaxSpendMicros, "max_spend_micros"},
		{limit.SoftSpendMicros, "soft_spend_micros"},
		{limit.DailyTokens, "daily_tokens"},
		{limit.MonthlyTokens, "monthly_tokens"},
		{limit.MaxInputTokens, "max_input_tokens"},
		{limit.MaxOutputTokens, "max_output_tokens"},
	} {
		if field.value < 0 {
			return gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_limit", field.name+" must be greater than or equal to zero")
		}
	}
	if limit.BudgetDuration.Duration < 0 {
		return gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_limit", "budget_duration must be greater than or equal to zero")
	}
	return nil
}
