package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"llmgw/gateway"
	"llmgw/observer"
	"llmgw/policy"
	"llmgw/store"

	httpSwagger "github.com/swaggo/http-swagger/v2"
)

type Server struct {
	Gateway    *gateway.Engine
	Config     *gateway.ConfigStore
	Obs        *observer.Observer
	Limits     store.QuotaLimitStore
	QuotaUsage store.QuotaUsageStore
	Readiness  func(context.Context) error
	ingresses  []Ingress
}

func (s *Server) logInternalError(r *http.Request, event string, err error) {
	if err == nil || gateway.AsAPIError(err).Status < http.StatusInternalServerError || s.Obs == nil || s.Obs.Logger == nil {
		return
	}
	s.Obs.Logger.Error(event, "request_id", requestID(r), "error_code", gateway.AsAPIError(err).Code, "error", observer.SafeErrorMessage(err))
}

type requestIDContextKey struct{}

func NewServer(gw *gateway.Engine, cfg *gateway.ConfigStore, observer *observer.Observer, limits store.QuotaLimitStore, quotaUsage store.QuotaUsageStore) *Server {
	return NewServerWithIngresses(gw, cfg, observer, limits, quotaUsage)
}

// NewServerWithIngresses builds a server with an explicit provider-ingress
// registry. An empty registry uses DefaultIngresses for compatibility.
func NewServerWithIngresses(gw *gateway.Engine, cfg *gateway.ConfigStore, observer *observer.Observer, limits store.QuotaLimitStore, quotaUsage store.QuotaUsageStore, ingresses ...Ingress) *Server {
	if len(ingresses) == 0 {
		ingresses = DefaultIngresses()
	}
	return &Server{
		Gateway:    gw,
		Config:     cfg,
		Obs:        observer,
		Limits:     limits,
		QuotaUsage: quotaUsage,
		ingresses:  append([]Ingress(nil), ingresses...),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	ingresses := s.configuredIngresses()
	mux.HandleFunc("/healthz", methodHandler(http.MethodGet, s.healthz))
	mux.HandleFunc("/readyz", methodHandler(http.MethodGet, s.readyz))
	mux.Handle("/openapi.json", s.protectOperations(methodHandler(http.MethodGet, s.openapiJSON)))
	mux.Handle("/openapi.yaml", s.protectOperations(methodHandler(http.MethodGet, s.openapiYAML)))
	mux.Handle("/docs", s.protectOperations(methodHandler(http.MethodGet, s.docsRedirect)))
	mux.Handle("/docs/", s.protectOperations(httpSwagger.Handler(
		httpSwagger.URL("/openapi.json"),
		httpSwagger.DocExpansion("list"),
		httpSwagger.DomID("swagger-ui"),
		httpSwagger.DeepLinking(true),
		httpSwagger.PersistAuthorization(true),
	)))
	mux.HandleFunc("/v1/models", methodHandler(http.MethodGet, s.models))
	mux.HandleFunc("/v1/limits", s.limits())
	s.registerIngresses(mux, ingresses)
	if s.Obs != nil {
		mux.Handle("/metrics", s.protectOperations(methodHandler(http.MethodGet, s.metrics)))
	}
	// ServeMux's default fallback is text/plain, which would violate the public
	// provider-compatible error contract for unknown API paths.
	mux.HandleFunc("/", s.notFoundHandler(ingresses))
	return withRequestID(mux)
}

// protectOperations keeps deployment metadata and traffic labels off the
// public data-plane surface. Anonymous access remains possible only when it is
// explicitly enabled for the whole gateway (useful for local development).
func (s *Server) protectOperations(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := s.Config.Load()
		if cfg == nil {
			writeError(w, gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded"))
			return
		}
		principal, err := authenticateBearerRequest(cfg, r, true)
		if err != nil {
			writeError(w, err)
			return
		}
		if principal != nil && !principal.HasPermission(gateway.PermissionViewOperations) {
			writeError(w, gateway.NewError(http.StatusForbidden, "invalid_request_error", "permission_denied", "view_operations permission is required"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	cfg := s.Config.Load()
	if cfg == nil {
		writeError(w, gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded"))
		return
	}
	principal, err := authenticateBearerRequest(cfg, r, true)
	if err != nil {
		writeError(w, err)
		return
	}
	subject := gateway.Subject{KeyID: "anonymous"}
	if principal != nil {
		subject = principal.Subject()
	}
	var quotaLimit *gateway.LimitSpec
	if scope, _, found, err := s.resolveLimitsScope(r.Context(), cfg, subject); err != nil {
		writeError(w, err)
		return
	} else if found {
		limit := scope.Limits
		quotaLimit = &limit
	}
	_ = writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   modelsForPrincipal(cfg, principal, quotaLimit),
	})
}

func authenticateBearerRequest(cfg *gateway.Snapshot, r *http.Request, permitAnonymous bool) (*gateway.Principal, error) {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth == "" && permitAnonymous && cfg.Auth.AllowAnonymous {
		return nil, nil
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return nil, gateway.Unauthorized("missing bearer token")
	}
	return policy.AuthenticatePrincipal(cfg, strings.TrimSpace(parts[1]))
}

func modelsForPrincipal(cfg *gateway.Snapshot, principal *gateway.Principal, quotaLimit *gateway.LimitSpec) []gateway.ModelDescriptor {
	models := cfg.Models()
	filtered := make([]gateway.ModelDescriptor, 0, len(models))
	for _, model := range models {
		if principal != nil && len(principal.Models) > 0 && !containsString(principal.Models, model.ID) {
			continue
		}
		if quotaLimit != nil && len(quotaLimit.ModelAllowlist) > 0 && !containsString(quotaLimit.ModelAllowlist, model.ID) {
			continue
		}
		visibleRoute := false
		for _, route := range cfg.Routes {
			if route == nil || route.Model != model.ID {
				continue
			}
			if principal != nil && len(principal.Providers) > 0 && !containsString(principal.Providers, route.Provider) {
				continue
			}
			if quotaLimit != nil && len(quotaLimit.ProviderAllowlist) > 0 && !containsString(quotaLimit.ProviderAllowlist, route.Provider) {
				continue
			}
			visibleRoute = true
			break
		}
		if !visibleRoute {
			continue
		}
		filtered = append(filtered, model)
	}
	return filtered
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (s *Server) handleIngressOperation(ingress Ingress, op gateway.Operation, decoder RequestDecoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provider := ingress.providerName()
		cfg := s.Config.Load()
		if cfg == nil {
			err := gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded")
			s.observeIngressError(r, provider, op, err)
			ingress.writeError(w, err)
			return
		}
		principal, err := ingress.authenticate(cfg, r)
		if err != nil {
			s.observeIngressError(r, provider, op, err)
			ingress.writeError(w, err)
			return
		}
		if principal != nil {
			r = r.WithContext(policy.WithAuthenticatedPrincipal(r.Context(), principal))
		}
		env, err := decoder(r, cfg.Auth.MaxBodyBytes)
		if err != nil {
			s.observeIngressError(r, provider, op, err)
			ingress.writeError(w, err)
			return
		}
		if env == nil {
			err = gateway.NewError(http.StatusInternalServerError, "server_error", "invalid_ingress_request", "ingress decoder returned no request")
			s.observeIngressError(r, provider, op, err)
			ingress.writeError(w, err)
			return
		}
		env.Provider = provider
		env.Operation = op
		ctx, exec, err := s.execute(r, cfg, env)
		w.Header().Set("X-Request-ID", env.Meta.RequestID)
		if err != nil {
			ingress.writeError(w, err)
			return
		}
		var actual gateway.Usage
		var responseErr error
		defer func() {
			panicValue := recover()
			if panicValue != nil && responseErr == nil {
				responseErr = fmt.Errorf("response handling panicked")
			}
			settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			settleErr := exec.Settle(settleCtx, actual, responseErr)
			cancel()
			if settleErr != nil && s.Obs != nil && s.Obs.Logger != nil {
				s.Obs.Logger.Error("settlement_failed", "request_id", env.Meta.RequestID, "error", observer.SafeErrorMessage(settleErr))
			}
			if responseErr != nil && s.Obs != nil && s.Obs.Logger != nil {
				s.Obs.Logger.Warn("response_write_failed", "request_id", env.Meta.RequestID, "error", observer.SafeErrorMessage(responseErr))
			}
			if panicValue != nil {
				panic(panicValue)
			}
		}()
		if exec.Result.RawStream != nil {
			actual, responseErr = writeProviderStream(w, exec.Result, exec.MarkFirstByte, cfg.Server.StreamWriteTimeout.Duration)
		} else {
			actual, responseErr = writeProviderResult(w, exec.Result, cfg.Server.StreamWriteTimeout.Duration)
		}
	}
}

func (s *Server) execute(r *http.Request, cfg *gateway.Snapshot, env *gateway.Request) (context.Context, *gateway.Execution, error) {
	env.Meta.RequestID = requestID(r)
	env.Meta.ReceivedAt = time.Now()
	ctx := r.Context()
	exec, err := s.Gateway.ExecuteWithSnapshot(ctx, cfg, env)
	if err != nil {
		return nil, nil, err
	}
	return ctx, exec, nil
}

func (s *Server) observeIngressError(r *http.Request, provider string, op gateway.Operation, err error) {
	if s.Obs == nil {
		return
	}
	apiErr := gateway.AsAPIError(err)
	if s.Obs.Metrics != nil {
		s.Obs.Metrics.RequestCounter(string(op), "unknown", "error").Inc()
	}
	if s.Obs.Logger != nil {
		s.Obs.Logger.Info("request",
			"request_id", requestID(r),
			"operation", string(op),
			"model", "unknown",
			"provider", provider,
			"status", "error",
			"error_code", apiErr.Code,
		)
	}
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	_ = writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.Config.Load() == nil {
		writeError(w, gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded"))
		return
	}
	if s.Readiness != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		err := s.Readiness(ctx)
		cancel()
		if err != nil {
			writeError(w, gateway.NewError(http.StatusServiceUnavailable, "server_error", "dependency_not_ready", "required dependency is unavailable"))
			return
		}
	}
	_ = writeJSON(w, http.StatusOK, map[string]any{"ready": true})
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	if s.Obs == nil || s.Obs.Metrics == nil || s.Obs.Metrics.Set == nil {
		writeError(w, gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "metrics not configured"))
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	s.Obs.Metrics.Set.WritePrometheus(w)
}

func (s *Server) openapiYAML(w http.ResponseWriter, _ *http.Request) {
	body, err := OpenAPIYAML(s.Config.Load())
	if err != nil {
		writeError(w, gateway.NewError(http.StatusInternalServerError, "server_error", "spec_generation_failed", "OpenAPI document generation failed"))
		return
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) openapiJSON(w http.ResponseWriter, _ *http.Request) {
	body, err := OpenAPIJSON(s.Config.Load())
	if err != nil {
		writeError(w, gateway.NewError(http.StatusInternalServerError, "server_error", "spec_generation_failed", "OpenAPI document generation failed"))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) docsRedirect(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/docs/index.html", http.StatusMovedPermanently)
}

func requestID(r *http.Request) string {
	if r != nil {
		if value, ok := r.Context().Value(requestIDContextKey{}).(string); ok && value != "" {
			return value
		}
	}
	if value := strings.TrimSpace(r.Header.Get("X-Request-ID")); validRequestID(value) {
		return value
	}
	return generatedRequestID()
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := requestID(r)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDContextKey{}, id)))
	})
}

func generatedRequestID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "req_" + hex.EncodeToString([]byte(time.Now().Format("150405.000000000")))
	}
	return "req_" + hex.EncodeToString(buf[:])
}

func validRequestID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if r < 0x21 || r > 0x7e {
			return false
		}
	}
	return true
}

func methodHandler(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			writeError(w, gateway.NewError(http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "method not allowed"))
			return
		}
		next(w, r)
	}
}
