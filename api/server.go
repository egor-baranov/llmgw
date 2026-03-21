package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"strings"
	"time"

	"llmgw/gateway"
	"llmgw/observer"
	proxyproviders "llmgw/proxy/providers"
	"llmgw/store"

	httpSwagger "github.com/swaggo/http-swagger/v2"
)

type Server struct {
	Gateway    *gateway.Engine
	Config     *gateway.ConfigStore
	Obs        *observer.Observer
	Limits     store.QuotaLimitStore
	QuotaUsage store.QuotaUsageStore
}

type requestDecoder func(r *http.Request, maxBytes int64) (*gateway.Request, error)

func NewServer(gw *gateway.Engine, cfg *gateway.ConfigStore, observer *observer.Observer, limits store.QuotaLimitStore, quotaUsage store.QuotaUsageStore) *Server {
	return &Server{Gateway: gw, Config: cfg, Obs: observer, Limits: limits, QuotaUsage: quotaUsage}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", methodHandler(http.MethodGet, s.healthz))
	mux.HandleFunc("/readyz", methodHandler(http.MethodGet, s.readyz))
	mux.HandleFunc("/openapi.json", methodHandler(http.MethodGet, s.openapiJSON))
	mux.HandleFunc("/openapi.yaml", methodHandler(http.MethodGet, s.openapiYAML))
	mux.HandleFunc("/docs", methodHandler(http.MethodGet, s.docsRedirect))
	mux.Handle("/docs/", httpSwagger.Handler(
		httpSwagger.URL("/openapi.json"),
		httpSwagger.DocExpansion("list"),
		httpSwagger.DomID("swagger-ui"),
		httpSwagger.DeepLinking(true),
		httpSwagger.PersistAuthorization(true),
	))
	mux.HandleFunc("/v1/models", methodHandler(http.MethodGet, s.models))
	mux.HandleFunc("/v1/limits", s.limits())
	mux.HandleFunc("/v1/chat/completions", methodHandler(http.MethodPost, s.handleProviderOperation("openai", gateway.OpChatCompletions, openAIProviderDecoder(gateway.OpChatCompletions))))
	mux.HandleFunc("/v1/responses", methodHandler(http.MethodPost, s.handleProviderOperation("openai", gateway.OpResponses, openAIProviderDecoder(gateway.OpResponses))))
	mux.HandleFunc("/v1/completions", methodHandler(http.MethodPost, s.handleProviderOperation("openai", gateway.OpCompletions, openAIProviderDecoder(gateway.OpCompletions))))
	mux.HandleFunc("/v1/embeddings", methodHandler(http.MethodPost, s.handleProviderOperation("openai", gateway.OpEmbeddings, openAIProviderDecoder(gateway.OpEmbeddings))))
	mux.HandleFunc("/v1/messages", methodHandler(http.MethodPost, s.handleProviderOperation("anthropic", gateway.OpChatCompletions, proxyproviders.AnthropicRequest)))
	mux.HandleFunc("/v1beta/models/", methodHandler(http.MethodPost, s.handleGeminiModelOperation("/v1beta/models/")))
	mux.HandleFunc("/v1/models/", methodHandler(http.MethodPost, s.handleGeminiModelOperation("/v1/models/")))
	if s.Obs != nil {
		mux.HandleFunc("/metrics", methodHandler(http.MethodGet, s.metrics))
	}
	return mux
}

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	cfg := s.Config.Load()
	if cfg == nil {
		writeError(w, gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded"))
		return
	}
	_ = writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   cfg.Models(),
	})
}

func (s *Server) handleProviderOperation(provider string, op gateway.Operation, decoder requestDecoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg := s.Config.Load()
		if cfg == nil {
			writeError(w, gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded"))
			return
		}
		env, err := decoder(r, cfg.Auth.MaxBodyBytes)
		if err != nil {
			writeError(w, err)
			return
		}
		env.Provider = provider
		env.Operation = op
		ctx, exec, err := s.execute(r, env)
		if err != nil {
			writeError(w, err)
			return
		}
		var actual gateway.Usage
		if exec.Result.RawStream != nil {
			actual, err = writeProviderStream(w, exec.Result, exec.MarkFirstByte)
		} else {
			actual, err = writeProviderResult(w, exec.Result)
		}
		_ = exec.Settle(ctx, actual, err)
		if err != nil {
			if !env.Stream {
				writeError(w, err)
			}
			return
		}
	}
}

func (s *Server) execute(r *http.Request, env *gateway.Request) (context.Context, *gateway.Execution, error) {
	env.Meta.RequestID = requestID(r)
	env.Meta.ReceivedAt = time.Now()
	ctx := r.Context()
	exec, err := s.Gateway.Execute(ctx, env)
	if err != nil {
		return nil, nil, err
	}
	return ctx, exec, nil
}

func openAIProviderDecoder(op gateway.Operation) requestDecoder {
	return func(r *http.Request, maxBytes int64) (*gateway.Request, error) {
		return proxyproviders.OpenAIRequest(r, op, maxBytes)
	}
}

func (s *Server) handleGeminiModelOperation(prefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		model, op, ok := parseGeminiOperation(r.URL.Path, prefix)
		if !ok {
			writeError(w, gateway.NewError(http.StatusNotFound, "invalid_request_error", "not_found", "endpoint not found"))
			return
		}
		req := withModelQuery(r, model)
		switch op {
		case gateway.OpChatCompletions:
			decoder := func(r *http.Request, maxBytes int64) (*gateway.Request, error) {
				out, err := proxyproviders.GeminiGenerateRequest(r, maxBytes)
				if err != nil {
					return nil, err
				}
				out.Model = model
				return out, nil
			}
			s.handleProviderOperation("gemini", gateway.OpChatCompletions, decoder)(w, req)
		case gateway.OpEmbeddings:
			decoder := func(r *http.Request, maxBytes int64) (*gateway.Request, error) {
				out, err := proxyproviders.GeminiEmbeddingRequest(r, maxBytes)
				if err != nil {
					return nil, err
				}
				out.Model = model
				return out, nil
			}
			s.handleProviderOperation("gemini", gateway.OpEmbeddings, decoder)(w, req)
		default:
			writeError(w, gateway.UnsupportedOperation("gemini operation is not supported"))
		}
	}
}

func parseGeminiOperation(path, prefix string) (model string, op gateway.Operation, ok bool) {
	rest, found := strings.CutPrefix(path, prefix)
	if !found || rest == "" {
		return "", "", false
	}
	switch {
	case strings.HasSuffix(rest, ":generateContent"):
		model = strings.TrimSuffix(rest, ":generateContent")
		op = gateway.OpChatCompletions
	case strings.HasSuffix(rest, ":embedContent"):
		model = strings.TrimSuffix(rest, ":embedContent")
		op = gateway.OpEmbeddings
	default:
		return "", "", false
	}
	model, err := url.PathUnescape(strings.TrimSpace(model))
	if err != nil || model == "" || strings.Contains(model, "/") {
		return "", "", false
	}
	return model, op, true
}

func withModelQuery(r *http.Request, model string) *http.Request {
	cloned := r.Clone(r.Context())
	urlCopy := *r.URL
	query := cloned.URL.Query()
	if strings.TrimSpace(query.Get("model")) == "" {
		query.Set("model", model)
	}
	urlCopy.RawQuery = query.Encode()
	cloned.URL = &urlCopy
	return cloned
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	_ = writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) readyz(w http.ResponseWriter, _ *http.Request) {
	if s.Config.Load() == nil {
		writeError(w, gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded"))
		return
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
		writeError(w, gateway.NewError(http.StatusInternalServerError, "server_error", "spec_generation_failed", err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) openapiJSON(w http.ResponseWriter, _ *http.Request) {
	body, err := OpenAPIJSON(s.Config.Load())
	if err != nil {
		writeError(w, gateway.NewError(http.StatusInternalServerError, "server_error", "spec_generation_failed", err.Error()))
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
	if value := r.Header.Get("X-Request-ID"); value != "" {
		return value
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "req_fallback"
	}
	return "req_" + hex.EncodeToString(buf[:])
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
