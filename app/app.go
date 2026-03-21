package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"llmgw/api"
	"llmgw/gateway"
	"llmgw/observer"
	"llmgw/policy"
	"llmgw/proxy"
	"llmgw/store"

	"github.com/samber/lo"
)

type Service struct {
	configPath string
	config     *gateway.ConfigStore
	observer   *observer.Observer
	httpServer *http.Server
	listener   net.Listener
	limitClose io.Closer
}

func New(configPath string) (*Service, error) {
	cfg, err := gateway.LoadConfigFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	configStore := gateway.NewConfigStore(cfg)
	obsrv := observer.New(cfg.Telemetry.ServiceName)

	rateStore, quotaStore, limitStore, err := buildStores(cfg)
	if err != nil {
		return nil, fmt.Errorf("build stores: %w", err)
	}
	var limitCloser io.Closer
	if closer, ok := limitStore.(io.Closer); ok {
		limitCloser = closer
	}

	breaker := policy.NewBreaker()
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	providers := []gateway.Provider{
		proxy.New("openai", httpClient),
		proxy.New("anthropic", httpClient),
		proxy.New("gemini", httpClient),
	}
	attemptLimits := &policy.AttemptLimits{Rates: rateStore, Breakers: breaker}
	if state, ok := rateStore.(store.State); ok {
		attemptLimits.State = state
	}
	engine := gateway.NewEngine(
		configStore,
		providers,
		[]gateway.RequestInterceptor{
			observer.RequestMetrics{Obs: obsrv},
			policy.Auth{},
			policy.RequireUser{},
			policy.RequestSize{},
			policy.TokenValidation{Counters: tokenCounters(providers), Builders: effectiveBuilders(providers)},
			policy.ACL{},
			policy.ResolveScopes{Limits: limitStore},
			policy.Quota{Store: quotaStore, Obs: obsrv},
		},
		[]gateway.AttemptInterceptor{
			policy.AttemptHeaders{},
			observer.AttemptMetrics{Obs: obsrv},
			attemptLimits,
		},
	)
	quotaUsage, _ := quotaStore.(store.QuotaUsageStore)
	server := api.NewServer(engine, configStore, obsrv, limitStore, quotaUsage)

	ln, err := net.Listen("tcp", cfg.Server.Listen)
	if err != nil {
		if limitCloser != nil {
			_ = limitCloser.Close()
		}
		return nil, fmt.Errorf("listen: %w", err)
	}

	httpServer := &http.Server{
		Handler:           server.Handler(),
		ReadTimeout:       cfg.Server.ReadTimeout.Duration,
		WriteTimeout:      cfg.Server.WriteTimeout.Duration,
		IdleTimeout:       cfg.Server.IdleTimeout.Duration,
		ReadHeaderTimeout: cfg.Server.ReadTimeout.Duration,
	}

	return &Service{
		configPath: configPath,
		config:     configStore,
		observer:   obsrv,
		httpServer: httpServer,
		listener:   ln,
		limitClose: limitCloser,
	}, nil
}

func OpenAPIYAML(configPath string) ([]byte, error) {
	cfg, err := gateway.LoadConfigFile(configPath)
	if err != nil {
		return nil, err
	}
	return api.OpenAPIYAML(cfg)
}

func (s *Service) ShutdownTimeout() time.Duration {
	if s == nil || s.config == nil {
		return 10 * time.Second
	}
	cfg := s.config.Load()
	if cfg == nil {
		return 10 * time.Second
	}
	return cfg.Server.ShutdownTimeout.Duration
}

func (s *Service) Reload() error {
	if s == nil || s.config == nil {
		return errors.New("service not initialized")
	}
	cfg, err := s.config.Reload(s.configPath)
	if err != nil {
		if s.observer != nil {
			s.observer.Logger.Error("reload_failed", "error", err)
		}
		return err
	}
	if s.observer != nil {
		s.observer.Logger.Info("reloaded", "loaded_at", cfg.LoadedAt)
	}
	return nil
}

func (s *Service) Serve() error {
	if s == nil || s.httpServer == nil || s.listener == nil {
		return errors.New("service not initialized")
	}
	if s.observer != nil {
		s.observer.Logger.Info("listen", "addr", s.listener.Addr().String())
	}
	err := s.httpServer.Serve(s.listener)
	if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	var shutdownErr error
	if s.httpServer != nil {
		shutdownErr = s.httpServer.Shutdown(ctx)
	}
	var closeErr error
	if s.limitClose != nil {
		closeErr = s.limitClose.Close()
		s.limitClose = nil
	}
	if shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) && !errors.Is(shutdownErr, net.ErrClosed) {
		if closeErr != nil {
			return errors.Join(shutdownErr, closeErr)
		}
		return shutdownErr
	}
	return closeErr
}

func buildStores(cfg *gateway.Snapshot) (store.RateStore, store.QuotaStore, store.QuotaLimitStore, error) {
	limitStore := store.QuotaLimitStore(store.NewMemoryQuotaLimitStore())
	if cfg.Store.PostgresDSN != "" {
		pg, err := store.NewPostgresQuotaLimitStore(context.Background(), cfg.Store.PostgresDSN, cfg.Store.QuotaTable)
		if err != nil {
			return nil, nil, nil, err
		}
		limitStore = store.NewCachedQuotaLimitStore(pg, cfg.Store.LimitCacheTTL.Duration)
	}
	if cfg.Store.Mode == "redis" {
		redisStore := store.NewRedisStore(cfg.Store.RedisAddr, cfg.Store.RedisPasswordEnv, cfg.Store.RedisDB)
		return redisStore, redisStore, limitStore, nil
	}
	return store.NewMemoryRateStore(), store.NewMemoryQuotaStore(), limitStore, nil
}

func tokenCounters(providers []gateway.Provider) map[string]gateway.TokenCounter {
	return lo.FromEntries(lo.FilterMap(providers, func(provider gateway.Provider, _ int) (lo.Entry[string, gateway.TokenCounter], bool) {
		counter, ok := provider.(gateway.TokenCounter)
		if !ok {
			return lo.Entry[string, gateway.TokenCounter]{}, false
		}
		return lo.Entry[string, gateway.TokenCounter]{Key: counter.Name(), Value: counter}, true
	}))
}

func effectiveBuilders(providers []gateway.Provider) map[string]gateway.EffectiveParamBuilder {
	return lo.FromEntries(lo.FilterMap(providers, func(provider gateway.Provider, _ int) (lo.Entry[string, gateway.EffectiveParamBuilder], bool) {
		builder, ok := provider.(gateway.EffectiveParamBuilder)
		if !ok {
			return lo.Entry[string, gateway.EffectiveParamBuilder]{}, false
		}
		return lo.Entry[string, gateway.EffectiveParamBuilder]{Key: provider.Name(), Value: builder}, true
	}))
}
