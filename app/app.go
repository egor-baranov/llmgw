package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"llmgw/api"
	"llmgw/gateway"
	"llmgw/observer"
	"llmgw/policy"
	"llmgw/store"
)

type Service struct {
	configPath        string
	config            *gateway.ConfigStore
	providers         []gateway.Provider
	observer          *observer.Observer
	httpServer        *http.Server
	listener          net.Listener
	storeClose        []io.Closer
	upstreamTransport interface{ CloseIdleConnections() }
	reloadMu          sync.Mutex
	requestMu         sync.Mutex
	activeRequests    int
	shuttingDown      bool
	requestsDrained   chan struct{}
	drainClosed       bool
	shutdown          sync.Once
	shutdownErr       error
}

func New(configPath string) (*Service, error) {
	cfg, err := gateway.LoadConfigFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	configStore := gateway.NewConfigStore(cfg)
	obsrv := observer.New(cfg.Telemetry.ServiceName)
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	httpClient := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: transport,
	}
	providers := api.DefaultProviders(httpClient)
	if err := gateway.ValidateProviders(cfg, providers); err != nil {
		return nil, fmt.Errorf("validate providers: %w", err)
	}

	rateStore, quotaStore, limitStore, err := buildStores(cfg, obsrv)
	if err != nil {
		return nil, fmt.Errorf("build stores: %w", err)
	}
	storeClosers := uniqueClosers(rateStore, quotaStore, limitStore)
	healthChecks := uniqueHealthChecks(rateStore, quotaStore, limitStore)
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), cfg.Store.StartupTimeout.Duration)
	defer cancelStartup()
	for _, checker := range healthChecks {
		var err error
		if validator, ok := checker.(store.StartupValidator); ok {
			err = validator.ValidateStartup(startupCtx)
		} else {
			err = checker.Ping(startupCtx)
		}
		if err != nil {
			for _, closer := range storeClosers {
				_ = closer.Close()
			}
			return nil, fmt.Errorf("store readiness check: %w", err)
		}
	}

	breaker := policy.NewBreaker()
	attemptLimits := &policy.AttemptLimits{
		Rates:    rateStore,
		Breakers: breaker,
		OnBreakerUpdateError: func(operation string, err error) {
			if obsrv.Logger != nil {
				obsrv.Logger.Error("breaker_state_update_failed", "operation", operation, "error", err)
			}
		},
	}
	if state, ok := rateStore.(store.State); ok {
		attemptLimits.State = state
	}
	providerHooks := collectProviderPolicyHooks(providers)
	engine := gateway.NewEngine(
		configStore,
		providers,
		[]gateway.RequestInterceptor{
			observer.RequestMetrics{Obs: obsrv},
			policy.Auth{},
			policy.MetadataValidation{},
			policy.RequireUser{},
			policy.RequestSize{},
			policy.TokenValidation{
				Counters:   providerHooks.counters,
				Builders:   providerHooks.builders,
				Projectors: providerHooks.projectors,
			},
			policy.ACL{},
			gateway.NewCandidatePreflight(providers),
			policy.ResolveScopes{Limits: limitStore},
			policy.Quota{Store: quotaStore, Obs: obsrv},
		},
		[]gateway.AttemptInterceptor{
			attemptLimits,
			policy.AttemptHeaders{},
			observer.AttemptMetrics{Obs: obsrv},
		},
	)
	quotaUsage, _ := quotaStore.(store.QuotaUsageStore)
	server := api.NewServerWithIngresses(engine, configStore, obsrv, limitStore, quotaUsage, api.DefaultIngresses()...)
	server.Readiness = func(ctx context.Context) error {
		for _, checker := range healthChecks {
			if err := checker.Ping(ctx); err != nil {
				return err
			}
		}
		return nil
	}

	ln, err := net.Listen("tcp", cfg.Server.Listen)
	if err != nil {
		for _, closer := range storeClosers {
			_ = closer.Close()
		}
		return nil, fmt.Errorf("listen: %w", err)
	}

	service := &Service{
		configPath:        configPath,
		config:            configStore,
		providers:         append([]gateway.Provider(nil), providers...),
		observer:          obsrv,
		listener:          ln,
		storeClose:        storeClosers,
		upstreamTransport: transport,
	}
	httpServer := &http.Server{
		Handler:           service.trackRequests(server.Handler()),
		ReadTimeout:       cfg.Server.ReadTimeout.Duration,
		WriteTimeout:      cfg.Server.WriteTimeout.Duration,
		IdleTimeout:       cfg.Server.IdleTimeout.Duration,
		ReadHeaderTimeout: cfg.Server.ReadTimeout.Duration,
	}

	service.httpServer = httpServer
	return service, nil
}

func OpenAPIYAML(configPath string) ([]byte, error) {
	cfg, err := gateway.LoadConfigFile(configPath)
	if err != nil {
		return nil, err
	}
	if err := gateway.ValidateProviders(cfg, api.DefaultProviders(nil)); err != nil {
		return nil, fmt.Errorf("validate providers: %w", err)
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
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	cfg, err := gateway.LoadConfigFile(s.configPath)
	if err != nil {
		if s.observer != nil {
			s.observer.Logger.Error("reload_failed", "error", err)
		}
		return err
	}
	if err := gateway.ValidateProviders(cfg, s.providers); err != nil {
		if s.observer != nil {
			s.observer.Logger.Error("reload_failed", "error", err)
		}
		return fmt.Errorf("validate providers: %w", err)
	}
	current := s.config.Load()
	if current != nil {
		if !reflect.DeepEqual(current.Server, cfg.Server) ||
			!reflect.DeepEqual(current.Store, cfg.Store) ||
			!reflect.DeepEqual(current.Telemetry, cfg.Telemetry) {
			err := errors.New("reload changes restart-only server, store, or telemetry settings")
			if s.observer != nil {
				s.observer.Logger.Error("reload_failed", "error", err)
			}
			return err
		}
	}
	s.config.Swap(cfg)
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
	s.shutdown.Do(func() {
		drained := s.beginShutdown()
		var shutdownErr error
		if s.httpServer != nil {
			if err := s.httpServer.Shutdown(ctx); !expectedCloseError(err) {
				shutdownErr = err
				// Shutdown stops accepting new work but leaves active handlers
				// running when its context expires. Close their connections so
				// request contexts are canceled before deferred resource cleanup.
				if closeErr := s.httpServer.Close(); !expectedCloseError(closeErr) {
					shutdownErr = errors.Join(shutdownErr, closeErr)
				}
			}
		}
		var closeErr error
		if s.listener != nil {
			if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				closeErr = errors.Join(closeErr, err)
			}
		}
		if shutdownErr == nil {
			// A successful http.Server.Shutdown has drained all handlers. The
			// explicit tracker also covers handlers that are rejected at the
			// shutdown boundary and documents the store-lifetime invariant.
			<-drained
			closeErr = errors.Join(closeErr, s.closeOperationalResources())
		} else {
			select {
			case <-drained:
				closeErr = errors.Join(closeErr, s.closeOperationalResources())
			default:
				// Do not close Redis/Postgres while a handler can still settle
				// quota. Finish cleanup after the force-closed handlers return.
				go func() {
					<-drained
					if err := s.closeOperationalResources(); err != nil && s.observer != nil && s.observer.Logger != nil {
						s.observer.Logger.Error("shutdown_resource_close_failed", "error", err)
					}
				}()
			}
		}
		s.shutdownErr = errors.Join(shutdownErr, closeErr)
	})
	return s.shutdownErr
}

func (s *Service) trackRequests(next http.Handler) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.beginRequest() {
			w.Header().Set("Connection", "close")
			http.Error(w, "service is shutting down", http.StatusServiceUnavailable)
			return
		}
		defer s.endRequest()
		next.ServeHTTP(w, r)
	})
}

func (s *Service) beginRequest() bool {
	if s == nil {
		return false
	}
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if s.shuttingDown {
		return false
	}
	if s.requestsDrained == nil {
		s.requestsDrained = make(chan struct{})
	}
	s.activeRequests++
	return true
}

func (s *Service) endRequest() {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if s.activeRequests > 0 {
		s.activeRequests--
	}
	if s.shuttingDown && s.activeRequests == 0 && !s.drainClosed {
		close(s.requestsDrained)
		s.drainClosed = true
	}
}

func (s *Service) beginShutdown() <-chan struct{} {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if s.requestsDrained == nil {
		s.requestsDrained = make(chan struct{})
	}
	s.shuttingDown = true
	if s.activeRequests == 0 && !s.drainClosed {
		close(s.requestsDrained)
		s.drainClosed = true
	}
	return s.requestsDrained
}

func (s *Service) closeOperationalResources() error {
	if s.upstreamTransport != nil {
		s.upstreamTransport.CloseIdleConnections()
	}
	var err error
	for _, closer := range s.storeClose {
		err = errors.Join(err, closer.Close())
	}
	return err
}

func expectedCloseError(err error) bool {
	return err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed)
}

func uniqueClosers(values ...any) []io.Closer {
	seen := make(map[io.Closer]struct{})
	out := make([]io.Closer, 0, len(values))
	for _, value := range values {
		closer, ok := value.(io.Closer)
		if !ok || closer == nil {
			continue
		}
		if _, ok := seen[closer]; ok {
			continue
		}
		seen[closer] = struct{}{}
		out = append(out, closer)
	}
	return out
}

func uniqueHealthChecks(values ...any) []store.HealthChecker {
	seen := make(map[store.HealthChecker]struct{})
	out := make([]store.HealthChecker, 0, len(values))
	for _, value := range values {
		checker, ok := value.(store.HealthChecker)
		if !ok || checker == nil {
			continue
		}
		if _, ok := seen[checker]; ok {
			continue
		}
		seen[checker] = struct{}{}
		out = append(out, checker)
	}
	return out
}

func buildStores(cfg *gateway.Snapshot, obsrv *observer.Observer) (store.RateStore, store.QuotaStore, store.QuotaLimitStore, error) {
	limitStore := store.QuotaLimitStore(store.NewMemoryQuotaLimitStore())
	if cfg.Store.PostgresDSN != "" {
		startupCtx, cancel := context.WithTimeout(context.Background(), cfg.Store.StartupTimeout.Duration)
		defer cancel()
		pg, err := store.NewPostgresRefreshingQuotaLimitStore(
			startupCtx,
			cfg.Store.PostgresDSN,
			cfg.Store.QuotaTable,
			cfg.Store.LimitCacheTTL.Duration,
		)
		if err != nil {
			return nil, nil, nil, err
		}
		pg.SetRefreshErrorHook(func(err error) {
			if obsrv != nil && obsrv.Metrics != nil {
				if err != nil {
					obsrv.Metrics.QuotaLimitSnapshotStaleGauge().Set(1)
				} else {
					obsrv.Metrics.QuotaLimitSnapshotStaleGauge().Set(0)
				}
			}
			if err != nil && obsrv != nil && obsrv.Logger != nil {
				obsrv.Logger.Error("quota_limit_snapshot_refresh_failed", "error", err)
			}
		})
		limitStore = pg
	}
	if cfg.Store.Mode == "redis" {
		var redisStore *store.RedisStore
		var err error
		if cfg.Store.RedisURL != "" {
			redisStore, err = store.NewRedisStoreFromURL(cfg.Store.RedisURL, cfg.Store.RedisNamespace)
			if err != nil {
				if closer, ok := limitStore.(io.Closer); ok {
					_ = closer.Close()
				}
				return nil, nil, nil, err
			}
		} else {
			redisStore = store.NewRedisStoreWithNamespace(cfg.Store.RedisAddr, cfg.Store.RedisPasswordEnv, cfg.Store.RedisDB, cfg.Store.RedisNamespace)
		}
		return redisStore, redisStore, limitStore, nil
	}
	return store.NewMemoryRateStore(), store.NewMemoryQuotaStore(), limitStore, nil
}

type providerPolicyHooks struct {
	counters   map[string]gateway.TokenCounter
	builders   map[string]gateway.EffectiveParamBuilder
	projectors map[string]gateway.TokenProjector
}

func collectProviderPolicyHooks(providers []gateway.Provider) providerPolicyHooks {
	hooks := providerPolicyHooks{
		counters:   make(map[string]gateway.TokenCounter),
		builders:   make(map[string]gateway.EffectiveParamBuilder),
		projectors: make(map[string]gateway.TokenProjector),
	}
	for _, provider := range providers {
		if counter, ok := provider.(gateway.TokenCounter); ok {
			hooks.counters[counter.Name()] = counter
		}
		if builder, ok := provider.(gateway.EffectiveParamBuilder); ok {
			hooks.builders[provider.Name()] = builder
		}
		if projector, ok := provider.(gateway.TokenProjector); ok {
			hooks.projectors[provider.Name()] = projector
		}
	}
	return hooks
}
