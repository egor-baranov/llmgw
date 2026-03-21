package observer

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"llmgw/gateway"

	vmmetrics "github.com/VictoriaMetrics/metrics"
)

type Observer struct {
	Logger  *slog.Logger
	Metrics *Metrics
	Tracer  Tracer
}

type Metrics struct {
	Set      *vmmetrics.Set
	InFlight *vmmetrics.Gauge
}

type Attribute struct {
	Key   string
	Value string
}

type Span interface {
	End()
	RecordError(error)
}

type Tracer interface {
	Start(ctx context.Context, name string, attrs ...Attribute) (context.Context, Span)
}

type NoopTracer struct{}

type noopSpan struct{}

func (NoopTracer) Start(ctx context.Context, _ string, _ ...Attribute) (context.Context, Span) {
	return ctx, noopSpan{}
}

func (noopSpan) End() {}

func (noopSpan) RecordError(error) {}

func New(service string) *Observer {
	if service == "" {
		service = "llmgw"
	}
	vmmetrics.ExposeMetadata(true)
	set := vmmetrics.NewSet()
	metrics := &Metrics{
		Set:      set,
		InFlight: set.NewGauge("llmgw_inflight_requests", nil),
	}
	return &Observer{
		Logger:  slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})),
		Metrics: metrics,
		Tracer:  NoopTracer{},
	}
}

func (m *Metrics) RequestCounter(operation, model, status string) *vmmetrics.Counter {
	return m.Set.GetOrCreateCounter(labeledMetric("llmgw_requests_total",
		"operation", operation,
		"model", model,
		"status", status,
	))
}

func (m *Metrics) AttemptCounter(provider, route, status string) *vmmetrics.Counter {
	return m.Set.GetOrCreateCounter(labeledMetric("llmgw_attempts_total",
		"provider", provider,
		"route", route,
		"status", status,
	))
}

func (m *Metrics) LatencyHistogram(operation, model string) *vmmetrics.PrometheusHistogram {
	return m.Set.GetOrCreatePrometheusHistogram(labeledMetric("llmgw_request_duration_seconds",
		"operation", operation,
		"model", model,
	))
}

func (m *Metrics) QuotaDeniedCounter(scope, reason string) *vmmetrics.Counter {
	return m.Set.GetOrCreateCounter(labeledMetric("llmgw_quota_denied_total",
		"scope", scope,
		"reason", reason,
	))
}

func (m *Metrics) QuotaReservedTokensCounter(scope string) *vmmetrics.Counter {
	return m.Set.GetOrCreateCounter(labeledMetric("llmgw_quota_reserved_tokens_total",
		"scope", scope,
	))
}

func (m *Metrics) QuotaCommittedSpendCounter(scope string) *vmmetrics.Counter {
	return m.Set.GetOrCreateCounter(labeledMetric("llmgw_quota_committed_spend_micros_total",
		"scope", scope,
	))
}

func (m *Metrics) QuotaRefundedSpendCounter(scope string) *vmmetrics.Counter {
	return m.Set.GetOrCreateCounter(labeledMetric("llmgw_quota_refunded_spend_micros_total",
		"scope", scope,
	))
}

type RequestMetrics struct {
	Obs *Observer
}

func (m RequestMetrics) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if m.Obs == nil {
			return next(ctx, state)
		}
		start := time.Now()
		m.Obs.Metrics.InFlight.Inc()
		ctx, span := m.Obs.Tracer.Start(ctx, "gateway.request",
			Attribute{Key: "request.id", Value: state.Request.Meta.RequestID},
			Attribute{Key: "operation", Value: string(state.Request.Operation)},
			Attribute{Key: "model", Value: state.Request.Model},
		)
		defer span.End()
		exec, err := next(ctx, state)
		status := "ok"
		if err != nil {
			status = "error"
			span.RecordError(err)
		}
		m.Obs.Metrics.InFlight.Dec()
		m.Obs.Metrics.RequestCounter(string(state.Request.Operation), state.Request.Model, status).Inc()
		m.Obs.Metrics.LatencyHistogram(string(state.Request.Operation), state.Request.Model).UpdateDuration(start)
		attrs := []any{
			slog.String("request_id", state.Request.Meta.RequestID),
			slog.String("operation", string(state.Request.Operation)),
			slog.String("model", state.Request.Model),
			slog.String("status", status),
			slog.Duration("duration", time.Since(start)),
		}
		if exec != nil && exec.Attempt != nil {
			attrs = append(attrs,
				slog.String("route", exec.Attempt.Route.Route.Name),
				slog.String("provider", exec.Attempt.Route.Route.Provider),
			)
		}
		m.Obs.Logger.Info("request", attrs...)
		return exec, err
	}
}

type AttemptMetrics struct {
	Obs *Observer
}

func (m AttemptMetrics) WrapAttempt(next gateway.AttemptHandler) gateway.AttemptHandler {
	return func(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt) (*gateway.Result, error) {
		if m.Obs == nil {
			return next(ctx, state, attempt)
		}
		ctx, span := m.Obs.Tracer.Start(ctx, "gateway.attempt",
			Attribute{Key: "provider", Value: attempt.Route.Route.Provider},
			Attribute{Key: "route", Value: attempt.Route.Route.Name},
			Attribute{Key: "request.id", Value: state.Request.Meta.RequestID},
		)
		defer span.End()
		result, err := next(ctx, state, attempt)
		status := "ok"
		if err != nil {
			status = "error"
			span.RecordError(err)
		}
		m.Obs.Metrics.AttemptCounter(attempt.Route.Route.Provider, attempt.Route.Route.Name, status).Inc()
		if result != nil {
			m.Obs.Logger.Info("attempt",
				slog.String("request_id", state.Request.Meta.RequestID),
				slog.String("route", attempt.Route.Route.Name),
				slog.String("provider", attempt.Route.Route.Provider),
				slog.String("status", status),
			)
		}
		return result, err
	}
}

func labeledMetric(name string, labels ...string) string {
	if len(labels)%2 != 0 {
		panic("observer: labels must be key/value pairs")
	}
	if len(labels) == 0 {
		return name
	}
	var b strings.Builder
	b.Grow(len(name) + len(labels)*12)
	b.WriteString(name)
	b.WriteByte('{')
	for i := 0; i < len(labels); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(labels[i])
		b.WriteByte('=')
		b.WriteString(strconv.Quote(labels[i+1]))
	}
	b.WriteByte('}')
	return b.String()
}
