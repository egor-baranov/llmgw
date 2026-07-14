package observer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"llmgw/gateway"

	vmmetrics "github.com/VictoriaMetrics/metrics"
)

type Observer struct {
	ServiceName string
	Logger      *slog.Logger
	Metrics     *Metrics
	Tracer      Tracer
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
		ServiceName: service,
		Logger: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With(
			slog.String("service", service),
		),
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

func (m *Metrics) QuotaLimitSnapshotStaleGauge() *vmmetrics.Gauge {
	return m.Set.GetOrCreateGauge("llmgw_quota_limit_snapshot_stale", nil)
}

func (m *Metrics) QuotaSoftSpendExceededCounter(scope string) *vmmetrics.Counter {
	return m.Set.GetOrCreateCounter(labeledMetric("llmgw_quota_soft_spend_exceeded_total",
		"scope", scope,
	))
}

type RequestMetrics struct {
	Obs *Observer
}

func (m RequestMetrics) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if m.Obs == nil || m.Obs.Metrics == nil || m.Obs.Tracer == nil {
			return next(ctx, state)
		}
		start := time.Now()
		m.Obs.Metrics.InFlight.Inc()
		model := boundedModelLabel(state)
		ctx, span := m.Obs.Tracer.Start(ctx, "gateway.request",
			Attribute{Key: "service.name", Value: m.Obs.ServiceName},
			Attribute{Key: "request.id", Value: state.Request.Meta.RequestID},
			Attribute{Key: "operation", Value: string(state.Request.Operation)},
			Attribute{Key: "model", Value: model},
		)
		var once sync.Once
		finish := func(exec *gateway.Execution, err error) {
			once.Do(func() {
				status := "ok"
				if err != nil {
					status = "error"
					span.RecordError(errors.New(SafeErrorMessage(err)))
				}
				span.End()
				m.Obs.Metrics.InFlight.Dec()
				m.Obs.Metrics.RequestCounter(string(state.Request.Operation), model, status).Inc()
				m.Obs.Metrics.LatencyHistogram(string(state.Request.Operation), model).UpdateDuration(start)
				if m.Obs.Logger == nil {
					return
				}
				attrs := []any{
					slog.String("request_id", state.Request.Meta.RequestID),
					slog.String("operation", string(state.Request.Operation)),
					slog.String("model", model),
					slog.String("status", status),
					slog.Duration("duration", time.Since(start)),
				}
				if exec != nil && exec.Attempt != nil && exec.Attempt.Route.Route != nil {
					attrs = append(attrs,
						slog.String("route", exec.Attempt.Route.Route.Name),
						slog.String("provider", exec.Attempt.Route.Route.Provider),
					)
				}
				if err != nil {
					attrs = append(attrs,
						slog.String("error_code", errorCode(err)),
						slog.String("error", SafeErrorMessage(err)),
					)
				}
				m.Obs.Logger.Info("request", attrs...)
			})
		}
		var exec *gateway.Execution
		var err error
		defer func() {
			if panicValue := recover(); panicValue != nil {
				finish(exec, errors.New("request pipeline panicked"))
				panic(panicValue)
			}
		}()
		exec, err = next(ctx, state)
		if err != nil {
			finish(nil, err)
			return nil, err
		}
		if exec == nil {
			err = gateway.NewError(500, "server_error", "empty_execution", "gateway returned no execution")
			finish(nil, err)
			return nil, err
		}
		previous := exec.Finalize
		exec.Finalize = func(finalizeCtx context.Context, actual gateway.Usage, callErr error) (finalizeErr error) {
			defer func() {
				if panicValue := recover(); panicValue != nil {
					finish(exec, errors.New("request settlement panicked"))
					panic(panicValue)
				}
			}()
			if previous != nil {
				finalizeErr = previous(finalizeCtx, actual, callErr)
			}
			observedErr := callErr
			if finalizeErr != nil {
				observedErr = finalizeErr
			}
			finish(exec, observedErr)
			return finalizeErr
		}
		return exec, nil
	}
}

type AttemptMetrics struct {
	Obs *Observer
}

func (m AttemptMetrics) WrapAttempt(next gateway.AttemptHandler) gateway.AttemptHandler {
	return func(ctx context.Context, state *gateway.RequestState, attempt *gateway.Attempt) (*gateway.Result, error) {
		if m.Obs == nil || m.Obs.Metrics == nil || m.Obs.Tracer == nil {
			return next(ctx, state, attempt)
		}
		start := time.Now()
		ctx, span := m.Obs.Tracer.Start(ctx, "gateway.attempt",
			Attribute{Key: "service.name", Value: m.Obs.ServiceName},
			Attribute{Key: "provider", Value: attempt.Route.Route.Provider},
			Attribute{Key: "route", Value: attempt.Route.Route.Name},
			Attribute{Key: "request.id", Value: requestIDForState(state)},
		)
		var once sync.Once
		finish := func(err error) {
			once.Do(func() {
				status := "ok"
				if err != nil {
					status = "error"
					span.RecordError(errors.New(SafeErrorMessage(err)))
				}
				span.End()
				m.Obs.Metrics.AttemptCounter(attempt.Route.Route.Provider, attempt.Route.Route.Name, status).Inc()
				if m.Obs.Logger == nil {
					return
				}
				attrs := []any{
					slog.String("request_id", requestIDForState(state)),
					slog.String("route", attempt.Route.Route.Name),
					slog.String("provider", attempt.Route.Route.Provider),
					slog.String("status", status),
					slog.Duration("duration", time.Since(start)),
				}
				if err != nil {
					attrs = append(attrs,
						slog.String("error_code", errorCode(err)),
						slog.String("error", SafeErrorMessage(err)),
					)
				}
				m.Obs.Logger.Info("attempt", attrs...)
			})
		}
		var result *gateway.Result
		var err error
		defer func() {
			if panicValue := recover(); panicValue != nil {
				finish(errors.New("attempt pipeline panicked"))
				panic(panicValue)
			}
		}()
		result, err = next(ctx, state, attempt)
		if err != nil {
			finish(err)
			return nil, err
		}
		if result == nil || result.RawStream == nil {
			finish(nil)
			return result, nil
		}
		result.RawStream = &observedReadCloser{ReadCloser: result.RawStream, Context: ctx, Finish: finish}
		return result, nil
	}
}

// SafeErrorMessage returns a bounded, non-sensitive description suitable for
// logs and trace exporters. Error causes may contain provider URLs, credentials,
// request data, or datastore connection details, so observability must never
// unwrap or serialize them directly.
func SafeErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	var apiErr *gateway.APIError
	if errors.As(err, &apiErr) {
		if message := http.StatusText(apiErr.Status); message != "" {
			return strings.ToLower(message)
		}
		return "request failed"
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled.Error()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded.Error()
	}
	return "internal error"
}

func requestIDForState(state *gateway.RequestState) string {
	if state == nil || state.Request == nil {
		return ""
	}
	return state.Request.Meta.RequestID
}

type observedReadCloser struct {
	io.ReadCloser
	Context context.Context
	Finish  func(error)
	once    sync.Once
}

func (r *observedReadCloser) Read(p []byte) (n int, err error) {
	defer func() {
		if panicValue := recover(); panicValue != nil {
			r.finish(errors.New("attempt stream read panicked"))
			panic(panicValue)
		}
	}()
	n, err = r.ReadCloser.Read(p)
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.finish(nil)
		} else {
			r.finish(err)
		}
	}
	return n, err
}

func (r *observedReadCloser) Close() (err error) {
	defer func() {
		if panicValue := recover(); panicValue != nil {
			r.finish(errors.New("attempt stream close panicked"))
			panic(panicValue)
		}
	}()
	err = r.ReadCloser.Close()
	observedErr := err
	if observedErr == nil && r.Context != nil {
		observedErr = r.Context.Err()
	}
	r.finish(observedErr)
	return err
}

func (r *observedReadCloser) finish(err error) {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.Finish != nil {
			r.Finish(err)
		}
	})
}

func boundedModelLabel(state *gateway.RequestState) string {
	if state == nil || state.Request == nil || state.Snapshot == nil {
		return "unknown"
	}
	requested := state.Request.Model
	if state.Snapshot.HasModel(requested) {
		return requested
	}
	return "unknown"
}

func errorCode(err error) string {
	var apiErr *gateway.APIError
	if errors.As(err, &apiErr) && apiErr.Code != "" {
		return apiErr.Code
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "internal_error"
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
