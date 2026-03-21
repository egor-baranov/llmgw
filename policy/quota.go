package policy

import (
	"context"
	"time"

	"llmgw/gateway"
	"llmgw/observer"
	"llmgw/store"
)

type Quota struct {
	Store       store.QuotaStore
	Reservation time.Duration
	Obs         *observer.Observer
}

func (q Quota) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if q.Store == nil || len(state.Scopes) == 0 {
			return next(ctx, state)
		}
		candidates, err := state.ResolveCandidates()
		if err != nil {
			return nil, err
		}
		if len(candidates) == 0 {
			return nil, gateway.UnsupportedOperation("no resolved route passed quota resolution")
		}
		ttl := q.Reservation
		if ttl == 0 {
			ttl = state.Snapshot.Store.ReservationTTL.Duration
		}
		reserve := reserveEstimate(candidates)
		ticket, err := q.Store.Reserve(ctx, state.Request.Meta.RequestID, state.Scopes, reserve, ttl)
		if err != nil {
			q.observeQuotaDenied(state.Scopes, "reserve")
			return nil, err
		}
		state.Quota = &ticket
		state.Reserved = reserve
		q.observeQuotaReserved(state.Scopes, reserve)
		exec, err := next(ctx, state)
		if err != nil {
			_ = q.Store.Refund(ctx, ticket)
			q.observeQuotaRefunded(state.Scopes, reserve)
			return nil, err
		}
		prev := exec.Finalize
		exec.Finalize = func(ctx context.Context, actual gateway.Usage, callErr error) error {
			settled := settledQuotaUsage(exec, actual, reserve)
			if callErr == nil || exec.Stage() == gateway.ExecutionFirstByte || exec.Stage() == gateway.ExecutionAborted || settled.TotalTokens() > 0 {
				if err := q.Store.Commit(ctx, ticket, settled); err != nil {
					return err
				}
				q.observeQuotaCommitted(state.Scopes, settled)
			} else {
				if err := q.Store.Refund(ctx, ticket); err != nil {
					return err
				}
				q.observeQuotaRefunded(state.Scopes, reserve)
			}
			if prev != nil {
				return prev(ctx, actual, callErr)
			}
			return nil
		}
		return exec, nil
	}
}

func settledQuotaUsage(exec *gateway.Execution, actual gateway.Usage, reserved gateway.EstimatedUsage) gateway.ActualUsage {
	settled := gateway.ActualUsage{
		InputTokens:  actual.InputTokens,
		OutputTokens: actual.OutputTokens,
	}
	if settled.InputTokens == 0 && (exec.Stage() == gateway.ExecutionFirstByte || exec.Stage() == gateway.ExecutionAborted) {
		settled.InputTokens = reserved.InputTokens
	}
	if settled.InputTokens == 0 && actual.TotalTokens > 0 {
		settled.InputTokens = actual.TotalTokens - actual.OutputTokens
	}
	if settled.OutputTokens == 0 && actual.TotalTokens > settled.InputTokens {
		settled.OutputTokens = actual.TotalTokens - settled.InputTokens
	}
	if settled.InputTokens < 0 {
		settled.InputTokens = 0
	}
	if settled.OutputTokens < 0 {
		settled.OutputTokens = 0
	}
	if exec != nil && exec.Attempt != nil && exec.Attempt.Route.Route != nil {
		settled.SpendMicros = actualSpendMicros(exec.Attempt.Route.Route.Pricing, settled)
	}
	return settled
}

func estimatedSpendMicros(pricing gateway.Pricing, usage gateway.Usage) int64 {
	input := float64(usage.InputTokens) * pricing.InputPer1M / 1_000_000
	output := float64(usage.OutputTokens) * pricing.OutputPer1M / 1_000_000
	return int64((input + output) * 1_000_000)
}

func actualSpendMicros(pricing gateway.Pricing, usage gateway.ActualUsage) int64 {
	input := float64(usage.InputTokens) * pricing.InputPer1M / 1_000_000
	output := float64(usage.OutputTokens) * pricing.OutputPer1M / 1_000_000
	return int64((input + output) * 1_000_000)
}

func (q Quota) observeQuotaDenied(scopes []gateway.ScopedLimit, reason string) {
	if q.Obs == nil || q.Obs.Metrics == nil {
		return
	}
	for _, scope := range scopes {
		q.Obs.Metrics.QuotaDeniedCounter(string(scope.Ref.Kind), reason).Inc()
	}
}

func (q Quota) observeQuotaReserved(scopes []gateway.ScopedLimit, usage gateway.EstimatedUsage) {
	if q.Obs == nil || q.Obs.Metrics == nil {
		return
	}
	for _, scope := range scopes {
		q.Obs.Metrics.QuotaReservedTokensCounter(string(scope.Ref.Kind)).AddInt64(usage.TotalTokens())
	}
}

func (q Quota) observeQuotaCommitted(scopes []gateway.ScopedLimit, usage gateway.ActualUsage) {
	if q.Obs == nil || q.Obs.Metrics == nil {
		return
	}
	for _, scope := range scopes {
		q.Obs.Metrics.QuotaCommittedSpendCounter(string(scope.Ref.Kind)).AddInt64(usage.SpendMicros)
	}
}

func (q Quota) observeQuotaRefunded(scopes []gateway.ScopedLimit, usage gateway.EstimatedUsage) {
	if q.Obs == nil || q.Obs.Metrics == nil {
		return
	}
	for _, scope := range scopes {
		q.Obs.Metrics.QuotaRefundedSpendCounter(string(scope.Ref.Kind)).AddInt64(usage.EstimatedSpendMicros)
	}
}
