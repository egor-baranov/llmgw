package policy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"llmgw/gateway"
	"llmgw/observer"
	"llmgw/store"
)

type Quota struct {
	Store             store.QuotaStore
	Reservation       time.Duration
	MaxReservationAge time.Duration
	RenewalInterval   time.Duration
	Obs               *observer.Observer
}

type attemptQuotaReservation struct {
	estimate gateway.EstimatedUsage
}

type quotaSettlement struct {
	ticket          gateway.QuotaTicket
	reserved        gateway.EstimatedUsage
	ttl             time.Duration
	actual          gateway.ActualUsage
	commit          bool
	operationSuffix string
}

func (q Quota) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		if q.Store == nil || !hasQuotaAccounting(state.Scopes) {
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
		ttl = quotaReservationTTL(candidates, ttl)
		// Establish the request-level reservation (RPM/concurrency and an
		// idempotent ticket) without pre-authorizing routes that may never be
		// attempted. Each provider call is topped up immediately before dispatch.
		reserve := gateway.EstimatedUsage{}
		reservationID := state.Request.Meta.ExecutionID
		if reservationID == "" {
			// Direct callers that bypass Engine.Execute do not receive an execution ID.
			reservationID = state.Request.Meta.RequestID
		}
		ticket, err := q.Store.Reserve(ctx, reservationID, state.Scopes, reserve, ttl)
		if err != nil {
			q.observeQuotaDenied(state.Scopes, "reserve")
			return nil, quotaAdmissionError(ctx, err)
		}
		state.Quota = &ticket
		state.Reserved = reserve
		var reserveMu sync.Mutex
		reservedAttempts := make(map[string]attemptQuotaReservation)
		credit := gateway.EstimatedUsage{}
		reservedUsage := func() gateway.EstimatedUsage {
			reserveMu.Lock()
			defer reserveMu.Unlock()
			return reserve
		}
		state.SetAttemptReservation(func(attemptCtx context.Context, attempt *gateway.Attempt) error {
			if attempt == nil {
				return nil
			}
			reserveMu.Lock()
			defer reserveMu.Unlock()
			if _, exists := reservedAttempts[attempt.ID]; exists {
				return nil
			}
			delta := attemptReserveEstimate(attempt.Route)
			if delta.TotalTokens() == 0 && delta.EstimatedSpendMicros == 0 && !state.Estimate.IsZero() {
				delta = estimatedUsageForRoute(attempt.Route.Route, state.Estimate)
			}
			applied, remainingCredit := consumeReservationCredit(delta, credit)
			q.observeSoftSpendThreshold(attemptCtx, state, applied)
			if applied.TotalTokens() > 0 || applied.EstimatedSpendMicros > 0 {
				if err := q.Store.TopUp(attemptCtx, ticket, state.Scopes, applied, ttl); err != nil {
					q.observeQuotaDenied(state.Scopes, "attempt_reserve")
					return quotaAdmissionError(attemptCtx, err)
				}
			}
			credit = remainingCredit
			reservedAttempts[attempt.ID] = attemptQuotaReservation{estimate: delta}
			reserve.InputTokens = positiveTokenTotal(reserve.InputTokens, applied.InputTokens)
			reserve.ReservedOutputTokens = positiveTokenTotal(reserve.ReservedOutputTokens, applied.ReservedOutputTokens)
			reserve.EstimatedSpendMicros = positiveTokenTotal(reserve.EstimatedSpendMicros, applied.EstimatedSpendMicros)
			state.Reserved = reserve
			q.observeQuotaReserved(state.Scopes, applied)
			return nil
		})
		state.SetAttemptReservationRelease(func(attemptID string) {
			reserveMu.Lock()
			defer reserveMu.Unlock()
			record, exists := reservedAttempts[attemptID]
			if !exists {
				return
			}
			delete(reservedAttempts, attemptID)
			delta := record.estimate
			credit.InputTokens = positiveTokenTotal(credit.InputTokens, delta.InputTokens)
			credit.ReservedOutputTokens = positiveTokenTotal(credit.ReservedOutputTokens, delta.ReservedOutputTokens)
			credit.EstimatedSpendMicros = positiveTokenTotal(credit.EstimatedSpendMicros, delta.EstimatedSpendMicros)
		})
		state.SetAttemptReservationReconcile(func(reconcileCtx context.Context, attemptID string, actual gateway.ActualUsage) error {
			reserveMu.Lock()
			defer reserveMu.Unlock()
			record, exists := reservedAttempts[attemptID]
			if !exists {
				return nil
			}
			estimatedTokens := positiveTokenTotal(record.estimate.InputTokens, record.estimate.ReservedOutputTokens)
			actualTokens := positiveTokenTotal(actual.InputTokens, actual.OutputTokens)
			var overage gateway.EstimatedUsage
			if actualTokens > estimatedTokens {
				overage.InputTokens = actualTokens - estimatedTokens
			}
			if actual.SpendMicros > record.estimate.EstimatedSpendMicros {
				overage.EstimatedSpendMicros = actual.SpendMicros - record.estimate.EstimatedSpendMicros
			}
			if overage.TotalTokens() > 0 || overage.EstimatedSpendMicros > 0 {
				if err := q.Store.TopUp(reconcileCtx, ticket, state.Scopes, overage, ttl); err != nil {
					q.observeQuotaDenied(state.Scopes, "attempt_reconcile")
					return gateway.DisallowFallback(quotaAdmissionError(reconcileCtx, err))
				}
				reserve.InputTokens = positiveTokenTotal(reserve.InputTokens, overage.InputTokens)
				reserve.ReservedOutputTokens = positiveTokenTotal(reserve.ReservedOutputTokens, overage.ReservedOutputTokens)
				reserve.EstimatedSpendMicros = positiveTokenTotal(reserve.EstimatedSpendMicros, overage.EstimatedSpendMicros)
				state.Reserved = reserve
				q.observeQuotaReserved(state.Scopes, overage)
			}
			delete(reservedAttempts, attemptID)
			if estimatedTokens > actualTokens {
				credit.InputTokens = positiveTokenTotal(credit.InputTokens, estimatedTokens-actualTokens)
			}
			if record.estimate.EstimatedSpendMicros > actual.SpendMicros {
				credit.EstimatedSpendMicros = positiveTokenTotal(credit.EstimatedSpendMicros, record.estimate.EstimatedSpendMicros-actual.SpendMicros)
			}
			return nil
		})
		stopRenewal := q.startReservationRenewal(ctx, ticket, state.Scopes, ttl, state)
		var exec *gateway.Execution
		var nextErr error
		var panicValue any
		func() {
			defer func() { panicValue = recover() }()
			exec, nextErr = next(ctx, state)
		}()
		if panicValue != nil {
			renewalErr := stopRenewal()
			if renewalErr != nil {
				q.observeSettlementError(state, "renew", renewalErr)
			}
			settled := state.TotalAttemptUsage()
			if settlementErr := q.settleReserved(ctx, state, quotaSettlement{
				ticket:          ticket,
				reserved:        reservedUsage(),
				ttl:             ttl,
				actual:          settled,
				commit:          hasActualUsage(settled),
				operationSuffix: "after_panic",
			}); settlementErr != nil {
				q.observeSettlementError(state, "settle_after_panic", settlementErr)
			}
			panic(panicValue)
		}
		err = nextErr
		if err != nil {
			renewalErr := stopRenewal()
			settled := state.TotalAttemptUsage()
			settlementErr := q.settleReserved(ctx, state, quotaSettlement{
				ticket:   ticket,
				reserved: reservedUsage(),
				ttl:      ttl,
				actual:   settled,
				commit:   hasActualUsage(settled),
			})
			return nil, errors.Join(err, renewalErr, settlementErr)
		}
		if exec == nil {
			renewalErr := stopRenewal()
			settleCtx, cancel := quotaSettlementContext(ctx)
			refundErr := q.Store.Refund(settleCtx, ticket)
			cancel()
			return nil, errors.Join(gateway.NewError(500, "server_error", "empty_execution", "gateway returned no execution"), renewalErr, refundErr)
		}
		prev := exec.Finalize
		exec.Finalize = func(ctx context.Context, actual gateway.Usage, callErr error) error {
			renewalErr := stopRenewal()
			failed := callErr != nil || exec.Stage() == gateway.ExecutionAborted || exec.Stage() == gateway.ExecutionFailed
			state.CompleteResultUsage(exec.Result, actual, failed)
			settled := state.TotalAttemptUsage()
			if !hasActualUsage(settled) {
				settled = settledQuotaUsage(exec, actual, singleAttemptReserve(exec))
			}
			commit := callErr == nil || exec.Stage() == gateway.ExecutionFirstByte || exec.Stage() == gateway.ExecutionAborted || hasActualUsage(settled)
			settlementErr := errors.Join(renewalErr, q.settleReserved(ctx, state, quotaSettlement{
				ticket:   ticket,
				reserved: reservedUsage(),
				ttl:      ttl,
				actual:   settled,
				commit:   commit,
			}))
			if prev != nil {
				settlementErr = errors.Join(settlementErr, prev(ctx, actual, callErr))
			}
			return settlementErr
		}
		return exec, nil
	}
}

func hasQuotaAccounting(scopes []gateway.ScopedLimit) bool {
	for _, scope := range scopes {
		limit := scope.Limits
		if limit.RPM > 0 || limit.TPM > 0 || limit.MaxParallel > 0 ||
			limit.MaxSpendMicros > 0 || limit.SoftSpendMicros > 0 ||
			limit.DailyTokens > 0 || limit.MonthlyTokens > 0 {
			return true
		}
	}
	return false
}

func (q Quota) settleReserved(ctx context.Context, state *gateway.RequestState, settlement quotaSettlement) error {
	settleCtx, cancel := quotaSettlementContext(ctx)
	defer cancel()
	operation := func(base string) string {
		if settlement.operationSuffix == "" {
			return base
		}
		return base + "_" + settlement.operationSuffix
	}
	if !settlement.commit {
		if err := q.Store.Refund(settleCtx, settlement.ticket); err != nil {
			q.observeSettlementError(state, operation("refund"), err)
			return fmt.Errorf("refund quota reservation: %w", err)
		}
		q.observeQuotaRefunded(state.Scopes, settlement.reserved)
		return nil
	}
	var settlementErr error
	delta := quotaTopUpDelta(settlement.reserved, settlement.actual)
	if delta.TotalTokens() > 0 || delta.EstimatedSpendMicros > 0 {
		if err := q.Store.TopUp(settleCtx, settlement.ticket, state.Scopes, delta, settlement.ttl); err != nil {
			q.observeSettlementError(state, operation("top_up"), err)
			settlementErr = errors.Join(settlementErr, fmt.Errorf("top up quota reservation: %w", err))
		}
	}
	if err := q.Store.Commit(settleCtx, settlement.ticket, settlement.actual); err != nil {
		q.observeSettlementError(state, operation("commit"), err)
		settlementErr = errors.Join(settlementErr, fmt.Errorf("commit quota reservation: %w", err))
	} else {
		q.observeQuotaCommitted(state.Scopes, settlement.actual)
	}
	return settlementErr
}

func hasActualUsage(usage gateway.ActualUsage) bool {
	return usage.InputTokens > 0 || usage.OutputTokens > 0 || usage.SpendMicros > 0
}

func singleAttemptReserve(exec *gateway.Execution) gateway.EstimatedUsage {
	if exec == nil || exec.Attempt == nil {
		return gateway.EstimatedUsage{}
	}
	return attemptReserveEstimate(exec.Attempt.Route)
}

func attemptReserveEstimate(route gateway.ResolvedRoute) gateway.EstimatedUsage {
	return estimatedUsageForRoute(route.Route, route.Estimate)
}

func estimatedUsageForRoute(route *gateway.Route, estimate gateway.Usage) gateway.EstimatedUsage {
	reserved := gateway.EstimatedUsage{
		InputTokens:          estimate.InputTokens,
		ReservedOutputTokens: estimate.OutputTokens,
	}
	if route != nil {
		reserved.EstimatedSpendMicros = route.Pricing.SpendMicrosForUsage(estimate)
	}
	return reserved
}

func (q Quota) observeSoftSpendThreshold(ctx context.Context, state *gateway.RequestState, reserve gateway.EstimatedUsage) {
	usageStore, ok := q.Store.(store.QuotaUsageStore)
	if !ok || state == nil {
		return
	}
	for _, scope := range state.Scopes {
		if scope.Limits.SoftSpendMicros <= 0 {
			continue
		}
		usage, err := usageStore.GetUsage(ctx, scope)
		if err != nil {
			q.observeSettlementError(state, "soft_spend_lookup", err)
			continue
		}
		projected := positiveTokenTotal(usage.SpendUsedMicros, usage.SpendHeldMicros)
		projected = positiveTokenTotal(projected, reserve.EstimatedSpendMicros)
		if projected <= scope.Limits.SoftSpendMicros {
			continue
		}
		if q.Obs != nil && q.Obs.Metrics != nil {
			q.Obs.Metrics.QuotaSoftSpendExceededCounter(string(scope.Ref.Kind)).Inc()
		}
		if q.Obs != nil && q.Obs.Logger != nil {
			q.Obs.Logger.Warn("quota soft spend threshold exceeded",
				"request_id", state.Request.Meta.RequestID,
				"scope", string(scope.Ref.Kind),
				"projected_spend_micros", projected,
				"soft_spend_micros", scope.Limits.SoftSpendMicros,
			)
		}
	}
}

func (q Quota) startReservationRenewal(parent context.Context, ticket gateway.QuotaTicket, scopes []gateway.ScopedLimit, ttl time.Duration, state *gateway.RequestState) func() error {
	if q.Store == nil || ticket.RequestID == "" || ttl <= 0 {
		return func() error { return nil }
	}
	interval := q.RenewalInterval
	if interval <= 0 {
		interval = ttl / 3
		if interval > 30*time.Second {
			interval = 30 * time.Second
		}
		if interval < 100*time.Millisecond {
			interval = 100 * time.Millisecond
		}
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	maxAge := q.MaxReservationAge
	if maxAge <= 0 {
		if ttl > time.Duration(math.MaxInt64)/2 {
			maxAge = time.Duration(math.MaxInt64)
		} else {
			maxAge = ttl * 2
		}
	}
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		maxTimer := time.NewTimer(maxAge)
		defer maxTimer.Stop()
		var renewalErr error
		for {
			select {
			case <-ctx.Done():
				done <- renewalErr
				return
			case <-maxTimer.C:
				if renewalErr == nil {
					renewalErr = errors.New("quota reservation renewal reached its maximum lifetime")
				}
				done <- renewalErr
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(ctx, 5*time.Second)
				err := q.Store.TopUp(renewCtx, ticket, scopes, gateway.EstimatedUsage{}, ttl)
				renewCancel()
				if err != nil {
					q.observeSettlementError(state, "renew", err)
					if renewalErr == nil {
						renewalErr = fmt.Errorf("renew quota reservation: %w", err)
					}
				}
			}
		}
	}()
	var once sync.Once
	var renewalErr error
	return func() error {
		once.Do(func() {
			cancel()
			renewalErr = <-done
		})
		return renewalErr
	}
}

func quotaReservationTTL(candidates []gateway.ResolvedRoute, configured time.Duration) time.Duration {
	const settlementBuffer = 10 * time.Second
	required := settlementBuffer
	for _, candidate := range candidates {
		if candidate.Route == nil {
			continue
		}
		timeout := candidate.Route.Timeout.Duration
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		tries := routeAttemptCount(candidate.Route)
		if timeout > (time.Duration(math.MaxInt64)-required)/time.Duration(tries) {
			required = time.Duration(math.MaxInt64)
			break
		}
		required += timeout * time.Duration(tries)
	}
	if configured < required {
		return required
	}
	return configured
}

func quotaSettlementContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func quotaTopUpDelta(reserved gateway.EstimatedUsage, actual gateway.ActualUsage) gateway.EstimatedUsage {
	var delta gateway.EstimatedUsage
	reservedTokens := positiveTokenTotal(reserved.InputTokens, reserved.ReservedOutputTokens)
	actualTokens := positiveTokenTotal(actual.InputTokens, actual.OutputTokens)
	if actualTokens > reservedTokens {
		delta.InputTokens = actualTokens - reservedTokens
	}
	if actual.SpendMicros > reserved.EstimatedSpendMicros {
		delta.EstimatedSpendMicros = actual.SpendMicros - reserved.EstimatedSpendMicros
	}
	return delta
}

func positiveTokenTotal(first, second int64) int64 {
	if first < 0 {
		first = 0
	}
	if second < 0 {
		second = 0
	}
	if first > math.MaxInt64-second {
		return math.MaxInt64
	}
	return first + second
}

func consumeReservationCredit(want, available gateway.EstimatedUsage) (gateway.EstimatedUsage, gateway.EstimatedUsage) {
	wantTokens := positiveTokenTotal(want.InputTokens, want.ReservedOutputTokens)
	availableTokens := positiveTokenTotal(available.InputTokens, available.ReservedOutputTokens)
	usedTokens := min(wantTokens, availableTokens)
	appliedTokens := wantTokens - usedTokens
	remainingTokens := availableTokens - usedTokens
	appliedInput := min(want.InputTokens, appliedTokens)
	applied := gateway.EstimatedUsage{
		InputTokens:          appliedInput,
		ReservedOutputTokens: appliedTokens - appliedInput,
	}
	usedSpend := min(max(want.EstimatedSpendMicros, int64(0)), max(available.EstimatedSpendMicros, int64(0)))
	applied.EstimatedSpendMicros = max(want.EstimatedSpendMicros-usedSpend, 0)
	remaining := gateway.EstimatedUsage{
		InputTokens:          remainingTokens,
		EstimatedSpendMicros: max(available.EstimatedSpendMicros-usedSpend, 0),
	}
	return applied, remaining
}

func quotaAdmissionError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return err
	}
	var apiErr *gateway.APIError
	if errors.As(err, &apiErr) {
		return err
	}
	return gateway.NewError(503, "server_error", "quota_store_unavailable", "quota admission is temporarily unavailable").
		WithCause(err).
		WithDisposition(false, false, false)
}

func settledQuotaUsage(exec *gateway.Execution, actual gateway.Usage, reserved gateway.EstimatedUsage) gateway.ActualUsage {
	settled := gateway.ActualUsage{
		InputTokens:  actual.InputTokens,
		OutputTokens: actual.OutputTokens,
	}
	if actual.IsZero() {
		settled.InputTokens = reserved.InputTokens
		settled.OutputTokens = reserved.ReservedOutputTokens
	}
	if settled.InputTokens == 0 && (exec.Stage() == gateway.ExecutionFirstByte || exec.Stage() == gateway.ExecutionAborted) {
		settled.InputTokens = reserved.InputTokens
	}
	if settled.OutputTokens == 0 && (exec.Stage() == gateway.ExecutionAborted || exec.Stage() == gateway.ExecutionFailed) {
		settled.OutputTokens = reserved.ReservedOutputTokens
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
		billable := actual
		billable.InputTokens = settled.InputTokens
		billable.OutputTokens = settled.OutputTokens
		billable.TotalTokens = positiveTokenTotal(settled.InputTokens, settled.OutputTokens)
		settled.SpendMicros = exec.Attempt.Route.Route.Pricing.SpendMicrosForUsage(billable)
	}
	return settled
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

func (q Quota) observeSettlementError(state *gateway.RequestState, operation string, err error) {
	if q.Obs == nil || q.Obs.Logger == nil || err == nil {
		return
	}
	requestID := ""
	executionID := ""
	if state != nil && state.Request != nil {
		requestID = state.Request.Meta.RequestID
		executionID = state.Request.Meta.ExecutionID
	}
	q.Obs.Logger.Error("quota settlement failed",
		"request_id", requestID,
		"execution_id", executionID,
		"operation", operation,
		"error", observer.SafeErrorMessage(err),
	)
}
