package gateway

import (
	"errors"
	"math"
	"sync"
)

// AttemptUsageAccumulator keeps one charge per real provider invocation. Its
// zero value is ready to use. Retry IDs are unique, so retries and fallbacks add
// to the request total while repeated stream close/settlement callbacks remain
// idempotent.
type AttemptUsageAccumulator struct {
	mu      sync.Mutex
	entries map[string]attemptUsageEntry
}

type attemptUsageEntry struct {
	estimate Usage
	pricing  Pricing
	charge   ActualUsage
	final    bool
}

func (a *AttemptUsageAccumulator) begin(id string, route ResolvedRoute) {
	if a == nil || id == "" {
		return
	}
	estimate := normalizeAttemptUsage(route.Estimate)
	pricing := Pricing{}
	if route.Route != nil {
		pricing = route.Route.Pricing
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.entries == nil {
		a.entries = make(map[string]attemptUsageEntry)
	}
	if _, exists := a.entries[id]; exists {
		return
	}
	a.entries[id] = attemptUsageEntry{
		estimate: estimate,
		pricing:  pricing,
		charge:   chargeForUsage(pricing, estimate),
	}
}

func (a *AttemptUsageAccumulator) cancel(id string) {
	if a == nil || id == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if entry, exists := a.entries[id]; exists && !entry.final {
		delete(a.entries, id)
	}
}

func (a *AttemptUsageAccumulator) complete(id string, usage Usage, partial bool) {
	if a == nil || id == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, exists := a.entries[id]
	if !exists || entry.final {
		return
	}
	normalized := ReconcileReportedUsage(usage, entry.estimate)
	if partial {
		// Aborted-stream usage snapshots are commonly partial. Fill
		// each unavailable dimension conservatively from this route's estimate.
		if normalized.InputTokens == 0 {
			normalized.InputTokens = entry.estimate.InputTokens
		}
		if normalized.OutputTokens == 0 {
			normalized.OutputTokens = entry.estimate.OutputTokens
		}
		normalized.TotalTokens = saturatingUsageAdd(normalized.InputTokens, normalized.OutputTokens)
		normalized = retainEstimatedAuxiliaryUsage(normalized, entry.estimate)
	}
	entry.charge = chargeForUsage(entry.pricing, normalized)
	entry.final = true
	a.entries[id] = entry
}

func (a *AttemptUsageAccumulator) total() ActualUsage {
	if a == nil {
		return ActualUsage{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var total ActualUsage
	for _, entry := range a.entries {
		total.InputTokens = saturatingUsageAdd(total.InputTokens, entry.charge.InputTokens)
		total.OutputTokens = saturatingUsageAdd(total.OutputTokens, entry.charge.OutputTokens)
		total.SpendMicros = saturatingUsageAdd(total.SpendMicros, entry.charge.SpendMicros)
	}
	return total
}

func (a *AttemptUsageAccumulator) charge(id string) (ActualUsage, bool) {
	if a == nil || id == "" {
		return ActualUsage{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, exists := a.entries[id]
	if !exists || !entry.final {
		return ActualUsage{}, false
	}
	return entry.charge, true
}

func normalizeAttemptUsage(usage Usage) Usage {
	if usage.InputTokens < 0 {
		usage.InputTokens = 0
	}
	if usage.OutputTokens < 0 {
		usage.OutputTokens = 0
	}
	if usage.TotalTokens < 0 {
		usage.TotalTokens = 0
	}
	if usage.CacheReadTokens < 0 {
		usage.CacheReadTokens = 0
	}
	if usage.CacheWriteTokens < 0 {
		usage.CacheWriteTokens = 0
	}
	usage.InputDetails = normalizeUsageDetails(usage.InputDetails)
	usage.OutputDetails = normalizeUsageDetails(usage.OutputDetails)
	if usage.CacheReadTokens == 0 && usage.InputDetails != nil {
		usage.CacheReadTokens = usage.InputDetails.CachedTokens
	}
	if usage.ProviderDetails != nil {
		providerDetails := make(map[string]int64, len(usage.ProviderDetails))
		for unit, count := range usage.ProviderDetails {
			if count > 0 {
				providerDetails[unit] = count
			}
		}
		usage.ProviderDetails = providerDetails
		if len(providerDetails) == 0 {
			usage.ProviderDetails = nil
		}
	}
	if usage.InputTokens == 0 && usage.TotalTokens > usage.OutputTokens {
		usage.InputTokens = usage.TotalTokens - usage.OutputTokens
	}
	if usage.OutputTokens == 0 && usage.TotalTokens > usage.InputTokens {
		usage.OutputTokens = usage.TotalTokens - usage.InputTokens
	}
	sum := saturatingUsageAdd(usage.InputTokens, usage.OutputTokens)
	if usage.TotalTokens > sum {
		// Some providers report billed tokens (for example reasoning or tool
		// processing) only in total_tokens. Treat the unexplained remainder as
		// output so token quotas and spend cannot silently undercount it.
		usage.OutputTokens = saturatingUsageAdd(usage.OutputTokens, usage.TotalTokens-sum)
		sum = saturatingUsageAdd(usage.InputTokens, usage.OutputTokens)
	}
	if usage.TotalTokens < sum {
		usage.TotalTokens = sum
	}
	return usage
}

// ReconcileReportedUsage normalizes provider usage and conservatively fills
// malformed or wholly missing token dimensions from a pre-call estimate.
// Positive auxiliary counters reported by the provider remain authoritative.
func ReconcileReportedUsage(reported, fallback Usage) Usage {
	invalidInput := reported.InputTokens < 0
	invalidOutput := reported.OutputTokens < 0
	invalidTotal := reported.TotalTokens < 0
	hasPositiveTokens := reported.InputTokens > 0 || reported.OutputTokens > 0 || reported.TotalTokens > 0
	normalizedReported := normalizeAttemptUsage(reported)
	normalizedFallback := normalizeAttemptUsage(fallback)
	if hasPositiveTokens && !invalidInput && !invalidOutput && !invalidTotal {
		return restoreMalformedAuxiliaryUsage(normalizedReported, reported, normalizedFallback)
	}

	reconciled := normalizedFallback
	if hasPositiveTokens {
		if !invalidInput {
			reconciled.InputTokens = normalizedReported.InputTokens
		}
		if !invalidOutput {
			reconciled.OutputTokens = normalizedReported.OutputTokens
		}
		if !invalidTotal {
			reconciled.TotalTokens = normalizedReported.TotalTokens
		}
	}
	reconciled = overlayPositiveUsageDetails(reconciled, normalizedReported)
	return normalizeAttemptUsage(reconciled)
}

func restoreMalformedAuxiliaryUsage(usage, reported, fallback Usage) Usage {
	if reported.CacheReadTokens < 0 {
		usage.CacheReadTokens = fallback.CacheReadTokens
	}
	if reported.CacheWriteTokens < 0 {
		usage.CacheWriteTokens = fallback.CacheWriteTokens
	}
	usage.InputDetails = restoreMalformedUsageDetails(usage.InputDetails, reported.InputDetails, fallback.InputDetails)
	usage.OutputDetails = restoreMalformedUsageDetails(usage.OutputDetails, reported.OutputDetails, fallback.OutputDetails)
	for unit, count := range reported.ProviderDetails {
		if count >= 0 {
			continue
		}
		reserved := fallback.ProviderDetails[unit]
		if reserved <= 0 {
			delete(usage.ProviderDetails, unit)
			continue
		}
		if usage.ProviderDetails == nil {
			usage.ProviderDetails = make(map[string]int64)
		}
		usage.ProviderDetails[unit] = reserved
	}
	return normalizeAttemptUsage(usage)
}

func restoreMalformedUsageDetails(current, reported, fallback *UsageDetails) *UsageDetails {
	if reported == nil {
		return current
	}
	current = normalizeUsageDetails(current)
	fallback = normalizeUsageDetails(fallback)
	ensureCurrent := func() {
		if current == nil {
			current = &UsageDetails{}
		}
	}
	if reported.TextTokens < 0 {
		ensureCurrent()
		if fallback != nil {
			current.TextTokens = fallback.TextTokens
		}
	}
	if reported.AudioTokens < 0 {
		ensureCurrent()
		if fallback != nil {
			current.AudioTokens = fallback.AudioTokens
		}
	}
	if reported.ReasoningTokens < 0 {
		ensureCurrent()
		if fallback != nil {
			current.ReasoningTokens = fallback.ReasoningTokens
		}
	}
	if reported.ToolTokens < 0 {
		ensureCurrent()
		if fallback != nil {
			current.ToolTokens = fallback.ToolTokens
		}
	}
	if reported.CachedTokens < 0 {
		ensureCurrent()
		if fallback != nil {
			current.CachedTokens = fallback.CachedTokens
		}
	}
	for modality, count := range reported.ByModality {
		if count >= 0 {
			continue
		}
		ensureCurrent()
		if current.ByModality == nil {
			current.ByModality = make(map[string]int64)
		}
		reserved := int64(0)
		if fallback != nil {
			reserved = fallback.ByModality[modality]
		}
		if reserved > 0 {
			current.ByModality[modality] = reserved
		} else {
			delete(current.ByModality, modality)
		}
	}
	return normalizeUsageDetails(current)
}

func normalizeUsageDetails(details *UsageDetails) *UsageDetails {
	if details == nil {
		return nil
	}
	normalized := &UsageDetails{
		TextTokens:      max(details.TextTokens, 0),
		AudioTokens:     max(details.AudioTokens, 0),
		ReasoningTokens: max(details.ReasoningTokens, 0),
		ToolTokens:      max(details.ToolTokens, 0),
		CachedTokens:    max(details.CachedTokens, 0),
	}
	if len(details.ByModality) > 0 {
		normalized.ByModality = make(map[string]int64, len(details.ByModality))
		for modality, count := range details.ByModality {
			if count > 0 {
				normalized.ByModality[modality] = count
			}
		}
		if len(normalized.ByModality) == 0 {
			normalized.ByModality = nil
		}
	}
	if normalized.TextTokens == 0 && normalized.AudioTokens == 0 && normalized.ReasoningTokens == 0 &&
		normalized.ToolTokens == 0 && normalized.CachedTokens == 0 && len(normalized.ByModality) == 0 {
		return nil
	}
	return normalized
}

func overlayPositiveUsageDetails(base, reported Usage) Usage {
	if reported.CacheReadTokens > 0 {
		base.CacheReadTokens = reported.CacheReadTokens
	}
	if reported.CacheWriteTokens > 0 {
		base.CacheWriteTokens = reported.CacheWriteTokens
	}
	base.InputDetails = overlayUsageDetails(base.InputDetails, reported.InputDetails)
	base.OutputDetails = overlayUsageDetails(base.OutputDetails, reported.OutputDetails)
	if len(reported.ProviderDetails) > 0 {
		if base.ProviderDetails == nil {
			base.ProviderDetails = make(map[string]int64, len(reported.ProviderDetails))
		}
		for unit, count := range reported.ProviderDetails {
			base.ProviderDetails[unit] = count
		}
	}
	return base
}

func overlayUsageDetails(base, reported *UsageDetails) *UsageDetails {
	if reported == nil {
		return base
	}
	if base == nil {
		base = &UsageDetails{}
	}
	if reported.TextTokens > 0 {
		base.TextTokens = reported.TextTokens
	}
	if reported.AudioTokens > 0 {
		base.AudioTokens = reported.AudioTokens
	}
	if reported.ReasoningTokens > 0 {
		base.ReasoningTokens = reported.ReasoningTokens
	}
	if reported.ToolTokens > 0 {
		base.ToolTokens = reported.ToolTokens
	}
	if reported.CachedTokens > 0 {
		base.CachedTokens = reported.CachedTokens
	}
	if len(reported.ByModality) > 0 {
		if base.ByModality == nil {
			base.ByModality = make(map[string]int64, len(reported.ByModality))
		}
		for modality, count := range reported.ByModality {
			base.ByModality[modality] = count
		}
	}
	return base
}

func retainEstimatedAuxiliaryUsage(usage, estimate Usage) Usage {
	usage.CacheReadTokens = max(usage.CacheReadTokens, estimate.CacheReadTokens)
	usage.CacheWriteTokens = max(usage.CacheWriteTokens, estimate.CacheWriteTokens)
	usage.InputDetails = maxUsageDetails(usage.InputDetails, estimate.InputDetails)
	usage.OutputDetails = maxUsageDetails(usage.OutputDetails, estimate.OutputDetails)
	for unit, reserved := range estimate.ProviderDetails {
		if usage.ProviderDetails == nil {
			usage.ProviderDetails = make(map[string]int64)
		}
		usage.ProviderDetails[unit] = max(usage.ProviderDetails[unit], reserved)
	}
	return normalizeAttemptUsage(usage)
}

func maxUsageDetails(actual, estimate *UsageDetails) *UsageDetails {
	actual = normalizeUsageDetails(actual)
	estimate = normalizeUsageDetails(estimate)
	if estimate == nil {
		return actual
	}
	if actual == nil {
		return estimate
	}
	actual.TextTokens = max(actual.TextTokens, estimate.TextTokens)
	actual.AudioTokens = max(actual.AudioTokens, estimate.AudioTokens)
	actual.ReasoningTokens = max(actual.ReasoningTokens, estimate.ReasoningTokens)
	actual.ToolTokens = max(actual.ToolTokens, estimate.ToolTokens)
	actual.CachedTokens = max(actual.CachedTokens, estimate.CachedTokens)
	for modality, count := range estimate.ByModality {
		if actual.ByModality == nil {
			actual.ByModality = make(map[string]int64)
		}
		actual.ByModality[modality] = max(actual.ByModality[modality], count)
	}
	return actual
}

func chargeForUsage(pricing Pricing, usage Usage) ActualUsage {
	usage = normalizeAttemptUsage(usage)
	return ActualUsage{
		InputTokens:  usage.InputTokens,
		OutputTokens: usage.OutputTokens,
		SpendMicros:  pricing.SpendMicrosForUsage(usage),
	}
}

// SpendMicrosForUsage prices token, cache, and provider-metered usage for one
// attempt. Cache-specific rates fall back to the ordinary input rate so an
// omitted optimization rate cannot make cached tokens free.
func (p Pricing) SpendMicrosForUsage(usage Usage) int64 {
	usage = normalizeAttemptUsage(usage)
	cacheRead := min(max(usage.CacheReadTokens, int64(0)), usage.InputTokens)
	remaining := usage.InputTokens - cacheRead
	cacheWrite := min(max(usage.CacheWriteTokens, int64(0)), remaining)
	uncachedInput := remaining - cacheWrite
	cacheReadRate := p.CacheReadPer1M
	if cacheReadRate == 0 {
		cacheReadRate = p.InputPer1M
	}
	cacheWriteRate := p.CacheWritePer1M
	if cacheWriteRate == 0 {
		cacheWriteRate = p.InputPer1M
	}
	value := float64(uncachedInput)*p.InputPer1M +
		float64(cacheRead)*cacheReadRate +
		float64(cacheWrite)*cacheWriteRate +
		float64(usage.OutputTokens)*p.OutputPer1M
	for unit, count := range usage.ProviderDetails {
		if count <= 0 {
			continue
		}
		if unitPrice, ok := p.ProviderUnits[unit]; ok {
			value += float64(count) * unitPrice.MicrosPerUnit
		}
	}
	if value <= 0 {
		return 0
	}
	if value >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(math.Ceil(value))
}

// SpendMicros prices one provider invocation. Per-attempt rounding prevents a
// sequence of sub-micro retries from bypassing a spend limit.
func (p Pricing) SpendMicros(inputTokens, outputTokens int64) int64 {
	if inputTokens < 0 {
		inputTokens = 0
	}
	if outputTokens < 0 {
		outputTokens = 0
	}
	value := float64(inputTokens)*p.InputPer1M + float64(outputTokens)*p.OutputPer1M
	if value <= 0 {
		return 0
	}
	if value >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(math.Ceil(value))
}

func saturatingUsageAdd(first, second int64) int64 {
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

type attemptErrorMetadata struct {
	err        error
	usage      Usage
	unbillable bool
}

func (e *attemptErrorMetadata) Error() string { return e.err.Error() }
func (e *attemptErrorMetadata) Unwrap() error { return e.err }

// WithAttemptUsage preserves usage returned alongside an upstream error.
func WithAttemptUsage(err error, usage Usage) error {
	if err == nil {
		return nil
	}
	return &attemptErrorMetadata{err: err, usage: usage}
}

// WithoutAttemptCharge marks a provider error that happened before any
// upstream request was dispatched (for example, a local bridge validation).
func WithoutAttemptCharge(err error) error {
	if err == nil {
		return nil
	}
	return &attemptErrorMetadata{err: err, unbillable: true}
}

func attemptErrorUsage(err error) (Usage, bool, bool) {
	var metadata *attemptErrorMetadata
	if !errors.As(err, &metadata) {
		return Usage{}, false, false
	}
	return metadata.usage, !metadata.usage.IsZero(), metadata.unbillable
}

func AttemptUnbillable(err error) bool {
	_, _, unbillable := attemptErrorUsage(err)
	return unbillable
}
