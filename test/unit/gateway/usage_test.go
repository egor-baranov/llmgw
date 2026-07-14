package gateway_test

import (
	"math"
	"testing"

	"llmgw/gateway"
)

func TestQuotaUsageTotalsSaturateInsteadOfOverflowing(t *testing.T) {
	estimated := gateway.EstimatedUsage{InputTokens: math.MaxInt64, ReservedOutputTokens: 1}
	if got := estimated.TotalTokens(); got != math.MaxInt64 {
		t.Fatalf("EstimatedUsage.TotalTokens() = %d, want MaxInt64", got)
	}
	actual := gateway.ActualUsage{InputTokens: math.MaxInt64, OutputTokens: 1}
	if got := actual.TotalTokens(); got != math.MaxInt64 {
		t.Fatalf("ActualUsage.TotalTokens() = %d, want MaxInt64", got)
	}
}

func TestReconcileReportedUsagePreservesAuxiliaryCountersWithFallback(t *testing.T) {
	fallback := gateway.Usage{
		InputTokens: 10, OutputTokens: 20, TotalTokens: 30, CacheWriteTokens: 10,
		ProviderDetails: map[string]int64{"web_search_requests": 4},
	}
	reported := gateway.Usage{
		CacheReadTokens: 5,
		InputDetails:    &gateway.UsageDetails{CachedTokens: 5, AudioTokens: -1},
		ProviderDetails: map[string]int64{"web_search_requests": 2, "invalid": -1},
	}
	got := gateway.ReconcileReportedUsage(reported, fallback)
	if got.InputTokens != 10 || got.OutputTokens != 20 || got.TotalTokens != 30 {
		t.Fatalf("reconciled tokens = %#v, want fallback token dimensions", got)
	}
	if got.CacheReadTokens != 5 || got.CacheWriteTokens != 10 || got.InputDetails == nil || got.InputDetails.CachedTokens != 5 {
		t.Fatalf("reconciled cache usage = %#v, want reported read plus conservative write", got)
	}
	if got.InputDetails.AudioTokens != 0 || got.ProviderDetails["web_search_requests"] != 2 {
		t.Fatalf("reconciled auxiliary usage = %#v, want sanitized reported counters", got)
	}
	if _, exists := got.ProviderDetails["invalid"]; exists {
		t.Fatalf("reconciled provider usage retained a non-positive counter: %#v", got.ProviderDetails)
	}
}

func TestReconcileReportedUsageDoesNotAliasInputs(t *testing.T) {
	reported := gateway.Usage{
		InputTokens: 1, OutputTokens: 2, TotalTokens: 3,
		InputDetails:    &gateway.UsageDetails{CachedTokens: 1, ByModality: map[string]int64{"text": 1}},
		ProviderDetails: map[string]int64{"search": 1},
	}
	got := gateway.ReconcileReportedUsage(reported, gateway.Usage{})
	got.InputDetails.CachedTokens = 99
	got.InputDetails.ByModality["text"] = 99
	got.ProviderDetails["search"] = 99
	if reported.InputDetails.CachedTokens != 1 || reported.InputDetails.ByModality["text"] != 1 || reported.ProviderDetails["search"] != 1 {
		t.Fatalf("reported usage was mutated through reconciled result: %#v", reported)
	}

	fallback := gateway.Usage{
		InputTokens: 4, OutputTokens: 5, TotalTokens: 9,
		OutputDetails:   &gateway.UsageDetails{ReasoningTokens: 2, ByModality: map[string]int64{"text": 2}},
		ProviderDetails: map[string]int64{"search": 2},
	}
	got = gateway.ReconcileReportedUsage(gateway.Usage{ProviderDetails: map[string]int64{}}, fallback)
	if got.ProviderDetails == nil {
		t.Fatal("fallback provider details were unexpectedly removed")
	}
	got.OutputDetails.ReasoningTokens = 99
	got.OutputDetails.ByModality["text"] = 99
	got.ProviderDetails["search"] = 99
	if fallback.OutputDetails.ReasoningTokens != 2 || fallback.OutputDetails.ByModality["text"] != 2 || fallback.ProviderDetails["search"] != 2 {
		t.Fatalf("fallback usage was mutated through reconciled result: %#v", fallback)
	}

	empty := gateway.ReconcileReportedUsage(gateway.Usage{
		InputTokens: 1, OutputTokens: 1, TotalTokens: 2, ProviderDetails: map[string]int64{},
	}, gateway.Usage{})
	if empty.ProviderDetails != nil {
		t.Fatalf("empty provider details were not canonicalized: %#v", empty.ProviderDetails)
	}
}

func TestReconcileReportedUsageRetainsFallbackForMalformedAuxiliaryCounters(t *testing.T) {
	fallback := gateway.Usage{
		InputTokens: 10, OutputTokens: 20, TotalTokens: 30,
		CacheReadTokens: 3, CacheWriteTokens: 10,
		InputDetails: &gateway.UsageDetails{
			CachedTokens: 3, AudioTokens: 4, ByModality: map[string]int64{"audio": 4},
		},
		OutputDetails:   &gateway.UsageDetails{ReasoningTokens: 5},
		ProviderDetails: map[string]int64{"search": 4},
	}
	reported := gateway.Usage{
		InputTokens: 10, OutputTokens: 2, TotalTokens: 12,
		CacheReadTokens: -1, CacheWriteTokens: -1,
		InputDetails: &gateway.UsageDetails{
			CachedTokens: -1, AudioTokens: -1, ByModality: map[string]int64{"audio": -1},
		},
		OutputDetails:   &gateway.UsageDetails{ReasoningTokens: -1},
		ProviderDetails: map[string]int64{"search": -1},
	}
	got := gateway.ReconcileReportedUsage(reported, fallback)
	if got.InputTokens != 10 || got.OutputTokens != 2 || got.TotalTokens != 12 {
		t.Fatalf("core usage = %#v, want valid provider token dimensions", got)
	}
	if got.CacheReadTokens != 3 || got.CacheWriteTokens != 10 || got.ProviderDetails["search"] != 4 {
		t.Fatalf("auxiliary usage = %#v, want malformed dimensions restored from fallback", got)
	}
	if got.InputDetails == nil || got.InputDetails.CachedTokens != 3 || got.InputDetails.AudioTokens != 4 ||
		got.InputDetails.ByModality["audio"] != 4 || got.OutputDetails == nil || got.OutputDetails.ReasoningTokens != 5 {
		t.Fatalf("detailed usage = %#v, want malformed details restored from fallback", got)
	}

	authoritative := gateway.ReconcileReportedUsage(gateway.Usage{
		InputTokens: 10, OutputTokens: 2, TotalTokens: 12,
	}, fallback)
	if authoritative.CacheReadTokens != 0 || authoritative.CacheWriteTokens != 0 ||
		authoritative.InputDetails != nil || authoritative.OutputDetails != nil || len(authoritative.ProviderDetails) != 0 {
		t.Fatalf("valid completed usage did not refund absent auxiliary reservations: %#v", authoritative)
	}
}
