package proxy_test

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"llmgw/gateway"
	"llmgw/proxy"
)

func TestStreamUsageMergeSaturatesProviderTokenTotals(t *testing.T) {
	maxTokens := strconv.FormatInt(math.MaxInt64, 10)
	body := "data: {\"id\":\"chat_1\",\"choices\":[],\"usage\":{\"prompt_tokens\":" + maxTokens + ",\"completion_tokens\":0,\"total_tokens\":1}}\n\n" +
		"data: {\"id\":\"chat_1\",\"choices\":[],\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":1,\"total_tokens\":1}}\n\n" +
		"data: [DONE]\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()

	provider := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client())
	result, err := provider.Invoke(context.Background(), gateway.ResolvedRoute{Route: &gateway.Route{
		Provider: "openai",
		BaseURL:  upstream.URL + "/v1",
		Model:    "route-model",
	}}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Stream:    true,
		RawBody:   json.RawMessage(`{"model":"public-model","stream":true,"messages":[]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, result.RawStream); err != nil {
		t.Fatal(err)
	}
	usage := result.FinalUsage()
	if usage.InputTokens != math.MaxInt64 || usage.OutputTokens != 1 || usage.TotalTokens != math.MaxInt64 {
		t.Fatalf("merged usage = %#v, want saturated input/output total", usage)
	}
}

func TestStreamUsageMergePreservesMalformedCacheSignal(t *testing.T) {
	body := "data: {\"id\":\"chat_1\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"total_tokens\":12,\"prompt_tokens_details\":{\"cached_tokens\":-1}}}\n\n" +
		"data: [DONE]\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()

	estimate := gateway.Usage{
		InputTokens: 10, OutputTokens: 20, TotalTokens: 30, CacheReadTokens: 5,
		InputDetails: &gateway.UsageDetails{CachedTokens: 5},
	}
	result, err := proxy.NewProvider(proxy.OpenAIAdapter(), upstream.Client()).Invoke(context.Background(), gateway.ResolvedRoute{
		Route:    &gateway.Route{Provider: "openai", BaseURL: upstream.URL + "/v1", Model: "route-model"},
		Estimate: estimate,
	}, &gateway.Request{
		Operation: gateway.OpChatCompletions,
		Stream:    true,
		RawBody:   json.RawMessage(`{"model":"public-model","stream":true,"messages":[]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, result.RawStream); err != nil {
		t.Fatal(err)
	}
	reported := result.FinalUsage()
	if reported.CacheReadTokens >= 0 || reported.InputDetails == nil || reported.InputDetails.CachedTokens >= 0 {
		t.Fatalf("tracked usage = %#v, want malformed cache signal preserved", reported)
	}
	reconciled := gateway.ReconcileReportedUsage(reported, estimate)
	if reconciled.InputTokens != 10 || reconciled.OutputTokens != 2 || reconciled.CacheReadTokens != 5 {
		t.Fatalf("reconciled usage = %#v, want valid core with fallback cache usage", reconciled)
	}
}
