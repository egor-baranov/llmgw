package policy

import (
	"fmt"
	"testing"
	"time"

	"llmgw/gateway"
)

func TestLocalLimiterPrunesIdleRouteChurnWithoutDroppingActiveEntry(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	limits := &AttemptLimits{now: func() time.Time { return now }}
	activeRelease, err := limits.acquireLimiter("route:active", 1, "busy")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < localPolicyPruneEvery*3; i++ {
		release, err := limits.acquireLimiter(fmt.Sprintf("route:old-%d", i), 1, "busy")
		if err != nil {
			t.Fatal(err)
		}
		release()
		now = now.Add(localPolicyStateIdleTTL + time.Second)
	}

	_, activeExists := limits.limiters.Load("route:active")
	count := 0
	limits.limiters.Range(func(_, _ any) bool {
		count++
		return true
	})
	if !activeExists {
		t.Fatal("active limiter was pruned")
	}
	if count > localPolicyPruneEvery+1 {
		t.Fatalf("limiter state = %d entries, want <= %d", count, localPolicyPruneEvery+1)
	}
	activeRelease()
}

func TestLocalBreakerPrunesIdleRouteChurnAndRetainsOpenCircuit(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	breaker := NewBreaker()
	breaker.now = func() time.Time { return now }
	for i := 0; i < localPolicyPruneEvery*3; i++ {
		route := &gateway.Route{Name: fmt.Sprintf("old-%d", i), Provider: "openai", Model: "model"}
		breaker.SuccessAttempt(route, now)
		now = now.Add(localPolicyStateIdleTTL + time.Second)
	}
	if got := len(breaker.states); got > localPolicyPruneEvery {
		t.Fatalf("breaker state = %d entries, want <= %d", got, localPolicyPruneEvery)
	}

	now = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	breaker = NewBreaker()
	breaker.now = func() time.Time { return now }
	openRoute := &gateway.Route{
		Name: "open", Provider: "openai", Model: "model",
		Circuit: gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Hour}},
	}
	breaker.FailAttempt(openRoute, now, fmt.Errorf("failed"))
	now = now.Add(20 * time.Minute)
	for i := 1; i < localPolicyPruneEvery; i++ {
		breaker.SuccessAttempt(&gateway.Route{Name: fmt.Sprintf("live-%d", i), Provider: "openai", Model: "model"}, now)
	}
	if _, exists := breaker.states[routeBreakerKey(openRoute)]; !exists {
		t.Fatal("open circuit was pruned before its cooldown elapsed")
	}
}

func TestLocalBreakerRetainsNewerEvidenceWhileOlderCallCanStillFinish(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	breaker := NewBreaker()
	breaker.now = func() time.Time { return now }
	route := &gateway.Route{
		Name: "long", Provider: "openai", Model: "model",
		Timeout: gateway.Duration{Duration: 30 * time.Minute},
		Circuit: gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Hour}},
	}
	_, olderStartedAt := breaker.AllowAttempt(route)
	now = now.Add(time.Minute)
	_, newerStartedAt := breaker.AllowAttempt(route)
	breaker.SuccessAttempt(route, newerStartedAt)

	now = now.Add(localPolicyStateIdleTTL + time.Minute)
	breaker.operations = localPolicyPruneEvery - 1
	breaker.SuccessAttempt(&gateway.Route{Name: "prune-trigger", Provider: "openai", Model: "model"}, now)
	key := routeBreakerKey(route)
	if _, exists := breaker.states[key]; !exists {
		t.Fatal("newer breaker evidence was pruned while an older call could still be in flight")
	}

	breaker.FailAttempt(route, olderStartedAt, fmt.Errorf("stale failure"))
	state := breaker.states[key]
	if !state.LastFailure.IsZero() || state.OpenUntil.After(now) {
		t.Fatalf("stale failure changed retained breaker state: %#v", state)
	}
}

func TestLocalBreakerOrdersOutOfOrderResultsByAdmission(t *testing.T) {
	now := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	breaker := NewBreaker()
	breaker.now = func() time.Time { return now }
	route := &gateway.Route{
		Name: "ordered", Provider: "openai", Model: "model",
		Circuit: gateway.CircuitConfig{Failures: 1, Cooldown: gateway.Duration{Duration: time.Hour}},
	}
	_, olderStartedAt := breaker.AllowAttempt(route)
	now = now.Add(time.Second)
	_, newerStartedAt := breaker.AllowAttempt(route)
	now = now.Add(time.Second)
	breaker.FailAttempt(route, olderStartedAt, fmt.Errorf("older request failed first"))
	now = now.Add(time.Second)
	breaker.SuccessAttempt(route, newerStartedAt)
	if allowed, _ := breaker.AllowAttempt(route); !allowed {
		t.Fatal("newer admitted success did not close the older failure's circuit")
	}
	state := breaker.states[routeBreakerKey(route)]
	if !state.LastFailure.Equal(olderStartedAt) || !state.LastSuccess.Equal(newerStartedAt) {
		t.Fatalf("breaker evidence = %#v, want admission timestamps", state)
	}

	breaker = NewBreaker()
	breaker.now = func() time.Time { return now }
	_, olderStartedAt = breaker.AllowAttempt(route)
	now = now.Add(time.Second)
	_, newerStartedAt = breaker.AllowAttempt(route)
	now = now.Add(time.Second)
	breaker.FailAttempt(route, newerStartedAt, fmt.Errorf("newer request failed first"))
	now = now.Add(time.Second)
	breaker.FailAttempt(route, olderStartedAt, fmt.Errorf("older request failed later"))
	state = breaker.states[routeBreakerKey(route)]
	if !state.LastFailure.Equal(newerStartedAt) {
		t.Fatalf("last failure = %v, want newest admission %v", state.LastFailure, newerStartedAt)
	}
}

func TestLocalBreakerAdmissionTimestampsAreMonotonic(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	breaker := NewBreaker()
	breaker.now = func() time.Time { return now }
	route := &gateway.Route{Name: "monotonic", Provider: "openai", Model: "model"}

	_, first := breaker.AllowAttempt(route)
	_, second := breaker.AllowAttempt(route)
	_, third := breaker.AllowAttempt(route)
	if !second.After(first) || !third.After(second) {
		t.Fatalf("admission timestamps = (%v, %v, %v), want strictly increasing", first, second, third)
	}
}
