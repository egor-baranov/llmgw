package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"llmgw/gateway"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

var (
	redisAcquireSlotScript = redis.NewScript(`
local setKey = KEYS[1]
local countKey = KEYS[2]
local limit = tonumber(ARGV[1])
local token = ARGV[2]
local ttl = tonumber(ARGV[3])
if limit <= 0 then
	return 1
end
local added = redis.call("SADD", setKey, token)
if added == 0 then
	redis.call("PEXPIRE", setKey, ttl)
	redis.call("PEXPIRE", countKey, ttl)
	return 1
end
local current = redis.call("INCR", countKey)
if current > limit then
	redis.call("DECR", countKey)
	redis.call("SREM", setKey, token)
	return 0
end
redis.call("PEXPIRE", setKey, ttl)
redis.call("PEXPIRE", countKey, ttl)
return 1
`)
	redisReleaseSlotScript = redis.NewScript(`
local setKey = KEYS[1]
local countKey = KEYS[2]
local token = ARGV[1]
local removed = redis.call("SREM", setKey, token)
if removed == 1 then
	local current = redis.call("DECR", countKey)
	if current <= 0 then
		redis.call("DEL", countKey)
	end
end
if redis.call("SCARD", setKey) == 0 then
	redis.call("DEL", setKey)
end
return 1
`)
	redisBreakerAllowScript = redis.NewScript(`
local key = KEYS[1]
local nowMs = tonumber(ARGV[1])
local openUntilMs = tonumber(redis.call("HGET", key, "open_until_ms") or "0")
if openUntilMs > nowMs then
	return 0
end
return 1
`)
	redisBreakerFailScript = redis.NewScript(`
local key = KEYS[1]
local nowMs = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local cooldownMs = tonumber(ARGV[3])
local ttlMs = tonumber(ARGV[4])
local message = ARGV[5]
if threshold <= 0 then
	threshold = 1
end
if cooldownMs <= 0 then
	cooldownMs = 1000
end
local openUntilMs = tonumber(redis.call("HGET", key, "open_until_ms") or "0")
if openUntilMs > nowMs then
	redis.call("HSET", key, "last_failure_ms", nowMs, "last_error", message)
	redis.call("PEXPIRE", key, ttlMs)
	return openUntilMs
end
local failures = tonumber(redis.call("HGET", key, "failures") or "0") + 1
if failures >= threshold then
	openUntilMs = nowMs + cooldownMs
	failures = 0
end
redis.call("HSET", key,
	"failures", failures,
	"open_until_ms", openUntilMs,
	"last_failure_ms", nowMs,
	"last_error", message
)
redis.call("PEXPIRE", key, ttlMs)
return openUntilMs
`)
	redisBreakerSuccessScript = redis.NewScript(`
local key = KEYS[1]
local nowMs = tonumber(ARGV[1])
local ttlMs = tonumber(ARGV[2])
redis.call("HSET", key,
	"failures", 0,
	"open_until_ms", 0,
	"last_success_ms", nowMs,
	"last_error", ""
)
redis.call("PEXPIRE", key, ttlMs)
return 1
`)
	redisQuotaReserveScript = redis.NewScript(`
local scopeCount = tonumber(ARGV[1])
local holdTokens = tonumber(ARGV[2])
local holdSpend = tonumber(ARGV[3])
local reserveTtlMs = tonumber(ARGV[4])
local minuteTtlMs = tonumber(ARGV[5])
local payload = ARGV[6]
local ticketKey = KEYS[scopeCount * 9 + 1]
if redis.call("EXISTS", ticketKey) == 1 then
	return {2, "exists"}
end
for i = 1, scopeCount do
	local keyOffset = (i - 1) * 9
	local activeKey = KEYS[keyOffset + 1]
	local rpmKey = KEYS[keyOffset + 2]
	local tpmKey = KEYS[keyOffset + 3]
	local spendUsedKey = KEYS[keyOffset + 4]
	local spendHeldKey = KEYS[keyOffset + 5]
	local dayUsedKey = KEYS[keyOffset + 6]
	local dayHeldKey = KEYS[keyOffset + 7]
	local monthUsedKey = KEYS[keyOffset + 8]
	local monthHeldKey = KEYS[keyOffset + 9]
	local argOffset = 6 + (i - 1) * 6
	local maxParallel = tonumber(ARGV[argOffset + 1])
	local rpmLimit = tonumber(ARGV[argOffset + 2])
	local tpmLimit = tonumber(ARGV[argOffset + 3])
	local maxSpend = tonumber(ARGV[argOffset + 4])
	local dailyTokens = tonumber(ARGV[argOffset + 5])
	local monthlyTokens = tonumber(ARGV[argOffset + 6])
	if maxParallel > 0 then
		local current = tonumber(redis.call("GET", activeKey) or "0")
		if current + 1 > maxParallel then
			return {0, "max_parallel"}
		end
	end
	if rpmLimit > 0 then
		local current = tonumber(redis.call("GET", rpmKey) or "0")
		if current + 1 > rpmLimit then
			return {0, "rpm"}
		end
	end
	if tpmLimit > 0 then
		local current = tonumber(redis.call("GET", tpmKey) or "0")
		if current + holdTokens > tpmLimit then
			return {0, "tpm"}
		end
	end
	if maxSpend > 0 then
		local used = tonumber(redis.call("GET", spendUsedKey) or "0")
		local held = tonumber(redis.call("GET", spendHeldKey) or "0")
		if used + held + holdSpend > maxSpend then
			return {0, "spend"}
		end
	end
	if dailyTokens > 0 then
		local used = tonumber(redis.call("GET", dayUsedKey) or "0")
		local held = tonumber(redis.call("GET", dayHeldKey) or "0")
		if used + held + holdTokens > dailyTokens then
			return {0, "daily_tokens"}
		end
	end
	if monthlyTokens > 0 then
		local used = tonumber(redis.call("GET", monthUsedKey) or "0")
		local held = tonumber(redis.call("GET", monthHeldKey) or "0")
		if used + held + holdTokens > monthlyTokens then
			return {0, "monthly_tokens"}
		end
	end
end
for i = 1, scopeCount do
	local keyOffset = (i - 1) * 9
	local activeKey = KEYS[keyOffset + 1]
	local rpmKey = KEYS[keyOffset + 2]
	local tpmKey = KEYS[keyOffset + 3]
	local spendHeldKey = KEYS[keyOffset + 5]
	local dayHeldKey = KEYS[keyOffset + 7]
	local monthHeldKey = KEYS[keyOffset + 9]
	redis.call("INCRBY", activeKey, 1)
	redis.call("PEXPIRE", activeKey, reserveTtlMs)
	redis.call("INCRBY", rpmKey, 1)
	redis.call("PEXPIRE", rpmKey, minuteTtlMs)
	redis.call("INCRBY", tpmKey, holdTokens)
	redis.call("PEXPIRE", tpmKey, minuteTtlMs)
	redis.call("INCRBY", spendHeldKey, holdSpend)
	redis.call("PEXPIRE", spendHeldKey, reserveTtlMs)
	redis.call("INCRBY", dayHeldKey, holdTokens)
	redis.call("PEXPIRE", dayHeldKey, reserveTtlMs)
	redis.call("INCRBY", monthHeldKey, holdTokens)
	redis.call("PEXPIRE", monthHeldKey, reserveTtlMs)
end
redis.call("SET", ticketKey, payload, "PX", reserveTtlMs)
return {1, "ok"}
`)
)

type RedisStore struct {
	client  *redis.Client
	limiter *redis_rate.Limiter
}

func NewRedisStore(addr, passwordEnv string, db int) *RedisStore {
	return &RedisStore{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: os.Getenv(passwordEnv),
			DB:       db,
		}),
	}
}

func (s *RedisStore) initLimiter() {
	if s.limiter == nil {
		s.limiter = redis_rate.NewLimiter(s.client)
	}
}

func (s *RedisStore) Allow(ctx context.Context, key string, limit RateLimit, n int64) error {
	if limit.Rate <= 0 || n <= 0 {
		return nil
	}
	s.initLimiter()
	result, err := s.limiter.AllowN(ctx, key, redis_rate.Limit{
		Rate:   int(limit.Rate),
		Burst:  burst(limit),
		Period: normalizeRatePeriod(limit.Period),
	}, int(n))
	if err != nil {
		return err
	}
	if result.Allowed < int(n) {
		return gateway.RateLimited("rate limit exceeded")
	}
	return nil
}

func (s *RedisStore) Add(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	pipe := s.client.TxPipeline()
	value := pipe.IncrBy(ctx, key, delta)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return value.Val(), nil
}

func (s *RedisStore) AcquireSlot(ctx context.Context, bucket, token string, limit int64, ttl time.Duration) error {
	if bucket == "" || limit <= 0 {
		return nil
	}
	if token == "" {
		token = randomToken()
	}
	ttl = normalizeAttemptSlotTTL(ttl)
	result, err := redisAcquireSlotScript.Run(ctx, s.client, []string{
		s.attemptSlotSetKey(bucket),
		s.attemptSlotCountKey(bucket),
	}, limit, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return err
	}
	if result != 1 {
		return gateway.RateLimited("concurrency limit exceeded")
	}
	return nil
}

func (s *RedisStore) ReleaseSlot(ctx context.Context, bucket, token string) error {
	if bucket == "" || token == "" {
		return nil
	}
	_, err := redisReleaseSlotScript.Run(ctx, s.client, []string{
		s.attemptSlotSetKey(bucket),
		s.attemptSlotCountKey(bucket),
	}, token).Result()
	return err
}

func (s *RedisStore) BreakerAllow(ctx context.Context, route string, now time.Time) (bool, error) {
	if route == "" {
		return true, nil
	}
	result, err := redisBreakerAllowScript.Run(ctx, s.client, []string{s.breakerKey(route)}, now.UnixMilli()).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (s *RedisStore) BreakerFail(ctx context.Context, route string, threshold int, cooldown time.Duration, message string, now time.Time) error {
	if route == "" {
		return nil
	}
	if threshold <= 0 {
		threshold = 1
	}
	if cooldown <= 0 {
		cooldown = time.Second
	}
	ttl := breakerStateTTL(cooldown)
	_, err := redisBreakerFailScript.Run(ctx, s.client, []string{s.breakerKey(route)}, now.UnixMilli(), threshold, cooldown.Milliseconds(), ttl.Milliseconds(), message).Result()
	return err
}

func (s *RedisStore) BreakerSuccess(ctx context.Context, route string, now time.Time) error {
	if route == "" {
		return nil
	}
	ttl := breakerStateTTL(time.Minute)
	_, err := redisBreakerSuccessScript.Run(ctx, s.client, []string{s.breakerKey(route)}, now.UnixMilli(), ttl.Milliseconds()).Result()
	return err
}

type redisReservation struct {
	Ticket gateway.QuotaTicket     `json:"ticket"`
	Held   gateway.EstimatedUsage  `json:"held"`
	Scopes []redisScopeReservation `json:"scopes"`
}

type redisScopeReservation struct {
	Ref  gateway.ScopeRef `json:"ref"`
	Keys redisQuotaKeys   `json:"keys"`
}

type redisQuotaKeys struct {
	Active    string `json:"active"`
	RPM       string `json:"rpm"`
	TPM       string `json:"tpm"`
	SpendUsed string `json:"spend_used"`
	SpendHeld string `json:"spend_held"`
	DayUsed   string `json:"day_used"`
	DayHeld   string `json:"day_held"`
	MonthUsed string `json:"month_used"`
	MonthHeld string `json:"month_held"`
}

func (s *RedisStore) Reserve(ctx context.Context, requestID string, scopes []gateway.ScopedLimit, estimate gateway.EstimatedUsage, ttl time.Duration) (gateway.QuotaTicket, error) {
	if requestID == "" {
		return gateway.QuotaTicket{}, nil
	}
	now := time.Now()
	ttl = normalizeReservationTTL(ttl)
	resolved := resolveScopeReservations(scopes, now)
	ticket := gateway.QuotaTicket{RequestID: requestID, Scopes: scopedRefs(resolved)}
	payload, err := json.Marshal(redisReservation{
		Ticket: ticket,
		Held:   estimate,
		Scopes: toRedisScopes(resolved),
	})
	if err != nil {
		return gateway.QuotaTicket{}, err
	}
	if len(resolved) == 0 {
		if err := s.client.Set(ctx, s.ticketKey(requestID), payload, ttl).Err(); err != nil {
			return gateway.QuotaTicket{}, err
		}
		return ticket, nil
	}
	keys, args := reserveScriptInput(requestID, resolved, estimate, ttl, now, payload)
	result, err := redisQuotaReserveScript.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return gateway.QuotaTicket{}, err
	}
	status, reason, err := parseScriptTuple(result)
	if err != nil {
		return gateway.QuotaTicket{}, err
	}
	switch status {
	case 1, 2:
		return ticket, nil
	default:
		return gateway.QuotaTicket{}, gateway.RateLimited("quota exceeded: " + quotaReasonLabel(reason))
	}
}

func (s *RedisStore) TopUp(ctx context.Context, ticket gateway.QuotaTicket, scopes []gateway.ScopedLimit, delta gateway.EstimatedUsage, ttl time.Duration) error {
	if ticket.RequestID == "" || delta.TotalTokens() == 0 && delta.EstimatedSpendMicros == 0 {
		return nil
	}
	reservation, err := s.loadReservation(ctx, ticket.RequestID)
	if err != nil || reservation == nil {
		return err
	}
	now := time.Now()
	for _, scope := range resolveScopeReservations(scopes, now) {
		if err := s.checkScope(ctx, scope, delta, true); err != nil {
			return err
		}
	}
	for _, scope := range reservation.Scopes {
		if err := s.applyScope(ctx, memoryScopeReservation{ref: scope.Ref, keys: fromRedisKeys(scope.Keys)}, delta, ttl, true); err != nil {
			return err
		}
	}
	reservation.Held.InputTokens += delta.InputTokens
	reservation.Held.ReservedOutputTokens += delta.ReservedOutputTokens
	reservation.Held.EstimatedSpendMicros += delta.EstimatedSpendMicros
	payload, err := json.Marshal(reservation)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.ticketKey(ticket.RequestID), payload, ttl).Err()
}

func (s *RedisStore) Commit(ctx context.Context, ticket gateway.QuotaTicket, actual gateway.ActualUsage) error {
	reservation, err := s.loadReservation(ctx, ticket.RequestID)
	if err != nil || reservation == nil {
		return err
	}
	pipe := s.client.TxPipeline()
	for _, scope := range reservation.Scopes {
		pipe.DecrBy(ctx, scope.Keys.Active, 1)
		pipe.DecrBy(ctx, scope.Keys.SpendHeld, reservation.Held.EstimatedSpendMicros)
		pipe.DecrBy(ctx, scope.Keys.DayHeld, reservation.Held.TotalTokens())
		pipe.DecrBy(ctx, scope.Keys.MonthHeld, reservation.Held.TotalTokens())
		pipe.IncrBy(ctx, scope.Keys.SpendUsed, actual.SpendMicros)
		pipe.IncrBy(ctx, scope.Keys.DayUsed, actual.TotalTokens())
		pipe.IncrBy(ctx, scope.Keys.MonthUsed, actual.TotalTokens())
	}
	pipe.Del(ctx, s.ticketKey(ticket.RequestID))
	_, err = pipe.Exec(ctx)
	return err
}

func (s *RedisStore) Refund(ctx context.Context, ticket gateway.QuotaTicket) error {
	reservation, err := s.loadReservation(ctx, ticket.RequestID)
	if err != nil || reservation == nil {
		return err
	}
	pipe := s.client.TxPipeline()
	for _, scope := range reservation.Scopes {
		pipe.DecrBy(ctx, scope.Keys.Active, 1)
		pipe.DecrBy(ctx, scope.Keys.SpendHeld, reservation.Held.EstimatedSpendMicros)
		pipe.DecrBy(ctx, scope.Keys.DayHeld, reservation.Held.TotalTokens())
		pipe.DecrBy(ctx, scope.Keys.MonthHeld, reservation.Held.TotalTokens())
	}
	pipe.Del(ctx, s.ticketKey(ticket.RequestID))
	_, err = pipe.Exec(ctx)
	return err
}

func (s *RedisStore) loadReservation(ctx context.Context, requestID string) (*redisReservation, error) {
	if requestID == "" {
		return nil, nil
	}
	raw, err := s.client.Get(ctx, s.ticketKey(requestID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var reservation redisReservation
	if err := json.Unmarshal(raw, &reservation); err != nil {
		return nil, err
	}
	return &reservation, nil
}

func (s *RedisStore) checkScope(ctx context.Context, scope memoryScopeReservation, estimate gateway.EstimatedUsage, topUp bool) error {
	if scope.limit.MaxParallel > 0 && !topUp {
		current, err := s.client.Get(ctx, scope.keys.active).Int64()
		if err != nil && err != redis.Nil {
			return err
		}
		if current+1 > scope.limit.MaxParallel {
			return gateway.RateLimited("quota exceeded: max parallel")
		}
	}
	if scope.limit.RPM > 0 && !topUp {
		current, err := s.client.Get(ctx, scope.keys.rpm).Int64()
		if err != nil && err != redis.Nil {
			return err
		}
		if current+1 > scope.limit.RPM {
			return gateway.RateLimited("quota exceeded: rpm")
		}
	}
	if scope.limit.TPM > 0 {
		current, err := s.client.Get(ctx, scope.keys.tpm).Int64()
		if err != nil && err != redis.Nil {
			return err
		}
		if current+estimate.TotalTokens() > scope.limit.TPM {
			return gateway.RateLimited("quota exceeded: tpm")
		}
	}
	if scope.limit.MaxSpendMicros > 0 {
		used, err := s.client.Get(ctx, scope.keys.spendUsed).Int64()
		if err != nil && err != redis.Nil {
			return err
		}
		held, err := s.client.Get(ctx, scope.keys.spendHeld).Int64()
		if err != nil && err != redis.Nil {
			return err
		}
		if used+held+estimate.EstimatedSpendMicros > scope.limit.MaxSpendMicros {
			return gateway.RateLimited("quota exceeded: spend")
		}
	}
	if scope.limit.DailyTokens > 0 {
		used, err := s.client.Get(ctx, scope.keys.dayUsed).Int64()
		if err != nil && err != redis.Nil {
			return err
		}
		held, err := s.client.Get(ctx, scope.keys.dayHeld).Int64()
		if err != nil && err != redis.Nil {
			return err
		}
		if used+held+estimate.TotalTokens() > scope.limit.DailyTokens {
			return gateway.RateLimited("quota exceeded: daily tokens")
		}
	}
	if scope.limit.MonthlyTokens > 0 {
		used, err := s.client.Get(ctx, scope.keys.monthUsed).Int64()
		if err != nil && err != redis.Nil {
			return err
		}
		held, err := s.client.Get(ctx, scope.keys.monthHeld).Int64()
		if err != nil && err != redis.Nil {
			return err
		}
		if used+held+estimate.TotalTokens() > scope.limit.MonthlyTokens {
			return gateway.RateLimited("quota exceeded: monthly tokens")
		}
	}
	return nil
}

func (s *RedisStore) applyScope(ctx context.Context, scope memoryScopeReservation, estimate gateway.EstimatedUsage, ttl time.Duration, topUp bool) error {
	now := time.Now()
	pipe := s.client.TxPipeline()
	if !topUp {
		pipe.IncrBy(ctx, scope.keys.active, 1)
		pipe.Expire(ctx, scope.keys.active, ttl)
		pipe.IncrBy(ctx, scope.keys.rpm, 1)
		pipe.Expire(ctx, scope.keys.rpm, timeUntilNextMinute(now))
	}
	pipe.IncrBy(ctx, scope.keys.tpm, estimate.TotalTokens())
	pipe.Expire(ctx, scope.keys.tpm, timeUntilNextMinute(now))
	pipe.IncrBy(ctx, scope.keys.spendHeld, estimate.EstimatedSpendMicros)
	pipe.Expire(ctx, scope.keys.spendHeld, ttl)
	pipe.IncrBy(ctx, scope.keys.dayHeld, estimate.TotalTokens())
	pipe.Expire(ctx, scope.keys.dayHeld, ttl)
	pipe.IncrBy(ctx, scope.keys.monthHeld, estimate.TotalTokens())
	pipe.Expire(ctx, scope.keys.monthHeld, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisStore) attemptSlotSetKey(bucket string) string {
	return "attempt:" + bucket + ":slots"
}

func (s *RedisStore) attemptSlotCountKey(bucket string) string {
	return "attempt:" + bucket + ":count"
}

func (s *RedisStore) breakerKey(route string) string {
	return "breaker:route:" + route
}

func (s *RedisStore) ticketKey(requestID string) string {
	return "quota:ticket:" + requestID
}

func reserveScriptInput(requestID string, scopes []memoryScopeReservation, estimate gateway.EstimatedUsage, ttl time.Duration, now time.Time, payload []byte) ([]string, []interface{}) {
	keys := make([]string, 0, len(scopes)*9+1)
	args := make([]interface{}, 0, 6+len(scopes)*6)
	args = append(args,
		int64(len(scopes)),
		estimate.TotalTokens(),
		estimate.EstimatedSpendMicros,
		normalizeScriptTTL(ttl.Milliseconds()),
		normalizeScriptTTL(timeUntilNextMinute(now).Milliseconds()),
		string(payload),
	)
	for _, scope := range scopes {
		keys = append(keys,
			scope.keys.active,
			scope.keys.rpm,
			scope.keys.tpm,
			scope.keys.spendUsed,
			scope.keys.spendHeld,
			scope.keys.dayUsed,
			scope.keys.dayHeld,
			scope.keys.monthUsed,
			scope.keys.monthHeld,
		)
		args = append(args,
			scope.limit.MaxParallel,
			scope.limit.RPM,
			scope.limit.TPM,
			scope.limit.MaxSpendMicros,
			scope.limit.DailyTokens,
			scope.limit.MonthlyTokens,
		)
	}
	keys = append(keys, "quota:ticket:"+requestID)
	return keys, args
}

func parseScriptTuple(value any) (int64, string, error) {
	items, ok := value.([]interface{})
	if !ok || len(items) == 0 {
		return 0, "", fmt.Errorf("invalid redis script result: %T", value)
	}
	status, err := toInt64(items[0])
	if err != nil {
		return 0, "", err
	}
	reason := ""
	if len(items) > 1 {
		switch raw := items[1].(type) {
		case string:
			reason = raw
		case []byte:
			reason = string(raw)
		default:
			reason = fmt.Sprint(raw)
		}
	}
	return status, reason, nil
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("invalid integer type: %T", value)
	}
}

func randomToken() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func breakerStateTTL(cooldown time.Duration) time.Duration {
	if cooldown <= 0 {
		cooldown = time.Minute
	}
	ttl := cooldown * 3
	if ttl < time.Hour {
		ttl = time.Hour
	}
	return ttl
}

func normalizeAttemptSlotTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 30 * time.Second
	}
	return ttl
}

func normalizeReservationTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 10 * time.Minute
	}
	return ttl
}

func normalizeScriptTTL(ms int64) int64 {
	if ms <= 0 {
		return 1
	}
	return ms
}

func quotaReasonLabel(reason string) string {
	switch reason {
	case "max_parallel":
		return "max parallel"
	case "rpm":
		return "rpm"
	case "tpm":
		return "tpm"
	case "spend":
		return "spend"
	case "daily_tokens":
		return "daily tokens"
	case "monthly_tokens":
		return "monthly tokens"
	default:
		return "quota"
	}
}

func toRedisScopes(scopes []memoryScopeReservation) []redisScopeReservation {
	out := make([]redisScopeReservation, 0, len(scopes))
	for _, scope := range scopes {
		out = append(out, redisScopeReservation{
			Ref: scope.ref,
			Keys: redisQuotaKeys{
				Active:    scope.keys.active,
				RPM:       scope.keys.rpm,
				TPM:       scope.keys.tpm,
				SpendUsed: scope.keys.spendUsed,
				SpendHeld: scope.keys.spendHeld,
				DayUsed:   scope.keys.dayUsed,
				DayHeld:   scope.keys.dayHeld,
				MonthUsed: scope.keys.monthUsed,
				MonthHeld: scope.keys.monthHeld,
			},
		})
	}
	return out
}

func fromRedisKeys(keys redisQuotaKeys) quotaKeys {
	return quotaKeys{
		active:    keys.Active,
		rpm:       keys.RPM,
		tpm:       keys.TPM,
		spendUsed: keys.SpendUsed,
		spendHeld: keys.SpendHeld,
		dayUsed:   keys.DayUsed,
		dayHeld:   keys.DayHeld,
		monthUsed: keys.MonthUsed,
		monthHeld: keys.MonthHeld,
	}
}

func normalizeRatePeriod(period time.Duration) time.Duration {
	if period <= 0 {
		return time.Minute
	}
	return period
}
