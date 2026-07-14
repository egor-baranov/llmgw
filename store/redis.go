package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"llmgw/gateway"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

var (
	// redisRateBatchScript is the same GCRA admission model used by
	// go-redis/redis_rate, extended to validate every key before mutating any of
	// them. Keys retain redis_rate's "rate:" prefix so switching between a
	// single route limit and combined RPM+TPM admission preserves state.
	redisRateBatchScript = redis.NewScript(`
redis.replicate_commands()

local jan_1_2017 = 1483228800
local redis_time = redis.call("TIME")
local now = (redis_time[1] - jan_1_2017) + (redis_time[2] / 1000000)
local states = {}

for i = 1, #KEYS do
	local arg_offset = (i - 1) * 4
	local burst = tonumber(ARGV[arg_offset + 1])
	local rate = tonumber(ARGV[arg_offset + 2])
	local period = tonumber(ARGV[arg_offset + 3])
	local cost = tonumber(ARGV[arg_offset + 4])
	local emission_interval = period / rate
	local increment = emission_interval * cost
	local burst_offset = emission_interval * burst
	local tat = tonumber(redis.call("GET", KEYS[i]) or now)
	tat = math.max(tat, now)
	local new_tat = tat + increment
	local allow_at = new_tat - burst_offset
	if now - allow_at < 0 then
		return {0, i}
	end
	states[i] = {new_tat, new_tat - now}
end

for i = 1, #KEYS do
	local reset_after = states[i][2]
	if reset_after > 0 then
		redis.call("SET", KEYS[i], states[i][1], "EX", math.ceil(reset_after))
	end
end
return {1, 0}
`)
	redisAcquireSlotScript = redis.NewScript(`
local leaseKey = KEYS[1]
local limit = tonumber(ARGV[1])
local token = ARGV[2]
local ttlMs = tonumber(ARGV[3])
local redisTime = redis.call("TIME")
local nowMs = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
local expiresAtMs = nowMs + ttlMs
if limit <= 0 then
	return 1
end
redis.call("ZREMRANGEBYSCORE", leaseKey, "-inf", nowMs)
if redis.call("ZSCORE", leaseKey, token) then
	redis.call("ZADD", leaseKey, expiresAtMs, token)
	local latest = redis.call("ZREVRANGE", leaseKey, 0, 0, "WITHSCORES")
	if #latest > 0 then
		redis.call("PEXPIREAT", leaseKey, tonumber(latest[2]))
	end
	return 1
end
if redis.call("ZCARD", leaseKey) >= limit then
	return 0
end
redis.call("ZADD", leaseKey, expiresAtMs, token)
local latest = redis.call("ZREVRANGE", leaseKey, 0, 0, "WITHSCORES")
if #latest > 0 then
	redis.call("PEXPIREAT", leaseKey, tonumber(latest[2]))
end
return 1
`)
	redisReleaseSlotScript = redis.NewScript(`
local leaseKey = KEYS[1]
local token = ARGV[1]
redis.call("ZREM", leaseKey, token)
local latest = redis.call("ZREVRANGE", leaseKey, 0, 0, "WITHSCORES")
if #latest == 0 then
	redis.call("DEL", leaseKey)
else
	redis.call("PEXPIREAT", leaseKey, tonumber(latest[2]))
end
return 1
`)
	redisBreakerAllowScript = redis.NewScript(`
local key = KEYS[1]
local retentionMs = tonumber(ARGV[1])
if not retentionMs or retentionMs <= 0 then retentionMs = 1000 end
local redisTime = redis.call("TIME")
local nowUs = tonumber(redisTime[1]) * 1000000 + tonumber(redisTime[2])
local nowMs = math.floor(nowUs / 1000)
local openUntilMs = tonumber(redis.call("HGET", key, "open_until_ms") or "0")
if openUntilMs > nowMs then
	return {0, nowUs}
end
local lastAdmissionUs = tonumber(redis.call("HGET", key, "last_admission_us") or "0")
local admissionUs = nowUs
if admissionUs <= lastAdmissionUs then admissionUs = lastAdmissionUs + 1 end
redis.call("HSET", key, "last_admission_us", admissionUs)
local currentTtlMs = redis.call("PTTL", key)
if currentTtlMs < retentionMs then redis.call("PEXPIRE", key, retentionMs) end
return {1, admissionUs}
`)
	redisBreakerFailScript = redis.NewScript(`
local key = KEYS[1]
local function expireNoEarlier(ttlMs)
	local currentTtlMs = redis.call("PTTL", key)
	if currentTtlMs < ttlMs then redis.call("PEXPIRE", key, ttlMs) end
end
redis.call("HDEL", key, "last_error")
local startedAtUs = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local cooldownMs = tonumber(ARGV[3])
local ttlMs = tonumber(ARGV[4])
local redisTime = redis.call("TIME")
local nowUs = tonumber(redisTime[1]) * 1000000 + tonumber(redisTime[2])
local nowMs = math.floor(nowUs / 1000)
if not startedAtUs or startedAtUs <= 0 then
	startedAtUs = nowUs
end
if threshold <= 0 then
	threshold = 1
end
if cooldownMs <= 0 then
	cooldownMs = 1000
end
local openUntilMs = tonumber(redis.call("HGET", key, "open_until_ms") or "0")
local lastSuccessUs = tonumber(redis.call("HGET", key, "last_success_us") or "0")
if lastSuccessUs > 0 and startedAtUs <= lastSuccessUs then
	return openUntilMs
end
local lastFailureUs = tonumber(redis.call("HGET", key, "last_failure_us") or "0")
if startedAtUs > lastFailureUs then
	lastFailureUs = startedAtUs
end
if openUntilMs > nowMs then
	redis.call("HSET", key, "last_failure_us", lastFailureUs)
	expireNoEarlier(ttlMs)
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
	"last_failure_us", lastFailureUs
)
expireNoEarlier(ttlMs)
return openUntilMs
`)
	redisBreakerSuccessScript = redis.NewScript(`
local key = KEYS[1]
local function expireNoEarlier(ttlMs)
	local currentTtlMs = redis.call("PTTL", key)
	if currentTtlMs < ttlMs then redis.call("PEXPIRE", key, ttlMs) end
end
redis.call("HDEL", key, "last_error")
local startedAtUs = tonumber(ARGV[1])
local ttlMs = tonumber(ARGV[2])
local redisTime = redis.call("TIME")
local nowUs = tonumber(redisTime[1]) * 1000000 + tonumber(redisTime[2])
if not startedAtUs or startedAtUs <= 0 then
	startedAtUs = nowUs
end
local lastFailureUs = tonumber(redis.call("HGET", key, "last_failure_us") or "0")
if lastFailureUs > 0 and startedAtUs <= lastFailureUs then
	return 0
end
local lastSuccessUs = tonumber(redis.call("HGET", key, "last_success_us") or "0")
if startedAtUs > lastSuccessUs then
	lastSuccessUs = startedAtUs
end
redis.call("HSET", key,
	"failures", 0,
	"open_until_ms", 0,
	"last_success_us", lastSuccessUs
)
expireNoEarlier(ttlMs)
return 1
`)
	redisQuotaReserveScript = redis.NewScript(`
local function decrementNonnegative(key, amount)
	if not key or key == "" or amount <= 0 then
		return
	end
	local value = redis.call("INCRBY", key, -amount)
	if value <= 0 then
		redis.call("DEL", key)
	end
end
local function expireNoEarlier(key, nowMs, expiresAtMs)
	if not key or key == "" then return end
	local desired = expiresAtMs - nowMs
	if desired < 1 then desired = 1 end
	local current = redis.call("PTTL", key)
	if current < desired then
		redis.call("PEXPIRE", key, desired)
	end
end
local function prune(leaseSetKey, leaseDataKey, nowMs)
	local expired = redis.call("ZRANGEBYSCORE", leaseSetKey, "-inf", nowMs)
	for _, requestID in ipairs(expired) do
		local raw = redis.call("HGET", leaseDataKey, requestID)
		if raw then
			local lease = cjson.decode(raw)
			decrementNonnegative(lease.active, 1)
			decrementNonnegative(lease.tpm, tonumber(lease.tokens) or 0)
			decrementNonnegative(lease.spend_held, tonumber(lease.spend) or 0)
			decrementNonnegative(lease.day_held, tonumber(lease.tokens) or 0)
			decrementNonnegative(lease.month_held, tonumber(lease.tokens) or 0)
			redis.call("HDEL", leaseDataKey, requestID)
		end
	end
	if #expired > 0 then
		redis.call("ZREMRANGEBYSCORE", leaseSetKey, "-inf", nowMs)
	end
	if redis.call("ZCARD", leaseSetKey) == 0 then
		redis.call("DEL", leaseSetKey, leaseDataKey)
	end
end
local scopeCount = tonumber(ARGV[1])
local holdTokens = tonumber(ARGV[2])
local holdSpend = tonumber(ARGV[3])
local reserveTtlMs = tonumber(ARGV[4])
local minuteTtlMs = tonumber(ARGV[5])
local payload = ARGV[6]
local requestID = ARGV[7]
local fingerprint = ARGV[8]
local nowMs = tonumber(ARGV[9])
local maxAccounting = tonumber(ARGV[10])
local function counterValue(key)
	if not key or key == "" then return 0 end
	local raw = redis.call("GET", key)
	if not raw then return 0 end
	if not string.match(raw, "^%d+$") then return nil end
	local value = tonumber(raw)
	if not value or value < 0 or value > maxAccounting then return nil end
	return value
end
local expiresAtMs = nowMs + reserveTtlMs
local ticketKey = KEYS[scopeCount * 11 + 1]
local settledSetKey = KEYS[scopeCount * 11 + 2]
for i = 1, scopeCount do
	local keyOffset = (i - 1) * 11
	prune(KEYS[keyOffset + 10], KEYS[keyOffset + 11], nowMs)
end
redis.call("ZREMRANGEBYSCORE", settledSetKey, "-inf", nowMs)
if redis.call("ZSCORE", settledSetKey, requestID) then
	return {3, "conflict"}
end
if redis.call("EXISTS", ticketKey) == 1 then
	local existing = cjson.decode(redis.call("GET", ticketKey))
	if existing.fingerprint == fingerprint then
		return {2, "exists"}
	end
	return {3, "conflict"}
end
for i = 1, scopeCount do
	local keyOffset = (i - 1) * 11
	local activeKey = KEYS[keyOffset + 1]
	local rpmKey = KEYS[keyOffset + 2]
	local tpmKey = KEYS[keyOffset + 3]
	local spendUsedKey = KEYS[keyOffset + 4]
	local spendHeldKey = KEYS[keyOffset + 5]
	local dayUsedKey = KEYS[keyOffset + 6]
	local dayHeldKey = KEYS[keyOffset + 7]
	local monthUsedKey = KEYS[keyOffset + 8]
	local monthHeldKey = KEYS[keyOffset + 9]
	local argOffset = 10 + (i - 1) * 6
	local maxParallel = tonumber(ARGV[argOffset + 1])
	local rpmLimit = tonumber(ARGV[argOffset + 2])
	local tpmLimit = tonumber(ARGV[argOffset + 3])
	local maxSpend = tonumber(ARGV[argOffset + 4])
	local dailyTokens = tonumber(ARGV[argOffset + 5])
	local monthlyTokens = tonumber(ARGV[argOffset + 6])
	local active = counterValue(activeKey)
	local rpm = counterValue(rpmKey)
	local tpm = counterValue(tpmKey)
	local spendUsed = counterValue(spendUsedKey)
	local spendHeld = counterValue(spendHeldKey)
	local dayUsed = counterValue(dayUsedKey)
	local dayHeld = counterValue(dayHeldKey)
	local monthUsed = counterValue(monthUsedKey)
	local monthHeld = counterValue(monthHeldKey)
	if not active or not rpm or not tpm or not spendUsed or not spendHeld or
		not dayUsed or not dayHeld or not monthUsed or not monthHeld then
		return {4, "accounting_capacity"}
	end
	if (activeKey ~= "" and 1 > maxAccounting - active) or
		(rpmKey ~= "" and 1 > maxAccounting - rpm) or
		(tpmKey ~= "" and holdTokens > maxAccounting - tpm) or
		(spendHeldKey ~= "" and holdSpend > maxAccounting - spendHeld) or
		(dayHeldKey ~= "" and holdTokens > maxAccounting - dayHeld) or
		(monthHeldKey ~= "" and holdTokens > maxAccounting - monthHeld) then
		return {4, "accounting_capacity"}
	end
	if maxParallel > 0 then
		if active + 1 > maxParallel then
			return {0, "max_parallel"}
		end
	end
	if rpmLimit > 0 then
		if rpm + 1 > rpmLimit then
			return {0, "rpm"}
		end
	end
	if tpmLimit > 0 then
		if tpm + holdTokens > tpmLimit then
			return {0, "tpm"}
		end
	end
	if maxSpend > 0 then
		if spendUsed + spendHeld + holdSpend > maxSpend then
			return {0, "spend"}
		end
	end
	if dailyTokens > 0 then
		if dayUsed + dayHeld + holdTokens > dailyTokens then
			return {0, "daily_tokens"}
		end
	end
	if monthlyTokens > 0 then
		if monthUsed + monthHeld + holdTokens > monthlyTokens then
			return {0, "monthly_tokens"}
		end
	end
end
for i = 1, scopeCount do
	local keyOffset = (i - 1) * 11
	local activeKey = KEYS[keyOffset + 1]
	local rpmKey = KEYS[keyOffset + 2]
	local tpmKey = KEYS[keyOffset + 3]
	local spendHeldKey = KEYS[keyOffset + 5]
	local dayHeldKey = KEYS[keyOffset + 7]
	local monthHeldKey = KEYS[keyOffset + 9]
	local leaseSetKey = KEYS[keyOffset + 10]
	local leaseDataKey = KEYS[keyOffset + 11]
		if activeKey ~= "" then redis.call("INCRBY", activeKey, 1) end
		if rpmKey ~= "" then
			redis.call("INCRBY", rpmKey, 1)
			redis.call("PEXPIRE", rpmKey, minuteTtlMs)
		end
		if tpmKey ~= "" then
			redis.call("INCRBY", tpmKey, holdTokens)
			redis.call("PEXPIRE", tpmKey, minuteTtlMs)
		end
		if spendHeldKey ~= "" then redis.call("INCRBY", spendHeldKey, holdSpend) end
		if dayHeldKey ~= "" then redis.call("INCRBY", dayHeldKey, holdTokens) end
		if monthHeldKey ~= "" then redis.call("INCRBY", monthHeldKey, holdTokens) end
	local lease = cjson.encode({
		active = activeKey,
		tpm = tpmKey,
		spend_held = spendHeldKey,
		day_held = dayHeldKey,
		month_held = monthHeldKey,
		spend = holdSpend,
		tokens = holdTokens
	})
	redis.call("ZADD", leaseSetKey, expiresAtMs, requestID)
	redis.call("HSET", leaseDataKey, requestID, lease)
	local latest = redis.call("ZREVRANGE", leaseSetKey, 0, 0, "WITHSCORES")
	local latestExpiry = expiresAtMs
	if #latest > 0 then latestExpiry = tonumber(latest[2]) end
	redis.call("PEXPIREAT", leaseSetKey, latestExpiry + minuteTtlMs)
	redis.call("PEXPIREAT", leaseDataKey, latestExpiry + minuteTtlMs)
	expireNoEarlier(activeKey, nowMs, expiresAtMs)
	expireNoEarlier(spendHeldKey, nowMs, expiresAtMs)
	expireNoEarlier(dayHeldKey, nowMs, expiresAtMs)
	expireNoEarlier(monthHeldKey, nowMs, expiresAtMs)
end
redis.call("SET", ticketKey, payload, "PX", reserveTtlMs)
return {1, "ok"}
`)
	redisQuotaTopUpScript = redis.NewScript(`
local function decrementNonnegative(key, amount)
	if not key or key == "" or amount <= 0 then return end
	local value = redis.call("INCRBY", key, -amount)
	if value <= 0 then redis.call("DEL", key) end
end
local function prune(leaseSetKey, leaseDataKey, nowMs, protectedID)
	local expired = redis.call("ZRANGEBYSCORE", leaseSetKey, "-inf", nowMs)
	for _, expiredID in ipairs(expired) do
		if expiredID ~= protectedID then
			local raw = redis.call("HGET", leaseDataKey, expiredID)
			if raw then
				local lease = cjson.decode(raw)
				decrementNonnegative(lease.active, 1)
				decrementNonnegative(lease.tpm, tonumber(lease.tokens) or 0)
				decrementNonnegative(lease.spend_held, tonumber(lease.spend) or 0)
				decrementNonnegative(lease.day_held, tonumber(lease.tokens) or 0)
				decrementNonnegative(lease.month_held, tonumber(lease.tokens) or 0)
				redis.call("HDEL", leaseDataKey, expiredID)
			end
			redis.call("ZREM", leaseSetKey, expiredID)
		end
	end
end
local function expireNoEarlier(key, nowMs, expiresAtMs)
	if not key or key == "" then return end
	local desired = expiresAtMs - nowMs
	if desired < 1 then desired = 1 end
	local current = redis.call("PTTL", key)
	if current < desired then redis.call("PEXPIRE", key, desired) end
end
local ticketKey = KEYS[1]
local requestID = ARGV[1]
local deltaTokens = tonumber(ARGV[2])
local deltaSpend = tonumber(ARGV[3])
local ttlMs = tonumber(ARGV[4])
local maxAccounting = tonumber(ARGV[5])
local function counterValue(key)
	if not key or key == "" then return 0 end
	local raw = redis.call("GET", key)
	if not raw then return 0 end
	if not string.match(raw, "^%d+$") then return nil end
	local value = tonumber(raw)
	if not value or value < 0 or value > maxAccounting then return nil end
	return value
end
local redisTime = redis.call("TIME")
local nowMs = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
local minuteTtlMs = (math.floor(nowMs / 60000) + 1) * 60000 - nowMs
if minuteTtlMs < 1 then minuteTtlMs = 1 end
local raw = redis.call("GET", ticketKey)
if not raw then return {0, "missing"} end
local reservation = cjson.decode(raw)
local heldTokens = tonumber(reservation.held.InputTokens or 0) + tonumber(reservation.held.ReservedOutputTokens or 0)
local heldSpend = tonumber(reservation.held.EstimatedSpendMicros or 0)
if heldTokens < 0 or heldTokens > maxAccounting or deltaTokens > maxAccounting - heldTokens or
	heldSpend < 0 or heldSpend > maxAccounting or deltaSpend > maxAccounting - heldSpend then
	return {0, "accounting_capacity"}
end
for _, scope in ipairs(reservation.scopes) do
	prune(scope.keys.lease_set, scope.keys.lease_data, nowMs, requestID)
end
for _, scope in ipairs(reservation.scopes) do
	local keys = scope.keys
	local limits = scope.limit or {}
	if not redis.call("HGET", keys.lease_data, requestID) then return {0, "missing"} end
	local tpm = counterValue(keys.tpm)
	local spendUsed = counterValue(keys.spend_used)
	local spendHeld = counterValue(keys.spend_held)
	local dayUsed = counterValue(keys.day_used)
	local dayHeld = counterValue(keys.day_held)
	local monthUsed = counterValue(keys.month_used)
	local monthHeld = counterValue(keys.month_held)
	if not tpm or not spendUsed or not spendHeld or not dayUsed or not dayHeld or
		not monthUsed or not monthHeld then
		return {0, "accounting_capacity"}
	end
	if (keys.tpm ~= "" and deltaTokens > maxAccounting - tpm) or
		(keys.spend_held ~= "" and deltaSpend > maxAccounting - spendHeld) or
		(keys.day_held ~= "" and deltaTokens > maxAccounting - dayHeld) or
		(keys.month_held ~= "" and deltaTokens > maxAccounting - monthHeld) then
		return {0, "accounting_capacity"}
	end
	if tonumber(limits.tpm or 0) > 0 then
		if tpm + deltaTokens > tonumber(limits.tpm) then return {0, "tpm"} end
	end
	if tonumber(limits.max_spend_micros or 0) > 0 then
		if spendUsed + spendHeld + deltaSpend > tonumber(limits.max_spend_micros) then return {0, "spend"} end
	end
	if tonumber(limits.daily_tokens or 0) > 0 then
		if dayUsed + dayHeld + deltaTokens > tonumber(limits.daily_tokens) then return {0, "daily_tokens"} end
	end
	if tonumber(limits.monthly_tokens or 0) > 0 then
		if monthUsed + monthHeld + deltaTokens > tonumber(limits.monthly_tokens) then return {0, "monthly_tokens"} end
	end
end
local expiresAtMs = nowMs + ttlMs
for _, scope in ipairs(reservation.scopes) do
	local keys = scope.keys
		if keys.tpm ~= "" and deltaTokens > 0 then
			redis.call("INCRBY", keys.tpm, deltaTokens)
			redis.call("PEXPIRE", keys.tpm, minuteTtlMs)
		end
		if keys.spend_held ~= "" and deltaSpend > 0 then redis.call("INCRBY", keys.spend_held, deltaSpend) end
		if keys.day_held ~= "" and deltaTokens > 0 then redis.call("INCRBY", keys.day_held, deltaTokens) end
		if keys.month_held ~= "" and deltaTokens > 0 then redis.call("INCRBY", keys.month_held, deltaTokens) end
	local lease = cjson.decode(redis.call("HGET", keys.lease_data, requestID))
	lease.spend = tonumber(lease.spend or 0) + deltaSpend
	lease.tokens = tonumber(lease.tokens or 0) + deltaTokens
	redis.call("HSET", keys.lease_data, requestID, cjson.encode(lease))
	redis.call("ZADD", keys.lease_set, expiresAtMs, requestID)
	local latest = redis.call("ZREVRANGE", keys.lease_set, 0, 0, "WITHSCORES")
	local latestExpiry = expiresAtMs
	if #latest > 0 then latestExpiry = tonumber(latest[2]) end
		redis.call("PEXPIREAT", keys.lease_set, latestExpiry + minuteTtlMs)
		redis.call("PEXPIREAT", keys.lease_data, latestExpiry + minuteTtlMs)
	expireNoEarlier(keys.active, nowMs, expiresAtMs)
	expireNoEarlier(keys.spend_held, nowMs, expiresAtMs)
	expireNoEarlier(keys.day_held, nowMs, expiresAtMs)
	expireNoEarlier(keys.month_held, nowMs, expiresAtMs)
end
reservation.held.InputTokens = heldTokens + deltaTokens
reservation.held.ReservedOutputTokens = 0
reservation.held.EstimatedSpendMicros = heldSpend + deltaSpend
redis.call("SET", ticketKey, cjson.encode(reservation), "PX", ttlMs)
return {1, "ok"}
`)
	redisQuotaSettleScript = redis.NewScript(`
local function decrementNonnegative(key, amount)
	if not key or key == "" or amount <= 0 then return end
	local value = redis.call("INCRBY", key, -amount)
	if value <= 0 then redis.call("DEL", key) end
end
local function refreshLeaseExpiry(leaseSetKey, leaseDataKey, metadataGraceMs)
	local latest = redis.call("ZREVRANGE", leaseSetKey, 0, 0, "WITHSCORES")
	if #latest == 0 then
		redis.call("DEL", leaseSetKey, leaseDataKey)
	else
		redis.call("PEXPIREAT", leaseSetKey, tonumber(latest[2]) + metadataGraceMs)
		redis.call("PEXPIREAT", leaseDataKey, tonumber(latest[2]) + metadataGraceMs)
	end
end
local function prune(leaseSetKey, leaseDataKey, nowMs, protectedID)
	local expired = redis.call("ZRANGEBYSCORE", leaseSetKey, "-inf", nowMs)
	for _, expiredID in ipairs(expired) do
		if expiredID ~= protectedID then
			local raw = redis.call("HGET", leaseDataKey, expiredID)
			if raw then
				local lease = cjson.decode(raw)
				decrementNonnegative(lease.active, 1)
				decrementNonnegative(lease.tpm, tonumber(lease.tokens) or 0)
				decrementNonnegative(lease.spend_held, tonumber(lease.spend) or 0)
				decrementNonnegative(lease.day_held, tonumber(lease.tokens) or 0)
				decrementNonnegative(lease.month_held, tonumber(lease.tokens) or 0)
				redis.call("HDEL", leaseDataKey, expiredID)
			end
			redis.call("ZREM", leaseSetKey, expiredID)
		end
	end
end
local ticketKey = KEYS[1]
local settledSetKey = KEYS[2]
local requestID = ARGV[1]
local commit = tonumber(ARGV[2])
local actualTokens = tonumber(ARGV[3])
local actualSpend = tonumber(ARGV[4])
local tombstoneTtlMs = tonumber(ARGV[5])
local bucketGraceMs = tonumber(ARGV[6])
local maxAccounting = tonumber(ARGV[7])
local function counterValue(key)
	if not key or key == "" then return 0, false end
	local raw = redis.call("GET", key)
	if not raw then return 0, false end
	if not string.match(raw, "^%d+$") then return nil, true end
	local value = tonumber(raw)
	if not value or value < 0 or value > maxAccounting then return nil, true end
	return value, true
end
local raw = redis.call("GET", ticketKey)
local redisTime = redis.call("TIME")
local nowMs = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
redis.call("ZREMRANGEBYSCORE", settledSetKey, "-inf", nowMs)
if not raw then
	if redis.call("ZSCORE", settledSetKey, requestID) then return 2 end
	return 0
end
local reservation = cjson.decode(raw)
for _, scope in ipairs(reservation.scopes) do
	if not redis.call("HGET", scope.keys.lease_data, requestID) then
		return 0
	end
end
for _, scope in ipairs(reservation.scopes) do
	prune(scope.keys.lease_set, scope.keys.lease_data, nowMs, requestID)
end
local heldTokens = tonumber(reservation.held.InputTokens or 0) + tonumber(reservation.held.ReservedOutputTokens or 0)
local heldSpend = tonumber(reservation.held.EstimatedSpendMicros or 0)
if heldTokens < 0 or heldTokens > maxAccounting or heldSpend < 0 or heldSpend > maxAccounting then
	return 3
end
for _, scope in ipairs(reservation.scopes) do
	local keys = scope.keys
	local tpm, tpmExists = counterValue(keys.tpm)
	local active = counterValue(keys.active)
	local spendHeld = counterValue(keys.spend_held)
	local dayHeld = counterValue(keys.day_held)
	local monthHeld = counterValue(keys.month_held)
	local spendUsed = counterValue(keys.spend_used)
	local dayUsed = counterValue(keys.day_used)
	local monthUsed = counterValue(keys.month_used)
	if not tpm or not active or not spendHeld or not dayHeld or not monthHeld or
		not spendUsed or not dayUsed or not monthUsed then
		return 3
	end
	if commit == 1 then
		if actualTokens > heldTokens and tpmExists and actualTokens - heldTokens > maxAccounting - tpm then
			return 3
		end
		if keys.spend_used ~= "" and actualSpend > maxAccounting - spendUsed then return 3 end
		if keys.day_used ~= "" and actualTokens > maxAccounting - dayUsed then return 3 end
		if keys.month_used ~= "" and actualTokens > maxAccounting - monthUsed then return 3 end
	end
end
for _, scope in ipairs(reservation.scopes) do
	local keys = scope.keys
	local retainedTokens = 0
	if commit == 1 then retainedTokens = actualTokens end
	if heldTokens > retainedTokens then
		decrementNonnegative(keys.tpm, heldTokens - retainedTokens)
	elseif retainedTokens > heldTokens and keys.tpm ~= "" and redis.call("EXISTS", keys.tpm) == 1 then
		redis.call("INCRBY", keys.tpm, retainedTokens - heldTokens)
	end
	decrementNonnegative(keys.active, 1)
	decrementNonnegative(keys.spend_held, heldSpend)
	decrementNonnegative(keys.day_held, heldTokens)
	decrementNonnegative(keys.month_held, heldTokens)
	redis.call("ZREM", keys.lease_set, requestID)
	redis.call("HDEL", keys.lease_data, requestID)
		refreshLeaseExpiry(keys.lease_set, keys.lease_data, bucketGraceMs)
		if commit == 1 then
			if keys.spend_used ~= "" and actualSpend > 0 then
				redis.call("INCRBY", keys.spend_used, actualSpend)
				local expiry = tonumber(scope.spend_used_expires_at_ms or 0)
				if expiry > 0 then
					if expiry <= nowMs then expiry = nowMs + bucketGraceMs end
					redis.call("PEXPIREAT", keys.spend_used, expiry)
				end
			end
			if keys.day_used ~= "" and actualTokens > 0 then
				redis.call("INCRBY", keys.day_used, actualTokens)
				local expiry = tonumber(scope.day_used_expires_at_ms or 0)
				if expiry <= nowMs then expiry = nowMs + bucketGraceMs end
				redis.call("PEXPIREAT", keys.day_used, expiry)
			end
			if keys.month_used ~= "" and actualTokens > 0 then
				redis.call("INCRBY", keys.month_used, actualTokens)
				local expiry = tonumber(scope.month_used_expires_at_ms or 0)
				if expiry <= nowMs then expiry = nowMs + bucketGraceMs end
				redis.call("PEXPIREAT", keys.month_used, expiry)
			end
		end
end
redis.call("ZADD", settledSetKey, nowMs + tombstoneTtlMs, requestID)
redis.call("PEXPIRE", settledSetKey, tombstoneTtlMs)
redis.call("DEL", ticketKey)
return 1
`)
	redisQuotaPruneScript = redis.NewScript(`
local function decrementNonnegative(key, amount)
	if not key or key == "" or amount <= 0 then return end
	local value = redis.call("INCRBY", key, -amount)
	if value <= 0 then redis.call("DEL", key) end
end
local leaseSetKey = KEYS[1]
local leaseDataKey = KEYS[2]
local redisTime = redis.call("TIME")
local nowMs = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
local expired = redis.call("ZRANGEBYSCORE", leaseSetKey, "-inf", nowMs)
for _, requestID in ipairs(expired) do
	local raw = redis.call("HGET", leaseDataKey, requestID)
	if raw then
		local lease = cjson.decode(raw)
		decrementNonnegative(lease.active, 1)
		decrementNonnegative(lease.tpm, tonumber(lease.tokens) or 0)
		decrementNonnegative(lease.spend_held, tonumber(lease.spend) or 0)
		decrementNonnegative(lease.day_held, tonumber(lease.tokens) or 0)
		decrementNonnegative(lease.month_held, tonumber(lease.tokens) or 0)
		redis.call("HDEL", leaseDataKey, requestID)
	end
end
if #expired > 0 then redis.call("ZREMRANGEBYSCORE", leaseSetKey, "-inf", nowMs) end
if redis.call("ZCARD", leaseSetKey) == 0 then redis.call("DEL", leaseSetKey, leaseDataKey) end
return #expired
`)
	redisCounterAddScript = redis.NewScript(`
local key = KEYS[1]
local delta = tonumber(ARGV[1])
local ttlMs = tonumber(ARGV[2])
local value = tonumber(redis.call("GET", key) or "0") + delta
if value < 0 then value = 0 end
redis.call("SET", key, value, "PX", ttlMs)
return value
`)
	redisCapabilityProbeScript = redis.NewScript(`
local redisTime = redis.call("TIME")
local nowMs = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
redis.call("PING")
redis.call("SET", KEYS[1], "1", "PX", 10000)
redis.call("EXISTS", KEYS[1])
redis.call("GET", KEYS[1])
redis.call("INCRBY", KEYS[1], 1)
redis.call("PTTL", KEYS[1])
redis.call("PEXPIREAT", KEYS[1], nowMs + 10000)
redis.call("HSET", KEYS[2], "field", "value")
redis.call("HGET", KEYS[2], "field")
redis.call("HDEL", KEYS[2], "field")
redis.call("HSET", KEYS[2], "field", "value")
redis.call("PEXPIRE", KEYS[2], 10000)
redis.call("ZADD", KEYS[3], 1, "expired", 2, "live", 3, "removed")
redis.call("ZSCORE", KEYS[3], "live")
redis.call("ZCARD", KEYS[3])
redis.call("ZRANGEBYSCORE", KEYS[3], "-inf", "+inf")
redis.call("ZREVRANGE", KEYS[3], 0, 0, "WITHSCORES")
redis.call("ZREM", KEYS[3], "removed")
redis.call("ZREMRANGEBYSCORE", KEYS[3], "-inf", 1)
redis.call("PEXPIRE", KEYS[3], 10000)
redis.call("DEL", KEYS[1], KEYS[2], KEYS[3])
return tonumber(redisTime[1])
`)
)

const (
	maximumRedisTimeout         = 30 * time.Second
	maximumRedisRetryBackoff    = 5 * time.Second
	maximumRedisRetries         = 10
	defaultRedisPoolSize        = 64
	defaultRedisMaxActiveConns  = 128
	maximumRedisPoolConnections = 4096
)

type RedisStore struct {
	client      *redis.Client
	namespace   string
	limiter     *redis_rate.Limiter
	limiterOnce sync.Once
	closeOnce   sync.Once
	closeErr    error
}

var (
	_ State               = (*RedisStore)(nil)
	_ OrderedBreakerState = (*RedisStore)(nil)
)

func NewRedisStore(addr, passwordEnv string, db int) *RedisStore {
	return NewRedisStoreWithNamespace(addr, passwordEnv, db, "")
}

func NewRedisStoreWithNamespace(addr, passwordEnv string, db int, namespace string) *RedisStore {
	return newRedisStore(&redis.Options{
		Addr:           addr,
		Password:       os.Getenv(passwordEnv),
		DB:             db,
		PoolSize:       defaultRedisPoolSize,
		MaxIdleConns:   defaultRedisPoolSize,
		MaxActiveConns: defaultRedisMaxActiveConns,
	}, namespace)
}

// NewRedisStoreFromURL supports redis:// and rediss:// URLs, including ACL
// usernames, passwords, database numbers, and TLS options understood by
// go-redis. Production callers should load the URL from an environment-backed
// configuration field rather than checking credentials into YAML.
func NewRedisStoreFromURL(rawURL, namespace string) (*RedisStore, error) {
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, safeRedisURLParseError(err)
	}
	if err := normalizeAndValidateRedisClientOptions(options); err != nil {
		return nil, err
	}
	return newRedisStore(options, namespace), nil
}

func safeRedisURLParseError(err error) error {
	var parsed *url.Error
	if errors.As(err, &parsed) && parsed.Err != nil {
		return fmt.Errorf("parse Redis URL: %v", parsed.Err)
	}
	return fmt.Errorf("parse Redis URL: %v", err)
}

func newRedisStore(options *redis.Options, namespace string) *RedisStore {
	options.ContextTimeoutEnabled = true
	return &RedisStore{client: redis.NewClient(options), namespace: strings.TrimSpace(namespace)}
}

func normalizeAndValidateRedisClientOptions(options *redis.Options) error {
	if options == nil {
		return fmt.Errorf("redis options are missing")
	}
	if options.DB < 0 {
		return fmt.Errorf("redis database must be greater than or equal to zero")
	}
	if options.Protocol != 0 && options.Protocol != 2 && options.Protocol != 3 {
		return fmt.Errorf("redis protocol must be 2 or 3")
	}
	if options.MaxRetries < -1 || options.MaxRetries > maximumRedisRetries {
		return fmt.Errorf("redis max_retries must be between -1 and %d", maximumRedisRetries)
	}
	for _, field := range []struct {
		name  string
		value time.Duration
	}{
		{name: "dial_timeout", value: options.DialTimeout},
		{name: "read_timeout", value: options.ReadTimeout},
		{name: "write_timeout", value: options.WriteTimeout},
		{name: "pool_timeout", value: options.PoolTimeout},
	} {
		name, value := field.name, field.value
		if value < 0 {
			return fmt.Errorf("redis %s must not disable timeouts", name)
		}
		if value > maximumRedisTimeout {
			return fmt.Errorf("redis %s must not exceed %s", name, maximumRedisTimeout)
		}
	}
	for _, field := range []struct {
		name  string
		value time.Duration
	}{
		{name: "min_retry_backoff", value: options.MinRetryBackoff},
		{name: "max_retry_backoff", value: options.MaxRetryBackoff},
	} {
		if field.value < -1 {
			return fmt.Errorf("redis %s must be -1 or greater", field.name)
		}
		if field.value > maximumRedisRetryBackoff {
			return fmt.Errorf("redis %s must not exceed %s", field.name, maximumRedisRetryBackoff)
		}
	}
	for _, field := range []struct {
		name  string
		value int
	}{
		{name: "pool_size", value: options.PoolSize},
		{name: "min_idle_conns", value: options.MinIdleConns},
		{name: "max_idle_conns", value: options.MaxIdleConns},
		{name: "max_active_conns", value: options.MaxActiveConns},
	} {
		if field.value < 0 {
			return fmt.Errorf("redis %s must be greater than or equal to zero", field.name)
		}
		if field.value > maximumRedisPoolConnections {
			return fmt.Errorf("redis %s must not exceed %d", field.name, maximumRedisPoolConnections)
		}
	}
	if options.PoolSize == 0 {
		options.PoolSize = defaultRedisPoolSize
	}
	if options.MaxIdleConns == 0 {
		options.MaxIdleConns = options.PoolSize
	}
	if options.MaxActiveConns == 0 {
		options.MaxActiveConns = options.PoolSize * 2
		if options.MaxActiveConns < defaultRedisMaxActiveConns {
			options.MaxActiveConns = defaultRedisMaxActiveConns
		}
		if options.MaxActiveConns > maximumRedisPoolConnections {
			options.MaxActiveConns = maximumRedisPoolConnections
		}
	}
	if options.MinIdleConns > options.PoolSize {
		return fmt.Errorf("redis min_idle_conns must not exceed pool_size")
	}
	if options.MaxIdleConns > options.PoolSize {
		return fmt.Errorf("redis max_idle_conns must not exceed pool_size")
	}
	if options.MinIdleConns > options.MaxIdleConns {
		return fmt.Errorf("redis min_idle_conns must not exceed max_idle_conns")
	}
	if options.PoolSize > options.MaxActiveConns {
		return fmt.Errorf("redis pool_size must not exceed max_active_conns")
	}
	if options.MinIdleConns > options.MaxActiveConns {
		return fmt.Errorf("redis min_idle_conns must not exceed max_active_conns")
	}
	if options.MaxIdleConns > options.MaxActiveConns {
		return fmt.Errorf("redis max_idle_conns must not exceed max_active_conns")
	}
	effectiveMinBackoff := options.MinRetryBackoff
	if effectiveMinBackoff == 0 {
		effectiveMinBackoff = 8 * time.Millisecond
	} else if effectiveMinBackoff == -1 {
		effectiveMinBackoff = 0
	}
	effectiveMaxBackoff := options.MaxRetryBackoff
	if effectiveMaxBackoff == 0 {
		effectiveMaxBackoff = 512 * time.Millisecond
	} else if effectiveMaxBackoff == -1 {
		effectiveMaxBackoff = 0
	}
	if effectiveMinBackoff > 0 && effectiveMaxBackoff > 0 && effectiveMinBackoff > effectiveMaxBackoff {
		return fmt.Errorf("redis min_retry_backoff must not exceed max_retry_backoff")
	}
	return nil
}

func (s *RedisStore) Ping(ctx context.Context) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis client is not configured")
	}
	return s.client.Ping(ctx).Err()
}

func (s *RedisStore) ValidateStartup(ctx context.Context) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis client is not configured")
	}
	prefix := s.key("startup-probe:" + randomToken())
	keys := []string{prefix + ":string", prefix + ":hash", prefix + ":zset"}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.client.Del(cleanupCtx, keys...).Err()
	}()
	if _, err := redisCapabilityProbeScript.Run(ctx, s.client, keys).Result(); err != nil {
		return fmt.Errorf("redis requires a writable standalone server and the full command set used by gateway scripts: %w", err)
	}
	return nil
}

func (s *RedisStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.closeErr = s.client.Close()
	})
	return s.closeErr
}

func (s *RedisStore) initLimiter() {
	s.limiterOnce.Do(func() {
		s.limiter = redis_rate.NewLimiter(s.client)
	})
}

func (s *RedisStore) Allow(ctx context.Context, key string, limit RateLimit, n int64) error {
	if limit.Rate <= 0 || n <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.initLimiter()
	result, err := s.limiter.AllowN(ctx, s.key(key), redis_rate.Limit{
		Rate:   int(limit.Rate),
		Burst:  burstSize(limit),
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

func (s *RedisStore) AllowBatch(ctx context.Context, requests []RateRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	active, err := normalizeRateRequests(requests)
	if err != nil {
		return err
	}
	if len(active) == 0 {
		return nil
	}
	if len(active) == 1 {
		return s.Allow(ctx, active[0].Key, active[0].Limit, active[0].N)
	}
	keys := make([]string, 0, len(active))
	args := make([]any, 0, len(active)*4)
	for _, request := range active {
		period := normalizeRatePeriod(request.Limit.Period)
		keys = append(keys, "rate:"+s.key(request.Key))
		args = append(args, burstSize(request.Limit), request.Limit.Rate, period.Seconds(), request.N)
	}
	result, err := redisRateBatchScript.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return err
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return fmt.Errorf("unexpected redis rate batch result %T", result)
	}
	allowed, ok := values[0].(int64)
	if !ok {
		return fmt.Errorf("unexpected redis rate batch status %T", values[0])
	}
	if allowed == 1 {
		return nil
	}
	failed, ok := values[1].(int64)
	if !ok || failed < 1 || failed > int64(len(active)) {
		return gateway.RateLimited("rate limit exceeded")
	}
	return &BatchRateLimitError{Key: active[failed-1].Key}
}

func (s *RedisStore) Add(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	return redisCounterAddScript.Run(ctx, s.client, []string{s.key(key)}, delta, normalizeScriptTTL(ttl.Milliseconds())).Int64()
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
	}, token).Result()
	return err
}

func (s *RedisStore) BreakerAllowAttempt(ctx context.Context, route string, retention time.Duration) (bool, time.Time, error) {
	if route == "" {
		return true, time.Now(), nil
	}
	result, err := redisBreakerAllowScript.Run(ctx, s.client, []string{s.breakerKey(route)}, normalizeScriptTTL(retention.Milliseconds())).Result()
	if err != nil {
		return false, time.Time{}, err
	}
	items, ok := result.([]interface{})
	if !ok || len(items) != 2 {
		return false, time.Time{}, fmt.Errorf("unexpected redis breaker allow result %T", result)
	}
	allowed, err := toInt64(items[0])
	if err != nil {
		return false, time.Time{}, err
	}
	startedAtMicros, err := toInt64(items[1])
	if err != nil {
		return false, time.Time{}, err
	}
	return allowed == 1, time.UnixMicro(startedAtMicros), nil
}

func (s *RedisStore) BreakerFailAttempt(ctx context.Context, route string, startedAt time.Time, threshold int, cooldown time.Duration, _ string) error {
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
	_, err := redisBreakerFailScript.Run(ctx, s.client, []string{s.breakerKey(route)}, startedAt.UnixMicro(), threshold, cooldown.Milliseconds(), ttl.Milliseconds()).Result()
	return err
}

func (s *RedisStore) BreakerSuccessAttempt(ctx context.Context, route string, startedAt time.Time) error {
	if route == "" {
		return nil
	}
	ttl := breakerStateTTL(time.Minute)
	_, err := redisBreakerSuccessScript.Run(ctx, s.client, []string{s.breakerKey(route)}, startedAt.UnixMicro(), ttl.Milliseconds()).Result()
	return err
}

// BreakerAllow preserves the original State contract. Attempt policy uses
// BreakerAllowAttempt when the store advertises OrderedBreakerState.
func (s *RedisStore) BreakerAllow(ctx context.Context, route string, _ time.Time) (bool, error) {
	allowed, _, err := s.BreakerAllowAttempt(ctx, route, time.Hour)
	return allowed, err
}

// BreakerFail preserves the original State contract without persisting the
// supplied failure text. The timestamp is completion time for legacy callers.
func (s *RedisStore) BreakerFail(ctx context.Context, route string, threshold int, cooldown time.Duration, _ string, now time.Time) error {
	return s.BreakerFailAttempt(ctx, route, now, threshold, cooldown, "provider_failure")
}

// BreakerSuccess preserves the original State contract. Ordered callers use
// the admission timestamp through BreakerSuccessAttempt instead.
func (s *RedisStore) BreakerSuccess(ctx context.Context, route string, now time.Time) error {
	return s.BreakerSuccessAttempt(ctx, route, now)
}

type redisReservation struct {
	Ticket      gateway.QuotaTicket     `json:"ticket"`
	Held        gateway.EstimatedUsage  `json:"held"`
	Scopes      []redisScopeReservation `json:"scopes"`
	Fingerprint string                  `json:"fingerprint"`
}

type redisScopeReservation struct {
	Ref                      gateway.ScopeRef  `json:"ref"`
	Limit                    gateway.LimitSpec `json:"limit"`
	Keys                     redisQuotaKeys    `json:"keys"`
	SpendUsedExpiresAtMillis int64             `json:"spend_used_expires_at_ms,omitempty"`
	DayUsedExpiresAtMillis   int64             `json:"day_used_expires_at_ms,omitempty"`
	MonthUsedExpiresAtMillis int64             `json:"month_used_expires_at_ms,omitempty"`
}

type redisReserveScriptInput struct {
	requestID   string
	fingerprint string
	scopes      []memoryScopeReservation
	estimate    gateway.EstimatedUsage
	ttl         time.Duration
	now         time.Time
	payload     []byte
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
	LeaseSet  string `json:"lease_set"`
	LeaseData string `json:"lease_data"`
}

func (s *RedisStore) Reserve(ctx context.Context, requestID string, scopes []gateway.ScopedLimit, estimate gateway.EstimatedUsage, ttl time.Duration) (gateway.QuotaTicket, error) {
	if requestID == "" {
		return gateway.QuotaTicket{}, nil
	}
	now, err := s.redisNow(ctx)
	if err != nil {
		return gateway.QuotaTicket{}, err
	}
	ttl = normalizeReservationTTL(ttl)
	estimate, err = validateRedisEstimatedUsage(estimate)
	if err != nil {
		return gateway.QuotaTicket{}, err
	}
	resolved := resolveScopeReservations(scopes, now)
	ticket := gateway.QuotaTicket{RequestID: requestID, Scopes: scopedRefs(resolved)}
	fingerprint, err := quotaReservationFingerprint(scopes, estimate)
	if err != nil {
		return gateway.QuotaTicket{}, err
	}
	payload, err := json.Marshal(redisReservation{
		Ticket:      ticket,
		Held:        estimate,
		Scopes:      s.toRedisScopes(resolved),
		Fingerprint: fingerprint,
	})
	if err != nil {
		return gateway.QuotaTicket{}, err
	}
	keys, args := reserveScriptInput(redisReserveScriptInput{
		requestID:   requestID,
		fingerprint: fingerprint,
		scopes:      resolved,
		estimate:    estimate,
		ttl:         ttl,
		now:         now,
		payload:     payload,
	})
	for i := range keys {
		keys[i] = s.key(keys[i])
	}
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
	case 3:
		return gateway.QuotaTicket{}, ErrQuotaReservationConflict
	case 4:
		return gateway.QuotaTicket{}, ErrQuotaAccountingCapacity
	default:
		return gateway.QuotaTicket{}, gateway.RateLimited("quota exceeded: " + quotaReasonLabel(reason))
	}
}

func (s *RedisStore) TopUp(ctx context.Context, ticket gateway.QuotaTicket, scopes []gateway.ScopedLimit, delta gateway.EstimatedUsage, ttl time.Duration) error {
	if ticket.RequestID == "" {
		return nil
	}
	var err error
	delta, err = validateRedisEstimatedUsage(delta)
	if err != nil {
		return err
	}
	_ = scopes // limits and accounting keys are fixed when the reservation is created.
	ttl = normalizeReservationTTL(ttl)
	result, err := redisQuotaTopUpScript.Run(ctx, s.client, []string{s.ticketKey(ticket.RequestID)},
		ticket.RequestID,
		delta.TotalTokens(),
		delta.EstimatedSpendMicros,
		normalizeScriptTTL(ttl.Milliseconds()),
		gateway.MaximumQuotaValue,
	).Result()
	if err != nil {
		return err
	}
	status, reason, err := parseScriptTuple(result)
	if err != nil {
		return err
	}
	if status == 0 {
		if reason == "missing" {
			return ErrQuotaReservationNotFound
		}
		if reason == "accounting_capacity" {
			return ErrQuotaAccountingCapacity
		}
		return gateway.RateLimited("quota exceeded: " + quotaReasonLabel(reason))
	}
	return nil
}

func (s *RedisStore) Commit(ctx context.Context, ticket gateway.QuotaTicket, actual gateway.ActualUsage) error {
	if ticket.RequestID == "" {
		return nil
	}
	var err error
	actual, err = validateRedisActualUsage(actual)
	if err != nil {
		return err
	}
	return s.settle(ctx, ticket, true, actual)
}

func validateRedisEstimatedUsage(usage gateway.EstimatedUsage) (gateway.EstimatedUsage, error) {
	usage = normalizeEstimatedUsage(usage)
	if usage.InputTokens > gateway.MaximumQuotaValue ||
		usage.ReservedOutputTokens > gateway.MaximumQuotaValue-usage.InputTokens ||
		usage.EstimatedSpendMicros > gateway.MaximumQuotaValue {
		return gateway.EstimatedUsage{}, ErrQuotaAccountingCapacity
	}
	return usage, nil
}

func validateRedisActualUsage(usage gateway.ActualUsage) (gateway.ActualUsage, error) {
	usage = normalizeActualUsage(usage)
	if usage.InputTokens > gateway.MaximumQuotaValue ||
		usage.OutputTokens > gateway.MaximumQuotaValue-usage.InputTokens ||
		usage.SpendMicros > gateway.MaximumQuotaValue {
		return gateway.ActualUsage{}, ErrQuotaAccountingCapacity
	}
	return usage, nil
}

func (s *RedisStore) Refund(ctx context.Context, ticket gateway.QuotaTicket) error {
	return s.settle(ctx, ticket, false, gateway.ActualUsage{})
}

func (s *RedisStore) settle(ctx context.Context, ticket gateway.QuotaTicket, commit bool, actual gateway.ActualUsage) error {
	if ticket.RequestID == "" {
		return nil
	}
	commitValue := 0
	if commit {
		commitValue = 1
	}
	result, err := redisQuotaSettleScript.Run(ctx, s.client, []string{s.ticketKey(ticket.RequestID), s.settledSetKey()},
		ticket.RequestID,
		commitValue,
		actual.TotalTokens(),
		actual.SpendMicros,
		normalizeScriptTTL(quotaSettlementReplayWindow.Milliseconds()),
		normalizeScriptTTL(quotaBucketExpiryGrace.Milliseconds()),
		gateway.MaximumQuotaValue,
	).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		return ErrQuotaReservationNotFound
	}
	if result == 3 {
		return ErrQuotaAccountingCapacity
	}
	return nil
}

func (s *RedisStore) attemptSlotSetKey(bucket string) string {
	return s.key("attempt:" + bucket + ":leases")
}

func (s *RedisStore) breakerKey(route string) string {
	return s.key("breaker:route:" + route)
}

func (s *RedisStore) ticketKey(requestID string) string {
	return s.key("quota:ticket:" + requestID)
}

func (s *RedisStore) settledSetKey() string {
	return s.key("quota:settled:recent")
}

func (s *RedisStore) key(key string) string {
	if key == "" || s == nil || s.namespace == "" {
		return key
	}
	return s.namespace + ":" + key
}

func (s *RedisStore) redisNow(ctx context.Context) (time.Time, error) {
	if s == nil || s.client == nil {
		return time.Time{}, fmt.Errorf("redis client is not configured")
	}
	now, err := s.client.Time(ctx).Result()
	if err != nil {
		return time.Time{}, fmt.Errorf("read Redis time: %w", err)
	}
	return now, nil
}

func reserveScriptInput(input redisReserveScriptInput) ([]string, []interface{}) {
	keys := make([]string, 0, len(input.scopes)*11+2)
	args := make([]interface{}, 0, 10+len(input.scopes)*6)
	args = append(args,
		int64(len(input.scopes)),
		input.estimate.TotalTokens(),
		input.estimate.EstimatedSpendMicros,
		normalizeScriptTTL(input.ttl.Milliseconds()),
		normalizeScriptTTL(timeUntilNextMinute(input.now).Milliseconds()),
		string(input.payload),
		input.requestID,
		input.fingerprint,
		input.now.UnixMilli(),
		gateway.MaximumQuotaValue,
	)
	for _, scope := range input.scopes {
		leaseSet, leaseData := redisLeaseKeys(scope.ref)
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
			leaseSet,
			leaseData,
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
	keys = append(keys, "quota:ticket:"+input.requestID, "quota:settled:recent")
	return keys, args
}

func quotaReservationFingerprint(scopes []gateway.ScopedLimit, estimate gateway.EstimatedUsage) (string, error) {
	payload, err := json.Marshal(struct {
		Scopes   []gateway.ScopedLimit  `json:"scopes"`
		Estimate gateway.EstimatedUsage `json:"estimate"`
	}{Scopes: scopes, Estimate: estimate})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
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
	ttl := time.Duration(math.MaxInt64)
	if cooldown <= time.Duration(math.MaxInt64)/3 {
		ttl = cooldown * 3
	}
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

func (s *RedisStore) toRedisScopes(scopes []memoryScopeReservation) []redisScopeReservation {
	out := make([]redisScopeReservation, 0, len(scopes))
	for _, scope := range scopes {
		leaseSet, leaseData := redisLeaseKeys(scope.ref)
		out = append(out, redisScopeReservation{
			Ref:                      scope.ref,
			Limit:                    scope.limit,
			SpendUsedExpiresAtMillis: unixMilliOrZero(scope.expires.spendUsed),
			DayUsedExpiresAtMillis:   unixMilliOrZero(scope.expires.dayUsed),
			MonthUsedExpiresAtMillis: unixMilliOrZero(scope.expires.monthUsed),
			Keys: redisQuotaKeys{
				Active:    s.key(scope.keys.active),
				RPM:       s.key(scope.keys.rpm),
				TPM:       s.key(scope.keys.tpm),
				SpendUsed: s.key(scope.keys.spendUsed),
				SpendHeld: s.key(scope.keys.spendHeld),
				DayUsed:   s.key(scope.keys.dayUsed),
				DayHeld:   s.key(scope.keys.dayHeld),
				MonthUsed: s.key(scope.keys.monthUsed),
				MonthHeld: s.key(scope.keys.monthHeld),
				LeaseSet:  s.key(leaseSet),
				LeaseData: s.key(leaseData),
			},
		})
	}
	return out
}

func unixMilliOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixMilli()
}

func redisLeaseKeys(ref gateway.ScopeRef) (string, string) {
	prefix := "quota:" + string(ref.Kind) + ":" + ref.ID
	return prefix + ":leases", prefix + ":lease_data"
}

func normalizeRatePeriod(period time.Duration) time.Duration {
	if period <= 0 {
		return time.Minute
	}
	return period
}
