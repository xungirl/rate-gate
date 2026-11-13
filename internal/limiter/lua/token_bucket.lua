-- Token bucket, executed atomically inside Redis.
-- KEYS[1]  hash with fields: tokens, ts (microseconds)
-- ARGV[1]  limit (tokens added per window)
-- ARGV[2]  window in microseconds
-- ARGV[3]  capacity (burst)
-- Returns {allowed (0/1), remaining, reset_after_us, retry_after_us}

local limit    = tonumber(ARGV[1])
local window   = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local rate     = limit / window -- tokens per microsecond

-- Use Redis server time so every gateway instance agrees on "now".
local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil then -- new key: full bucket
  tokens = capacity
  ts = now
end

local elapsed = now - ts
if elapsed > 0 then
  tokens = math.min(capacity, tokens + elapsed * rate)
  ts = now
end

local allowed, retry = 0, 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.ceil((1 - tokens) / rate)
end

local reset = math.ceil((capacity - tokens) / rate)
-- string.format keeps full precision (default number->string can use %.14g).
redis.call('HSET', KEYS[1], 'tokens', string.format('%.17g', tokens), 'ts', string.format('%d', ts))
-- A full bucket is equivalent to a missing key, so let Redis expire it then.
redis.call('PEXPIRE', KEYS[1], math.ceil(reset / 1000) + 1)
return {allowed, math.floor(tokens), reset, retry}
