-- Fixed window counter, executed atomically inside Redis.
-- KEYS[1]  hash with fields: window (index), count
-- ARGV[1]  limit, ARGV[2] window in microseconds
-- Returns {allowed (0/1), remaining, reset_after_us, retry_after_us}

local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local idx = math.floor(now / window)
local reset = (idx + 1) * window - now

local state = redis.call('HMGET', KEYS[1], 'window', 'count')
local count = tonumber(state[2]) or 0
if tonumber(state[1]) ~= idx then -- a new window started
  count = 0
end

local allowed, retry = 0, 0
if count < limit then
  count = count + 1
  allowed = 1
else
  retry = reset
end

redis.call('HSET', KEYS[1], 'window', string.format('%d', idx), 'count', count)
redis.call('PEXPIRE', KEYS[1], math.ceil(reset / 1000) + 1)
return {allowed, limit - count, reset, retry}
