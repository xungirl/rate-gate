-- Sliding window counter, executed atomically inside Redis.
-- KEYS[1]  hash with fields: window (index), curr, prev
-- ARGV[1]  limit, ARGV[2] window in microseconds
-- Returns {allowed (0/1), remaining, reset_after_us, retry_after_us}
-- The math mirrors slidingCounterDecide in memory.go.

local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local idx = math.floor(now / window)
local elapsed = now - idx * window

local state = redis.call('HMGET', KEYS[1], 'window', 'curr', 'prev')
local stored = tonumber(state[1])
local curr = tonumber(state[2]) or 0
local prev = tonumber(state[3]) or 0
if stored == idx then
  -- same window
elseif stored == idx - 1 then
  prev = curr
  curr = 0
else
  prev = 0
  curr = 0
end

local estimate = prev * (window - elapsed) / window + curr
local allowed, retry = 0, 0
if estimate + 1 <= limit then
  allowed = 1
  curr = curr + 1
  estimate = estimate + 1
elseif curr + 1 > limit then
  retry = (window - elapsed) + math.max(0, window * (1 - (limit - 1) / curr))
else
  retry = math.max(1, (window - elapsed) - (limit - curr - 1) * window / prev)
end

local remaining = math.max(0, math.floor(limit - estimate))
local reset = window - elapsed
if curr > 0 then
  reset = 2 * window - elapsed
end

redis.call('HSET', KEYS[1], 'window', string.format('%d', idx), 'curr', curr, 'prev', prev)
redis.call('PEXPIRE', KEYS[1], math.ceil((2 * window - elapsed) / 1000) + 1)
return {allowed, remaining, math.ceil(reset), math.ceil(retry)}
