-- Sliding window log, executed atomically inside Redis.
-- KEYS[1]  sorted set: member = unique request id, score = time (microseconds)
-- ARGV[1]  limit, ARGV[2] window in microseconds, ARGV[3] unique request id
-- Returns {allowed (0/1), remaining, reset_after_us, retry_after_us}

local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

-- Drop entries that slid out of (now - window, now].
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', string.format('%d', now - window))
local count = redis.call('ZCARD', KEYS[1])

local allowed, retry = 0, 0
if count < limit then
  -- The member must be unique, otherwise two requests in the same
  -- microsecond would collapse into one ZSET entry.
  redis.call('ZADD', KEYS[1], string.format('%d', now), ARGV[3])
  count = count + 1
  allowed = 1
else
  local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  retry = tonumber(oldest[2]) + window - now
end

local newest = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
local reset = tonumber(newest[2]) + window - now
redis.call('PEXPIRE', KEYS[1], math.ceil(window / 1000) + 1)
return {allowed, limit - count, reset, retry}
