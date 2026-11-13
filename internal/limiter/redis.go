package limiter

import (
	"context"
	"crypto/rand"
	_ "embed" // enables the //go:embed directives below
	"encoding/hex"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// The Lua sources are compiled into the binary with go:embed, so there is no
// file to ship next to the executable.
var (
	//go:embed lua/token_bucket.lua
	tokenBucketLua string
	//go:embed lua/fixed_window.lua
	fixedWindowLua string
	//go:embed lua/sliding_log.lua
	slidingLogLua string
	//go:embed lua/sliding_counter.lua
	slidingCounterLua string
)

// redis.NewScript computes the script's SHA1 once. Run() then sends EVALSHA
// (just the hash, not the source) and falls back to EVAL only if the server
// hasn't cached the script yet.
var scripts = map[Algorithm]*redis.Script{
	TokenBucket:          redis.NewScript(tokenBucketLua),
	FixedWindow:          redis.NewScript(fixedWindowLua),
	SlidingWindowLog:     redis.NewScript(slidingLogLua),
	SlidingWindowCounter: redis.NewScript(slidingCounterLua),
}

// RedisOptions configures a Redis-backed limiter.
type RedisOptions struct {
	// Prefix namespaces keys, e.g. "rg:api:token_bucket:".
	Prefix string
	// Timeout bounds each Redis round trip. A slow Redis must not stall the
	// gateway; on timeout Allow returns an error and the caller's failure
	// policy (fail open / closed / local fallback) decides.
	Timeout time.Duration
}

// RedisLimiter enforces a Rule using state stored in Redis.
//
// Why Lua: each algorithm is a read-modify-write ("read count, compare to
// limit, write count+1"). Done as separate commands, two gateway instances
// could both read count=99 for limit=100 and both allow. Redis runs a Lua
// script atomically - no other command interleaves - so the check and update
// happen as one step, and every instance sharing the Redis sees one limit.
//
// Clock: scripts read the time with Redis TIME rather than taking "now" from
// the gateway. All instances therefore use one clock (the Redis primary's),
// so skew between gateway hosts cannot make them disagree about windows.
type RedisLimiter struct {
	client redis.Scripter
	script *redis.Script
	alg    Algorithm
	rule   Rule
	opts   RedisOptions

	// Sliding-log entries need unique ZSET members: instanceID + counter.
	instanceID string
	seq        atomic.Uint64
}

// NewRedis builds a Redis-backed limiter. client can be *redis.Client,
// *redis.ClusterClient or anything else implementing redis.Scripter.
func NewRedis(client redis.Scripter, alg Algorithm, rule Rule, opts RedisOptions) (*RedisLimiter, error) {
	if err := rule.Validate(); err != nil {
		return nil, err
	}
	script, ok := scripts[alg]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownAlgorithm, alg)
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 50 * time.Millisecond
	}
	return &RedisLimiter{
		client:     client,
		script:     script,
		alg:        alg,
		rule:       rule,
		opts:       opts,
		instanceID: randomID(),
	}, nil
}

// Allow runs the algorithm's Lua script for key.
func (l *RedisLimiter) Allow(ctx context.Context, key string) (Decision, error) {
	ctx, cancel := context.WithTimeout(ctx, l.opts.Timeout)
	defer cancel()

	// {braces} are a Redis Cluster "hash tag": only the part inside is hashed
	// to choose the shard, so all of one client's state lands on one node.
	redisKey := l.opts.Prefix + "{" + key + "}"
	windowUs := l.rule.Window.Microseconds()

	var args []any
	switch l.alg {
	case TokenBucket:
		args = []any{l.rule.Limit, windowUs, l.rule.Capacity()}
	case SlidingWindowLog:
		member := l.instanceID + "-" + strconv.FormatUint(l.seq.Add(1), 10)
		args = []any{l.rule.Limit, windowUs, member}
	default:
		args = []any{l.rule.Limit, windowUs}
	}

	res, err := l.script.Run(ctx, l.client, []string{redisKey}, args...).Int64Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("redis %s: %w", l.alg, err)
	}
	if len(res) != 4 {
		return Decision{}, fmt.Errorf("redis %s: unexpected reply length %d", l.alg, len(res))
	}
	limit := l.rule.Limit
	if l.alg == TokenBucket {
		limit = l.rule.Capacity()
	}
	return Decision{
		Allowed:    res[0] == 1,
		Limit:      limit,
		Remaining:  max(0, res[1]),
		ResetAfter: time.Duration(res[2]) * time.Microsecond,
		RetryAfter: time.Duration(res[3]) * time.Microsecond,
	}, nil
}

func randomID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b) // crypto/rand.Read does not fail on supported platforms
	return hex.EncodeToString(b)
}
