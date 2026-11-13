# rate-gate

A distributed rate-limiting API gateway in Go. It sits in front of your HTTP
services as a reverse proxy, identifies each caller (API key, user ID or IP),
and enforces per-route and per-key limits with one of four algorithms. Limits
are kept either in process memory or in Redis. With Redis, any number of
gateway instances enforce **one** shared limit, because every decision is made
by an atomic Lua script.

```
go test -race ./...                      # all tests (miniredis, no setup needed)
go run ./cmd/upstream &                  # demo backend on :9001
go run ./cmd/gateway -config configs/example.yaml
curl -i -H 'X-API-Key: demo' localhost:8080/api/hello
```

Measured on one Apple M2 laptop (8 cores, everything on localhost; details,
commands and caveats in [BENCHMARK.md](BENCHMARK.md)):

- 3 gateway instances sharing one Redis, offered 6000 req/s against a
  1000 req/s limit for 10 s, allowed exactly 10,000 requests with the token
  bucket, fixed window and sliding log (9,990 with the sliding counter). The
  in-memory backend, as a contrast, allowed ~3x.
- At 64 connections the in-memory limiter was within run-to-run noise of the
  bare proxy (42,939 vs. 43,366 req/s, p50 0.942 vs. 0.922 ms); the Redis
  backend ran at 24,396 req/s with p50 2.323 ms; direct-to-upstream was
  118,978 req/s.
- One in-memory decision takes ~40-150 ns with 0 allocations (`go test -bench`).

## Architecture

```
                 clients
                    |
        +-----------+-----------+            any number of identical
        v           v           v            gateway processes
  +-----------+ +-----------+ +-----------+
  | gateway 1 | | gateway 2 | | gateway 3 |
  +-----------+ +-----------+ +-----------+
        |  1. match route (longest path prefix)
        |  2. key = api_key | user_id | header | client IP
        |  3. limiter.Allow(ctx, key) ---------------------------+
        |  4a. denied  -> 429 + Retry-After + X-RateLimit-*      |
        |  4b. allowed -> httputil.ReverseProxy -> upstream      |
        v                                                        v
  +-----------+                         +--------------------------------------+
  | upstreams |                         | Redis: one Lua script per decision   |
  +-----------+                         |  - TIME (one clock for all gateways) |
                                        |  - read state, decide, write, EXPIRE |
                                        +--------------------------------------+

Inside one gateway, per route:

  Limiter (interface)
    |-- memory:  token bucket | fixed window | sliding log | sliding counter
    |            on a 64-shard map (mutex per shard) + TTL cleanup goroutine
    '-- Resilient (decorator, only for the redis backend)
          |-- primary : RedisLimiter (EVALSHA of the algorithm's Lua script)
          |-- breaker : closed -> open after N consecutive errors -> half-open probe
          '-- policy  : fail_open | fail_closed (503) | local_fallback (memory limiter)

  /metrics  Prometheus     /healthz  JSON status incl. breaker state
  SIGHUP    reload routes  SIGINT/SIGTERM  graceful shutdown
```

### Code map

| Path | What it is |
|---|---|
| `internal/clock` | `Clock` interface + `Fake` clock so algorithm tests never sleep |
| `internal/limiter/limiter.go` | `Limiter` interface, `Rule`, `Decision` |
| `internal/limiter/store.go` | generic sharded map with expiry (in-memory state) |
| `internal/limiter/memory.go` | the four algorithms, in memory |
| `internal/limiter/lua/*.lua`, `redis.go` | the four algorithms as atomic Redis scripts |
| `internal/breaker` | circuit breaker |
| `internal/limiter/resilient.go` | breaker + fail-open / fail-closed / local fallback |
| `internal/config` | YAML config, defaults, validation |
| `internal/gateway` | routing, key extraction, headers, proxy, hot reload |
| `internal/metrics` | Prometheus metrics |
| `cmd/gateway`, `cmd/upstream`, `cmd/loadgen` | binaries: gateway, demo backend, load generator |
| `scripts/bench.sh` | reproduces every number in BENCHMARK.md |

## Algorithms

All four implement the same interface and exist in both backends:

```go
type Limiter interface {
    Allow(ctx context.Context, key string) (Decision, error)
}
```

| Algorithm | State per key | Accuracy | Burst behaviour | Redis type |
|---|---|---|---|---|
| Token bucket | 2 numbers (tokens, last refill) | Exact for its model (rate + bucket size) | Allows up to `burst` at once, then the steady refill rate | HASH |
| Fixed window | 1 counter + window id | Up to **2x the limit** in a short span across a window boundary | 2x at boundaries (the known flaw) | HASH |
| Sliding window log | 1 timestamp per allowed request (up to `limit`) | Exact: never more than `limit` in any window | None beyond the limit | ZSET |
| Sliding window counter | 2 counters + window id | Approximate: assumes the previous window's requests were evenly spread | Smooths the boundary burst | HASH |

The boundary flaw is demonstrated by tests, not just claimed:
`TestBoundaryBurst` sends 10 requests at t=0.9 s and 10 at t=1.0 s against
a 10/s limit. Fixed window allows all 20; sliding log and sliding counter
allow 10; token bucket allows 11 (10 burst + 1 token refilled in 100 ms).
`TestSlidingLogNeverExceedsLimitInAnyWindow` checks every 1 s span over a
5 s run: the fixed window reaches 100 in one span (limit 50), the sliding log
never exceeds 50.

**Choosing:** token bucket for APIs that should tolerate short bursts (the
default); sliding window log for small, strict limits such as login attempts
(memory is O(limit) per key); sliding window counter for large limits where
O(1) memory matters and a small approximation is fine; fixed window only as
a baseline.

## Design decisions and trade-offs

**Atomic Lua scripts instead of GET-then-SET.** Each algorithm is a
read-modify-write. Done as separate commands, two gateways could both read
"99 of 100 used" and both allow, so the limit is exceeded under concurrency.
Redis runs a script to completion before any other command, so check and
update are one step. `TestConcurrentNeverExceedsLimit` (100 goroutines x 20
requests, limit 100, exactly 100 allowed) and `TestMultiInstanceSharedRedis`
(3 gateway instances, 300 concurrent requests, exactly 50 allowed for a limit
of 50) run against miniredis and a real Redis. Scripts are sent with
`EVALSHA` (hash only); go-redis falls back to `EVAL` if Redis lost its script
cache.

**Redis server time, not gateway time.** Scripts call `redis.call('TIME')`.
All gateways then read one clock (the Redis primary that owns the key), so
clock skew between gateway hosts cannot make them disagree about which window
it is or how many tokens have refilled. Costs: the time is only as good as
the Redis host's clock, and a failover to a replica with a different clock
can shift windows by that difference. Calling `TIME` before writes is allowed
because Redis 5+ replicates script *effects*, not the script itself, and
Redis 7+ does only that. In memory, time comes from an injected `Clock`, which
is what makes the algorithm tests deterministic.

**One key per client, hash-tagged.** Every algorithm keeps its state in a
single Redis key (`rg:<route>:<alg>:{<client>}`), with `PEXPIRE` so idle
clients disappear by themselves. The `{...}` hash tag keeps a client's state
on one shard if you move to Redis Cluster, and since each script touches one
declared key it is Cluster-safe. The algorithm is part of the key so changing
algorithms on reload never misreads old state.

**Sharded in-memory store.** A single mutex would serialize every request in
the process. The store has 64 shards (`maphash` of the key picks one), each
with its own mutex and map, so different clients rarely contend. A
background goroutine sweeps expired keys shard by shard, so memory is bounded
by *active* clients.

**Failure policy is explicit and per deployment.** When Redis errors or times
out (default budget 50 ms per call):
- `fail_open`: allow. Availability first; right when limits protect fairness.
- `fail_closed`: 503 with `Retry-After`. Protection first; right for paid
  quotas or fragile upstreams.
- `local_fallback` (default): use an in-memory limiter with the same rule. Each
  instance enforces the limit alone, so N instances allow up to N x the limit:
  degraded but bounded.

A circuit breaker (5 consecutive errors -> open for 2 s -> one half-open probe)
stops every request from waiting for a dead Redis. Tests check that after the
threshold Redis is not called again until the cool-down ends, and that a
cancelled client request does not count as a Redis failure. Degraded
responses carry `X-RateLimit-Degraded: true`, are counted in
`rategate_degraded_decisions_total`, and `/healthz` reports `degraded`.

**HTTP semantics.** Allowed and rejected responses carry `X-RateLimit-Limit`,
`X-RateLimit-Remaining` and `X-RateLimit-Reset` (seconds until the full
allowance is back: a delta like the IETF RateLimit draft, so it does not
depend on the client's clock). Rejections are `429` with `Retry-After` in
whole seconds, rounded up so a client that waits that long is not early, and
a JSON body. Rejected requests are never forwarded.

**Hot reload without locks on the request path.** A config produces an
immutable route table. `SIGHUP` builds a new table and swaps an
`atomic.Pointer`; requests in flight keep the old table. Backend changes
(memory vs. Redis, address) are rejected and need a restart. In-memory state
starts fresh after a reload; Redis state survives.

**Proxy tuning that mattered.** Go's default transport keeps only 2 idle
connections per upstream host; the gateway uses 1024. `ReverseProxy` without
a `BufferPool` allocates a 32 KiB copy buffer per request; adding a
`sync.Pool`-backed pool raised the proxy-only route from about 30.6k to 53.0k
req/s in a quick 3 s run at 64 connections (see BENCHMARK.md, "Experiments").

## Configuration

See [`configs/example.yaml`](configs/example.yaml) (every option is
commented). Example route:

```yaml
routes:
  - name: api
    path_prefix: /api/
    upstream: http://127.0.0.1:9001
    rate_limit:
      algorithm: token_bucket     # token_bucket | fixed_window | sliding_window_log | sliding_window_counter
      key_by: api_key             # ip | api_key (X-API-Key) | user_id (X-User-ID) | header:<Name>
      limit: 10                   # per window; refill rate for the token bucket
      window: 1s
      burst: 20                   # token bucket capacity
      overrides:
        - { key: premium-key-123, limit: 100, burst: 200 }
```

A request without the identifying header is limited by client IP. Unknown
YAML fields are errors (a typo like `limt:` won't be silently ignored).

## Running

Requires Go 1.23+. Redis is optional (only for `backend.type: redis`).

```bash
go build -o bin/ ./cmd/...

bin/upstream -listen :9001 &
bin/gateway -config configs/example.yaml           # memory backend

redis-server --port 6390 --daemonize yes
bin/gateway -config configs/bench-redis.yaml       # redis backend on :8081
bin/gateway -config configs/accuracy-redis.yaml -listen :8092   # more instances

kill -HUP  <pid>   # reload routes
kill -TERM <pid>   # graceful shutdown (waits for in-flight requests, 10s max)
```

Tests:

```bash
go test -race ./...                                         # uses miniredis
RATEGATE_REDIS_ADDR=127.0.0.1:6390 go test -race ./...      # also real Redis
RATEGATE_REDIS_ADDR=127.0.0.1:6390 go test -race -count=10 ./...   # flakiness check
scripts/bench.sh                                            # all benchmarks
```

### Metrics

| Metric | Labels |
|---|---|
| `rategate_requests_total` | `route`, `decision` = allowed / limited / unavailable / no_route |
| `rategate_limiter_duration_seconds` (histogram) | `route`, `backend` |
| `rategate_redis_errors_total` | |
| `rategate_degraded_decisions_total` | `mode` |
| `rategate_breaker_state` | 0 closed, 1 open, 2 half-open |
| `rategate_upstream_errors_total` | `route` |
| `rategate_config_reloads_total` | `result` |

Plus Go runtime and process metrics. Logs are JSON (`log/slog`); per-request
access logs are off by default (`log.access: true`) because they are
expensive at high request rates.

## Limitations

- Benchmarks ran on one laptop: load generator, gateway, upstream and Redis
  share 8 cores over localhost. Absolute numbers would differ on separate
  machines with real network latency; the relative overhead is the useful part.
- One Redis instance is a single point of contention and failure for shared
  limits; the failure policy bounds the damage but does not remove it. Redis
  Cluster should work (single-key, hash-tagged scripts) but was not tested.
- A very hot key (one client at very high rates) serializes on one Redis
  shard. Mitigations not implemented here: local pre-aggregation (each
  gateway takes a batch of tokens at a time) or splitting the limit across
  sub-keys.
- `local_fallback` allows up to N x the limit with N instances. It does not
  divide the limit by the instance count, because the gateways don't know N.
- The sliding window counter is an approximation, and its `Retry-After` is
  computed from the same approximation.
- No TLS termination, auth, retries, or load balancing across multiple
  upstream hosts per route; this project focuses on rate limiting.
- `trust_forwarded_for` takes the right-most `X-Forwarded-For` entry, which is
  correct behind exactly one trusted proxy; deeper proxy chains would need a
  configurable trusted-hop count.
