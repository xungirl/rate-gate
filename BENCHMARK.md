# Benchmarks

Every number below comes from runs on the machine described here, on
2026-11-12. Raw output for each run is in [`results/`](results/) (one JSON
file per loadgen run, plus `microbench.txt` and `env.txt`). Nothing is
estimated. Derived values (overhead ratios, ops/s from ns/op) are plain
arithmetic on those measurements and are marked as derived.

## Environment

```
date:  Thu Nov 12 17:35:34 PST 2026   (results/env.txt)
go:    go1.23.2 darwin/arm64
os:    macOS 15.4 arm64
cpu:   Apple M2, 8 cores, 24 GB RAM
redis: Redis server v=8.10.3 (Homebrew), port 6390, persistence off
```

**Read these numbers with the setup in mind.** The load generator, the
gateway, the upstream and Redis all ran on **one laptop**, sharing the same 8
cores, over **localhost** (no real network latency). The load generator itself
uses a lot of the CPU. Absolute throughput would be different on separate
machines; what these runs show well is *relative* cost: direct vs. proxy vs.
proxy + limiter.

## How to reproduce

```bash
scripts/bench.sh     # sections 1-3 below; starts/stops everything itself
scripts/repeat.sh    # section 1b: c=64 runs repeated 3 times
```

`scripts/bench.sh` builds the binaries and starts:

| Process | Command |
|---|---|
| upstream (returns a 12-byte JSON body) | `bin/upstream -listen :9001` |
| gateway, memory backend, :8080 | `bin/gateway -config configs/bench-memory.yaml` |
| gateway, Redis backend, :8081 | `bin/gateway -config configs/bench-redis.yaml` |
| Redis | `redis-server --port 6390 --save '' --appendonly no --daemonize yes` |

The throughput configs use a token bucket with `limit: 1000000000` per second,
so **no request is ever rejected**: every request pays the full limiter cost
and is then proxied. Requests rotate over 1000 API keys (`-keys 1000`). The
`/plain/` route on :8080 is the same proxy with no limiter, to separate the
cost of the proxy hop from the cost of the limiter.

## 1. Throughput and latency: direct vs. gateway

Closed loop: `-c` connections each send requests back to back for 10 s after a
2 s warm-up. Command per cell (target URL varies):

```bash
bin/loadgen -targets <url> -c <16|64|256> -d 10s -warmup 2s -keys 1000 -json
#  direct      http://127.0.0.1:9001/api/x
#  proxy-only  http://127.0.0.1:8080/plain/x   (gateway, no limiter)
#  memory      http://127.0.0.1:8080/api/x     (gateway + in-memory token bucket)
#  redis       http://127.0.0.1:8081/api/x     (gateway + Redis Lua token bucket)
```

| Connections | Target | Throughput (req/s) | p50 (ms) | p99 (ms) | Errors | Non-200 |
|---|---|---|---|---|---|---|
| 16 | direct | 105,969 | 0.104 | 0.756 | 0 | 0 |
| 16 | proxy-only | 43,408 | 0.269 | 1.628 | 0 | 0 |
| 16 | memory | 41,845 | 0.279 | 1.710 | 0 | 0 |
| 16 | redis | 23,744 | 0.581 | 1.768 | 0 | 0 |
| 64 | direct | 118,978 | 0.320 | 3.199 | 0 | 0 |
| 64 | proxy-only | 43,366 | 0.922 | 8.409 | 0 | 0 |
| 64 | memory | 42,939 | 0.942 | 8.170 | 0 | 0 |
| 64 | redis | 24,396 | 2.323 | 8.564 | 0 | 0 |
| 256 | direct | 114,650 | 1.552 | 12.320 | 0 | 0 |
| 256 | proxy-only | 50,217 | 3.649 | 28.207 | 0 | 0 |
| 256 | memory | 44,264 | 4.131 | 30.760 | 0 | 0 |
| 256 | redis | 23,266 | 9.982 | 33.886 | 0 | 0 |

Source: `results/throughput-<target>-c<N>.json`.

Added latency at p50, derived from the table (gateway minus direct):

| Connections | proxy-only | + memory limiter | + Redis limiter |
|---|---|---|---|
| 16 | +0.165 ms | +0.175 ms | +0.477 ms |
| 64 | +0.602 ms | +0.622 ms | +2.003 ms |
| 256 | +2.097 ms | +2.579 ms | +8.430 ms |

### 1b. Run-to-run variance (c = 64, 3 more runs)

`scripts/repeat.sh` ran the c=64 row three more times, a few minutes after
section 1 (same commands, `results/repeat-<target>-c64-run<i>.json`):

| Target | Run | Throughput (req/s) | p50 (ms) | p99 (ms) | Errors | Non-200 |
|---|---|---|---|---|---|---|
| direct | 1 | 114,627 | 0.321 | 3.585 | 0 | 0 |
| direct | 2 | 123,863 | 0.303 | 3.237 | 0 | 0 |
| direct | 3 | 127,328 | 0.316 | 2.768 | 0 | 0 |
| proxy-only | 1 | 51,913 | 0.820 | 6.251 | 0 | 0 |
| proxy-only | 2 | 51,992 | 0.840 | 6.117 | 0 | 0 |
| proxy-only | 3 | 52,755 | 0.839 | 5.868 | 0 | 0 |
| memory | 1 | 53,539 | 0.837 | 5.663 | 0 | 0 |
| memory | 2 | 49,197 | 0.877 | 6.604 | 0 | 0 |
| memory | 3 | 50,401 | 0.869 | 6.229 | 0 | 0 |
| redis | 1 | 28,929 | 2.007 | 6.318 | 0 | 0 |
| redis | 2 | 28,827 | 2.025 | 5.962 | 0 | 0 |
| redis | 3 | 30,729 | 1.946 | 5.379 | 0 | 0 |

These gateway runs came out roughly 15-26% faster than the same cells in
section 1 (for example memory 49,197-53,539 vs. 42,939 req/s). Same binary, same
commands; the laptop's state (thermal, background load) differed. So treat
any single number as ±20% on this machine. The *ordering* held in every run.

### What the throughput numbers say

- **The proxy hop dominates the overhead, not the limiter.** In every run the
  in-memory limiter sits within run-to-run noise of proxy-only (at c=64:
  42,939 vs. 43,366 req/s in section 1; 49,197-53,539 vs. 51,913-52,755 in 1b,
  where one memory run was even faster than every proxy-only run). The
  micro-benchmarks below put one in-memory decision at roughly 40-150 ns.
- **Going through the reverse proxy here costs about 2.2-2.7x in throughput**
  compared with hitting the upstream directly: proxying means a second HTTP
  exchange, and all of it competes with the load generator for the same 8 cores.
- **The Redis backend costs one localhost round trip per request:** p50 went
  from 0.942 to 2.323 ms at c=64 (section 1), and throughput dropped to
  about 53-61% of the memory backend in the same run (24,396 vs. 42,939 at
  c=64; 1b: 28,827-30,729 vs. 49,197-53,539). No errors and no non-200 responses in any run.

## 2. Limiter accuracy: 3 instances sharing one Redis

Three gateway processes (`-listen :8091/:8092/:8093`) run
`configs/accuracy-redis.yaml`: four routes, one per algorithm, each limited
to **1000 requests/s per API key**. The load generator offers **6000 req/s**
(open loop) with a single API key, round-robin across the three instances,
for 10 s after a 2 s warm-up (the warm-up also uses up the token bucket's
initial burst, so the measured window is steady state). The same run is then
repeated with `configs/accuracy-memory.yaml` (same routes, in-memory backend)
as a contrast.

```bash
bin/gateway -config configs/accuracy-redis.yaml -listen :8091   # and :8092, :8093
bin/loadgen -targets http://127.0.0.1:8091/<alg>/,http://127.0.0.1:8092/<alg>/,http://127.0.0.1:8093/<alg>/ \
  -rate 6000 -c 128 -d 10s -warmup 2s -key acc-<alg> -series -json
```

| Backend | Algorithm | Sent | 2xx | 429 | Observed allowed rate | Limit | 2xx in each 1 s bucket |
|---|---|---|---|---|---|---|---|
| Redis, 3 instances | token bucket | 59,994 | 10,000 | 49,994 | **1000.0/s** | 1000/s | 1000, 1003, 997, 1000, 1000, 1000, 1000, 1000, 1000, 1000 |
| Redis, 3 instances | fixed window | 59,994 | 10,000 | 49,994 | **1000.0/s** | 1000/s | 1000 x 10 |
| Redis, 3 instances | sliding window log | 59,994 | 10,000 | 49,994 | **1000.0/s** | 1000/s | 1000 x 10 |
| Redis, 3 instances | sliding window counter | 59,994 | 9,990 | 50,004 | **999.0/s** | 1000/s | 999 x 10 |
| memory, 3 instances | token bucket | 59,994 | 29,976 | 30,018 | 2997.5/s | 1000/s each | 2974-3008 |
| memory, 3 instances | fixed window | 59,994 | 30,000 | 29,994 | 2999.9/s | 1000/s each | 2987-3013 |
| memory, 3 instances | sliding window log | 59,994 | 30,000 | 29,994 | 2999.9/s | 1000/s each | 3000 x 10 |
| memory, 3 instances | sliding window counter | 59,994 | 29,834 | 30,160 | 2983.3/s | 1000/s each | 2963-2999 |

Source: `results/accuracy-<backend>-<alg>.json` (measured duration 10.000 s,
0 transport errors in every run). "Observed allowed rate" = 2xx / measured
duration.

- With shared Redis, three instances together enforced the limit to within
  0.1% (10,000 of 10,000 expected for three algorithms; 9,990 for the sliding
  window counter, which admits only while `estimate + 1 <= limit` and so
  stays just under).
- The token bucket's per-second counts (1003, 997) differ from 1000 only
  because tokens refill continuously while the buckets are fixed 1 s slices;
  the 10 s total is exactly 10,000.
- With in-memory state, each instance enforced 1000/s on its own, so the
  three together allowed about 3x the limit. This is also what
  `local_fallback` mode gives you when Redis is down.

## 3. Go micro-benchmarks

```bash
RATEGATE_REDIS_ADDR=127.0.0.1:6390 go test -run '^$' -bench . -benchmem -benchtime 2s -count 1 ./internal/limiter/
```

Rule: 100 requests/s per key (a realistic per-client limit; a huge limit
would make the sliding log store millions of timestamps). `parallel-*` uses
`b.RunParallel` (GOMAXPROCS = 8 goroutines; 16 x 8 = 128 for Redis, since each
call waits on the network). With `RunParallel`, ns/op is wall time divided by
total operations across all goroutines.

| Algorithm | Memory, serial, 1 key | Memory, parallel, 10k keys | Memory, parallel, 1 hot key | Redis (real), parallel, 10k keys |
|---|---|---|---|---|
| token bucket | 81.15 ns/op | 43.01 ns/op | 148.3 ns/op | 17,150 ns/op (1000 B, 23 allocs) |
| fixed window | 55.62 ns/op | 38.90 ns/op | 133.9 ns/op | 15,091 ns/op (968 B, 23 allocs) |
| sliding window log | 56.53 ns/op | 40.69 ns/op | 132.5 ns/op | 17,783 ns/op (1048 B, 26 allocs) |
| sliding window counter | 76.19 ns/op | 39.75 ns/op | 147.1 ns/op | 15,272 ns/op (968 B, 23 allocs) |

Source: `results/microbench.txt`. All in-memory benchmarks: 0 B/op, 0 allocs/op.

Derived: ~17,150 ns/op for the Redis token bucket is about 58,000 limiter
decisions per second from one process with 128 concurrent callers against one
local Redis; the fixed window (15,091 ns/op) is about 66,000/s.

Observations:
- One hot key across 8 goroutines (~130-150 ns/op) is about 3-4x slower than
  10k keys (~40 ns/op): all goroutines contend for one shard's mutex. Sharding
  helps only when keys differ.
- The in-memory parallel numbers don't scale linearly with 8 cores (40 ns/op
  parallel vs. 56-81 ns/op serial); I haven't profiled where the rest goes.
- The sliding log is cheap here because the log is short (at most 100 entries
  per key). Its cost grows with the limit; that is the trade-off in the README
  table.

## 4. Experiments

### ReverseProxy BufferPool

`httputil.ReverseProxy` allocates a new 32 KiB buffer per request unless it is
given a `BufferPool`. I measured before and after adding a `sync.Pool`-backed
pool (commit "Reuse proxy copy buffers via ReverseProxy.BufferPool"). These
were **quick 3 s runs** (1 s warm-up) at 64 connections against a temporary
config identical to `configs/bench-memory.yaml` but listening on :8085:

```bash
bin/loadgen -targets http://127.0.0.1:8085/plain/x -c 64 -d 3s -warmup 1s -keys 1000
bin/loadgen -targets http://127.0.0.1:8085/api/x   -c 64 -d 3s -warmup 1s -keys 1000
```

| Route | Before (req/s, p50 / p99 ms) | After (req/s, p50 / p99 ms) |
|---|---|---|
| proxy-only (`/plain/`) | 30,647 (1.664 / 7.666) | 52,969 (0.846 / 5.835) |
| with limiter (`/api/`) | 29,568 (1.705 / 8.264) | 49,717 (0.880 / 6.519) |

These were single short runs (raw output not saved), so take the size of
the gain loosely, given the ±20% variance above. The change was large enough
to keep.

## 5. Correctness tests (for reference)

```bash
RATEGATE_REDIS_ADDR=127.0.0.1:6390 go test -race -count=10 ./...
```

passed on this machine (all packages `ok`; real-Redis tests included). See
the README for what the tests cover.
