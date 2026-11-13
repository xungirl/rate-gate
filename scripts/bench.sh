#!/usr/bin/env bash
# Reproduces every number in BENCHMARK.md.
#
#   scripts/bench.sh            # everything (needs redis-server on PATH)
#   DURATION=5s scripts/bench.sh
#
# Starts: upstream (:9001), Redis (:6390, unless already running),
# gateway-memory (:8080), gateway-redis (:8081), 3 accuracy gateways
# (:8091-8093). Everything is stopped on exit. Raw output goes to results/.
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${DURATION:-10s}
WARMUP=${WARMUP:-2s}
REDIS_PORT=6390
OUT=results
mkdir -p "$OUT" bin

go build -o bin/ ./cmd/...

PIDS=()
cleanup() {
  for p in ${PIDS[@]+"${PIDS[@]}"}; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [[ "${STARTED_REDIS:-0}" == 1 ]]; then redis-cli -p $REDIS_PORT shutdown nosave >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

start() { "$@" >/dev/null 2>&1 & PIDS+=($!); }

if ! redis-cli -p $REDIS_PORT ping >/dev/null 2>&1; then
  redis-server --port $REDIS_PORT --save '' --appendonly no --daemonize yes \
    --logfile "$PWD/$OUT/redis.log" >/dev/null
  STARTED_REDIS=1
  sleep 0.5
fi
redis-cli -p $REDIS_PORT flushall >/dev/null

start bin/upstream -listen :9001
start bin/gateway -config configs/bench-memory.yaml
start bin/gateway -config configs/bench-redis.yaml
sleep 1

{
  echo "date: $(date)"
  echo "go: $(go version)"
  echo "os: $(sw_vers -productName 2>/dev/null) $(sw_vers -productVersion 2>/dev/null) $(uname -m)"
  echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null), $(sysctl -n hw.ncpu 2>/dev/null) cores"
  echo "redis: $(redis-server --version)"
} > "$OUT/env.txt"
cat "$OUT/env.txt"

# 1) Throughput/latency: direct to upstream vs gateway without limiter
#    (proxy-only) vs gateway + in-memory limiter vs gateway + Redis limiter.
for c in 16 64 256; do
  for target in direct:http://127.0.0.1:9001/api/x proxy-only:http://127.0.0.1:8080/plain/x \
                memory:http://127.0.0.1:8080/api/x redis:http://127.0.0.1:8081/api/x; do
    name=${target%%:*}; url=${target#*:}
    echo "== throughput $name c=$c"
    bin/loadgen -targets "$url" -c "$c" -d "$DURATION" -warmup "$WARMUP" -keys 1000 -json \
      | tee "$OUT/throughput-$name-c$c.json" | grep -E '"(throughput_rps|p50_ms|p99_ms|errors)"'
  done
done

# 2) Accuracy: 3 gateway instances share Redis; one API key offered 6000 req/s
#    against a limit of 1000 req/s. Repeat with the memory backend as contrast.
for backend in redis memory; do
  ACC=()
  for port in 8091 8092 8093; do
    start bin/gateway -config "configs/accuracy-$backend.yaml" -listen ":$port"
    ACC+=($!)
  done
  sleep 1
  for alg in tb fw swl swc; do
    echo "== accuracy $backend $alg"
    bin/loadgen -targets "http://127.0.0.1:8091/$alg/,http://127.0.0.1:8092/$alg/,http://127.0.0.1:8093/$alg/" \
      -rate 6000 -c 128 -d "$DURATION" -warmup "$WARMUP" -key "acc-$alg" -series -json \
      | tee "$OUT/accuracy-$backend-$alg.json" | grep -E '"(allowed_rps|throughput_rps|errors)"'
  done
  # stop the three accuracy gateways before the next backend
  kill "${ACC[@]}"; wait "${ACC[@]}" 2>/dev/null || true
  sleep 1
done

# 3) Go micro-benchmarks (memory and real Redis).
echo "== go test -bench"
RATEGATE_REDIS_ADDR=127.0.0.1:$REDIS_PORT go test -run '^$' -bench . -benchmem -benchtime 2s -count 1 \
  ./internal/limiter/ | tee "$OUT/microbench.txt"
