#!/usr/bin/env bash
# Run-to-run variance check: repeats the c=64 throughput runs N times.
#   scripts/repeat.sh        (N=3, 10s each; needs redis-server on PATH)
set -euo pipefail
cd "$(dirname "$0")/.."
N=${N:-3}; DURATION=${DURATION:-10s}; WARMUP=${WARMUP:-2s}; REDIS_PORT=6390; OUT=results
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
  redis-server --port $REDIS_PORT --save '' --appendonly no --daemonize yes --logfile "$PWD/$OUT/redis.log" >/dev/null
  STARTED_REDIS=1; sleep 0.5
fi
start bin/upstream -listen :9001
start bin/gateway -config configs/bench-memory.yaml
start bin/gateway -config configs/bench-redis.yaml
sleep 1
for i in $(seq 1 "$N"); do
  for target in direct:http://127.0.0.1:9001/api/x proxy-only:http://127.0.0.1:8080/plain/x \
                memory:http://127.0.0.1:8080/api/x redis:http://127.0.0.1:8081/api/x; do
    name=${target%%:*}; url=${target#*:}
    bin/loadgen -targets "$url" -c 64 -d "$DURATION" -warmup "$WARMUP" -keys 1000 -json > "$OUT/repeat-$name-c64-run$i.json"
    echo "run $i $name: $(grep -E '"throughput_rps"' "$OUT/repeat-$name-c64-run$i.json")"
  done
done
