#!/usr/bin/env bash
# Local stack in WSL/Linux: redis on :6390 + mock exchange on :8090.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-$HOME/pq-venv/bin/python}
RUN=${RUN:-/tmp/pq-run}
mkdir -p "$RUN"
case "${1:-up}" in
  up)
    redis-cli -p 6390 ping >/dev/null 2>&1 || redis-server --port 6390 --save "" --appendonly no --daemonize yes --logfile "$RUN/redis.log"
    if ! curl -s localhost:8090/position >/dev/null; then
      setsid nohup "$PY" -m pyquant.exchange --port 8090 --latency-ms "${LAT_MS:-0}" >"$RUN/mockex.log" 2>&1 < /dev/null & disown
      for _ in $(seq 50); do curl -s localhost:8090/position >/dev/null && break; sleep 0.1; done
    fi
    echo "stack up";;
  down)
    redis-cli -p 6390 shutdown nosave 2>/dev/null || true
    pkill -f "pyquant.exchange" || true;;
esac
