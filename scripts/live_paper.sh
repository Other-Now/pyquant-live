#!/usr/bin/env bash
# Live paper run: real Binance public trades -> Redis (db 1) -> trader -> mock exchange :8091.
# usage: scripts/live_paper.sh [seconds]   (needs scripts/stack.sh up for redis)
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-$HOME/pq-venv/bin/python}
SECS=${1:-600}
R=redis://127.0.0.1:6390/1
OUT=results/live_paper.txt
redis-cli -p 6390 -n 1 flushdb >/dev/null
rm -f /tmp/pq-live.db*
setsid nohup "$PY" -m pyquant.exchange --port 8091 > /tmp/pq-live-ex.log 2>&1 < /dev/null & EX=$!
sleep 1
"$PY" -m pyquant.trader --redis $R --exchange http://127.0.0.1:8091 --db /tmp/pq-live.db --band-check \
  --metrics-port 9108 --uvloop --exit-when-idle --block-ms 15000 > /tmp/pq-live-trader.out 2>&1 &
TR=$!
sleep 1
"$PY" -m pyquant.md live --redis $R --seconds "$SECS" --uvloop > /tmp/pq-live-md.out 2>&1
curl -s localhost:9108/metrics | grep -E '^pq_' > /tmp/pq-live-metrics.txt || true
wait $TR || true
kill $EX || true
{ echo "# live paper run $(date -u +%FT%TZ), ${SECS}s, BTCUSDT public trade stream"
  echo "## md";      cat /tmp/pq-live-md.out
  echo "## trader";  cat /tmp/pq-live-trader.out
  echo "## metrics (scraped at end)"; cat /tmp/pq-live-metrics.txt; } > $OUT
cat $OUT
