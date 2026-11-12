#!/usr/bin/env bash
# Regenerate everything in results/ (Linux/WSL). ~6 min. Needs scripts/stack.sh up + data.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-$HOME/pq-venv/bin/python}
FT=${FT:-$HOME/pq-venv-ft/bin/python}
CSV=${CSV:-data/BTCUSDT-aggTrades-2026-10-15.csv}
mkdir -p results
"$PY" -m bench.hotpath --csv "$CSV"
"$PY" -m bench.concurrency && "$FT" -m bench.concurrency
"$PY" -m bench.dataproc --csv "$CSV"
"$PY" -m bench.e2e --csv "$CSV" --n 20000
"$PY" -m pyquant.md replay --csv "$CSV" --limit 200000 --fresh
"$PY" -m bench.chaos --kills 30
