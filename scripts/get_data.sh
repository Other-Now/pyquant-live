#!/usr/bin/env bash
# One real day of Binance spot BTCUSDT aggregated trades (1,106,978 rows, 16 MB zip).
set -euo pipefail
cd "$(dirname "$0")/../data"
f=BTCUSDT-aggTrades-2026-10-15
[ -f $f.csv ] || { curl -sSfO https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/$f.zip && unzip -oq $f.zip; }
wc -l $f.csv
