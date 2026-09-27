#!/usr/bin/env bash
set -u
cd /home/user
while true; do
  n=$(find data/raw/comtrade_batched -name '*.json' | wc -l)
  echo "[$(date -u +%FT%TZ)] saved=$n/1144"
  if [ "$n" -ge 1144 ]; then
    echo "All raw downloads are present. Running final assembly."
    exec python -u code/download_comtrade_brics_batched.py
  fi
  python -u code/download_comtrade_brics_batched.py || true
  sleep 10
done
