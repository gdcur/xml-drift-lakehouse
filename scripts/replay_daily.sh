#!/usr/bin/env bash
# replay_daily.sh - replay data/daily into the Iceberg tables, one day at a time (ADR-0001).
#
# Each day folder is parsed to a temporary Parquet batch, then loaded with
# iceberg_writer.py --load, and the tables are checked after every day.
#
# Usage:
#   bash scripts/replay_daily.sh [DAILY_DIR] [WAREHOUSE]
#   bash scripts/replay_daily.sh data/daily output/iceberg
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${1:-$ROOT/data/daily}"
WH="${2:-$ROOT/output/iceberg}"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

for day in $(find "$SRC" -mindepth 1 -maxdepth 1 -type d | sort); do
  name="$(basename "$day")"
  python "$ROOT/ingestion/parser.py" --src "$day" --root "$SRC" --dst "$STAGE/$name" > /dev/null
  echo "== $name"
  python "$ROOT/ingestion/iceberg_writer.py" --warehouse "$WH" --load --src "$STAGE/$name" --check \
    | grep -E "received|consistency"
done
