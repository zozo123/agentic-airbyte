#!/usr/bin/env bash
#
# run_e2e.sh — the self-contained ETL proof a sandbox worker executes.
#
# Source:      a local ClickHouse server (system of record)
# Destination: a DuckDB warehouse file
# Engine:      worker/etl.py  (Airbyte source->destination record-stream contract)
#
# It is identical whether run on a laptop or inside a borrowed islo sandbox:
# bootstrap deps -> boot ClickHouse -> seed real data -> move it -> verify ->
# emit reports/metrics.json. Exit code 0 only if every parity check passes.
#
set -euo pipefail
cd "$(dirname "$0")"

ROWS="${ETL_ROWS:-50000}"
CH_PORT="${CH_PORT:-8123}"
RUNDIR="$(pwd)/.run"
mkdir -p "$RUNDIR" reports

log() { printf '\033[1;36m[e2e]\033[0m %s\n' "$*"; }

# ---- 1. bootstrap deps (no-op if already present) --------------------------
echo "::CRABBOX_PHASE::bootstrap"
if ! command -v clickhouse >/dev/null 2>&1 && [ ! -x ./clickhouse ]; then
  log "installing clickhouse static binary"
  curl -fsSL https://clickhouse.com/ | sh >/dev/null 2>&1
fi
CH_BIN="$(command -v clickhouse || echo ./clickhouse)"

if [ ! -d .venv ]; then
  log "creating venv + installing duckdb/requests"
  python3 -m venv .venv
  ./.venv/bin/pip -q install --upgrade pip >/dev/null
  ./.venv/bin/pip -q install duckdb requests >/dev/null
fi
PY=./.venv/bin/python

# ---- 2. boot ClickHouse server ---------------------------------------------
echo "::CRABBOX_PHASE::boot_clickhouse"
log "starting ClickHouse server on :$CH_PORT"
mkdir -p "$RUNDIR/ch"
"$CH_BIN" server -- \
  --path="$RUNDIR/ch" \
  --http_port="$CH_PORT" --tcp_port=9111 --mysql_port=0 \
  --listen_host=127.0.0.1 \
  >"$RUNDIR/clickhouse.log" 2>&1 &
CH_PID=$!
trap 'kill $CH_PID 2>/dev/null || true' EXIT

for i in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$CH_PORT/?query=SELECT%201" >/dev/null 2>&1; then
    log "ClickHouse up: $(curl -fsS "http://127.0.0.1:$CH_PORT/?query=SELECT%20version()")"
    break
  fi
  sleep 1
  [ "$i" = 60 ] && { log "ClickHouse failed to start"; tail -20 "$RUNDIR/clickhouse.log"; exit 1; }
done

# ---- 3. seed source --------------------------------------------------------
echo "::CRABBOX_PHASE::seed"
log "seeding source with $ROWS events"
$PY worker/seed.py --host 127.0.0.1 --port "$CH_PORT" --rows "$ROWS"

# ---- 4. move + verify ------------------------------------------------------
log "running ETL worker (ClickHouse -> DuckDB)"
$PY worker/etl.py

log "done. evidence in reports/metrics.json + reports/warehouse.duckdb"
