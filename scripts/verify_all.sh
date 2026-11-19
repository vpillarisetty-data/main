#!/usr/bin/env bash
# Verifies every documented use case: lint, all tests (Spark ones if pyspark is installed),
# a clean non-editable install of all 7 packages, and every CLI with its EXPECTED exit code.
#   scripts/verify_all.sh                 # uses `python` on PATH
#   PY=/path/to/python scripts/verify_all.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PY:-python}"
cd "$ROOT"
pass=0; fail=0
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
expect() {   # expect <exit-code> <description> <command...>
  local want=$1 desc=$2; shift 2
  set +e; out="$("$@" 2>&1)"; got=$?; set -e
  if [ "$got" = "$want" ]; then pass=$((pass+1)); printf '  ok   %-58s (exit %s)\n' "$desc" "$got"
  else fail=$((fail+1)); printf '  FAIL %-58s (exit %s, expected %s)\n%s\n' "$desc" "$got" "$want" "$out"; fi
}

step "lint"
expect 0 "ruff check" "$PY" -m ruff check projects scenarios conftest.py

step "tests (unit + Spark/Delta integration + industry scenarios)"
expect 0 "pytest" "$PY" -m pytest -q -p no:cacheprovider

step "clean install of every package into a fresh virtualenv"
VENV="$(mktemp -d)/venv"
"$PY" -m venv "$VENV" && "$VENV/bin/python" -m pip install -q --upgrade pip >/dev/null
expect 0 "pip install ./projects/*" "$VENV/bin/python" -m pip install -q ./projects/*
B="$VENV/bin"; WS="$ROOT"; cd /tmp

step "every CLI, run from outside the repository"
expect 0 "import all 7 packages" "$B/python" -c "import medallion, streamguard, contractkit, deltadoctor, piiguard, catalog_agent, migrationkit"
expect 0 "medallion-run --validate-only (banking)" "$B/medallion-run" "$WS/projects/medallion-framework/conf/pipelines/card_transactions.yaml" --validate-only
expect 0 "medallion-run --validate-only (telecom)" "$B/medallion-run" "$WS/scenarios/telecom/cdr_pipeline.yaml" --validate-only
expect 0 "medallion-run --validate-only (hospitality)" "$B/medallion-run" "$WS/scenarios/hospitality/reservations_pipeline.yaml" --validate-only
expect 1 "medallion-run without pyspark explains how to install" "$B/medallion-run" "$WS/projects/medallion-framework/conf/pipelines/card_transactions.yaml"
expect 1 "contractkit diff: banking breaking change blocked" "$B/contractkit" diff "$WS/projects/data-contracts/contracts/customer_rewards.v1.yaml" "$WS/projects/data-contracts/contracts/customer_rewards.v2-proposed.yaml"
expect 1 "contractkit diff: telecom breaking change blocked" "$B/contractkit" diff "$WS/scenarios/telecom/contracts/network_usage_daily.v1.yaml" "$WS/scenarios/telecom/contracts/network_usage_daily.v2-proposed.yaml"
expect 1 "contractkit diff: hospitality breaking change blocked" "$B/contractkit" diff "$WS/scenarios/hospitality/contracts/property_daily_kpis.v1.yaml" "$WS/scenarios/hospitality/contracts/property_daily_kpis.v2-proposed.yaml"
expect 0 "contractkit diff: identical contract passes" "$B/contractkit" diff "$WS/projects/data-contracts/contracts/customer_rewards.v1.yaml" "$WS/projects/data-contracts/contracts/customer_rewards.v1.yaml"
expect 0 "contractkit ddl" "$B/contractkit" ddl "$WS/scenarios/hospitality/contracts/reservation.v1.yaml"
expect 0 "contractkit expectations" "$B/contractkit" expectations "$WS/scenarios/hospitality/contracts/reservation.v1.yaml"
expect 0 "piiguard: banking customer sample -> UC policies" "$B/piiguard" "$WS/projects/pii-guardian/examples/customer_sample.jsonl" --table fin.silver.customers
expect 0 "piiguard --json" "$B/piiguard" "$WS/projects/pii-guardian/examples/customer_sample.jsonl" --table t --json
expect 0 "migrationkit convert-ddl oracle (banking)" "$B/migrationkit" convert-ddl --from oracle "$WS/projects/migration-validator/examples/oracle_schema.sql"
expect 0 "migrationkit convert-ddl hive (telecom)" "$B/migrationkit" convert-ddl --from hive "$WS/scenarios/telecom/hive_cdr.sql"
expect 0 "migrationkit convert-ddl sqlserver (hospitality)" "$B/migrationkit" convert-ddl --from sqlserver "$WS/scenarios/hospitality/sqlserver_reservations.sql"
expect 0 "migrationkit demo" "$B/migrationkit" demo
expect 0 "catalog-agent-demo" "$B/catalog-agent-demo"
expect 0 "catalog-agent-evals (bundled)" "$B/catalog-agent-evals"
expect 0 "catalog-agent-evals telecom catalog + suite" "$B/catalog-agent-evals" "$WS/scenarios/telecom/catalog.yaml" --suite "$WS/scenarios/telecom/evals.yaml"
expect 0 "catalog-agent-evals hospitality catalog + suite" "$B/catalog-agent-evals" "$WS/scenarios/hospitality/catalog.yaml" --suite "$WS/scenarios/hospitality/evals.yaml"
expect 2 "catalog-agent-evals custom catalog without suite is refused" "$B/catalog-agent-evals" "$WS/scenarios/telecom/catalog.yaml"
expect 0 "deltadoctor --help" "$B/deltadoctor" --help

# migrationkit reconcile + deltadoctor on real files
TMP="$(mktemp -d)"
"$B/python" - "$TMP" <<'PYEOF'
import json, os, sqlite3, sys
d = sys.argv[1]
for name, rows in (("src", [(f"R{i}", i * 1.5) for i in range(100)]), ("ok", [(f"R{i}", i * 1.5) for i in range(100)]),
                   ("bad", [(f"R{i}", i * 1.5) for i in range(99)])):
    c = sqlite3.connect(f"{d}/{name}.db"); c.execute("CREATE TABLE t (id TEXT, amount REAL)")
    c.executemany("INSERT INTO t VALUES (?,?)", rows); c.commit()
log = f"{d}/table/_delta_log"; os.makedirs(log)
with open(f"{log}/{0:020d}.json", "w") as fh:
    fh.write(json.dumps({"metaData": {"id": "x", "partitionColumns": [], "configuration": {},
                         "schemaString": json.dumps({"type": "struct", "fields": []})}}) + "\n")
    for i in range(200):
        fh.write(json.dumps({"add": {"path": f"f{i}.parquet", "size": 2048, "partitionValues": {},
                                     "modificationTime": 0, "dataChange": True}}) + "\n")
PYEOF
expect 0 "migrationkit reconcile: matching tables PASS" "$B/migrationkit" reconcile --source-db "$TMP/src.db" --target-db "$TMP/ok.db" --table t --keys id --numeric amount
expect 1 "migrationkit reconcile: missing row FAILs (cutover gate)" "$B/migrationkit" reconcile --source-db "$TMP/src.db" --target-db "$TMP/bad.db" --table t --keys id --numeric amount
expect 0 "deltadoctor report" "$B/deltadoctor" "$TMP/table"
expect 1 "deltadoctor --fail-under 90 gates a small-file table" "$B/deltadoctor" "$TMP/table" --fail-under 90

printf '\n\033[1m%s passed, %s failed\033[0m\n' "$pass" "$fail"
[ "$fail" = 0 ]
