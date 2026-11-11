#!/usr/bin/env bash
# Trigger an ingest DAG and check it schedules exactly one dbt_build run that
# succeeds (the Phase 2 asset-trigger contract).
#
#   scripts/check_dbt_trigger.sh ingest_zillow
#
# Exits 0 only if: the ingest run succeeds, exactly one new dbt_build run
# appears, and that run succeeds.
set -euo pipefail

DAG="${1:?usage: check_dbt_trigger.sh INGEST_DAG_ID [TIMEOUT_SECONDS]}"
TIMEOUT="${2:-1200}"
HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE=(docker compose -f "$HERE/../docker/compose.yaml" --profile core)
af() { "${COMPOSE[@]}" exec -T airflow-scheduler airflow "$@"; }

dbt_runs_json() { af dags list-runs dbt_build -o json 2>/dev/null; }

before="$(dbt_runs_json | python3 -c 'import json,sys; print(json.dumps([r["run_id"] for r in json.load(sys.stdin)]))')"

"$HERE/run_dag.sh" "$DAG" "$TIMEOUT"

deadline=$((SECONDS + TIMEOUT))
summary="no new dbt_build run yet"
while (( SECONDS < deadline )); do
  summary="$(dbt_runs_json | BEFORE="$before" python3 -c '
import json, os, sys
before = set(json.loads(os.environ["BEFORE"]))
new = [r for r in json.load(sys.stdin) if r["run_id"] not in before]
states = sorted(r["state"] for r in new)
print(f"{len(new)} new run(s): {states}")
')"
  case "$summary" in
    *running*|*queued*|"0 new run(s): []") sleep 6 ;;
    *) break ;;
  esac
done
echo "dbt_build after $DAG: $summary"
[[ "$summary" == "1 new run(s): ['success']" ]]
