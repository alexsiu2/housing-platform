#!/usr/bin/env bash
# Trigger an ingest DAG and check the whole downstream chain:
#   ingest -> exactly one green dbt_build -> exactly one green compute_scores
#
#   scripts/check_score_chain.sh ingest_census_acs
set -euo pipefail

DAG="${1:?usage: check_score_chain.sh INGEST_DAG_ID [TIMEOUT_SECONDS]}"
TIMEOUT="${2:-1800}"
HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE=(docker compose -f "$HERE/../docker/compose.yaml" --profile core)
af() { "${COMPOSE[@]}" exec -T airflow-scheduler airflow "$@"; }

run_ids() { af dags list-runs "$1" -o json 2>/dev/null | python3 -c 'import json,sys; print(json.dumps([r["run_id"] for r in json.load(sys.stdin)]))'; }

new_runs() {  # $1 = dag, $2 = JSON list of run ids seen before
  af dags list-runs "$1" -o json 2>/dev/null | BEFORE="$2" python3 -c '
import json, os, sys
before = set(json.loads(os.environ["BEFORE"]))
new = [r for r in json.load(sys.stdin) if r["run_id"] not in before]
print(f"{len(new)} new run(s): {sorted(r[\"state\"] for r in new)}")
'
}

wait_for() {  # $1 = dag, $2 = before; prints final summary
  local deadline=$((SECONDS + TIMEOUT)) s="none"
  while (( SECONDS < deadline )); do
    s="$(new_runs "$1" "$2")"
    case "$s" in *running*|*queued*|"0 new run(s): []") sleep 6 ;; *) break ;; esac
  done
  echo "$s"
}

dbt_before="$(run_ids dbt_build)"
score_before="$(run_ids compute_scores)"

"$HERE/run_dag.sh" "$DAG" "$TIMEOUT"
dbt="$(wait_for dbt_build "$dbt_before")";        echo "dbt_build:      $dbt"
score="$(wait_for compute_scores "$score_before")"; echo "compute_scores: $score"

[[ "$dbt" == "1 new run(s): ['success']" && "$score" == "1 new run(s): ['success']" ]]
