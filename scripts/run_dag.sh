#!/usr/bin/env bash
# Trigger an Airflow DAG inside the running stack and wait for it to finish.
#
#   make smoke-test                  # platform_smoke_test
#   make run-dag DAG=ingest_fred     # any DAG
#
# Exits 0 only if the run ends in state "success".
set -euo pipefail

DAG="${1:?usage: run_dag.sh DAG_ID [TIMEOUT_SECONDS]}"
TIMEOUT="${2:-900}"
COMPOSE=(docker compose -f "$(dirname "$0")/../docker/compose.yaml" --profile core)
af() { "${COMPOSE[@]}" exec -T airflow-scheduler airflow "$@"; }

RUN_ID="cli__$(date -u +%Y%m%dT%H%M%S)"
af dags unpause "$DAG" >/dev/null
af dags trigger "$DAG" --run-id "$RUN_ID" >/dev/null
echo "Triggered $DAG ($RUN_ID); waiting up to ${TIMEOUT}s..."

deadline=$((SECONDS + TIMEOUT))
while (( SECONDS < deadline )); do
  state=$(af dags list-runs "$DAG" -o json 2>/dev/null \
    | python3 -c "import json,sys; r=[x for x in json.load(sys.stdin) if x['run_id']=='$RUN_ID']; print(r[0]['state'] if r else 'unknown')")
  case "$state" in
    success) echo "$DAG: success"; exit 0 ;;
    failed)  echo "$DAG: FAILED - see the Airflow UI or logs/airflow/" >&2; exit 1 ;;
  esac
  sleep 5
done
echo "$DAG: timed out (last state: $state)" >&2
exit 1
