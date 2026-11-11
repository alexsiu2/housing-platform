#!/usr/bin/env sh
# Fail fast (before `docker compose up`) if the Airflow UI host port is taken.
#
#   scripts/check_port.sh            # port from $AIRFLOW_PORT, then .env, then 8080
#   scripts/check_port.sh 8081
#
# Containers from this project's own compose stack ("rpof") are ignored:
# `make up` recreates them. Anything else on the port is reported with a fix.

port="${1:-${AIRFLOW_PORT:-}}"
if [ -z "$port" ] && [ -f .env ]; then
  port=$(sed -n 's/^AIRFLOW_PORT=//p' .env | tail -n 1)
fi
port="${port:-8080}"

hint="Stop it, or pick another port: set AIRFLOW_PORT=<port> in .env."

if command -v docker >/dev/null 2>&1; then
  others=$(docker ps --filter "publish=$port" \
    --format '{{.Names}}|{{.Label "com.docker.compose.project"}}' 2>/dev/null \
    | awk -F'|' '$2 != "rpof" { p = ($2 == "" ? "none" : $2); print "  " $1 "  (compose project: " p ")" }')
  if [ -n "$others" ]; then
    echo "Port $port is already published by another container:"
    echo "$others"
    echo "Stop it with: docker stop <name>  (or 'docker compose -p <project> down')."
    echo "$hint"
    exit 1
  fi
  ours=$(docker ps -q --filter "publish=$port" \
    --filter "label=com.docker.compose.project=rpof" 2>/dev/null)
  [ -n "$ours" ] && exit 0
fi

if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port $port is in use by a non-Docker process:"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN
  echo "$hint"
  exit 1
fi
exit 0
