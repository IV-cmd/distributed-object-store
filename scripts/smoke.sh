#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

trap 'docker compose logs --no-color; docker compose down' EXIT
docker compose up --build -d

for attempt in {1..30}; do
  ready=0
  for node in fst-1 fst-2 fst-3; do
    if docker compose logs --no-color "$node" | grep -Fq "RegisterNode succeeded: node_id=$node "; then
      ready=$((ready + 1))
    fi
  done
  if [[ "$ready" -eq 3 ]]; then
    break
  fi
  sleep 1
done
[[ "$ready" -eq 3 ]]
[[ "$(docker compose ps --status running --services | sort)" == $'fst-1\nfst-2\nfst-3\nmgm' ]]

for node in fst-1 fst-2 fst-3; do
  docker compose logs --no-color mgm | grep -F "RegisterNode accepted: node_id=$node endpoint=$node:50052"
done

check_unimplemented() {
  local output status
  if output=$(docker compose exec -T mgm client mgm:50051 "$@" 2>&1); then
    status=0
  else
    status=$?
  fi
  printf '%s\n' "$output"
  [[ "$status" -eq 3 ]]
  grep -Fq 'RPC status 12:' <<< "$output"
  grep -Fq 'not implemented in M0' <<< "$output"
}

check_unimplemented get example
check_unimplemented put example 42
