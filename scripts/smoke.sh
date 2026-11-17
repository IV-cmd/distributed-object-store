#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

export COMPOSE_PROJECT_NAME="objectstore-smoke-${GITHUB_RUN_ID:-$$}"
export FST_PORT=0
trap 'docker compose logs --no-color; docker compose down --volumes' EXIT
docker compose up --build -d

ready=0
for attempt in {1..30}; do
  output=$(docker compose exec -T fst client get readiness-probe /tmp/readiness-probe 2>&1) || true
  if grep -Fq 'RPC status 5:' <<< "$output"; then
    ready=1
    break
  fi
  sleep 1
done
[[ "$ready" -eq 1 ]]
[[ "$(docker compose ps --status running --services)" == fst ]]

docker compose exec -T fst sh -s <<'SH'
set -eu
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
dd if=/dev/zero of="$work/input" bs=1048576 count=2 status=none
printf 'tail\000binary\n' >> "$work/input"
client put "$work/input" roundtrip
client get roundtrip "$work/output"
cmp "$work/input" "$work/output"
printf 'PASS: multi-chunk CLI round-trip is byte-identical\n'
client rm roundtrip
if output=$(client get roundtrip "$work/deleted" 2>&1); then
  echo 'FAIL: deleted object was readable'
  exit 1
fi
printf '%s\n' "$output"
printf '%s' "$output" | grep -F 'RPC status 5:'
printf 'PASS: deleted object returns NOT_FOUND\n'

: > "$work/empty"
client put "$work/empty" empty
client get empty "$work/empty-out"
cmp "$work/empty" "$work/empty-out"
client rm empty
printf 'PASS: empty-file CLI round-trip\n'

client put "$work/input" corrupt
printf '\001' | dd of=/var/lib/objectstore/corrupt/0.chunk bs=1 count=1 conv=notrunc status=none
if output=$(client get corrupt "$work/corrupt-output" 2>&1); then
  echo 'FAIL: corrupted chunk was readable'
  exit 1
fi
printf '%s\n' "$output"
printf '%s' "$output" | grep -F 'RPC status 15:'
printf '%s' "$output" | grep -F 'CHECKSUM_MISMATCH'
[ ! -e "$work/corrupt-output" ]
[ ! -e "$work/corrupt-output.download" ]
client rm corrupt
printf 'PASS: flipped disk byte returns DATA_LOSS / CHECKSUM_MISMATCH\n'
SH
