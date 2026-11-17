#!/usr/bin/env bash
set -euo pipefail

project="objectstore-smoke-$$"
export MGM_PORT=0 HEARTBEAT_INTERVAL_MS=250 HEARTBEAT_TIMEOUT_MS=4000 REPAIR_INTERVAL_MS=100
compose=(docker compose -p "$project")
cleanup() {
  "${compose[@]}" unpause fst4 >/dev/null 2>&1 || true
  "${compose[@]}" logs --no-color || true
  "${compose[@]}" down --volumes --remove-orphans
}
trap cleanup EXIT
client() {
  "${compose[@]}" exec -T mgm env MGM_ADDRESS=mgm:50051 client "$@"
}
wait_node() {
  local node=$1 state=$2
  for attempt in $(seq 1 150); do
    if client nodes | grep -q "node_id=$node state=$state "; then
      return
    fi
    sleep 0.2
  done
  echo "Timed out waiting for $node $state" >&2
  exit 1
}
wait_replicas() {
  local object=$1 count=$2
  for attempt in $(seq 1 150); do
    metadata=$(client stat "$object")
    if [[ $(grep -c "replicas=$count nodes=" <<<"$metadata" || true) == 3 ]]; then
      printf '%s\n' "$metadata"
      return
    fi
    sleep 0.2
  done
  echo "Timed out waiting for $object replicas=$count" >&2
  exit 1
}
"${compose[@]}" up --build -d mgm fst1 fst2 fst3 fst4
for attempt in $(seq 1 60); do
  registered=$("${compose[@]}" logs --no-color fst1 fst2 fst3 fst4 | grep -c 'RegisterNode succeeded' || true)
  if [[ "$registered" == 4 ]]; then break; fi
  [[ "$attempt" != 60 ]] || exit 1
  sleep 1
done
for node in fst1 fst2 fst3 fst4; do wait_node "$node" UP; done
"${compose[@]}" exec -T mgm sh -c 'dd if=/dev/zero of=/tmp/input bs=1048576 count=2 && printf "repair-tail" >> /tmp/input'
client put /tmp/input sample
wait_replicas sample 3
echo "TEST stop_fst2 timestamp_ms=$(date +%s%3N)"
"${compose[@]}" stop -t 1 fst2
wait_node fst2 DOWN
wait_replicas sample 3
metadata=$(client stat sample)
[[ $(grep -c 'nodes=fst1,fst3,fst4' <<<"$metadata") == 3 ]]
echo "PASS: redundancy restored on fst1,fst3,fst4 timestamp_ms=$(date +%s%3N)"
client get sample /tmp/repaired
"${compose[@]}" exec -T mgm cmp /tmp/input /tmp/repaired
"${compose[@]}" exec -T mgm md5sum /tmp/input /tmp/repaired
"${compose[@]}" run --rm --no-deps --entrypoint sh fst2 -c 'printf "\001" | dd of=/var/lib/objectstore/sample/0.chunk bs=1 count=1 conv=notrunc; rm /var/lib/objectstore/sample/1.chunk /var/lib/objectstore/sample/1.crc; cp -r /var/lib/objectstore/sample /var/lib/objectstore/unexpected'
echo "TEST start_fst2 timestamp_ms=$(date +%s%3N)"
"${compose[@]}" start fst2
wait_node fst2 UP
metadata=$(client stat sample)
printf '%s\n' "$metadata"
grep -q 'chunk=0 .* replicas=3 nodes=fst1,fst3,fst4' <<<"$metadata"
grep -q 'chunk=1 .* replicas=3 nodes=fst1,fst3,fst4' <<<"$metadata"
grep -q 'chunk=2 .* replicas=4 nodes=fst1,fst2,fst3,fst4\|chunk=2 .* replicas=4 nodes=fst1,fst3,fst4,fst2' <<<"$metadata"
"${compose[@]}" logs --no-color mgm | grep 'event=node_up node=fst2 verified=1 missing=1 corrupt=1 unexpected=2'
echo 'PASS: rejoin verifies valid copies, excludes corrupt/missing copies, flags unexpected data'
client get sample /tmp/rejoined
"${compose[@]}" exec -T mgm cmp /tmp/input /tmp/rejoined
client put /tmp/input twofail
echo "TEST two_failures_begin timestamp_ms=$(date +%s%3N)"
"${compose[@]}" stop -t 1 fst2
sleep 2
"${compose[@]}" pause fst4
for attempt in $(seq 1 100); do
  if "${compose[@]}" logs --no-color mgm | grep -q 'event=repair_started object=twofail .*target=fst4'; then
    break
  fi
  [[ "$attempt" != 100 ]] || exit 1
  sleep 0.1
done
echo "TEST stop_fst3_during_copy timestamp_ms=$(date +%s%3N)"
"${compose[@]}" stop -t 1 fst3
"${compose[@]}" unpause fst4
wait_node fst2 DOWN
wait_node fst3 DOWN
wait_replicas twofail 2
client get twofail /tmp/twofail
"${compose[@]}" exec -T mgm cmp /tmp/input /tmp/twofail
echo 'PASS: two failures during repair preserve verified reads; RF=2, no false full-redundancy claim'
"${compose[@]}" stop -t 1 fst1 fst4
wait_node fst1 DOWN
wait_node fst4 DOWN
if client get twofail /tmp/unavailable; then
  echo 'All replicas offline unexpectedly allowed a read' >&2
  exit 1
fi
echo 'PASS: all replicas offline makes reads unavailable; stopped disks are not permanent data loss'
"${compose[@]}" logs --no-color mgm | grep -E 'event=(node_down|node_up|repair_started|repair_completed|replica_quarantined|unexpected_chunk)'
