#!/usr/bin/env bash
set -euo pipefail

project="objectstore-smoke-$$"
export MGM_PORT=0
compose=(docker compose -p "$project")
cleanup() {
  "${compose[@]}" logs --no-color || true
  "${compose[@]}" down --volumes --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" up --build -d
for attempt in $(seq 1 60); do
  registered=$("${compose[@]}" logs --no-color fst1 fst2 fst3 | grep -c 'RegisterNode succeeded' || true)
  if [[ "$registered" == 3 ]]; then
    break
  fi
  if [[ "$attempt" == 60 ]]; then
    echo 'FST registration timed out' >&2
    exit 1
  fi
  sleep 1
done
client() {
  "${compose[@]}" exec -T mgm env MGM_ADDRESS=mgm:50051 client "$@"
}
"${compose[@]}" exec -T mgm python3 -c 'from pathlib import Path; Path("/tmp/input").write_bytes(bytes(range(256)) * 8192 + b"replica-tail")'
client put /tmp/input sample
metadata=$(client stat sample)
printf '%s\n' "$metadata"
grep -q 'chunks=3 complete=true' <<<"$metadata"
[[ $(grep -c 'replicas=3 nodes=fst1,fst2,fst3' <<<"$metadata") == 3 ]]
echo 'PASS: MGM records three distinct replicas and a checksum per chunk'
client get sample /tmp/output
"${compose[@]}" exec -T mgm cmp /tmp/input /tmp/output
before=$("${compose[@]}" exec -T mgm md5sum /tmp/input | cut -d' ' -f1)
after=$("${compose[@]}" exec -T mgm md5sum /tmp/output | cut -d' ' -f1)
[[ "$before" == "$after" ]]
echo "PASS: healthy round-trip md5sum=$before"
for node in fst1 fst2 fst3; do
  "${compose[@]}" kill "$node"
  log=$(client get sample "/tmp/output-$node" 2>&1)
  printf '%s\n' "$log"
  grep -q "replica rejected node_id=$node" <<<"$log"
  "${compose[@]}" exec -T mgm cmp /tmp/input "/tmp/output-$node"
  degraded=$("${compose[@]}" exec -T mgm md5sum "/tmp/output-$node" | cut -d' ' -f1)
  [[ "$before" == "$degraded" ]]
  echo "PASS: killed $node; verified fallback md5sum=$degraded"
  "${compose[@]}" start "$node"
done
"${compose[@]}" exec -T fst1 python3 -c 'from pathlib import Path; p=Path("/var/lib/objectstore/sample/0.chunk"); b=bytearray(p.read_bytes()); b[0]^=1; p.write_bytes(b)'
log=$(client get sample /tmp/corrupt-fallback 2>&1)
printf '%s\n' "$log"
grep -q 'replica rejected node_id=fst1 chunk=0 code=15' <<<"$log"
"${compose[@]}" exec -T mgm cmp /tmp/input /tmp/corrupt-fallback
echo 'PASS: checksum failure on disk falls back to a verified replica'
client rm sample
if client get sample /tmp/deleted; then
  echo 'Deleted object was readable' >&2
  exit 1
fi
echo 'PASS: healthy-cluster remove deletes the object'
"${compose[@]}" exec -T mgm touch /tmp/empty
client put /tmp/empty empty
client get empty /tmp/empty-output
"${compose[@]}" exec -T mgm test ! -s /tmp/empty-output
echo 'PASS: empty-file metadata-only round-trip'
"${compose[@]}" kill fst2
if client put /tmp/input partial; then
  echo 'Upload with a dead replica unexpectedly succeeded' >&2
  exit 1
fi
metadata=$(client stat partial)
printf '%s\n' "$metadata"
grep -q 'complete=false' <<<"$metadata"
grep -q 'replicas=2 nodes=fst1,fst3' <<<"$metadata"
if client get partial /tmp/partial-output; then
  echo 'Incomplete file was readable' >&2
  exit 1
fi
echo 'PASS: failed upload records two acknowledged replicas and stays incomplete'
