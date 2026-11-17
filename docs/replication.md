# Metadata and three-way replication

## Ownership

MGM stores a mutex-protected, in-memory registry and file map:

```text
node_id -> {endpoint, capacity_bytes}
object_id -> {name, size_bytes, chunk_count, complete, targets[3],
              chunks[index] -> {chunk_id, checksum, committed, replicas[0..3]}}
```

A chunk ID is `(object_id, index)`, not a separate random identifier. Capacity is
available filesystem space sampled at FST startup; it is informational only.
Node IDs and endpoint strings must both be unique; matching re-registration is
allowed, but changing an existing node's endpoint is rejected. Different endpoint
strings do not prove different physical machines or failure domains.

`StatFile` and `ListFiles` include incomplete files and allocated chunks.
`committed=true` means the upload client reported its acknowledgements, **not**
that three copies exist: inspect `replicas` and the file's `complete` flag.

## Placement

`SelectNodes(available, replication_factor)` in `src/mgm/placement.cc` is pure:
it sorts by node ID/endpoint, ignores empty identities and duplicate IDs or
endpoints, and returns up to the requested number. It makes no RPCs and reads no
globals. MGM rejects allocation with `RESOURCE_EXHAUSTED` unless it gets three.
The input is the UP set; DOWN and REJOINING nodes are excluded.

The first allocation pins three UP nodes for the original upload;
each subsequent `AllocateChunk` returns that trio. Repair can add new targets. This deliberately preserves
the FST's existing whole-file manifest/finish contract: each chosen node must
hold every chunk. It is not round-robin load balancing; with more than three
registered nodes, later nodes are not selected. Capacity is not a placement rule.

## Write protocol

1. Client `Put` reserves a write-once ID in MGM and records name/size/chunk count.
2. For each chunk, client calls `AllocateChunk`. On the first, it also calls
   `BeginObject` on the three targets to reserve local pending manifests.
3. Client computes xxHash64 and calls `WriteChunk` sequentially on each usable
   FST. Each FST verifies the request and stores bytes plus a checksum sidecar.
4. Client calls `CommitChunk` with only nodes that acknowledged that chunk and
   its checksum. Duplicate or unallocated nodes are rejected. Even zero or two
   acknowledgements are recorded; no full-replication claim is invented.
5. A partial commit returns a nonzero client error and aborts further chunks.
   Metadata remains inspectable and `complete=false`; downloads reject it.
6. After all chunks have three acknowledgements, client finishes all FST local
   manifests, then calls MGM `CompleteFile`. MGM requires every expected chunk
   to be committed with exactly three distinct replicas.

Empty files have no chunks: they are completed entirely in MGM and create no
FST object directories. Direct FST API users can still create empty manifests.
No automatic upload retry/resume or background cleanup exists; remove a failed
ID before reusing it. Do not modify the source during upload; size changes are
checked, but same-size modifications are not detected as a file-level snapshot.

A lost RPC acknowledgement can leave an unrecorded copy; a failed metadata RPC
can leave successful writes unrecorded. The client reports the error but cannot
atomically commit across processes. Similarly, a failure during local finishes
leaves the MGM file incomplete even if every chunk had three acknowledgements.
Completion assumes the trusted client follows this protocol; MGM does not
independently audit each FST before accepting `CompleteFile`.

## Read protocol

Client checks `StatFile.complete`, then calls `LocateChunk` for each index.
For each chunk it rotates the starting replica by `index % replica_count`, then
tries each location at most once. This distributes first attempts and ensures a
multi-chunk killed-node check actually exercises fallback rather than always
hitting an unaffected first node. Replica RPC deadlines are five seconds.

An FST verifies its stored sidecar on every read. The client also requires:

- The expected chunk identity from MGM.
- The expected byte length for that index.
- A response checksum equal to MGM's recorded xxHash64 value.
- A freshly computed checksum of the received bytes equal to that value.

A readable response is not necessarily a valid response. Recomputed checksums
from a corrupted replica cannot replace the MGM reference. This is accidental
corruption detection, not adversarial security; trusted metadata and a
non-cryptographic hash cannot authenticate data.

Unavailable, missing, or corrupt replicas cause fallback. If none verify,
return an error (`DATA_LOSS` / `CHECKSUM_MISMATCH` if corruption was observed).
Downloads publish by rename only after all chunks verify; failure preserves any
existing destination. A hard-killed client can leave `<output>.download` behind.
Do not delete and reuse an ID during a download: reads are not versioned snapshots.

## Delete behavior

`rm` tries every planned target, including nodes with an incomplete local upload.
`NOT_FOUND` is acceptable during cleanup. MGM metadata is deleted only after all
FST deletes succeed or report missing data. An unreachable node returns an error
and retains metadata for retry. This is not an atomic distributed delete: already
deleted copies are not restored and the retained map can describe stale copies.

## Limits and verification

MGM restart loses all file metadata. Heartbeats restore node registrations, not
the namespace. UP/DOWN detection, bounded node-to-node repair, and inventory
reconciliation are described in [recovery](recovery.md). `known_replicas` retains
historical holders; `replicas` reports current eligible copies. A rejoin may leave
more than three verified copies; there is no rebalance or excess-copy trimming.

Metadata remains unpaged and in-memory; there is no fsync guarantee, distributed
transaction, orphan cleanup, scrubber, erasure coding, TLS, or authentication.
Failed uploads remain incomplete until removed; repair does not finish them.
Separate Compose volumes on one host are not independent failure domains.

Placement/metadata tests cover policy and lifecycle. Repair tests cover verified
copying, corrupt-source fallback, inventory reconciliation, and stale acknowledgements
after file reuse or target restart. `scripts/smoke.sh` exercises the actual
four-node stop/start path, a second failure during repair, and `cmp`/`md5sum`.
MD5 is only a comparison tool, not the store's checksum scheme.
