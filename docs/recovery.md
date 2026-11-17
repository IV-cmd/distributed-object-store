# Failure detection and replica recovery

## State and timing

An FST sends its node ID, advertised endpoint, local chunk-file count, and free
bytes to MGM. `HEARTBEAT_INTERVAL_MS` defaults to 2000. MGM records receipt using
`steady_clock`, not the wall clock. `HEARTBEAT_TIMEOUT_MS` defaults to 6000.
`REPAIR_INTERVAL_MS` defaults to 500; the worker scans between copy attempts.
All values are positive integer milliseconds. These are configuration defaults,
not measured detection or rebuild results.

An overdue node becomes DOWN and is excluded from placement and returned active
replicas. A resumed heartbeat or matching startup registration makes a known
node REJOINING, not immediately trusted. Successful inventory reconciliation
makes it UP. Endpoint strings and node IDs must remain unique and stable.
Failure detection is suspicion: a delayed or partitioned healthy node can be
marked down. Slow copies delay the next health scan; timing is not real-time.

## Decision versus execution

`ExpiredNodes` takes node snapshots, a supplied time, and a timeout; it returns
expired IDs. `DecideRepairs` takes file/node snapshots; it returns source/target
copy plans. Both are pure functions with no RPCs, globals, clocks, or disk I/O.
`SelectNodes` only selects UP nodes. The planner excludes existing holders,
including historical DOWN or quarantined holders, from replacement targets.

The MGM worker snapshots state under its mutex, releases it for RPCs, then checks
state again before publishing a result. One maintenance call and one copy RPC
run at a time. Failed sources are tried sequentially; subsequent passes rotate
among available chunk tasks. RPC deadlines bound network attempts, not disk I/O.
This is a concurrency cap, not a bytes-per-second traffic limiter.

## Copy path and publication

1. A completed file with fewer than three eligible copies needs repair.
2. MGM asks a healthy source to `CopyChunk` directly to an UP spare.
3. Source verifies its local sidecar, file size, and MGM's expected xxHash64.
4. Target `InstallChunk` verifies input length/checksum, writes the pair, and
   reads it back before acknowledging. Bytes never pass through MGM.
5. MGM adds the target only if the file generation and target incarnation still
   match and the target is UP. Failed or stale acknowledgements do not count.

The target uses a `repair` size/chunk-count marker, not a completed whole-file
manifest. A repaired object can be sparse; only installed, verified chunks are
readable. This avoids waiting for every chunk before advertising a valid repair.
Regular uploads remain unreadable until finished. Repair rejects pending upload
directories and will not overwrite an existing complete data/sidecar pair.
Identical installs are idempotent; corrupted existing pairs require intervention.

A source reporting missing data or verified corruption is quarantined for that chunk and another
source is tried. It is no longer counted as a healthy replica. With one dead
node plus a corrupt holder, a four-node cluster may restore only two verified
copies: it must not invent a third or overwrite quarantined data automatically.
No full-disk scrubber or in-place corruption repair is implemented.

`StatFile.chunks.replicas` contains currently eligible locations;
`known_replicas` retains recorded holders, including down or quarantined copies.
`invalid_replicas` identifies quarantined holders. The file's `complete` flag
means its original upload finished, not that its current RF is necessarily three.
In-progress/failed uploads are not automatically repaired or published.

## Rejoin reconciliation

MGM requests a full inventory carrying the node's current incarnation. FST scans
its local chunk files and verifies their sidecars before reporting checksums and
object sizes. Missing expected files are detected by absence from this inventory.
MGM rejects stale inventory responses and reconciles against its expected map:

- Expected and verified copies remain eligible.
- Missing copies are removed from recorded holders.
- Corrupt or mismatched copies remain quarantined, not counted.
- Unexpected copies are logged and left on disk; they are not adopted or deleted.

UP is published only after reconciliation. There is no rebalance on rejoin.
An original copy returning after replacement can therefore leave four valid
copies of a chunk. No automatic excess-copy trimming is implemented.

## Capacity and failure limits

Three distinct replicas require three healthy storage nodes. The default stack
has four FSTs so one can be a replacement. Four containers/volumes on one machine
are not independent physical disks, racks, or machine-failure domains.

If two nodes fail, reads work while at least one eligible checksum-valid copy of
each chunk survives. Full RF=3 cannot be restored with only two healthy nodes.
When every copy is offline, reads are unavailable; stopping processes does not
by itself erase their disks. Permanent loss/corruption of every copy makes that
chunk unrecoverable; no erasure coding or external backup is present.

MGM metadata remains in memory. Heartbeats re-register nodes after MGM restarts,
but cannot reconstruct the lost namespace. There is no metadata HA, TLS/auth,
versioned object API, fsync guarantee, rebalance, drain command, or persistence.
The inventory is an unpaged unary message and verifies all local data on rejoin;
large inventories can exceed RPC limits or heartbeat timeouts.

Delete/reuse can race with an in-flight copy. Generation checks prevent stale
metadata publication, but copied bytes may remain orphaned on a target. Metadata
and disk changes are not a distributed transaction; no orphan cleanup daemon
exists. Do not concurrently delete/reuse IDs during transfers or repairs.

## Evidence and logs

Structured lines use wall-clock epoch `timestamp_ms`, `event`, node/object/chunk
fields. Events include `node_down`, `node_rejoining`, `node_up`, `repair_started`,
`repair_completed`, `repair_failed`, `repair_discarded`, and `unexpected_chunk`.
Epoch timestamps are for observation; timeout decisions use monotonic time.

`client nodes` shows node state/counters; `client stat <id>` shows eligible RF.
`make smoke` uses an isolated four-FST Compose stack. It stops a node, waits for
replacement, checks byte equality/MD5, modifies its offline data to test rejoin,
and pauses the spare to keep a copy in flight while stopping a second holder.
It then verifies RF=2 and successful reads, followed by all-copies-offline refusal.
The test uses 250 ms heartbeats, a 4000 ms timeout, and 100 ms scans, not production
settings. Report actual log timestamps; do not treat this as a benchmark.
