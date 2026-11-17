# Storage layout and protocol

## Disk format

The chunk-size constant is `objectstore::kChunkSize` in `src/common/chunk.h`:
1,048,576 bytes (1 MiB). Replica3 only shortens the last chunk; RS42 uses up to
six 1 MiB fragments per 4 MiB stripe, with padded tail fragments. An empty object has
zero chunks. Direct FST API use creates an empty manifest; MGM-client
empty files live only in MGM metadata, without FST directories.

```text
<data_dir>/
  <object_id>/
    pending          # Uploading; renamed to manifest on successful finish
    manifest         # Completed original upload
    repair           # Sparse repaired object: only verified installed chunks readable
    0.chunk          # Raw binary bytes, no header
    0.crc            # xxHash64 of exactly 0.chunk, despite the .crc extension
    1.chunk
    1.crc
```

Object IDs contain 1-128 ASCII letters, digits, underscores or hyphens. No
slashes or dots are accepted. Chunk indices are zero-based unsigned 32-bit
integers; manifests contain at most UINT32_MAX chunks.

`pending`, `manifest`, and `repair` use the same ASCII size/count format:

```text
<size_bytes> <chunk_count> <layout>\n
```

New markers store layout 0 (`replica3`) or 1 (`rs42`). Legacy two-field markers
are read as replica3. `1048583 2 0` describes a full replicated chunk and a
seven-byte tail. The count is `ceil(size / 1 MiB)` for replica3 and
`6 * ceil(size / 4 MiB)` for RS42. The size remains the logical file size, not
local fragment bytes; readers validate the layout-specific count and lengths.

Each `.crc` contains exactly 16 hexadecimal digits followed by a newline:
xxHash64, seed 0, digest of the raw chunk bytes. The empty-input digest is
`ef46db3751d8e999`, although empty objects do not create empty chunk files.
The writer uses lowercase digits. The format is textual, not native-endian
binary. `chunk.tmp` and `crc.tmp` are unpublished write scratch files.

A chunk and its sidecar form the replication unit on each selected FST. The
manifest supplies local object boundaries; MGM owns the cross-node map.
Changing the constant later requires an on-disk format migration.

## Why xxHash64

[xxHash64](https://xxhash.com/doc/v0.8.2/group___x_x_h64__family.html) is a fast,
non-cryptographic 64-bit checksum with a small Ubuntu library dependency.
The client and FST share one implementation; no checksum code is duplicated.
The `.crc` suffix is a sidecar naming convention, **not a claim to use CRC32C**.

Checksum collisions are possible. An attacker can replace both data and its
checksum. This detects accidental corruption; it does not authenticate data,
replace TLS, or recover bytes. Replicated reads can use another verified copy;
automatic node-failure repair is described in [recovery](recovery.md).

## Replica3 upload and read lifecycle

1. `BeginObject(object_id, size_bytes)` reserves a directory and writes `pending`.
   Existing IDs return `ALREADY_EXISTS`, including incomplete uploads.
2. `WriteChunk(chunk, data, checksum)` requires the pending state, valid index,
   exact expected length, and a matching client checksum before touching disk.
   Completed data/sidecar pairs are not overwritten.
3. Chunk bytes and sidecars are written to scratch files, closed, then renamed.
   A partial pair after I/O failure is not publishable without verification.
4. `FinishObject(object_id)` reads and verifies every expected chunk, then
   renames `pending` to `manifest`. A failed finish never publishes the object.
5. `GetObjectInfo` returns size/count only for completed objects. `ReadChunk`
   reads bytes and the sidecar, verifies length and checksum, and only then
   returns bytes plus a `fixed64` checksum. The client verifies independently.
6. `DeleteObject` removes the entire directory, including incomplete uploads.
   Deletion permits ID reuse. `DeleteChunk` removes just that chunk/sidecar;
   it leaves metadata intact, so a subsequent read reports the missing chunk.

The original `WriteChunk`, `ReadChunk`, and `DeleteChunk` RPC names are retained.
They implement the put/get/delete-chunk operations. Object lifecycle RPCs are
FST-local. MGM supplies placement and completion metadata but never proxies
file bytes. `Scrub` remains `UNIMPLEMENTED`.

The client sends one unary RPC per chunk, not one whole-file message or a gRPC
streaming RPC. Replica3 payload buffering is bounded by chunk size; RS buffers one 4 MiB stripe
plus its fragments. MGM metadata
still grows with file/chunk count. Direct FST empty objects use begin/finish;
replicated empty files are metadata-only. Upload errors leave a reserved object rather
than silently treating missing chunks as end-of-file. To retry, remove the ID
and upload again; automatic retries/resume are not implemented.

Downloads use `<out_file>.download/data`, replacing the destination by rename
only after every chunk passes verification. Failed downloads clean up their
temporary directory and preserve existing output. A killed process can leave a
stale directory that requires manual removal. Source files must remain unchanged
during uploads. Do not delete/reuse IDs during downloads: reads are separate
RPCs, not versioned object snapshots.

## Error mapping

| Condition | gRPC status |
| --- | --- |
| Missing object, chunk, or sidecar | `NOT_FOUND` (5) |
| Client or stored checksum mismatch | `DATA_LOSS` (15), message includes `CHECKSUM_MISMATCH` |
| Invalid stored metadata/file length | `DATA_LOSS` (15) |
| Bad ID, index, requested chunk length, or object size | `INVALID_ARGUMENT` (3) |
| Existing ID/chunk or write to completed object | `ALREADY_EXISTS` (6) |
| Read of incomplete object / wrong upload state | `FAILED_PRECONDITION` (9) |
| Filesystem/open/read/write failures | `INTERNAL` (13) |

[gRPC has no CHECKSUM_MISMATCH status enum](https://grpc.io/docs/guides/status-codes/);
it is an explicit application error label carried by `DATA_LOSS`.
Transport limits, disconnections, and deadlines can also produce gRPC errors.
The CLI prints the numeric RPC status and message and returns nonzero.

## Boundaries

- One FST process exclusively owns a trusted data directory. IDs prevent RPC
  path traversal; this does not defend against local writers inserting symlinks.
- One mutex serializes storage RPCs, including finish verification. This favors
  correctness over concurrency; no throughput or latency claims are made.
- Closed files and atomic same-filesystem renames support normal restart reads.
  There is no `fsync`, journal, crash recovery, or power-loss durability promise.
  The two files of a chunk are not committed atomically as a pair.
- Manifests are validated but not checksummed. There are no versioned schemas,
  upload sessions, quotas, garbage collection, or partial-upload cleanup daemon.
- See [replication protocol](replication.md) and [erasure coding](erasure-coding.md)
  for MGM ownership and fallback. No scrubber, TLS, or authentication. Recovery is described
  in [recovery](recovery.md).
  The network is for trusted local development.

## Repaired chunks

A `repair` marker does not claim that every expected chunk exists. `ReadChunk`
can serve each verified installed pair independently; whole-object
`GetObjectInfo` still requires an original completed manifest. `InstallChunk`
rejects pending upload directories. Sparse repair publication never makes an
unfinished original upload readable.

RS uploads also use sparse `repair` markers from the start; a node stores only
its assigned slots. MGM completion, not an FST whole-object manifest, publishes
the file. A direct FST chunk read may see verified uploaded bytes before MGM
completion; the normal client refuses incomplete files.
