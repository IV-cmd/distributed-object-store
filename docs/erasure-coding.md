# Erasure coding

`replica3` remains the default. A nonempty `rs42` file needs six distinct UP
FSTs; two extra FSTs allow redundancy to recover while two holders stay down.
Empty files are metadata-only in both client layouts.

## Codec construction

`src/common/rs_codec.{h,cc}` operates on byte strings and shard indices only.
Its library has no gRPC dependency, file I/O, or global tables. Field tables
are built within each operation. `rs_transfer` handles network verification
separately, shared by the client and repair coordinator.

The from-scratch implementation keeps the learning surface small and explicit:

- GF(256) addition/subtraction is XOR. Multiplication uses exponent/log tables
  with primitive element 2 and polynomial `0x11d` (`x^8+x^4+x^3+x^2+1`).
- Generator rows 0–3 are the identity, so data fragments are stored unchanged.
  Parity row `i` uses `C[i,j] = inverse(i XOR (j+2))`, for `i=0,1`, `j=0..3`.
  Distinct, disjoint Cauchy parameters make the needed square minors invertible.
- Encoding splits an input into four equal pieces, zero-pads the tail, and
  computes two parity pieces. Decoding selects four distinct generator rows,
  inverts their 4x4 matrix by Gaussian elimination in the field, and trims
  padding to the supplied original byte count.

The codec does not locate unknown errors or authenticate bytes. Callers reject
bad checksums first, turning corruption into a known missing fragment. Fewer
than four verified fragments cannot reconstruct a stripe. Unit tests exercise
all 15 two-loss combinations, field inverses/distributivity, binary/unaligned
inputs, duplicate indices, invalid lengths, and checksum-before-decode handling.
The codec test target does not link gRPC or require a running cluster.

## Stripes and metadata

For a logical stripe of `n` bytes (at most 4 MiB), each fragment is `ceil(n/4)`
bytes, at most 1 MiB. A file therefore has `6 * ceil(file_size / 4 MiB)` chunks.
Chunk index `6 * group + slot` identifies the stripe group and slot; metadata
stores `stripe_index = slot` (0–5) and `parity = slot >= 4`.

One initial set of six targets is pinned for the file. Each slot has one holder
per stripe. MGM records the layout and expected size/hash of every fragment;
its original file size supplies the padding boundary. Repair can relocate
individual fragments to spare nodes.

The client encodes one bounded stripe, allocates/writes its fragments, and
commits only successful acknowledgements. RS FSTs use the existing sparse
`repair` marker because no individual FST contains every file fragment. MGM
publishes the complete file only after all six fragments of every stripe are
committed. A failed upload remains incomplete; removing the ID permits retry.
This is not a distributed transaction or an upload-resume protocol.

## Reads and repair

Healthy reads fetch slots 0–3 and concatenate without matrix decoding. A failed
RPC or checksum rejection causes the reader to seek other slots until four
verified fragments exist. Each response must match the expected length, its
reported checksum, and MGM's xxHash64. Re-encoding the reconstructed data checks
all six expected hashes, including on the healthy path. Foreign/reordered stripe
metadata is rejected. Downloads replace the destination only after full success;
three unavailable fragments return a clear error and preserve an existing file.

The pure MGM planner excludes current and historical stripe holders when
choosing spare targets. With at least four live fragments, the bounded repair
worker asks a healthy FST to reconstruct. That coordinator fetches verified
fragments, decodes if data is missing, re-encodes, and installs only the requested
fragment on the target. The target checks written bytes before acknowledging;
MGM checks file generation and node incarnations before publishing the location.
MGM never proxies chunk bytes. The simple implementation regenerates all six
fragments in memory, rather than optimizing computation for a single output.

Two unrepaired fragment losses are tolerated; three are not. A third node
failure **after full repair** is only one current fragment loss and need not
break reads. Stopped nodes may return: unavailability is not proof of permanent
data loss. The failure proof deliberately tests three losses without spares.

## Run and verify

On the Linux Docker/Compose setup described in README:

```sh
docker compose up --build -d
# Wait until client nodes reports all eight FSTs UP.
docker compose run --rm --no-deps client nodes
mkdir -p .objectstore-files
head -c 5242887 /dev/urandom > .objectstore-files/rs-input.bin
export LOCAL_UID=$(id -u) LOCAL_GID=$(id -g)
docker compose run --rm --no-deps client put /files/rs-input.bin rs-example rs42
docker compose stop fst1 fst5
docker compose run --rm --no-deps client get rs-example /files/rs-output.bin
cmp .objectstore-files/rs-input.bin .objectstore-files/rs-output.bin
# stat eventually shows six live fragment slots again, using fst7/fst8.
docker compose run --rm --no-deps client stat rs-example
docker compose start fst1 fst5
```

`make test` runs all C++ tests. `make erasure` uses disposable projects to test
all 15 holder-loss pairs without spares using nonperiodic 5 MiB+7-byte input
and six distinct expected hashes per stripe, corrupt-fragment reconstruction,
data/parity repair onto fst7/fst8 while fst1/fst5 remain DOWN, and an isolated
three-loss case with output preservation. [The Actions proof](https://github.com/IV-cmd/distributed-object-store/actions/runs/44084029111)
retains raw JSON, per-phase timings, and container logs. `make bench` measures
both layouts; [README](../README.md#measured-benchmarks) labels conditions and
links the raw record. Expensive workflows remain manually selected in Actions.

When adding spares to an already-running stack, use `docker compose up --no-deps
-d fst7 fst8`. Otherwise Compose can recreate MGM, destroying its in-memory
namespace and container-local inputs. The proof checks that the uploaded file
still exists after spare startup.

## Honest limits

- Scalar, table-based field arithmetic: no SIMD, ISA-L, parallel fragment RPCs,
  or optimized single-fragment reconstruction. The measurements include this
  implementation's CPU/verification costs, not an optimized RS speed claim.
- Stored chunk bytes are `6 * ceil(n/4)` per stripe. Aligned inputs use 1.5x
  versus replication's 3x, but one byte needs six fragment bytes before six
  sidecars and markers. Tiny files can cost more than replication.
- Returning original holders may leave extra valid copies. No excess-copy
  trimming exists, so post-repair disk usage can exceed the fresh-layout ratio.
- Read corruption does not notify MGM or proactively repair the bad copy.
  There is no background scrubber or in-place corruption overwrite.
- Metadata/inventories remain unpaged and can exceed RPC limits. MGM metadata
  is volatile; no metadata HA, authentication/TLS, fsync guarantee, or independent
  physical-host failure proof is added. Containers/volumes share one host.
