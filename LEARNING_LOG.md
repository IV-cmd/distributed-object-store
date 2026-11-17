# Learning log

## Build foundation

Build skeleton, gRPC plumbing, containers, and CI. No storage logic.

## Single-node storage



## Metadata and placement



## Redundant layouts



## Integrity and recovery



## Fault injection and measurements



## Engineering observations

Owner reflection sections above remain blank.

- Replica tests first failed to link because their target omitted the FST and
  client libraries; they now have a separately linked integration-test target.
- Piping the smoke script through `tee` hid its failed Docker build. CI now sets
  `pipefail`, so logging cannot turn a failed check into a successful job.
- Hand-written wrapping differed from clang-format; the actual Actions formatter
  diff was applied without disabling any format or static-analysis check.
- Whole-file FST manifests require stable per-file targets. Placement pins one
  trio instead of distributing different chunks to different node subsets.
- A partial write records acknowledged replicas but does not publish the file;
  checksums detect corruption and fallback reads another copy, not repair it.
- clang-tidy identified vector growth without a reservation and interchangeable
  string parameters in a test helper. The registry snapshot reserves its size;
  the test helper no longer needs an object-ID parameter.
- Automatic CI runs one GCC/test/format job for code changes. Slower compiler,
  static-analysis, and Compose checks are selected manually; docs-only pushes
  do not rebuild the store.

### Recovery engineering observations

- Whole-file manifests cannot publish one repaired chunk independently. Targets
  use a separate sparse-repair marker; pending uploads remain protected.
- A heartbeat alone cannot prove returning disk contents. Incarnation-bound
  inventories distinguish valid, missing, corrupt, and unexpected copies.
- RPCs run outside the metadata mutex. File generations and target incarnations
  fence late copy acknowledgements after file reuse or node restart.
- Quarantined source copies do not count toward RF. One dead node plus another
  corrupt holder can leave only two verified copies with four registered nodes.
- Format differences were found by Actions; its exact formatter diff was applied.
  Owner reflection sections remain blank.
- Final review found that missing source chunks returned NOT_FOUND, whereas only
  DATA_LOSS sources were quarantined. That could overcount healthy RF after fallback.
  Both confirmed local failures now quarantine that copy; a regression test verifies
  that only the surviving source and verified replacement are counted.

### Fault-injection and benchmark observations

Owner reflection above remains blank; these are recorded engineering facts.

- All three fault scenarios passed in the linked Actions proof run. Node-stop
  verified sampled reads during recovery and inventory-verified rejoin. Assertions
  wait for DOWN and exclude the failed holder, so stale initial RF=3 metadata
  cannot be mistaken for completed repair.
- Corrupting a running FST chunk caused DATA_LOSS and healthy-copy fallback.
  The client does not report this to MGM: reactive fallback leaves a metadata
  knowledge gap and is not a scrubber or automatic corruption repair.
- Real Linux netem worked using the optional tools image and a narrowly scoped
  NET_ADMIN capability. Delay is applied to outgoing packets, not once per
  object; CLI wall-time measurements include Docker exec and are distinct from
  the client-operation benchmark timer.
- The first successful proof run uploaded only the visible command logs:
  upload-artifact excluded the hidden .proof-results directory by default.
  Enabling hidden files for the explicitly scoped evidence paths and rerunning
  all three commands preserved the raw samples and container logs too.
- Shared-runner measurements varied across runs. In the retained run, the
  post-DOWN degraded-read median was slightly lower than healthy reads; this is
  a noisy short measurement after the failed node has already been excluded,
  not evidence that failure improves performance or that timeouts are free.
- The known-volume rebuild includes one-copy-at-a-time pacing. Its DOWN-to-RF=3
  duration is not a pure disk/network throughput measurement.

#### Retained measured results

| Object | RF=3 write MB/s | Healthy read MB/s |
|---|---:|---:|
| 1 MiB | 73.11 | 172.33 |
| 16 MiB | 179.04 | 284.34 |
| 64 MiB | 212.14 | 308.36 |

| 16 MiB reads | Median latency |
|---|---:|
| Three healthy holders | 58.47 ms |
| One holder DOWN; RF=2; spare not started | 57.09 ms |

| 64 MiB rebuild | Time |
|---|---:|
| Kill request to DOWN | 5.187 s |
| DOWN to RF=3 | 31.949 s |
| Kill request to RF=3 | 37.136 s |

Conditions and raw samples: [benchmark record](docs/benchmark-results.json).
Runtime evidence: [Actions proof](https://github.com/IV-cmd/distributed-object-store/actions/runs/44075050655).
No new storage race was observed in these scenarios; none is invented here.
