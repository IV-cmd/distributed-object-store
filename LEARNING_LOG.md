# Learning log

## Build foundation

Build skeleton, gRPC plumbing, containers, and CI. No storage logic.

## Single-node storage



## Metadata and placement



## Redundant layouts



## Integrity and recovery



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
