# Distributed Object Store with Replication & Erasure Coding

An EOS-inspired C++20 learning project for graduate/junior systems roles.
M0 establishes the build and RPC foundation; this is not a working object store.
This independent project is not CERN EOS or an affiliated implementation.

## Roadmap / not yet done

- Object storage and metadata persistence
- Three-way replication
- Reed-Solomon (4+2) erasure coding
- Per-chunk checksums and background scrubbing/repair
- Node drain, failure detection, and re-replication
- Fault injection and performance benchmarks
