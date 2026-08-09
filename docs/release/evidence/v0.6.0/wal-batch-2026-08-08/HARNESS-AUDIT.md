# WAL batching harness audit

The measurement uses the same resource-audit path as the proposal-pipeline
run. It records per-node RSS, CPU, queue depth, transport counters, disk
writes, and process-wide `sync_all` counts. Audit collection remains gated by
`--resource-audit`; normal workload timing does not include the diagnostic
digest or queue-map work.

The initial shared coordinator exposed a harness/implementation interaction:
because all 100 group WALs were synced at every barrier, `fsync_calls` grew by
roughly 30--36x without a corresponding workload benefit. This was diagnosed
from raw node metrics rather than hidden by the aggregate throughput number.
The coordinator now registers a weak writer plus an atomic dirty bit and only
includes dirty writers in a barrier. Failed syncs restore the dirty bit so a
later barrier retries durability.

Unit coverage includes concurrent cross-group barrier completion and the
existing WAL append/sync/recovery cases. `cargo test -p
alopex-chirps-raft-storage --lib wal_storage --locked` passed 20 tests.

The remaining environmental warning (`swap limit capabilities` unavailable)
is recorded by Docker and does not change the WAL correctness result.
