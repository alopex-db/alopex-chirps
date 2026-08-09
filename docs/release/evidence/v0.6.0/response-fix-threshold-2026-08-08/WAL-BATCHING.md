# WAL batching follow-up

## Diagnosis

The previous performance harness resolved `fsync_interval=0`, but the
`alopex-core 0.3.4` `WalWriter::append()` implementation unconditionally did
`flush + sync_all` for every record. Consequently the storage layer's
`fsync_interval` setting did not control physical sync frequency, and the
`fsync_calls` audit counter overstated the number of Raft durability barriers.

## Change

`alopex-chirps-raft-storage` now owns the compatible WAL framing through a
small buffered sink. Records are serialized with the same bincode/CRC framing
that `alopex-core::log::wal::WalReader` consumes. A Raft append batch writes all
records to the buffer and issues one `flush + sync_all` before invoking the
OpenRaft callback. `fsync_interval > 0` retains explicit interval barriers.

The diagnostic counter remains process-wide and optional: it is only sampled by
the performance runner's `--resource-audit` path, so removing that flag removes
the sampler without changing correctness or admission behavior.

## Unit evidence

- `fsync_interval_controls_sync_frequency`: 3 records at interval 2 produce 2
  durability barriers (threshold plus final drain).
- `zero_fsync_interval_group_commits_one_append_batch`: 3 records in one Raft
  append batch produce exactly 1 barrier.
- `cargo test -p alopex-chirps-raft-storage --locked`: 21 unit tests and 4
  resilience tests passed.
- `cargo clippy -p alopex-chirps-raft-storage --all-targets --locked -- -D warnings` passed.

## Remaining measurement

The prior controlled 5+5 release performance run predates this WAL change and
must not be reused as post-fix evidence. A fresh controlled run is required to
quantify throughput, latency, RSS, and `fsync_calls`; no release performance
verdict is made from the unit evidence alone.
