# Raft snapshot transfer finite-state model

This production-before model refines the existing `formal/file-transfer`
checkpoint-before-publish contract for Raft snapshots. It adds multiple chunks,
bounded parallel in-flight work, per-chunk retry accounting, whole-snapshot
digest verification, and a durable checkpoint before installation becomes
visible.

Run from `formal/` using the canonical digest-pinned service:

```sh
docker compose -f compose.yml run --rm apalache typecheck snapshot/model.tla
docker compose -f compose.yml run --rm apalache check --config=snapshot/SnapshotTransfer.cfg --length=10 snapshot/model.tla
```

The check is finite: two chunks, one retry per chunk, concurrency two, and ten
transitions. It proves only the listed safety invariants within that bound. It
does not prove OpenRaft consensus, crash durability, unbounded liveness, real
network behavior, or the 100 MB/s performance target. Those require the mapped
component/failure-injection tests and controlled profile in
`docs/perf/v0_6_tso_hlc_snapshot.md`.
