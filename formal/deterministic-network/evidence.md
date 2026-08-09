# Deterministic network model evidence

Status: PASS for Multi-Raft Requirement 5.3; PARTIAL for Issue #3 as a whole (2026-08-05 Asia/Tokyo)

## Checked scope

- seed: `1539` (`0x603`)
- checker: Apalache 0.58.3, image pinned by digest in `compose.yml`
- type mode: `--features=no-rows`
- JVM heap: 12 GiB
- schedule: 23 stable events grouped into 8 atomic checker transitions
- replicas: independent `original` and `replay` states, both updated through the
  pure `Apply(state, batch)` function
- fault paths: delay+duplicate+reorder composition, loss, asymmetric
  partition/heal, early delivery attempt, source-generation reconnect race,
  same-time timeout ordering, terminal queue classification

## Main result

```text
docker compose run --rm apalache --features=no-rows typecheck model.tla
Type checker [OK]

docker compose run --rm apalache --features=no-rows check \
  --config=DeterministicNetwork.cfg --length=8 model.tla
The outcome is: NoError
Checker reports no error up to computation length 8
Total time: 188.81 sec
EXITCODE: OK
```

The main check validates replay state equality, no partition/early/stale
delivery, duplicate bounds, per-event oracle accounting, reorder observation,
asymmetric link behavior, and terminal convergence.

## Mutation sensitivity

The following intentionally unsafe configurations produced the expected
counterexamples (`EXITCODE: ERROR (12)`):

| Mutation | Bound | Violated invariant | Time |
| --- | ---: | --- | ---: |
| `UnsafeEarly.cfg` | 2 | `NoEarlyDelivery` | 10.55 s |
| `UnsafePartition.cfg` | 3 | `NoPartitionDelivery` | 15.24 s |
| `UnsafeStale.cfg` | 5 | `NoStaleGenerationDelivery` | 31.72 s |

These negative checks prevent constant flags or unreachable guards from being
reported as meaningful safety evidence.

## Executable refinement

The normative mapping is `catalog.yaml`. The local engine tests exercise seed
replay, actual overtaking order, compound delay+duplicate injection,
partition-before-queue/heal delivery, and source/target reconnect generations.
The publish-disabled harness expands seed `0x0000000000000603` into the recorded
fault schedule, starts three real OS worker processes, creates two real
single-member Raft groups in each worker, routes encoded Raft frames through
`MultiRaftManager::route_frame`, and records an oracle batch after every network
event. Distinct sentinel proposals and worker-owned WAL/snapshot observations
check group-state and storage-path isolation. The passing artifact was replayed
in a fresh process/storage set with an identical trace digest:

```text
run artifact: docs/release/evidence/v0.6.0/multi-raft-fault-v2.json
trace digest: 59d426e6242072fed665345dee54aea7f3899d648259d7065962a0f7d24599f1
failure: null
fresh replay: identical
```

The controlled failure is attached to an actually delivered duplicate frame,
not merely to its scheduled event. Fresh-process minimization reduces the seed
expanded schedule to one duplicate rule with one effect, preserves the failure
signature, records the minimized reproduction digest, and verifies 1-minimality:

```text
controlled artifact: docs/release/evidence/v0.6.0/multi-raft-controlled-failure-v2.json
trace digest: a327d10969ad3d8d09943b2ab23d38c1f8d2501c60ec6221acbd3d9848b16953
failure: injected_duplicate_delivery_oracle
minimized reproduction digest: f374b9bf78f7db0ba47b8120a1301e785f083be2c3765220269a1c28facbe4bc
fresh replay and 1-minimality: pass
```

Exact run and replay commands are recorded in `docs/release/v0.6.0.md`.

JSON round-trip and minimizer 1-minimality are executable contracts, not TLA+
network-state properties. They are verified by the harness replay path and
`tools/chirps-deterministic-harness/tests/replay.rs`.

## Exclusions

This is not proof of unbounded liveness, three-node Raft consensus, SWIM
convergence, kernel scheduling, real UDP/QUIC packet loss, multi-OS behavior, or
physical performance. The deterministic backend injects application-frame
faults. Issue #3's full simulated Tokio timer/timeout, real SWIM/QUIC backend,
and reconnecting multi-process scope remains BLOCKED; the PASS verdict above is
limited to this Multi-Raft specification's Requirement 5.3.
