# Chirps v0.6 TSO, HLC, and snapshot performance procedure

Status: PROCEDURE LOCKED — CONTROLLED LOCAL MEASUREMENT PENDING

This contract fixes the workloads and artifact identity before implementation
or measurement. It deliberately uses one controlled Linux/WSL host with three
logical Chirps nodes. Three physical hosts are not a prerequisite. Optional
multi-host results are deployment evidence and do not replace these profiles.

Every artifact is bound to a 40-hex source commit, the SHA-256 of the measured
release binary, the resolved configuration, the complete ordered raw-sample
set, and the SHA-256 of every referenced raw file. A verifier must reject a
missing raw file, digest mismatch, shortened interval, changed workload, an
unauthenticated TSO request, or a result produced from a dirty source tree.

## TSO profile `tso-v06-local-v1`

- Three logical nodes in separate processes or containers on one host, with a
  dedicated three-voter TSO Raft group and node authentication enabled.
- Symmetric 1.0 ms RTT (`1.0 ms +/- 0.2 ms` p95) between every directed node
  pair. Preserve all six unloaded and shaped RTT observations.
- Empty durable state at the start of each sample. Bootstrap once, identify the
  leader through the public client path, and record the voter set and committed
  allocation floor after drain.
- Batch size 10,000, prefetch threshold 1,000, timestamp TTL 3 seconds. Use 300
  closed-loop clients split evenly across the three load-generator processes.
- Alternate five 15 s warm-up / 60 s measure / 5 s drain samples with cache
  enabled and five samples with batch size one (network-roundtrip reference).
- Count a timestamp only after the client has returned it. Reject duplicates,
  non-monotonic client sequences, overlapping committed ranges, unauthenticated
  issuance, or a timestamp outside a committed allocation.
- In a separate deterministic handoff sample, stop the leader, retain the old
  lease deadline, and require authenticated issuance to resume in under three
  seconds without any issuance before that deadline.

The gate requires zero correctness errors/timeouts in steady-state samples,
cache-enabled median throughput and its deterministic bootstrap 95% confidence
interval lower bound of at least 100,000 timestamps/s, cache-hit p99 below 1
ms, and batch-one network-roundtrip p99 below 5 ms. Bootstrap statistics use
seed `0x0000000000000620` and 10,000 sample-level resamples.

The release artifact is
`docs/release/evidence/v0.6.0/tso-performance.json` with schema
`chirps.tso-performance/v1`. It contains `commit_sha`, `binary_sha256`,
`runner_command`, host/process/CPU/RAM/kernel/storage/network-shaper metadata,
the resolved profile above, ten ordered steady-state samples, the handoff
sample, per-client monotonicity summaries, committed-range digests, latency
histograms, CPU/RSS/disk/network raw samples, raw-file digests, statistics, and
an explicit per-gate verdict.

## HLC profile `hlc-v06-local-v1`

- Run a Criterion benchmark against the public `LocalHlc::tick` path using an
  injected monotonic wall-clock source so clock syscalls are measured
  separately from the HLC state transition. Record both measurements.
- Pin one release-mode benchmark process to one declared physical core, disable
  frequency migration where the host permits it, perform at least 10 warm-up
  seconds and 30 measured seconds, and retain Criterion raw estimates.
- Exercise physical advance, same-physical logical advance, physical rollback,
  accepted receive, stale/reordered receive, duplicate receive, and rejected
  future-skew paths in separate distributions. The functional test corpus must
  prove monotonicity and rejection without mutation before timings are valid.
- Run five independent samples. The gate is the median per-sample p50 for the
  injected-clock `tick` path below 100 ns. Report p95/p99 and clock-source cost
  but do not substitute them for the specified gate.

The release artifact is
`docs/release/evidence/v0.6.0/hlc-performance.json` with schema
`chirps.hlc-performance/v1`. It includes the common identity/host fields,
benchmark command, clock-source definition, five sample estimates for every
path, functional-test digest, Criterion raw-file digests, and verdict.

## Snapshot profile `snapshot-v06-local-v1`

- Three logical nodes in separate processes or containers on one host and one
  three-voter Raft group. Transfer a deterministic 1 GiB snapshot from leader
  to a caught-up learner replacement using 1 MiB chunks, threshold 10 MiB,
  four concurrent chunks, and a 60 s transfer timeout.
- Shape the sender/receiver path to a recorded 1 Gbit/s rate. RTT is reported
  but is not silently inherited from the Multi-Raft profile. Start every sample
  from empty receiver snapshot state.
- Alternate five optimized samples with five single-stream whole-snapshot
  reference samples. Inject one deterministic corruption/drop per optimized
  sample and require only the affected chunk to be retransmitted.
- Count completion only after every chunk checksum and whole-snapshot digest
  match, durable checkpoint completes, the snapshot is atomically installed,
  and the StateMachine reports the expected applied index and state digest.
- Record chunk attempts, retries by index, maximum observed concurrency,
  progress samples, transferred bytes, latency, throughput, CPU/RSS/disk/network
  counters, and final Raft membership/applied state.

The gate requires all five optimized samples to install exactly once with no
partial visibility, exactly one retried chunk for each injected fault, observed
concurrency between one and four and greater than one while work is available,
monotonic progress ending at 100%, and median end-to-end throughput at least
100 MB/s. A host unable to provide the declared 1 Gbit/s shaper records the
result as non-gating rather than weakening the threshold.

The release artifact is
`docs/release/evidence/v0.6.0/snapshot-performance.json` with schema
`chirps.snapshot-performance/v1`. It includes the common identity fields,
resolved snapshot and shaper configuration, ten ordered samples, per-chunk
attempt/checksum/progress records, installed state digests, raw-file digests,
and verdict.

## Current evidence status

No TSO, HLC, or snapshot performance claim is made by this document. Missing
implementation, runner, raw samples, or verifier is BLOCKED, never skipped.
Loopback evidence may satisfy these controlled profiles when all recorded host
and shaper constraints hold; physical topology is not an implicit gate.
