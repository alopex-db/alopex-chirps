# Chirps v0.6 Multi-Raft performance procedure

Status: PROCEDURE LOCKED — PHYSICAL MEASUREMENT PENDING

This document fixes the measurement contract before any v0.6 performance
claim. v0.5.2 FileTransfer evidence is unrelated and must not be reused.

## Workload and topology

- 3 physical Chirps nodes, one process per host, release profile, identical
  binary and configuration, wired L2 network.
- 100 Raft groups. Every group has the same 3 voters; group IDs are the
  canonical range `1..=100`.
- Exactly 1,024 payload bytes per proposal, generated once before measurement.
- Symmetric 1.0 ms RTT between every node pair. Measure the unloaded physical
  RTT first; add deterministic `tc netem` delay only when needed and record the
  resulting p50/p95 RTT.
- 300 in-flight client operations total: one closed-loop client per group on
  each node. A client submits its next proposal only after the prior commit
  acknowledgement or timeout.
- 15 s warm-up, 60 s measured interval, 5 s drain. Run five independent samples
  for both Multi-Raft and single-group baseline, alternating their order.
- Single-group baseline uses the same 3 nodes, payload, 300 clients, RTT,
  duration, binary, and storage configuration. Only the group count changes.

The runner must bootstrap each group on one node and join the other two as
learners/voters; initializing three independent single-member groups is invalid.
Until the repository has an executable three-node bootstrap/join runner, the
physical gate is BLOCKED rather than skipped.

## Host and process controls

- Record CPU model, physical/logical core count, RAM, kernel, Rust version,
  filesystem, storage device, NIC/link speed, IRQ/RSS configuration, governor,
  container/VM/WSL status, and whether swap changed during the run.
- Pin each Chirps process and its load generator to declared, non-overlapping
  CPU sets. Record the exact affinity; do not silently reduce the available
  cores between baseline and Multi-Raft.
- Start from empty WAL/snapshot directories for every sample. Keep fsync and
  snapshot settings identical. Record the resolved configuration, not only CLI
  defaults.
- Sample process CPU, RSS, disk bytes/fsync, network bytes, retransmits, and
  per-group queue depth once per second. Preserve raw samples.

## Statistics and gates

For each 60 s sample, count proposals only after the client receives a commit
acknowledgement. Report aggregate proposals/s, per-group proposals/s, p50/p95/p99
latency, errors, timeouts, CPU-seconds, peak RSS, disk bytes, and network bytes.

The v0.6 performance claim passes only when all conditions hold:

1. Five valid Multi-Raft samples and five matching baseline samples exist.
2. Multi-Raft median aggregate throughput is at least 100,000 committed
   proposals/s and the bootstrap 95% confidence-interval lower bound is also at
   least 100,000 proposals/s.
3. `1 - multi_raft_median / single_group_median < 0.10`; negative overhead is
   reported as measured and not clamped.
4. There are zero proposal errors, timeouts, divergent committed values, or
   missing groups.
5. Every group makes progress; the slowest group's throughput is at least 50%
   of the per-group median.
6. No sample is invalidated by OOM, swap growth, process restart, link-rate
   mismatch, or an RTT p95 outside `1.0 ms ± 0.2 ms`.

Use a deterministic bootstrap resample of the five sample-level throughput
values (fixed statistics seed `0x0000000000000600`, 10,000 resamples) for the
confidence interval. Do not treat individual proposal latencies as independent
throughput samples.

## Required artifact schema

The release artifact path is
`docs/release/evidence/v0.6.0/multi-raft-performance.json` and must contain:

```json
{
  "schema": "chirps.multi-raft-performance/v1",
  "commit_sha": "40-hex",
  "binary_sha256": "64-hex",
  "runner_command": ["argv", "without", "shell-expansion"],
  "hosts": [{"name": "...", "cpu": "...", "cores": 0, "ram_bytes": 0, "kernel": "...", "storage": "...", "nic": "..."}],
  "resolved_config": {"nodes": 3, "groups": 100, "payload_bytes": 1024, "rtt_ms": 1.0, "clients": 300, "warmup_seconds": 15, "measure_seconds": 60, "samples": 5},
  "samples": [{"mode": "multi_raft", "index": 0, "committed": 0, "throughput_per_sec": 0.0, "latency_ms": {"p50": 0.0, "p95": 0.0, "p99": 0.0}, "errors": 0, "timeouts": 0, "cpu_seconds": 0.0, "peak_rss_bytes": 0}],
  "per_group": [{"group_id": 1, "committed": 0, "throughput_per_sec": 0.0}],
  "raw_metrics_artifacts": ["relative/path"],
  "statistics": {"seed": "0x0000000000000600", "resamples": 10000, "multi_raft_median": 0.0, "multi_raft_ci95_lower": 0.0, "single_group_median": 0.0, "overhead_ratio": 0.0},
  "verdict": {"throughput": "pass|fail", "overhead": "pass|fail", "integrity": "pass|fail", "overall": "pass|fail"}
}
```

Absolute temporary paths and wall-clock timestamps are metadata, not identity.
The artifact identity is commit SHA, binary digest, resolved configuration,
ordered samples, raw artifact digests, statistics seed, and verdict.

## Current evidence status

No physical throughput or overhead claim has been made. The deterministic
harness evidence in `docs/release/evidence/v0.6.0/multi-raft-fault-v2.json` proves
local replay/lifecycle/routing/isolation behavior only and cannot satisfy this
performance gate.
