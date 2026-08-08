# Chirps v0.6 Multi-Raft performance procedure

Status: PROCEDURE LOCKED — CONTROLLED NATIVE RUN COMPLETE; TIKV-COMPATIBLE CONTRACT ADDED

This document fixes the measurement contract before any v0.6 performance
claim. v0.5.2 FileTransfer evidence is unrelated and must not be reused.

## Workload and topology

- 3 logical Chirps nodes in three isolated processes or containers on one
  controlled Linux/WSL host, release profile, identical binary and
  configuration. Physical hosts are optional deployment evidence, not a
  product-performance prerequisite.
- 100 Raft groups. Every group has the same 3 voters; group IDs are the
  canonical range `1..=100`.
- Exactly 1,024 payload bytes per proposal, generated once before measurement.
- Symmetric 1.0 ms RTT between every node pair. Apply the delay at the shared
  loopback/container network boundary (`tc netem` or an equivalent recorded
  deterministic shaper), measure the unloaded and shaped p50/p95 RTT, and
  preserve the shaper configuration.
- 300 in-flight client operations total: one closed-loop client per group on
  each node. A client submits its next proposal only after the prior commit
  acknowledgement or timeout.
- 15 s warm-up, 60 s measured interval, 5 s drain. Run five independent samples
  for both Multi-Raft and single-group baseline, alternating their order.
- Single-group baseline uses the same 3 nodes, payload, 300 clients, RTT,
  duration, binary, and storage configuration. Assign 100 clients on each node
  to group 1, so the client count remains 300 while only the group count changes.
- Each `GroupHandle` admits at most eight concurrent proposals. This is a
  correctness/backpressure contract: without it, a 300-client single-group
  flood can starve heartbeats and cause leader churn. The regression is covered
  by `three_voters_survive_baseline_level_single_group_concurrency` before any
  controlled performance run.
- The controlled harness uses a bounded 32-frame dispatch FIFO per group and a
  4,096-entry QUIC send queue. The previous 65,536-entry default had a
  theoretical multi-gigabyte envelope at the 64 KiB frame limit and was not a
  defensible memory-bounded performance configuration.
- The proposal-throughput lane uses `snapshot_threshold=10,000` and keeps at
  least one quarter of that log window. This deliberately keeps automatic
  snapshot/compaction out of the 60 s proposal measurement. Snapshot creation,
  installation, restart, and recovery remain separate storage tests; a run
  using the old `512` threshold exposed `Read Snapshot(None): snapshot not
  found` server errors and is not valid throughput evidence.

The runner bootstraps each group on node 1 and joins nodes 2 and 3 sequentially
as learners before promoting the common voter set. Initializing three
independent single-member groups is invalid. The implementation uses the public
production seams for:

1. creation and publication of an uninitialized replica on nodes 2 and 3;
2. lifecycle-safe learner addition, catch-up, and promotion to the common
   voter set `{1, 2, 3}`; and
3. inbound dispatch which distinguishes requests from responses and delivers a
   response to the group transport's pending correlation instead of treating it
   as a new request.

The executable controlled-local implementation is
`scripts/perf/run-controlled-container-multi-raft.sh`. It starts one
`QuicBackend` per node container, routes inbound Raft request/response frames
through `MultiRaftManager::dispatch_frame`, and uses
`create_group_uninitialized`, `add_learner`, and `change_membership` for the
sequential bootstrap. Absence of three physical hosts is not a blocker and must
not be reported as one.

## Host and process controls

- Record CPU model, physical/logical core count, RAM, kernel, Rust version,
  filesystem, storage device, execution class (`native`, `container`, or
  `wsl`), namespace/container IDs, network shaper, governor, and whether swap
  changed during the run. Record NIC/link/IRQ/RSS only for optional physical
  deployment evidence; these fields are not required for loopback evidence.
- Pin each Chirps process, its load generator, and the controller to declared,
  non-overlapping CPU sets. The 12-CPU default reserves two CPUs per node and
  one each for its load generator and the controller (`0-1/2`, `3-4/5`,
  `6-7/8`, `9`). The runner rejects overlapping or malformed sets before
  starting Docker. Record the exact affinity; do not silently reduce the
  available cores between baseline and Multi-Raft.
- Start from empty WAL/snapshot directories for every sample. Keep fsync and
  snapshot settings identical. Record the resolved configuration, not only CLI
  defaults.
- Sample process CPU, RSS, disk bytes/fsync, network bytes, retransmits, and
  per-group queue depth once per second. Preserve raw samples.
- Detailed dispatch-budget, response-send, error-classification, and reason
  counters are opt-in (`--resource-audit`/metrics output) and are represented
  as optional schema fields with defaults. Removing the audit flag removes the
  sampler and counters from the normal path; no correctness or admission
  behavior depends on the diagnostics.
- The same detachable metrics stream records `leader_by_group` when audit is
  enabled. This is specifically for diagnosing proposal timeouts versus leader
  churn; old raw artifacts remain readable because the field defaults to an
  empty map.

The controlled container profile uses a fresh 8 GiB `tmpfs` at `/work` for each
node and sample. `fsync_calls` is the count of completed WAL durability barriers
(`sync_all`) in the Chirps sink. With `fsync_interval=0`, one Raft append batch
has one barrier; interval mode adds barriers at configured boundaries. It is
operational evidence, not a claim about when a physical device persisted the
bytes. `/proc/self/io` disk bytes may therefore legitimately be zero on this
profile and are still preserved verbatim.

## Controlled-local execution

Run only from a clean committed revision; the script builds from `git archive`
and rejects an image whose revision label does not match the source SHA.

```bash
scripts/perf/run-controlled-container-multi-raft.sh \
  --output /absolute/empty/evidence-directory
```

The run needs Docker Compose, OpenSSL, `jq`, `tc`, seven disjoint available CPU
IDs (defaults use node sets `0-2`, `4-6`, `8-10` and load-generator CPUs
`3`, `7`, `11`), and enough RAM for three 8 GiB container limits. It runs
ten fresh-storage samples in the fixed order
`M0,S0,S1,M1,M2,S2,S3,M3,M4,S4`. Each sample preserves three node JSONL metric
streams, three exact load-generator histograms, container/network inspection,
the six-pair unloaded and shaped network RTT probes (200 ICMP samples per
direction), qdisc state, and post-drain
membership/digest observations. The final `assemble` command recomputes all
statistics and the final `verify` command re-reads and hashes every raw input.

This implementation does not create release evidence merely by existing. The
canonical release path is populated only after the full controlled run succeeds
and its artifact is independently reviewed.

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
6. No sample is invalidated by OOM, swap growth, process restart, network-shaper
   mismatch, or an RTT p95 outside `1.0 ms ± 0.2 ms`.

Use a deterministic bootstrap resample of the five sample-level throughput
values (fixed statistics seed `0x0000000000000600`, 10,000 resamples) for the
confidence interval. Do not treat individual proposal latencies as independent
throughput samples.

## TiKV-compatible comparison lane

The native Chirps gate above remains the product-facing Multi-Raft contract:
it counts only 1 KiB proposals that receive a quorum commit acknowledgement.
It must not be relabeled as YCSB OPS.

For cross-project comparison, the performance tool fixes a TiKV/YCSB Workload A
contract: 50% READ / 50% UPDATE, RawKV semantics, 10 million records, at least
30 million operations, and 1 KiB values. The comparison metrics are READ OPS,
UPDATE OPS, total OPS, average/min/max latency, and p99/p99.9/p99.99 latency.
The contract is represented by `TikvWorkloadA::reference()` and validated by
unit tests in `tools/chirps-multi-raft-perf/src/tikv.rs`.

This lane is comparison evidence, not a replacement for the native committed-
proposal gate. A READ is not a Raft proposal, and UPDATE OPS is not automatically
equivalent to committed proposals/s. A future RawKV-compatible service runner
must implement the same read/update semantics before its measurements are used
for a numerical cross-project claim.

Reference material:

- TiKV Benchmark Instructions: <https://tikv.org/docs/6.5/deploy/performance/instructions/>
- TiKV 3-node performance overview: <https://tikv.org/docs/6.1/deploy/performance/overview/>
- TiKV Multi-Raft design and WriteBatch batching: <https://www.pingcap.com/blog/design-and-implementation-of-multi-raft/>
- TiKV architecture and three-peer replication: <https://docs.pingcap.com/tidb/v8.1/tikv-overview/>

## Required artifact schema

The release artifact path is
`docs/release/evidence/v0.6.0/multi-raft-performance.json` and must contain:

```json
{
  "schema": "chirps.multi-raft-performance/v1",
  "commit_sha": "40-hex",
  "binary_sha256": "64-hex",
  "runner_command": ["argv", "without", "shell-expansion"],
  "execution_environment": {"class": "container", "host_count": 1, "logical_nodes": 3, "process_or_container_ids": ["..."], "node_cpu_sets": {"1": "0", "2": "2", "3": "4"}, "loadgen_cpu_sets": {"1": "1", "2": "3", "3": "5"}, "cpu": "...", "cores": 0, "ram_bytes": 0, "kernel": "...", "rust_version": "...", "storage": "...", "filesystem": "tmpfs", "network_shaper": "...", "governor": "...", "physical_deployment": false, "swap_bytes_before": 0, "swap_bytes_after": 0},
  "resolved_config": {"nodes": 3, "groups": 100, "payload_bytes": 1024, "rtt_ms": 1.0, "clients": 300, "clients_per_node": 100, "warmup_seconds": 15, "measure_seconds": 60, "drain_seconds": 5, "samples": 5, "fsync_interval": 0, "snapshot_threshold": 10000, "send_queue_capacity": 4096},
  "samples": [{"mode": "multi_raft", "index": 0, "group_count": 100, "clients": 300, "process_or_container_ids": ["..."], "actual_measure_duration_ms": 60000, "monotonic_start_ns": 0, "monotonic_end_ns": 0, "network_rtt_ms": [{"source": 1, "destination": 2, "unloaded": {"p50": 0.0, "p95": 0.0}, "shaped": {"p50": 0.0, "p95": 0.0}}], "group_membership_after_drain": [{"group_id": 1, "replicas": [{"node_id": 1, "voters": [1, 2, 3], "leader_id": 1, "last_applied": 0, "committed_digest": "64-hex"}]}], "committed": 0, "throughput_per_sec": 0.0, "latency_ms": {"p50": 0.0, "p95": 0.0, "p99": 0.0}, "errors": 0, "timeouts": 0, "cpu_seconds": 0.0, "peak_rss_bytes": 0, "disk_bytes": 0, "fsync_calls": 0, "network_bytes": 0, "oom_killed": false, "process_restarted": false, "shaper_mismatch": false}],
  "per_group": [{"mode": "multi_raft", "sample_index": 0, "group_id": 1, "committed": 0, "throughput_per_sec": 0.0}],
  "raw_metrics_artifacts": [{"kind": "node_metrics_jsonl", "path": "relative/path", "sha256": "64-hex"}],
  "raw_artifact_set_sha256": "64-hex",
  "statistics": {"seed": "0x0000000000000600", "resamples": 10000, "multi_raft_median": 0.0, "multi_raft_ci95_lower": 0.0, "single_group_median": 0.0, "overhead_ratio": 0.0},
  "verdict": {"throughput": "pass|fail", "overhead": "pass|fail", "integrity": "pass|fail", "overall": "pass|fail"}
}
```

The verifier must reject an artifact unless every Multi-Raft sample covers
exactly groups `1..=100`, every baseline sample covers only group 1 with 300
clients, and every per-group row belongs to an existing `(mode, sample_index)`.
For every sample independently, it must verify a monotonic measured interval of
at least 60,000 ms, all six directed node-pair RTT observations, identical voter
set `{1, 2, 3}` on every replica, exactly one leader per group, and matching
commit position/digest after drain. It must also verify each raw file digest and
the aggregate raw artifact-set digest.

Absolute temporary paths and wall-clock timestamps are metadata, not identity.
The artifact identity is commit SHA, binary digest, resolved configuration,
ordered samples, raw artifact digests, statistics seed, and verdict.

## Current evidence status

The controlled local native run was executed with three logical nodes in three
containers on one host. Its artifact and result summary are recorded under
`docs/release/evidence/v0.6.0/multi-raft-native-2026-08-07/`. Physical host
availability was not used as a prerequisite. The native result is a measured
engineering result, not a claim that the 100,000/s release target passed.

The TiKV-compatible contract and validation tests are present, but no
RawKV/YCSB-compatible read/update service runner has been claimed or used as a
release gate. This distinction is intentional: the current Chirps runner
counts committed proposals, while TiKV reports YCSB operation classes.
