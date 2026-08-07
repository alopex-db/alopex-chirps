# Performance harness resource audit

## Evidence reviewed

The corrected run's committed summaries report approximately 19–23 MiB RSS
for single-group samples, while Multi-Raft samples report 0.82 GiB, 3.66 GiB,
0.20 GiB, 0.22 GiB, and 2.08 GiB. The same summaries show that the high-RSS
samples also have large timeout counts and transport bytes. This correlation is
evidence of a pressure condition, not proof of one root cause; raw per-node
RSS/queue traces were not preserved in the earlier evidence bundle.

## Findings

1. The harness created one FIFO task per active Raft group, but its channel was
   `mpsc::unbounded_channel`. A slow Raft dispatch path could therefore retain
   an arbitrary number of inbound frames, including payload bytes.
2. The controlled profile passed `send_queue_capacity=65536` (the CLI default).
   At the transport's 64 KiB maximum frame size this is a theoretical 4 GiB
   envelope per node before allocator and connection overhead. The workload's
   normal 1 KiB frames are smaller, but the configuration was not memory-bounded
   by contract.
3. The existing `per_group_queue_depth` evidence measured proposal inflight
   counters, not the dispatch FIFO or QUIC send queue. It could not explain a
   large RSS spike.
4. The load generator's 100 tasks per node, shared counter mutex, latency
   histogram, and audit SHA-256 state machine are bounded and small relative to
   the above queues. They remain workload overhead and must be included in the
   measured profile, but they do not have an unbounded collection.
5. The WAL log cache is bounded at 1,024 entries per group, but the resolved
   profile did not expose this parameter. Its per-group replication cost remains
   a separate unresolved contributor and must be recorded in the next run.

## Correction applied

- Per-group dispatch FIFO: bounded `mpsc` capacity 32 with async backpressure.
- Controlled QUIC send queue: explicit capacity 4,096, replacing the implicit
  65,536 default.
- Added a unit test that fills the dispatch queue and asserts the next send is
  rejected until the consumer drains it.

The harness unit suite is now 10/10 passing. This correction changes the
measurement configuration, so the prior performance numbers must not be reused
as evidence for the corrected profile. A new controlled run is required before
any performance or release decision.

## Bounded-profile rerun

Revision `d7afda1` was measured with both corrections active. Peak RSS fell from
`3,658,006,528 B` to `1,914,302,464 B` (about 47.7% lower), while
single-group remained near 20 MiB. The remaining Multi-Raft RSS was concentrated
on node 1: about 1.87 GiB in sample 0 and 1.42 GiB in sample 4, versus less than
1 GiB on node 2 and less than 80 MiB on node 3. This is consistent with
leader-side retention, but is not sufficient to distinguish OpenRaft pending
proposals, WAL/log cache, retransmission, or allocator retention.

The rerun still had timeouts in four of five Multi-Raft samples and all five
single-group samples; its median was 324.45/s. The full per-node metrics for the
high-RSS samples are preserved in the sibling `harness-bounded-2026-08-07`
evidence directory. The harness is therefore materially safer but not yet
validated as a non-interfering performance instrument.

## Queue and load-generator RSS audit

Revision `2011052` added explicit dispatch/retransmit/transport counters and a
100 ms sampler for each loadgen process. In the fixed 10-sample run, loadgen
peak RSS was 4.6–5.4 MiB per process (5.17 MiB maximum), whereas node RSS
reached 1.86 GiB. Retransmission buffer/count, queue overflow, and backpressure
were zero in all emitted node samples. Several high-RSS nodes had dispatch
depth only 7–13; the largest observed depth (1,268) occurred on a separate
1.43 GiB node. This rules out the loadgen and the currently measured queues as
the dominant remaining RSS consumer, without proving whether the bytes are
OpenRaft pending state, WAL/log cache, snapshots, or allocator retention.

Complete diagnostic evidence is in
`../queue-metrics-2026-08-07/RESULT.md`. The run's Multi-Raft median was
373.13/s and every fixed profile still contained timeouts, so release remains
blocked.

## Full phase and unit-level audit

The first phase-instrumented run exposed a real observation gap: one node had
59 one-second samples inside a 60-second window, so the existing verifier
stopped. The gate was not weakened. Revision `b13fba2` changed node sampling to
250 ms and reran the complete fixed order. It produced 130/130 phase boundary
records (13 per sample), 239–240 node samples per 60-second window, and
warmup/measure/drain RSS for all 30 loadgen reports. The UT suite expanded to
14/14, including deterministic payload-budget and phase-boundary contracts.

The rerun's Multi-Raft median was 296.72/s, with 13,493 timeouts and a maximum
node RSS of 2.03 GiB. Loadgen remained at 5.03 MiB maximum. Evidence is in
`../phase-audit-2026-08-07/RESULT.md`. This confirms measurement coverage, but
not performance correctness or release readiness.

## Node-side storage fix and remeasurement

The code audit found that the WAL fallback built a `BTreeMap` of the entire
WAL before applying the requested range. This was contrary to the bounded
indexed-read approach used by TiKV Raft Engine. Revision `64f62b9` now filters
entries while replaying, deduplicates cache-order indexes, and removes a full
snapshot payload clone during install. New storage tests cover range-bounded
reads and replacement-index cache order.

The fixed full run improved Multi-Raft median from 296.72/s to 408.85/s and
reduced maximum node RSS from 2.03 GiB to 1.76 GiB. Timeout failures remain,
including a zero-commit sample, so the release gate stays BLOCKED. Detailed
after-fix evidence is in `../wal-range-fix-2026-08-07/RESULT.md`.

## Diagnostic instrumentation detachability

The detailed bottleneck instrumentation is explicitly opt-in. Normal node and
load-generator paths do not start the node metrics sampler, the 100-ms RSS
sampler, or shell-level phase snapshots. The controlled diagnostic runner
enables them only with `--resource-audit`; that mode also selects the 250-ms
node interval with `--metrics-interval-ms 250`. The default interval is 1 s,
and the diagnostic tasks are behind a single runtime flag, so they can be
removed without changing proposal, transport, WAL, or snapshot behavior.

This is a release constraint: numbers without `--resource-audit` represent the
normal harness path, while phase/RSS evidence must record the audit flag. The
detachability UT/build gate passed (`chirps-multi-raft-perf`: 14/14). The
post-retention full three-node run is still required for the final performance
verdict.
