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
