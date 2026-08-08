# Proposal pipeline and detachable audit changes

## Production change

`RaftConfig::proposal_concurrency` is now explicit and defaults to 64 instead
of an unconfigurable per-group semaphore size of 8. `GroupHandle` uses the
setting with bounded admission and normalizes zero to one. This preserves
heartbeat/election protection while allowing a single group to pipeline
client writes, matching the batching/pipelining shape used by reference
Raftstore implementations.

## Harness change

The resource sampler previously emitted `per_group_queue_depth` only after a
group had received an inbound frame. During a valid measurement interval this
could produce an empty map, causing verifier failure even though the workload
was healthy. The sampler now enumerates all managed groups and emits an
explicit zero for idle groups. A unit test covers missing-group normalization;
the node harness tests passed 11/11.

The audit remains detachable: queue depth, leader map, RSS, CPU, and transport
metrics are emitted only with `--resource-audit`; default workload behavior is
unchanged.

The perf-only audit state machine also gates its per-command SHA-256 digest on
`--resource-audit`. Normal measurements retain the applied-count contract but
do not charge the benchmark for a diagnostic hash over every 1 KiB command.
The detach behavior is covered by a unit test (12 node-harness tests passed).

## Verification

In the corrected run, every measured Multi-Raft node emitted 240/240 records
with exactly 100 queue-depth keys; single-group records emitted exactly one
key. All 10 loadgen samples had `errors=0` and `timeouts=0`. The final verifier
failure was solely the shaped RTT p95 environmental gate described in
`RESULT.md`, not missing queue evidence or loadgen failure.
