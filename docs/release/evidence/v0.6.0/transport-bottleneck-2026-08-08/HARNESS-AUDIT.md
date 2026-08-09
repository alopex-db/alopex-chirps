# Harness and transport audit

The initial failure was not treated as a performance-only defect.  Unit-level
coverage now fixes the two observed causes:

1. `ClientSession` resolves the Raft leader before its first proposal instead
   of sending to node 1 while `current_leader` is still unknown.
2. QUIC `SendStream::stopped()` waiting is configurable.  It remains enabled
   by default (protecting single-group backpressure) and is disabled only by
   the controlled Multi-Raft fanout command-line flag.  Handshake streams
   still wait for peer stop.

The transport crate has explicit tests for both configuration states; the
targeted run passed 2/2.  The Multi-Raft performance harness unit suite had
already passed 25/25 before this measurement, and the transport/integration
suite passed 26 transport tests plus its integration tests.

The optional `--resource-audit` sampler and `leader_by_group` telemetry remain
detachable: the default workload/schema is unchanged, while the audit run
emits phase RSS, CPU, queue, and leader evidence only when requested.

The remaining single-group timeout is not hidden by relaxing the verifier.  It
must be addressed by a separate single-group tail-latency/backpressure change
or by revisiting the workload contract; the current evidence is insufficient
to claim that change is safe.
