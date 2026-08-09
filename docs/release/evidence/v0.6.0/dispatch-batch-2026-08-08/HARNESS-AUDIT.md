# Raft dispatch batching harness audit

The diagnostic change batches only the detachable harness-side queue drains:
the outer per-group FIFO drains up to 32 pending frames into its bounded
channel, and the receiver processes up to 32 already-enqueued items per wake.
No transport, Raft ordering, lifecycle, or durability contract changes.

The queue unit test verifies FIFO order, full depth accounting, and completion
of 100 items across a batch boundary. The full `chirps-multi-raft-perf` library
suite passed 28 tests (loopback tests required the approved elevated run).

The raw run has zero dispatch-budget waits and a maximum observed dispatch depth
of 28, so the queue is not saturated in this profile. The small throughput
difference is therefore recorded as a diagnostic result rather than attributed
causally to the batching change.
