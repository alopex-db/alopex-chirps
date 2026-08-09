# QUIC transport diagnostics audit

`ExtendedTransportMetrics` previously performed a histogram lock, metrics
recorder update, and debug event for every sent and received frame regardless
of whether the caller requested resource-audit evidence. The perf node now
passes `diagnostics_enabled = args.resource_audit` into the QUIC transport.
When disabled, the detailed per-stream counters, histograms, queue-utilization
updates, retransmission diagnostics, and tracing calls return before touching
their hot-path state. The lightweight transport sent/received/drop counters
used for correctness remain enabled.

Unit coverage:

- `alopex-chirps-transport-quic`: 30 library tests, including the disabled
  diagnostics contract.
- `chirps-multi-raft-perf`: 28 library tests (loopback tests run with the
  approved elevated permission).

The audit flag is the sole runtime switch; no production protocol or ordering
contract changes. A normal run must keep the flag off, while a diagnostic run
must keep it on and preserve the resulting raw metrics.
