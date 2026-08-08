# Harness audit

- The normal node path does not sample resource counters. Detailed metrics are
  enabled only by `--resource-audit` and optional metrics interval arguments.
- Schema additions (`server_errors`, `transport_errors`, reason histogram,
  response-send counters, dispatch-budget counters) are optional/defaulted, so
  old artifacts remain readable and the diagnostics can be removed without
  changing proposal admission or correctness behavior.
- The dispatch budget is payload-byte based (32 MiB aggregate) rather than an
  unbounded frame-count queue. Response dispatch is bounded and uses the
  transport-aligned send concurrency of 64.
- The initial 30 s per-group bootstrap readiness timeout was too short for the
  constrained host; commit `95cd320` raises only this harness readiness timeout
  to 120 s. The measured warm-up (15 s), measurement (60 s), and drain (5 s)
  intervals are unchanged.
- The full run retained raw per-node JSONL, phase snapshots, load-generator
  histograms, membership/digest observations, and host facts. Its final verdict
  is invalid because swap grew; it must not be presented as release evidence.
- Snapshot-specific storage tests are separate from the proposal lane and ran
  successfully: 20 unit tests plus 4 resilience tests in
  `alopex-chirps-raft-storage`.
- The response-permit regression is now covered by
  `node::tests::response_dispatch_reuses_the_pump_permit` (21 performance-tool
  unit tests total, commit `016884e`).
- The release/CI gates now enable `multi-raft` through `--all-features` and run
  the heavy in-memory Raft integration tests with `--test-threads=1`; a prior
  no-feature invocation silently ran zero 3-voter tests, while concurrent host
  scheduling produced false timeout/leader-churn failures.
