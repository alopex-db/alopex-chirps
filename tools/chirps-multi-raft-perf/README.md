# Chirps Multi-Raft controlled-local harness

This workspace-only binary implements the locked procedure in
`docs/perf/v0_6_multi_raft.md`. It is deliberately not a synthetic benchmark:
node processes use the public `QuicBackend`, manager frame dispatch, WAL-backed
Raft groups, and public learner/membership APIs.

The canonical entry point is:

```bash
scripts/perf/run-controlled-container-multi-raft.sh --output /absolute/empty/path
```

The `verify` subcommand is fail-closed. It uses strict Serde schemas, re-hashes
the canonical raw file set, validates the fixed ten-sample order and raw-file
cardinalities, checks all six directed RTT pairs and all replica digests, and
recomputes deterministic bootstrap statistics and verdicts.

Unit tests use generated fixture data solely to test verifier acceptance and
tamper rejection. Those fixtures are never performance evidence.
