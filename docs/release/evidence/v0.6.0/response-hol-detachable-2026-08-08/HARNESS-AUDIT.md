# Harness audit

- Detailed resource/phase instrumentation is opt-in and detachable: only `--resource-audit` enables 250 ms phase snapshots, RSS sampling, and per-node metric files.
- The default node path does not construct the sampler or audit maps, so the diagnostic code is not part of ordinary throughput runs.
- Dispatch admission is bounded (`4096` global slots, bounded per-group queues); response admission is separately bounded (`4096` slots).
- The fixed-order run recorded host facts, phase metrics, per-sample summaries, membership, node metrics, network RTT, and container inspection data under this directory.
- The WSL2 Docker kernel reported unavailable swap-limit cgroups; memory limits were applied without swap. This limits portability of RSS results.
- The harness verifier correctly failed on non-zero errors/timeouts. No failed measurement was relabeled as a pass.

The raw sample directories are retained with this evidence so the diagnostics can be removed from the runtime binary without losing the release audit trail.
