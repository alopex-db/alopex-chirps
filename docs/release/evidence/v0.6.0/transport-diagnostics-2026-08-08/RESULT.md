# Transport diagnostics detach measurement (2026-08-08)

## Scope

The controlled 3-node container harness was run with `resource_audit=false`,
which disables node samplers and QUIC detailed transport diagnostics while
retaining loadgen, RTT, membership, commit, latency, error, and timeout
evidence. This isolates the diagnostic overhead from the workload path.

The run executed all 10 prescribed samples (5 Multi-Raft, 5 Single-Group) with
60 seconds of measured traffic per sample. Final artifact assembly initially
failed because the harness jq invocation omitted the newly added flag; the
script was corrected immediately. The per-sample summaries remain intact at
`/tmp/chirps-transport-normal4/summaries.ndjson`.

## Results

| Mode | Throughput samples (committed/s) | Exact median | Errors/timeouts |
|---|---|---:|---:|
| Multi-Raft | 698.9333, 696.5500, 717.3167, 717.9000, 808.7333 | **717.3167** | 0 / 0 |
| Single-Group | 215.8167, 214.8167, 218.0667, 220.0833, 217.9833 | **217.9833** | 0 / 0 |

The no-diagnostics path therefore completed without functional errors. It is
not a resource-audit artifact: CPU, RSS, fsync, and network byte fields are
intentionally absent/zero in this mode. The throughput/latency evidence is
usable for comparing diagnostic overhead; resource claims require the paired
audit-enabled run.

## Comparison and diagnosis

The paired audit-enabled dispatch-batch run had Multi-Raft median **715.5500**
committed/s and Single-Group median **217.0167** committed/s. The diagnostic-
off run is +0.25% and +0.45%, respectively—within run-to-run noise and not a
causal bottleneck removal. The next dominant candidate remains per-frame QUIC
unidirectional stream setup (about 175k transport sends per node in the audit
run), not the detachable metrics path.

## Reproducibility

- Output: `/tmp/chirps-transport-normal4`
- Raw summary SHA256: `3426e8c69bde07812472f8badd8a49c8eb3e485f76ae2f541c1fff0b49e4125b`
- Environment: 1 host, 3 containers, fixed 1 ms shaped RTT, tmpfs, no physical
  3-node deployment; Docker reported swap-controller limitation.
- Final artifact assembly bug fixed in
  `scripts/perf/run-controlled-container-multi-raft.sh`.
