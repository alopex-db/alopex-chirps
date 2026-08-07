# Retention tuning and detachable diagnostics (2026-08-07)

Revision `389bd32` applies the storage-retention tuning and makes detailed
resource instrumentation opt-in. The fixed-order container run was executed
with `--resource-audit`, so this evidence includes phase snapshots, 250 ms node
metrics, and load-generator RSS phases. Normal runs do not execute those
samplers.

## Result

| metric | result |
|---|---:|
| samples | 10/10 (5 Multi-Raft, 5 single-group) |
| phase records | 130/130 (13 per sample) |
| node metric coverage | 845–887 records per node/sample (audit interval 250 ms) |
| Multi-Raft rates | 375.92, 0, 0, 687.75, 0 /s |
| Multi-Raft median | 0/s |
| maximum node/loadgen RSS observed | 368,791,552 B |
| Multi-Raft timeout total | 11,402 |
| Multi-Raft error total | 2,015 |

The run failed the existing error/timeout gate (`loadgen report contains
errors or timeouts`). Three Multi-Raft samples committed zero proposals, and
the measured median is therefore 0/s. This is not release-ready and does not
support the former 100,000/s claim. Single-group samples remained around
73–78/s, showing that the failure is specific to the multi-group/distributed
path under this harness configuration.

The evidence is intentionally compact: `sample-summary.tsv`,
`node-measurement-summary.tsv`, `loadgen-phase-summary.tsv`, and the complete
`phase-metrics.ndjson`. The host reported that Docker swap limits are
unsupported; that environment attribute is retained in the raw run directory
and limits the scope of RSS conclusions.
