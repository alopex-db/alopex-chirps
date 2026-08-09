# Full phase and unit-level resource audit (2026-08-07)

## Coverage

Revision `b13fba2` records every control-run boundary in
`phase-metrics.ndjson`. The fixed order contains 10 samples and each sample
has exactly 13 records:

`sample-start`, `nodes-started`, `health-ready`, `rtt-unloaded`,
`network-shaped`, `rtt-shaped`, `bootstrap-complete`, `loadgen-start`,
`loadgen-complete`, `drain-complete`, `resource-inspected`,
`summary-complete`, `sample-end`.

This is 130/130 phase records. Each record includes runner RSS/CPU and Docker
node memory/CPU snapshots. The 30 loadgen reports were reduced to
`loadgen-phase-summary.tsv`; every row contains RSS for `warmup`, `measure`,
and `drain`, plus commit/error/timeout counters.

Node metrics were sampled every 250 ms. The measurement-window counts are
239–240 per node per sample (the old 1-second interval produced an occasional
59-point window and correctly failed the gate). No coverage threshold was
relaxed.

## Unit-level resource contracts

The harness UT suite is 14/14 PASS. The added tests cover:

- explicit warmup/measure/drain phase boundaries;
- RSS probe availability;
- fixed 100-client payload working set (100 × 1024 bytes);
- bounded group queue payload budget (32 × 1024 bytes).

These are deterministic local contracts; they are not substitutes for the
container measurement.

## Measurement result

| item | result |
|---|---:|
| Multi-Raft throughput median | 296.72 committed proposals/s |
| Multi-Raft timeout total | 13,493 |
| Maximum node RSS | 2,178,207,744 B (2.03 GiB) |
| Maximum loadgen RSS | 5,271,552 B (5.03 MiB) |
| Node metric coverage | 239–240 points/node/60 s window |
| Phase records | 130/130 |

The run still fails the existing timeout verifier. The result is diagnostic
evidence only and does not support release. The full per-node measurement
summary is in `node-measurement-summary.tsv`; the complete phase boundary
stream and lossless phase/counter summary are retained beside this file.
Latency histograms and membership digests remain in the original runner output
under `/tmp` only and are not used for this resource verdict.
