# Multi-Raft native controlled measurement — 2026-08-07

## Execution

- Source revision: `2f596e1` (`docs: add TiKV-compatible multi-raft comparison contract`)
- Runner: `scripts/perf/run-controlled-container-multi-raft.sh`
- Environment: one host, three logical Chirps containers
- Workload: 100 groups / 3 voters per group / 1,024-byte proposal / shaped RTT 1 ms
- Schedule: five Multi-Raft samples and five single-group baseline samples
- Duration: 15 s warm-up / 60 s measurement / 5 s drain per sample
- Physical hosts: none; this is controlled local evidence as permitted by the procedure
- Raw run directory: `/tmp/chirps-v06-mr-native-20260807`

## Observed values

Multi-Raft aggregate committed proposals/s:

| sample | committed | throughput/s | errors | timeouts |
|---:|---:|---:|---:|---:|
| 0 | 23,502 | 391.7 | 0 | 1,631 |
| 1 | 37,667 | 627.8 | 0 | 0 |
| 2 | 26,213 | 436.9 | 0 | 1,500 |
| 3 | 42,634 | 710.6 | 0 | 0 |
| 4 | 9,717 | 162.0 | 0 | 3,000 |

- median: **436.9 committed proposals/s**
- deterministic bootstrap 95% lower bound (seed `0x600`, 10,000 resamples): **162.0/s**
- p95 proposal latency by sample: 743.8, 964.6, 727.6, 688.0, 644.4 ms
- Multi-Raft throughput gate: **FAIL** (`436.9 < 100,000`)
- Multi-Raft CI gate: **FAIL** (`162.0 < 100,000`)
- Multi-Raft correctness samples: all had zero proposal errors; timeout-bearing samples are invalid for the release gate

Single-group baseline:

- all five samples recorded 3,600 timeouts and zero committed proposals;
- baseline leader/replica state did not converge in the final sample;
- baseline median is therefore 0/s and Multi-Raft overhead is **not computable**.

The runner completed all ten load phases, but final artifact assembly rejected
the run. The run also exposed a runner bug in the final container-ID jq
projection; that projection was fixed in the follow-up commit. Even with that
mechanical fix, the baseline remains invalid because all five baseline samples
timed out. This is a measurement failure, not a performance pass.

## TiKV-compatible lane

The TiKV/YCSB Workload A contract and unit tests were added in
`tools/chirps-multi-raft-perf/src/tikv.rs`. This run did not claim a RawKV/YCSB
measurement: the existing service runner counts quorum-committed proposals and
does not implement TiKV's READ/UPDATE operation classes. The TiKV-compatible
lane is therefore contract-ready but measurement-pending.

## Release decision

`v0.6.0` is **not release-ready** from this evidence set. The blocking facts
are the failed native throughput/CI gates and an invalid single-group baseline,
not the absence of physical three-node hardware. Before release, fix or
reproduce the baseline path, rerun the full ten-sample native profile, and then
run a true RawKV/YCSB-compatible comparison if a cross-project numeric claim is
desired.
