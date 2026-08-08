# v0.6.0 proposal-pipeline and audit-harness measurement

Date: 2026-08-08 (JST)  
Revision: `d9449b9` (configurable per-group proposal pipeline plus complete
zero-depth queue telemetry). Environment: one WSL2 host, three logical Docker
nodes, fixed CPU sets, 250 us `tc netem`, 1 KiB payload, 100 groups, 300
clients, resource audit enabled.

| mode | sample | committed/s | errors | timeouts |
|---|---:|---:|---:|---:|
| Multi-Raft | 0 | 689.1833 | 0 | 0 |
| Multi-Raft | 1 | 688.7833 | 0 | 0 |
| Multi-Raft | 2 | 690.2833 | 0 | 0 |
| Multi-Raft | 3 | 685.6667 | 0 | 0 |
| Multi-Raft | 4 | 688.1500 | 0 | 0 |
| single-group | 0 | 222.2833 | 0 | 0 |
| single-group | 1 | 218.1000 | 0 | 0 |
| single-group | 2 | 214.8333 | 0 | 0 |
| single-group | 3 | 215.9833 | 0 | 0 |
| single-group | 4 | 214.9167 | 0 | 0 |

Multi-Raft median: **688.7833 committed proposals/s**.  Single-group median:
**215.9833 committed proposals/s**.  Compared with the previous fixed gate of
8 concurrent proposals, single-group throughput rose from about 93/s to about
216/s and all ten samples had zero timeout/error.

The loadgen and queue telemetry are complete, but the release verifier rejected
the aggregate because one environmental RTT observation (single-group sample
0, directed pair 3->2) had shaped p95=1.25 ms, outside the existing 1.0 +/-
0.2 ms gate.  The gate was not weakened.  This is therefore a valid performance
diagnostic result, but not a valid release artifact.

The 100,000/s historical native gate remains unmet by a wide margin.  The run
uses Docker/WSL2 logical nodes, not physical three-node hardware, and Docker
reported unavailable cgroup swap-limit capability.

Raw evidence is retained at `/tmp/chirps-proposal64-fixed/`:

- `summaries.ndjson`: `d3778bb135425c7201f1db70919a245f37db1978d21d5aa7979a2f9b90faea99`
- `samples.json`: `2a16033cb1750b7f82383bfe426f9cbd7b4747024f1551daa8ce80c26aba188d`
- `host-facts.txt`: `99f133bc73aebed8a359f544e830a9718080094f69a9654bd03d60a6ef04f0d1`
- `artifact-input.json`: `5b9c7ddd1b45f4a765d8f3971ae004b4a41ebb505a24cdb84cf0dd32a203d36a`

## Decision

`RELEASE BLOCKED`: the implementation bottleneck improved and all loadgen
correctness counters passed, but the RTT evidence gate and throughput gate did
not pass. No tag, push, or release was created.
