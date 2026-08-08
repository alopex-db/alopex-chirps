# v0.6.0 Raft dispatch queue batching measurement

Date: 2026-08-08 (JST)  
Revision: `3148d53` (`perf: batch raft dispatch queue draining`)  
Environment: one WSL2 host, three logical Docker nodes, fixed CPU sets, 250 us
`tc netem`, 1 KiB payload, 100 groups, 300 clients, resource audit enabled.

| mode | sample | committed/s | errors | timeouts |
|---|---:|---:|---:|---:|
| Multi-Raft | 0 | 711.4667 | 0 | 0 |
| Multi-Raft | 1 | 715.5500 | 0 | 0 |
| Multi-Raft | 2 | 713.1000 | 0 | 0 |
| Multi-Raft | 3 | 733.3500 | 0 | 0 |
| Multi-Raft | 4 | 718.4667 | 0 | 0 |
| single-group | 0 | 219.6167 | 0 | 0 |
| single-group | 1 | 216.9833 | 0 | 0 |
| single-group | 2 | 217.0167 | 0 | 0 |
| single-group | 3 | 217.0167 | 0 | 0 |
| single-group | 4 | 221.1667 | 0 | 0 |

Exact five-point medians are **715.5500/s** for Multi-Raft and **217.0167/s**
for single-group. The prior dirty-WAL run's corresponding exact medians are
712.6667/s and 215.9000/s. This is a small directional improvement, not proof
that queue draining is the dominant bottleneck.

All ten samples had `errors=0` and `timeouts=0`. The verifier rejected the run
only on the existing shaped RTT p95 gate (`1.0 +/- 0.2 ms`); that gate was not
weakened. The 100,000/s gate remains unmet and this is logical Docker/WSL2
evidence, not physical three-node evidence.

Resource comparison for Multi-Raft sample 0:

| profile | node-1 peak RSS | node-1 CPU | max dispatch depth | fsync/node |
|---|---:|---:|---:|---:|
| dirty-WAL baseline | 136.5 MiB | 151.85 s | 16 | 53--54k |
| dispatch batch | 141.9 MiB | 152.24 s | 28 | 53--55k |

The change reduces queue lock/receiver wakeups by draining up to 32 frames at a
time, while preserving FIFO order and the existing byte-budget backpressure.
The raw evidence shows no material CPU/RSS reduction; QUIC stream creation and
per-frame transport work remain the next candidate.

Raw evidence is retained at `/tmp/chirps-dispatchbatch32/`:

- `summaries.ndjson`: `90edc2ed50950cae8ca2797b7d8bf18f7d24468fe1e4fedd94579a23dfba43bb`
- `samples.json`: `d9e2d4e56e79e79ac7f3ea130b50f502e704977fa0102342439ee27d7fffb1aa`
- `host-facts.txt`: `b6262f94b9db7a071bedf02a26dfead230bc9dd77a7210acc0ce0e7469fc378f`
- `artifact-input.json`: `2ccf2c95e4c5712b84a5a368342c756a865cd95ff42a5a0fb1e7f889585afd25`

## Decision

`RELEASE BLOCKED`: dispatch batching is retained as a bounded, tested
micro-optimization, but it does not change the release gates. No tag, push, or
release was created.
