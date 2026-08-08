# v0.6.0 WAL durability batching measurement

Date: 2026-08-08 (JST)  
Revision: `ad96558` (shared barrier plus dirty-participant fix)  
Environment: one WSL2 host, three logical Docker nodes, fixed CPU sets, 250 us
`tc netem`, 1 KiB payload, 100 groups, 300 clients, resource audit enabled.

| mode | sample | committed/s | errors | timeouts |
|---|---:|---:|---:|---:|
| Multi-Raft | 0 | 712.6833 | 0 | 0 |
| Multi-Raft | 1 | 721.9000 | 0 | 0 |
| Multi-Raft | 2 | 718.0000 | 0 | 0 |
| Multi-Raft | 3 | 715.6667 | 0 | 0 |
| Multi-Raft | 4 | 680.0167 | 0 | 0 |
| single-group | 0 | 215.8833 | 0 | 0 |
| single-group | 1 | 212.7833 | 0 | 0 |
| single-group | 2 | 215.9000 | 0 | 0 |
| single-group | 3 | 218.0167 | 0 | 0 |

The final single-group sample was collected by the runner but was not emitted
to `summaries.ndjson` before the verifier stopped on the existing shaped RTT
gate. The nine emitted loadgen samples all had zero errors and timeouts.

Multi-Raft median of the five samples: **716.8333 committed proposals/s**.
Single-group median of the four emitted samples: **215.8917 committed
proposals/s** (the missing fifth sample does not affect the WAL diagnosis).

The verifier rejected the run solely because shaped RTT p95 was outside the
existing 1.0 +/- 0.2 ms gate. That gate was not weakened. The 100,000/s native
throughput gate remains unmet; these are Docker/WSL2 logical-node diagnostics,
not physical three-node evidence.

## WAL result

The first coordinator implementation synced every registered WAL on every
barrier. Its diagnostic run produced 1.74--2.03 million `fsync_calls` per node,
versus 56--58 thousand in the prior run, so it was rejected as a regression.
The follow-up fix tracks a dirty bit per WAL and syncs only dirty participants.
After the fix, Multi-Raft sample 0 recorded 53,415--54,458 fsync calls per node,
with CPU 151.87--157.39 s and disk writes around 2.0 MiB. This is back in the
prior range while throughput is comparable or higher.

Raw evidence is retained at `/tmp/chirps-walbatch250-dirty/`:

- `summaries.ndjson`: `6818936fff9fa471cdbcdea623438ed25e6e4f34f15a39f951592c00e8c2118e`
- `samples.json`: `e3fb64b9abb2b7d8c684ae2588ae45b0b9988595acf20a7774afad429df76b93`
- `host-facts.txt`: `4b71c23f9b3722d0b8e005a44ad1f4604d1a4fa41a413aecec807a9eedd2d32f`
- `artifact-input.json`: `21332c20347d8317c00d8c3a03218c2e01257dd693b4c3a3a9f766c9145b0e98`

## Decision

`RELEASE BLOCKED`: the dirty-participant WAL optimization is retained as a
diagnosed, tested improvement, but release remains blocked by the throughput
and shaped-RTT gates. No tag, push, or release was created.
