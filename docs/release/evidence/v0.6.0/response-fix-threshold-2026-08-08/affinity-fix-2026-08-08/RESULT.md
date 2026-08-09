# Controlled measurement after CPU-affinity fix

## Change under test

The previous runner assigned `LOADGEN3_CPU=11` and `CONTROLLER_CPU=11` on a
12-CPU host, violating its own non-overlap contract. The runner now defaults to
`node1=0-1/loadgen1=2`, `node2=3-4/loadgen2=5`,
`node3=6-7/loadgen3=8`, `controller=9`, and rejects any overlapping or malformed
custom CPU sets before Docker startup.

The verifier also accepts Linux `tc`'s kernel-rounded `delay 249us` rendering
for a requested 250us netem delay, while rejecting 248us and non-netem output.

## Samples

| mode | throughput/s | timeouts | errors | fsync barriers | peak RSS |
|---|---:|---:|---:|---:|---:|
| Multi-Raft 0 | 826.37 | 0 | 0 | 91,430 | 441 MiB |
| Multi-Raft 1 | 842.80 | 0 | 0 | 93,288 | 439 MiB |
| Multi-Raft 2 | 0.00 | 3,600 | 0 | 813 | 140 MiB |
| Multi-Raft 3 | 752.52 | 0 | 0 | 99,557 | 142 MiB |
| Multi-Raft 4 | 737.47 | 0 | 0 | 74,614 | 461 MiB |
| single-group 0 | 94.28 | 0 | 0 | 8,073 | 13 MiB |
| single-group 1 | 92.22 | 0 | 0 | 7,949 | 13 MiB |
| single-group 2 | 93.90 | 0 | 0 | 8,034 | 13 MiB |
| single-group 3 | 95.02 | 0 | 0 | 8,128 | 13 MiB |
| single-group 4 | 96.20 | 0 | 0 | 8,258 | 13 MiB |

Four of five Multi-Raft samples completed without errors or timeouts. Their
median is **789.66/s**; including the timeout sample, the ordered five-sample
median is **737.47/s**. The run is diagnostic evidence only because one sample
violated the timeout gate. The runner otherwise completed all ten samples; the
old qdisc verifier would have rejected the run solely because the kernel printed
249us instead of 250us.

## Validation

- `NODE3_CPU=8 LOADGEN3_CPU=8` is rejected before startup with an explicit
  overlap error.
- `cargo test -p chirps-multi-raft-perf --locked`: 22 tests passed.
- The remaining Multi-Raft timeout is intermittent host/runtime evidence, not
  a shaper or CPU-set overlap; release performance acceptance remains pending.
