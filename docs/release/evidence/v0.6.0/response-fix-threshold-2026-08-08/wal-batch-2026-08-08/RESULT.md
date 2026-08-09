# WAL batching controlled measurement

## Conditions

- Revision: `773b53b` (`perf: batch WAL durability barriers per raft append`)
- Controlled 3-node container profile, 100 groups / 300 clients, 60 s measure
  + 15 s warmup + 5 s drain, 5 Multi-Raft and 5 single-group samples
- `snapshot_threshold=10000`, `fsync_interval=0`, `--resource-audit`
- Host warning: kernel has no swap-limit capability; the runner's final verdict
  rejected the run because host swap grew during the run.

## Samples

| mode | throughput/s | committed | timeouts | errors | fsync barriers | peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| Multi-Raft 0 | 73.20 | 4,392 | 3,600 | 0 | 9,223 | 550 MiB |
| Multi-Raft 1 | 878.38 | 52,703 | 0 | 0 | 97,147 | 438 MiB |
| Multi-Raft 2 | 716.15 | 42,969 | 1,188 | 0 | 73,356 | 334 MiB |
| Multi-Raft 3 | 499.82 | 29,989 | 2,196 | 0 | 56,442 | 683 MiB |
| Multi-Raft 4 | 770.08 | 46,205 | 0 | 0 | 102,287 | 144 MiB |
| single-group 0 | 92.32 | 5,539 | 0 | 0 | 7,962 | 13 MiB |
| single-group 1 | 92.25 | 5,535 | 0 | 0 | 8,004 | 13 MiB |
| single-group 2 | 93.77 | 5,626 | 0 | 0 | 8,062 | 14 MiB |
| single-group 3 | 91.53 | 5,492 | 0 | 0 | 7,941 | 13 MiB |
| single-group 4 | 93.65 | 5,619 | 0 | 0 | 8,071 | 13 MiB |

Multi-Raft median is **716.15 committed proposals/s**; single-group median is
**92.32/s**. Three Multi-Raft samples had zero timeout/error, while two samples
had timeout bursts. No server, transport, or proposal error was reported.

## Verdict

The run is diagnostic evidence, not release evidence: the host-swap guard
failed, and the Multi-Raft timeout samples violate the release gate. The WAL
change is unit- and integration-tested, but it does not by itself establish the
target throughput. The next optimization target remains scheduling/host
contention and per-group append batching; `fsync_calls` shows that the current
OpenRaft workload often submits small batches, so the new sink cannot reduce a
barrier that the caller never batches.
