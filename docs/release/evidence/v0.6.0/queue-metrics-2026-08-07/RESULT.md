# Harness / node resource audit (2026-08-07)

## Scope

Revision `2011052` adds two diagnostic surfaces to the controlled three-node
container run:

- each node emits dispatch depth, transport queue utilization, retransmission
  bytes/count, overflow, and backpressure counters;
- each load generator samples its own `/proc/self/status` RSS every 100 ms and
  records `peak_rss_bytes` in its report.

The run used the existing fixed 10-sample order, 3 nodes, 300 clients, 1024 B
payloads, 15 s warmup / 60 s measure / 5 s drain, and the bounded 4096-frame
send queue. The verifier stopped at the expected loadgen timeout gate; this is
diagnostic evidence, not a release pass.

## Results

| item | observed |
|---|---:|
| Multi-Raft throughput median (5 samples) | 373.13 committed proposals/s |
| Multi-Raft committed range | 0–30,037/sample |
| Node peak RSS (all samples) | 1,999,663,104 B (1.86 GiB) |
| Loadgen peak RSS (all 30 processes) | 5,423,104 B (5.17 MiB) |
| Dispatch depth at node peaks | 1–1,268 frames (sample/node dependent) |
| Retransmission buffer / count | 0 / 0 in every emitted node metric |
| Queue overflow / backpressure | 0 / 0 in every emitted node metric |

Representative node peaks:

```
sample  node  peak_rss_bytes  peak_dispatch_depth
0       1     681,361,408     35
0       3   1,537,662,976  1,268
1       1   1,666,785,280      7
2       1   1,588,703,232      7
3       1   2,024,079,360      7
4       1   1,838,653,440     13
4       2     966,664,192  1,018
```

## Interpretation

The load generator is not the memory consumer in this run: its measured peak
RSS is roughly 5 MiB per process, while node RSS reaches 1–2 GiB. The high RSS
also occurs with dispatch depth as low as 7–13 and with all transport/retry
counters at zero. Therefore the earlier unbounded FIFO issue was real and is
fixed, but it does not explain the remaining node RSS. The remaining
investigation boundary is node-side Raft/WAL/log/snapshot/allocator retention;
the current metrics do not identify which of those components owns the bytes.

The result is not release-ready: throughput is far below the TiKV-aligned
measurement target and every controlled profile contains loadgen timeouts.
