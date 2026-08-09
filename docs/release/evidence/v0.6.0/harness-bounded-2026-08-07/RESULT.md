# Memory-bounded harness rerun

Date: 2026-08-07 (JST)  
Revision: `d7afda1`  
Profile changes: per-group dispatch FIFO 32; QUIC send queue 4,096.

## Results

| mode | sample | committed | proposals/s | timeouts | peak RSS |
|---|---:|---:|---:|---:|---:|
| Multi-Raft | 0 | 19,467 | 324.4500 | 2,090 | 1,914,302,464 B |
| Multi-Raft | 1 | 42,767 | 712.7833 | 0 | 320,774,144 B |
| Multi-Raft | 2 | 0 | 0.0000 | 3,600 | 157,491,200 B |
| Multi-Raft | 3 | 0 | 0.0000 | 3,600 | 155,602,944 B |
| Multi-Raft | 4 | 23,459 | 390.9833 | 1,800 | 990,167,040 B |
| single-group | 0–4 | 3,428–3,837 | 57.1333–63.9500 | 681–1,090 | 19,349,504–20,713,472 B |

The Multi-Raft sample median is `324.4500/s`; the validity predicate is false
because samples contain timeouts, so no confidence bound is reported. The
runner completed the ten load phases but failed closed during final verification
with `loadgen report contains errors or timeouts`.

## Memory comparison

The previous profile reached `3,658,006,528 B` peak RSS. The bounded profile
reached `1,914,302,464 B`, a reduction of approximately 47.7%, but it remains
far above single-group RSS. In Multi-Raft sample 0, node maxima were roughly
1.87 GiB, 0.99 GiB, and 39 MiB respectively; sample 4 was roughly 1.42 GiB,
188 MiB, and 76 MiB. The concentration on node 1 (the seed/leader side) means
the harness queue fix removed a substantial retention source but did not prove
that all remaining retention is harness-only.

## Decision

This is diagnostic evidence, not release evidence. The profile still has
timeouts and is far below the 100,000 committed proposals/s contract. The next
investigation must expose per-node dispatch/send/retransmit queue depth and the
per-group log-cache setting before attributing the remaining RSS to the harness
or the Raft implementation.
