# v0.6.0 corrected native Multi-Raft run

Date: 2026-08-07 (JST)  
Revision: `a1f421e` (`fix: backpressure single-group raft proposals`)  
Environment: one Linux host, three logical Docker nodes, 250 µs `tc netem` per node, 1 KiB payload, 100 groups, 300 clients, 5 Multi-Raft and 5 single-group phases.

## Local safety result

The preflight harness ran before this measurement:

- `multi_raft_three_voter`: 9/9 passed, including 32 concurrent proposals and
  300 baseline-level proposals with all three voters converged.
- `chirps-multi-raft-perf`: 9/9 unit tests passed (loopback TCP test rerun with
  the required bind permission).

## Corrected-run observations

| mode | sample | committed | proposals/s | errors | timeouts |
|---|---:|---:|---:|---:|---:|
| Multi-Raft | 0 | 3,727 | 62.1167 | 0 | 3,300 |
| Multi-Raft | 1 | 18,936 | 315.6000 | 0 | 1,851 |
| Multi-Raft | 2 | 0 | 0.0000 | 0 | 3,600 |
| Multi-Raft | 3 | 0 | 0.0000 | 0 | 3,571 |
| Multi-Raft | 4 | 23,557 | 392.6167 | 0 | 1,688 |
| single-group | 0 | 2,546 | 42.4333 | 0 | 1,765 |
| single-group | 1 | 2,934 | 48.9000 | 0 | 1,386 |
| single-group | 2 | 2,758 | 45.9667 | 0 | 1,426 |
| single-group | 3 | 2,771 | 46.1833 | 0 | 1,436 |
| single-group | 4 | 4,002 | 66.7000 | 0 | 644 |

The raw run completed all ten phases, but the release verifier rejected the
aggregate because host swap usage grew during the run. Independently of that
environmental rejection, every sample contains timeouts and the Multi-Raft
median is 62.1167 committed proposals/s, far below the 100,000/s gate. No
bootstrap confidence lower bound or overhead ratio is reported because the
sample validity predicate is false.

## Decision

`RELEASE BLOCKED`. The UT-detectable leader-churn defect is fixed and covered,
but the corrected controlled measurement does not satisfy the no-timeout,
100,000/s, or valid-environment gates. No tag, push, or GitHub Release was
created.
