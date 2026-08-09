# Response HOL separation measurement (2026-08-08)

## Verdict

**BLOCKED — not release-ready.** The harness completed the fixed-order ten-sample run, but the verifier rejected the artifact because Multi-Raft still has errors/timeouts. No tag, push, or registry publication was performed.

## Environment and contract

- Revision: `85e2597b4b3e810a6db97c23db003f7870a760cb`
- Host: WSL2 (`hayabusa-dbook`), 12 logical CPUs, Docker 29.4.1
- This is a controlled loopback/container measurement; it is not a physical three-node result.
- Workload: 100 groups / 300 clients, 1 KiB proposals, 60 s measured phase, fixed order, shaped RTT p50 about 0.83–0.86 ms.
- Instrumentation was enabled explicitly with `--resource-audit`; normal runs do not allocate samplers or phase probes.

## Samples

| mode/index | committed | errors | timeouts | throughput/s | peak RSS |
|---|---:|---:|---:|---:|---:|
| multi_raft/0 | 18,547 | 2,519 | 4 | 309.12 | 362,475,520 |
| multi_raft/1 | 0 | 0 | 3,600 | 0.00 | 148,180,992 |
| multi_raft/2 | 18,603 | 2,636 | 183 | 310.05 | 446,009,344 |
| multi_raft/3 | 23,541 | 2,398 | 3 | 392.35 | 301,998,080 |
| multi_raft/4 | 17,938 | 2,689 | 1 | 298.97 | 340,463,616 |

Multi-Raft median throughput is **309.12/s**, with 78,629 commits, 10,242 errors, and 3,791 timeouts across the five Multi-Raft samples. The single-group controls were 83.68–89.55/s with zero errors/timeouts. The run therefore demonstrates improvement over the previous all-zero samples, but not correctness or a release-quality error budget.

## Diagnosis and changes

1. The original central pump awaited a full per-group queue and caused cross-group head-of-line blocking. It was replaced by a bounded global admission queue with independent FIFO drainers (`d327fb1`).
2. A global awaited `tick_all()` allowed one slow group to delay unrelated heartbeats. The performance node now uses bounded, independent per-group background ticks (`4d57d27`).
3. Measurements showed response frames queued behind state-mutating frames. Correlated responses now bypass the per-group mutation queue and are bounded by a separate response semaphore (`85e2597`). This removed the deterministic zero-commit symptom in some samples, but sample-to-sample zero-commit behavior remains.

The remaining errors/timeouts and non-deterministic zero sample must be resolved by further local UT/failure-injection work before any release decision can change.
