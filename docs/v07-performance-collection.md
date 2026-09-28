# Performance collection correctness

The performance driver completes a clean warmup before activating an audit
probe or a safety control. Error and timeout controls must be observed during
measurement; they must not abort warmup and prevent an evidence artifact.
A failed warmup does not activate the control. Adapter shutdown and probe
cleanup are attempted on collection failure.

Scheduled measurement retains the complete declared arrival window in its
throughput denominator. Fast final operations cannot shorten the window;
slow completions extend it. Latency continues to include admission queue wait
and operation timeout, measured from the scheduled arrival.

The ordinary official SDK Direct arm confirms broker acceptance. It does not
attest the compatible extension's OS-synced receipt. Plans comparing it with
an `OsSyncedAccepted` Full arm are rejected before workload execution. Such
a comparison needs an independently implemented Direct control at the same
strong boundary. This restriction does not establish performance at either
boundary: a real audit probe, A/A calibration, all eight safety controls,
frozen bounds, and paired measurements remain required.

## Verification

Target: `chirps-durable-perf/src/main.rs`, measurement sequencing, arrival
window, and confirmation boundary validation.

Focused tests: 22 passed. Warmup/control ordering and shortened-window
regressions first failed against the previous implementation. A further
regression verifies that warmup failure never activates the safety control.
Scoped all-target Clippy and formatting passed.

Mutation check: `cargo +stable mutants --in-place -p chirps-durable-perf
--in-diff <change.diff> -F
'measure_with_probe|run_scheduled_operations|validate_direct_boundary|Adapter::send'`.
Four mutants: two caught, two unviable because generated replacement return
values require unimplemented `Default` traits. No surviving viable mutants.
The unviable mutants provide no coverage; the ordering/window regressions
provide the targeted evidence for those paths.

Coupling: scoped source analysis found no high/critical issues. Its two medium
large-module warnings concern the existing driver/evidence modules. Their
shared metric model is intentional; splitting probe execution from workload
collection remains a follow-up. Timing and dynamic probe behavior are blind
spots and require runtime tests. The branch comparison against `main` could
not analyze a tool absent at that baseline; the scoped current-source report
was used instead.

No production benchmark result or release qualification is implied by these
unit and tool checks. Kani and Miri are not applicable to this change.

## Offline verification

Replay a collected chain without contacting the broker, invoking the probe,
reading credential variables, or creating files:

```sh
chirps-durable-perf --mode verify --candidate-manifest candidate.json \
  --evidence-root evidence/performance
```

The root must contain `aa/aa.json`, `aa/bounds.json`,
`safety/safety.json`, `safety/freeze.json`, and `paired/paired.json`.
The candidate manifest is hashed as exact bytes. Its declared axes and sample
count bind every observation, including phase/arm/sample identities. Verification
recomputes throughput from elapsed time and completed operations, percentiles
from all scheduled-operation latency samples, A/A bounds, each of the eight
isolated safety controls, the freeze digest chain, and the paired verdict.
An altered serialized `Pass` cannot override the recomputed result. Exit zero
means the complete chain recomputed to `Pass`; missing data, inconsistent data,
and a genuine recomputed regression all return nonzero.

`--output` and `--bounds` are rejected in verify mode. Offline verification does
not require collection-host payload, certificate, or executable paths to exist.
The archived measurements still require authentic collection provenance: replay
can detect internal inconsistency, but cannot establish that an invented complete
raw dataset was observed on real machines. Publication must retain the collector
identity, source/build provenance, and raw observation artifacts alongside this
chain.

Offline-verifier checks: 26 package tests passed, including chain replay and
18 independent tampering cases, read-only CLI argument validation, and rejection
of a correctly recomputed performance regression. Scoped Clippy and formatting
passed. Mutation testing of `src/verify.rs` caught all six viable mutants;
two generic deserializer replacements were unviable because `T` does not have a
`Default` bound. No surviving viable mutants remain. Scoped coupling found no
high/critical issues; existing driver/evidence model coupling is retained so
collection and replay use the same estimators. Runtime provenance remains a
static-analysis blind spot. Kani and Miri are not applicable to this safe
filesystem/serialization change.
