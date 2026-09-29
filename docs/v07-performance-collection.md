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

## Runtime audit boundaries

The v2 audit subprocess receives the client PID and canonical checkpoint root,
plus the observation identity and requested control. Its separately pinned
configuration and program are passed as `--config`, `--config-sha256`, and
`--chirps-audit-request-json`. Platform metrics are limited to measured peak
RSS, disk growth, and a measured hard-limit violation. The driver owns operation
errors/timeouts, admission-queue observations, and broker readback verification.

Before each send, the driver records an expected logical sequence and the concrete
attempt ID. Expected source, target, generation, partition, ordering key, and
payload hash come from the candidate and original application payload. The
independent official-SDK audit reader polls the actual broker bytes without
committing consumer offsets. Full payloads pass the production canonical-envelope
decoder, then match the independent expected fields. Multiple known attempts for
the same logical sequence count as a duplicate. Unknown attempts, wrong identities,
changed payloads, and invalid canonical digests cannot become successful audits.
Raw readback bytes and their ledger are embedded in each observation; offline
verification recomputes the audit and binds its expected fields to the candidate.

Application bytes and broker payload bytes are distinct counters. Full broker
payloads include the canonical envelope; Direct payloads are the application
bytes. The additional envelope overhead is retained in the evidence. The driver
uses the same broker-accepted confirmation boundary for both arms.

The reported queue is the instrumented workload admission queue at the mutable
public API mutex. Its safety control holds a real additional mutex admission
until the drain observation, then cancels it without sending. It is not a claim
about an unobserved SDK internal queue. The lag metric counts confirmed logical
messages not yet verified by the audit reader; its control leaves one actual
broker message unverified. Error and timeout controls alter the send operation,
while identity, payload, and duplicate controls send actual altered messages or
additional attempts. The hard-resource control is measured by the platform
collector under its explicit control-only cap. No control sets an audit counter
to a canned positive value.

Full backend bounded-queue usage is additionally read from
`DurableHandle::local_state_status()` and retained as
`backend_queue_after_drain`. Offline replay requires this observation to be zero
for Full. Direct records `null` for this supplementary backend field; it does not
substitute zero for an unavailable SDK internal gauge.

Runtime-audit development checks: 39 package tests and scoped all-target Clippy
passed, including Direct pre-send UUID/payload binding, canonical byte controls,
all truncated-envelope prefixes, independent candidate/ledger binding, actual
mutex waiter observation/cancellation, and all-target identity-control proptest.
The readback auditor caught all 37 scoped mutants. A second 22-mutant check of
controls/admission/ledger binding found two surviving target-bit replacements
and one timeout mutation. The survivors exposed a fixed-input test gap: OR/AND
can fail to change particular target bytes. The new proptest covers arbitrary
16-byte targets and retains its reduced regression seeds. The final focused
16-mutant control/Direct-ID check caught all mutants, including those two
replacements and the previously timing-out branch. No unresolved viable survivor
remains in those selected functions.

Coupling found four modules, no high/critical issues, and the two existing medium
large-module warnings. The shared canonical decoder and evidence estimator are
intentional dependencies; broker timing and process sampler behavior remain
runtime blind spots. These checks do not qualify a release: Linux collector
integration, actual broker polling, every live safety control, A/A calibration,
and the frozen paired run still require execution on the final candidate.
