# Reproducible durable formal collection

The four durable models declare 4 typechecks, 4 normal checks, 125 RED profiles,
and 27 reachability witnesses. Normal bounds are 24, 28, 24, and 20. This is a
bounded model check, not a proof of unbounded execution or of the Rust refinement.

The original Compose runner removes temporary checker outputs. The release
collector retains logs, generated configurations, counterexamples, checker
outputs, per-file SHA-256, commands, exit codes, elapsed time, source identity,
image identity, and resource limits. Its result is always `collected-unverified`
or a failure; no label alone proves that a release passed.

## Inputs and execution

Use Python 3 with PyYAML, Git, Linux Podman, and the already available image
`ghcr.io/apalache-mc/apalache@sha256:fde994fd109323934b9abb7ad169de37b29acf2141483367f2913cae30ff3795`.
The pinned checker reports Apalache 0.58.3. The collector does not install tools,
access the network from a container, or modify the source checkout.

```sh
python3 -B scripts/release/export-v07-formal.py "$TRUSTED_SOURCE_ROOT" "$FULL_COMMIT" "$NEW_SNAPSHOT"
python3 -B scripts/release/v07_formal_collect.py "$NEW_SNAPSHOT" "$NEW_OUTPUT" --workers 2 --timeout 2700
```

Both destinations must be new. Export reads only 14 public model inputs from
immutable Git objects; it does not copy private specification documents or the
working tree. Use a frozen release candidate for final evidence. An older
candidate's evidence must retain its original identity and remain development
evidence even when its model text resembles the new candidate.

Each worker is limited to 1 CPU, 2 GiB memory/swap, and a 1,400 MiB Java heap.
The output budget across workers is 1 GiB, monitored every 500 ms; a small
sampling overshoot is possible. The timeout is per job and at most 2,700 seconds.
An interrupted, timed-out, or budget-exceeding run preserves its partial output.
Only containers with names allocated by this collector are stopped and removed;
existing containers, image stores, and source inputs are untouched. Container
removal uncertainty is recorded as failure. Automatic retries are disabled.
The full run completes phases in order: typecheck, normal, RED, witness.

For cost investigation only, `--kind normal`, repeated `--job ID`, or `--smoke`
select a subset. The report records every planned job and marks such runs
`development-subset`; subsets cannot establish complete model evidence. A
SendLease typecheck took about four minutes on a shared development host; its
normal bound-24 run did not finish in ten minutes. Budget several hours for the
full matrix and measure the actual candidate; never lower bounds or omit
invariants, profiles, or witnesses to fit a time budget.

## Independent acceptance still required

A collector's exit code is an observation, not an independent verifier. Release
validation must derive the expected 160 jobs, configurations, bounds and exact
14 input hashes from the trusted candidate Git objects, reject missing/extra
jobs, and check every retained file digest. Revalidate the fixed image and
checker version, actual completed normal bound, all selected invariants, and
successful typechecking. For RED and witnesses, exit 12 is insufficient: require
the declared target's counterexample and reject accidental TypeOK violations.
For witnesses, verify the catalog's reachability predicate in the trace.

The separate catalog gate and all ten completeness probes must also be retained
and checked for their expected diagnostics. Use `v07_formal_refinements.py` with
the exact Chirps and Iggy commits to reject stale production/test references.
Passing references does not demonstrate that their runtime tests passed.

Run the collector's non-container contract tests with:

```sh
python3 -B scripts/release/test-v07-formal-collect.py
```
