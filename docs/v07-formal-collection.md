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

## Raw evidence verifier

`scripts/release/v07_formal_evidence.py` reads the caller's trusted Git checkout,
reconstructs the complete matrix and generated configurations, and validates
retained artifacts without running candidate scripts or trusting PASS labels.

```sh
python3 -B scripts/release/v07_formal_evidence.py \
  --source-root "$TRUSTED_SOURCE_ROOT" --source-commit "$FULL_COMMIT" \
  --report "$NEW_OUTPUT/report.json"
```

The production API is
`verify_formal_report(source_root, report_path, source_commit)`. It requires all
160 jobs. `--development-subset` is explicitly non-release and returns
`development-verified`, never full evidence. Catalog completeness probes and
exact-commit refinement checks remain separate mandatory evidence.

Apalache numbers normalized verification conditions, so a numbered invariant
violation does not alone identify the original property. For every RED/witness,
the verifier checks that the actual CFG and VCGen contain exactly `TypeOK` and
the catalog target, then evaluates the trusted model's finite `TypeOK` contract
on every ITF state. This excludes accidental TypeOK counterexamples. The trace's
constants must exactly match the generated CFG. Witnesses additionally evaluate
the trusted reachability predicate on the actual terminal state, requiring it
to remain absent in preceding states. Unknown syntax fails closed.

These evaluators deliberately cover only the four models' finite TypeOK domains
and current witness predicates. They are not general TLA+ interpreters and do
not independently re-prove transition reachability; that evidence comes from
the pinned checker and its preserved raw execution. Evidence provenance and CI
attestation must establish the origin of these raw artifacts.

Catalog observations have a separate collector and verifier. Run them outside
solver resource allocations; they use at most 0.2 CPU and 256 MiB, one at a time.
Each has a 30-second timeout and retains failure logs. The verifier requires all
11 observations, exact source/command/image binding, and the precise expected
diagnostic and exit code for each negative probe. An unrelated permission error
or a missing tool in the wrong probe cannot masquerade as a successful negative.

```sh
python3 -B scripts/release/v07_formal_catalog.py collect "$NEW_SNAPSHOT" "$NEW_CATALOG_OUTPUT"
python3 -B scripts/release/v07_formal_catalog.py verify \
  --source-root "$TRUSTED_SOURCE_ROOT" --source-commit "$FULL_COMMIT" \
  --report "$NEW_CATALOG_OUTPUT/report.json"
```

The programmatic API is
`verify_catalog_report(source_root, report_path, source_commit)`.

Before loading raw contents, both verifiers total all referenced artifact sizes
and reject totals above the collector's 1 GiB budget. Reports are limited to
16 MiB and all parsed JSON rejects duplicate keys. Exit codes and trace indices
require actual integers; JSON booleans are not accepted as integer zero or one.

## Full model category and foreign Git source in CI

The central v0.7 evidence verifier requires exactly one `model` entry with ID
`formal-models`. Its payload is `chirps.formal-release/v1`, with exact
`source_commit` and `iggy_commit` plus three `{path, sha256}` references named
`raw`, `catalog`, and `refinements`. The central verifier replays all 160 raw
jobs, all 11 catalog observations, and all production/test references against
both immutable Git trees. Missing foreign objects are an error. Development
subsets cannot be selected by the composite report.

`CHIRPS_SOURCE_ROOT` and `CHIRPS_IGGY_SOURCE_ROOT` are trusted caller inputs;
evidence files cannot choose either path. The latter can be a bare repository.
Prepare it from the candidate's source manifests and hash-bound bundle:

```sh
python3 -B "$RELEASE_TOOLS/scripts/release/prepare-v07-iggy-source.py" \
  --source-root "$CHIRPS_SOURCE_ROOT" --source-commit "$RELEASE_COMMIT" \
  --iggy-commit "$IGGY_COMMIT" --output "$RUNNER_TEMP/chirps-v07-iggy-source.git"
export CHIRPS_IGGY_SOURCE_ROOT="$RUNNER_TEMP/chirps-v07-iggy-source.git"
```

The release workflow supplies Python 3.11 and the pinned
`scripts/release/requirements-verifier.txt` dependency set (PyYAML 6.0.2) in both
trusted verifier jobs. Local verification also requires Python 3.11+ and PyYAML. `IGGY_COMMIT` is the
candidate JSON's exact commit, which the helper must match to the candidate Git
manifest. The helper permits only the Apache Iggy upstream, fetches its exact
baseline at depth 1, imports the bundle into a new bare repository, and checks
commit, parent, tree, commit count, both manifests, bundle SHA, complete diff,
Cargo.lock, toolchain hash, and Git object integrity. It never builds source or
modifies an existing repository. An offline operator may supply
`--baseline-repository "$READ_ONLY_LOCAL_IGGY_REPOSITORY"`; this also uses depth
1 and does not copy the full history. Failed outputs are left for diagnosis and
must not be reused.

In the release workflow, run preparation in **both** `ci-gate` and publication
jobs after downloading the candidate and before their first frozen-evidence
verification. Set `RELEASE_TOOLS` to the workflow revision's `release-tools`
checkout, not to a path supplied by the artifact. In GitHub Actions persist the
foreign root for subsequent steps with:

```sh
printf 'CHIRPS_IGGY_SOURCE_ROOT=%s\n' "$RUNNER_TEMP/chirps-v07-iggy-source.git" >> "$GITHUB_ENV"
```

After collecting the final candidate, write the strict refinement report and
assemble the composite only when every component passes:

```sh
python3 -B "$RELEASE_TOOLS/scripts/release/v07_formal_refinements.py" \
  --chirps-root "$CHIRPS_SOURCE_ROOT" --chirps-commit "$RELEASE_COMMIT" \
  --iggy-root "$CHIRPS_IGGY_SOURCE_ROOT" --iggy-commit "$IGGY_COMMIT" \
  --output "$FORMAL_ROOT/refinements.json"
python3 -B "$RELEASE_TOOLS/scripts/release/v07_formal_release.py" \
  --source-root "$CHIRPS_SOURCE_ROOT" --source-commit "$RELEASE_COMMIT" \
  --iggy-root "$CHIRPS_IGGY_SOURCE_ROOT" --iggy-commit "$IGGY_COMMIT" \
  --raw "$FORMAL_ROOT/raw/report.json" --catalog "$FORMAL_ROOT/catalog/report.json" \
  --refinements "$FORMAL_ROOT/refinements.json" --output "$FORMAL_ROOT/formal-models.json"
```

The schema unit self-test and isolated publisher protocol fixture explicitly
mock model execution; their synthetic bytes are not model evidence. Production
has no environment variable or CLI switch to bypass the composite gate. The
strict model modules, Git reconstruction fixtures, and authentic raw runs are
verified separately.
