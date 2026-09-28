# TSO and snapshot options without breaking v0.6.1 consumers

The pinned public API comparison found fields added to exhaustive public
configuration structs and a variant added to an exhaustive error enum. These
changes prevented existing v0.6.1 struct literals and matches from compiling.
The legacy structs and error variants now retain their original shape; new
controls use additive option types and constructors.

- `TsoConfig` retains `timestamp_ttl`. Move the v0.6.3 allocation validation
  controls to `TsoOracleOptions { batch_size, prefetch_threshold }` and call
  `TimestampOracle::with_options`. The original `new` uses the same valid
  defaults (10,000 and 1,000). These oracle options validate caller settings;
  allocation counts still come from each request, as before.
- `TsoClientConfig` retains its original batch/retry/backoff fields. Move
  `prefetch_threshold` to `TsoClientOptions`, supplied through `with_options`
  or `with_sleeper_and_options`. Existing constructors keep threshold 1,000.
  Zero disables prefetch. A threshold greater than the batch size remains valid
  and requests eager prefetch, preserving the previous implementation behavior.
- `SnapshotTransferConfig` retains chunk threshold/size, concurrency and retries.
  Move `transfer_timeout` to `SnapshotTransferOptions`, passed to
  `SnapshotSender::with_options`. `new` retains the 60-second total deadline.
  Zero deadlines remain invalid.
- `SnapshotTransferError` retains its five legacy variants. Existing `transfer`
  reports deadline expiry as `Terminal` with an actionable timeout message.
  Callers needing a typed timeout use `transfer_detailed`, whose additive
  `SnapshotTransferFailure` distinguishes `Timeout` from `Transfer(error)`.
  Both routes share the same deadline and abort path; partially transferred
  snapshots remain invisible.

## Verification plan

First run only the affected facade tests: `tso_client`, `tso_oracle`,
`tso_handoff`, `snapshot_transfer`, `v061_public_compatibility` and
`issue_v063_work_order2` with all features. The legacy fixture includes exact
struct literals and exhaustive snapshot error matching. Regressions cover
zero/equal/out-of-range oracle settings, zero/eager/default client prefetch,
zero/default deadlines and both timeout result mappings with exactly one abort.
Then run package-scoped coupling and changed-function mutation checks for the
options/deadline boundaries. Run stable Clippy on the affected facade and hand
the committed candidate to the independent pinned semver checker. Full release
compatibility is not claimed until that comparison passes.

## Results

The six focused targets passed 30 tests, and facade all-target/all-feature
Clippy passed on stable Rust 1.98.1. The initial prefetch fixture lacked the
third response requested by the established eager-refill behavior; its response
script and exact request-count assertion now cover that behavior.

Target: `TimestampOracle::with_options` and `SnapshotSender` deadline adapters.
Scoped oracle cargo-mutants: 5 candidates, 4 caught, 1 unviable (the generated
`Default` constructor does not exist). Snapshot cargo-mutants: all 3 generated
candidates were unviable for the same missing-`Default` type reason; this is not
claimed as mutation coverage. Three explicit snapshot source-fault injections
instead proved that tests fail when zero deadlines are allowed, timeout abort
is removed, or the legacy timeout is incorrectly classified as retryable.
Each fault was restored before committing. No surviving behavior mutant remains.

Coupling ran on the facade source tree before mutation: 34 modules, 436 internal
and 855 external couplings, grade C, no critical issues. Existing `lib`, durable,
Raft transport, mesh and Raft node hotspots remain. New option types remain in
the same owning modules; the common snapshot deadline/abort path is reused by
both public entry points. Runtime timing, implicit logic, macros/inactive cfgs
and temporal co-change are outside this static `--no-git` analysis. Focused
runtime boundary tests cover these changed adapters; no unsafe or bounded
arithmetic implementation was added, so Miri/Kani were not selected.

The complete pinned semver comparison is a separate integration result and must
be re-run with the independent transport compatibility repair.
