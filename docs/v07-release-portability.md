# v0.7 release portability and CI repair

The first PR84 CI run on `d1f0f45` failed in five independent boundaries:
external specification includes (stable tests and coverage), newly enforced
Clippy slice iteration lint, deprecated beta atomic update naming, a Windows
unused argument, and vulnerable h2/rustls lock entries.

## Scope and sequence

1. Corpus unit tests use explicitly synthetic byte inputs in temporary
   directories. They must still reject mismatched requirements/design digests,
   reconstruct the same cases, replay, and produce repeatable bytes. These are
   unit tests of the generator and verifier, not release qualification.
   The release generator and E2E authentic specification hashes remain intact.
2. Fix the concrete stable/beta/Windows diagnostics without warning exemptions.
   Test atomic zero/concurrent decrement behavior and hexadecimal validation
   before broad checks. Existing schema/source integrity assertions stay active.
3. Update client h2 and rustls to patched versions and validate QUIC mutual TLS
   and Iggy transport behavior. Do not change the separately attested server
   artifact identity as a side effect of this client change.
4. After focused prerequisites pass, run cached all-target/all-feature Clippy
   and workspace tests once on the installed stable toolchain. These checks
   establish local macOS coverage; Linux/Windows CI and real-server E2E are
   separate evidence. Inspect failures before starting wider verification.
5. Inspect package file inventories and construct packages using registry-only
   dependencies where versions exist. Unpublished v0.7 dependency resolution
   must remain an explicit limitation; do not substitute paths and call that
   registry evidence.

Budget: reuse the existing native target cache, sequential builds, initial
15-minute/12-GiB target budget for focused plus full local checks. Reassess
before extending it; no duplicate remote compilation. Keep package archives and
logs bounded and retain only reviewable evidence after the checks.

## Verification and remaining boundaries

Validation used stable Rust 1.98.1 on macOS. The all-feature suite completed
with 589 passed, 0 failed, and 41 ignored across 93 test targets, including doc
tests. The ignored real-server lanes still require their attested Linux server;
this result does not qualify those lanes. The first sandboxed attempt could not
bind loopback; the completed run allowed local networking.

Commands and results:

- `cargo +stable fmt --all -- --check`: PASS.
- `cargo +stable test --locked --offline -p alopex-chirps-backend-iggy --lib --all-features`:
  132 PASS, including three portable corpus tests and independent wrong-input
  digest rejection. The verification-observer fixture now requests the same
  BrokerAccepted boundary that its fake port returns. The separate regression
  still rejects a weak response for an OsSyncedAccepted request.
- `cargo +stable test --locked --offline -p chirps-fault-oracle --lib`: 44 PASS.
- `cargo +stable test --locked --offline -p chirps-durable-perf`: 18 PASS.
- `cargo +stable test --locked -p alopex-chirps-transport-quic --test issue54_mtls`:
  PASS with the updated TLS graph.
- `cargo +stable test --locked --offline --workspace --all-features --no-fail-fast -- --test-threads=1`:
  PASS, totals above. This includes the v0.6.1 downstream fixture's three tests.
- `cargo +stable clippy --locked --all-targets --all-features -- -D warnings`:
  blocked by existing `wal_storage.rs::get_entries_by_log_id` public return
  type (`result_large_err`). The separate storage integration owns that fix;
  full Clippy is not recorded as passing here. Backend-scoped Clippy passed
  before the TLS lock update.
- `cargo +stable package --locked --offline --allow-dirty -p alopex-chirps-wire`:
  archive construction and verification build PASS.
- Backend `cargo package --offline --list --allow-dirty` inventory PASS, with
  source-local test fixtures and no external specification inclusion.
  Actual backend package construction fails both offline and against the current
  registry index because core 0.7.0 is unpublished (latest available 0.6.3).
  The existing v0.7 registry fixture lock is not fresh publication evidence;
  regenerate it from the final published packages and re-run both feature modes.

Dependency repair updates h2 to 0.4.16, rustls to 0.23.45 (also the direct
manifest minimum), rustls-webpki to 0.103.15, and yanked chacha20 to 0.10.2.
Cargo also reselected existing Windows system dependencies within their accepted
version ranges. A fresh Linux audit found only the pre-existing rkyv 0.7.46
advisory exception plus bincode/paste maintenance warnings. `cargo tree --locked
--offline -i rkyv --all-features --target all` reports no active dependency:
rust_decimal retains rkyv as an optional lock entry. No new audit exception was
added. The CI audit command retains its existing explicit RUSTSEC-2026-0235
exception; removal requires a compatible upstream dependency change.

The v0.6.1 test no longer substitutes unchanged implementation bytes for public
compatibility. Its struct literals, exhaustive matches, required-only trait
implementation, and typed call sites remain. The runtime extension checks assert
Control/Ephemeral delegation, return-value propagation, and Durable rejection
without fallback. Full eight-crate semver comparison against the pinned v0.6.1
commit is a separate integration gate, not claimed by these fixture tests.

## Test-gap handoff

Target: `chirps-multi-raft-perf/src/node.rs::decrement_queue_depth`.
Focused concurrent decrement and zero-saturation regression: PASS.
Mutation check: `cargo mutants --package chirps-multi-raft-perf --file tools/chirps-multi-raft-perf/src/node.rs --re decrement_queue_depth --in-diff <scoped-diff> --no-config --in-place --baseline skip --timeout 60 --cargo-test-arg=--lib --cargo-test-arg=queue_depth_decrement_is_saturating_and_atomic`.
Result: 4 caught, 0 survived. Strengthening: focused concurrency regression.
The analogous file-transfer test-only fault counter has an exact concurrent
budget-consumption regression; production file-transfer behavior is unchanged.

Scoped coupling before mutation used the perf source tree with `--no-git
--json`: 11 modules, 112 internal and 277 external couplings, grade C. Existing
schema and verifier hotspots remain accepted for this local helper change;
no module boundary or new impact path was introduced. Static analysis cannot
observe runtime timing, implicit duplicated logic, macro/cfg expansion, or
organizational distance; temporal coupling was not analyzed. No workspace-wide
mutation or coupling scan was run.

The shared native cache reached 21 GiB during full validation and parallel
storage work. The initial 12-GiB estimate was revised before broad testing;
complete the running validation and retain the cache for the ongoing integration,
then inventory it at handoff. Logs and mutation/coupling reports remain outside
tracked source. Windows and beta fixes still need confirmation on their CI
platforms; macOS success is not a substitute for those jobs.

### Integrated WAL and final stable lint follow-up

After integrating the Core 0.8.15 WAL adapter, the complete command
`cargo +stable clippy --locked --offline --all-targets --all-features -- -D warnings`
passes. The additional stable diagnostics used equivalent `as_chunks` iteration
and simplified test expressions. Large Openraft errors retain their existing
public types: allowances are confined to the affected functions and explain the
compatibility constraint. The test-only materialization helper retains explicit
independently bound inputs with a local argument-count allowance.
Facade library, profile, and v0.6.1 downstream tests and the file-transfer
persistence target pass after integration. These source-equivalent lint edits
do not add behavior or require another mutation run.
