# v0.6.1 public API compatibility gate

Chirps v0.7 keeps the public API of its eight existing crates compatible with
v0.6.1 commit `3ff0ce6a631fd235fc4a3e3e08c8a9665f3d8bd9`. The new Iggy crate has
no v0.6.1 baseline. `scripts/verify-v061-public-api.py` compares rustdoc APIs,
including publicly reachable items, signatures and trait implementations, with
`cargo-semver-checks 0.50.0`. It forces `--release-type minor`: inferring a major
change from `0.6.1 -> 0.7.0` would otherwise permit the breaks this contract forbids.
The script rejects package/workspace lint overrides.

Install the pinned tool with `cargo install cargo-semver-checks --version 0.50.0
--locked`, or use the digest-verified official binary in CI. Use Rust 1.96.0 for
the pinned rustdoc format. A tool/build error is a failed gate, never a pass.

```bash
# Inspect the full matrix without building. This does not create passing evidence.
python3 scripts/verify-v061-public-api.py --plan

# Commit the candidate first. Keep evidence and build products outside its checkout.
# SEMVER_CHECKS_BIN may point to a task-local binary with the exact pinned version.
CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=/absolute/path/api-target \
  python3 scripts/verify-v061-public-api.py --output /absolute/path/evidence/api.json

python3 -B -m unittest discover -s scripts/tests -p test_v061_public_api.py
```

The matrix includes all features, Cargo defaults, no defaults, and every named
public feature separately without defaults. Implicit optional-dependency features
are included. A feature added since v0.6.1 is compared to the old no-default API;
removed baseline features fail before rustdoc runs. `--only-explicit-features` is
the tool's no-default option. This avoids the tool's default heuristic that omits
some features. Each run compares the current manifest to the immutable Git commit;
it never substitutes the latest registry release or a movable tag. The script
exports that exact commit with `git archive` into an owned temporary directory,
then gives the checker each baseline/current crate manifest explicitly. This
avoids the duplicate `alopex-chirps-raft-storage` manifest in the independent
registry-consumer fixture; no baseline source is deleted or rewritten.

Evidence records candidate commit, baseline, tool and compiler versions, candidate
lock hash, baseline archive hash, complete expected matrix, command/exit status
and SHA-256 of each log.
The checkout must be clean and unchanged throughout. Existing output is never
overwritten. A missing, interrupted or failed check is not passing evidence.
Builds run sequentially with two Cargo jobs by default; the separate target can be
removed after its evidence logs are retained. The script removes ambient Rust
flags that could hide API items, but records the host compiler/target; CI runs on
Linux, macOS and Windows so target-specific APIs are checked on each platform.

This is an API check, not a byte-identity requirement. It permits internal fixes
and additive APIs. It complements the v0.6.1 compile fixtures, backend default-method
runtime tests, wire format fixtures and full workspace feature tests. Like all
static API tools it cannot prove behavior, wire compatibility, every combination
of conditional features, or every possible SemVer rule. Genuine failures must be
fixed or explicitly resolved against the release requirement; do not add lint
suppression or mark unavailable rustdoc runs as compatible.

Tool reference: [pinned cargo-semver-checks documentation](https://github.com/obi1kenobi/cargo-semver-checks/tree/v0.50.0).
