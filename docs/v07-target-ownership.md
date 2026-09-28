# Ownership of temporary build targets

The corpus generator previously registered its exit trap before rejecting an
existing target. That rejection could call `cargo clean` on a directory created
by another invocation. It now atomically creates its target and enables target
cleanup only after that allocation succeeds. Existing empty directories,
non-empty directories, files and symlinks are preserved on rejection.

The corpus and compatible-server build scripts share `scripts/release/owned-target.sh`.
It skips the cleaner when an owned target is already absent, rejects target
symlinks, and removes only empty leftovers after a successful cleaner command.
A failed cleaner or non-empty leftover remains an error; there is no recursive
deletion fallback. Callers remain responsible for allocating and owning the
target. These checks do not claim isolation from hostile concurrent filesystem
replacement by another process.

Verification: `python3 -B scripts/release/test-owned-target.py` passes 12 cases.
The actual previous corpus script, run with only temporary-path substitutions
and a bounded fake Cargo cleaner, fails the preexisting-data regression. The
corrected script preserves it and still cleans its newly owned target after a
later failure. Tests also cover a budget failure before ownership, repeated
absent cleanup, non-empty leftovers, cleaner failure and dangling symlinks.
No test runs Cargo or accesses a shared build directory. Shell syntax and
whitespace checks pass. Rust mutation testing, Kani and Miri do not apply to
this shell-only change; full server builds are separate verification.
