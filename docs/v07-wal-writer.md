# v0.7 Raft WAL writer integration

Issue #50 requested that Raft storage use `alopex-core::log::wal::WalWriter`.
Core 0.3.4 synced every append, which conflicted with Chirps' existing group
commit boundary. Published Core 0.8.15 exposes separate buffered `append` and
durable `sync` methods. Chirps now delegates writing and framing to that API,
while retaining its coordinator and one barrier per Raft append batch.

The dependency is private to the implementation: no public API exposes Core
types. The WAL version remains 1. Its length, CRC32 and bincode encoding are
unchanged. A fixed legacy record verifies byte-for-byte append compatibility
and deferred sync; ordinary recovery, vote, snapshot and batching tests cover
the surrounding storage contract.

Core 0.8.15 repairs a damaged tail when opening its writer and treats checksum
damage as end-of-log when reading. Raft must fail closed instead. Chirps validates
framing and record decoding before constructing the writer, and checks the
reader's valid-prefix length before returning WAL entries. Invalid recovery
preserves the original file. A regression reproduced the otherwise silent
checksum failure after opening storage, then passed with the prefix check.

The public `get_entries_by_log_id` keeps its existing Openraft `StorageError`
return type. Its narrowly scoped `result_large_err` allowance documents why
boxing the error would break callers.

## Verification

All commands used a shared, explicit Cargo target directory; no server build
or release artifact was produced. Tests used native Rust 1.96.0 and lint checks
also used CI's installed stable Rust 1.98.1.

```text
Target: alopex-chirps/alopex-chirps-raft-storage/wal_storage.rs
Mutation check: scoped RealWalSink and validate_wal_framing, 22 mutants:
                18 caught, 4 equivalent survivors; boundary recheck 6:
                2 caught, same 4 equivalent survivors.
                Final changed-line check: 9 mutants, 6 caught, 3 unviable.
Survivors: equivalent for the reject/accept and byte-preservation contract.
Strengthening: golden legacy framing, non-destructive invalid record test,
               proptest append-boundary model, live checksum-damage regression.
Verification: cargo test --locked -p alopex-chirps-raft-storage --all-features
              32 passed; 5 pre-existing ignored documentation examples.
              cargo +stable clippy --locked -p alopex-chirps-raft-storage
                  --all-targets --all-features -- -D warnings: PASS.
              cargo fmt --all -- --check: PASS.
```

The four parser survivors alter early rejection checks: either subtraction to
addition in remaining-length calculations, or `<` to `==` / `<=` in the short
header check. The subsequent bounded `read_exact`, checksum and decoding checks
still reject every incomplete prefix without changing bytes. The `<=` case
additionally rejects an eight-byte header with no decodable record body, which
was already invalid. Error wording is not a stable API. The property test
checks every byte prefix of generated record sequences against the independently
recorded append boundaries. The three final unviable mutants construct
Openraft entries using nonexistent constructors or an unsupported conversion.
Mutation-generated seeds were preserved in local verification records, not
promoted to reports of genuine production counterexamples.

Scoped `cargo coupling crates/chirps-raft-storage/src --changed-since main
--impact-depth 4 --json` inspected five modules (grade C, no critical issues).
The established shared Raft types and storage/snapshot traits are intentionally
cohesive. The new Core writer stays behind the private `WalSink`; no new public
coupling was introduced. Static analysis cannot establish synchronization,
timing, shared-state ordering, macro expansion or inactive cfg behavior. Core's
reader/writer semantics therefore received direct boundary tests. Kani/Miri
were not selected for this safe file-I/O adapter change.

This evidence covers the storage change. Full candidate compatibility, CI,
performance and release gates still run on the final integrated source.
