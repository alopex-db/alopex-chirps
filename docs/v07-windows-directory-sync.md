# Windows directory durability in the verification harness and WAL storage

The fault oracle head replacement and creation corpus materializer previously
opened parent directories with Unix-style read access. Windows rejected those
opens with `PermissionDenied`, causing five Windows CI failures. Both paths now
use one private helper in the harness. WAL snapshot replacement uses a private
helper in its own crate and now flushes the parent directory on Windows too;
previously its parent flush was compiled only on Unix.

Windows directory handles require `FILE_FLAG_BACKUP_SEMANTICS` to open and write
access for `File::sync_all` / `FlushFileBuffers`. The helpers retain the existing
Unix read-open/flush operation and propagate both open and flush errors. The
file-sync-before-rename ordering, oracle locking, and public APIs are unchanged.
Private duplication between the product storage crate and independent oracle
avoids introducing a product dependency into the verification boundary.

Development verification:

- `cargo +stable test -p chirps-fault-oracle`: 45 tests passed.
- `cargo +stable test -p alopex-chirps-raft-storage`: 37 tests passed; five existing
  documentation examples remain ignored.
- Scoped all-target/all-feature Clippy with `-D warnings` and workspace format
  check passed.
- Native Windows Rust 1.90 compiled the actual std-only helper and its test.
  The previous Unix-only behavior reproduced error 5; the new helper passed.
  Removing write access or the directory flag each reproduced error 5.
  The storage helper's production function is byte-identical to the tested
  harness helper. This is a native helper test, not a full Windows crate run.

```text
Target: each crate's private fs::sync_directory helper
Mutation check: cargo +stable mutants --in-place -p <package> -f <src/fs.rs>
                -- --lib fs::tests::syncs_existing_directory_and_reports_missing_directory
                One selected mutant per package; both caught.
Survivors: none; Windows-only flags additionally checked by native manual mutants.
Strengthening: existing-directory/missing-path regression; snapshot create and
               replacement reopen exact bytes; failed replacement cleans its
               temporary file while preserving the destination directory.
Verification: focused package tests and native Windows behavior above; full
              Windows CI remains required on the integrated candidate.
```

Scoped coupling found no critical issues: the existing oracle module retains
one high complexity finding and the WAL module one medium finding. The private
helper intentionally joins the oracle's two durability consumers. The harness
path does not exist on `main`, so its branch-comparison analysis was unavailable;
the scoped structural analysis used `--no-git`. Static analysis does not prove
filesystem ordering or inactive platform branches. The native check covers the
Windows opening/flush contract; it does not simulate power loss.
