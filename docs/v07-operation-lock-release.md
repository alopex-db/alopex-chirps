# Releasing failed creation operations' directory locks

An unsuccessful owner installation previously dropped its `File` without an
explicit unlock. On Unix, `flock` belongs to the open file description. A
duplicated descriptor can therefore keep the lock alive after that drop. A
concurrent process spawn may temporarily inherit such a descriptor until exec,
despite close-on-exec being enabled. A subsequent reopen can report
`OwnerLockUnavailable` even after the original operation returned.

Every successfully acquired directory lock now has a private RAII guard that
explicitly unlocks on drop. Successful creation transfers that guard into the
active subscription for its complete lifetime. Failed installs and earlier
error exits release the guard. Failed acquisition never constructs a guard or
unlocks another owner's handle. Public APIs, persistent formats and durability
boundaries do not change.

The coverage job at source `e1e6e07` failed the immediate-recovery test after an
unknown owner-install outcome. Its original error lacked the outcome details,
so descriptor inheritance is a plausible cause of that observed failure, not
a directly captured event from that job. The deterministic regression confirms
the underlying defect: retain a duplicate descriptor, exercise all four
owner-install failure boundaries, and reopen while the duplicate remains live.
The old implementation fails with `OwnerLockUnavailable`; the guard fixes it.
Test helper failures now include the actual outcome for future diagnosis.

```text
Target: alopex-chirps-backend-iggy/src/state/creation.rs operation-lock lifetime
Mutation check: scoped cargo-mutants on OwnedDirectoryLock::drop, 1/1 caught
Survivors: none in that scope
Strengthening: four-boundary duplicate-descriptor regression; existing active
               owner and separate-process exclusion checks
Verification: focused creation tests 13 passed; old implementation regression RED;
              backend all-feature library tests 137 passed; scoped all-target,
              all-feature Clippy and workspace formatting passed
```

This is an intra-module ownership change with unchanged module/public interfaces;
no cross-module coupling analysis was needed. The regression uses safe descriptor
duplication rather than adding unsafe fork code. Kani/Miri are not selected for
this OS-lock behavior. Full CI at the corrected source remains required.
The first full-library attempt hit sandbox EPERM in six existing localhost
listener tests; with local-listener execution permitted, all 137 tests passed.
