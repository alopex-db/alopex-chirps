# v0.7 package and publication verification

An unpublished nine-crate release cannot pass a crates.io-only consumer build.
The gate therefore records two distinct checks. Neither check rebuilds the
production `.crate` archives or the production OCI image during publication.

## Before publication: stored archives

Construct the nine archives in dependency order with Cargo's normalized package
manifests. `cargo package --locked --no-verify --exclude-lockfile -p PACKAGE`
allows unpublished sibling crates to be packaged without inventing registry
checksums. The consumer resolves its own fresh lockfile; repository fixtures
are never used as proof of published versions.

Prepare an ordered `package-set.json` with `source_commit` and the nine package
records (`name`, `version`, relative `path`, `size`, `sha256`). The release bundle
uses the same records, with its additional registry upload metadata.

```sh
python3 scripts/release/v07_consumer_evidence.py \
  --phase stored-archives --bundle package-set.json --output NEW_REPORT_DIR
```

This unpacks only the stored archives into an isolated directory, adds explicit
`[patch.crates-io]` entries for those nine directories, and builds feature-off
and `durable-iggy` consumers. Third-party dependencies must come from crates.io;
the optional Iggy graph must appear only when enabled. This is **stored-archive
consumer evidence**, not registry-only evidence.

The report retains a copy of the archive catalog and bytes, Cargo metadata,
lockfiles, build JSON, stderr, exact commands, and hashes. It binds a canonical
package-set digest, not the candidate or release-bundle digest, to avoid a hash
cycle. The candidate's package digest binds this report. The publisher also
compares the report with the actual nine archives it will upload.

## After upload: actual crates.io consumers

The protected publisher uploads or checksum-matches all nine archives, then
runs the helper with `--phase registry`. It downloads all nine real crates.io
archives and compares their bytes with the stored bundle, uses a new Cargo home
and target directory without path patches, and builds both feature modes.
Metadata and lockfile checks reject path/git/alternate-registry substitutions,
wrong versions, or differing registry checksums. Cargo build scripts receive an
explicit platform/toolchain environment and an empty home; publication tokens
are not inherited.

`CHIRPS_POSTPUBLISH_EVIDENCE_DIR` must identify a new output directory outside
the publisher's disposable scratch directory. The workflow preserves that
directory as an artifact even on failure. A registry failure stops before OCI
copy or GitHub Release promotion. The already uploaded crates remain a partial
publication; a resume verifies their checksums and repeats both consumer builds.

New GitHub releases remain drafts until the dedicated assets API has been read
through every page and its exact inventory, IDs, sizes, and streamed bytes match
the stored bundle. A second snapshot rejects changes during verification.

## Trusted performance replay

The CI gate builds only the `chirps-durable-perf` verification executable from
the clean checkout of the workflow's exact `github.sha`. Its source/tree/lock,
platform, binary, and build-log hashes accompany the executable in a same-run
artifact. Publication downloads this fixed artifact from its own workflow run,
not the candidate's supplied artifact run. `CHIRPS_PERF_VERIFIER` points to that
binary and `CHIRPS_RELEASE_TOOLS_COMMIT` pins the workflow source.

The central verifier rechecks the tool identity before and after invoking
`--mode verify`. This replays A/A bounds, all safety controls, the freeze record,
broker readback, and paired results. It does not run the candidate's audit probe.
Both the CI gate and protected publisher keep candidate source identity separate
from trusted release tooling, including during recovery of an older candidate.

The mandatory `compatibility-matrix` entry replays all 34 API comparisons against
immutable v0.6.1 Git objects, all eight legacy wire/runtime cells, the public
facade against the unmodified official baseline, and both complete compatible
server lanes. Source tree/lock, corpus, and environment bindings are checked
across the cells. API success alone or a lower-level official connection probe
cannot qualify the matrix; see [compatibility evidence](v07-compatibility-evidence.md).

## Development fixtures

Synthetic Cargo/tool/API fixtures are explicitly labeled and exercise parser,
source, failure, resume, pagination, and workflow bindings. The loopback-only
`fixture-registry` phase is distinct from `registry` and is rejected as production
registry evidence. Fixture success does not certify live performance or a
published release.
