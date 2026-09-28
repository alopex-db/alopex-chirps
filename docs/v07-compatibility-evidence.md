# v0.7 compatibility evidence

Compatibility requires five reports for the same immutable Chirps candidate:

- Public API comparisons against the pinned v0.6.1 baseline.
- All eight feature/target cells of the legacy wire and runtime matrix.
- An actual public-facade probe against the unmodified official Iggy baseline.
- The complete production-compatible server E2E lane.
- The complete fault-artifact E2E lane.

`scripts/release/v07_compatibility_matrix.py` resolves each portable report and
replays its validator. It requires matching Git tree/lock identities, matching
production/fault corpus and environment, and distinct production/fault server
artifacts. Individual pass labels or API checks alone cannot complete this
matrix. The synthetic Python regression fixtures are not release evidence.

## Official baseline execution

After the separate official development binary and its manifest/build log have
been created, run from a clean committed source checkout on the Linux host:

```sh
python3 scripts/release/v07_official_run.py \
  --source-root "$CHIRPS_SOURCE_ROOT" \
  --official-manifest "$OFFICIAL_MANIFEST" \
  --server-build-log "$OFFICIAL_BUILD_LOG" \
  --output "$NEW_EVIDENCE_DIRECTORY" \
  --require-public
```

The collector builds the example, extracts its executable from Cargo JSON,
hashes the executable, and runs that executable against the manifest. It keeps
the raw build/run logs and the original server build log. It rejects changes to
the source, manifest, executable, or build log during execution. A failed build
or run leaves a failed execution record and cannot seal a compatibility cell.

The independent observation verifier reconstructs canonical envelopes and both
digests, then checks the independently polled bytes, message identities,
offsets, routes, payloads, explicit deduplication-off configuration, weak ACK
boundary, rejected strong request, and graceful cleanup.

Observation schema v1 records only `DevelopmentAppendConnection` and explicitly
does not validate public `DurableConfig`. Schema v2 is reserved for the actual
public `DurableConfig` path and records its public `Unavailable` error for a
strong request. The collector accepts either scope for development unless
`--require-public` is set; the complete compatibility matrix always requires v2.
Neither scope claims restart presence, strong receipts, or a publishable
official-server artifact.

Revalidate a stored execution without starting Cargo or a broker:

```sh
python3 scripts/release/v07_official_run.py \
  --source-root "$CHIRPS_SOURCE_ROOT" \
  --source-commit "$CANDIDATE_COMMIT" \
  --report "$EVIDENCE_DIRECTORY/execution.json" \
  --require-public
```

The raw reports are observations whose provenance must be supplied by the
trusted release runner; they are not signed attestations. Do not relabel an old
candidate's result as evidence for a new commit.

## Assemble the matrix

Place all five reports and their referenced files under one evidence directory.
Use `v07_compatibility_matrix.py --output NEW_MATRIX_JSON` with `--source-root`,
`--source-commit`, `--iggy-commit`, and the five report arguments `--api`, `--wire`,
`--official`, `--production`, and `--fault`. Sealing validates every cell before
writing the final report; an existing report is never overwritten. Use
`--report MATRIX_JSON` with the same immutable source arguments for read-only
revalidation.
