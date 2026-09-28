# Credential and diagnostics release evidence

The security report replays the actual production and fault
`durable_diagnostics/report.json` targets using the ordinary E2E validator.
It then requires all 23 production scenarios and four fault scenarios exactly
once, with their expected results. The production inventory includes all six
missing-credential controls, rejected runtime migration, and all seven snapshot
types over binary and HTTP for authorized admin and denied runtime principals,
including restart. Fault evidence includes both collector partial-failure
controls. Artifact kind and digest substitution must fail in both lanes.

The secret scan reads the retained runtime log and scenario JSONL for both
targets. Canary values come from the immutable candidate's Git objects, not
the verifier checkout or operator environment. Literal, base64, and hexadecimal
forms of the six fixture credentials and JWT canary are checked. Replay injects
each of these 21 representations into an in-memory negative control and requires
detection. Matching values are never printed in rejection messages.

This scan covers diagnostic runtime output. It is not a general secret scanner
for source archives, compiled binaries, arbitrary credentials, or every possible
encoding. Snapshot content and server stdout/stderr are checked inside the
actual fresh-process tests; the secret-bearing snapshots themselves are not
published. Other required session, TLS, permission and readback checks remain
in the complete E2E lanes and compatibility gate. A hash or `pass` label cannot
substitute for those executions.

After actual diagnostic target collection, seal a new report under the common
evidence root so both target directories are descendants of its parent:

```sh
python3 scripts/release/v07_security_evidence.py \
  --source-root "$CHIRPS_SOURCE_ROOT" --source-commit "$CANDIDATE_COMMIT" \
  --iggy-commit "$IGGY_COMMIT" \
  --production "$EVIDENCE_ROOT/production/durable_diagnostics/report.json" \
  --fault "$EVIDENCE_ROOT/fault/durable_diagnostics/report.json" \
  --output "$EVIDENCE_ROOT/security.json"
```

Use `--report "$EVIDENCE_ROOT/security.json"` instead of the three output/input
options to revalidate stored evidence without starting a broker or modifying
files. Reports and raw logs must retain trusted runner provenance; these checks
do not cryptographically attest that invented observations occurred.

Verification: nine Python tests pass, including actual candidate canary
extraction and synthetic raw E2E report composition, missing/duplicate/replaced
scenarios, rehashed credential leakage, disabled-scanner negative control,
altered scan summary, and input-size rejection. Synthetic fixtures qualify
the verifier's checks only. Actual server security qualification remains pending.

The central release verifier also requires each referenced diagnostic report to
have the same digest as the corresponding target in the complete production or
fault E2E lane. A separately passing diagnostic run from another binary or
configuration cannot be substituted into the candidate matrix.
