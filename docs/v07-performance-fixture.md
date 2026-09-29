# Owned server fixture for live performance collection

Use the production lane's existing manifest, executable, source-reconstruction,
and corpus verification before starting the fixture:

```sh
tests/e2e/scripts/run-v07-lane.sh --lane production \
  --perf-fixture-dir /absolute/new-fixture-directory \
  --fixture-lifetime-seconds 7200
```

The usual `CHIRPS_SERVER_MANIFEST`, `CHIRPS_ARTIFACT_KIND`,
`CHIRPS_REQUIRE_OUTPUT_DIGEST`, and `CHIRPS_LOCAL_CORPUS_ROOT` inputs remain
mandatory. This mode does not run or seal an E2E lane. It uses the verified
production executable to create a dedicated temporary server, provisions its
topic and least-privilege runtime user through the official SDK, stops it,
independently reads the persisted resource identity, and restarts under the
production profile.

Wait for `ready.json`, then read `fixture.json`. The descriptor includes the
actual server PID, executable/source identities, data root, startup file,
certificate references, checkpoint root, and independently derived partition
projection. It contains no passwords or private-key bytes. The runtime
credential is the isolated fixture's existing `RUNTIME_USERNAME` and
`RUNTIME_PASSWORD`; provide its JSON representation through the driver's
`CHIRPS_PERF_CREDENTIAL` environment reference. Never substitute a deployment
credential for this temporary fixture.

The configuration file explicitly disables message deduplication and is passed
as `IGGY_CONFIG_PATH`. The same file reference supplies the driver's
`broker_startup_config`. Remaining explicit `IGGY_*` overrides are included in
the collector's environment fingerprint. Use the verified production OCI
archive containing these exact server bytes when preparing the collector.

The parent retains ownership of the server. Creating `stop` in the output
directory requests graceful shutdown; the lifetime also ends it automatically.
`stopped.json` records whether shutdown required force. Do not reuse a stopped
descriptor or treat the readiness file as a performance result. Checkpoint
files remain in the output directory; the fixture's temporary server data and
private key are removed on normal exit. The lane runner retains its existing
owned-process cleanup trap for abnormal exits.

This preparation establishes neither an E2E pass nor performance acceptance.
The final candidate still requires A/A calibration, all eight live safety
controls, frozen bounds, paired collection, and independent replay.

## Development verification

Target: `chirps-e2e` fixture configuration and owned-child identity.
Scoped example Clippy and the existing fixture-library tests pass. A focused
regression checks unstarted, live, and exited child processes using a dedicated
test subprocess. Native Linux bootstrap and performance collection remain
separate runtime gates.

Mutation check: `cargo +stable mutants --in-place -p chirps-e2e --in-diff
<fixture.diff> -F running_process_id -- --lib
fixture_reports_only_its_live_owned_child` caught both selected mutants.
Survivors: none. Strengthening: focused owned-process regression. The tool
listed no mutations for the example target, so this result covers the library
helper only. Kani and Miri are not applicable to this safe process orchestration.

The scoped coupling check (`tests/e2e --changed-since main --impact-depth 4`)
reports the existing E2E library as one high-coupling integration module with
48 dependencies and no critical finding. Reusing its verified bootstrap and
resource projection is intentional. The analyzer does not inspect this example
as a workspace module and cannot establish process timing, ownership, or
cleanup; live Linux execution must cover those gaps. Extracting the fixture
lifecycle into a smaller test-support module remains a follow-up.
