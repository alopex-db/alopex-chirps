# v0.7 main integration verification plan

Target: `c2ca3b2` durable branch plus `origin/main` at `9b12624`.

The integration preserves the v0.7 durable facade and endpoint resolver while
restoring v0.6.3 mutual TLS, transport health and memory bounds, shared memory
APIs, Multi-Raft/TSO mesh wiring, and release approval freshness checks.

Before builds, the selected failure scenarios and checks are:

1. A resolved endpoint cannot impersonate a different handshake node: QUIC
   `connectivity_tests`, followed by `issue54_mtls` and `issue42_health_check`.
2. The scheduler retains both deterministic fairness and main's dispatch-p99
   assertions: `qos_integration`. The wall-clock throughput test previously
   removed by v0.7 is not reinstated; no performance claim is made.
3. Memory and Raft/TSO public wiring remain available alongside the durable
   API: facade and affected memory/Multi-Raft component tests after transport
   prerequisites pass.
4. Historical release tooling and exact-byte v0.7 publication coexist:
   release integrity tests, v0.7 structure gate, publication workflow validator,
   and registry static validation. All protected publication jobs retain the
   stale approval check; no workflow dispatch or publication is performed.

Commands run sequentially using native Cargo. Coupling analysis is scoped to
transport and facade before scoped mutation testing. No workspace build or
workspace mutation campaign is planned. Linux Iggy E2E and release evidence
remain separate prerequisites and are not inferred from local unit results.

## Recorded local results

- Formatting and whitespace checks passed.
- QUIC endpoint/handshake component tests: 2 passed. The zero-capacity
  regression verifies that resolver construction preserves the caller's queue
  validation instead of silently substituting the transport default.
- Mutual TLS, stale-peer health check, and both deterministic QoS tests:
  4 passed.
- Durable facade, memory API, v0.6.3 public contracts, and Multi-Raft routing:
  11 passed with all facade features enabled.
- Release integrity: 10 passed. Candidate/tooling checkout isolation: 4 passed,
  including rejection of a substituted candidate revision, incorrect working
  directory, and historical release-tool revision.
- v0.7 structure gate, exact-byte publication fixture, and feature-on registry
  static verification passed. The publication fixture used only its local HTTP
  server; no release, tag, registry upload, or workflow dispatch occurred.

### Test-gap decision

Target: `alopex-chirps-transport-quic/src/lib.rs` resolver constructor and
`remote_identity_matches`.

Mutation check: `cargo mutants --package alopex-chirps-transport-quic --file
crates/chirps-transport-quic/src/lib.rs --re remote_identity_matches --exclude-re
'delete field' --no-config --in-place --baseline skip --timeout 60
--cargo-test-arg=--lib --cargo-test-arg=connectivity_tests`.

The installed tool also generated one constructor field-deletion mutation despite
these name filters. The first run caught all three identity mutations and missed
the queue-capacity deletion with identity-only tests. After adding the focused
zero-capacity regression, the repeated four-mutant run caught all four. No
survivors were excluded as equivalent. No unsafe production code was changed;
Miri and Kani were not required for this merge.

### Coupling review and limits

`cargo coupling` ran separately on `crates/chirps-transport-quic` and
`crates/alopex-chirps`, with `--changed-since main --impact-depth 4 --json`.
The transport report covered 21 modules and the facade report 61 modules.
Reports used AST fallback because workspace metadata tried to fetch missing
platform-specific dependencies in the restricted network environment.

Accepted strong coupling: the transport root wires config/handshake/receive/
reconnect and the public facade wires durable/Multi-Raft/TSO. The reports identify
transport root and facade/durable as hotspots; this integration preserves those
existing public boundaries rather than refactoring them during a release merge.
Newly combined impact paths include Mesh constructors to durable, Multi-Raft
manager, and TSO client. A later dedicated change can split orchestration from
configuration and TLS material handling, with focused behavior tests first.

The tool cannot establish runtime identity/order/timing properties, macro or cfg
expansion, organizational distance, or all implicit duplicated logic. Focused
runtime tests above cover selected behaviors, not those entire blind spots.
Linux Iggy E2E, physical-node performance, complete workspace tests, and a full
release evidence bundle are not established by these local results.

### Scoped commands and lint status at merge

```console
cargo fmt --all -- --check
cargo test --locked --offline -p alopex-chirps-transport-quic --lib connectivity_tests
cargo test --locked --offline -p alopex-chirps-transport-quic --test issue54_mtls --test issue42_health_check --test qos_integration -- --test-threads=1
cargo test --locked -p alopex-chirps --all-features --test durable_facade --test issue46_memory_api --test issue_v063_work_order2 --test multi_raft_transport -- --test-threads=1
cargo clippy --locked --offline -p alopex-chirps-transport-quic --lib --test issue54_mtls --test issue42_health_check --test qos_integration -- -D warnings
python3 -m unittest scripts/tests/test_release_integrity.py scripts/tests/test_v07_workflow_integration.py
bash scripts/verify-release-contract.sh --publication-workflow
bash scripts/run-v0.7-release-gate.sh --structure-only
bash scripts/release/test-publish-v0.7-bundle.sh
bash scripts/verify-registry-dependency.sh --version 0.7.0 --feature-on --registry-only --static-only
```

The checks above passed within the stated local scope. The following facade lint
check failed at merge; this is not hidden by the passing transport lint:

```console
cargo clippy --locked --offline -p alopex-chirps --all-features --lib --test durable_facade --test issue46_memory_api --test issue_v063_work_order2 --test multi_raft_transport -- -D warnings
```

It reported six pre-existing backend lints: `runtime.rs` nested conditions at
lines 528 and 1373, unspecified truncation at line 547, let-and-return at line
1069, redundant conversion at line 1478, and `state/compaction.rs` tuple type
complexity at line 670. Those files are identical to the original v0.7 branch
in this merge; a separate focused change will address them.

## Backend lint follow-up plan

The follow-up preserves runtime behavior while addressing the six reported
lints. `.chirps-compaction.lock` is an advisory ownership lock, so opening it
must retain existing contents (`truncate(false)`), including before ownership
has been acquired. Nested conditional simplification preserves short-circuit
order; the compaction alias preserves the exact tuple type.

Focused checks: backend `runtime::tests` covers capacity ownership and shutdown,
and `state::compaction::tests` covers materialization and recovery. Repeat the
same facade lint command after these changes. No additional mutation campaign
is required for these equivalent expression/type cleanups. Initial backend unit
compilation exposed a pre-existing external specification dependency: corpus
tests include requirements/design files outside the Git repository. That
prerequisite must be restored from the original files without substituting
fixtures or skipping tests.

### Backend follow-up results

After restoring the original requirements/design files to their expected local
workspace location, the focused backend checks passed:

```console
cargo test --locked --offline -p alopex-chirps-backend-iggy --lib runtime::tests -- --test-threads=1
cargo test --locked --offline -p alopex-chirps-backend-iggy --lib state::compaction::tests -- --test-threads=1
```

Runtime: 4 passed. Compaction: 8 passed, including all cutover faults,
materialization, and reproducible provisional corpus generation. The corpus
check requires `CARGO_TARGET_DIR` to name the local build directory. Original
specification files remain outside the repository and are not republished here.

The exact facade scoped Clippy command recorded above now passes with
`-D warnings`. Final formatting and whitespace checks also pass. The earlier
lint and missing-spec failures remain recorded above; these later successful
runs address their identified causes. The pure lint follow-up introduced no
new behavior or dependency path and required no additional mutation campaign.
