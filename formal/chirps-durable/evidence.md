# Chirps Durable formal verification evidence

## Verdict

**PASS (2026-08-11 JST).** One catalog-driven Compose service validated and ran
all 4 explicit typechecks, all 4 normal models, all 125 declared unsafe RED
profiles, and all 27 reachability witnesses. The accepted 160-job run exited
`0` with no infrastructure retry.

This is evidence for the finite state spaces and exact inputs below. It is not
an infinite proof.

## Shared invocation

Run from the Chirps repository root:

```bash
rtk docker compose -f formal/compose.yml config --quiet
rtk docker compose -f formal/compose.yml run --rm \
  -e CHIRPS_DURABLE_MODE=catalog chirps-durable-suite
rtk docker compose -f formal/compose.yml run --rm chirps-durable-suite
```

The normative FORMAL lane was also exercised directly for each model with the
exact command shape below before the aggregate run; all four typechecks exited
`0`:

```bash
rtk docker compose -f formal/compose.yml run --rm apalache \
  typecheck chirps-durable/SendLease.tla
rtk docker compose -f formal/compose.yml run --rm apalache \
  typecheck chirps-durable/Subscription.tla
rtk docker compose -f formal/compose.yml run --rm apalache \
  typecheck chirps-durable/LifecycleState.tla
rtk docker compose -f formal/compose.yml run --rm apalache \
  typecheck chirps-durable/MetadataRecovery.tla
```

The service refuses pre-existing `formal/tmp` or `formal/_apalache-out`,
owns those generated paths, and removes them on exit. JVM fatal-error and replay
files are routed into the same task-owned `_apalache-out` tree, so the same
cleanup boundary covers them. It mounts no container socket, requests no
privileged mode, and performs no publication.

## Immutable suite inventory

| Model | Catalog | Bound | Required actions | RED profiles | Witnesses | Requirement mappings |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Send/lease | `catalog.yaml` | 24 | 39 | 28 | 1 | 34 |
| Subscription/checkpoint/liveness | `subscription-catalog.yaml` | 28 | 53 | 49 | 5 | 37 |
| Lifecycle/capacity/compaction | `lifecycle-catalog.yaml` | 24 | 43 | 26 | 8 | 15 |
| Metadata snapshot/WAL | `metadata-catalog.yaml` | 20 | 44 | 22 | 13 | 8 |
| **Total** | 4 catalogs | — | **179** | **125** | **27** | **94** |

Checker image:

`ghcr.io/apalache-mc/apalache@sha256:fde994fd109323934b9abb7ad169de37b29acf2141483367f2913cae30ff3795`

The declarative service checks every registered file and its expected SHA-256,
the exact model-specific requirement-ID set, exact bound, checker digest, model
ID, extracted `AllActions`, available action-catalog bijection, profile target
and unsafe mode, witness operator, requirement property and RED reference, and
every refinement/test repository, path, and task before model checking starts.
The six suite-level meta-properties are connected to executed registry,
scenario-matrix, action, or invariant-to-RED gates rather than accepted as free
text.

The runner enforces four barriers: all typechecks finish, then all normal
bounded checks finish, then all RED profiles finish, then all witnesses run.
Jobs inside a phase may use four workers. Profiles and witnesses retain their
catalog order and exact per-entry bounds; scheduling does not change a checker
command or expected result.

## Requirement and reverse-refinement coverage

| Requirement | Owning evidence |
| --- | --- |
| V7-MODEL-001 | Four-model registry and missing-model gate |
| V7-MODEL-002 | 179-action inventory and missing-action gate |
| V7-MODEL-003 | Send and subscription CFG invariant-to-RED completeness |
| V7-MODEL-004 | Metadata old/new-complete, WAL, corruption, and stage-crash model |
| V7-MODEL-005 | Lifecycle outcome-preserving shutdown model |
| V7-MODEL-006 | Subscription conditional liveness lane and assumption mutations |

Every one of the 94 catalog requirement entries must be in its model's exact
ID set and retain both a production `refinement` location and a `planned_test`
or `planned_local_component_test` location. Catalog SHA pinning protects the
approved reverse mapping, while repository-root, path-shape, and approved-task
validation rejects malformed or invented mapping coordinates. Planned paths
may identify later tasks whose files do not exist yet; the suite does not
pretend planned production code is already present.

## Negative completeness probes

The same service supports in-memory, non-writing validation probes through
`CHIRPS_DURABLE_PROBE`:

- `missing-tool`
- `missing-model`
- `missing-action`
- `missing-action-catalog`
- `missing-scenario-matrix`
- `missing-red-profile`
- `missing-refinement`
- `missing-requirement`
- `bad-property`
- `bad-repository`

Each probe must make catalog validation fail before Apalache jobs start. The
probe changes only the validator's observed input and never edits source files.

Run a probe by overriding the service environment, for example:

```bash
rtk docker compose -f formal/compose.yml run --rm \
  -e CHIRPS_DURABLE_MODE=catalog \
  -e CHIRPS_DURABLE_PROBE=missing-refinement chirps-durable-suite
```

## Infrastructure-failure boundary

A checker result is accepted directly only when its exit code equals the
catalog expectation (`0` for normal models and `12` for RED profiles and
witnesses). The only retryable infrastructure result is exit `134` accompanied
by the OpenJDK fatal-error marker. Such a job is queued once for a serial retry
after the parallel portion of the same phase and before that phase's barrier,
and the retry is printed in the aggregate output. Any other mismatch, or a
retry that does not produce the catalog expectation, fails the suite. Model
counterexamples are never retried or promoted.

## Exact inputs

| Artifact | SHA-256 |
| --- | --- |
| `SendLease.tla` | `decf0a86691a6ad8fd40aec667c57e6cae57ff0b844a1b479b9e951bec7d42b9` |
| `SendLease.cfg` | `012f62cc5d5a9a7da340bd872fe2341713b1975b5b267b37d5acdfed40106981` |
| `catalog.yaml` | `f65851a3665b9af700b6c91b8703cd34a219b53ff4d6af63463ceb3f5653235c` |
| `Subscription.tla` | `28e2eca3ca4d57b0b04176f0a7e3daa4c683464ee656a46f4e8c4d8d8347d401` |
| `Subscription.cfg` | `787497cc43ce006340dbcc101e171086657e9245286d3dcc05a91a1a21bea2a7` |
| `subscription-catalog.yaml` | `09cb487670418664994100dd8ddaddce3582f0c6eaf653e89bc2488c7437eb31` |
| `LifecycleState.tla` | `2190c9d037b88a10321f21e4e53971aa5e5e04ccabd511f35ba5517d62024ef3` |
| `LifecycleState.cfg` | `5b4af7f2c968e6763113713384c2cbfa9c15cecb24c88d72a84b0c8624b36091` |
| `lifecycle-catalog.yaml` | `6d900a8d3f329ca8d89997aed5ddcbfa1c443aafd346552e6119e3055396a52e` |
| `MetadataRecovery.tla` | `229ec5743cecf752d370b76ed10daea86a9893292cffb7ff58a5cd9e12cb151e` |
| `MetadataRecovery.cfg` | `8230048ebc266569422c7da7359bafc39550ddd6396db8ca5705a4a1ec4c57ad` |
| `metadata-catalog.yaml` | `cb9da8fa3d08cf0cd494d49c37b7c09a0265f786be3c1269dd281bd1a6770a5b` |
| `chirps-durable/compose.yml` | `da19cb3c3d8f903f972050ef8c2884e2706f89a0c6ef2e06ffc9ebe7242823d0` |
| root `formal/compose.yml` | `4b2758da1a8f00885c1700ff74645bb6cf0230f94550e54cd46256f1b8d61dfe` |

## Scope and omitted environment behavior

The checked domains are finite: attempts, offsets, owner/resource generations,
capacity, frames, crash stages, and computation lengths are exactly those
declared by the four catalogs. Conditional liveness assumes selected strong
records, broker and subscription availability, retention, fair poll/redelivery,
valid state, sufficient capacity, and unchanged owner/resource/inbox
generations.

The suite does not model or claim HA, replication, broker exactly-once, mTLS,
device power-loss durability, atomic data snapshots, unbounded fairness,
arbitrary filesystem schedules, or infinite execution. Physical TCP/TLS,
filesystem, compatible-server, component, fresh-process, and E2E behavior is
owned by the production and local-test refinements in later tasks. The pinned
Iggy source boundary remains
`f5350d999d883fd3ca9dd33b3dc2754ddb0df049`; it is a stage reference, not the
active compatible-server metadata store.

## Validation record

| Check | Observed result |
| --- | --- |
| Compose resolution | Exit `0` from `config --quiet` |
| Catalog-only validation | Exit `0`; 4 models, 125 profiles, 27 witnesses, 94 requirement mappings, V7-MODEL-001 through V7-MODEL-006 |
| Direct normative typechecks | All 4 exact `apalache typecheck FILE` commands exited `0` |
| Missing tool probe | Rejected before jobs, exit `2` |
| Missing model probe | Rejected before jobs, exit `9` |
| Missing action probe | Rejected before jobs, exit `255` |
| Missing action-catalog probe | Rejected by executed meta-property gate, exit `255` |
| Missing scenario-matrix probe | Rejected by executed meta-property gate, exit `255` |
| Missing RED profile probe | Rejected before jobs, exit `255` |
| Missing refinement probe | Rejected before jobs, exit `255` |
| Missing requirement probe | Rejected by exact requirement-set gate, exit `255` |
| Bad property probe | Rejected by property/meta-property resolution, exit `255` |
| Bad repository probe | Rejected by repository mapping validation, exit `255` |
| Shared full invocation | Container exit `0`; typecheck `4`, normal `4`, profiles `125`, witnesses `27`, workers `4`, infrastructure retries `0` |
| Phase ordering | Phase 1–4 each reported `PASS` before the next phase started |
| Cleanup | `formal/tmp`, `formal/_apalache-out`, `formal/hs_err_pid*.log`, `formal/replay_pid*.log`, and Compose task containers absent |

The accepted run used the exact Compose digests above and did not exercise the
bounded JVM retry path. Generated checker output is not retained in either
source repository.
