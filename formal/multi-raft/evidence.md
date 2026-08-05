# Multi-Raft model evidence

Status: PASS (independently reviewed model, 2026-08-05 Asia/Tokyo)

## Bound

- checker: Apalache, pinned by digest in `compose.yml`
- groups: 2 (`g1`, `g2`)
- maximum in-flight work per group: one route and one tick concurrently
- maximum transition length: 10 (covers create -> route+tick -> concurrent remove -> drain -> shutdown -> final removal)
- modeled failures: storage/node/registration create abort, unknown routing,
  per-group tick failure, removal concurrent with in-flight route completion

## Result

```text
docker compose run --rm apalache typecheck model.tla
Type checker [OK]
APALACHE version: 0.58.3 | build: v0.58.3

docker compose run --rm apalache check --config=MultiRaft.cfg --length=10 model.tla
The outcome is: NoError
Checker reports no error up to computation length 10
EXITCODE: OK
```

The final ten-step check completed in 8 minutes 36 seconds with all nine
configured invariants enabled. Independent review first found missing tick
drain, weak namespace/unknown-route observations, missing idempotent rejection
actions, and an insufficient bound. After those were added, two counterexamples
identified stale diagnostic observations that incorrectly outlived unrelated
route/tick progress. The observation lifetimes were corrected, the model was
re-typechecked, and only the final ten-step `NoError` run is the PASS verdict.

## Properties and refinement

The normative mapping is `catalog.yaml`. It maps lifecycle publication and
rollback, canonical storage isolation, unknown-route isolation, independent
tick progress, and drain-before-shutdown to production source locations and the
smallest local component tests.

## Exclusions

This bounded check is not evidence for unbounded liveness, OpenRaft consensus
correctness, physical networking, multi-OS behavior, or the v0.6 performance
targets.
