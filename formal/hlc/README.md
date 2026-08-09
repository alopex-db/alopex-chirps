# HLC finite-state model

This model was the production-before contract and is now refined by the
CHIRPS v0.6 Requirement 5 production implementation mapped in `catalog.yaml`.
It captures the canonical Hybrid Logical Clock local tick and receive rules,
future-clock-skew rejection without mutation, physical clock rollback,
out-of-order SWIM/Gossip delivery, and duplicate application.

## Run

Run the shared, digest-pinned Apalache service from `formal/`:

```sh
docker compose -f compose.yml run --rm apalache typecheck hlc/model.tla
docker compose -f compose.yml run --rm apalache check --config=hlc/Tick.cfg --length=5 hlc/model.tla
docker compose -f compose.yml run --rm apalache check --config=hlc/Reorder.cfg --length=6 hlc/model.tla
docker compose -f compose.yml run --rm apalache check --config=hlc/Skew.cfg --length=2 hlc/model.tla
docker compose -f compose.yml run --rm apalache check --config=hlc/Hlc.cfg --length=4 hlc/model.tla
```

`Hlc.cfg` is the short exploratory combined-transition profile. The other
three profiles fix a reviewable behavior and bound.

## Scope and interpretation

The state space contains two nodes, two messages (one SWIM and one Gossip),
physical values 0..3, logical values 0..6, and a maximum accepted future skew
of one unit. Reorder covers Gossip arriving before the earlier SWIM event and a
duplicate Gossip delivery. Skew covers a future timestamp beyond the bound.

The profiles prove configured invariants only within these finite bounds. They
do not prove gossip convergence, SWIM failure-detection semantics, persistence,
arbitrary networks, past-skew policy, logical overflow, malicious peers,
unbounded liveness/fairness, or performance. Production paths, tests, and the
host-qualified Criterion evidence are mapped in `catalog.yaml`; the bounded
model still makes no unbounded or cross-host performance claim.

The root `formal/compose.yml` is the canonical runner for new models. Existing
model-local Compose files remain unchanged and may be migrated separately.
