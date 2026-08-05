# Chirps Multi-Raft finite model

This model checks the v0.6 group lifecycle, group-scoped routing, storage
isolation, and drain-before-shutdown contracts before production implementation.

## Local command

```bash
cd formal/multi-raft
docker compose run --rm apalache typecheck model.tla
docker compose run --rm apalache check --config=MultiRaft.cfg --length=10 model.tla
```

The bounded state space contains two groups, at most one route and one tick
concurrently in flight per group, and ten transitions. Ten covers the longest
critical sequence: create, route and tick, concurrent remove, drain both kinds
of work, shutdown, and final removal. A
successful result does not prove unbounded liveness, Raft consensus,
physical-network behavior, or performance. The image digest, constants, and
source/test refinements are fixed in `catalog.yaml`.
