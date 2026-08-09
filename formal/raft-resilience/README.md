# Raft resilience model

The model bounds a three-voter execution to ten transitions and checks that
minority/stale attempts cannot advance the single committed history and that
applied indexes never exceed it. Election liveness and exhaustive five-node
schedules remain outside this bounded claim.

Run from `formal/`:

```sh
docker compose -f compose.yml run --rm apalache typecheck raft-resilience/model.tla
docker compose -f compose.yml run --rm apalache check --config=raft-resilience/RaftResilience.cfg --length=10 raft-resilience/model.tla
```
