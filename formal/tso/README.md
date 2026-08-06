# TSO finite-state model

This model is the production-before contract for CHIRPS v0.6 Requirements 3
and 4. It separates the timestamp oracle into a dedicated Raft group, permits
only the lease-owning leader to allocate or issue timestamps, commits bounded
batch ranges, waits for the old lease during handoff, and preserves monotonicity
when the physical clock moves backwards. The client profile covers a stale
leader hint, a retryable transport failure, batch refill, and local cache use.

## Run

Run the shared, digest-pinned Apalache service from `formal/`:

```sh
docker compose -f compose.yml run --rm apalache typecheck tso/model.tla
docker compose -f compose.yml run --rm apalache check --config=tso/Allocation.cfg --length=6 tso/model.tla
docker compose -f compose.yml run --rm apalache check --config=tso/Follower.cfg --length=3 tso/model.tla
docker compose -f compose.yml run --rm apalache check --config=tso/Handoff.cfg --length=9 tso/model.tla
docker compose -f compose.yml run --rm apalache check --config=tso/Client.cfg --length=5 tso/model.tla
docker compose -f compose.yml run --rm apalache check --config=tso/Tso.cfg --length=5 tso/model.tla
```

`Tso.cfg` is the short exploratory combined-transition profile. The other four
profiles fix a reviewable failure/success path and bound.

## Scope and interpretation

The state space contains three nodes, one dedicated TSO group, batch sizes one
and two, physical clock values 0..3, scalar timestamp values 0..20, one client,
at most two retryable failures, and one handoff. A scalar physical floor stands
in for the production physical/logical timestamp encoding.

The profiles prove the configured invariants only within their stated finite
bounds. Handoff and retry paths are bounded reachability witnesses, not proofs
of unbounded liveness or fairness. This model does not prove Raft consensus or
durability, concurrent-client allocation, logical-counter overflow, real-time
lease precision, the duration of exponential backoff, transport behavior, or
throughput. Production paths and tests in `catalog.yaml` are planned refinement
targets; their presence here does not claim that implementation already exists.

The root `formal/compose.yml` is the canonical runner for new models. Existing
model-local Compose files remain unchanged and may be migrated separately.
