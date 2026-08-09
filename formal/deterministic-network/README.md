# Deterministic network finite model

The Issue #3 model expands a fixed 23-event schedule before execution and
applies the same pure `Apply(state, event)` transition independently to an
original state and a replay state. It covers a composite
delay+duplicate+reorder packet, a later packet overtaking it, loss, asymmetric
partition/heal, a source-generation reconnect race, explicit same-time timeout
ordering, terminal queue classification, and one oracle check per event.
The pinned compose service allocates a 12 GiB JVM heap because Apalache inlines
the pure batched record transitions before checking them.

```bash
cd formal/deterministic-network
docker compose run --rm apalache --features=no-rows typecheck model.tla
docker compose run --rm apalache --features=no-rows check --config=DeterministicNetwork.cfg --length=8 model.tla
```

The three mutation configurations must each produce a counterexample. They
show that the partition, early-delivery, and stale-generation invariants are
capable of failing rather than being constant flags.

```bash
docker compose run --rm apalache --features=no-rows check --config=UnsafePartition.cfg --length=3 model.tla
docker compose run --rm apalache --features=no-rows check --config=UnsafeEarly.cfg --length=2 model.tla
docker compose run --rm apalache --features=no-rows check --config=UnsafeStale.cfg --length=5 model.tla
```

The longest path is the complete 23-event schedule grouped into eight atomic
checker transitions. Prefixes of two, three, and five transitions reach the
early, partition, and stale mutations respectively. This bounded
model does not prove real networking, unbounded liveness, Raft consensus, SWIM
convergence, JSON serialization, or minimizer 1-minimality; those are mapped to
local component/integration tests.
