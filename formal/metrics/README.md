# Prometheus metrics state and authorization model

This production-before model defines the observable relationship between live
group state, the last successfully encoded registry snapshot, and an optional
bearer-token endpoint policy. A failed refresh may preserve an older successful
snapshot, but an endpoint response must never invent a future revision or mix
labels from different cached revisions. When authentication is enabled, missing
or invalid credentials cannot expose a body.

Run from `formal/`:

```sh
docker compose -f compose.yml run --rm apalache typecheck metrics/model.tla
docker compose -f compose.yml run --rm apalache check --config=metrics/AuthRequired.cfg --length=6 metrics/model.tla
docker compose -f compose.yml run --rm apalache check --config=metrics/Public.cfg --length=6 metrics/model.tla
```

The model is finite and does not prove exporter uptime, network security,
unbounded freshness, algorithm correctness, or performance. Production tests
must still compare actual registry text with Raft, TSO, HLC, and snapshot state.
