# Storage recovery model

This finite model separates volatile writes, durable fsync state, temporary
snapshot construction, publication, corruption, rejection, and installation.
Length 10 is sufficient to compose every failure action with recovery or
installation; it does not claim physical-media ordering or unbounded liveness.

Run from `formal/`:

```sh
docker compose -f compose.yml run --rm apalache typecheck storage-recovery/model.tla
docker compose -f compose.yml run --rm apalache check --config=storage-recovery/StorageRecovery.cfg --length=10 storage-recovery/model.tla
```
