# Transport QoS model

The model bounds each class queue and exercises weighted service under mixed
enqueue pressure. It proves only finite queue and wait bounds; the executable
component tests provide the local scheduler/backpressure refinement evidence.

Run from `formal/`:

```sh
docker compose -f compose.yml run --rm apalache typecheck transport-qos/model.tla
docker compose -f compose.yml run --rm apalache check --config=transport-qos/TransportQos.cfg --length=12 transport-qos/model.tla
```
