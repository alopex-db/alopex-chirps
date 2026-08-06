# Chirps v0.6 Prometheus metrics

`ChirpsMetricsCollector` is the canonical name for the single registry for
Multi-Raft, TSO, HLC, and snapshot observations. `RaftMetricsCollector` remains
as a backward-compatible alias. `serve_metrics_authorized` adapts that registry
to an HTTP `/metrics` handler and returns Prometheus text format 0.0.4.

## Endpoint policy

Production deployments should use bearer authentication and TLS at the server
or ingress boundary:

```rust,ignore
use alopex_chirps::{ChirpsMetricsCollector, MetricsEndpointAuth, serve_metrics_authorized};

let collector = ChirpsMetricsCollector::new();
let policy = MetricsEndpointAuth::bearer(load_secret_from_runtime())?;
let response = serve_metrics_authorized(
    &collector,
    &policy,
    request.headers().get("Authorization").and_then(|v| v.to_str().ok()),
);
```

Missing or invalid credentials return `401` without metric text. Secrets are
redacted from the policy's `Debug` output. The legacy `serve_metrics` function
remains a public-mode wrapper for local or externally protected deployments.

## Metric families

The public names are the Requirement 7 `chirps_raft_*`, `chirps_tso_*`, and
`chirps_hlc_*` families. Raft state is a one-hot gauge labelled by `group_id`
and `state`. Proposal results, message types, TSO results, HLC results, and
failure reasons are mapped to finite label sets; arbitrary error strings are
never used as labels.

## HLC production wiring

Enable the `hlc` feature and pass the shared registry when starting the mesh:

```rust,ignore
use alopex_chirps::{ChirpsMetricsCollector, NodeConfig, start_with_metrics};
use std::sync::Arc;

let metrics = Arc::new(ChirpsMetricsCollector::new());
let mesh = start_with_metrics(NodeConfig::default(), Arc::clone(&metrics)).await?;
```

This connects actual `LocalHlc::tick` and `LocalHlc::receive` outcomes,
including rejected future skew, to the `chirps_hlc_*` families. Receive labels
are selected from bounded enums (`success` or `skew_error`); clock skew is a
numeric histogram observation. `start` and `LocalHlc::new` remain uninstrumented
and avoid Prometheus locking and dynamic dispatch on their hot paths. Direct HLC
users can opt in with `LocalHlc::with_metrics` or inject both a clock and a sink
with `LocalHlc::with_clock_and_metrics`.

`chirps_raft_groups_total` and `chirps_raft_group_states_total{state=...}` are
low-cardinality summaries. The default `MultiRaftConfig::max_groups` is 100.
Operators who explicitly raise it should estimate Prometheus series growth,
retain the summary panels, and narrow dashboard queries by node/instance at the
scrape layer. The collector does not add peer IDs, command contents, paths, or
user values as labels.

Counter fields in update structures are deltas. Gauges are the latest observed
state. The collector stores only a completely encoded response as its fallback;
an encoding failure cannot publish a mixed or partially updated body.

## Dashboard

Import [`grafana/chirps-v0.6.json`](grafana/chirps-v0.6.json). The dashboard has
a `group_id` variable and panels for group/state counts, proposal throughput and
P99 latency, Raft traffic, snapshot size, TSO allocation/latency, and HLC skew.

## Local verification

```sh
cargo test -p alopex-chirps --test metrics_api
cargo test -p alopex-chirps --features hlc --test hlc_metrics
cargo test -p alopex-chirps --features multi-raft --test multi_raft_lifecycle
```

The first test compares registry text with explicit Raft, TSO, HLC, and snapshot
updates and verifies authorization. The HLC component test drives actual tick,
receive, and skew-rejection paths through the registry. The final test attaches
the collector to a real `MultiRaftManager` and checks group-count changes at
create/shutdown boundaries.
