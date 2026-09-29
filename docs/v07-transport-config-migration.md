# v0.7 transport resource configuration

v0.7 restores the v0.6.1 constructible shapes of `TransportConfigV04` and
`TransportMetricsSnapshot`. The extra fields introduced in v0.6.3 move to new
types. Rust cannot preserve exhaustive struct literals for both differing
shapes; v0.6.3 callers using the added fields must make the migration below.

Keep message scheduling, queues, retransmission, QoS and handshake settings in
`TransportConfigV04`. Move `stream_receive_window`, `receive_window`,
`send_window`, `max_concurrent_uni_streams`, `max_idle_timeout`,
`keep_alive_interval` and `max_connections` into `TransportResourceConfig`:

```rust,no_run
use alopex_chirps_transport_quic::{QuicBackend, TransportConfigV04, TransportResourceConfig};
# async fn configure(node_id: alopex_chirps_wire::node_id::NodeId,
#     config: std::sync::Arc<alopex_chirps_core::config::NodeConfig>) -> anyhow::Result<()> {
let resources = TransportResourceConfig {
    max_connections: 32,
    ..Default::default()
};
let backend = QuicBackend::new_with_resource_config(
    node_id, config, TransportConfigV04::default(), resources,
).await?;
let resources = backend.resource_metrics();
println!("active connections: {}", resources.active_connections);
# Ok(())
# }
```

Resolver users can call `new_with_config_and_resources_and_endpoint_resolver`.
Existing constructors continue to use the v0.6.3 production resource defaults:
16 MiB stream receive window, 64 MiB connection receive/send windows, 256
unidirectional streams, 30-second idle timeout, no keep-alive and 64 retained
connections. Connection rejection, idle eviction and health checks keep using
the supplied limits.

`metrics()` keeps its v0.6.1 seven-field result. Read the five newer counters
(`active_connections`, `active_streams`, `max_active_streams`,
`connection_rejections`, `idle_evictions`) through `resource_metrics()`, returning
`TransportResourceMetricsSnapshot`. Internal atomic counters and Prometheus
instruments remain connected to the same operations.

For a custom Quinn endpoint use
`TransportResourceConfig::file_transfer_performance().to_quinn_transport_config()`.
The corresponding methods on `TransportConfigV04` remain available and select
the default production resources. This change does not alter `MemoryConfig`,
`MemoryManager` or `get_memory_stats()`; it separates transport-specific limits
from the legacy message configuration.
