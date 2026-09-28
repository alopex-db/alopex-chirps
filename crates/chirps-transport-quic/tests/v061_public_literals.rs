//! Downstream literals that compiled against the exact v0.6.1 public shapes.
use alopex_chirps_transport_quic::{TransportConfigV04, TransportMetricsSnapshot};
use std::time::Duration;

#[test]
fn v061_transport_configuration_literal_remains_constructible() {
    let config = TransportConfigV04 {
        send_timeout: Duration::from_millis(200),
        await_peer_stop: true,
        diagnostics_enabled: true,
        send_queue_capacity: 1024,
        priority: Default::default(),
        raft_stream_batch_size: 32,
        retransmit: Default::default(),
        qos: Default::default(),
        handshake: Default::default(),
    };
    assert!(config.to_quinn_transport_config().is_ok());
}

#[test]
fn v061_metrics_literal_remains_constructible() {
    let snapshot = TransportMetricsSnapshot {
        sent: 1,
        received: 2,
        dropped: 3,
        retried: 4,
        concurrent_sends: 5,
        max_concurrent_sends: 6,
        streams_opened: 7,
    };
    assert_eq!(snapshot.streams_opened, 7);
}
