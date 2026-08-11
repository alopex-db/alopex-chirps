use alopex_chirps::{ChirpsMetricsCollector, SwimMetricsUpdate, TransportMetricsUpdate};

#[test]
fn unified_registry_publishes_transport_and_swim_contract_series() {
    let collector = ChirpsMetricsCollector::new();
    collector.update_transport(&TransportMetricsUpdate {
        messages_sent: 3,
        messages_received: 2,
        bytes_sent: 300,
        bytes_received: 200,
        active_connections: 4,
    });
    collector.update_swim(&SwimMetricsUpdate {
        node_id: 7,
        state: "alive",
        event: "alive",
    });

    let output = collector.encode().unwrap();
    assert!(output.contains("chirps_transport_messages_sent_total"));
    assert!(output.contains("chirps_transport_messages_received_total"));
    assert!(output.contains("chirps_transport_bytes_sent_total"));
    assert!(output.contains("chirps_transport_connections_active"));
    assert!(output.contains("chirps_swim_node_state"));
    assert!(output.contains("chirps_swim_events_total"));
}
