use alopex_chirps::raft::{
    GroupId, HlcMetricsUpdate, MetricsEndpointAuth, RaftMessageMetric, RaftMetricsCollector,
    RaftMetricsUpdate, TsoMetricsUpdate, serve_metrics_authorized,
};
use http::StatusCode;
use openraft::ServerState;

#[test]
fn registry_output_reflects_raft_tso_hlc_and_snapshot_state() {
    let collector = RaftMetricsCollector::new();
    collector.set_groups_total(2);
    collector.update(&RaftMetricsUpdate {
        group_id: GroupId(7),
        state: ServerState::Leader,
        term: 11,
        commit_index: Some(41),
        applied_index: Some(40),
        log_entries_count: Some(9),
        snapshot_total: 1,
        snapshot_size_bytes: Some(16 * 1024),
        proposals_total: 1,
        proposal_latency_seconds: Some(0.004),
        message_sent: Some(RaftMessageMetric::new("append_entries", 2)),
        message_received: Some(RaftMessageMetric::new("vote", 1)),
        ..Default::default()
    });
    collector.update_tso(&TsoMetricsUpdate {
        result: Some("success"),
        request_count: 1,
        request_latency_seconds: Some(0.002),
        allocated: 8,
        physical_time: Some(1_700_000_000_000),
        logical_counter: Some(3),
        batch_size: Some(8),
    });
    collector.update_hlc(&HlcMetricsUpdate {
        ticks: 1,
        receive_result: Some("success"),
        receives: 1,
        clock_skew_seconds: Some(0.025),
        logical_advances: 2,
        physical_advances: 1,
    });

    let body = collector
        .encode()
        .expect("Prometheus encoding must succeed");
    for expected in [
        "chirps_raft_groups_total 2",
        "chirps_raft_state{group_id=\"7\",state=\"leader\"} 1",
        "chirps_raft_term{group_id=\"7\"} 11",
        "chirps_raft_commit_index{group_id=\"7\"} 41",
        "chirps_raft_applied_index{group_id=\"7\"} 40",
        "chirps_raft_proposals_total{group_id=\"7\",result=\"success\"} 1",
        "chirps_raft_messages_sent_total{group_id=\"7\",msg_type=\"append_entries\"} 2",
        "chirps_raft_messages_received_total{group_id=\"7\",msg_type=\"vote\"} 1",
        "chirps_raft_log_entries{group_id=\"7\"} 9",
        "chirps_raft_snapshot_total{group_id=\"7\"} 1",
        "chirps_raft_snapshot_size_bytes{group_id=\"7\"} 16384",
        "chirps_tso_requests_total{result=\"success\"} 1",
        "chirps_tso_allocated_total 8",
        "chirps_tso_physical_time 1700000000000",
        "chirps_tso_logical_counter 3",
        "chirps_hlc_ticks_total 1",
        "chirps_hlc_receives_total{result=\"success\"} 1",
        "chirps_hlc_logical_advances_total 2",
        "chirps_hlc_physical_advances_total 1",
    ] {
        assert!(
            body.contains(expected),
            "missing metric: {expected}\n{body}"
        );
    }
    assert!(body.contains("chirps_raft_proposals_latency_seconds_count{group_id=\"7\"} 1"));
    assert!(body.contains("chirps_tso_request_latency_seconds_count 1"));
    assert!(body.contains("chirps_tso_batch_size_count 1"));
    assert!(body.contains("chirps_hlc_clock_skew_seconds_count 1"));
}

#[test]
fn protected_endpoint_rejects_missing_or_invalid_token() {
    let collector = RaftMetricsCollector::new();
    collector.set_groups_total(1);
    let auth = MetricsEndpointAuth::bearer("correct horse battery staple").unwrap();

    for supplied in [None, Some("Bearer wrong"), Some("Basic ignored")] {
        let response = serve_metrics_authorized(&collector, &auth, supplied);
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(!response.body().contains("chirps_"));
    }

    let response = serve_metrics_authorized(
        &collector,
        &auth,
        Some("Bearer correct horse battery staple"),
    );
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.body().contains("chirps_raft_groups_total 1"));
}

#[test]
fn public_endpoint_remains_backward_compatible() {
    let collector = RaftMetricsCollector::new();
    collector.set_groups_total(1);
    let response = serve_metrics_authorized(&collector, &MetricsEndpointAuth::Public, None);
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.body().contains("chirps_raft_groups_total 1"));
}

#[test]
fn grafana_dashboard_is_valid_and_queries_every_required_family() {
    let dashboard: serde_json::Value = serde_json::from_str(include_str!(
        "../../../docs/observability/grafana/chirps-v0.6.json"
    ))
    .expect("dashboard must be valid JSON");
    let rendered = dashboard.to_string();
    for metric in [
        "chirps_raft_state",
        "chirps_raft_term",
        "chirps_raft_commit_index",
        "chirps_raft_applied_index",
        "chirps_raft_proposals_total",
        "chirps_raft_messages_sent_total",
        "chirps_raft_log_entries",
        "chirps_raft_snapshot_total",
        "chirps_tso_requests_total",
        "chirps_tso_physical_time",
        "chirps_tso_logical_counter",
        "chirps_tso_batch_size",
        "chirps_hlc_ticks_total",
        "chirps_hlc_receives_total",
        "chirps_hlc_logical_advances_total",
        "chirps_hlc_physical_advances_total",
    ] {
        assert!(rendered.contains(metric), "dashboard omits {metric}");
    }
    assert!(!rendered.contains("\"unit\":\"Bps\""));
    assert!(rendered.contains("\"unit\":\"bytes\""));
}
