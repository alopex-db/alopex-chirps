#![cfg(all(feature = "multi-raft", feature = "snapshot", feature = "tso"))]

use alopex_chirps::snapshot::SnapshotTransferConfig;
use alopex_chirps::tso::{ChirpsTsoTransport, TsoClientConfig, TsoConfig};
use alopex_chirps::{ChirpsMetricsCollector, MeshHandle, RaftNode};
use std::time::Duration;

#[test]
fn public_work_order_two_apis_exist() {
    let _ = MeshHandle::raft_transport;
    let _ = ChirpsTsoTransport::new;
    let _ = RaftNode::is_started;

    let _ = TsoConfig {
        batch_size: 10_000,
        prefetch_threshold: 1_000,
        timestamp_ttl: Duration::from_secs(3),
    };
    let _ = TsoClientConfig {
        batch_size: 10_000,
        prefetch_threshold: 1_000,
        max_retries: 10,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_secs(1),
    };
    let _ = SnapshotTransferConfig {
        chunk_size: 1024,
        chunk_threshold: 10 * 1024,
        max_concurrent_chunks: 4,
        max_retries: 3,
        transfer_timeout: Duration::from_secs(60),
    };
}

#[test]
fn v05_raft_metric_names_are_exported_alongside_v06_names() {
    let collector = ChirpsMetricsCollector::new();
    collector.update(&alopex_chirps::RaftMetricsUpdate {
        group_id: alopex_chirps::raft::GroupId(7),
        node_id: 1,
        term: 3,
        state: openraft::ServerState::Leader,
        commit_index: Some(11),
        applied_index: Some(10),
        last_log_index: Some(12),
        leader_id: Some(1),
        votes_granted: Some(2),
        log_entries_count: Some(12),
        snapshot_total: 1,
        proposals_total: 1,
        proposals_failed_total: 1,
        ..Default::default()
    });

    let body = collector.encode().unwrap();
    for name in [
        "raft_state",
        "raft_term",
        "raft_commit_index",
        "raft_applied_index",
        "raft_last_log_index",
        "raft_leader_id",
        "raft_votes_granted",
        "raft_log_entries_count",
        "raft_snapshot_total",
        "raft_proposals_total",
        "raft_proposals_failed_total",
    ] {
        assert!(body.contains(name), "missing v0.5 metric {name}: {body}");
    }
}
