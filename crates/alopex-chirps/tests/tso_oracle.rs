#![cfg(feature = "tso")]

mod support;

use alopex_chirps::multi_raft::GroupId;
use alopex_chirps::tso::{
    NodeAuthenticator, TSO_GROUP_ID, TimestampOracle, TsoConfig, TsoError, TsoRequest, TsoService,
    TsoStateMachine,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use support::{ManualClock, leader_oracle, manager};

#[tokio::test]
async fn leader_allocates_monotonic_non_overlapping_ranges() {
    let root = tempfile::tempdir().unwrap();
    let manager = manager(root.path(), 1).await;
    let state_machine = TsoStateMachine::default();
    let clock = Arc::new(ManualClock::new(1_000));
    let oracle = leader_oracle(&manager, state_machine.clone(), clock).await;
    let before = manager
        .get_group(TSO_GROUP_ID)
        .unwrap()
        .metrics()
        .last_applied;

    let first = oracle.get_timestamps(3).await.unwrap();
    let second = oracle.get_timestamps(2).await.unwrap();

    assert_eq!(first.count, 3);
    assert_eq!(second.count, 2);
    assert!(first.end < second.start);
    let committed = state_machine.state().await;
    assert_eq!(committed.last, Some(second.end));
    assert_eq!(committed.committed_ranges, 2);
    assert!(
        manager
            .get_group(TSO_GROUP_ID)
            .unwrap()
            .metrics()
            .last_applied
            > before,
        "allocation must pass through and apply a real Raft log entry"
    );
    manager.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn follower_returns_not_leader_without_allocating() {
    let root = tempfile::tempdir().unwrap();
    let manager = manager(root.path(), 2).await;
    let state_machine = TsoStateMachine::default();
    manager
        .create_group_uninitialized(TSO_GROUP_ID, state_machine.clone())
        .await
        .unwrap();
    let clock: Arc<dyn alopex_chirps::tso::Clock> = Arc::new(ManualClock::new(1_000));
    let oracle = TimestampOracle::new(
        2,
        manager.get_group(TSO_GROUP_ID).unwrap(),
        clock,
        TsoConfig::default(),
    )
    .unwrap();

    assert_eq!(oracle.get_timestamp().await, Err(TsoError::NotLeader(None)));
    assert_eq!(state_machine.state().await.committed_ranges, 0);
    manager.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn physical_clock_rollback_uses_logical_floor() {
    let root = tempfile::tempdir().unwrap();
    let manager = manager(root.path(), 1).await;
    let state_machine = TsoStateMachine::default();
    let clock = Arc::new(ManualClock::new(2_000));
    let oracle = leader_oracle(&manager, state_machine, Arc::clone(&clock)).await;

    let before_rollback = oracle.get_timestamp().await.unwrap();
    clock.set(1_900);
    let after_rollback = oracle.get_timestamp().await.unwrap();

    assert!(after_rollback > before_rollback);
    assert_eq!(after_rollback.physical, before_rollback.physical);
    assert!(after_rollback.logical > before_rollback.logical);
    manager.shutdown_all().await.unwrap();
}

struct TokenAuthenticator;

#[async_trait]
impl NodeAuthenticator for TokenAuthenticator {
    async fn authenticate(&self, node_id: u64, credential: &[u8]) -> bool {
        node_id == 7 && credential == b"node-7-secret"
    }
}

#[test]
fn request_debug_redacts_node_credential() {
    let request = TsoRequest {
        requester: 7,
        credential: b"node-7-secret".to_vec(),
        count: 1,
    };
    let debug = format!("{request:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("node-7-secret"));
}

#[tokio::test]
async fn unauthenticated_request_is_rejected_before_raft_allocation() {
    let root = tempfile::tempdir().unwrap();
    let manager = manager(root.path(), 1).await;
    let state_machine = TsoStateMachine::default();
    let oracle = leader_oracle(
        &manager,
        state_machine.clone(),
        Arc::new(ManualClock::new(3_000)),
    )
    .await;
    let service = TsoService::new(Arc::new(oracle), Arc::new(TokenAuthenticator));

    let rejected = service
        .get_timestamps(TsoRequest {
            requester: 7,
            credential: b"wrong".to_vec(),
            count: 2,
        })
        .await;
    assert_eq!(rejected, Err(TsoError::Unauthenticated { node_id: 7 }));
    assert_eq!(state_machine.state().await.committed_ranges, 0);

    let accepted = service
        .get_timestamps(TsoRequest {
            requester: 7,
            credential: b"node-7-secret".to_vec(),
            count: 2,
        })
        .await
        .unwrap();
    assert_eq!(accepted.count, 2);
    manager.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn oracle_rejects_non_dedicated_group() {
    let root = tempfile::tempdir().unwrap();
    let manager = manager(root.path(), 1).await;
    let data_group = GroupId(42);
    manager
        .create_group(
            data_group,
            std::collections::BTreeSet::from([1]),
            TsoStateMachine::default(),
        )
        .await
        .unwrap();
    let clock: Arc<dyn alopex_chirps::tso::Clock> = Arc::new(ManualClock::new(1_000));

    let error = TimestampOracle::new(
        1,
        manager.get_group(data_group).unwrap(),
        clock,
        TsoConfig {
            timestamp_ttl: Duration::from_secs(3),
        },
    )
    .err()
    .expect("a data group must not be accepted as the TSO group");
    assert_eq!(
        error,
        TsoError::InvalidTsoGroup {
            expected: TSO_GROUP_ID,
            actual: data_group,
        }
    );
    manager.shutdown_all().await.unwrap();
}
