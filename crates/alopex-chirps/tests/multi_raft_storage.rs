#![cfg(feature = "multi-raft")]

use alopex_chirps::multi_raft::{
    GroupId, MultiRaftError, MultiRaftManager, RaftStorageFactory, WalRaftStorageFactory,
    group_namespace, parse_group_namespace,
};
use alopex_chirps::raft::{RaftFramePayload, RaftMetricsCollector};
use alopex_chirps::{ChirpsRaftTransport, RaftConfig, RaftMessage};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_mock::{MockBackend, MockNetwork};
use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
use alopex_chirps_raft_storage::types::{LogId, Vote, VoteRequest};
use alopex_chirps_raft_storage::wal_storage::WalStorageConfig;
use alopex_chirps_wire::frame::{Frame, RaftFrame};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use openraft::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;

#[derive(Default)]
struct EchoStateMachine;

#[async_trait]
impl StateMachine for EchoStateMachine {
    type Command = Vec<u8>;
    type Response = Vec<u8>;

    async fn apply(
        &mut self,
        _log_id: LogId<u64>,
        command: Self::Command,
    ) -> StateMachineResult<Self::Response> {
        Ok(command)
    }

    async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn restore(&mut self, _snapshot: Box<dyn AsyncSnapshotData>) -> StateMachineResult<()> {
        Ok(())
    }
}

#[test]
fn group_namespaces_are_canonical_and_isolated() {
    assert_eq!(group_namespace(GroupId(0)), "groups/0000000000000000");
    assert_eq!(group_namespace(GroupId(1)), "groups/0000000000000001");
    assert_eq!(
        group_namespace(GroupId(u64::MAX)),
        "groups/ffffffffffffffff"
    );
    assert_ne!(group_namespace(GroupId(1)), group_namespace(GroupId(2)));
}

#[test]
fn namespace_parser_rejects_non_canonical_or_path_like_input() {
    assert_eq!(
        parse_group_namespace("groups/000000000000002a").unwrap(),
        GroupId(42)
    );

    for invalid in [
        "groups/2a",
        "groups/000000000000002A",
        "groups/../000000000000002a",
        "/groups/000000000000002a",
        "other/000000000000002a",
        "groups/000000000000002a/child",
    ] {
        assert!(
            matches!(
                parse_group_namespace(invalid),
                Err(MultiRaftError::InvalidGroupId { .. })
            ),
            "accepted non-canonical namespace: {invalid}"
        );
    }
}

#[test]
fn lifecycle_errors_have_stable_serializable_shape() {
    let error = MultiRaftError::StorageCreation {
        group_id: GroupId(7),
        reason: "injected".to_owned(),
    };
    let value = serde_json::to_value(error).unwrap();
    assert_eq!(value["kind"], "storage_creation");
    assert_eq!(value["group_id"], 7);
    assert_eq!(value["reason"], "injected");
}

#[tokio::test]
async fn storage_factory_isolates_group_paths_and_abort_removes_partial_state() {
    let root = tempfile::tempdir().unwrap();
    let config = WalStorageConfig {
        wal_dir: root.path().join("wal"),
        snapshot_dir: root.path().join("snapshot"),
        ..WalStorageConfig::default()
    };
    let factory = WalRaftStorageFactory::<EchoStateMachine>::new(config, 9);

    let first = factory
        .begin_storage(GroupId(1), EchoStateMachine)
        .await
        .unwrap();
    let second = factory
        .begin_storage(GroupId(2), EchoStateMachine)
        .await
        .unwrap();

    assert_eq!(first.namespace(), "groups/0000000000000001");
    assert_eq!(second.namespace(), "groups/0000000000000002");
    assert_ne!(first.wal_dir(), second.wal_dir());
    assert_ne!(first.snapshot_dir(), second.snapshot_dir());
    assert!(first.wal_dir().exists());
    assert!(first.snapshot_dir().exists());

    let first_wal = first.wal_dir().to_owned();
    let first_snapshot = first.snapshot_dir().to_owned();
    first.abort().await.unwrap();
    assert!(!first_wal.exists());
    assert!(!first_snapshot.exists());

    second.abort().await.unwrap();
}

#[tokio::test]
async fn late_bound_metrics_observe_durable_snapshot_completion() {
    let root = tempfile::tempdir().unwrap();
    let config = WalStorageConfig {
        wal_dir: root.path().join("wal"),
        snapshot_dir: root.path().join("snapshot"),
        ..WalStorageConfig::default()
    };
    let factory = WalRaftStorageFactory::<EchoStateMachine>::new(config, 9);
    let transaction = factory
        .begin_storage(GroupId(19), EchoStateMachine)
        .await
        .unwrap();
    let mut storage = transaction.storage();

    let collector = Arc::new(RaftMetricsCollector::new());
    factory.set_snapshot_completion_hook(collector.clone());
    let mut builder = storage.get_snapshot_builder().await;
    builder.build_snapshot().await.unwrap();

    let body = collector.encode().unwrap();
    assert!(body.contains("chirps_raft_snapshot_total{group_id=\"19\"} 1"));
    assert!(body.contains("chirps_raft_snapshot_size_bytes{group_id=\"19\"} 0"));
    transaction.abort().await.unwrap();
}

#[tokio::test]
async fn manager_create_list_get_and_remove_are_consistent_and_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(WalRaftStorageFactory::<EchoStateMachine>::new(
        WalStorageConfig {
            wal_dir: root.path().join("wal"),
            snapshot_dir: root.path().join("snapshot"),
            ..WalStorageConfig::default()
        },
        1,
    ));
    let network = MockNetwork::new();
    let backend = network
        .add_node([0; 16].into(), MockBackend::ephemeral_addr())
        .await;
    let backend: Arc<dyn MessageBackend> = Arc::new(backend);
    let transport = Arc::new(ChirpsRaftTransport::new(backend, GroupId(0), 1));
    let manager = MultiRaftManager::new(
        transport,
        factory,
        RaftConfig {
            node_id: 1,
            ..RaftConfig::default()
        },
    );
    let members: BTreeSet<_> = [1].into_iter().collect();

    manager
        .create_group(GroupId(2), members.clone(), EchoStateMachine)
        .await
        .unwrap();
    manager
        .create_group(GroupId(1), members.clone(), EchoStateMachine)
        .await
        .unwrap();

    assert_eq!(manager.groups_count(), 2);
    assert_eq!(manager.list_groups(), vec![GroupId(1), GroupId(2)]);
    assert_eq!(
        manager.get_group(GroupId(1)).unwrap().group_id(),
        GroupId(1)
    );
    assert!(manager.get_group(GroupId(3)).is_none());
    assert!(matches!(
        manager
            .create_group(GroupId(1), members, EchoStateMachine)
            .await,
        Err(MultiRaftError::GroupAlreadyExists {
            group_id: GroupId(1)
        })
    ));

    assert!(manager.remove_group(GroupId(1)).await.unwrap());
    assert!(!manager.remove_group(GroupId(1)).await.unwrap());
    assert!(manager.get_group(GroupId(1)).is_none());
    assert_eq!(manager.list_groups(), vec![GroupId(2)]);

    let routed = manager
        .route_message(
            9,
            1,
            RaftFramePayload {
                correlation_id: 77,
                message: RaftMessage::Vote {
                    group_id: GroupId(2),
                    request: VoteRequest {
                        vote: Vote::new(2, 9),
                        last_log_id: None,
                    },
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(routed.source, 1);
    assert_eq!(routed.destination, 9);
    assert_eq!(routed.correlation_id, 77);
    assert_eq!(routed.message.group_id(), GroupId(2));

    let wire_payload = RaftFramePayload {
        correlation_id: 79,
        message: RaftMessage::Vote {
            group_id: GroupId(2),
            request: VoteRequest {
                vote: Vote::new(3, 9),
                last_log_id: None,
            },
        },
    };
    let wire_routed = manager
        .route_frame(
            wire_node_id(9),
            wire_node_id(1),
            Frame::Raft(RaftFrame {
                group_id: 2,
                payload: bincode::serialize(&wire_payload).unwrap(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(wire_routed.destination, 9);
    assert_eq!(wire_routed.correlation_id, 79);
    assert_eq!(wire_routed.message.group_id(), GroupId(2));

    assert!(matches!(
        manager
            .route_message(
                9,
                1,
                RaftFramePayload {
                    correlation_id: 78,
                    message: RaftMessage::Vote {
                        group_id: GroupId(3),
                        request: VoteRequest {
                            vote: Vote::new(2, 9),
                            last_log_id: None,
                        },
                    },
                },
            )
            .await,
        Err(MultiRaftError::UnknownGroup {
            group_id: GroupId(3)
        })
    ));
    manager.shutdown_all().await.unwrap();
}

fn wire_node_id(node_id: u64) -> NodeId {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&node_id.to_be_bytes());
    NodeId::from(bytes)
}
