#![cfg(feature = "multi-raft")]

use alopex_chirps::multi_raft::{
    GroupId, MultiRaftError, RaftStorageFactory, WalRaftStorageFactory, group_namespace,
    parse_group_namespace,
};
use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
use alopex_chirps_raft_storage::types::LogId;
use alopex_chirps_raft_storage::wal_storage::WalStorageConfig;
use async_trait::async_trait;
use std::io::Cursor;

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
