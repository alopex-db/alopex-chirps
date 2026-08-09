use alopex_chirps_raft_storage::traits::{
    AsyncSnapshotData, RaftStorage, StateMachine, StateMachineResult,
};
use alopex_chirps_raft_storage::types::{
    BasicNode, GroupId, LogId, SnapshotMeta, StoredMembership, Vote,
};
use alopex_chirps_raft_storage::wal_storage::{
    CURRENT_FORMAT_VERSION, WalRaftStorage, WalStorageConfig,
};
use async_trait::async_trait;
use std::io::Cursor;

#[derive(Default)]
struct TestStateMachine;

#[async_trait]
impl StateMachine for TestStateMachine {
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

fn config(root: &std::path::Path) -> WalStorageConfig {
    WalStorageConfig {
        wal_dir: root.join("wal"),
        snapshot_dir: root.join("snapshot"),
        ..WalStorageConfig::default()
    }
}

fn wal_path(config: &WalStorageConfig, group: GroupId, node: u64) -> std::path::PathBuf {
    config.wal_dir.join(format!("raft-{}-{node}.wal", group.0))
}

fn snapshot_meta(id: &str) -> SnapshotMeta<u64, BasicNode> {
    SnapshotMeta {
        last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(3, 7), 11)),
        last_membership: StoredMembership::default(),
        snapshot_id: id.to_owned(),
    }
}

#[tokio::test]
async fn corrupted_and_truncated_wal_are_rejected_during_recovery() {
    for truncate in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let group = GroupId(7);
        {
            let mut storage =
                WalRaftStorage::new(config.clone(), group, 7, TestStateMachine).unwrap();
            storage.save_vote(&Vote::new(4, 7)).await.unwrap();
        }
        let path = wal_path(&config, group, 7);
        let mut bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() > 8);
        if truncate {
            bytes.truncate(bytes.len() - 1);
        } else {
            let index = bytes.len() - 1;
            bytes[index] ^= 0xff;
        }
        std::fs::write(&path, bytes).unwrap();

        let recovered = WalRaftStorage::recover(config, group, 7, TestStateMachine);
        assert!(
            recovered.is_err(),
            "{} WAL must fail closed",
            if truncate { "truncated" } else { "corrupted" }
        );
    }
}

#[tokio::test]
async fn wal_format_mismatch_and_vote_regression_across_restart_are_observable() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let group = GroupId(8);
    {
        let mut storage = WalRaftStorage::new(config.clone(), group, 8, TestStateMachine).unwrap();
        storage.save_vote(&Vote::new(4, 8)).await.unwrap();
    }
    {
        let mut recovered =
            WalRaftStorage::recover(config.clone(), group, 8, TestStateMachine).unwrap();
        assert_eq!(recovered.read_vote().await.unwrap(), Some(Vote::new(4, 8)));
        recovered.save_vote(&Vote::new(5, 8)).await.unwrap();
    }
    let mut recovered =
        WalRaftStorage::recover(config.clone(), group, 8, TestStateMachine).unwrap();
    assert_eq!(recovered.read_vote().await.unwrap(), Some(Vote::new(5, 8)));
    drop(recovered);

    let mut incompatible = config;
    incompatible.format_version = CURRENT_FORMAT_VERSION + 1;
    assert!(WalRaftStorage::recover(incompatible, group, 8, TestStateMachine).is_err());
}

#[tokio::test]
async fn corrupted_truncated_and_version_mismatched_snapshots_are_never_returned() {
    for mutation in ["corrupt", "truncate", "version"] {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let group = GroupId(9);
        let meta = snapshot_meta(mutation);
        let mut storage = WalRaftStorage::new(config.clone(), group, 9, TestStateMachine).unwrap();
        storage
            .install_snapshot(&meta, Box::new(Cursor::new(vec![1, 2, 3, 4])))
            .await
            .unwrap();
        let path = config
            .snapshot_dir
            .join(format!("snapshot-{}-{}.alopex", group.0, meta.snapshot_id));
        let mut bytes = std::fs::read(&path).unwrap();
        match mutation {
            "corrupt" => {
                let index = bytes.len() / 2;
                bytes[index] ^= 0xff;
            }
            "truncate" => bytes.truncate(bytes.len() - 1),
            "version" => bytes[4..8].copy_from_slice(&2u32.to_le_bytes()),
            _ => unreachable!(),
        }
        std::fs::write(path, bytes).unwrap();
        assert!(
            storage.get_current_snapshot().await.is_err(),
            "{mutation} snapshot must fail closed"
        );
    }
}

#[cfg(unix)]
#[test]
fn read_only_wal_directory_reports_an_error() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    std::fs::create_dir_all(&config.wal_dir).unwrap();
    std::fs::set_permissions(&config.wal_dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    let result = WalRaftStorage::new(config.clone(), GroupId(10), 10, TestStateMachine);
    std::fs::set_permissions(&config.wal_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(result.is_err());
}
