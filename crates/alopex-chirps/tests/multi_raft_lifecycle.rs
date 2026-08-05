#![cfg(feature = "multi-raft")]

use alopex_chirps::multi_raft::{GroupId, MultiRaftError, MultiRaftManager, WalRaftStorageFactory};
use alopex_chirps::{ChirpsRaftTransport, RaftConfig};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_mock::{MockBackend, MockNetwork};
use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
use alopex_chirps_raft_storage::types::LogId;
use alopex_chirps_raft_storage::wal_storage::WalStorageConfig;
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::Barrier;

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

async fn manager(
    root: &std::path::Path,
    raft_config: RaftConfig,
) -> Arc<MultiRaftManager<WalRaftStorageFactory<EchoStateMachine>>> {
    let factory = Arc::new(WalRaftStorageFactory::new(
        WalStorageConfig {
            wal_dir: root.join("wal"),
            snapshot_dir: root.join("snapshot"),
            ..WalStorageConfig::default()
        },
        1,
    ));
    let network = MockNetwork::new();
    let backend = network
        .add_node(NodeId::from([0; 16]), MockBackend::ephemeral_addr())
        .await;
    let backend: Arc<dyn MessageBackend> = Arc::new(backend);
    let transport = Arc::new(ChirpsRaftTransport::new(backend, GroupId(0), 1));
    Arc::new(MultiRaftManager::new(transport, factory, raft_config))
}

#[tokio::test]
async fn node_initialization_failure_rolls_back_registry_and_group_paths() {
    let root = tempfile::tempdir().unwrap();
    let manager = manager(
        root.path(),
        RaftConfig {
            node_id: 1,
            election_timeout_ms: 0,
            ..RaftConfig::default()
        },
    )
    .await;

    let result = manager
        .create_group(GroupId(11), BTreeSet::from([1]), EchoStateMachine)
        .await;

    assert!(matches!(
        result,
        Err(MultiRaftError::NodeInitialization {
            group_id: GroupId(11),
            ..
        })
    ));
    assert_eq!(manager.groups_count(), 0);
    assert!(!root.path().join("wal/groups/000000000000000b").exists());
    assert!(
        !root
            .path()
            .join("snapshot/groups/000000000000000b")
            .exists()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_remove_and_tick_do_not_mutate_an_unrelated_group() {
    let root = tempfile::tempdir().unwrap();
    let manager = manager(
        root.path(),
        RaftConfig {
            node_id: 1,
            ..RaftConfig::default()
        },
    )
    .await;
    for group_id in [GroupId(1), GroupId(2)] {
        manager
            .create_group(group_id, BTreeSet::from([1]), EchoStateMachine)
            .await
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(3));
    let tick = {
        let manager = Arc::clone(&manager);
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            manager.tick_all().await
        })
    };
    let remove = {
        let manager = Arc::clone(&manager);
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            manager.remove_group(GroupId(1)).await
        })
    };
    barrier.wait().await;

    let tick_results = tick.await.unwrap();
    assert!(remove.await.unwrap().unwrap());
    assert!(
        tick_results
            .iter()
            .any(|result| result.group_id == GroupId(2))
    );
    assert!(
        tick_results
            .iter()
            .filter(|result| result.group_id == GroupId(2))
            .all(|result| result.result.is_ok())
    );
    assert!(manager.get_group(GroupId(1)).is_none());
    assert!(manager.get_group(GroupId(2)).unwrap().is_accepting());

    let remaining = manager.tick_all().await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].group_id, GroupId(2));
    assert!(remaining[0].result.is_ok());
    manager.shutdown_all().await.unwrap();
}
