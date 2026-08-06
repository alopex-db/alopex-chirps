#![cfg(feature = "multi-raft")]

use alopex_chirps::multi_raft::{GroupId, MultiRaftManager, WalRaftStorageFactory};
use alopex_chirps::raft::RaftFramePayload;
use alopex_chirps::{ChirpsRaftTransport, RaftConfig};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_mock::{MockBackend, MockNetwork};
use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
use alopex_chirps_raft_storage::types::LogId;
use alopex_chirps_raft_storage::wal_storage::WalStorageConfig;
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

#[derive(Clone, Default)]
struct EchoStateMachine {
    applied: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl EchoStateMachine {
    async fn values(&self) -> Vec<Vec<u8>> {
        self.applied.lock().await.clone()
    }
}

#[async_trait]
impl StateMachine for EchoStateMachine {
    type Command = Vec<u8>;
    type Response = Vec<u8>;

    async fn apply(
        &mut self,
        _log_id: LogId<u64>,
        command: Self::Command,
    ) -> StateMachineResult<Self::Response> {
        self.applied.lock().await.push(command.clone());
        Ok(command)
    }

    async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn restore(&mut self, _snapshot: Box<dyn AsyncSnapshotData>) -> StateMachineResult<()> {
        Ok(())
    }
}

fn wire_node_id(node_id: u64) -> NodeId {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&node_id.to_be_bytes());
    NodeId::from(bytes)
}

type TestManager = MultiRaftManager<WalRaftStorageFactory<EchoStateMachine>>;

async fn isolated_manager(
    root: &std::path::Path,
    node_id: u64,
    raft_config: RaftConfig,
) -> Arc<TestManager> {
    let factory = Arc::new(WalRaftStorageFactory::new(
        WalStorageConfig {
            wal_dir: root.join("wal"),
            snapshot_dir: root.join("snapshot"),
            ..WalStorageConfig::default()
        },
        node_id,
    ));
    let network = MockNetwork::new();
    let backend = network
        .add_node(wire_node_id(node_id), MockBackend::ephemeral_addr())
        .await;
    let backend: Arc<dyn MessageBackend> = Arc::new(backend);
    let transport = Arc::new(ChirpsRaftTransport::new(backend, GroupId(0), node_id));
    Arc::new(MultiRaftManager::new(transport, factory, raft_config))
}

struct TestCluster {
    group_id: GroupId,
    _roots: Vec<tempfile::TempDir>,
    managers: BTreeMap<u64, Arc<TestManager>>,
    states: BTreeMap<u64, EchoStateMachine>,
    tasks: Vec<JoinHandle<()>>,
}

impl TestCluster {
    async fn new(group_id: GroupId) -> Self {
        let network = MockNetwork::new();
        let mut roots = Vec::new();
        let mut managers = BTreeMap::new();
        let mut states = BTreeMap::new();
        let mut backends = BTreeMap::new();

        for node_id in [1, 2, 3] {
            let root = tempfile::tempdir().unwrap();
            let state = EchoStateMachine::default();
            let backend = network
                .add_node(wire_node_id(node_id), MockBackend::ephemeral_addr())
                .await;
            let backend: Arc<dyn MessageBackend> = Arc::new(backend);
            let transport = Arc::new(ChirpsRaftTransport::new(
                Arc::clone(&backend),
                GroupId(0),
                node_id,
            ));
            let factory = Arc::new(WalRaftStorageFactory::new(
                WalStorageConfig {
                    wal_dir: root.path().join("wal"),
                    snapshot_dir: root.path().join("snapshot"),
                    ..WalStorageConfig::default()
                },
                node_id,
            ));
            let manager = Arc::new(MultiRaftManager::new(
                transport,
                factory,
                RaftConfig {
                    node_id,
                    election_timeout_ms: 120,
                    heartbeat_interval_ms: 40,
                    ..RaftConfig::default()
                },
            ));
            roots.push(root);
            managers.insert(node_id, manager);
            states.insert(node_id, state);
            backends.insert(node_id, backend);
        }

        let mut tasks = Vec::new();
        for node_id in [1, 2, 3] {
            let backend = Arc::clone(&backends[&node_id]);
            let manager = Arc::clone(&managers[&node_id]);
            let mut incoming = backend
                .subscribe()
                .await
                .expect("each backend must have exactly one receive loop");
            tasks.push(tokio::spawn(async move {
                while let Some((source, frame)) = incoming.recv().await {
                    let Ok(Some(response)) = manager
                        .dispatch_frame(source, wire_node_id(node_id), frame)
                        .await
                    else {
                        continue;
                    };
                    let response_frame =
                        ChirpsRaftTransport::encode_group_frame(RaftFramePayload {
                            correlation_id: response.correlation_id,
                            message: response.message,
                        })
                        .expect("manager response must encode");
                    let _ = backend
                        .send(wire_node_id(response.destination), response_frame)
                        .await;
                }
            }));

            let manager = Arc::clone(&managers[&node_id]);
            tasks.push(tokio::spawn(async move {
                loop {
                    let _ = manager.tick_all().await;
                    sleep(Duration::from_millis(10)).await;
                }
            }));
        }

        Self {
            group_id,
            _roots: roots,
            managers,
            states,
            tasks,
        }
    }

    async fn create_replicas(&self) {
        self.managers[&1]
            .create_group(self.group_id, BTreeSet::from([1]), self.states[&1].clone())
            .await
            .unwrap();
        for node_id in [2, 3] {
            self.managers[&node_id]
                .create_group_uninitialized(self.group_id, self.states[&node_id].clone())
                .await
                .unwrap();
        }
        timeout(Duration::from_secs(3), async {
            loop {
                if self.managers[&1]
                    .get_group(self.group_id)
                    .unwrap()
                    .metrics()
                    .current_leader
                    == Some(1)
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("seed must become leader");
    }

    async fn bootstrap_three_voters(&self) {
        self.create_replicas().await;
        let seed = self.managers[&1].get_group(self.group_id).unwrap();
        for node_id in [2, 3] {
            seed.add_learner(
                node_id,
                openraft::BasicNode {
                    addr: format!("node-{node_id}"),
                },
            )
            .await
            .unwrap();
        }

        seed.propose(b"before-promotion".to_vec()).await.unwrap();
        self.wait_for_applied_len(1).await;
        seed.change_membership(BTreeSet::from([1, 2, 3]))
            .await
            .unwrap();
        self.wait_for_voters(BTreeSet::from([1, 2, 3])).await;
    }

    async fn wait_for_applied_len(&self, expected: usize) {
        timeout(Duration::from_secs(3), async {
            loop {
                let mut complete = true;
                for state in self.states.values() {
                    complete &= state.values().await.len() == expected;
                }
                if complete {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all replicas must apply the committed values");
    }

    async fn wait_for_voters(&self, expected: BTreeSet<u64>) {
        timeout(Duration::from_secs(3), async {
            loop {
                if self.managers.values().all(|manager| {
                    voters(&manager.get_group(self.group_id).unwrap().metrics()) == expected
                }) {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all replicas must observe the common voter set");
    }

    async fn shutdown(&self) {
        for manager in self.managers.values() {
            manager.shutdown_all().await.unwrap();
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn voters(
    metrics: &openraft::metrics::RaftMetrics<u64, openraft::BasicNode>,
) -> std::collections::BTreeSet<u64> {
    metrics
        .membership_config
        .membership()
        .get_joint_config()
        .iter()
        .flatten()
        .copied()
        .collect()
}

#[tokio::test]
async fn uninitialized_replica_is_published_without_single_member_initialize() {
    let root = tempfile::tempdir().unwrap();
    let manager = isolated_manager(root.path(), 2, RaftConfig::default()).await;

    manager
        .create_group_uninitialized(GroupId(7), EchoStateMachine::default())
        .await
        .unwrap();

    // Successful completion is the publication boundary: storage must already
    // be committed, while OpenRaft membership must still be empty.
    assert!(root.path().join("wal/groups/0000000000000007").exists());
    let replica = manager
        .get_group(GroupId(7))
        .expect("committed uninitialized replica must be published");
    assert!(voters(&replica.metrics()).is_empty());
    manager.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn replica_publication_failure_is_atomic() {
    let root = tempfile::tempdir().unwrap();
    let manager = isolated_manager(
        root.path(),
        2,
        RaftConfig {
            election_timeout_ms: 0,
            ..RaftConfig::default()
        },
    )
    .await;

    assert!(
        manager
            .create_group_uninitialized(GroupId(8), EchoStateMachine::default())
            .await
            .is_err()
    );
    assert!(manager.get_group(GroupId(8)).is_none());
    assert!(!root.path().join("wal/groups/0000000000000008").exists());
    assert!(
        !root
            .path()
            .join("snapshot/groups/0000000000000008")
            .exists()
    );
}

#[tokio::test]
async fn learners_catch_up_before_common_voter_promotion() {
    let cluster = TestCluster::new(GroupId(9)).await;
    cluster.bootstrap_three_voters().await;
    for state in cluster.states.values() {
        assert_eq!(state.values().await, vec![b"before-promotion".to_vec()]);
    }
    cluster.shutdown().await;
}

#[tokio::test]
async fn membership_failure_keeps_previous_configuration() {
    let cluster = TestCluster::new(GroupId(10)).await;
    cluster.create_replicas().await;
    let group = cluster.managers[&1].get_group(GroupId(10)).unwrap();

    assert!(
        group
            .change_membership(BTreeSet::from([1, 2]))
            .await
            .is_err(),
        "an unpublished, non-learner replica must not be promoted"
    );
    assert_eq!(voters(&group.metrics()), BTreeSet::from([1]));
    cluster.shutdown().await;
}

#[tokio::test]
async fn three_voters_elect_one_leader_and_commit_consistently() {
    let cluster = TestCluster::new(GroupId(11)).await;
    cluster.bootstrap_three_voters().await;
    let seed = cluster.managers[&1].get_group(GroupId(11)).unwrap();
    assert_eq!(
        seed.propose(b"three-voter".to_vec()).await.unwrap(),
        b"three-voter"
    );
    cluster.wait_for_applied_len(2).await;

    let observations = cluster
        .managers
        .values()
        .map(|manager| manager.get_group(GroupId(11)).unwrap().metrics())
        .collect::<Vec<_>>();
    assert_eq!(
        observations
            .iter()
            .filter_map(|metrics| metrics.current_leader)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        1
    );
    assert!(
        observations
            .iter()
            .all(|metrics| voters(metrics) == BTreeSet::from([1, 2, 3]))
    );
    assert!(
        observations
            .iter()
            .all(|metrics| metrics.last_applied == observations[0].last_applied)
    );
    let expected = vec![b"before-promotion".to_vec(), b"three-voter".to_vec()];
    for state in cluster.states.values() {
        assert_eq!(state.values().await, expected);
    }
    cluster.shutdown().await;
}
