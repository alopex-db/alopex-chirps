#![cfg(feature = "multi-raft")]

use alopex_chirps::multi_raft::{GroupId, MultiRaftManager, WalRaftStorageFactory};
use alopex_chirps::raft::RaftFramePayload;
use alopex_chirps::{ChirpsRaftTransport, RaftConfig, RaftMessage};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_mock::{MockBackend, MockNetwork};
use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
use alopex_chirps_raft_storage::types::{LogId, Vote, VoteResponse};
use alopex_chirps_raft_storage::wal_storage::WalStorageConfig;
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
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
    receiver_paused: BTreeMap<u64, Arc<AtomicBool>>,
    held_correlation: BTreeMap<u64, Arc<AtomicU64>>,
    held_source: BTreeMap<u64, Arc<AtomicU64>>,
    hold_next_proposal_response: Arc<AtomicBool>,
    proposal_correlation: Arc<AtomicU64>,
    tasks: Vec<JoinHandle<()>>,
}

impl TestCluster {
    async fn new(group_id: GroupId) -> Self {
        let network = MockNetwork::new();
        let mut roots = Vec::new();
        let mut managers = BTreeMap::new();
        let mut states = BTreeMap::new();
        let mut backends = BTreeMap::new();
        let mut receiver_paused = BTreeMap::new();
        let mut held_correlation = BTreeMap::new();
        let mut held_source = BTreeMap::new();
        let hold_next_proposal_response = Arc::new(AtomicBool::new(false));
        let proposal_correlation = Arc::new(AtomicU64::new(0));

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
            receiver_paused.insert(node_id, Arc::new(AtomicBool::new(false)));
            held_correlation.insert(node_id, Arc::new(AtomicU64::new(0)));
            held_source.insert(node_id, Arc::new(AtomicU64::new(0)));
        }

        let mut tasks = Vec::new();
        for node_id in [1, 2, 3] {
            let backend = Arc::clone(&backends[&node_id]);
            let manager = Arc::clone(&managers[&node_id]);
            let paused = Arc::clone(&receiver_paused[&node_id]);
            let correlation = Arc::clone(&held_correlation[&node_id]);
            let held_source = Arc::clone(&held_source[&node_id]);
            let hold_proposal = Arc::clone(&hold_next_proposal_response);
            let proposal_correlation = Arc::clone(&proposal_correlation);
            let mut incoming = backend
                .subscribe()
                .await
                .expect("each backend must have exactly one receive loop");
            tasks.push(tokio::spawn(async move {
                while let Some((source, frame)) = incoming.recv().await {
                    let payload = ChirpsRaftTransport::decode_frame(frame.clone())
                        .expect("test receive loop must receive a Raft frame");
                    if hold_proposal.load(Ordering::Acquire)
                        && matches!(
                            &payload.message,
                            RaftMessage::AppendEntries { request, .. } if !request.entries.is_empty()
                        )
                    {
                        let _ = proposal_correlation.compare_exchange(
                            0,
                            payload.correlation_id,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        );
                    }
                    let is_response = matches!(
                        &payload.message,
                            RaftMessage::AppendEntriesResponse { .. }
                                | RaftMessage::VoteResponse { .. }
                                | RaftMessage::InstallSnapshotResponse { .. }
                    );
                    let hold_selected_proposal = node_id == 1
                        && is_response
                        && hold_proposal.load(Ordering::Acquire)
                        && proposal_correlation.load(Ordering::Acquire) == payload.correlation_id;
                    if paused.load(Ordering::Acquire) || hold_selected_proposal {
                        paused.store(true, Ordering::Release);
                        correlation.store(payload.correlation_id, Ordering::Release);
                        held_source.store(
                            u64::from_be_bytes(
                                source.as_bytes()[8..]
                                    .try_into()
                                    .expect("canonical test node id"),
                            ),
                            Ordering::Release,
                        );
                        while paused.load(Ordering::Acquire) {
                            sleep(Duration::from_millis(5)).await;
                        }
                    }
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
            receiver_paused,
            held_correlation,
            held_source,
            hold_next_proposal_response,
            proposal_correlation,
            tasks,
        }
    }

    async fn create_replicas(&self) {
        self.create_seed().await;
        for node_id in [2, 3] {
            self.publish_replica(node_id).await;
        }
    }

    async fn create_seed(&self) {
        self.managers[&1]
            .create_group(self.group_id, BTreeSet::from([1]), self.states[&1].clone())
            .await
            .unwrap();
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

    async fn publish_replica(&self, node_id: u64) {
        self.managers[&node_id]
            .create_group_uninitialized(self.group_id, self.states[&node_id].clone())
            .await
            .unwrap();
    }

    async fn bootstrap_three_voters(&self) {
        self.create_seed().await;
        let seed = self.managers[&1].get_group(self.group_id).unwrap();
        self.publish_replica(2).await;
        seed.add_learner(
            2,
            openraft::BasicNode {
                addr: "node-2".to_owned(),
            },
        )
        .await
        .unwrap();
        seed.change_membership(BTreeSet::from([1, 2]))
            .await
            .unwrap();
        self.wait_for_voters_on(&[1, 2], BTreeSet::from([1, 2]))
            .await;

        self.publish_replica(3).await;
        seed.add_learner(
            3,
            openraft::BasicNode {
                addr: "node-3".to_owned(),
            },
        )
        .await
        .unwrap();
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
        self.wait_for_voters_on(&[1, 2, 3], expected).await;
    }

    async fn wait_for_voters_on(&self, node_ids: &[u64], expected: BTreeSet<u64>) {
        timeout(Duration::from_secs(3), async {
            loop {
                if node_ids.iter().all(|node_id| {
                    voters(
                        &self.managers[node_id]
                            .get_group(self.group_id)
                            .unwrap()
                            .metrics(),
                    ) == expected
                }) {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all replicas must observe the common voter set");
    }

    async fn wait_until_response_is_held(&self, node_id: u64) -> (u64, u64) {
        timeout(Duration::from_secs(3), async {
            loop {
                let correlation = self.held_correlation[&node_id].load(Ordering::Acquire);
                if correlation != 0 {
                    let source = self.held_source[&node_id].load(Ordering::Acquire);
                    break (source, correlation);
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the paused receiver must observe an RPC response")
    }

    fn resume_receiver(&self, node_id: u64) {
        self.receiver_paused[&node_id].store(false, Ordering::Release);
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
async fn sequential_groups_all_finish_common_voter_promotion() {
    let cluster = TestCluster::new(GroupId(100)).await;

    for raw_group_id in 100..110 {
        let group_id = GroupId(raw_group_id);
        cluster.managers[&1]
            .create_group(group_id, BTreeSet::from([1]), EchoStateMachine::default())
            .await
            .unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                if cluster.managers[&1]
                    .get_group(group_id)
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
        .expect("each seed must become leader");

        let seed = cluster.managers[&1].get_group(group_id).unwrap();
        for node_id in [2, 3] {
            cluster.managers[&node_id]
                .create_group_uninitialized(group_id, EchoStateMachine::default())
                .await
                .unwrap();
            seed.add_learner(
                node_id,
                openraft::BasicNode {
                    addr: format!("node-{node_id}"),
                },
            )
            .await
            .unwrap();
        }
        timeout(
            Duration::from_secs(3),
            seed.change_membership(BTreeSet::from([1, 2, 3])),
        )
        .await
        .expect("membership promotion must not stall")
        .unwrap();
    }

    for raw_group_id in 100..110 {
        let group_id = GroupId(raw_group_id);
        for node_id in [1, 2, 3] {
            assert_eq!(
                voters(
                    &cluster.managers[&node_id]
                        .get_group(group_id)
                        .unwrap()
                        .metrics()
                ),
                BTreeSet::from([1, 2, 3]),
                "group {raw_group_id} must converge on node {node_id}"
            );
        }
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
async fn remove_drains_pending_proposal_and_only_valid_response_completes_it() {
    let cluster = TestCluster::new(GroupId(12)).await;
    cluster.bootstrap_three_voters().await;
    cluster
        .hold_next_proposal_response
        .store(true, Ordering::Release);
    cluster.proposal_correlation.store(0, Ordering::Release);

    let group = cluster.managers[&1].get_group(GroupId(12)).unwrap();
    let proposal = {
        let group = Arc::clone(&group);
        tokio::spawn(async move { group.propose(b"drain-race".to_vec()).await })
    };
    let (source, correlation) = cluster.wait_until_response_is_held(1).await;

    let remove = {
        let manager = Arc::clone(&cluster.managers[&1]);
        tokio::spawn(async move { manager.remove_group(GroupId(12)).await })
    };
    timeout(Duration::from_secs(1), async {
        while group.is_accepting() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("remove must close admission before waiting for the RPC");

    let response_frame = |correlation_id| {
        ChirpsRaftTransport::encode_group_frame(RaftFramePayload {
            correlation_id,
            message: RaftMessage::VoteResponse {
                group_id: GroupId(12),
                response: VoteResponse {
                    vote: Vote::new(1, 2),
                    vote_granted: true,
                    last_log_id: None,
                },
            },
        })
        .unwrap()
    };
    assert!(
        cluster.managers[&1]
            .dispatch_frame(
                wire_node_id(source + 10),
                wire_node_id(1),
                response_frame(correlation),
            )
            .await
            .is_err(),
        "the correct correlation from the wrong source must be rejected"
    );
    assert!(
        cluster.managers[&1]
            .dispatch_frame(
                wire_node_id(source),
                wire_node_id(1),
                response_frame(correlation + 10_000),
            )
            .await
            .is_err(),
        "an unknown correlation from the correct source must be rejected"
    );
    assert!(!proposal.is_finished());
    assert!(!remove.is_finished());

    cluster.resume_receiver(1);
    cluster
        .hold_next_proposal_response
        .store(false, Ordering::Release);
    assert_eq!(
        timeout(Duration::from_secs(3), proposal)
            .await
            .expect("the valid response must complete the pending RPC")
            .unwrap()
            .unwrap(),
        b"drain-race".to_vec()
    );
    assert!(
        timeout(Duration::from_secs(3), remove)
            .await
            .expect("remove must finish after the pending RPC drains")
            .unwrap()
            .unwrap()
    );
    assert!(cluster.managers[&1].get_group(GroupId(12)).is_none());
    assert!(
        !cluster.managers[&1]
            .remove_group(GroupId(12))
            .await
            .unwrap()
    );
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
    let leader = observations[0]
        .current_leader
        .expect("seed replica must report a leader");
    assert_eq!(
        observations
            .iter()
            .map(|metrics| metrics.current_leader)
            .collect::<Vec<_>>(),
        vec![Some(leader); 3]
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

#[tokio::test]
async fn concurrent_single_group_proposals_converge_on_all_voters() {
    let cluster = TestCluster::new(GroupId(13)).await;
    cluster.bootstrap_three_voters().await;
    let seed = cluster.managers[&1].get_group(GroupId(13)).unwrap();

    let proposals = (0..32)
        .map(|index| {
            let seed = Arc::clone(&seed);
            tokio::spawn(async move {
                let command = format!("concurrent-{index}").into_bytes();
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    match seed.propose(command.clone()).await {
                        Ok(response) => break Ok(response),
                        Err(_error) if Instant::now() < deadline => {
                            sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => break Err(error),
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for proposal in proposals {
        proposal.await.unwrap().unwrap();
    }

    cluster.wait_for_applied_len(33).await;
    let observations = cluster
        .managers
        .values()
        .map(|manager| manager.get_group(GroupId(13)).unwrap().metrics())
        .collect::<Vec<_>>();
    let expected_last_applied = observations[0].last_applied;
    assert!(observations.iter().all(|metrics| {
        metrics.current_leader.is_some()
            && voters(metrics) == BTreeSet::from([1, 2, 3])
            && metrics.last_applied == expected_last_applied
    }));
    let expected_len = cluster.states[&1].values().await.len();
    assert_eq!(expected_len, 33);
    for state in cluster.states.values() {
        assert_eq!(state.values().await.len(), expected_len);
    }
    cluster.shutdown().await;
}

#[tokio::test]
async fn three_voters_survive_baseline_level_single_group_concurrency() {
    let cluster = TestCluster::new(GroupId(14)).await;
    cluster.bootstrap_three_voters().await;
    let seed = cluster.managers[&1].get_group(GroupId(14)).unwrap();

    let proposals = (0..300)
        .map(|index| {
            let seed = Arc::clone(&seed);
            tokio::spawn(async move {
                let command = format!("baseline-{index}").into_bytes();
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    match seed.propose(command.clone()).await {
                        Ok(_) => break Ok(()),
                        Err(_error) if Instant::now() < deadline => {
                            sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => break Err(error),
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for proposal in proposals {
        proposal.await.unwrap().unwrap();
    }
    cluster.wait_for_applied_len(301).await;
    let observations = cluster
        .managers
        .values()
        .map(|manager| manager.get_group(GroupId(14)).unwrap().metrics())
        .collect::<Vec<_>>();
    assert!(observations.iter().all(|metrics| {
        metrics.current_leader.is_some()
            && voters(metrics) == BTreeSet::from([1, 2, 3])
            && metrics.last_applied == observations[0].last_applied
    }));
    cluster.shutdown().await;
}

#[tokio::test]
async fn concurrent_multi_group_proposals_do_not_starve_each_other() {
    let cluster = TestCluster::new(GroupId(20)).await;
    let mut groups = Vec::new();
    for raw_group_id in 20..23 {
        let group_id = GroupId(raw_group_id);
        cluster.managers[&1]
            .create_group(group_id, BTreeSet::from([1]), EchoStateMachine::default())
            .await
            .unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                if cluster.managers[&1]
                    .get_group(group_id)
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
        .expect("each concurrent seed must become leader");
        for node_id in [2, 3] {
            cluster.managers[&node_id]
                .create_group_uninitialized(group_id, EchoStateMachine::default())
                .await
                .unwrap();
            cluster.managers[&1]
                .get_group(group_id)
                .unwrap()
                .add_learner(
                    node_id,
                    openraft::BasicNode {
                        addr: format!("node-{node_id}"),
                    },
                )
                .await
                .unwrap();
        }
        cluster.managers[&1]
            .get_group(group_id)
            .unwrap()
            .change_membership(BTreeSet::from([1, 2, 3]))
            .await
            .unwrap();
        groups.push([
            cluster.managers[&1].get_group(group_id).unwrap(),
            cluster.managers[&2].get_group(group_id).unwrap(),
            cluster.managers[&3].get_group(group_id).unwrap(),
        ]);
    }

    let proposals = groups
        .into_iter()
        .enumerate()
        .flat_map(|(group_index, replicas)| {
            (0..3).map(move |proposal_index| {
                let replicas = replicas.clone();
                tokio::spawn(async move {
                    let command =
                        format!("multi-group-{group_index}-{proposal_index}").into_bytes();
                    let deadline = Instant::now() + Duration::from_secs(3);
                    loop {
                        let mut last_error = None;
                        for target in &replicas {
                            match target.propose(command.clone()).await {
                                Ok(_) => return Ok(()),
                                Err(error) => last_error = Some(error),
                            }
                        }
                        if Instant::now() < deadline {
                            sleep(Duration::from_millis(10)).await;
                        } else {
                            break Err(last_error.expect("at least one replica must be present"));
                        }
                    }
                })
            })
        })
        .collect::<Vec<_>>();
    for proposal in proposals {
        proposal
            .await
            .expect("multi-group proposal task must join")
            .expect("multi-group proposal must eventually commit");
    }
    cluster.shutdown().await;
}
