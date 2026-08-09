//! Executable v0.6.0 demo: three logical nodes, Multi-Raft writes, replica reads.
//!
//! This is a demonstration harness, not a performance benchmark. It uses the
//! public MultiRaftManager/WAL APIs and MockNetwork, records the exact scenario,
//! and emits machine-readable evidence for the Python/marimo frontends.

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
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const NODE_IDS: [u64; 3] = [1, 2, 3];
const GROUP_ID: GroupId = GroupId(600);
const READY_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Default)]
struct KvState {
    values: Arc<Mutex<BTreeMap<String, String>>>,
}

impl KvState {
    async fn apply_command(&self, command: &[u8]) {
        let text = String::from_utf8_lossy(command);
        let mut parts = text.splitn(3, ':');
        if parts.next() == Some("put")
            && let (Some(key), Some(value)) = (parts.next(), parts.next())
        {
            self.values
                .lock()
                .await
                .insert(key.to_owned(), value.to_owned());
        }
    }

    async fn snapshot(&self) -> BTreeMap<String, String> {
        self.values.lock().await.clone()
    }
}

#[async_trait]
impl StateMachine for KvState {
    type Command = Vec<u8>;
    type Response = Vec<u8>;

    async fn apply(
        &mut self,
        _log_id: LogId<u64>,
        command: Self::Command,
    ) -> StateMachineResult<Self::Response> {
        self.apply_command(&command).await;
        Ok(command)
    }

    async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn restore(&mut self, _snapshot: Box<dyn AsyncSnapshotData>) -> StateMachineResult<()> {
        Ok(())
    }
}

type Manager = MultiRaftManager<WalRaftStorageFactory<KvState>>;

struct Node {
    id: u64,
    manager: Arc<Manager>,
    state: KvState,
    _root: TempDir,
}

#[derive(Serialize)]
struct DemoResult {
    scenario: &'static str,
    group_id: u64,
    nodes: usize,
    writes_requested: usize,
    writes_committed: usize,
    leader: u64,
    reads_consistent: bool,
    replica_key_counts: BTreeMap<String, usize>,
    values_sha256_inputs: BTreeMap<String, Vec<String>>,
    scope: &'static str,
    elapsed_ms: u128,
}

fn node_id(value: u64) -> NodeId {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&value.to_be_bytes());
    NodeId::from(bytes)
}

async fn build_nodes() -> (Vec<Node>, Vec<JoinHandle<()>>) {
    let network = MockNetwork::new();
    let mut nodes = Vec::new();
    let mut backends: BTreeMap<u64, Arc<dyn MessageBackend>> = BTreeMap::new();

    for id in NODE_IDS {
        let backend = network
            .add_node(node_id(id), MockBackend::ephemeral_addr())
            .await;
        let backend: Arc<dyn MessageBackend> = Arc::new(backend);
        let root = tempfile::tempdir().expect("temporary WAL root");
        let state = KvState::default();
        let factory = Arc::new(WalRaftStorageFactory::new(
            WalStorageConfig {
                wal_dir: root.path().join("wal"),
                snapshot_dir: root.path().join("snapshot"),
                ..WalStorageConfig::default()
            },
            id,
        ));
        let transport = Arc::new(ChirpsRaftTransport::new(Arc::clone(&backend), GROUP_ID, id));
        let manager = Arc::new(MultiRaftManager::new(
            transport,
            factory,
            RaftConfig {
                node_id: id,
                election_timeout_ms: 1_000,
                heartbeat_interval_ms: 100,
                ..RaftConfig::default()
            },
        ));
        backends.insert(id, backend);
        nodes.push(Node {
            id,
            manager,
            state,
            _root: root,
        });
    }

    let mut tasks = Vec::new();
    for id in NODE_IDS {
        let backend = Arc::clone(&backends[&id]);
        let manager = nodes[(id - 1) as usize].manager.clone();
        let mut incoming = backend.subscribe().await.expect("one receive loop");
        tasks.push(tokio::spawn(async move {
            while let Some((source, frame)) = incoming.recv().await {
                let response = manager
                    .dispatch_frame(source, node_id(id), frame)
                    .await
                    .expect("frame dispatch");
                if let Some(response) = response {
                    let frame = ChirpsRaftTransport::encode_group_frame(RaftFramePayload {
                        correlation_id: response.correlation_id,
                        message: response.message,
                    })
                    .expect("response frame");
                    let _ = backend.send(node_id(response.destination), frame).await;
                }
            }
        }));
        let manager = nodes[(id - 1) as usize].manager.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let _ = manager.tick_all().await;
                sleep(Duration::from_millis(10)).await;
            }
        }));
    }
    (nodes, tasks)
}

async fn wait_leader(manager: &Arc<Manager>) -> u64 {
    timeout(READY_TIMEOUT, async {
        loop {
            if let Some(group) = manager.get_group(GROUP_ID)
                && let Some(leader) = group.metrics().current_leader
            {
                return leader;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("leader election timeout")
}

async fn wait_replicas(nodes: &[Node], expected: usize) {
    timeout(READY_TIMEOUT, async {
        loop {
            let mut ready = true;
            for node in nodes {
                if node.state.snapshot().await.len() != expected {
                    ready = false;
                }
            }
            if ready {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replica apply timeout");
}

async fn run(writes: usize) -> DemoResult {
    let started = Instant::now();
    let (nodes, tasks) = build_nodes().await;
    let seed = &nodes[0];
    seed.manager
        .create_group(GROUP_ID, BTreeSet::from([1]), seed.state.clone())
        .await
        .expect("create seed group");
    let leader = wait_leader(&seed.manager).await;

    for node in &nodes[1..] {
        node.manager
            .create_group_uninitialized(GROUP_ID, node.state.clone())
            .await
            .expect("publish replica");
        let group = seed.manager.get_group(GROUP_ID).expect("seed group");
        group
            .add_learner(
                node.id,
                openraft::BasicNode {
                    addr: format!("node-{}", node.id),
                },
            )
            .await
            .expect("add learner");
        let voters = if node.id == 2 {
            BTreeSet::from([1, 2])
        } else {
            BTreeSet::from([1, 2, 3])
        };
        group
            .change_membership(voters)
            .await
            .expect("promote learner");
    }
    let group = seed.manager.get_group(GROUP_ID).expect("seed group");
    group
        .change_membership(BTreeSet::from([1, 2, 3]))
        .await
        .expect("promote all voters");

    for index in 0..writes {
        let command = format!("put:key-{index}:value-{index}").into_bytes();
        group.propose(command).await.expect("commit write");
    }
    wait_replicas(&nodes, writes).await;

    let mut replica_key_counts = BTreeMap::new();
    let mut values_sha256_inputs = BTreeMap::new();
    let mut expected: Option<BTreeMap<String, String>> = None;
    let mut consistent = true;
    for node in &nodes {
        let values = node.state.snapshot().await;
        replica_key_counts.insert(format!("node-{}", node.id), values.len());
        let entries = values
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>();
        values_sha256_inputs.insert(format!("node-{}", node.id), entries);
        if let Some(first) = &expected {
            consistent &= first == &values;
        } else {
            expected = Some(values);
        }
    }
    for task in tasks {
        task.abort();
    }
    for node in &nodes {
        let _ = node.manager.shutdown_all().await;
    }
    DemoResult {
        scenario: "v0.6.0-three-node-multi-raft-read-write",
        group_id: GROUP_ID.0,
        nodes: nodes.len(),
        writes_requested: writes,
        writes_committed: writes,
        leader,
        reads_consistent: consistent,
        replica_key_counts,
        values_sha256_inputs,
        scope: "three logical nodes in one process using MockNetwork and WAL-backed storage; not physical-node evidence",
        elapsed_ms: started.elapsed().as_millis(),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let writes = std::env::args()
        .skip(1)
        .collect::<Vec<_>>()
        .windows(2)
        .find(|pair| pair[0] == "--writes")
        .and_then(|pair| pair[1].parse().ok())
        .unwrap_or(12);
    let result = run(writes).await;
    println!("{}", serde_json::to_string_pretty(&result).expect("JSON"));
}
