use alopex_chirps::{ChirpsRaftTransport, RaftConfig, RaftError, RaftNode};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_mock::{MockBackend, MockNetwork};
use alopex_chirps_raft_storage::types::{
    BasicNode, ChirpsNodeId, ChirpsTypeConfig, Entry, EntryPayload, GroupId, LogId, LogState,
    Snapshot, SnapshotMeta, StoredMembership, Vote,
};
use alopex_chirps_wire::node_id::NodeId;
use anyhow::{Result, bail};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    CommittedLeaderId, ErrorSubject, ErrorVerb, OptionalSend, RaftLogReader, StorageError,
    StorageIOError,
};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;

#[derive(Clone)]
struct TestStateHandle {
    data: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl TestStateHandle {
    async fn values(&self) -> Vec<Vec<u8>> {
        self.data.lock().await.clone()
    }
}

#[derive(Clone)]
struct MemorySnapshotBuilder {
    meta: SnapshotMeta<ChirpsNodeId, BasicNode>,
    data: Vec<u8>,
}

impl RaftSnapshotBuilder<ChirpsTypeConfig> for MemorySnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ChirpsTypeConfig>, StorageError<ChirpsNodeId>> {
        Ok(Snapshot {
            meta: self.meta.clone(),
            snapshot: Box::new(Cursor::new(self.data.clone())),
        })
    }
}

struct MemoryStoreState {
    logs: Vec<Entry<ChirpsTypeConfig>>,
    last_purged: Option<LogId<ChirpsNodeId>>,
    vote: Option<Vote<ChirpsNodeId>>,
    state: TestStateHandle,
    last_applied: Option<LogId<ChirpsNodeId>>,
    last_membership: StoredMembership<ChirpsNodeId, BasicNode>,
    snapshot: Option<Snapshot<ChirpsTypeConfig>>,
    committed: Option<LogId<ChirpsNodeId>>,
    snapshot_counter: u64,
}

impl MemoryStoreState {
    fn new(state: TestStateHandle) -> Self {
        Self {
            logs: Vec::new(),
            last_purged: None,
            vote: None,
            state,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot: None,
            committed: None,
            snapshot_counter: 0,
        }
    }
}

#[derive(Clone)]
struct MemoryStore {
    inner: Arc<Mutex<MemoryStoreState>>,
}

impl MemoryStore {
    fn new(handle: TestStateHandle) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryStoreState::new(handle))),
        }
    }
}

impl RaftLogReader<ChirpsTypeConfig> for MemoryStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ChirpsTypeConfig>>, StorageError<ChirpsNodeId>>
    where
        RB: std::ops::RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let guard = self.inner.lock().await;
        let entries = guard
            .logs
            .iter()
            .filter(|e| range_contains(&range, e.log_id.index))
            .cloned()
            .collect();
        Ok(entries)
    }
}

impl RaftLogStorage<ChirpsTypeConfig> for MemoryStore {
    type LogReader = MemoryStore;

    async fn save_vote(
        &mut self,
        vote: &Vote<ChirpsNodeId>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        let mut guard = self.inner.lock().await;
        guard.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<ChirpsNodeId>>, StorageError<ChirpsNodeId>> {
        let guard = self.inner.lock().await;
        Ok(guard.vote)
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<ChirpsTypeConfig>, StorageError<ChirpsNodeId>> {
        let guard = self.inner.lock().await;
        Ok(LogState {
            last_purged_log_id: guard.last_purged,
            last_log_id: guard.logs.last().map(|e| e.log_id),
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<ChirpsNodeId>>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        let mut guard = self.inner.lock().await;
        guard.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<ChirpsNodeId>>, StorageError<ChirpsNodeId>> {
        let guard = self.inner.lock().await;
        Ok(guard.committed)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<ChirpsTypeConfig>,
    ) -> Result<(), StorageError<ChirpsNodeId>>
    where
        I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut guard = self.inner.lock().await;
        let new_entries: Vec<_> = entries.into_iter().collect();
        if let Some(first) = new_entries.first() {
            guard.logs.retain(|e| e.log_id.index < first.log_id.index);
            guard.logs.extend(new_entries);
        }
        drop(guard);
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<ChirpsNodeId>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        let mut guard = self.inner.lock().await;
        guard.logs.retain(|e| e.log_id.index < log_id.index);
        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<ChirpsNodeId>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        let mut guard = self.inner.lock().await;
        guard.logs.retain(|e| e.log_id.index > log_id.index);
        guard.last_purged = Some(log_id);
        Ok(())
    }
}

impl RaftStateMachine<ChirpsTypeConfig> for MemoryStore {
    type SnapshotBuilder = MemorySnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<ChirpsNodeId>>,
            StoredMembership<ChirpsNodeId, BasicNode>,
        ),
        StorageError<ChirpsNodeId>,
    > {
        let guard = self.inner.lock().await;
        Ok((guard.last_applied, guard.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Vec<u8>>, StorageError<ChirpsNodeId>>
    where
        I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut guard = self.inner.lock().await;
        let mut responses = Vec::new();
        for entry in entries {
            guard.last_applied = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Normal(data) => {
                    guard.state.data.lock().await.push(data.clone());
                    responses.push(data.clone());
                }
                EntryPayload::Membership(m) => {
                    guard.last_membership = StoredMembership::new(Some(entry.log_id), m.clone());
                    responses.push(Vec::new());
                }
                EntryPayload::Blank => {
                    responses.push(Vec::new());
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let mut guard = self.inner.lock().await;
        let mut index = guard.last_applied.as_ref().map(|l| l.index).unwrap_or(0);
        index = index.max(guard.snapshot_counter + 1);
        guard.snapshot_counter = index;
        let leader = guard
            .last_applied
            .as_ref()
            .map(|l| l.leader_id)
            .unwrap_or_else(|| CommittedLeaderId::new(0, 0));
        let last_log_id = Some(LogId::new(leader, index));
        if guard
            .committed
            .as_ref()
            .map(|c| c.index < index)
            .unwrap_or(true)
        {
            guard.committed = last_log_id;
        }
        let bytes = bincode::serialize(&*guard.state.data.lock().await).unwrap_or_default();
        let meta = SnapshotMeta {
            last_log_id,
            last_membership: guard.last_membership.clone(),
            snapshot_id: format!("mem-{}", now_micros()),
        };
        MemorySnapshotBuilder { meta, data: bytes }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<ChirpsNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<ChirpsNodeId, BasicNode>,
        mut snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        let mut buf = Vec::new();
        snapshot
            .read_to_end(&mut buf)
            .await
            .map_err(|e| to_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        let restored: Vec<Vec<u8>> = if buf.is_empty() {
            Vec::new()
        } else {
            bincode::deserialize(&buf)
                .map_err(|e| to_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?
        };

        let mut guard = self.inner.lock().await;
        *guard.state.data.lock().await = restored;
        guard.last_applied = meta.last_log_id;
        guard.last_membership = meta.last_membership.clone();
        guard.committed = meta.last_log_id;
        guard.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(buf)),
        });
        if let Some(id) = &meta.last_log_id {
            guard.snapshot_counter = id.index;
        }
        guard.logs.clear();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ChirpsTypeConfig>>, StorageError<ChirpsNodeId>> {
        let guard = self.inner.lock().await;
        Ok(guard.snapshot.clone())
    }
}

fn to_io_error(
    subject: ErrorSubject<ChirpsNodeId>,
    verb: ErrorVerb,
    err: impl std::error::Error + Send + Sync + 'static,
) -> StorageError<ChirpsNodeId> {
    StorageIOError::new(subject, verb, &err).into()
}

fn range_contains<RB>(range: &RB, value: u64) -> bool
where
    RB: std::ops::RangeBounds<u64>,
{
    use std::ops::Bound;
    let start_ok = match range.start_bound() {
        Bound::Included(v) => value >= *v,
        Bound::Excluded(v) => value > *v,
        Bound::Unbounded => true,
    };
    let end_ok = match range.end_bound() {
        Bound::Included(v) => value <= *v,
        Bound::Excluded(v) => value < *v,
        Bound::Unbounded => true,
    };
    start_ok && end_ok
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[allow(dead_code)]
struct TestNode {
    id: ChirpsNodeId,
    node: Arc<RaftNode>,
    transport: Arc<ChirpsRaftTransport>,
    backend: Arc<dyn MessageBackend>,
    state: TestStateHandle,
    running: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    pump: JoinHandle<()>,
    ticker: JoinHandle<()>,
}

impl TestNode {
    async fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    async fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }
}

struct TestCluster {
    group_id: GroupId,
    network: MockNetwork,
    nodes: HashMap<ChirpsNodeId, TestNode>,
    partitioned: Arc<Mutex<HashSet<ChirpsNodeId>>>,
    snapshot_threshold: u64,
}

impl TestCluster {
    async fn new(node_ids: &[ChirpsNodeId], snapshot_threshold: u64) -> Result<Self> {
        let mut cluster = Self {
            group_id: GroupId(7),
            network: MockNetwork::new(),
            nodes: HashMap::new(),
            partitioned: Arc::new(Mutex::new(HashSet::new())),
            snapshot_threshold,
        };

        for id in node_ids {
            cluster.add_node(*id).await?;
        }
        cluster.initialize().await?;
        Ok(cluster)
    }

    async fn add_node(&mut self, id: ChirpsNodeId) -> Result<()> {
        let backend = self
            .network
            .add_node(to_wire_node(id), MockBackend::ephemeral_addr())
            .await;
        let backend: Arc<dyn MessageBackend> = Arc::new(backend);
        let transport = Arc::new(ChirpsRaftTransport::new(backend.clone(), self.group_id, id));
        let state_handle = TestStateHandle {
            data: Arc::new(Mutex::new(Vec::new())),
        };
        let store = MemoryStore::new(state_handle.clone());

        let cfg = RaftConfig {
            group_id: self.group_id,
            node_id: id,
            election_timeout_ms: 120,
            heartbeat_interval_ms: 40,
            snapshot_threshold: self.snapshot_threshold,
            max_in_snapshot_log_to_keep: 2 * self.snapshot_threshold,
            ..Default::default()
        };

        let mut node = RaftNode::new(
            cfg,
            ChirpsRaftTransport::factory(transport.clone()),
            store.clone(),
            store,
            transport.clone(),
        )
        .await?;
        node.start().await?;
        let node = Arc::new(node);

        let running = Arc::new(AtomicBool::new(true));
        let paused = Arc::new(AtomicBool::new(false));
        let pump = spawn_pump(
            id,
            transport.clone(),
            node.clone(),
            backend.clone(),
            running.clone(),
            paused.clone(),
            self.partitioned.clone(),
        );
        let ticker = spawn_ticker(node.clone(), running.clone(), paused.clone());

        self.nodes.insert(
            id,
            TestNode {
                id,
                node,
                transport,
                backend,
                state: state_handle,
                running,
                paused,
                pump,
                ticker,
            },
        );
        Ok(())
    }

    async fn initialize(&self) -> Result<()> {
        let members: BTreeSet<_> = self.nodes.keys().copied().collect();
        if let Some(first) = members.iter().next() {
            let leader = self.nodes.get(first).unwrap();
            leader.node.initialize(members).await?;
        }
        Ok(())
    }

    async fn wait_for_leader(&self, timeout: Duration) -> Result<ChirpsNodeId> {
        let start = Instant::now();
        loop {
            for node in self.nodes.values() {
                if let Some(current) = node.node.leader_id() {
                    return Ok(current);
                }
            }
            if start.elapsed() > timeout {
                bail!("leader not elected within {:?}", timeout);
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_state_len(
        &self,
        expected: usize,
        timeout: Duration,
        require_all: bool,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            let mut all_match = true;
            for node in self.nodes.values() {
                let is_partitioned = {
                    let guard = self.partitioned.lock().await;
                    guard.contains(&node.id)
                };
                if !require_all && (is_partitioned || node.paused.load(Ordering::SeqCst)) {
                    continue;
                }
                if node.state.values().await.len() != expected {
                    all_match = false;
                    break;
                }
            }
            if all_match {
                return Ok(());
            }
            if start.elapsed() > timeout {
                bail!(
                    "state length did not reach {} within {:?}",
                    expected,
                    timeout
                );
            }
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn propose(&self, leader: ChirpsNodeId, payload: &[u8]) -> Result<Vec<u8>> {
        let node = self.nodes.get(&leader).unwrap();
        Ok(node.node.propose(payload.to_vec()).await?)
    }

    async fn wait_for_membership(
        &self,
        expected: &BTreeSet<ChirpsNodeId>,
        timeout: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            for node in self.nodes.values() {
                let metrics = node.node.metrics();
                let voters = membership_voters(&metrics);
                if &voters == expected {
                    return Ok(());
                }
            }
            if start.elapsed() > timeout {
                bail!(
                    "membership {:?} not observed within {:?}",
                    expected,
                    timeout
                );
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn change_membership_with_retry(
        &self,
        leader: ChirpsNodeId,
        members: BTreeSet<ChirpsNodeId>,
        timeout: Duration,
    ) -> Result<()> {
        let leader_node = self.nodes.get(&leader).unwrap();
        let mut last_err = None;
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match leader_node.node.change_membership(members.clone()).await {
                Ok(()) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    self.wait_for_membership(&members, remaining).await?;
                    return Ok(());
                }
                Err(RaftError::MembershipChangeInProgress) => {
                    last_err = Some(RaftError::MembershipChangeInProgress);
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        if let Some(e) = last_err {
            return Err(e.into());
        }
        bail!("membership change did not complete after retries");
    }

    async fn add_learner_with_retry(
        &self,
        leader: ChirpsNodeId,
        learner: ChirpsNodeId,
        node: BasicNode,
        timeout: Duration,
    ) -> Result<()> {
        let leader_node = self.nodes.get(&leader).unwrap();
        let mut last_err = None;
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match leader_node.node.add_learner(learner, node.clone()).await {
                Ok(()) => return Ok(()),
                Err(RaftError::MembershipChangeInProgress) => {
                    last_err = Some(RaftError::MembershipChangeInProgress);
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        if let Some(e) = last_err {
            return Err(e.into());
        }
        bail!("add learner did not complete after retries");
    }

    async fn pause_node(&self, id: ChirpsNodeId) {
        if let Some(node) = self.nodes.get(&id) {
            node.pause().await;
        }
    }

    async fn resume_node(&self, id: ChirpsNodeId) {
        if let Some(node) = self.nodes.get(&id) {
            node.resume().await;
        }
    }

    async fn isolate(&self, id: ChirpsNodeId) {
        let mut guard = self.partitioned.lock().await;
        guard.insert(id);
    }

    async fn heal(&self, id: ChirpsNodeId) {
        let mut guard = self.partitioned.lock().await;
        guard.remove(&id);
    }

    async fn states(&self) -> HashMap<ChirpsNodeId, Vec<Vec<u8>>> {
        let mut map = HashMap::new();
        for (id, node) in &self.nodes {
            map.insert(*id, node.state.values().await);
        }
        map
    }
}

fn spawn_pump(
    self_id: ChirpsNodeId,
    transport: Arc<ChirpsRaftTransport>,
    node: Arc<RaftNode>,
    backend: Arc<dyn MessageBackend>,
    running: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    partitioned: Arc<Mutex<HashSet<ChirpsNodeId>>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut rx = backend.subscribe().await.expect("subscribe");
        while running.load(Ordering::SeqCst) {
            if let Some((from, frame)) = rx.recv().await {
                if paused.load(Ordering::SeqCst) {
                    continue;
                }
                let sender = from_wire_node(from);
                let drop_msg = {
                    let guard = partitioned.lock().await;
                    guard.contains(&sender) || guard.contains(&self_id)
                };
                if drop_msg {
                    continue;
                }
                let Some(payload) = ChirpsRaftTransport::decode_frame(frame) else {
                    continue;
                };
                let Some(request) = transport.consume_incoming(payload).await else {
                    continue;
                };
                let correlation_id = request.correlation_id;
                if let Ok(response) = node.handle_message(request).await {
                    let _ = transport
                        .send_response(sender, correlation_id, response)
                        .await;
                }
            } else {
                break;
            }
        }
    })
}

fn spawn_ticker(
    node: Arc<RaftNode>,
    running: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while running.load(Ordering::SeqCst) {
            if !paused.load(Ordering::SeqCst) {
                let _ = node.tick().await;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
}

fn to_wire_node(id: ChirpsNodeId) -> NodeId {
    let mut buf = [0u8; 16];
    buf[8..].copy_from_slice(&id.to_be_bytes());
    NodeId::from(buf)
}

fn from_wire_node(id: NodeId) -> ChirpsNodeId {
    let bytes = id.as_bytes();
    ChirpsNodeId::from_be_bytes(bytes[8..].try_into().unwrap())
}

fn membership_voters(
    metrics: &openraft::metrics::RaftMetrics<ChirpsNodeId, BasicNode>,
) -> BTreeSet<ChirpsNodeId> {
    metrics
        .membership_config
        .membership()
        .get_joint_config()
        .iter()
        .flatten()
        .cloned()
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_leader() -> Result<()> {
    let cluster = TestCluster::new(&[1, 2, 3], 10).await?;
    let start = Instant::now();
    let leader = cluster.wait_for_leader(Duration::from_millis(500)).await?;
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "leader {} elected too slowly",
        leader
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proposals_replicate_to_all_nodes() -> Result<()> {
    let cluster = TestCluster::new(&[11, 12, 13], 20).await?;
    let leader = cluster.wait_for_leader(Duration::from_millis(800)).await?;
    cluster.propose(leader, b"cmd-1").await?;
    cluster.propose(leader, b"cmd-2").await?;
    cluster
        .wait_for_state_len(2, Duration::from_secs(1), true)
        .await?;

    let states = cluster.states().await;
    let expected: Vec<Vec<u8>> = vec![b"cmd-1".to_vec(), b"cmd-2".to_vec()];
    for (id, values) in states {
        assert_eq!(values, expected, "state mismatch on node {}", id);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_changes_promote_and_remove_voters() -> Result<()> {
    let mut cluster = TestCluster::new(&[21, 22], 15).await?;
    cluster.add_node(23).await?;
    let leader = cluster.wait_for_leader(Duration::from_millis(800)).await?;
    let initial_voters: BTreeSet<_> = [21u64, 22u64].into_iter().collect();
    cluster
        .wait_for_membership(&initial_voters, Duration::from_secs(1))
        .await?;

    cluster
        .add_learner_with_retry(
            leader,
            23,
            BasicNode {
                addr: "node-23".into(),
            },
            Duration::from_secs(3),
        )
        .await?;
    cluster
        .change_membership_with_retry(
            leader,
            [21u64, 22u64, 23u64].into_iter().collect(),
            Duration::from_secs(3),
        )
        .await?;
    cluster.propose(leader, b"after-promotion").await?;
    cluster
        .wait_for_state_len(1, Duration::from_secs(2), true)
        .await?;

    let removal: BTreeSet<_> = [21u64, 23u64].into_iter().collect();
    cluster
        .change_membership_with_retry(leader, removal, Duration::from_secs(3))
        .await?;
    cluster.propose(leader, b"after-removal").await?;
    cluster.isolate(22).await;
    cluster
        .wait_for_state_len(2, Duration::from_secs(2), false)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_transfer_catches_up_new_node() -> Result<()> {
    let mut cluster = TestCluster::new(&[31, 32, 33], 5).await?;
    let leader = cluster.wait_for_leader(Duration::from_millis(800)).await?;
    for i in 0..8u8 {
        cluster.propose(leader, &[i]).await?;
    }
    cluster
        .wait_for_state_len(8, Duration::from_secs(2), true)
        .await?;

    cluster.add_node(34).await?;
    cluster
        .add_learner_with_retry(
            leader,
            34,
            BasicNode {
                addr: "node-34".into(),
            },
            Duration::from_secs(3),
        )
        .await?;
    cluster
        .nodes
        .get(&leader)
        .unwrap()
        .node
        .change_membership([31u64, 32u64, 33u64, 34u64].into_iter().collect())
        .await?;
    cluster
        .wait_for_state_len(8, Duration::from_secs(3), true)
        .await?;
    cluster.propose(leader, b"post-snapshot").await?;
    cluster
        .wait_for_state_len(9, Duration::from_secs(2), true)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_handles_node_failure_and_partition() -> Result<()> {
    let cluster = TestCluster::new(&[41, 42, 43], 20).await?;
    let leader = cluster.wait_for_leader(Duration::from_millis(800)).await?;

    cluster.propose(leader, b"before-failure").await?;
    cluster
        .wait_for_state_len(1, Duration::from_secs(1), true)
        .await?;

    let failing = if leader == 41 { 42 } else { 41 };
    cluster.pause_node(failing).await;
    cluster.propose(leader, b"during-failure").await?;
    cluster
        .wait_for_state_len(2, Duration::from_secs(1), false)
        .await?;
    cluster.resume_node(failing).await;
    cluster
        .wait_for_state_len(2, Duration::from_secs(2), true)
        .await?;

    let isolated = 43;
    cluster.isolate(isolated).await;
    cluster.propose(leader, b"during-partition").await?;
    cluster
        .wait_for_state_len(3, Duration::from_secs(1), false)
        .await?;
    cluster.heal(isolated).await;
    cluster
        .wait_for_state_len(3, Duration::from_secs(2), true)
        .await?;
    Ok(())
}
