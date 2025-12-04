// Benchmark suite for Chirps Raft integration.

use alopex_chirps::{ChirpsRaftTransport, RaftConfig, RaftNode};
use chirps_core::backend::MessageBackend;
use chirps_mock::{MockBackend, MockNetwork};
use chirps_raft_storage::types::{
    BasicNode, ChirpsNodeId, ChirpsTypeConfig, Entry, EntryPayload, GroupId, LogId, LogState,
    Snapshot, SnapshotMeta, StoredMembership, Vote,
};
use chirps_wire::node_id::NodeId;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use openraft::storage::{Adaptor, RaftSnapshotBuilder};
use openraft::{
    CommittedLeaderId, ErrorSubject, ErrorVerb, OptionalSend, RaftLogReader, RaftStorage,
    StorageError, StorageIOError,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
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

// --- In-memory storage and cluster harness (derived from integration tests) ---

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

impl RaftStorage<ChirpsTypeConfig> for MemoryStore {
    type LogReader = MemoryStore;
    type SnapshotBuilder = MemorySnapshotBuilder;

    async fn save_vote(
        &mut self,
        vote: &Vote<ChirpsNodeId>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        let mut guard = self.inner.lock().await;
        guard.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<ChirpsNodeId>>, StorageError<ChirpsNodeId>> {
        let guard = self.inner.lock().await;
        Ok(guard.vote.clone())
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
        Ok(guard.committed.clone())
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<ChirpsTypeConfig>, StorageError<ChirpsNodeId>> {
        let guard = self.inner.lock().await;
        Ok(LogState {
            last_purged_log_id: guard.last_purged.clone(),
            last_log_id: guard.logs.last().map(|e| e.log_id.clone()),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<ChirpsNodeId>>
    where
        I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + OptionalSend,
    {
        let mut guard = self.inner.lock().await;
        let new_entries: Vec<_> = entries.into_iter().collect();
        if new_entries.is_empty() {
            return Ok(());
        }
        let first = new_entries.first().unwrap().log_id.index;
        guard.logs.retain(|e| e.log_id.index < first);
        guard.logs.extend(new_entries);
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<ChirpsNodeId>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        let mut guard = self.inner.lock().await;
        guard.logs.retain(|e| e.log_id.index < log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<ChirpsNodeId>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        let mut guard = self.inner.lock().await;
        guard.logs.retain(|e| e.log_id.index > log_id.index);
        guard.last_purged = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<ChirpsNodeId>>,
            StoredMembership<ChirpsNodeId, BasicNode>,
        ),
        StorageError<ChirpsNodeId>,
    > {
        let guard = self.inner.lock().await;
        Ok((guard.last_applied.clone(), guard.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<ChirpsTypeConfig>],
    ) -> Result<Vec<Vec<u8>>, StorageError<ChirpsNodeId>> {
        let mut guard = self.inner.lock().await;
        let mut responses = Vec::new();
        for entry in entries {
            guard.last_applied = Some(entry.log_id.clone());
            match &entry.payload {
                EntryPayload::Normal(data) => {
                    guard.state.data.lock().await.push(data.clone());
                    responses.push(data.clone());
                }
                EntryPayload::Membership(m) => {
                    guard.last_membership =
                        StoredMembership::new(Some(entry.log_id.clone()), m.clone());
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
            .map(|l| l.leader_id.clone())
            .unwrap_or_else(|| CommittedLeaderId::new(0, 0));
        let last_log_id = Some(LogId::new(leader, index));
        if guard
            .committed
            .as_ref()
            .map(|c| c.index < index)
            .unwrap_or(true)
        {
            guard.committed = last_log_id.clone();
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
        guard.last_applied = meta.last_log_id.clone();
        guard.last_membership = meta.last_membership.clone();
        guard.committed = meta.last_log_id.clone();
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

struct BenchNode {
    id: ChirpsNodeId,
    node: Arc<RaftNode>,
    backend: Arc<dyn MessageBackend>,
    running: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    pump: JoinHandle<()>,
    ticker: JoinHandle<()>,
}

struct BenchCluster {
    group_id: GroupId,
    network: MockNetwork,
    nodes: HashMap<ChirpsNodeId, BenchNode>,
    partitioned: Arc<Mutex<HashSet<ChirpsNodeId>>>,
    snapshot_threshold: u64,
}

impl BenchCluster {
    async fn new(node_ids: &[ChirpsNodeId], snapshot_threshold: u64) -> anyhow::Result<Self> {
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

    async fn add_node(&mut self, id: ChirpsNodeId) -> anyhow::Result<()> {
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
        let (log_store, state_machine) = Adaptor::new(store.clone());

        let mut cfg = RaftConfig::default();
        cfg.group_id = self.group_id;
        cfg.node_id = id;
        cfg.election_timeout_ms = 120;
        cfg.heartbeat_interval_ms = 40;
        cfg.snapshot_threshold = self.snapshot_threshold;
        cfg.max_in_snapshot_log_to_keep = 2 * self.snapshot_threshold;

        let mut node = RaftNode::new(
            cfg,
            ChirpsRaftTransport::factory(transport.clone()),
            log_store,
            state_machine,
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
            BenchNode {
                id,
                node,
                backend,
                running,
                paused,
                pump,
                ticker,
            },
        );
        Ok(())
    }

    async fn initialize(&self) -> anyhow::Result<()> {
        let members: BTreeSet<_> = self.nodes.keys().copied().collect();
        if let Some(first) = members.iter().next() {
            let leader = self.nodes.get(first).unwrap();
            leader.node.initialize(members).await?;
        }
        Ok(())
    }

    async fn wait_for_leader(&self, timeout: Duration) -> anyhow::Result<ChirpsNodeId> {
        let start = Instant::now();
        loop {
            for (_id, node) in &self.nodes {
                if let Some(current) = node.node.leader_id() {
                    return Ok(current);
                }
            }
            if start.elapsed() > timeout {
                anyhow::bail!("leader not elected within {:?}", timeout);
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn propose(&self, leader: ChirpsNodeId, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
        let node = self.nodes.get(&leader).unwrap();
        Ok(node.node.propose(payload.to_vec()).await?)
    }

    async fn pause_node(&self, id: ChirpsNodeId) {
        if let Some(node) = self.nodes.get(&id) {
            node.paused.store(true, Ordering::SeqCst);
        }
    }

    async fn resume_node(&self, id: ChirpsNodeId) {
        if let Some(node) = self.nodes.get(&id) {
            node.paused.store(false, Ordering::SeqCst);
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

    async fn wait_for_state_len(&self, expected: usize, timeout: Duration) -> anyhow::Result<()> {
        let start = Instant::now();
        loop {
            let mut all_match = true;
            for node in self.nodes.values() {
                if node.paused.load(Ordering::SeqCst) {
                    continue;
                }
                if node.node.metrics().last_applied.map(|l| l.index as usize) < Some(expected) {
                    all_match = false;
                    break;
                }
            }
            if all_match {
                return Ok(());
            }
            if start.elapsed() > timeout {
                anyhow::bail!(
                    "state length did not reach {} within {:?}",
                    expected,
                    timeout
                );
            }
            sleep(Duration::from_millis(20)).await;
        }
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
                if let Some(payload) = ChirpsRaftTransport::decode_frame(frame) {
                    if let Some(request) = transport.consume_incoming(payload).await {
                        let correlation_id = request.correlation_id;
                        if let Ok(response) = node.handle_message(request).await {
                            let _ = transport
                                .send_response(sender, correlation_id, response)
                                .await;
                        }
                    }
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

// --- Benchmarks ---

fn bench_proposal_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let cluster = rt
        .block_on(BenchCluster::new(&[1, 2, 3], 50))
        .expect("cluster");
    let leader = rt
        .block_on(cluster.wait_for_leader(Duration::from_millis(800)))
        .expect("leader");
    let payload = vec![0u8; 1024];

    c.bench_with_input(
        BenchmarkId::new("proposal_throughput", "3_nodes_1kb"),
        &payload,
        |b, data| {
            b.to_async(&rt).iter_custom(|iters| {
                let cluster = &cluster;
                let data = data.clone();
                async move {
                    let start = Instant::now();
                    for _ in 0..iters {
                        cluster.propose(leader, &data).await.unwrap();
                    }
                    start.elapsed()
                }
            });
        },
    );
}

fn bench_proposal_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let cluster = rt
        .block_on(BenchCluster::new(&[11, 12, 13], 50))
        .expect("cluster");
    let leader = rt
        .block_on(cluster.wait_for_leader(Duration::from_millis(800)))
        .expect("leader");
    let payload = vec![1u8; 256];

    c.bench_function("proposal_latency_p99", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let cluster = &cluster;
            let mut rng = StdRng::seed_from_u64(7);
            let payload = payload.clone();
            async move {
                let mut latencies = Vec::with_capacity(iters as usize);
                for _ in 0..iters {
                    let mut data = payload.clone();
                    data[0] = rng.r#gen();
                    let start = Instant::now();
                    cluster.propose(leader, &data).await.unwrap();
                    latencies.push(start.elapsed());
                }
                latencies.sort();
                let p99_idx = ((latencies.len().saturating_sub(1) as f64) * 0.99) as usize;
                let p99 = latencies[p99_idx];
                println!("p99 latency: {:?} over {} samples", p99, iters);
                latencies.iter().sum()
            }
        });
    });
}

fn bench_election_time(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let cluster = rt
        .block_on(BenchCluster::new(&[21, 22, 23], 50))
        .expect("cluster");

    c.bench_function("leader_election_after_failure", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let cluster = &cluster;
            async move {
                let mut total = Duration::from_millis(0);
                for _ in 0..iters {
                    let leader = cluster
                        .wait_for_leader(Duration::from_millis(800))
                        .await
                        .unwrap();
                    cluster.pause_node(leader).await;
                    let start = Instant::now();
                    let new_leader = cluster
                        .wait_for_leader(Duration::from_millis(1000))
                        .await
                        .unwrap();
                    total += start.elapsed();
                    cluster.resume_node(leader).await;
                    cluster.heal(new_leader).await;
                }
                total
            }
        });
    });
}

fn bench_snapshot_build(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    c.bench_function("snapshot_build_100mb", |b| {
        b.to_async(&rt).iter(|| async {
            let state_handle = TestStateHandle {
                data: Arc::new(Mutex::new(vec![vec![42u8; 100 * 1024 * 1024]])),
            };
            let mut store = MemoryStore::new(state_handle.clone());
            let mut builder = store.get_snapshot_builder().await;
            let snapshot = builder.build_snapshot().await.unwrap();
            criterion::black_box(snapshot);
        });
    });
}

criterion_group!(
    raft_benches,
    bench_proposal_throughput,
    bench_proposal_latency,
    bench_election_time,
    bench_snapshot_build
);
criterion_main!(raft_benches);
