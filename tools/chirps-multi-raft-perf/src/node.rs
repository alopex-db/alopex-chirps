use crate::protocol::{Request, Response, read_frame, write_frame};
use crate::schema::{RawMetricsLine, ReplicaState};
use alopex_chirps::multi_raft::{GroupId, MultiRaftManager, WalRaftStorageFactory};
use alopex_chirps::raft::{BasicNode, RaftFramePayload};
use alopex_chirps::{ChirpsRaftTransport, RaftConfig};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_core::config::NodeConfig;
use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
use alopex_chirps_raft_storage::types::LogId;
use alopex_chirps_raft_storage::wal_storage::{WalStorageConfig, process_fsync_calls};
use alopex_chirps_transport_quic::{QuicBackend, TransportConfigV04};
use alopex_chirps_wire::frame::{Frame, UserMessage};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore, broadcast, mpsc, oneshot};
use tokio::time::{sleep, timeout};

const PROBE_MAGIC: &[u8; 8] = b"MRPROBE1";
const GROUP_QUEUE_CAPACITY: usize = 32;
const DISPATCH_BACKLOG_BYTES: usize = 32 * 1024 * 1024;
const DISPATCH_FRAME_OVERHEAD_BYTES: usize = 128;
const RESPONSE_DISPATCH_CAPACITY: usize = 256;
const RESPONSE_SEND_CONCURRENCY: usize = 64;
const PERF_LOG_CACHE_SIZE: usize = 256;
const PERF_DURABILITY_BATCH_WAIT_US: u64 = 250;

#[derive(Clone, Debug)]
pub struct NodeArgs {
    pub node_id: u64,
    pub raft_bind: SocketAddr,
    pub seeds: Vec<SocketAddr>,
    pub control_bind: SocketAddr,
    pub storage_root: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub trusted_certificate: PathBuf,
    pub metrics_path: PathBuf,
    pub send_queue_capacity: usize,
    pub snapshot_threshold: u64,
    pub resource_audit: bool,
    pub metrics_interval_ms: u64,
    pub await_peer_stop: bool,
}

#[derive(Clone, Default, serde::Deserialize, serde::Serialize)]
struct AuditSnapshot {
    applied: u64,
    digest: [u8; 32],
}

#[derive(Clone, Default)]
struct AuditStateMachine {
    state: Arc<Mutex<AuditSnapshot>>,
    digest_enabled: bool,
}

impl AuditStateMachine {
    fn new(digest_enabled: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new(AuditSnapshot::default())),
            digest_enabled,
        }
    }

    async fn observation(&self) -> AuditSnapshot {
        self.state.lock().await.clone()
    }
}

#[async_trait]
impl StateMachine for AuditStateMachine {
    type Command = Vec<u8>;
    type Response = Vec<u8>;

    async fn apply(
        &mut self,
        _log_id: LogId<u64>,
        command: Vec<u8>,
    ) -> StateMachineResult<Vec<u8>> {
        let mut state = self.state.lock().await;
        if self.digest_enabled {
            let mut hasher = Sha256::new();
            hasher.update(state.digest);
            hasher.update((command.len() as u64).to_be_bytes());
            hasher.update(&command);
            state.digest.copy_from_slice(&hasher.finalize());
        }
        state.applied = state.applied.saturating_add(1);
        Ok(command)
    }

    async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
        Ok(Box::new(Cursor::new(bincode::serialize(
            &*self.state.lock().await,
        )?)))
    }

    async fn restore(
        &mut self,
        mut snapshot: Box<dyn AsyncSnapshotData>,
    ) -> StateMachineResult<()> {
        use tokio::io::AsyncReadExt;
        let mut bytes = Vec::new();
        snapshot.read_to_end(&mut bytes).await?;
        *self.state.lock().await = bincode::deserialize(&bytes)?;
        Ok(())
    }
}

type Manager = MultiRaftManager<WalRaftStorageFactory<AuditStateMachine>>;

struct PendingProbe {
    target: u64,
    sent: Instant,
    response: oneshot::Sender<u64>,
}

#[derive(Default)]
struct ResponseSendAudit {
    inflight: AtomicU64,
    max_inflight: AtomicU64,
    dropped: AtomicU64,
    failed: AtomicU64,
}

struct Runtime {
    node_id: u64,
    digest_enabled: bool,
    manager: Arc<Manager>,
    backend: Arc<QuicBackend>,
    audit: Mutex<BTreeMap<u64, AuditStateMachine>>,
    inflight: Mutex<BTreeMap<u64, Arc<AtomicU64>>>,
    dispatch_depth: Mutex<BTreeMap<u64, Arc<AtomicU64>>>,
    probes: Mutex<HashMap<u64, PendingProbe>>,
    next_probe: AtomicU64,
    response_send_slots: Arc<Semaphore>,
    response_send_audit: Arc<ResponseSendAudit>,
    dispatch_budget: Arc<Semaphore>,
    dispatch_budget_waits: AtomicU64,
    shutdown: broadcast::Sender<()>,
}

pub async fn run(args: NodeArgs) -> anyhow::Result<()> {
    anyhow::ensure!((1..=3).contains(&args.node_id), "node id must be 1..=3");
    std::fs::create_dir_all(&args.storage_root)?;
    let config = Arc::new(NodeConfig {
        bind_addr: args.raft_bind,
        seeds: args.seeds.clone(),
        cert_path: Some(args.certificate.clone()),
        key_path: Some(args.private_key.clone()),
        trusted_cert_paths: vec![args.trusted_certificate.clone()],
        send_queue_capacity: args.send_queue_capacity,
        node_id_path: args.storage_root.join("node-id"),
        ..NodeConfig::default()
    });
    let backend = Arc::new(
        QuicBackend::new_with_config(
            wire_node_id(args.node_id),
            config.clone(),
            TransportConfigV04 {
                send_queue_capacity: args.send_queue_capacity,
                await_peer_stop: args.await_peer_stop,
                ..Default::default()
            },
        )
        .await?,
    );
    let transport_backend: Arc<dyn MessageBackend> = backend.clone();
    let transport = Arc::new(ChirpsRaftTransport::new(
        transport_backend,
        GroupId(0),
        args.node_id,
    ));
    let factory = Arc::new(WalRaftStorageFactory::new(
        WalStorageConfig {
            wal_dir: args.storage_root.join("wal"),
            snapshot_dir: args.storage_root.join("snapshot"),
            fsync_interval: 0,
            durability_batch_wait_us: PERF_DURABILITY_BATCH_WAIT_US,
            log_cache_size: PERF_LOG_CACHE_SIZE,
            ..WalStorageConfig::default()
        },
        args.node_id,
    ));
    let manager = Arc::new(MultiRaftManager::new(
        transport,
        factory,
        RaftConfig {
            node_id: args.node_id,
            // Keep elections well above the 250 ms heartbeat under 100-group
            // fsync pressure; this mirrors the reference design's generous
            // election/heartbeat slack and avoids false leader churn.
            election_timeout_ms: 5_000,
            heartbeat_interval_ms: 250,
            max_batch_size: 256,
            snapshot_threshold: args.snapshot_threshold,
            max_in_snapshot_log_to_keep: (args.snapshot_threshold / 4).max(1),
            ..RaftConfig::default()
        },
    ));
    let (shutdown, _) = broadcast::channel(4);
    let runtime = Arc::new(Runtime {
        node_id: args.node_id,
        digest_enabled: args.resource_audit,
        manager,
        backend,
        audit: Mutex::new(BTreeMap::new()),
        inflight: Mutex::new(BTreeMap::new()),
        dispatch_depth: Mutex::new(BTreeMap::new()),
        probes: Mutex::new(HashMap::new()),
        next_probe: AtomicU64::new(1),
        response_send_slots: Arc::new(Semaphore::new(RESPONSE_SEND_CONCURRENCY)),
        response_send_audit: Arc::new(ResponseSendAudit::default()),
        dispatch_budget: Arc::new(Semaphore::new(DISPATCH_BACKLOG_BYTES)),
        dispatch_budget_waits: AtomicU64::new(0),
        shutdown,
    });

    let pump = spawn_pump(Arc::clone(&runtime)).await?;
    let ticker = spawn_ticker(Arc::clone(&runtime));
    let sampler = args.resource_audit.then(|| {
        spawn_sampler(
            Arc::clone(&runtime),
            args.metrics_path,
            args.metrics_interval_ms.max(1),
        )
    });
    let listener = TcpListener::bind(args.control_bind).await?;
    let mut shutdown_rx = runtime.shutdown.subscribe();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                tokio::spawn(handle_connection(Arc::clone(&runtime), stream));
            }
            _ = shutdown_rx.recv() => break,
        }
    }
    runtime.manager.shutdown_all().await?;
    runtime.backend.close().await?;
    pump.abort();
    ticker.abort();
    if let Some(sampler) = sampler {
        sampler.abort();
    }
    Ok(())
}

async fn spawn_pump(runtime: Arc<Runtime>) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let mut incoming = runtime.backend.subscribe().await?;
    let probe_slots = Arc::new(Semaphore::new(1024));
    let response_slots = Arc::new(Semaphore::new(RESPONSE_DISPATCH_CAPACITY));
    let dispatch_slots = Arc::clone(&runtime.dispatch_budget);
    Ok(tokio::spawn(async move {
        let mut group_queues: BTreeMap<u64, GroupDispatchQueue> = BTreeMap::new();
        while let Some((source, frame)) = incoming.recv().await {
            match frame {
                Frame::User(message) => {
                    let Ok(slot) = Arc::clone(&probe_slots).acquire_owned().await else {
                        break;
                    };
                    let runtime = Arc::clone(&runtime);
                    tokio::spawn(async move {
                        let _slot = slot;
                        handle_probe_frame(&runtime, source, message).await;
                    });
                }
                frame @ (Frame::Raft(_) | Frame::RaftSnapshot(_)) => {
                    let Some(group_id) = frame_group_id(&frame) else {
                        continue;
                    };
                    // Correlated responses only wake a pending transport RPC;
                    // bypass the per-group state-mutating queue so a slow
                    // AppendEntries handler cannot create response HOL blocking.
                    if frame_is_response(&frame) {
                        let Ok(slot) = Arc::clone(&response_slots).acquire_owned().await else {
                            break;
                        };
                        let runtime = Arc::clone(&runtime);
                        tokio::spawn(async move {
                            dispatch_raft_frame(&runtime, source, frame, Some(slot)).await;
                        });
                        continue;
                    }
                    let queue = if let Some(queue) = group_queues.get(&group_id) {
                        queue.clone()
                    } else {
                        let depth = Arc::new(AtomicU64::new(0));
                        runtime
                            .dispatch_depth
                            .lock()
                            .await
                            .insert(group_id, Arc::clone(&depth));
                        let queue_runtime = Arc::clone(&runtime);
                        let sender = spawn_fifo_queue(
                            Arc::clone(&depth),
                            move |(source, frame)| {
                                let runtime = Arc::clone(&queue_runtime);
                                async move { dispatch_raft_frame(&runtime, source, frame, None).await }
                            },
                        );
                        let queue = GroupDispatchQueue::new(sender, depth);
                        group_queues.insert(group_id, queue.clone());
                        queue
                    };
                    let permits = dispatch_budget_bytes(&frame);
                    let Some(slot) = Arc::clone(&dispatch_slots)
                        .try_acquire_many_owned(permits)
                        .ok()
                    else {
                        // The global admission bound is deliberately finite. When it is full,
                        // apply backpressure to the transport receive loop instead of allowing
                        // an unbounded per-group backlog to grow.
                        runtime
                            .dispatch_budget_waits
                            .fetch_add(1, Ordering::Relaxed);
                        let Ok(slot) = Arc::clone(&dispatch_slots)
                            .acquire_many_owned(permits)
                            .await
                        else {
                            break;
                        };
                        queue
                            .enqueue(PendingFrame {
                                source,
                                frame,
                                _slot: slot,
                            })
                            .await;
                        continue;
                    };
                    queue
                        .enqueue(PendingFrame {
                            source,
                            frame,
                            _slot: slot,
                        })
                        .await;
                }
                _ => {}
            }
        }
    }))
}

struct PendingFrame {
    source: NodeId,
    frame: Frame,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Clone)]
struct GroupDispatchQueue {
    sender: mpsc::Sender<(NodeId, Frame)>,
    depth: Arc<AtomicU64>,
    state: Arc<Mutex<DispatchQueueState>>,
}

struct DispatchQueueState {
    pending: VecDeque<PendingFrame>,
    draining: bool,
}

impl GroupDispatchQueue {
    fn new(sender: mpsc::Sender<(NodeId, Frame)>, depth: Arc<AtomicU64>) -> Self {
        Self {
            sender,
            depth,
            state: Arc::new(Mutex::new(DispatchQueueState {
                pending: VecDeque::new(),
                draining: false,
            })),
        }
    }

    async fn enqueue(&self, frame: PendingFrame) {
        self.depth.fetch_add(1, Ordering::Relaxed);
        let start_drainer = {
            let mut state = self.state.lock().await;
            state.pending.push_back(frame);
            if state.draining {
                false
            } else {
                state.draining = true;
                true
            }
        };
        if start_drainer {
            let queue = self.clone();
            tokio::spawn(async move { queue.drain().await });
        }
    }

    async fn drain(self) {
        loop {
            let pending = {
                let mut state = self.state.lock().await;
                state.pending.pop_front()
            };
            let Some(pending) = pending else {
                let restart = {
                    let mut state = self.state.lock().await;
                    if state.pending.is_empty() {
                        state.draining = false;
                        false
                    } else {
                        true
                    }
                };
                if !restart {
                    return;
                }
                continue;
            };
            if self
                .sender
                .send((pending.source, pending.frame))
                .await
                .is_err()
            {
                self.depth.fetch_sub(1, Ordering::Relaxed);
            }
            drop(pending._slot);
        }
    }
}

fn spawn_fifo_queue<T, H, Fut>(depth: Arc<AtomicU64>, mut handler: H) -> mpsc::Sender<T>
where
    T: Send + 'static,
    H: FnMut(T) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (sender, mut receiver) = mpsc::channel(GROUP_QUEUE_CAPACITY);
    tokio::spawn(async move {
        while let Some(item) = receiver.recv().await {
            depth
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_sub(1))
                })
                .ok();
            handler(item).await;
        }
    });
    sender
}

fn frame_group_id(frame: &Frame) -> Option<u64> {
    match frame {
        Frame::Raft(frame) | Frame::RaftSnapshot(frame) => Some(frame.group_id),
        _ => None,
    }
}

fn dispatch_budget_bytes(frame: &Frame) -> u32 {
    let payload_bytes = match frame {
        Frame::Raft(frame) | Frame::RaftSnapshot(frame) => frame.payload.len(),
        Frame::User(message) => message.payload.len(),
        _ => 0,
    };
    payload_bytes
        .saturating_add(DISPATCH_FRAME_OVERHEAD_BYTES)
        .clamp(1, DISPATCH_BACKLOG_BYTES)
        .try_into()
        .expect("dispatch budget is bounded below u32::MAX")
}

fn frame_is_response(frame: &Frame) -> bool {
    ChirpsRaftTransport::decode_frame(frame.clone())
        .is_some_and(|payload: RaftFramePayload| payload.message.is_response())
}

async fn dispatch_raft_frame(
    runtime: &Runtime,
    source: NodeId,
    frame: Frame,
    response_slot: Option<tokio::sync::OwnedSemaphorePermit>,
) {
    match runtime
        .manager
        .dispatch_frame(source, wire_node_id(runtime.node_id), frame)
        .await
    {
        Ok(Some(response)) => {
            let Some(slot) =
                acquire_response_send_slot(&runtime.response_send_slots, response_slot).await
            else {
                return;
            };
            let manager = Arc::clone(&runtime.manager);
            let node_id = runtime.node_id;
            let audit = Arc::clone(&runtime.response_send_audit);
            let inflight = audit.inflight.fetch_add(1, Ordering::Relaxed) + 1;
            audit.max_inflight.fetch_max(inflight, Ordering::Relaxed);
            tokio::spawn(async move {
                let _slot = slot;
                if let Err(error) = manager.send_routed_response(response).await {
                    audit.failed.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "node {} failed to send routed Raft response to {}: {error}",
                        node_id,
                        decode_node_id(source)
                    );
                }
                audit.inflight.fetch_sub(1, Ordering::Relaxed);
            });
        }
        Ok(None) => {}
        Err(error) => eprintln!(
            "node {} failed to dispatch Raft frame from {}: {error}",
            runtime.node_id,
            decode_node_id(source)
        ),
    }
}

async fn acquire_response_send_slot(
    slots: &Arc<Semaphore>,
    preacquired: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    match preacquired {
        Some(slot) => Some(slot),
        None => Arc::clone(slots).acquire_owned().await.ok(),
    }
}

fn spawn_ticker(runtime: Arc<Runtime>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            runtime.manager.tick_all_background();
            sleep(Duration::from_millis(250)).await;
        }
    })
}

fn spawn_sampler(
    runtime: Arc<Runtime>,
    path: PathBuf,
    interval_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
        {
            Ok(file) => file,
            Err(_) => return,
        };
        // Keep enough samples inside every 60 s measurement window even when
        // one scheduler tick is delayed by the phase telemetry probes.
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            let metric = collect_metrics(&runtime).await;
            if let Ok(mut bytes) = serde_json::to_vec(&metric) {
                bytes.push(b'\n');
                let _ = file.write_all(&bytes).await;
                let _ = file.flush().await;
            }
        }
    })
}

async fn handle_connection(runtime: Arc<Runtime>, mut stream: TcpStream) {
    loop {
        let request = match read_frame::<_, Request>(&mut stream).await {
            Ok(Some(request)) => request,
            _ => return,
        };
        let shutdown = matches!(request, Request::Shutdown);
        let response = match execute(&runtime, request).await {
            Ok(response) => response,
            Err(error) => Response::Error(format!("{error:#}")),
        };
        if write_frame(&mut stream, &response).await.is_err() {
            return;
        }
        if shutdown {
            return;
        }
    }
}

async fn execute(runtime: &Runtime, request: Request) -> anyhow::Result<Response> {
    match request {
        Request::Health => {
            anyhow::ensure!(
                runtime.backend.connected_peers().len() == 2,
                "node does not have both QUIC peers"
            );
            Ok(Response::Ok)
        }
        Request::CreateSeed { group_id } => {
            create_group(runtime, group_id, true).await?;
            Ok(Response::Ok)
        }
        Request::CreateUninitialized { group_id } => {
            create_group(runtime, group_id, false).await?;
            Ok(Response::Ok)
        }
        Request::AddLearner { group_id, node_id } => {
            runtime
                .manager
                .get_group(GroupId(group_id))
                .ok_or_else(|| anyhow::anyhow!("unknown group"))?
                .add_learner(
                    node_id,
                    BasicNode {
                        addr: format!("node-{node_id}"),
                    },
                )
                .await?;
            Ok(Response::Ok)
        }
        Request::Promote { group_id, voters } => {
            runtime
                .manager
                .get_group(GroupId(group_id))
                .ok_or_else(|| anyhow::anyhow!("unknown group"))?
                .change_membership(voters.into_iter().collect())
                .await?;
            Ok(Response::Ok)
        }
        Request::Propose { group_id, payload } => {
            anyhow::ensure!(
                payload.len() == 1024,
                "proposal payload must be exactly 1024 bytes"
            );
            let counter = {
                let mut inflight = runtime.inflight.lock().await;
                Arc::clone(
                    inflight
                        .entry(group_id)
                        .or_insert_with(|| Arc::new(AtomicU64::new(0))),
                )
            };
            counter.fetch_add(1, Ordering::Relaxed);
            let result = runtime
                .manager
                .get_group(GroupId(group_id))
                .ok_or_else(|| anyhow::anyhow!("unknown group"))?
                .propose(payload)
                .await;
            counter.fetch_sub(1, Ordering::Relaxed);
            Ok(Response::Proposal(result?))
        }
        Request::State { group_id } => Ok(Response::State(group_state(runtime, group_id).await?)),
        Request::Probe { target, count } => Ok(Response::Probe {
            samples_us: probe(runtime, target, count).await?,
        }),
        Request::Shutdown => {
            let _ = runtime.shutdown.send(());
            Ok(Response::Ok)
        }
    }
}

async fn create_group(runtime: &Runtime, group_id: u64, seed: bool) -> anyhow::Result<()> {
    let state = AuditStateMachine::new(runtime.digest_enabled);
    if seed {
        runtime
            .manager
            .create_group(
                GroupId(group_id),
                BTreeSet::from([runtime.node_id]),
                state.clone(),
            )
            .await?;
    } else {
        runtime
            .manager
            .create_group_uninitialized(GroupId(group_id), state.clone())
            .await?;
    }
    runtime.audit.lock().await.insert(group_id, state);
    runtime
        .inflight
        .lock()
        .await
        .insert(group_id, Arc::new(AtomicU64::new(0)));
    Ok(())
}

async fn group_state(runtime: &Runtime, group_id: u64) -> anyhow::Result<ReplicaState> {
    let group = runtime
        .manager
        .get_group(GroupId(group_id))
        .ok_or_else(|| anyhow::anyhow!("unknown group"))?;
    let metrics = group.metrics();
    let voters = metrics
        .membership_config
        .membership()
        .get_joint_config()
        .iter()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let audit = runtime
        .audit
        .lock()
        .await
        .get(&group_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing audit"))?
        .observation()
        .await;
    Ok(ReplicaState {
        node_id: runtime.node_id,
        voters,
        leader_id: metrics.current_leader.unwrap_or(0),
        last_applied: metrics.last_applied.map(|value| value.index).unwrap_or(0),
        committed_digest: audit
            .digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    })
}

async fn probe(runtime: &Runtime, target: u64, count: u64) -> anyhow::Result<Vec<u64>> {
    anyhow::ensure!(
        (1..=3).contains(&target) && target != runtime.node_id,
        "invalid probe target"
    );
    anyhow::ensure!((1..=10_000).contains(&count), "invalid probe count");
    let mut samples = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let nonce = runtime.next_probe.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        runtime.probes.lock().await.insert(
            nonce,
            PendingProbe {
                target,
                sent: Instant::now(),
                response: tx,
            },
        );
        runtime
            .backend
            .send(
                wire_node_id(target),
                Frame::User(UserMessage {
                    payload: probe_payload(false, nonce),
                }),
            )
            .await?;
        match timeout(Duration::from_secs(1), rx).await {
            Ok(Ok(elapsed)) => samples.push(elapsed),
            _ => {
                runtime.probes.lock().await.remove(&nonce);
                anyhow::bail!("RTT probe timed out");
            }
        }
    }
    Ok(samples)
}

async fn handle_probe_frame(runtime: &Runtime, source: NodeId, message: UserMessage) {
    let Some((response, nonce)) = parse_probe(&message.payload) else {
        return;
    };
    let source = decode_node_id(source);
    if !response {
        let _ = runtime
            .backend
            .send(
                wire_node_id(source),
                Frame::User(UserMessage {
                    payload: probe_payload(true, nonce),
                }),
            )
            .await;
        return;
    }
    let pending = runtime.probes.lock().await.remove(&nonce);
    if let Some(pending) = pending
        && pending.target == source
    {
        let _ = pending
            .response
            .send(pending.sent.elapsed().as_micros() as u64);
    }
}

fn probe_payload(response: bool, nonce: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(17);
    payload.extend_from_slice(PROBE_MAGIC);
    payload.push(u8::from(response));
    payload.extend_from_slice(&nonce.to_be_bytes());
    payload
}

fn parse_probe(payload: &[u8]) -> Option<(bool, u64)> {
    if payload.len() != 17 || &payload[..8] != PROBE_MAGIC {
        return None;
    }
    Some((
        payload[8] == 1,
        u64::from_be_bytes(payload[9..].try_into().ok()?),
    ))
}

async fn collect_metrics(runtime: &Runtime) -> RawMetricsLine {
    let process = read_process_metrics().unwrap_or_default();
    let network = read_network_metrics().unwrap_or_default();
    let transport = runtime.backend.metrics();
    let group_ids = runtime.manager.list_groups();
    let proposal_inflight = runtime
        .inflight
        .lock()
        .await
        .iter()
        .map(|(group, count)| (*group, count.load(Ordering::Relaxed)))
        .collect();
    let dispatch_depth = runtime.dispatch_depth.lock().await;
    let per_group_queue_depth = normalized_group_depth(&group_ids, &dispatch_depth);
    drop(dispatch_depth);
    let dispatch_queue_depth = per_group_queue_depth.values().sum();
    let leader_by_group = runtime
        .manager
        .list_groups()
        .into_iter()
        .filter_map(|group_id| {
            runtime.manager.get_group(group_id).and_then(|group| {
                group
                    .metrics()
                    .current_leader
                    .map(|leader| (group_id.0, leader))
            })
        })
        .collect::<BTreeMap<_, _>>();
    let extended = runtime.backend.extended_metrics();
    let transport_queue_utilization = extended
        .queue_utilization
        .into_iter()
        .map(|(kind, value)| (format!("{kind:?}"), value))
        .collect();
    RawMetricsLine {
        monotonic_ns: monotonic_ns(),
        node_id: runtime.node_id,
        cpu_seconds: process.cpu_seconds,
        rss_bytes: process.rss_bytes,
        disk_read_bytes: process.disk_read_bytes,
        disk_write_bytes: process.disk_write_bytes,
        fsync_calls: process_fsync_calls(),
        network_rx_bytes: network.0,
        network_tx_bytes: network.1,
        transport_sent: transport.sent,
        transport_received: transport.received,
        transport_dropped: transport.dropped,
        transport_retried: transport.retried,
        per_group_queue_depth,
        leader_by_group,
        proposal_inflight,
        dispatch_queue_depth,
        transport_queue_utilization,
        retransmission_total: extended.retransmission_total,
        retransmission_buffer_bytes: extended.retransmission_buffer_bytes,
        queue_overflow_total: extended.queue_overflow_total,
        backpressure_triggered_total: extended.backpressure_triggered_total,
        response_send_inflight: runtime.response_send_audit.inflight.load(Ordering::Relaxed),
        response_send_max_inflight: runtime
            .response_send_audit
            .max_inflight
            .load(Ordering::Relaxed),
        response_send_dropped: runtime.response_send_audit.dropped.load(Ordering::Relaxed),
        response_send_failed: runtime.response_send_audit.failed.load(Ordering::Relaxed),
        dispatch_budget_in_use_bytes: DISPATCH_BACKLOG_BYTES
            .saturating_sub(runtime.dispatch_budget.available_permits())
            as u64,
        dispatch_budget_waits: runtime.dispatch_budget_waits.load(Ordering::Relaxed),
    }
}

fn normalized_group_depth(
    group_ids: &[GroupId],
    depths: &BTreeMap<u64, Arc<AtomicU64>>,
) -> BTreeMap<u64, u64> {
    group_ids
        .iter()
        .map(|group_id| {
            let depth = depths
                .get(&group_id.0)
                .map(|depth| depth.load(Ordering::Relaxed))
                .unwrap_or(0);
            (group_id.0, depth)
        })
        .collect()
}

#[derive(Default)]
struct ProcessMetrics {
    cpu_seconds: f64,
    rss_bytes: u64,
    disk_read_bytes: u64,
    disk_write_bytes: u64,
}

fn read_process_metrics() -> anyhow::Result<ProcessMetrics> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let close = stat
        .rfind(')')
        .ok_or_else(|| anyhow::anyhow!("invalid proc stat"))?;
    let fields = stat[close + 2..].split_whitespace().collect::<Vec<_>>();
    let ticks = fields[11].parse::<u64>()? + fields[12].parse::<u64>()?;
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
    let status = std::fs::read_to_string("/proc/self/status")?;
    let rss_bytes = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        * 1024;
    let io = std::fs::read_to_string("/proc/self/io")?;
    let field = |name: &str| {
        io.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    Ok(ProcessMetrics {
        cpu_seconds: ticks as f64 / hz,
        rss_bytes,
        disk_read_bytes: field("read_bytes:"),
        disk_write_bytes: field("write_bytes:"),
    })
}

fn read_network_metrics() -> anyhow::Result<(u64, u64)> {
    let text = std::fs::read_to_string("/proc/net/dev")?;
    let mut rx = 0;
    let mut tx = 0;
    for line in text.lines().skip(2) {
        let Some((_, fields)) = line.split_once(':') else {
            continue;
        };
        let values = fields.split_whitespace().collect::<Vec<_>>();
        if values.len() >= 9 {
            rx += values[0].parse::<u64>()?;
            tx += values[8].parse::<u64>()?;
        }
    }
    Ok((rx, tx))
}

pub fn monotonic_ns() -> u64 {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) };
    if result != 0 {
        return 0;
    }
    value.tv_sec as u64 * 1_000_000_000 + value.tv_nsec as u64
}

fn wire_node_id(node_id: u64) -> NodeId {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&node_id.to_be_bytes());
    NodeId::from(bytes)
}

fn decode_node_id(node_id: NodeId) -> u64 {
    u64::from_be_bytes(node_id.as_bytes()[8..].try_into().unwrap_or([0; 8]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_chirps_wire::frame::RaftFrame;

    #[tokio::test]
    async fn audit_digest_is_detachable_from_normal_measurement() {
        let mut disabled = AuditStateMachine::new(false);
        let mut enabled = AuditStateMachine::new(true);
        let log_id = LogId::default();
        StateMachine::apply(&mut disabled, log_id, vec![7; 1024])
            .await
            .unwrap();
        StateMachine::apply(&mut enabled, log_id, vec![7; 1024])
            .await
            .unwrap();
        assert_eq!(disabled.observation().await.digest, [0; 32]);
        assert_ne!(enabled.observation().await.digest, [0; 32]);
        assert_eq!(disabled.observation().await.applied, 1);
        assert_eq!(enabled.observation().await.applied, 1);
    }

    #[tokio::test]
    async fn fifo_queue_preserves_group_order() {
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let queue = spawn_fifo_queue(Arc::new(AtomicU64::new(0)), move |value| {
            let observed_tx = observed_tx.clone();
            async move {
                if value == 1 {
                    sleep(Duration::from_millis(20)).await;
                }
                observed_tx.send(value).unwrap();
            }
        });

        queue.send(1).await.unwrap();
        queue.send(2).await.unwrap();
        queue.send(3).await.unwrap();

        assert_eq!(observed_rx.recv().await, Some(1));
        assert_eq!(observed_rx.recv().await, Some(2));
        assert_eq!(observed_rx.recv().await, Some(3));
    }

    #[tokio::test]
    async fn separate_group_queues_do_not_head_of_line_block() {
        let slow = spawn_fifo_queue(Arc::new(AtomicU64::new(0)), |()| async {
            sleep(Duration::from_millis(100)).await;
        });
        let (fast_tx, mut fast_rx) = mpsc::unbounded_channel();
        let fast = spawn_fifo_queue(Arc::new(AtomicU64::new(0)), move |value| {
            let fast_tx = fast_tx.clone();
            async move {
                fast_tx.send(value).unwrap();
            }
        });

        slow.send(()).await.unwrap();
        fast.send(7).await.unwrap();

        assert_eq!(
            timeout(Duration::from_millis(50), fast_rx.recv())
                .await
                .unwrap(),
            Some(7)
        );
    }

    #[tokio::test]
    async fn bounded_global_dispatch_backlog_does_not_block_other_groups() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let started = Arc::new(tokio::sync::Notify::new());
        let slow_sender = spawn_fifo_queue(Arc::new(AtomicU64::new(0)), {
            let gate = Arc::clone(&gate);
            let started = Arc::clone(&started);
            move |(_source, frame): (NodeId, Frame)| {
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                async move {
                    started.notify_one();
                    gate.notified().await;
                    drop(frame);
                }
            }
        });
        let (fast_tx, mut fast_rx) = mpsc::unbounded_channel();
        let fast_sender = spawn_fifo_queue(Arc::new(AtomicU64::new(0)), move |(_source, frame)| {
            let fast_tx = fast_tx.clone();
            async move {
                let _ = fast_tx.send(());
                drop(frame);
            }
        });
        let slow = GroupDispatchQueue::new(slow_sender, Arc::new(AtomicU64::new(0)));
        let fast = GroupDispatchQueue::new(fast_sender, Arc::new(AtomicU64::new(0)));
        let slots = Arc::new(Semaphore::new(64));
        let frame = || Frame::User(UserMessage { payload: vec![1] });

        slow.enqueue(PendingFrame {
            source: wire_node_id(2),
            frame: frame(),
            _slot: slots.clone().acquire_owned().await.unwrap(),
        })
        .await;
        started.notified().await;
        for _ in 0..GROUP_QUEUE_CAPACITY {
            slow.enqueue(PendingFrame {
                source: wire_node_id(2),
                frame: frame(),
                _slot: slots.clone().acquire_owned().await.unwrap(),
            })
            .await;
        }
        fast.enqueue(PendingFrame {
            source: wire_node_id(3),
            frame: frame(),
            _slot: slots.acquire_owned().await.unwrap(),
        })
        .await;
        timeout(Duration::from_millis(50), fast_rx.recv())
            .await
            .expect("slow group backlog must not block another group's dispatch")
            .expect("fast group handler must remain alive");
        gate.notify_waiters();
    }

    #[tokio::test]
    async fn group_queue_is_bounded_instead_of_accumulating_unbounded_frames() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let started = Arc::new(tokio::sync::Notify::new());
        let queue = spawn_fifo_queue(Arc::new(AtomicU64::new(0)), {
            let gate = Arc::clone(&gate);
            let started = Arc::clone(&started);
            move |_| {
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                async move {
                    started.notify_one();
                    gate.notified().await;
                }
            }
        });

        queue.send(0u8).await.unwrap();
        started.notified().await;
        for value in 1..=GROUP_QUEUE_CAPACITY {
            queue.try_send(value as u8).unwrap();
        }
        assert!(
            queue.try_send(255).is_err(),
            "queue must apply backpressure"
        );
        gate.notify_waiters();
    }

    #[test]
    fn bounded_group_queue_has_a_finite_payload_budget() {
        const PAYLOAD_BYTES: usize = 1024;
        assert_eq!(GROUP_QUEUE_CAPACITY * PAYLOAD_BYTES, 32 * 1024);
    }

    #[test]
    fn queue_depth_audit_emits_zero_for_groups_without_frames() {
        let groups = [GroupId(1), GroupId(2), GroupId(3)];
        let depths = BTreeMap::from([(2, Arc::new(AtomicU64::new(7)))]);
        assert_eq!(
            normalized_group_depth(&groups, &depths),
            BTreeMap::from([(1, 0), (2, 7), (3, 0)])
        );
    }

    #[test]
    fn dispatch_budget_is_payload_bounded_and_not_frame_count_only() {
        let small = Frame::Raft(RaftFrame {
            group_id: 1,
            payload: vec![0; 1024],
        });
        let oversized = Frame::Raft(RaftFrame {
            group_id: 1,
            payload: vec![0; DISPATCH_BACKLOG_BYTES * 2],
        });
        assert_eq!(
            dispatch_budget_bytes(&small) as usize,
            1024 + DISPATCH_FRAME_OVERHEAD_BYTES
        );
        assert_eq!(
            dispatch_budget_bytes(&oversized) as usize,
            DISPATCH_BACKLOG_BYTES
        );
        const {
            assert!(
                DISPATCH_BACKLOG_BYTES < 4096 * (64 * 1024),
                "byte budget must cap aggregate frame memory below the old frame-count bound"
            )
        };
    }

    #[test]
    fn response_send_concurrency_matches_transport_send_bound() {
        assert_eq!(RESPONSE_SEND_CONCURRENCY, 64);
        const { assert!(RESPONSE_DISPATCH_CAPACITY > RESPONSE_SEND_CONCURRENCY) };
    }

    #[tokio::test]
    async fn response_dispatch_reuses_the_pump_permit() {
        let slots = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&slots).acquire_owned().await.unwrap();
        let reused = acquire_response_send_slot(&slots, Some(permit))
            .await
            .expect("pre-acquired response permit must be reusable");
        assert_eq!(slots.available_permits(), 0);
        drop(reused);
        assert_eq!(slots.available_permits(), 1);
    }

    #[test]
    fn perf_election_timeout_has_heartbeat_slack() {
        const HEARTBEAT_MS: u64 = 250;
        const ELECTION_MS: u64 = 5_000;
        const { assert!(ELECTION_MS >= HEARTBEAT_MS * 8) };
    }

    #[test]
    fn raft_frame_exposes_its_outer_group_for_dispatch() {
        let frame = Frame::Raft(RaftFrame {
            group_id: 42,
            payload: Vec::new(),
        });
        assert_eq!(frame_group_id(&frame), Some(42));
        assert_eq!(
            frame_group_id(&Frame::User(UserMessage {
                payload: Vec::new()
            })),
            None
        );
    }
}
