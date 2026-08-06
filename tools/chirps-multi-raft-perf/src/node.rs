use crate::protocol::{Request, Response, read_frame, write_frame};
use crate::schema::{RawMetricsLine, ReplicaState};
use alopex_chirps::multi_raft::{GroupId, MultiRaftManager, WalRaftStorageFactory};
use alopex_chirps::raft::BasicNode;
use alopex_chirps::{ChirpsRaftTransport, RaftConfig};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_core::config::NodeConfig;
use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
use alopex_chirps_raft_storage::types::LogId;
use alopex_chirps_raft_storage::wal_storage::{WalStorageConfig, process_fsync_calls};
use alopex_chirps_transport_quic::QuicBackend;
use alopex_chirps_wire::frame::{Frame, UserMessage};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore, broadcast, oneshot};
use tokio::time::{sleep, timeout};

const PROBE_MAGIC: &[u8; 8] = b"MRPROBE1";

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
}

#[derive(Clone, Default, serde::Deserialize, serde::Serialize)]
struct AuditSnapshot {
    applied: u64,
    digest: [u8; 32],
}

#[derive(Clone, Default)]
struct AuditStateMachine {
    state: Arc<Mutex<AuditSnapshot>>,
}

impl AuditStateMachine {
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
        let mut hasher = Sha256::new();
        hasher.update(state.digest);
        hasher.update((command.len() as u64).to_be_bytes());
        hasher.update(&command);
        state.digest.copy_from_slice(&hasher.finalize());
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

struct Runtime {
    node_id: u64,
    manager: Arc<Manager>,
    backend: Arc<QuicBackend>,
    audit: Mutex<BTreeMap<u64, AuditStateMachine>>,
    inflight: Mutex<BTreeMap<u64, Arc<AtomicU64>>>,
    probes: Mutex<HashMap<u64, PendingProbe>>,
    next_probe: AtomicU64,
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
    let backend = Arc::new(QuicBackend::new(wire_node_id(args.node_id), config).await?);
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
            ..WalStorageConfig::default()
        },
        args.node_id,
    ));
    let manager = Arc::new(MultiRaftManager::new(
        transport,
        factory,
        RaftConfig {
            node_id: args.node_id,
            election_timeout_ms: 300,
            heartbeat_interval_ms: 100,
            snapshot_threshold: args.snapshot_threshold,
            max_in_snapshot_log_to_keep: args.snapshot_threshold,
            ..RaftConfig::default()
        },
    ));
    let (shutdown, _) = broadcast::channel(4);
    let runtime = Arc::new(Runtime {
        node_id: args.node_id,
        manager,
        backend,
        audit: Mutex::new(BTreeMap::new()),
        inflight: Mutex::new(BTreeMap::new()),
        probes: Mutex::new(HashMap::new()),
        next_probe: AtomicU64::new(1),
        shutdown,
    });

    let pump = spawn_pump(Arc::clone(&runtime)).await?;
    let ticker = spawn_ticker(Arc::clone(&runtime));
    let sampler = spawn_sampler(Arc::clone(&runtime), args.metrics_path);
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
    sampler.abort();
    Ok(())
}

async fn spawn_pump(runtime: Arc<Runtime>) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let mut incoming = runtime.backend.subscribe().await?;
    let dispatch_slots = Arc::new(Semaphore::new(1024));
    Ok(tokio::spawn(async move {
        while let Some((source, frame)) = incoming.recv().await {
            let Ok(slot) = Arc::clone(&dispatch_slots).acquire_owned().await else {
                break;
            };
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                let _slot = slot;
                match frame {
                    Frame::User(message) => handle_probe_frame(&runtime, source, message).await,
                    frame @ (Frame::Raft(_) | Frame::RaftSnapshot(_)) => {
                        match runtime
                            .manager
                            .dispatch_frame(source, wire_node_id(runtime.node_id), frame)
                            .await
                        {
                            Ok(Some(response)) => {
                                if let Err(error) =
                                    runtime.manager.send_routed_response(response).await
                                {
                                    eprintln!(
                                        "node {} failed to send routed Raft response to {}: {error}",
                                        runtime.node_id,
                                        decode_node_id(source)
                                    );
                                }
                            }
                            Ok(None) => {}
                            Err(error) => eprintln!(
                                "node {} failed to dispatch Raft frame from {}: {error}",
                                runtime.node_id,
                                decode_node_id(source)
                            ),
                        }
                    }
                    _ => {}
                }
            });
        }
    }))
}

fn spawn_ticker(runtime: Arc<Runtime>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let _ = runtime.manager.tick_all().await;
            sleep(Duration::from_millis(20)).await;
        }
    })
}

fn spawn_sampler(runtime: Arc<Runtime>, path: PathBuf) -> tokio::task::JoinHandle<()> {
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
        let mut interval = tokio::time::interval(Duration::from_secs(1));
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
    let state = AuditStateMachine::default();
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
    let per_group_queue_depth = runtime
        .inflight
        .lock()
        .await
        .iter()
        .map(|(group, count)| (*group, count.load(Ordering::Relaxed)))
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
    }
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
