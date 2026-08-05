use alopex_chirps::multi_raft::{
    GroupId, MultiRaftError, MultiRaftManager, WalRaftStorageFactory, group_namespace,
};
use alopex_chirps::raft::RaftFramePayload;
use alopex_chirps::{ChirpsRaftTransport, RaftConfig, RaftMessage};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_core::error::TransportError;
use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
use alopex_chirps_raft_storage::types::{LogId, Vote, VoteRequest};
use alopex_chirps_raft_storage::wal_storage::WalStorageConfig;
use alopex_chirps_wire::frame::Frame;
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use chirps_deterministic_harness::protocol::{
    GroupObservation, ParentMessage, RouteObservation, WORKER_PROTOCOL_VERSION, WorkerAction,
    WorkerCommand, WorkerFailure, WorkerMessage, WorkerObservation, WorkerResult, read_message,
    write_message,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

type NetworkAcceptance = Result<(), TransportError>;
type PendingAcceptances = Arc<AsyncMutex<HashMap<u64, oneshot::Sender<NetworkAcceptance>>>>;

#[derive(Clone, Default)]
struct AuditHandle(Arc<Mutex<AuditState>>);

#[derive(Default)]
struct AuditState {
    applies: u64,
    digest: [u8; 32],
}

struct AuditedStateMachine {
    audit: AuditHandle,
}

#[async_trait]
impl StateMachine for AuditedStateMachine {
    type Command = Vec<u8>;
    type Response = Vec<u8>;

    async fn apply(
        &mut self,
        log_id: LogId<u64>,
        command: Self::Command,
    ) -> StateMachineResult<Self::Response> {
        let mut audit = self.audit.0.lock().unwrap();
        let mut hasher = Sha256::new();
        hasher.update(audit.digest);
        hasher.update(log_id.index.to_be_bytes());
        hasher.update(&command);
        audit.digest.copy_from_slice(&hasher.finalize());
        audit.applies += 1;
        Ok(command)
    }

    async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
        let digest = self.audit.0.lock().unwrap().digest.to_vec();
        Ok(Box::new(Cursor::new(digest)))
    }

    async fn restore(&mut self, _snapshot: Box<dyn AsyncSnapshotData>) -> StateMachineResult<()> {
        Ok(())
    }
}

struct IpcBackend {
    node_id: NodeId,
    output: mpsc::Sender<WorkerMessage>,
    pending: PendingAcceptances,
    next_outbound: AtomicU64,
    closed: AtomicBool,
}

#[async_trait]
impl MessageBackend for IpcBackend {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Connection(
                "worker backend is closed".into(),
            ));
        }
        let outbound_id = self.next_outbound.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(outbound_id, sender);
        self.output
            .send(WorkerMessage::OutboundFrame {
                outbound_id,
                source: self.node_id,
                target,
                frame: Box::new(frame),
            })
            .await
            .map_err(|error| TransportError::Send(error.to_string()))?;
        receiver
            .await
            .map_err(|_| TransportError::Connection("network acceptance channel closed".into()))?
    }

    async fn broadcast(&self, _frame: Frame) -> Result<usize, TransportError> {
        Err(TransportError::Send(
            "worker broadcast requires an explicit deterministic target".into(),
        ))
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        Err(TransportError::Subscribe(
            "worker receives frames through DeliverFrame commands".into(),
        ))
    }

    async fn close(&self) -> Result<(), TransportError> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        Vec::new()
    }
}

type Manager = MultiRaftManager<WalRaftStorageFactory<AuditedStateMachine>>;

pub async fn run_worker(node_id: u64, storage_root: PathBuf) -> anyhow::Result<()> {
    let (output_tx, mut output_rx) = mpsc::channel::<WorkerMessage>(128);
    let pending = Arc::new(AsyncMutex::new(HashMap::new()));
    let backend = Arc::new(IpcBackend {
        node_id: wire_node_id(node_id),
        output: output_tx.clone(),
        pending: Arc::clone(&pending),
        next_outbound: AtomicU64::new(1),
        closed: AtomicBool::new(false),
    });
    let backend_trait: Arc<dyn MessageBackend> = backend.clone();
    let transport = Arc::new(ChirpsRaftTransport::new(backend_trait, GroupId(0), node_id));
    let factory = Arc::new(WalRaftStorageFactory::new(
        WalStorageConfig {
            wal_dir: storage_root.join("wal"),
            snapshot_dir: storage_root.join("snapshot"),
            ..WalStorageConfig::default()
        },
        node_id,
    ));
    let manager = Arc::new(MultiRaftManager::new(
        transport,
        factory,
        RaftConfig {
            node_id,
            ..RaftConfig::default()
        },
    ));
    let audits = Arc::new(Mutex::new(BTreeMap::<u64, AuditHandle>::new()));
    let (command_tx, mut command_rx) = mpsc::channel::<WorkerCommand>(32);

    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(message) = output_rx.recv().await {
            write_message(&mut stdout, &message).await?;
        }
        anyhow::Ok(())
    });

    let reader_pending = Arc::clone(&pending);
    let reader = tokio::spawn(async move {
        let mut stdin = tokio::io::stdin();
        loop {
            let message: ParentMessage = match read_message(&mut stdin).await {
                Ok(message) => message,
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|io| io.kind() == std::io::ErrorKind::UnexpectedEof) =>
                {
                    break;
                }
                Err(error) => return Err(error),
            };
            match message {
                ParentMessage::Command(command) => {
                    if command_tx.send(*command).await.is_err() {
                        break;
                    }
                }
                ParentMessage::NetworkAccepted {
                    outbound_id,
                    accepted,
                    reason,
                } => {
                    if let Some(sender) = reader_pending.lock().await.remove(&outbound_id) {
                        let result = if accepted {
                            Ok(())
                        } else {
                            Err(TransportError::Send(
                                reason.unwrap_or_else(|| "network rejected frame".to_owned()),
                            ))
                        };
                        let _ = sender.send(result);
                    }
                }
            }
        }
        anyhow::Ok(())
    });

    output_tx
        .send(WorkerMessage::Ready {
            protocol_version: WORKER_PROTOCOL_VERSION,
            node_id,
            pid: std::process::id(),
        })
        .await?;

    while let Some(command) = command_rx.recv().await {
        let operation_id = command.operation_id;
        let is_shutdown = matches!(command.action, WorkerAction::Shutdown);
        let result = execute_command(
            node_id,
            &storage_root,
            &manager,
            &backend,
            &audits,
            command.action,
        )
        .await;
        output_tx
            .send(WorkerMessage::Response {
                operation_id,
                result,
            })
            .await?;
        if is_shutdown {
            break;
        }
    }

    // Release every output sender owned by the manager transport before
    // waiting for the stdout writer to observe channel closure.
    drop(manager);
    drop(backend);
    drop(output_tx);
    reader.abort();
    let _ = reader.await;
    writer.await??;
    Ok(())
}

async fn execute_command(
    node_id: u64,
    storage_root: &Path,
    manager: &Arc<Manager>,
    backend: &Arc<IpcBackend>,
    audits: &Arc<Mutex<BTreeMap<u64, AuditHandle>>>,
    action: WorkerAction,
) -> Result<WorkerResult, WorkerFailure> {
    match action {
        WorkerAction::CreateGroup { group_id } => {
            let audit = AuditHandle::default();
            manager
                .create_group(
                    GroupId(group_id),
                    BTreeSet::from([node_id]),
                    AuditedStateMachine {
                        audit: audit.clone(),
                    },
                )
                .await
                .map_err(|error| failure("group_create_failed", error, Some(group_id), None))?;
            audits.lock().unwrap().insert(group_id, audit);
            Ok(WorkerResult::Created { group_id })
        }
        WorkerAction::Propose { group_id, command } => {
            let group = manager
                .get_group(GroupId(group_id))
                .ok_or_else(|| failure("unknown_group", "group is absent", Some(group_id), None))?;
            let mut last_error = None;
            for _ in 0..100 {
                match group.propose(command.clone()).await {
                    Ok(response) => {
                        return Ok(WorkerResult::Proposed { group_id, response });
                    }
                    Err(error) => {
                        last_error = Some(error);
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
            }
            Err(failure(
                "proposal_failed",
                last_error.expect("proposal loop records an error"),
                Some(group_id),
                None,
            ))
        }
        WorkerAction::EmitRaftVote {
            target,
            group_id,
            correlation_id,
            term,
        } => {
            let frame = ChirpsRaftTransport::encode_group_frame(RaftFramePayload {
                correlation_id,
                message: RaftMessage::Vote {
                    group_id: GroupId(group_id),
                    request: VoteRequest {
                        vote: Vote::new(term, node_id),
                        last_log_id: None,
                    },
                },
            })
            .map_err(|error| failure("raft_encode_failed", error, Some(group_id), None))?;
            backend
                .send(target, frame)
                .await
                .map_err(|error| failure("network_enqueue_failed", error, Some(group_id), None))?;
            Ok(WorkerResult::Emitted { correlation_id })
        }
        WorkerAction::DeliverFrame {
            network_sequence,
            source,
            frame,
        } => {
            let routed = manager
                .route_frame(source, wire_node_id(node_id), *frame)
                .await
                .map_err(|error| {
                    let code = if matches!(error, MultiRaftError::UnknownGroup { .. }) {
                        "unknown_group"
                    } else {
                        "raft_route_rejected"
                    };
                    failure(code, error, None, Some(network_sequence))
                })?;
            Ok(WorkerResult::FrameAccepted {
                network_sequence,
                route: RouteObservation {
                    group_id: routed.message.group_id().0,
                    correlation_id: routed.correlation_id,
                    response_kind: raft_message_kind(&routed.message).to_owned(),
                },
            })
        }
        WorkerAction::TickRaft => {
            let results = manager.tick_all().await;
            if let Some(failed) = results.iter().find(|result| result.result.is_err()) {
                return Err(failure(
                    "raft_tick_failed",
                    failed.result.as_ref().unwrap_err(),
                    Some(failed.group_id.0),
                    None,
                ));
            }
            Ok(WorkerResult::Ticked {
                groups: results.iter().map(|result| result.group_id.0).collect(),
            })
        }
        WorkerAction::RemoveGroup { group_id } => {
            let existed = manager
                .remove_group(GroupId(group_id))
                .await
                .map_err(|error| failure("group_remove_failed", error, Some(group_id), None))?;
            Ok(WorkerResult::Removed { group_id, existed })
        }
        WorkerAction::Observe => Ok(WorkerResult::Observation {
            value: observe(node_id, storage_root, manager, audits),
        }),
        WorkerAction::Shutdown => {
            manager
                .shutdown_all()
                .await
                .map_err(|error| failure("worker_shutdown_failed", error, None, None))?;
            backend
                .close()
                .await
                .map_err(|error| failure("worker_shutdown_failed", error, None, None))?;
            Ok(WorkerResult::Shutdown)
        }
    }
}

fn observe(
    node_id: u64,
    storage_root: &std::path::Path,
    manager: &Manager,
    audits: &Arc<Mutex<BTreeMap<u64, AuditHandle>>>,
) -> WorkerObservation {
    let audit_guard = audits.lock().unwrap();
    let groups = manager
        .list_groups()
        .into_iter()
        .filter_map(|group_id| {
            let handle = manager.get_group(group_id)?;
            let audit = audit_guard.get(&group_id.0)?.0.lock().unwrap();
            let namespace = group_namespace(group_id);
            let wal_relative = std::path::Path::new("wal").join(&namespace);
            let snapshot_relative = std::path::Path::new("snapshot").join(&namespace);
            Some((
                group_id.0.to_string(),
                GroupObservation {
                    group_id: group_id.0,
                    namespace,
                    accepting: handle.is_accepting(),
                    state_machine_applies: audit.applies,
                    state_machine_digest: audit
                        .digest
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect(),
                    wal_exists: storage_root.join(&wal_relative).is_dir(),
                    snapshot_exists: storage_root.join(&snapshot_relative).is_dir(),
                    wal_path: wal_relative.to_string_lossy().into_owned(),
                    snapshot_path: snapshot_relative.to_string_lossy().into_owned(),
                },
            ))
        })
        .collect();
    WorkerObservation {
        node_id,
        active_groups: manager.list_groups().iter().map(|group| group.0).collect(),
        groups,
    }
}

fn failure(
    code: &str,
    error: impl std::fmt::Display,
    group_id: Option<u64>,
    network_sequence: Option<u64>,
) -> WorkerFailure {
    WorkerFailure {
        code: code.to_owned(),
        detail: error.to_string(),
        group_id,
        network_sequence,
    }
}

fn wire_node_id(node_id: u64) -> NodeId {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&node_id.to_be_bytes());
    NodeId::from(bytes)
}

fn raft_message_kind(message: &RaftMessage) -> &'static str {
    match message {
        RaftMessage::AppendEntries { .. } => "append_entries",
        RaftMessage::AppendEntriesResponse { .. } => "append_entries_response",
        RaftMessage::Vote { .. } => "vote",
        RaftMessage::VoteResponse { .. } => "vote_response",
        RaftMessage::InstallSnapshot { .. } => "install_snapshot",
        RaftMessage::InstallSnapshotResponse { .. } => "install_snapshot_response",
    }
}

#[allow(dead_code)]
fn deterministic_addr(node_id: u64) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, node_id as u16))
}
