#![cfg(feature = "snapshot")]

use alopex_chirps::snapshot::{
    DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_THRESHOLD, DEFAULT_MAX_CONCURRENT_CHUNKS,
    DEFAULT_MAX_RETRIES, SnapshotChunk, SnapshotChunkSink, SnapshotManifest, SnapshotProgress,
    SnapshotProgressObserver, SnapshotReceiver, SnapshotSender, SnapshotTransferConfig,
    SnapshotTransferError, SnapshotTransferReceipt,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;

#[test]
fn locked_snapshot_transfer_defaults_match_issue_22() {
    let config = SnapshotTransferConfig::default();
    assert_eq!(config.chunk_threshold, 10 * 1024 * 1024);
    assert_eq!(config.chunk_size, 1024 * 1024);
    assert_eq!(config.max_concurrent_chunks, 4);
    assert_eq!(config.max_retries, 3);
    assert_eq!(config.chunk_threshold, DEFAULT_CHUNK_THRESHOLD);
    assert_eq!(config.chunk_size, DEFAULT_CHUNK_SIZE);
    assert_eq!(config.max_concurrent_chunks, DEFAULT_MAX_CONCURRENT_CHUNKS);
    assert_eq!(config.max_retries, DEFAULT_MAX_RETRIES);
}

#[derive(Default)]
struct ProgressLog(Mutex<Vec<SnapshotProgress>>);

impl SnapshotProgressObserver for ProgressLog {
    fn observe(&self, progress: SnapshotProgress) {
        self.0.lock().unwrap().push(progress);
    }
}

struct HarnessSink {
    receiver: tokio::sync::Mutex<Option<SnapshotReceiver>>,
    attempts: Mutex<BTreeMap<u32, usize>>,
    fail_once: Option<u32>,
    active: AtomicUsize,
    max_active: AtomicUsize,
    wait_for_parallel: bool,
    durable: AtomicBool,
    visible: AtomicBool,
    progress: Arc<ProgressLog>,
    completions: AtomicUsize,
}

impl HarnessSink {
    fn new(fail_once: Option<u32>, wait_for_parallel: bool) -> Arc<Self> {
        Arc::new(Self {
            receiver: tokio::sync::Mutex::new(None),
            attempts: Mutex::new(BTreeMap::new()),
            fail_once,
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            wait_for_parallel,
            durable: AtomicBool::new(false),
            visible: AtomicBool::new(false),
            progress: Arc::new(ProgressLog::default()),
            completions: AtomicUsize::new(0),
        })
    }

    fn attempts(&self, index: u32) -> usize {
        self.attempts
            .lock()
            .unwrap()
            .get(&index)
            .copied()
            .unwrap_or_default()
    }
}

#[async_trait]
impl SnapshotChunkSink for HarnessSink {
    async fn begin(&self, manifest: SnapshotManifest) -> Result<(), SnapshotTransferError> {
        *self.receiver.lock().await = Some(SnapshotReceiver::new(manifest, self.progress.clone())?);
        Ok(())
    }

    async fn send_chunk(&self, chunk: SnapshotChunk) -> Result<(), SnapshotTransferError> {
        let attempt = {
            let mut attempts = self.attempts.lock().unwrap();
            let attempt = attempts.entry(chunk.index).or_default();
            *attempt += 1;
            *attempt
        };
        if self.fail_once == Some(chunk.index) && attempt == 1 {
            return Err(SnapshotTransferError::retryable("injected chunk fault"));
        }

        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        if self.wait_for_parallel {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        self.receiver.lock().await.as_mut().unwrap().accept(chunk)?;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    async fn finish(
        &self,
        snapshot_id: &str,
    ) -> Result<SnapshotTransferReceipt, SnapshotTransferError> {
        let receiver = self.receiver.lock().await.take().unwrap();
        let verified = receiver.into_verified(snapshot_id)?;
        self.durable.store(true, Ordering::SeqCst);
        assert!(self.durable.load(Ordering::SeqCst));
        self.visible.store(true, Ordering::SeqCst);
        self.completions.fetch_add(1, Ordering::SeqCst);
        Ok(SnapshotTransferReceipt::installed(
            verified.bytes.len() as u64
        ))
    }

    async fn abort(&self, _snapshot_id: &str) {
        *self.receiver.lock().await = None;
    }
}

fn config() -> SnapshotTransferConfig {
    SnapshotTransferConfig {
        chunk_threshold: 4,
        chunk_size: 4,
        max_concurrent_chunks: 2,
        max_retries: 1,
        transfer_timeout: Duration::from_secs(60),
    }
}

#[tokio::test]
async fn failed_chunk_is_retried_without_resending_verified_chunks() {
    let sink = HarnessSink::new(Some(1), false);
    let sender = SnapshotSender::new(config(), Arc::new(ProgressLog::default())).unwrap();

    sender
        .transfer("retry-one", b"abcdefghijkl".to_vec(), sink.clone())
        .await
        .unwrap();

    assert_eq!(sink.attempts(0), 1);
    assert_eq!(sink.attempts(1), 2);
    assert_eq!(sink.attempts(2), 1);
}

#[tokio::test]
async fn sender_never_exceeds_configured_parallelism() {
    let sink = HarnessSink::new(None, true);
    let sender = SnapshotSender::new(config(), Arc::new(ProgressLog::default())).unwrap();

    sender
        .transfer("bounded", b"abcdefghijkl".to_vec(), sink.clone())
        .await
        .unwrap();

    let observed = sink.max_active.load(Ordering::SeqCst);
    assert!(observed > 1, "work should overlap while chunks remain");
    assert!(observed <= 2, "configured concurrency must be respected");
}

#[tokio::test]
async fn receiver_installs_only_after_all_chunks_verify_and_checkpoint() {
    let sink = HarnessSink::new(None, false);
    let sender = SnapshotSender::new(config(), Arc::new(ProgressLog::default())).unwrap();

    sender
        .transfer("durable", b"abcdefghijkl".to_vec(), sink.clone())
        .await
        .unwrap();

    assert!(sink.durable.load(Ordering::SeqCst));
    assert!(sink.visible.load(Ordering::SeqCst));
    let progress = sink.progress.0.lock().unwrap();
    assert!(
        progress
            .windows(2)
            .all(|w| w[0].verified_chunks <= w[1].verified_chunks)
    );
    assert_eq!(progress.last().unwrap().percent(), 100);
    assert_eq!(sink.completions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_transfer_never_exposes_partial_snapshot() {
    let mut cfg = config();
    cfg.max_retries = 0;
    let sink = HarnessSink::new(Some(1), false);
    let sender = SnapshotSender::new(cfg, Arc::new(ProgressLog::default())).unwrap();

    let error = sender
        .transfer("terminal", b"abcdefghijkl".to_vec(), sink.clone())
        .await
        .unwrap_err();

    assert!(error.is_retryable());
    assert!(!sink.durable.load(Ordering::SeqCst));
    assert!(!sink.visible.load(Ordering::SeqCst));
    assert_eq!(sink.completions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn transfer_timeout_aborts_without_exposing_partial_snapshot() {
    let sink = HarnessSink::new(None, true);
    let mut cfg = config();
    cfg.transfer_timeout = Duration::from_millis(1);
    let sender = SnapshotSender::new(cfg, Arc::new(ProgressLog::default())).unwrap();

    let error = sender
        .transfer("timeout", b"abcdefghijkl".to_vec(), sink.clone())
        .await
        .unwrap_err();

    assert!(matches!(error, SnapshotTransferError::Timeout));
    assert!(!sink.durable.load(Ordering::SeqCst));
    assert!(!sink.visible.load(Ordering::SeqCst));
}

#[derive(Clone, Default)]
struct RestoredState(Arc<tokio::sync::Mutex<Vec<u8>>>);

#[async_trait]
impl alopex_chirps_raft_storage::traits::StateMachine for RestoredState {
    type Command = Vec<u8>;
    type Response = Vec<u8>;

    async fn apply(
        &mut self,
        _log_id: alopex_chirps_raft_storage::types::LogId<u64>,
        command: Self::Command,
    ) -> alopex_chirps_raft_storage::traits::StateMachineResult<Self::Response> {
        Ok(command)
    }

    async fn snapshot(
        &self,
    ) -> alopex_chirps_raft_storage::traits::StateMachineResult<
        Box<dyn alopex_chirps_raft_storage::traits::AsyncSnapshotData>,
    > {
        Ok(Box::new(Cursor::new(self.0.lock().await.clone())))
    }

    async fn restore(
        &mut self,
        mut snapshot: Box<dyn alopex_chirps_raft_storage::traits::AsyncSnapshotData>,
    ) -> alopex_chirps_raft_storage::traits::StateMachineResult<()> {
        let mut bytes = Vec::new();
        snapshot.read_to_end(&mut bytes).await?;
        *self.0.lock().await = bytes;
        Ok(())
    }
}

#[derive(Default)]
struct StorageCompletionLog(Mutex<Vec<alopex_chirps_raft_storage::SnapshotCompletionEvent>>);

impl alopex_chirps_raft_storage::SnapshotCompletionHook for StorageCompletionLog {
    fn completed(&self, event: alopex_chirps_raft_storage::SnapshotCompletionEvent) {
        self.0.lock().unwrap().push(event);
    }
}

fn wire_node_id(node_id: u64) -> alopex_chirps_wire::node_id::NodeId {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&node_id.to_be_bytes());
    alopex_chirps_wire::node_id::NodeId::from(bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_transport_and_wal_storage_install_verified_snapshot() {
    use alopex_chirps::multi_raft::{GroupId, MultiRaftManager, WalRaftStorageFactory};
    use alopex_chirps::raft::{ChirpsRaftTransport, RaftConfig, RaftFramePayload};
    use alopex_chirps_core::backend::MessageBackend;
    use alopex_chirps_mock::{MockBackend, MockNetwork};
    use alopex_chirps_raft_storage::types::{
        BasicNode, LogId, Snapshot, SnapshotMeta, StoredMembership, Vote,
    };
    use alopex_chirps_raft_storage::wal_storage::WalStorageConfig;
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};

    let network = MockNetwork::new();
    let sender_backend = network
        .add_node(wire_node_id(1), MockBackend::ephemeral_addr())
        .await;
    let receiver_backend = network
        .add_node(wire_node_id(2), MockBackend::ephemeral_addr())
        .await;
    let sender_backend: Arc<dyn MessageBackend> = Arc::new(sender_backend);
    let receiver_backend: Arc<dyn MessageBackend> = Arc::new(receiver_backend);
    let group_id = GroupId(22);
    let sender_transport = Arc::new(ChirpsRaftTransport::new(
        Arc::clone(&sender_backend),
        group_id,
        1,
    ));
    sender_transport
        .configure_snapshot_transfer(config())
        .unwrap();
    let sender_progress = Arc::new(ProgressLog::default());
    sender_transport.set_snapshot_progress_observer(sender_progress.clone());

    let receiver_root = tempfile::tempdir().unwrap();
    let storage_completion = Arc::new(StorageCompletionLog::default());
    let storage_factory = Arc::new(
        WalRaftStorageFactory::new(
            WalStorageConfig {
                wal_dir: receiver_root.path().join("wal"),
                snapshot_dir: receiver_root.path().join("snapshot"),
                ..WalStorageConfig::default()
            },
            2,
        )
        .with_snapshot_completion_hook(storage_completion.clone()),
    );
    let receiver_transport = Arc::new(ChirpsRaftTransport::new(
        Arc::clone(&receiver_backend),
        GroupId(0),
        2,
    ));
    let manager = Arc::new(MultiRaftManager::new(
        receiver_transport,
        storage_factory,
        RaftConfig {
            node_id: 2,
            snapshot_chunk_threshold: 4,
            snapshot_chunk_size: 4,
            snapshot_max_concurrent_chunks: 2,
            snapshot_max_retries: 1,
            ..RaftConfig::default()
        },
    ));
    let restored = RestoredState::default();
    manager
        .create_group_uninitialized(group_id, restored.clone())
        .await
        .unwrap();

    let mut receiver_incoming = receiver_backend.subscribe().await.unwrap();
    let receiver_loop = {
        let manager = Arc::clone(&manager);
        let receiver_backend = Arc::clone(&receiver_backend);
        tokio::spawn(async move {
            while let Some((source, frame)) = receiver_incoming.recv().await {
                let Ok(Some(response)) =
                    manager.dispatch_frame(source, wire_node_id(2), frame).await
                else {
                    continue;
                };
                let frame = ChirpsRaftTransport::encode_group_frame(RaftFramePayload {
                    correlation_id: response.correlation_id,
                    message: response.message,
                })
                .unwrap();
                receiver_backend
                    .send(wire_node_id(response.destination), frame)
                    .await
                    .unwrap();
            }
        })
    };
    let mut sender_incoming = sender_backend.subscribe().await.unwrap();
    let sender_loop = {
        let sender_transport = Arc::clone(&sender_transport);
        tokio::spawn(async move {
            while let Some((source, frame)) = sender_incoming.recv().await {
                let payload = ChirpsRaftTransport::decode_frame(frame).unwrap();
                sender_transport
                    .consume_incoming_from(2, payload)
                    .await
                    .unwrap();
                assert_eq!(source, wire_node_id(2));
            }
        })
    };

    let mut network_factory = ChirpsRaftTransport::factory(Arc::clone(&sender_transport));
    let mut client = network_factory
        .new_client(
            2,
            &BasicNode {
                addr: "mock://node-2".into(),
            },
        )
        .await;
    let bytes = b"abcdefghijkl".to_vec();
    let vote = Vote::new_committed(1, 1);
    let response = client
        .full_snapshot(
            vote,
            Snapshot {
                meta: SnapshotMeta {
                    last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 7)),
                    last_membership: StoredMembership::default(),
                    snapshot_id: "actual-raft-snapshot".into(),
                },
                snapshot: Box::new(Cursor::new(bytes.clone())),
            },
            futures::future::pending(),
            RPCOption::new(Duration::from_secs(5)),
        )
        .await
        .unwrap();

    assert_eq!(response.vote, vote);
    assert_eq!(*restored.0.lock().await, bytes);
    let completions = storage_completion.0.lock().unwrap();
    assert_eq!(completions.len(), 1);
    assert_eq!(
        completions[0].kind,
        alopex_chirps_raft_storage::SnapshotCompletionKind::Installed
    );
    assert_eq!(completions[0].size_bytes, 12);
    let progress = sender_progress.0.lock().unwrap();
    assert!(progress.last().unwrap().installed);
    assert_eq!(progress.last().unwrap().percent(), 100);
    assert!(
        progress
            .iter()
            .all(|sample| sample.max_observed_concurrency <= 2)
    );

    receiver_loop.abort();
    sender_loop.abort();
}
