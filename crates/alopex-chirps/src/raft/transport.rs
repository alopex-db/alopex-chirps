use crate::backend::MessageBackend;
use crate::raft::metrics::RaftMetricsCollector;
use crate::raft::{RaftError, RaftMessage};
use alopex_chirps_core::error::TransportError;
use alopex_chirps_raft_storage::types::{
    AppendEntriesRequest, AppendEntriesResponse, BasicNode, ChirpsNodeId, ChirpsTypeConfig,
    GroupId, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use alopex_chirps_wire::frame::{Frame, RaftFrame};
use alopex_chirps_wire::node_id::NodeId;
use openraft::error::{Infallible, InstallSnapshotError, NetworkError, RPCError, Timeout};
use openraft::network::{RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[cfg(feature = "snapshot")]
use std::future::Future;
#[cfg(feature = "snapshot")]
use std::sync::RwLock;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::sync::oneshot;
use tokio::time;

#[cfg(feature = "snapshot")]
use crate::snapshot::{
    NoopSnapshotProgressObserver, RaftSnapshotBegin, RaftSnapshotRequest, RaftSnapshotResponse,
    RaftSnapshotStatus, SnapshotChunk, SnapshotChunkSink, SnapshotProgressObserver, SnapshotSender,
    SnapshotTransferConfig, SnapshotTransferError, SnapshotTransferReceipt,
};
#[cfg(feature = "snapshot")]
use async_trait::async_trait;
#[cfg(feature = "snapshot")]
use openraft::error::{Fatal, ReplicationClosed, StreamingError};
#[cfg(feature = "snapshot")]
use openraft::raft::SnapshotResponse;
#[cfg(feature = "snapshot")]
use openraft::{Snapshot, Vote};
#[cfg(feature = "snapshot")]
use tokio::io::AsyncReadExt;

/// Raft RPCメッセージをフレーム化する際のコンテナ。レスポンス対応のため相関IDを保持する。
///
/// # 例
///
/// ```rust,ignore
/// use alopex_chirps::raft::transport::RaftFramePayload;
/// use alopex_chirps::raft::RaftMessage;
/// use alopex_chirps_raft_storage::types::GroupId;
///
/// let payload = RaftFramePayload {
///     correlation_id: 42,
///     message: RaftMessage::VoteResponse { group_id: GroupId(1), response: todo!() },
/// };
/// assert_eq!(payload.correlation_id, 42);
/// ```
#[derive(Debug, Serialize, Deserialize)]
pub struct RaftFramePayload {
    pub correlation_id: u64,
    pub message: RaftMessage,
}

struct PendingRpc {
    target: ChirpsNodeId,
    group_id: GroupId,
    rpc_type: RPCTypes,
    response: oneshot::Sender<RaftMessage>,
}

struct PendingRegistration {
    correlation_id: u64,
    pending: Arc<Mutex<HashMap<u64, PendingRpc>>>,
}

impl Drop for PendingRegistration {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.correlation_id);
    }
}

/// Raftネットワークのファクトリ。openraftのRaftNetworkFactoryを実装する。
///
/// # 例
///
/// ```rust,ignore
/// use alopex_chirps::raft::transport::ChirpsRaftTransport;
/// use alopex_chirps_raft_storage::types::GroupId;
///
/// let transport = ChirpsRaftTransport::new(my_backend(), GroupId(1), 1);
/// let factory = ChirpsRaftTransport::factory(transport);
/// ```
pub struct ChirpsRaftTransport {
    backend: Arc<dyn MessageBackend>,
    group_id: GroupId,
    node_id: ChirpsNodeId,
    next_corr: AtomicU64,
    accepting_rpcs: AtomicBool,
    pending: Arc<Mutex<HashMap<u64, PendingRpc>>>,
    metrics_collector: Mutex<Option<Arc<RaftMetricsCollector>>>,
    #[cfg(feature = "snapshot")]
    snapshot_config: RwLock<SnapshotTransferConfig>,
    #[cfg(feature = "snapshot")]
    snapshot_progress: RwLock<Arc<dyn SnapshotProgressObserver>>,
}

impl ChirpsRaftTransport {
    /// 新しいトランスポートを作成する。
    pub fn new(backend: Arc<dyn MessageBackend>, group_id: GroupId, node_id: ChirpsNodeId) -> Self {
        Self {
            backend,
            group_id,
            node_id,
            next_corr: AtomicU64::new(1),
            accepting_rpcs: AtomicBool::new(true),
            pending: Arc::new(Mutex::new(HashMap::new())),
            metrics_collector: Mutex::new(None),
            #[cfg(feature = "snapshot")]
            snapshot_config: RwLock::new(SnapshotTransferConfig::default()),
            #[cfg(feature = "snapshot")]
            snapshot_progress: RwLock::new(Arc::new(NoopSnapshotProgressObserver)),
        }
    }

    /// Creates an isolated per-group transport over the same backend.
    ///
    /// Correlation IDs and pending RPC state are deliberately not shared
    /// between groups.
    pub fn fork_for_group(&self, group_id: GroupId) -> Self {
        let fork = Self::new(Arc::clone(&self.backend), group_id, self.node_id);
        if let Some(collector) = self.metrics_collector() {
            fork.set_metrics_collector(collector);
        }
        #[cfg(feature = "snapshot")]
        {
            *fork
                .snapshot_config
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = *self
                .snapshot_config
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *fork
                .snapshot_progress
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = self
                .snapshot_progress
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
        }
        fork
    }

    pub fn node_id(&self) -> ChirpsNodeId {
        self.node_id
    }

    pub(crate) fn set_metrics_collector(&self, collector: Arc<RaftMetricsCollector>) {
        *self
            .metrics_collector
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(collector);
    }

    fn metrics_collector(&self) -> Option<Arc<RaftMetricsCollector>> {
        self.metrics_collector
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[cfg(feature = "snapshot")]
    pub fn configure_snapshot_transfer(
        &self,
        config: SnapshotTransferConfig,
    ) -> Result<(), SnapshotTransferError> {
        *self
            .snapshot_config
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = config.validate()?;
        Ok(())
    }

    #[cfg(feature = "snapshot")]
    pub fn set_snapshot_progress_observer(&self, observer: Arc<dyn SnapshotProgressObserver>) {
        *self
            .snapshot_progress
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = observer;
    }

    #[cfg(feature = "snapshot")]
    pub(crate) fn snapshot_progress_observer(&self) -> Arc<dyn SnapshotProgressObserver> {
        self.snapshot_progress
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn record_sent(&self, message_type: &'static str) {
        if let Some(collector) = self.metrics_collector() {
            collector.record_raft_message_sent(self.group_id, message_type, 1);
        }
    }

    pub(crate) fn record_received(&self, message_type: &'static str) {
        if let Some(collector) = self.metrics_collector() {
            collector.record_raft_message_received(self.group_id, message_type, 1);
        }
    }

    /// RaftNetworkFactoryをwrapした型を取得する。
    pub fn factory(inner: Arc<ChirpsRaftTransport>) -> ChirpsRaftNetworkFactory {
        ChirpsRaftNetworkFactory { inner }
    }

    /// Raftフレームをデコードし、外側とpayloadのgroup一致も検証する。
    pub fn decode_frame(frame: Frame) -> Option<RaftFramePayload> {
        let data = match frame {
            Frame::Raft(data) | Frame::RaftSnapshot(data) => data,
            _ => return None,
        };
        let payload = bincode::deserialize::<RaftFramePayload>(&data.payload).ok()?;
        if payload.message.group_id().0 != data.group_id {
            return None;
        }
        Some(payload)
    }

    /// Encodes a group-consistent Raft payload for an external receive-loop or
    /// deterministic transport harness.
    pub fn encode_group_frame(payload: RaftFramePayload) -> Result<Frame, bincode::Error> {
        let group_id = payload.message.group_id();
        let bytes = bincode::serialize(&payload)?;
        let raft_frame = RaftFrame {
            group_id: group_id.0,
            payload: bytes,
        };
        Ok(match payload.message {
            RaftMessage::InstallSnapshot { .. } | RaftMessage::InstallSnapshotResponse { .. } => {
                Frame::RaftSnapshot(raft_frame)
            }
            #[cfg(feature = "snapshot")]
            RaftMessage::SnapshotTransfer { .. } | RaftMessage::SnapshotTransferResponse { .. } => {
                Frame::RaftSnapshot(raft_frame)
            }
            _ => Frame::Raft(raft_frame),
        })
    }

    /// レスポンス用に送信待ちマップを確認する。マッチした場合は待ち受けに届け、リクエストの場合はSomeを返す。
    pub async fn consume_incoming(&self, payload: RaftFramePayload) -> Option<RaftFramePayload> {
        if payload.message.group_id() != self.group_id {
            return None;
        }
        self.record_received(payload.message.metric_type());
        let mut guard = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if is_response(&payload.message)
            && let Some(pending) = guard.remove(&payload.correlation_id)
        {
            let _ = pending.response.send(payload.message);
            None
        } else {
            Some(payload)
        }
    }

    /// Validates source, group, correlation and response shape before waking an RPC waiter.
    pub async fn consume_incoming_from(
        &self,
        source: ChirpsNodeId,
        payload: RaftFramePayload,
    ) -> Result<Option<RaftFramePayload>, TransportError> {
        let group_id = payload.message.group_id();
        if group_id != self.group_id {
            return Err(TransportError::Connection(format!(
                "Raft group mismatch: expected {}, got {}",
                self.group_id.0, group_id.0
            )));
        }
        self.record_received(payload.message.metric_type());
        if !is_response(&payload.message) {
            return Ok(Some(payload));
        }

        let mut guard = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(pending) = guard.get(&payload.correlation_id) else {
            return Err(TransportError::Connection(format!(
                "unknown Raft response correlation {}",
                payload.correlation_id
            )));
        };
        if pending.target != source || pending.group_id != group_id {
            return Err(TransportError::Connection(format!(
                "Raft response route mismatch for correlation {}",
                payload.correlation_id
            )));
        }
        if !response_matches(pending.rpc_type, &payload.message) {
            return Err(TransportError::Connection(format!(
                "Raft response type mismatch for correlation {}",
                payload.correlation_id
            )));
        }
        let pending = guard
            .remove(&payload.correlation_id)
            .expect("validated pending response must remain present");
        let _ = pending.response.send(payload.message);
        Ok(None)
    }

    /// 受信サイドからレスポンスを送る際に使用する。
    pub async fn send_response(
        &self,
        target: ChirpsNodeId,
        correlation_id: u64,
        message: RaftMessage,
    ) -> Result<(), TransportError> {
        let message_type = message.metric_type();
        let frame = self
            .encode_frame(RaftFramePayload {
                correlation_id,
                message,
            })
            .map_err(|e| TransportError::Send(e.to_string()))?;
        let target = encode_node_id(target);
        let result = self.backend.send(target, frame).await;
        if result.is_ok() {
            self.record_sent(message_type);
        }
        result
    }

    /// Stops admission of new outbound RPCs while keeping already pending
    /// responses deliverable during lifecycle drain.
    #[cfg(feature = "multi-raft")]
    pub(crate) fn close_rpc_admission(&self) {
        self.accepting_rpcs.store(false, Ordering::Release);
    }

    /// Cancels any residual RPCs after lifecycle-held operations have drained.
    #[cfg(feature = "multi-raft")]
    pub(crate) fn cancel_pending_rpcs(&self) {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// 内部でRPCを送信しレスポンスを待つ共通処理。
    async fn send_rpc<E>(
        &self,
        target: ChirpsNodeId,
        rpc_type: RPCTypes,
        message: RaftMessage,
        option: RPCOption,
    ) -> Result<
        RaftMessage,
        RPCError<ChirpsNodeId, BasicNode, openraft::error::RaftError<ChirpsNodeId, E>>,
    >
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        if !self.accepting_rpcs.load(Ordering::Acquire) {
            return Err(RPCError::Network(NetworkError::new(&RaftError::Internal(
                anyhow::anyhow!("Raft transport is shutting down"),
            ))));
        }
        let message_type = message.metric_type();
        let correlation_id = self.next_corr.fetch_add(1, Ordering::Relaxed);
        let frame = self
            .encode_frame(RaftFramePayload {
                correlation_id,
                message,
            })
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !self.accepting_rpcs.load(Ordering::Acquire) {
                return Err(RPCError::Network(NetworkError::new(&RaftError::Internal(
                    anyhow::anyhow!("Raft transport is shutting down"),
                ))));
            }
            guard.insert(
                correlation_id,
                PendingRpc {
                    target,
                    group_id: self.group_id,
                    rpc_type,
                    response: tx,
                },
            );
        }
        let _registration = PendingRegistration {
            correlation_id,
            pending: Arc::clone(&self.pending),
        };

        let send_res = self
            .backend
            .send(encode_node_id(target), frame)
            .await
            .map_err(|e| {
                map_transport_error::<E>(rpc_type, self.node_id, target, option.hard_ttl(), e)
            });

        send_res?;
        self.record_sent(message_type);

        match time::timeout(option.hard_ttl(), rx).await {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(_canceled)) => Err(RPCError::Network(NetworkError::new(&RaftError::Internal(
                anyhow::anyhow!("response channel closed"),
            )))),
            Err(_) => Err(RPCError::Timeout(Timeout {
                action: rpc_type,
                id: self.node_id,
                target,
                timeout: option.hard_ttl(),
            })),
        }
    }

    fn encode_frame(&self, payload: RaftFramePayload) -> Result<Frame, bincode::Error> {
        if payload.message.group_id() != self.group_id {
            return Err(Box::new(bincode::ErrorKind::Custom(format!(
                "Raft group mismatch: expected {}, got {}",
                self.group_id.0,
                payload.message.group_id().0
            ))));
        }
        Self::encode_group_frame(payload)
    }
}

fn is_response(message: &RaftMessage) -> bool {
    let standard = matches!(
        message,
        RaftMessage::AppendEntriesResponse { .. }
            | RaftMessage::VoteResponse { .. }
            | RaftMessage::InstallSnapshotResponse { .. }
    );
    #[cfg(feature = "snapshot")]
    return standard || matches!(message, RaftMessage::SnapshotTransferResponse { .. });
    #[cfg(not(feature = "snapshot"))]
    standard
}

fn response_matches(rpc_type: RPCTypes, message: &RaftMessage) -> bool {
    let standard = matches!(
        (rpc_type, message),
        (RPCTypes::Vote, RaftMessage::VoteResponse { .. })
            | (
                RPCTypes::AppendEntries,
                RaftMessage::AppendEntriesResponse { .. }
            )
            | (
                RPCTypes::InstallSnapshot,
                RaftMessage::InstallSnapshotResponse { .. }
            )
    );
    #[cfg(feature = "snapshot")]
    return standard
        || matches!(
            (rpc_type, message),
            (
                RPCTypes::InstallSnapshot,
                RaftMessage::SnapshotTransferResponse { .. }
            )
        );
    #[cfg(not(feature = "snapshot"))]
    standard
}

/// RaftNetworkFactory実装用のラッパー。
#[derive(Clone)]
pub struct ChirpsRaftNetworkFactory {
    pub(crate) inner: Arc<ChirpsRaftTransport>,
}

/// RaftNetworkの実体。targetごとに生成され、send_rpcを委譲する。
pub struct ChirpsRaftNetworkClient {
    inner: Arc<ChirpsRaftTransport>,
    target: ChirpsNodeId,
}

#[cfg(feature = "snapshot")]
struct RaftSnapshotSink {
    inner: Arc<ChirpsRaftTransport>,
    target: ChirpsNodeId,
    option: RPCOption,
    vote: Mutex<Vote<ChirpsNodeId>>,
    meta: openraft::SnapshotMeta<ChirpsNodeId, BasicNode>,
}

#[cfg(feature = "snapshot")]
impl RaftSnapshotSink {
    async fn request(
        &self,
        request: RaftSnapshotRequest,
    ) -> Result<RaftSnapshotResponse, SnapshotTransferError> {
        let message = RaftMessage::SnapshotTransfer {
            group_id: self.inner.group_id,
            request,
        };
        let response = self
            .inner
            .send_rpc::<InstallSnapshotError>(
                self.target,
                RPCTypes::InstallSnapshot,
                message,
                self.option.clone(),
            )
            .await
            .map_err(|error| SnapshotTransferError::retryable(error.to_string()))?;
        let RaftMessage::SnapshotTransferResponse { response, .. } = response else {
            return Err(SnapshotTransferError::terminal(
                "unexpected response to snapshot transfer request",
            ));
        };
        *self
            .vote
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = response.vote;
        Ok(response)
    }

    fn response_vote(&self) -> Vote<ChirpsNodeId> {
        *self
            .vote
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(feature = "snapshot")]
#[async_trait]
impl SnapshotChunkSink for RaftSnapshotSink {
    async fn begin(
        &self,
        manifest: crate::snapshot::SnapshotManifest,
    ) -> Result<(), SnapshotTransferError> {
        let snapshot_id = manifest.snapshot_id.clone();
        let response = self
            .request(RaftSnapshotRequest::Begin(RaftSnapshotBegin {
                vote: self.response_vote(),
                meta: self.meta.clone(),
                manifest,
            }))
            .await?;
        if response.snapshot_id != snapshot_id {
            return Err(SnapshotTransferError::terminal(
                "snapshot response id mismatch",
            ));
        }
        match response.status {
            RaftSnapshotStatus::Accepted => Ok(()),
            RaftSnapshotStatus::Rejected { reason } => Err(SnapshotTransferError::terminal(reason)),
            status => Err(SnapshotTransferError::terminal(format!(
                "unexpected snapshot begin response: {status:?}"
            ))),
        }
    }

    async fn send_chunk(&self, chunk: SnapshotChunk) -> Result<(), SnapshotTransferError> {
        let snapshot_id = chunk.snapshot_id.clone();
        let index = chunk.index;
        let response = self.request(RaftSnapshotRequest::Chunk(chunk)).await?;
        if response.snapshot_id != snapshot_id {
            return Err(SnapshotTransferError::terminal(
                "snapshot response id mismatch",
            ));
        }
        match response.status {
            RaftSnapshotStatus::ChunkVerified { index: ack } if ack == index => Ok(()),
            RaftSnapshotStatus::RetryChunk { index: ack, reason } if ack == index => {
                Err(SnapshotTransferError::retryable(reason))
            }
            RaftSnapshotStatus::Rejected { reason } => Err(SnapshotTransferError::terminal(reason)),
            status => Err(SnapshotTransferError::terminal(format!(
                "unexpected snapshot chunk response: {status:?}"
            ))),
        }
    }

    async fn finish(
        &self,
        snapshot_id: &str,
    ) -> Result<SnapshotTransferReceipt, SnapshotTransferError> {
        let response = self
            .request(RaftSnapshotRequest::Finish {
                snapshot_id: snapshot_id.to_owned(),
            })
            .await?;
        if response.snapshot_id != snapshot_id {
            return Err(SnapshotTransferError::terminal(
                "snapshot response id mismatch",
            ));
        }
        match response.status {
            RaftSnapshotStatus::Installed { size_bytes } => {
                Ok(SnapshotTransferReceipt::installed(size_bytes))
            }
            RaftSnapshotStatus::Rejected { reason } => Err(SnapshotTransferError::terminal(reason)),
            status => Err(SnapshotTransferError::terminal(format!(
                "unexpected snapshot finish response: {status:?}"
            ))),
        }
    }

    async fn abort(&self, snapshot_id: &str) {
        let _ = self
            .request(RaftSnapshotRequest::Abort {
                snapshot_id: snapshot_id.to_owned(),
            })
            .await;
    }
}

unsafe impl Send for ChirpsRaftNetworkClient {}
unsafe impl Sync for ChirpsRaftNetworkClient {}
unsafe impl Send for ChirpsRaftNetworkFactory {}
unsafe impl Sync for ChirpsRaftNetworkFactory {}

impl RaftNetworkFactory<ChirpsTypeConfig> for ChirpsRaftNetworkFactory {
    type Network = ChirpsRaftNetworkClient;

    fn new_client(
        &mut self,
        target: ChirpsNodeId,
        _node: &BasicNode,
    ) -> impl core::future::Future<Output = Self::Network> + Send {
        let inner = Arc::clone(&self.inner);
        async move { ChirpsRaftNetworkClient { inner, target } }
    }
}

impl RaftNetwork<ChirpsTypeConfig> for ChirpsRaftNetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ChirpsTypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<ChirpsNodeId>,
        RPCError<ChirpsNodeId, BasicNode, openraft::error::RaftError<ChirpsNodeId>>,
    > {
        let msg = RaftMessage::AppendEntries {
            group_id: self.inner.group_id,
            request: rpc,
        };
        match self
            .inner
            .send_rpc::<Infallible>(self.target, RPCTypes::AppendEntries, msg, option)
            .await?
        {
            RaftMessage::AppendEntriesResponse { response, .. } => Ok(response),
            other => Err(RPCError::Network(NetworkError::new(
                &RaftError::InvalidMessage(format!("unexpected response: {other:?}")),
            ))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<ChirpsTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<ChirpsNodeId>,
        RPCError<
            ChirpsNodeId,
            BasicNode,
            openraft::error::RaftError<ChirpsNodeId, InstallSnapshotError>,
        >,
    > {
        let msg = RaftMessage::InstallSnapshot {
            group_id: self.inner.group_id,
            request: rpc,
        };
        match self
            .inner
            .send_rpc::<InstallSnapshotError>(self.target, RPCTypes::InstallSnapshot, msg, option)
            .await?
        {
            RaftMessage::InstallSnapshotResponse { response, .. } => Ok(response),
            other => Err(RPCError::Network(NetworkError::new(
                &RaftError::InvalidMessage(format!("unexpected response: {other:?}")),
            ))),
        }
    }

    #[cfg(feature = "snapshot")]
    async fn full_snapshot(
        &mut self,
        vote: Vote<ChirpsNodeId>,
        mut snapshot: Snapshot<ChirpsTypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<ChirpsNodeId>, StreamingError<ChirpsTypeConfig, Fatal<ChirpsNodeId>>>
    {
        let mut bytes = Vec::new();
        snapshot
            .snapshot
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| {
                let storage = openraft::StorageIOError::read_snapshot(
                    Some(snapshot.meta.signature()),
                    &error,
                );
                openraft::StorageError::from(storage)
            })?;

        let config = *self
            .inner
            .snapshot_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if bytes.len() <= config.chunk_threshold {
            let request = InstallSnapshotRequest {
                vote,
                meta: snapshot.meta,
                offset: 0,
                data: bytes,
                done: true,
            };
            let response = self
                .install_snapshot(request, option)
                .await
                .map_err(rpc_to_streaming_error)?;
            return Ok(SnapshotResponse::new(response.vote));
        }

        let snapshot_id = snapshot.meta.snapshot_id.clone();
        let sink = Arc::new(RaftSnapshotSink {
            inner: Arc::clone(&self.inner),
            target: self.target,
            option,
            vote: Mutex::new(vote),
            meta: snapshot.meta,
        });
        let sender = SnapshotSender::new(config, self.inner.snapshot_progress_observer())
            .map_err(|error| StreamingError::Network(NetworkError::new(&error)))?;
        let transfer = sender.transfer(snapshot_id.clone(), bytes, Arc::clone(&sink));
        tokio::pin!(transfer);
        tokio::pin!(cancel);
        let result = tokio::select! {
            result = &mut transfer => result,
            closed = &mut cancel => {
                sink.abort(&snapshot_id).await;
                return Err(StreamingError::Closed(closed));
            },
        };

        match result {
            Ok(_) => Ok(SnapshotResponse::new(sink.response_vote())),
            Err(_) if sink.response_vote() > vote => {
                Ok(SnapshotResponse::new(sink.response_vote()))
            }
            Err(error) => Err(StreamingError::Network(NetworkError::new(&error))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ChirpsNodeId>,
        option: RPCOption,
    ) -> Result<
        VoteResponse<ChirpsNodeId>,
        RPCError<ChirpsNodeId, BasicNode, openraft::error::RaftError<ChirpsNodeId>>,
    > {
        let msg = RaftMessage::Vote {
            group_id: self.inner.group_id,
            request: rpc,
        };
        match self
            .inner
            .send_rpc::<Infallible>(self.target, RPCTypes::Vote, msg, option)
            .await?
        {
            RaftMessage::VoteResponse { response, .. } => Ok(response),
            other => Err(RPCError::Network(NetworkError::new(
                &RaftError::InvalidMessage(format!("unexpected response: {other:?}")),
            ))),
        }
    }
}

fn encode_node_id(id: ChirpsNodeId) -> NodeId {
    let mut buf = [0u8; 16];
    buf[8..].copy_from_slice(&id.to_be_bytes());
    NodeId::from(buf)
}

fn map_transport_error<E>(
    rpc: RPCTypes,
    id: ChirpsNodeId,
    target: ChirpsNodeId,
    timeout: std::time::Duration,
    err: TransportError,
) -> RPCError<ChirpsNodeId, BasicNode, openraft::error::RaftError<ChirpsNodeId, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match err {
        TransportError::Timeout(_) => RPCError::Timeout(Timeout {
            action: rpc,
            id,
            target,
            timeout,
        }),
        TransportError::Send(_) | TransportError::Connection(_) | TransportError::Io(_) => {
            RPCError::Network(NetworkError::new(&err))
        }
        _ => RPCError::Network(NetworkError::new(&err)),
    }
}

#[cfg(feature = "snapshot")]
fn rpc_to_streaming_error(
    error: RPCError<
        ChirpsNodeId,
        BasicNode,
        openraft::error::RaftError<ChirpsNodeId, InstallSnapshotError>,
    >,
) -> StreamingError<ChirpsTypeConfig, Fatal<ChirpsNodeId>> {
    match error {
        RPCError::Timeout(error) => StreamingError::Timeout(error),
        RPCError::Unreachable(error) => StreamingError::Unreachable(error),
        RPCError::Network(error) => StreamingError::Network(error),
        RPCError::PayloadTooLarge(error) => StreamingError::Network(NetworkError::new(&error)),
        RPCError::RemoteError(error) => StreamingError::Network(NetworkError::new(&error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_chirps_mock::{MockBackend, MockNetwork};
    use alopex_chirps_raft_storage::types::{AppendEntriesResponse, Vote};
    use std::time::Duration;

    #[tokio::test]
    async fn response_requires_matching_source_group_and_correlation() {
        let network = MockNetwork::new();
        let backend = network
            .add_node(NodeId::from([0; 16]), MockBackend::ephemeral_addr())
            .await;
        let backend: Arc<dyn MessageBackend> = Arc::new(backend);
        let transport = ChirpsRaftTransport::new(backend, GroupId(4), 1);
        let (tx, rx) = oneshot::channel();
        transport
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                9,
                PendingRpc {
                    target: 2,
                    group_id: GroupId(4),
                    rpc_type: RPCTypes::Vote,
                    response: tx,
                },
            );
        let response = || RaftFramePayload {
            correlation_id: 9,
            message: RaftMessage::VoteResponse {
                group_id: GroupId(4),
                response: VoteResponse {
                    vote: Vote::new(1, 2),
                    vote_granted: true,
                    last_log_id: None,
                },
            },
        };

        assert!(
            transport
                .consume_incoming_from(3, response())
                .await
                .is_err()
        );
        assert_eq!(
            transport
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1
        );
        let mut wrong_correlation = response();
        wrong_correlation.correlation_id = 10;
        assert!(
            transport
                .consume_incoming_from(2, wrong_correlation)
                .await
                .is_err()
        );
        assert_eq!(
            transport
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1
        );
        let wrong_type = RaftFramePayload {
            correlation_id: 9,
            message: RaftMessage::AppendEntriesResponse {
                group_id: GroupId(4),
                response: AppendEntriesResponse::Success,
            },
        };
        assert!(
            transport
                .consume_incoming_from(2, wrong_type)
                .await
                .is_err()
        );
        assert_eq!(
            transport
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1
        );
        assert!(
            transport
                .consume_incoming_from(2, response())
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            rx.await.unwrap(),
            RaftMessage::VoteResponse {
                group_id: GroupId(4),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn successful_transport_io_updates_message_metrics() {
        let network = MockNetwork::new();
        let backend = network
            .add_node(encode_node_id(1), MockBackend::ephemeral_addr())
            .await;
        let peer = network
            .add_node(encode_node_id(2), MockBackend::ephemeral_addr())
            .await;
        let mut peer_rx = peer.subscribe().await.unwrap();
        let backend: Arc<dyn MessageBackend> = Arc::new(backend);
        let transport = ChirpsRaftTransport::new(backend, GroupId(4), 1);
        let collector = Arc::new(RaftMetricsCollector::new());
        transport.set_metrics_collector(Arc::clone(&collector));

        transport
            .send_response(
                2,
                7,
                RaftMessage::VoteResponse {
                    group_id: GroupId(4),
                    response: VoteResponse {
                        vote: Vote::new(1, 1),
                        vote_granted: true,
                        last_log_id: None,
                    },
                },
            )
            .await
            .unwrap();
        peer_rx.recv().await.expect("peer receives response");

        let request = RaftFramePayload {
            correlation_id: 8,
            message: RaftMessage::Vote {
                group_id: GroupId(4),
                request: VoteRequest {
                    vote: Vote::new(2, 2),
                    last_log_id: None,
                },
            },
        };
        assert!(transport.consume_incoming(request).await.is_some());

        let body = collector.encode().unwrap();
        assert!(body.contains(
            "chirps_raft_messages_sent_total{group_id=\"4\",msg_type=\"vote_response\"} 1"
        ));
        assert!(
            body.contains(
                "chirps_raft_messages_received_total{group_id=\"4\",msg_type=\"vote\"} 1"
            )
        );
    }

    #[tokio::test]
    async fn cancelled_future_and_shutdown_cleanup_remove_pending_rpcs() {
        let network = MockNetwork::new();
        let backend = network
            .add_node(NodeId::from([0; 16]), MockBackend::ephemeral_addr())
            .await;
        let _peer = network
            .add_node(encode_node_id(2), MockBackend::ephemeral_addr())
            .await;
        let backend: Arc<dyn MessageBackend> = Arc::new(backend);
        let transport = Arc::new(ChirpsRaftTransport::new(backend, GroupId(4), 1));

        let task = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                transport
                    .send_rpc::<Infallible>(
                        2,
                        RPCTypes::Vote,
                        RaftMessage::Vote {
                            group_id: GroupId(4),
                            request: VoteRequest {
                                vote: Vote::new(1, 1),
                                last_log_id: None,
                            },
                        },
                        RPCOption::new(Duration::from_secs(60)),
                    )
                    .await
            })
        };
        time::timeout(Duration::from_secs(1), async {
            loop {
                if transport
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len()
                    == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("outbound RPC must register pending state");

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            transport
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );

        transport.close_rpc_admission();
        assert!(
            transport
                .send_rpc::<Infallible>(
                    2,
                    RPCTypes::Vote,
                    RaftMessage::Vote {
                        group_id: GroupId(4),
                        request: VoteRequest {
                            vote: Vote::new(1, 1),
                            last_log_id: None,
                        },
                    },
                    RPCOption::new(Duration::from_secs(60)),
                )
                .await
                .is_err()
        );
        transport.cancel_pending_rpcs();
        assert!(
            transport
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
    }
}
