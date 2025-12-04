use crate::backend::MessageBackend;
use crate::raft::{RaftError, RaftMessage};
use chirps_core::error::TransportError;
use chirps_raft_storage::types::{
    AppendEntriesRequest, AppendEntriesResponse, BasicNode, ChirpsNodeId, ChirpsTypeConfig,
    GroupId, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use chirps_wire::frame::{Frame, RaftFrame};
use chirps_wire::node_id::NodeId;
use openraft::error::{Infallible, InstallSnapshotError, NetworkError, RPCError, Timeout};
use openraft::network::{RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{Mutex, oneshot};
use tokio::time;

/// Raft RPCメッセージをフレーム化する際のコンテナ。レスポンス対応のため相関IDを保持する。
#[derive(Debug, Serialize, Deserialize)]
pub struct RaftFramePayload {
    pub correlation_id: u64,
    pub message: RaftMessage,
}

/// Raftネットワークのファクトリ。openraftのRaftNetworkFactoryを実装する。
pub struct ChirpsRaftTransport {
    backend: Arc<dyn MessageBackend>,
    group_id: GroupId,
    node_id: ChirpsNodeId,
    next_corr: AtomicU64,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<RaftMessage>>>>,
}

impl ChirpsRaftTransport {
    pub fn new(backend: Arc<dyn MessageBackend>, group_id: GroupId, node_id: ChirpsNodeId) -> Self {
        Self {
            backend,
            group_id,
            node_id,
            next_corr: AtomicU64::new(1),
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// RaftNetworkFactoryをwrapした型を取得する。
    pub fn factory(inner: Arc<ChirpsRaftTransport>) -> ChirpsRaftNetworkFactory {
        ChirpsRaftNetworkFactory { inner }
    }

    /// Raftフレームをデコードする。対象グループ以外はNoneを返す。
    pub fn decode_frame(frame: Frame) -> Option<RaftFramePayload> {
        match frame {
            Frame::Raft(data) | Frame::RaftSnapshot(data) => {
                bincode::deserialize::<RaftFramePayload>(&data.payload).ok()
            }
            _ => None,
        }
    }

    /// レスポンス用に送信待ちマップを確認する。マッチした場合は待ち受けに届け、リクエストの場合はSomeを返す。
    pub async fn consume_incoming(&self, payload: RaftFramePayload) -> Option<RaftFramePayload> {
        let mut guard = self.pending.lock().await;
        if let Some(tx) = guard.remove(&payload.correlation_id) {
            let _ = tx.send(payload.message);
            None
        } else {
            Some(payload)
        }
    }

    /// 受信サイドからレスポンスを送る際に使用する。
    pub async fn send_response(
        &self,
        target: ChirpsNodeId,
        correlation_id: u64,
        message: RaftMessage,
    ) -> Result<(), TransportError> {
        let frame = self
            .encode_frame(RaftFramePayload {
                correlation_id,
                message,
            })
            .map_err(|e| TransportError::Send(e.to_string()))?;
        let target = encode_node_id(target);
        self.backend.send(target, frame).await
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
        let correlation_id = self.next_corr.fetch_add(1, Ordering::Relaxed);
        let frame = self
            .encode_frame(RaftFramePayload {
                correlation_id,
                message,
            })
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self.pending.lock().await;
            guard.insert(correlation_id, tx);
        }

        let send_res = self
            .backend
            .send(encode_node_id(target), frame)
            .await
            .map_err(|e| {
                map_transport_error::<E>(rpc_type, self.node_id, target, option.hard_ttl(), e)
            });

        if let Err(err) = send_res {
            let mut guard = self.pending.lock().await;
            guard.remove(&correlation_id);
            return Err(err);
        }

        match time::timeout(option.hard_ttl(), rx).await {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(_canceled)) => {
                let mut guard = self.pending.lock().await;
                guard.remove(&correlation_id);
                Err(RPCError::Network(NetworkError::new(&RaftError::Internal(
                    anyhow::anyhow!("response channel closed"),
                ))))
            }
            Err(_) => {
                let mut guard = self.pending.lock().await;
                guard.remove(&correlation_id);
                Err(RPCError::Timeout(Timeout {
                    action: rpc_type,
                    id: self.node_id,
                    target,
                    timeout: option.hard_ttl(),
                }))
            }
        }
    }

    fn encode_frame(&self, payload: RaftFramePayload) -> Result<Frame, bincode::Error> {
        let bytes = bincode::serialize(&payload)?;
        let raft_frame = RaftFrame {
            group_id: self.group_id.0,
            payload: bytes,
        };
        let frame = match payload.message {
            RaftMessage::InstallSnapshot { .. } | RaftMessage::InstallSnapshotResponse { .. } => {
                Frame::RaftSnapshot(raft_frame)
            }
            _ => Frame::Raft(raft_frame),
        };
        Ok(frame)
    }
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
    fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ChirpsTypeConfig>,
        option: RPCOption,
    ) -> impl core::future::Future<
        Output = Result<
            AppendEntriesResponse<ChirpsNodeId>,
            RPCError<ChirpsNodeId, BasicNode, openraft::error::RaftError<ChirpsNodeId>>,
        >,
    > + Send {
        async move {
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
    }

    fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<ChirpsTypeConfig>,
        option: RPCOption,
    ) -> impl core::future::Future<
        Output = Result<
            InstallSnapshotResponse<ChirpsNodeId>,
            RPCError<
                ChirpsNodeId,
                BasicNode,
                openraft::error::RaftError<ChirpsNodeId, InstallSnapshotError>,
            >,
        >,
    > + Send {
        async move {
            let msg = RaftMessage::InstallSnapshot {
                group_id: self.inner.group_id,
                request: rpc,
            };
            match self
                .inner
                .send_rpc::<InstallSnapshotError>(
                    self.target,
                    RPCTypes::InstallSnapshot,
                    msg,
                    option,
                )
                .await?
            {
                RaftMessage::InstallSnapshotResponse { response, .. } => Ok(response),
                other => Err(RPCError::Network(NetworkError::new(
                    &RaftError::InvalidMessage(format!("unexpected response: {other:?}")),
                ))),
            }
        }
    }

    fn vote(
        &mut self,
        rpc: VoteRequest<ChirpsNodeId>,
        option: RPCOption,
    ) -> impl core::future::Future<
        Output = Result<
            VoteResponse<ChirpsNodeId>,
            RPCError<ChirpsNodeId, BasicNode, openraft::error::RaftError<ChirpsNodeId>>,
        >,
    > + Send {
        async move {
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
