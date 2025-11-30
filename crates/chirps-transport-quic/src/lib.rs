use async_trait::async_trait;
use bincode::{deserialize, serialize};
use chirps_core::backend::MessageBackend;
use chirps_core::config::NodeConfig;
use chirps_core::error::TransportError;
use chirps_wire::frame::Frame;
use chirps_wire::node_id::NodeId;
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, ClientConfig as RustlsClientConfig, PrivateKey, RootCertStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, oneshot};
use tokio::time;
use tracing::{info, warn};

mod priority;
mod qos;
mod reconnect;
mod retransmit;

use priority::Priority;
pub use qos::{
    BandwidthConfig, QosConfig, QosController, QosError, QosMetrics, QueueLimits, TokenBucket,
};
use reconnect::{ReconnectCommand, start_seed_reconnector};
pub use retransmit::{
    BufferError, BufferStats, BufferedMessage, RetransmissionBuffer, RetransmitConfig,
};

const DEFAULT_SERVER_NAME: &str = "alopex.local";
const MAX_FRAME_SIZE: usize = 64 * 1024;
const SEND_RETRY_ATTEMPTS: usize = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StreamKind {
    Control = 0,
    Gossip = 1,
    User = 2,
    Raft = 3,
    RaftSnapshot = 4,
}

impl StreamKind {
    pub(crate) fn priority(&self) -> Priority {
        match self {
            StreamKind::Control | StreamKind::Raft => Priority::High,
            StreamKind::Gossip | StreamKind::RaftSnapshot | StreamKind::User => Priority::Normal,
        }
    }

    pub(crate) fn requires_ack(&self) -> bool {
        matches!(
            self,
            StreamKind::Control | StreamKind::Raft | StreamKind::RaftSnapshot
        )
    }
}

impl TryFrom<u8> for StreamKind {
    type Error = TransportError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(StreamKind::Control),
            1 => Ok(StreamKind::Gossip),
            2 => Ok(StreamKind::User),
            3 => Ok(StreamKind::Raft),
            4 => Ok(StreamKind::RaftSnapshot),
            other => Err(TransportError::InvalidStreamKind(other)),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum WireMessage {
    Handshake(NodeId),
    Frame(FrameEnvelope),
}

#[derive(Serialize, Deserialize)]
struct FrameEnvelope {
    from: NodeId,
    frame: Frame,
}

#[derive(Default)]
struct TransportCounters {
    sent: AtomicU64,
    received: AtomicU64,
    dropped: AtomicU64,
    retried: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct TransportMetricsSnapshot {
    pub sent: u64,
    pub received: u64,
    pub dropped: u64,
    pub retried: u64,
}

enum SendCommand {
    Unicast {
        target: NodeId,
        frame: Frame,
        respond_to: oneshot::Sender<Result<(), TransportError>>,
    },
    Broadcast {
        frame: Frame,
        respond_to: oneshot::Sender<Result<usize, TransportError>>,
    },
}

pub struct QuicBackend {
    node_id: NodeId,
    endpoint: Endpoint,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    incoming_tx: mpsc::Sender<(NodeId, Frame)>,
    incoming_rx: Arc<Mutex<Option<mpsc::Receiver<(NodeId, Frame)>>>>,
    shutdown: broadcast::Sender<()>,
    reconnect_tx: mpsc::Sender<ReconnectCommand>,
    send_tx: mpsc::Sender<SendCommand>,
    send_timeout: Duration,
    metrics: Arc<TransportCounters>,
}

impl QuicBackend {
    pub async fn new(node_id: NodeId, config: Arc<NodeConfig>) -> anyhow::Result<Self> {
        let (server_config, client_config) = build_tls_configs(&config)?;
        let mut endpoint = Endpoint::server(server_config, config.bind_addr)?;
        endpoint.set_default_client_config(ClientConfig::new(client_config.clone()));

        let (incoming_tx, incoming_rx) = mpsc::channel(1024);
        let (send_tx, send_rx) = mpsc::channel(config.send_queue_capacity);
        let (shutdown, _) = broadcast::channel(4);
        let connections = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(TransportCounters::default());
        let reconnect_tx = start_seed_reconnector(
            config.seeds.clone(),
            endpoint.clone(),
            client_config.clone(),
            Arc::clone(&connections),
            incoming_tx.clone(),
            shutdown.clone(),
            node_id,
            Arc::clone(&metrics),
        );
        let backend = QuicBackend {
            node_id,
            endpoint: endpoint.clone(),
            connections,
            incoming_tx,
            incoming_rx: Arc::new(Mutex::new(Some(incoming_rx))),
            shutdown,
            reconnect_tx,
            send_tx,
            send_timeout: config.broadcast_timeout,
            metrics: Arc::clone(&metrics),
        };

        backend.spawn_accept_loop();
        spawn_send_loop(
            Arc::clone(&backend.connections),
            Arc::clone(&metrics),
            send_rx,
            backend.shutdown.subscribe(),
            backend.node_id,
            backend.send_timeout,
        );
        let _ = backend.reconnect_tx.try_send(ReconnectCommand::Trigger);

        Ok(backend)
    }

    fn spawn_accept_loop(&self) {
        let endpoint = self.endpoint.clone();
        let connections = Arc::clone(&self.connections);
        let incoming_tx = self.incoming_tx.clone();
        let mut shutdown_rx = self.shutdown.subscribe();
        let local_id = self.node_id;
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            loop {
                select! {
                    _ = shutdown_rx.recv() => break,
                    incoming = endpoint.accept() => {
                        match incoming {
                            Some(connecting) => {
                                match connecting.await {
                                    Ok(connection) => {
                                        let connections = Arc::clone(&connections);
                                        let incoming_tx = incoming_tx.clone();
                                        let mut shutdown_rx = shutdown_rx.resubscribe();
                                        let metrics = Arc::clone(&metrics);
                                        tokio::spawn(async move {
                                            if let Err(err) = handle_connection(connection, local_id, connections, incoming_tx, metrics, &mut shutdown_rx).await {
                                                warn!("connection handler failed: {err}");
                                            }
                                        });
                                    }
                                    Err(err) => warn!("failed to accept connection: {err}"),
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });
    }

    /// 手動トリガーでシードへの再接続を促す。
    pub async fn reconnect_to_seeds(&self) -> Result<(), TransportError> {
        self.reconnect_tx
            .send(ReconnectCommand::Trigger)
            .await
            .map_err(|_| TransportError::Connection("reconnect worker stopped".into()))
    }

    /// 現在のトランスポートメトリクスを取得する。
    pub fn metrics(&self) -> TransportMetricsSnapshot {
        TransportMetricsSnapshot {
            sent: self.metrics.sent.load(Ordering::Relaxed),
            received: self.metrics.received.load(Ordering::Relaxed),
            dropped: self.metrics.dropped.load(Ordering::Relaxed),
            retried: self.metrics.retried.load(Ordering::Relaxed),
        }
    }
}

#[async_trait]
impl MessageBackend for QuicBackend {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError> {
        let (respond_to, recv) = oneshot::channel();
        if let Err(err) = self.send_tx.try_send(SendCommand::Unicast {
            target,
            frame,
            respond_to,
        }) {
            if matches!(err, mpsc::error::TrySendError::Full(_)) {
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                warn!(target = ?target, "send queue is full; rejecting send");
            }
            return Err(map_queue_error(err));
        }
        recv.await
            .map_err(|_| TransportError::Send("send loop stopped".into()))?
    }

    async fn broadcast(&self, frame: Frame) -> Result<usize, TransportError> {
        let (respond_to, recv) = oneshot::channel();
        if let Err(err) = self
            .send_tx
            .try_send(SendCommand::Broadcast { frame, respond_to })
        {
            if matches!(err, mpsc::error::TrySendError::Full(_)) {
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                warn!("send queue is full; rejecting broadcast");
            }
            return Err(map_queue_error(err));
        }
        recv.await
            .map_err(|_| TransportError::Send("send loop stopped".into()))?
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        let mut guard = self.incoming_rx.lock().await;
        guard
            .take()
            .ok_or_else(|| TransportError::Subscribe("already subscribed".into()))
    }

    async fn close(&self) -> Result<(), TransportError> {
        let _ = self.shutdown.send(());
        self.endpoint.close(0u32.into(), b"shutdown");
        Ok(())
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        if let Ok(map) = self.connections.try_read() {
            map.iter()
                .map(|(id, conn)| (*id, conn.remote_address()))
                .collect()
        } else {
            Vec::new()
        }
    }
}

async fn handle_connection(
    connection: Connection,
    local_id: NodeId,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    incoming_tx: mpsc::Sender<(NodeId, Frame)>,
    metrics: Arc<TransportCounters>,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<(), TransportError> {
    send_handshake(&connection, local_id).await?;
    let remote_id = recv_handshake(&connection).await?;

    connections
        .write()
        .await
        .insert(remote_id, connection.clone());
    info!(
        peer = ?remote_id,
        addr = ?connection.remote_address(),
        "QUIC peer connected"
    );

    loop {
        select! {
            _ = shutdown_rx.recv() => {
                connections.write().await.remove(&remote_id);
                info!(peer = ?remote_id, "connection closed");
                break;
            }
            next = connection.accept_uni() => match next {
                Ok(recv) => {
                    let connections = Arc::clone(&connections);
                    let incoming_tx = incoming_tx.clone();
                    let connection = connection.clone();
                    let metrics = Arc::clone(&metrics);
                    tokio::spawn(async move {
                        if let Err(err) = handle_incoming_stream(recv, connection, connections, incoming_tx, metrics)
                        .await
                        {
                            warn!("failed to read stream: {err}");
                        }
                    });
                }
                Err(err) => {
                    connections.write().await.remove(&remote_id);
                    return Err(TransportError::Connection(err.to_string()));
                }
            },
        }
    }

    Ok(())
}

async fn send_handshake(connection: &Connection, node_id: NodeId) -> Result<(), TransportError> {
    send_wire_message(
        connection,
        StreamKind::Control,
        WireMessage::Handshake(node_id),
    )
    .await
}

async fn recv_handshake(connection: &Connection) -> Result<NodeId, TransportError> {
    match connection.accept_uni().await {
        Ok(recv) => match read_wire_message(recv).await? {
            (StreamKind::Control, WireMessage::Handshake(node_id)) => Ok(node_id),
            _ => Err(TransportError::Connection(
                "unexpected message during handshake".into(),
            )),
        },
        Err(err) => Err(TransportError::Connection(err.to_string())),
    }
}

async fn send_frame(
    connection: &Connection,
    from: &NodeId,
    frame: Frame,
) -> Result<(), TransportError> {
    let kind = stream_kind_for_frame(&frame);
    let env = WireMessage::Frame(FrameEnvelope { from: *from, frame });
    send_wire_message(connection, kind, env).await
}

async fn send_wire_message(
    connection: &Connection,
    kind: StreamKind,
    msg: WireMessage,
) -> Result<(), TransportError> {
    let bytes = serialize(&msg).map_err(|e| TransportError::Send(e.to_string()))?;
    let mut stream = connection
        .open_uni()
        .await
        .map_err(|e| TransportError::Send(e.to_string()))?;
    stream
        .write_u8(kind as u8)
        .await
        .map_err(|e| TransportError::Send(e.to_string()))?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| TransportError::Send(e.to_string()))?;
    stream
        .finish()
        .await
        .map_err(|e| TransportError::Send(e.to_string()))
}

async fn read_wire_message(
    mut recv: RecvStream,
) -> Result<(StreamKind, WireMessage), TransportError> {
    let bytes = recv
        .read_to_end(MAX_FRAME_SIZE + 1)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    let (kind_byte, payload) = bytes
        .split_first()
        .ok_or_else(|| TransportError::Io("empty stream".into()))?;
    let kind = StreamKind::try_from(*kind_byte)?;
    let msg = deserialize(payload).map_err(|e| TransportError::Io(e.to_string()))?;
    Ok((kind, msg))
}

async fn handle_incoming_stream(
    recv: RecvStream,
    connection: Connection,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    incoming_tx: mpsc::Sender<(NodeId, Frame)>,
    metrics: Arc<TransportCounters>,
) -> Result<(), TransportError> {
    match read_wire_message(recv).await {
        Ok((StreamKind::Control, WireMessage::Handshake(id))) => {
            connections.write().await.insert(id, connection);
        }
        Ok((
            StreamKind::Gossip | StreamKind::User | StreamKind::Raft | StreamKind::RaftSnapshot,
            WireMessage::Frame(env),
        )) => {
            let _ = incoming_tx.send((env.from, env.frame)).await;
            metrics.received.fetch_add(1, Ordering::Relaxed);
        }
        Ok((_, WireMessage::Handshake(_))) => {
            // Unexpected control message kind; ignore.
        }
        Ok((kind, WireMessage::Frame(_))) => {
            warn!("unhandled stream kind for frame: {kind:?}");
        }
        Err(err) => return Err(err),
    }

    Ok(())
}

fn stream_kind_for_frame(frame: &Frame) -> StreamKind {
    match frame {
        Frame::User(_) => StreamKind::User,
        _ => StreamKind::Gossip,
    }
}

fn map_queue_error(err: mpsc::error::TrySendError<SendCommand>) -> TransportError {
    match err {
        mpsc::error::TrySendError::Full(_) => TransportError::Timeout("send queue is full".into()),
        mpsc::error::TrySendError::Closed(_) => {
            TransportError::Connection("send loop has stopped".into())
        }
    }
}

fn spawn_send_loop(
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    metrics: Arc<TransportCounters>,
    mut rx: mpsc::Receiver<SendCommand>,
    mut shutdown_rx: broadcast::Receiver<()>,
    node_id: NodeId,
    timeout: Duration,
) {
    tokio::spawn(async move {
        loop {
            select! {
                _ = shutdown_rx.recv() => break,
                cmd = rx.recv() => match cmd {
                    Some(SendCommand::Unicast { target, frame, respond_to }) => {
                        let send_res = send_with_retry(&connections, &metrics, node_id, target, frame, timeout).await;
                        let _ = respond_to.send(send_res);
                    }
                    Some(SendCommand::Broadcast { frame, respond_to }) => {
                        let send_res =
                            broadcast_with_retry(&connections, &metrics, node_id, frame, timeout).await;
                        let _ = respond_to.send(send_res);
                    }
                    None => break,
                }
            }
        }
    });
}

async fn send_with_retry(
    connections: &Arc<RwLock<HashMap<NodeId, Connection>>>,
    metrics: &Arc<TransportCounters>,
    node_id: NodeId,
    target: NodeId,
    frame: Frame,
    timeout: Duration,
) -> Result<(), TransportError> {
    let mut attempts = 0;
    loop {
        let frame_clone = frame.clone();
        match time::timeout(
            timeout,
            send_to_peer(connections, metrics, node_id, target, frame_clone),
        )
        .await
        {
            Ok(res) => {
                if attempts > 0 {
                    metrics.retried.fetch_add(1, Ordering::Relaxed);
                    warn!(target = ?target, attempts, "send retry succeeded after timeout");
                }
                return res;
            }
            Err(_) if attempts < SEND_RETRY_ATTEMPTS => {
                attempts += 1;
                warn!(target = ?target, attempts, "send timed out, retrying");
                continue;
            }
            Err(_) => {
                warn!(target = ?target, "send timed out after retries");
                return Err(TransportError::Timeout("send timed out".into()));
            }
        }
    }
}

async fn broadcast_with_retry(
    connections: &Arc<RwLock<HashMap<NodeId, Connection>>>,
    metrics: &Arc<TransportCounters>,
    node_id: NodeId,
    frame: Frame,
    timeout: Duration,
) -> Result<usize, TransportError> {
    let mut attempts = 0;
    loop {
        let frame_clone = frame.clone();
        match time::timeout(
            timeout,
            broadcast_to_peers(connections, metrics, node_id, frame_clone),
        )
        .await
        {
            Ok(res) => {
                if attempts > 0 {
                    metrics.retried.fetch_add(1, Ordering::Relaxed);
                    warn!(attempts, "broadcast retry succeeded after timeout");
                }
                return res;
            }
            Err(_) if attempts < SEND_RETRY_ATTEMPTS => {
                attempts += 1;
                warn!(attempts, "broadcast timed out, retrying");
                continue;
            }
            Err(_) => {
                warn!("broadcast timed out after retries");
                return Err(TransportError::Timeout("broadcast timed out".into()));
            }
        }
    }
}

async fn send_to_peer(
    connections: &Arc<RwLock<HashMap<NodeId, Connection>>>,
    metrics: &Arc<TransportCounters>,
    node_id: NodeId,
    target: NodeId,
    frame: Frame,
) -> Result<(), TransportError> {
    let conn = {
        let map = connections.read().await;
        map.get(&target)
            .cloned()
            .ok_or_else(|| TransportError::Connection(format!("peer {target:?} not connected")))?
    };
    let res = send_frame(&conn, &node_id, frame).await;
    if res.is_ok() {
        metrics.sent.fetch_add(1, Ordering::Relaxed);
    }
    res
}

async fn broadcast_to_peers(
    connections: &Arc<RwLock<HashMap<NodeId, Connection>>>,
    metrics: &Arc<TransportCounters>,
    node_id: NodeId,
    frame: Frame,
) -> Result<usize, TransportError> {
    let peers: Vec<Connection> = {
        let map = connections.read().await;
        map.values().cloned().collect()
    };
    let mut sent = 0;
    for conn in peers {
        if let Err(err) = send_frame(&conn, &node_id, frame.clone()).await {
            warn!("broadcast send failed: {err}");
        } else {
            sent += 1;
            metrics.sent.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(sent)
}

fn build_tls_configs(
    config: &NodeConfig,
) -> anyhow::Result<(ServerConfig, Arc<RustlsClientConfig>)> {
    let (cert_der, key_der) = if let (Some(cert_path), Some(key_path)) =
        (config.cert_path.as_ref(), config.key_path.as_ref())
    {
        (fs::read(cert_path)?, fs::read(key_path)?)
    } else {
        let cert = generate_simple_self_signed([DEFAULT_SERVER_NAME.to_string()])?;
        (cert.serialize_der()?, cert.serialize_private_key_der())
    };

    let cert_chain = vec![Certificate(cert_der.clone())];
    let priv_key = PrivateKey(key_der);
    let server_config = ServerConfig::with_single_cert(cert_chain.clone(), priv_key)?;

    let mut roots = RootCertStore::empty();
    roots
        .add(&Certificate(cert_der))
        .map_err(|_| anyhow::anyhow!("failed to add root cert"))?;

    let mut client_crypto = RustlsClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"alopex".to_vec()];

    Ok((server_config, Arc::new(client_crypto)))
}
