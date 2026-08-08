//! QUIC transport for Chirps v0.4 with priority scheduling, retransmission, QoS, and versioned handshakes.
//!
//! Key concepts:
//! - [`StreamKind`] models control, gossip, user, Raft, and snapshot streams with per-kind priority.
//! - [`QosController`] applies backpressure and bandwidth throttling (e.g., snapshot token bucket).
//! - [`RetransmissionBuffer`] retains in-flight frames for reconnect replay with sequence/ack handling.
//! - [`ExtendedTransportMetrics`] exposes counters and latency histograms.
//! - [`HandshakeMessage`] enforces protocol versions and negotiates capabilities.
//!
//! Use [`QuicBackend`] via the `MessageBackend` trait to send/broadcast frames and subscribe to incoming messages.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_core::config::NodeConfig;
use alopex_chirps_core::error::TransportError;
use alopex_chirps_wire::node_id::NodeId;
use alopex_chirps_wire::{envelope::FrameEnvelopeV2, frame::Frame};
use async_trait::async_trait;
use bincode::{deserialize, serialize, serialized_size};
use quinn::{
    ClientConfig, Connection, Endpoint, RecvStream, ServerConfig,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
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
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, broadcast, mpsc, oneshot};
use tokio::time;
use tokio::time::Instant;
use tracing::{info, warn};

mod config;
mod events;
mod handshake;
mod metrics;
mod priority;
mod qos;
mod receive;
mod reconnect;
mod retransmit;
mod telemetry;

pub use config::{
    BandwidthConfig, HandshakeConfig, PriorityConfig, QosConfig, QueueLimits, RetransmitConfig,
    TransportConfigV04,
};
pub use events::{TransportEvent, emit_event};
pub use handshake::{
    Capabilities, HandshakeError, HandshakeMessage, MIN_COMPATIBLE_VERSION, NegotiatedCapabilities,
    PROTOCOL_VERSION, negotiate,
};
pub use metrics::{ExtendedTransportMetrics, LatencySnapshot, MetricsSnapshot};
use priority::{Priority, PriorityScheduler, ScheduledMessage, SchedulerConfig};
pub use qos::{QosController, QosError, QosMetrics, TokenBucket};
use receive::RAFT_BATCH_STREAM_MAGIC;
pub use receive::ReceiveHandler;
use reconnect::{ReconnectCommand, start_seed_reconnector};
pub use retransmit::{BufferError, BufferStats, BufferedMessage, RetransmissionBuffer};
pub use telemetry::{LogFormat, TelemetryConfig, init_metrics, init_test_tracing, init_tracing};

const DEFAULT_SERVER_NAME: &str = "alopex.local";
const MAX_FRAME_SIZE: usize = 64 * 1024;
const MAX_CONCURRENT_SENDS: usize = 64;
const SEND_RETRY_ATTEMPTS: usize = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
/// Transport stream categories with priority and reliability semantics.
pub enum StreamKind {
    /// Control plane traffic (highest priority, reliable).
    Control = 0,
    /// Gossip traffic for membership.
    Gossip = 1,
    /// User application traffic (lowest priority).
    User = 2,
    /// Raft consensus traffic (high priority, reliable).
    Raft = 3,
    /// Raft snapshot/install traffic (normal priority, throttled).
    RaftSnapshot = 4,
    /// File transfer control/data streams.
    FileTransfer = 5,
}

impl StreamKind {
    pub(crate) fn priority(&self) -> Priority {
        match self {
            StreamKind::Control | StreamKind::Raft => Priority::High,
            StreamKind::Gossip | StreamKind::RaftSnapshot | StreamKind::FileTransfer => {
                Priority::Normal
            }
            StreamKind::User => Priority::Low,
        }
    }

    pub(crate) fn requires_ack(&self) -> bool {
        matches!(
            self,
            StreamKind::Control
                | StreamKind::Raft
                | StreamKind::RaftSnapshot
                | StreamKind::FileTransfer
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
            5 => Ok(StreamKind::FileTransfer),
            other => Err(TransportError::InvalidStreamKind(other)),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize)]
enum WireMessage {
    Handshake(HandshakeMessage),
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
    concurrent_sends: AtomicU64,
    max_concurrent_sends: AtomicU64,
    streams_opened: AtomicU64,
}

struct BatchStream {
    stream: quinn::SendStream,
    envelopes: usize,
}

#[derive(Debug, Clone, Default)]
pub struct TransportMetricsSnapshot {
    pub sent: u64,
    pub received: u64,
    pub dropped: u64,
    pub retried: u64,
    /// Number of QUIC sends currently executing in the transport worker.
    pub concurrent_sends: u64,
    /// High-water mark of concurrently executing QUIC sends.
    pub max_concurrent_sends: u64,
    /// Number of Raft data streams opened (not envelope count).
    pub streams_opened: u64,
}

struct SendConcurrencyGuard {
    metrics: Arc<TransportCounters>,
}

impl SendConcurrencyGuard {
    fn enter(metrics: Arc<TransportCounters>) -> Self {
        let active = metrics.concurrent_sends.fetch_add(1, Ordering::Relaxed) + 1;
        metrics
            .max_concurrent_sends
            .fetch_max(active, Ordering::Relaxed);
        Self { metrics }
    }
}

impl Drop for SendConcurrencyGuard {
    fn drop(&mut self) {
        self.metrics
            .concurrent_sends
            .fetch_sub(1, Ordering::Relaxed);
    }
}

enum SendCommand {
    Unicast {
        target: NodeId,
        frame: Frame,
        respond_to: oneshot::Sender<Result<(), TransportError>>,
        _slot: OwnedSemaphorePermit,
    },
    Broadcast {
        frame: Frame,
        respond_to: oneshot::Sender<Result<usize, TransportError>>,
        _slot: OwnedSemaphorePermit,
    },
}

impl SendCommand {
    fn frame(&self) -> &Frame {
        match self {
            Self::Unicast { frame, .. } | Self::Broadcast { frame, .. } => frame,
        }
    }
}

/// QUIC transport backend implementing the `MessageBackend` trait with QoS, retransmission, and versioned handshakes.
pub struct QuicBackend {
    node_id: NodeId,
    endpoint: Endpoint,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    peer_capabilities: Arc<RwLock<HashMap<NodeId, NegotiatedCapabilities>>>,
    #[allow(dead_code)]
    incoming_tx: mpsc::Sender<(NodeId, Frame)>,
    incoming_rx: Arc<Mutex<Option<mpsc::Receiver<(NodeId, Frame)>>>>,
    file_transfer_rx: Arc<Mutex<Option<mpsc::Receiver<(NodeId, RecvStream)>>>>,
    shutdown: broadcast::Sender<()>,
    reconnect_tx: mpsc::Sender<ReconnectCommand>,
    send_tx: mpsc::Sender<SendCommand>,
    send_slots: Arc<Semaphore>,
    send_timeout: Duration,
    await_peer_stop: bool,
    metrics: Arc<TransportCounters>,
    retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
    raft_batch_streams: Arc<Mutex<HashMap<NodeId, BatchStream>>>,
    metrics_ext: Arc<ExtendedTransportMetrics>,
    receive_handler: Arc<ReceiveHandler>,
    handshake_config: HandshakeConfig,
}

impl QuicBackend {
    /// Create a backend using default transport config (v0.4) and provided node config.
    pub async fn new(node_id: NodeId, config: Arc<NodeConfig>) -> anyhow::Result<Self> {
        let transport_config = TransportConfigV04 {
            send_queue_capacity: config.send_queue_capacity,
            ..Default::default()
        };
        Self::new_with_config(node_id, config, transport_config).await
    }

    /// Create a backend with explicit transport configuration (priority, retransmit, QoS, handshake settings).
    pub async fn new_with_config(
        node_id: NodeId,
        config: Arc<NodeConfig>,
        transport_config: TransportConfigV04,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        if transport_config.send_queue_capacity == 0 {
            anyhow::bail!("send_queue_capacity must be greater than zero");
        }
        if transport_config.raft_stream_batch_size == 0 {
            anyhow::bail!("raft_stream_batch_size must be greater than zero");
        }
        let (server_config, client_config) = build_tls_configs(&config)?;
        let mut endpoint = Endpoint::server(server_config, config.bind_addr)?;
        endpoint.set_default_client_config(client_config.clone());

        let (incoming_tx, incoming_rx) = mpsc::channel(1024);
        let (file_transfer_tx, file_transfer_rx) = mpsc::channel(64);
        let (send_tx, send_rx) = mpsc::channel(transport_config.send_queue_capacity);
        let send_slots = Arc::new(Semaphore::new(transport_config.send_queue_capacity));
        let (shutdown, _) = broadcast::channel(4);
        let connections = Arc::new(RwLock::new(HashMap::new()));
        let peer_capabilities = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(TransportCounters::default());
        let metrics_ext = Arc::new(ExtendedTransportMetrics::new_with_enabled(
            transport_config.diagnostics_enabled,
        ));
        let retransmit_buffer = Arc::new(RwLock::new(RetransmissionBuffer::new(
            transport_config.retransmit.clone(),
        )));
        let raft_batch_streams = Arc::new(Mutex::new(HashMap::new()));
        let receive_handler = Arc::new(ReceiveHandler::new_with_file_transfer(
            Arc::clone(&retransmit_buffer),
            incoming_tx.clone(),
            Some(file_transfer_tx),
            Arc::clone(&metrics_ext),
        ));
        let reconnect_tx = start_seed_reconnector(
            config.seeds.clone(),
            endpoint.clone(),
            client_config.clone(),
            Arc::clone(&connections),
            Arc::clone(&receive_handler),
            Arc::clone(&peer_capabilities),
            Arc::clone(&retransmit_buffer),
            Arc::clone(&metrics_ext),
            shutdown.clone(),
            node_id,
            Arc::clone(&metrics),
            transport_config.handshake.clone(),
        );
        let backend = QuicBackend {
            node_id,
            endpoint: endpoint.clone(),
            connections,
            peer_capabilities,
            incoming_tx,
            incoming_rx: Arc::new(Mutex::new(Some(incoming_rx))),
            file_transfer_rx: Arc::new(Mutex::new(Some(file_transfer_rx))),
            shutdown,
            reconnect_tx,
            send_tx,
            send_slots,
            send_timeout: transport_config.send_timeout,
            await_peer_stop: transport_config.await_peer_stop,
            metrics: Arc::clone(&metrics),
            retransmit_buffer: Arc::clone(&retransmit_buffer),
            raft_batch_streams: Arc::clone(&raft_batch_streams),
            metrics_ext: Arc::clone(&metrics_ext),
            receive_handler: Arc::clone(&receive_handler),
            handshake_config: transport_config.handshake.clone(),
        };

        backend.spawn_accept_loop();
        spawn_send_loop(
            Arc::clone(&backend.connections),
            Arc::clone(&backend.peer_capabilities),
            Arc::clone(&metrics),
            Arc::clone(&backend.retransmit_buffer),
            Arc::clone(&backend.metrics_ext),
            Arc::clone(&backend.receive_handler),
            send_rx,
            backend.shutdown.subscribe(),
            backend.node_id,
            backend.send_timeout,
            backend.await_peer_stop,
            Arc::clone(&backend.raft_batch_streams),
            transport_config.raft_stream_batch_size,
            transport_config.priority.clone(),
        );
        let _ = backend.reconnect_tx.try_send(ReconnectCommand::Trigger);

        Ok(backend)
    }

    fn spawn_accept_loop(&self) {
        let endpoint = self.endpoint.clone();
        let connections = Arc::clone(&self.connections);
        let peer_capabilities = Arc::clone(&self.peer_capabilities);
        let receive_handler = Arc::clone(&self.receive_handler);
        let retransmit_buffer = Arc::clone(&self.retransmit_buffer);
        let metrics_ext = Arc::clone(&self.metrics_ext);
        let mut shutdown_rx = self.shutdown.subscribe();
        let local_id = self.node_id;
        let metrics = Arc::clone(&self.metrics);
        let handshake_config = self.handshake_config.clone();

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
                                        let handler = Arc::clone(&receive_handler);
                                        let peer_caps = Arc::clone(&peer_capabilities);
                                        let rt_buf = Arc::clone(&retransmit_buffer);
                                        let metrics_ext_inner = Arc::clone(&metrics_ext);
                                        let mut shutdown_rx = shutdown_rx.resubscribe();
                                        let metrics = Arc::clone(&metrics);
                                        let hs_cfg = handshake_config.clone();
                                        tokio::spawn(async move {
                                            if let Err(err) = handle_connection(
                                                connection,
                                                local_id,
                                                connections,
                                                peer_caps,
                                                handler,
                                                rt_buf,
                                                metrics_ext_inner,
                                                metrics,
                                                &mut shutdown_rx,
                                                hs_cfg,
                                            )
                                            .await
                                            {
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
            concurrent_sends: self.metrics.concurrent_sends.load(Ordering::Relaxed),
            max_concurrent_sends: self.metrics.max_concurrent_sends.load(Ordering::Relaxed),
            streams_opened: self.metrics.streams_opened.load(Ordering::Relaxed),
        }
    }

    /// Returns the detailed transport counters used by controlled diagnostics.
    pub fn extended_metrics(&self) -> MetricsSnapshot {
        self.metrics_ext.snapshot()
    }

    /// Opens a raw unidirectional stream on an established peer connection.
    ///
    /// The file-transfer codec writes its own stream discriminator and payload
    /// after this method returns.
    pub async fn open_file_transfer_stream(
        &self,
        target: NodeId,
    ) -> Result<quinn::SendStream, TransportError> {
        let connection = self
            .connections
            .read()
            .await
            .get(&target)
            .cloned()
            .ok_or_else(|| {
                TransportError::Connection(format!("peer {target:?} is not connected"))
            })?;
        connection
            .open_uni()
            .await
            .map_err(|error| TransportError::Send(error.to_string()))
    }

    /// Subscribes to incoming file-transfer chunk streams.
    ///
    /// Chunk streams have already had their one-byte stream discriminator
    /// consumed. A backend permits exactly one chunk-stream consumer.
    pub async fn subscribe_file_transfer_streams(
        &self,
    ) -> Result<mpsc::Receiver<(NodeId, RecvStream)>, TransportError> {
        self.file_transfer_rx.lock().await.take().ok_or_else(|| {
            TransportError::Subscribe("file transfer streams already subscribed".into())
        })
    }
}

#[async_trait]
impl MessageBackend for QuicBackend {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError> {
        let slot = self.send_slots.clone().try_acquire_owned().map_err(|_| {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            TransportError::Timeout("send queue is full".into())
        })?;
        let (respond_to, recv) = oneshot::channel();
        if let Err(err) = self.send_tx.try_send(SendCommand::Unicast {
            target,
            frame,
            respond_to,
            _slot: slot,
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
        let slot = self.send_slots.clone().try_acquire_owned().map_err(|_| {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            TransportError::Timeout("send queue is full".into())
        })?;
        let (respond_to, recv) = oneshot::channel();
        if let Err(err) = self.send_tx.try_send(SendCommand::Broadcast {
            frame,
            respond_to,
            _slot: slot,
        }) {
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
    peer_capabilities: Arc<RwLock<HashMap<NodeId, NegotiatedCapabilities>>>,
    receive_handler: Arc<ReceiveHandler>,
    retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
    metrics_ext: Arc<ExtendedTransportMetrics>,
    metrics: Arc<TransportCounters>,
    shutdown_rx: &mut broadcast::Receiver<()>,
    handshake_config: HandshakeConfig,
) -> Result<(), TransportError> {
    let local_msg = HandshakeMessage::new(local_id);
    time::timeout(
        handshake_config.timeout,
        send_handshake(&connection, &local_msg),
    )
    .await
    .map_err(|_| TransportError::Timeout("handshake send timed out".into()))??;
    let remote_msg = time::timeout(handshake_config.timeout, recv_handshake(&connection))
        .await
        .map_err(|_| TransportError::Timeout("handshake recv timed out".into()))??;
    if !remote_msg.is_compatible() {
        emit_event(TransportEvent::VersionMismatch {
            remote_version: remote_msg.version,
            local_version: local_msg.version,
        });
        warn!(
            peer = ?remote_msg.node_id,
            remote_version = remote_msg.version,
            local_version = local_msg.version,
            "version_mismatch"
        );
        return Err(TransportError::Connection("version mismatch".into()));
    }
    let negotiated = match negotiate(&local_msg, &remote_msg) {
        Ok(n) => n,
        Err(HandshakeError::VersionMismatch { local, remote }) => {
            emit_event(TransportEvent::VersionMismatch {
                remote_version: remote,
                local_version: local,
            });
            return Err(TransportError::Connection("version mismatch".into()));
        }
        Err(err) => {
            return Err(TransportError::Connection(format!(
                "handshake failed: {err:?}"
            )));
        }
    };
    let remote_id = remote_msg.node_id;
    let peer_label = format!("{remote_id:?}");

    connections
        .write()
        .await
        .insert(remote_id, connection.clone());
    peer_capabilities
        .write()
        .await
        .insert(remote_id, negotiated.clone());
    emit_event(TransportEvent::PeerConnected {
        node_id: peer_label.clone(),
        protocol_version: remote_msg.version,
        capabilities: negotiated.clone(),
    });
    info!(
        peer = ?remote_id,
        addr = ?connection.remote_address(),
        protocol_version = remote_msg.version,
        capabilities = ?negotiated,
        "peer_connected"
    );

    // Retransmit any buffered messages for this peer on reconnect.
    {
        let mut buf = retransmit_buffer.write().await;
        let messages = buf.drain_for_retransmit(remote_id);
        drop(buf);
        if !messages.is_empty() {
            emit_event(TransportEvent::RetransmissionStarted {
                node_id: peer_label.clone(),
                message_count: messages.len(),
            });
            let start = Instant::now();
            let mut success = 0;
            let mut failed = 0;
            let attempts = messages.len() as u64;
            let ack_seq = receive_handler.get_ack_seq_for_peer(remote_id).await;
            for msg in messages {
                let payload_len = serialized_size(&msg.frame).unwrap_or(0) as u32;
                let kind = stream_kind_for_frame(&msg.frame);
                let envelope = FrameEnvelopeV2::new(
                    kind as u8,
                    msg.seq,
                    ack_seq,
                    payload_len,
                    msg.frame.clone(),
                );
                if let Err(err) = send_envelope(&connection, envelope, true).await {
                    failed += 1;
                    warn!(peer=?remote_id, seq=msg.seq, "retransmit failed: {err}");
                } else {
                    success += 1;
                }
            }
            metrics_ext.record_retransmit(attempts, None);
            emit_event(TransportEvent::RetransmissionCompleted {
                node_id: peer_label.clone(),
                duration_ms: start.elapsed().as_millis() as u64,
                success_count: success,
                failed_count: failed,
            });
        }
    }

    loop {
        select! {
            _ = shutdown_rx.recv() => {
                let buffered = retransmit_buffer.read().await.stats(remote_id).buffered_count;
                emit_event(TransportEvent::PeerDisconnected {
                    node_id: peer_label.clone(),
                    reason: "shutdown".into(),
                    buffered_messages: buffered,
                });
                connections.write().await.remove(&remote_id);
                info!(peer = ?remote_id, "connection closed");
                break;
            }
            next = connection.accept_uni() => match next {
                Ok(recv) => {
                    let handler = Arc::clone(&receive_handler);
                    let metrics = Arc::clone(&metrics);
                    tokio::spawn(async move {
                        match handler.handle_stream(remote_id, recv).await {
                            Ok(_) => {
                                metrics.received.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(err) => {
                                warn!("failed to read stream: {err}");
                            }
                        }
                    });
                }
                Err(err) => {
                    let buffered = retransmit_buffer.read().await.stats(remote_id).buffered_count;
                    connections.write().await.remove(&remote_id);
                    emit_event(TransportEvent::PeerDisconnected {
                        node_id: peer_label.clone(),
                        reason: err.to_string(),
                        buffered_messages: buffered,
                    });
                    return Err(TransportError::Connection(err.to_string()));
                }
            },
        }
    }

    Ok(())
}

async fn send_handshake(
    connection: &Connection,
    msg: &HandshakeMessage,
) -> Result<(), TransportError> {
    send_wire_message(
        connection,
        StreamKind::Control,
        WireMessage::Handshake(msg.clone()),
    )
    .await
}

async fn recv_handshake(connection: &Connection) -> Result<HandshakeMessage, TransportError> {
    match connection.accept_uni().await {
        Ok(recv) => match read_wire_message(recv).await? {
            (StreamKind::Control, WireMessage::Handshake(msg)) => Ok(msg),
            _ => Err(TransportError::Connection(
                "unexpected message during handshake".into(),
            )),
        },
        Err(err) => Err(TransportError::Connection(err.to_string())),
    }
}

#[allow(dead_code)]
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
        .map_err(|e| TransportError::Send(e.to_string()))?;
    match stream
        .stopped()
        .await
        .map_err(|e| TransportError::Send(e.to_string()))?
    {
        Some(error_code) => Err(TransportError::Send(format!(
            "wire stream stopped by peer with code {error_code}"
        ))),
        None => Ok(()),
    }
}

async fn send_envelope(
    connection: &Connection,
    envelope: FrameEnvelopeV2,
    await_peer_stop: bool,
) -> Result<(), TransportError> {
    send_envelope_bytes(connection, envelope.encode(), await_peer_stop).await
}

async fn send_envelope_bytes(
    connection: &Connection,
    bytes: Vec<u8>,
    await_peer_stop: bool,
) -> Result<(), TransportError> {
    let mut stream = connection
        .open_uni()
        .await
        .map_err(|e| TransportError::Send(e.to_string()))?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| TransportError::Send(e.to_string()))?;
    stream
        .finish()
        .map_err(|e| TransportError::Send(e.to_string()))?;
    if await_peer_stop
        && let Some(error_code) = stream
            .stopped()
            .await
            .map_err(|e| TransportError::Send(e.to_string()))?
    {
        return Err(TransportError::Send(format!(
            "frame stream stopped by peer with code {error_code}"
        )));
    }
    Ok(())
}

/// Append ordinary Raft envelopes to a bounded temporary stream. Closing the
/// stream at the batch boundary keeps the existing stream lifecycle and
/// retransmission semantics while amortizing QUIC stream setup.
async fn send_batched_envelope(
    connection: &Connection,
    target: NodeId,
    encoded: Vec<u8>,
    streams: &Arc<Mutex<HashMap<NodeId, BatchStream>>>,
    batch_size: usize,
    streams_opened: &AtomicU64,
) -> Result<(), TransportError> {
    let encoded_len = u32::try_from(encoded.len())
        .map_err(|_| TransportError::Send("raft envelope exceeds u32 length".into()))?;
    let mut guard = streams.lock().await;
    if let std::collections::hash_map::Entry::Vacant(entry) = guard.entry(target) {
        let mut stream = connection
            .open_uni()
            .await
            .map_err(|e| TransportError::Send(e.to_string()))?;
        stream
            .write_u8(RAFT_BATCH_STREAM_MAGIC)
            .await
            .map_err(|e| TransportError::Send(e.to_string()))?;
        entry.insert(BatchStream {
            stream,
            envelopes: 0,
        });
        streams_opened.fetch_add(1, Ordering::Relaxed);
    }

    let write_result = async {
        let batch = guard
            .get_mut(&target)
            .ok_or_else(|| TransportError::Send("raft batch stream disappeared".into()))?;
        batch
            .stream
            .write_u32(encoded_len)
            .await
            .map_err(|e| TransportError::Send(e.to_string()))?;
        batch
            .stream
            .write_all(&encoded)
            .await
            .map_err(|e| TransportError::Send(e.to_string()))?;
        batch.envelopes += 1;
        Ok::<bool, TransportError>(batch.envelopes >= batch_size)
    }
    .await;

    let should_finish = match write_result {
        Ok(value) => value,
        Err(error) => {
            guard.remove(&target);
            return Err(error);
        }
    };
    if should_finish {
        let mut batch = guard
            .remove(&target)
            .ok_or_else(|| TransportError::Send("raft batch stream disappeared".into()))?;
        batch
            .stream
            .finish()
            .map_err(|e| TransportError::Send(e.to_string()))?;
    }
    Ok(())
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

#[allow(dead_code)]
async fn handle_incoming_stream(
    recv: RecvStream,
    connection: Connection,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    incoming_tx: mpsc::Sender<(NodeId, Frame)>,
    metrics: Arc<TransportCounters>,
) -> Result<(), TransportError> {
    match read_wire_message(recv).await {
        Ok((StreamKind::Control, WireMessage::Handshake(msg))) => {
            connections.write().await.insert(msg.node_id, connection);
        }
        Ok((
            StreamKind::Gossip
            | StreamKind::User
            | StreamKind::Raft
            | StreamKind::RaftSnapshot
            | StreamKind::FileTransfer,
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
        Frame::Raft(_) => StreamKind::Raft,
        Frame::RaftSnapshot(_) => StreamKind::RaftSnapshot,
        Frame::User(_) => StreamKind::User,
        Frame::FileTransfer(_) => StreamKind::FileTransfer,
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
    peer_capabilities: Arc<RwLock<HashMap<NodeId, NegotiatedCapabilities>>>,
    metrics: Arc<TransportCounters>,
    retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
    metrics_ext: Arc<ExtendedTransportMetrics>,
    receive_handler: Arc<ReceiveHandler>,
    mut rx: mpsc::Receiver<SendCommand>,
    mut shutdown_rx: broadcast::Receiver<()>,
    node_id: NodeId,
    timeout: Duration,
    await_peer_stop: bool,
    raft_batch_streams: Arc<Mutex<HashMap<NodeId, BatchStream>>>,
    raft_stream_batch_size: usize,
    priority_config: PriorityConfig,
) {
    tokio::spawn(async move {
        let mut scheduler = PriorityScheduler::new(SchedulerConfig {
            weights: priority_config.weights,
            quantum_bytes: MAX_FRAME_SIZE,
        });
        let mut in_flight = tokio::task::JoinSet::new();
        loop {
            while in_flight.len() < MAX_CONCURRENT_SENDS
                && let Some(command) = scheduler.dequeue()
            {
                let connections = Arc::clone(&connections);
                let peer_capabilities = Arc::clone(&peer_capabilities);
                let metrics = Arc::clone(&metrics);
                let retransmit_buffer = Arc::clone(&retransmit_buffer);
                let metrics_ext = Arc::clone(&metrics_ext);
                let receive_handler = Arc::clone(&receive_handler);
                let raft_batch_streams = Arc::clone(&raft_batch_streams);
                in_flight.spawn(async move {
                    let _concurrency = SendConcurrencyGuard::enter(Arc::clone(&metrics));
                    match command.payload {
                        SendCommand::Unicast {
                            target,
                            frame,
                            respond_to,
                            ..
                        } => {
                            let send_res = send_with_retry(
                                &connections,
                                &peer_capabilities,
                                &metrics,
                                &retransmit_buffer,
                                &metrics_ext,
                                &receive_handler,
                                &raft_batch_streams,
                                node_id,
                                target,
                                frame,
                                timeout,
                                await_peer_stop,
                                raft_stream_batch_size,
                            )
                            .await;
                            let _ = respond_to.send(send_res);
                        }
                        SendCommand::Broadcast {
                            frame, respond_to, ..
                        } => {
                            let send_res = broadcast_with_retry(
                                &connections,
                                &peer_capabilities,
                                &metrics,
                                &retransmit_buffer,
                                &metrics_ext,
                                &receive_handler,
                                &raft_batch_streams,
                                node_id,
                                frame,
                                timeout,
                                await_peer_stop,
                                raft_stream_batch_size,
                            )
                            .await;
                            let _ = respond_to.send(send_res);
                        }
                    }
                });
            }

            let command = select! {
                _ = shutdown_rx.recv() => {
                    in_flight.shutdown().await;
                    break;
                },
                result = in_flight.join_next(), if !in_flight.is_empty() => {
                    if let Some(Err(err)) = result {
                        warn!("send task failed: {err}");
                    }
                    continue;
                },
                command = rx.recv() => command,
            };
            let Some(command) = command else {
                while let Some(result) = in_flight.join_next().await {
                    if let Err(err) = result {
                        warn!("send task failed while draining: {err}");
                    }
                }
                break;
            };
            enqueue_send_command(&mut scheduler, command, priority_config.enabled);

            // Batch commands that were woken in the same scheduler turn before
            // selecting one.  This lets a control/gossip frame overtake queued
            // low-priority user traffic without preempting an active QUIC write.
            tokio::task::yield_now().await;
            while let Ok(command) = rx.try_recv() {
                enqueue_send_command(&mut scheduler, command, priority_config.enabled);
            }
        }
    });
}

fn enqueue_send_command(
    scheduler: &mut PriorityScheduler<SendCommand>,
    command: SendCommand,
    priority_enabled: bool,
) {
    let size_bytes = serialized_size(command.frame())
        .unwrap_or_default()
        .min(usize::MAX as u64) as usize;
    let priority = if priority_enabled {
        stream_kind_for_frame(command.frame()).priority()
    } else {
        Priority::Normal
    };
    scheduler.enqueue(
        ScheduledMessage::new(priority, size_bytes, command),
        priority,
    );
}

async fn send_with_retry(
    connections: &Arc<RwLock<HashMap<NodeId, Connection>>>,
    peer_capabilities: &Arc<RwLock<HashMap<NodeId, NegotiatedCapabilities>>>,
    metrics: &Arc<TransportCounters>,
    retransmit_buffer: &Arc<RwLock<RetransmissionBuffer>>,
    metrics_ext: &Arc<ExtendedTransportMetrics>,
    receive_handler: &Arc<ReceiveHandler>,
    raft_batch_streams: &Arc<Mutex<HashMap<NodeId, BatchStream>>>,
    node_id: NodeId,
    target: NodeId,
    frame: Frame,
    timeout: Duration,
    await_peer_stop: bool,
    raft_stream_batch_size: usize,
) -> Result<(), TransportError> {
    let mut attempts = 0;
    loop {
        let frame_clone = frame.clone();
        match time::timeout(
            timeout,
            send_to_peer(
                connections,
                peer_capabilities,
                metrics,
                retransmit_buffer,
                metrics_ext,
                receive_handler,
                raft_batch_streams,
                node_id,
                target,
                frame_clone,
                await_peer_stop,
                raft_stream_batch_size,
            ),
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
    peer_capabilities: &Arc<RwLock<HashMap<NodeId, NegotiatedCapabilities>>>,
    metrics: &Arc<TransportCounters>,
    retransmit_buffer: &Arc<RwLock<RetransmissionBuffer>>,
    metrics_ext: &Arc<ExtendedTransportMetrics>,
    receive_handler: &Arc<ReceiveHandler>,
    raft_batch_streams: &Arc<Mutex<HashMap<NodeId, BatchStream>>>,
    node_id: NodeId,
    frame: Frame,
    timeout: Duration,
    await_peer_stop: bool,
    raft_stream_batch_size: usize,
) -> Result<usize, TransportError> {
    let mut attempts = 0;
    loop {
        let frame_clone = frame.clone();
        match time::timeout(
            timeout,
            broadcast_to_peers(
                connections,
                peer_capabilities,
                metrics,
                retransmit_buffer,
                metrics_ext,
                receive_handler,
                raft_batch_streams,
                node_id,
                frame_clone,
                await_peer_stop,
                raft_stream_batch_size,
            ),
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

fn ensure_capabilities(
    kind: StreamKind,
    caps: &NegotiatedCapabilities,
) -> Result<(), TransportError> {
    if matches!(kind, StreamKind::Raft | StreamKind::RaftSnapshot) && !caps.priority_streams {
        return Err(TransportError::Connection(
            "peer lacks priority_streams capability".into(),
        ));
    }
    if kind.requires_ack() && !caps.retransmission {
        return Err(TransportError::Connection(
            "peer lacks retransmission capability".into(),
        ));
    }
    if !caps.qos && !matches!(kind, StreamKind::Control) {
        return Err(TransportError::Connection(
            "peer lacks qos capability".into(),
        ));
    }
    Ok(())
}

async fn send_to_peer(
    connections: &Arc<RwLock<HashMap<NodeId, Connection>>>,
    peer_capabilities: &Arc<RwLock<HashMap<NodeId, NegotiatedCapabilities>>>,
    metrics: &Arc<TransportCounters>,
    retransmit_buffer: &Arc<RwLock<RetransmissionBuffer>>,
    metrics_ext: &Arc<ExtendedTransportMetrics>,
    receive_handler: &Arc<ReceiveHandler>,
    raft_batch_streams: &Arc<Mutex<HashMap<NodeId, BatchStream>>>,
    _node_id: NodeId,
    target: NodeId,
    frame: Frame,
    await_peer_stop: bool,
    raft_stream_batch_size: usize,
) -> Result<(), TransportError> {
    let conn = {
        let map = connections.read().await;
        map.get(&target)
            .cloned()
            .ok_or_else(|| TransportError::Connection(format!("peer {target:?} not connected")))?
    };
    let kind = stream_kind_for_frame(&frame);
    let caps = {
        let map = peer_capabilities.read().await;
        map.get(&target).cloned().ok_or_else(|| {
            TransportError::Connection(format!("peer {target:?} capabilities unknown"))
        })?
    };
    ensure_capabilities(kind, &caps)?;
    let frame_body = serialize(&frame).map_err(|e| TransportError::Send(e.to_string()))?;
    let payload_len = u32::try_from(frame_body.len())
        .map_err(|_| TransportError::Send("frame exceeds u32 payload length".into()))?;
    let seq = {
        let mut buf = retransmit_buffer.write().await;
        match buf.buffer_with_size(target, frame.clone(), frame_body.len()) {
            Ok(seq) => seq,
            Err(e) => {
                return Err(TransportError::Send(format!(
                    "retransmit buffer error: {e:?}"
                )));
            }
        }
    };
    let ack_seq = receive_handler.get_ack_seq_for_peer(target).await;
    let envelope = alopex_chirps_wire::envelope::FrameEnvelopeV2::new(
        kind as u8,
        seq,
        ack_seq,
        payload_len,
        frame,
    );
    let encoded = envelope.encode_with_payload(&frame_body);
    let start = tokio::time::Instant::now();
    let res = if kind == StreamKind::Raft && raft_stream_batch_size > 1 {
        send_batched_envelope(
            &conn,
            target,
            encoded,
            raft_batch_streams,
            raft_stream_batch_size,
            &metrics.streams_opened,
        )
        .await
    } else {
        metrics.streams_opened.fetch_add(1, Ordering::Relaxed);
        send_envelope_bytes(&conn, encoded, await_peer_stop).await
    };
    if let Ok(()) = res {
        let elapsed = start.elapsed();
        metrics.sent.fetch_add(1, Ordering::Relaxed);
        metrics_ext.record_send(kind, Some(elapsed.as_micros() as u64));
    }
    res
}

async fn broadcast_to_peers(
    connections: &Arc<RwLock<HashMap<NodeId, Connection>>>,
    peer_capabilities: &Arc<RwLock<HashMap<NodeId, NegotiatedCapabilities>>>,
    metrics: &Arc<TransportCounters>,
    retransmit_buffer: &Arc<RwLock<RetransmissionBuffer>>,
    metrics_ext: &Arc<ExtendedTransportMetrics>,
    receive_handler: &Arc<ReceiveHandler>,
    raft_batch_streams: &Arc<Mutex<HashMap<NodeId, BatchStream>>>,
    node_id: NodeId,
    frame: Frame,
    await_peer_stop: bool,
    raft_stream_batch_size: usize,
) -> Result<usize, TransportError> {
    let peers: Vec<(NodeId, Connection)> = {
        let map = connections.read().await;
        map.iter().map(|(id, conn)| (*id, conn.clone())).collect()
    };
    let mut sent = 0;
    for (peer_id, _conn) in peers {
        if let Err(err) = send_to_peer(
            connections,
            peer_capabilities,
            metrics,
            retransmit_buffer,
            metrics_ext,
            receive_handler,
            raft_batch_streams,
            node_id,
            peer_id,
            frame.clone(),
            await_peer_stop,
            raft_stream_batch_size,
        )
        .await
        {
            warn!("broadcast send failed: {err}");
        } else {
            sent += 1;
        }
    }
    Ok(sent)
}

fn build_tls_configs(config: &NodeConfig) -> anyhow::Result<(ServerConfig, ClientConfig)> {
    // Workspace-wide feature resolution can enable more than one rustls
    // crypto provider, which makes automatic process-level selection panic.
    // Chirps pins this transport to ring, so select it explicitly. Repeated
    // calls are harmless when another test has already installed a provider.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (cert_der, key_der) = if let (Some(cert_path), Some(key_path)) =
        (config.cert_path.as_ref(), config.key_path.as_ref())
    {
        (fs::read(cert_path)?, fs::read(key_path)?)
    } else {
        let cert = generate_simple_self_signed([DEFAULT_SERVER_NAME.to_string()])?;
        (cert.serialize_der()?, cert.serialize_private_key_der())
    };

    let cert_chain = vec![CertificateDer::from(cert_der.clone())];
    let priv_key = PrivatePkcs8KeyDer::from(key_der).into();
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, priv_key)?;
    server_crypto.alpn_protocols = vec![b"alopex".to_vec()];
    let server_config =
        ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));

    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(cert_der))
        .map_err(|_| anyhow::anyhow!("failed to add root cert"))?;
    for cert_path in &config.trusted_cert_paths {
        let trusted_cert = fs::read(cert_path)?;
        roots
            .add(CertificateDer::from(trusted_cert))
            .map_err(|_| anyhow::anyhow!("failed to add trusted root cert"))?;
    }

    let mut client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"alopex".to_vec()];

    let client_config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    Ok((server_config, client_config))
}
