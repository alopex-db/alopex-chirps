use crate::backend::MessageBackend;
use crate::config::NodeConfig;
use crate::error::{MeshError, TransportError};
use crate::memory::{MemoryConfig, MemoryError, MemoryManager, MemoryStats};
#[cfg(feature = "multi-raft")]
use crate::multi_raft::{MultiRaftManager, RaftStorageFactory};
use crate::node_id::{NodeId, load_or_create_node_id};
use crate::profile::{EnvelopeMetadata, MessageProfile, resolve_profile};
#[cfg(feature = "multi-raft")]
use crate::raft::ChirpsRaftTransport;
#[cfg(feature = "tso")]
use crate::tso::TsoService;
use alopex_chirps_file_transfer::{
    ChunkStreamOpener, FileTransferConfig, FileTransferError, FileTransferServiceImpl,
};
use alopex_chirps_gossip_swim::engine::{
    GossipConfig, GossipEngine, Transport as GossipTransport,
    TransportError as GossipTransportError,
};
#[cfg(feature = "hlc")]
use alopex_chirps_gossip_swim::hlc::LocalHlc;
use alopex_chirps_gossip_swim::types::{MembershipView, Status};
use alopex_chirps_gossip_swim::util::StatusChange;
use alopex_chirps_transport_quic::QuicBackend;
use alopex_chirps_wire::frame::Frame;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::info;

/// メッシュへ外部から操作するためのハンドル。
#[derive(Clone)]
pub struct MeshHandle {
    inner: Arc<Mesh>,
}

impl MeshHandle {
    /// Creates the Raft transport adapter over this mesh's QUIC backend.
    #[cfg(feature = "multi-raft")]
    pub fn raft_transport(&self, raft_node_id: u64) -> Arc<ChirpsRaftTransport> {
        let transport = Arc::new(ChirpsRaftTransport::new(
            Arc::clone(&self.inner.backend),
            alopex_chirps_raft_storage::types::GroupId(0),
            raft_node_id,
        ));
        transport.register_node_id(raft_node_id, self.inner.node_id);
        transport
    }

    /// Installs the manager in the mesh receive path. Raft frames are then
    /// dispatched internally; callers do not need to run a subscriber loop.
    #[cfg(feature = "multi-raft")]
    pub fn attach_raft_manager<F>(&self, manager: Arc<MultiRaftManager<F>>)
    where
        F: RaftStorageFactory,
    {
        manager.register_node_id(manager.node_id(), self.inner.node_id);

        let identity_manager = Arc::clone(&manager);
        self.inner
            .raft_identity_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::new(move |wire_node_id, raft_node_id| {
                identity_manager.register_node_id(raft_node_id, wire_node_id);
            }));

        let join_mesh = self.clone();
        let join_identity = manager.identity_frame();
        self.on_node_join(move |peer| {
            let mesh = join_mesh.clone();
            let frame = join_identity.clone();
            tokio::spawn(async move {
                if let Err(error) = mesh.send_to(peer, frame).await {
                    tracing::debug!(peer = ?peer, %error, "Raft identity advertisement failed");
                }
            });
        });

        let announce_mesh = self.clone();
        let announce_identity = manager.identity_frame();
        tokio::spawn(async move {
            let _ = announce_mesh.broadcast(announce_identity).await;
        });

        self.inner
            .raft_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(manager);
    }

    /// Installs a TSO service in the mesh receive path.
    #[cfg(feature = "tso")]
    pub async fn attach_tso_service(&self, service: Arc<TsoService>) -> Result<(), MeshError> {
        let mut receiver = self.subscribe().await?;
        let mesh = self.clone();
        tokio::spawn(async move {
            while let Some((from, frame)) = receiver.recv().await {
                if let Some(response) = service.handle_frame(from, frame).await
                    && let Err(error) = mesh.send_to(from, response).await
                {
                    tracing::warn!(peer = ?from, %error, "TSO response send failed");
                }
            }
        });
        Ok(())
    }

    /// Dynamically changes the logical memory budget for this node.
    pub fn resize_memory_budget(&self, new_budget: usize) -> Result<(), MemoryError> {
        self.inner.memory.resize_memory_budget(new_budget)
    }

    /// Returns the current Chirps-owned memory accounting snapshot.
    pub fn get_memory_stats(&self) -> MemoryStats {
        self.inner.memory.get_memory_stats()
    }

    /// Applies configured caps and synchronously releases accounted cache usage.
    pub fn trigger_gc(&self) -> Result<(), MemoryError> {
        self.inner.memory.trigger_gc()
    }

    /// 指定したピアへフレームを送信する。
    pub async fn send_to(&self, target: NodeId, frame: Frame) -> Result<(), MeshError> {
        self.send_to_with_profile(target, frame, MessageProfile::Control)
            .await
    }

    /// プロファイル付きで送信する。
    pub async fn send_to_with_profile(
        &self,
        target: NodeId,
        frame: Frame,
        profile: MessageProfile,
    ) -> Result<(), MeshError> {
        self.send_enveloped(target, frame, profile, EnvelopeMetadata::default())
            .await
    }

    /// Profile-aware extension point carrying metadata reserved for Durable backends.
    pub async fn send_enveloped(
        &self,
        target: NodeId,
        frame: Frame,
        profile: MessageProfile,
        metadata: EnvelopeMetadata,
    ) -> Result<(), MeshError> {
        let effective = resolve_profile(&frame, profile, self.inner.backend.capabilities())?;
        self.inner
            .backend
            .send_with_profile(target, frame, effective, metadata)
            .await
            .map_err(MeshError::from)
    }

    /// 接続済みの全ピアへフレームをブロードキャストする。
    pub async fn broadcast(&self, frame: Frame) -> Result<usize, MeshError> {
        self.broadcast_with_profile(frame, MessageProfile::Control)
            .await
    }

    /// プロファイル付きでブロードキャストする。
    pub async fn broadcast_with_profile(
        &self,
        frame: Frame,
        profile: MessageProfile,
    ) -> Result<usize, MeshError> {
        self.broadcast_enveloped(frame, profile, EnvelopeMetadata::default())
            .await
    }

    /// Broadcast extension point carrying reserved acknowledgement/replay metadata.
    pub async fn broadcast_enveloped(
        &self,
        frame: Frame,
        profile: MessageProfile,
        metadata: EnvelopeMetadata,
    ) -> Result<usize, MeshError> {
        let effective = resolve_profile(&frame, profile, self.inner.backend.capabilities())?;
        self.inner
            .backend
            .broadcast_with_profile(frame, effective, metadata)
            .await
            .map_err(MeshError::from)
    }

    /// 受信フレーム購読を開始し、`(from, frame)` を受け取るチャネルを返す。
    pub async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, MeshError> {
        let (sender, receiver) = mpsc::channel(1024);
        self.inner.frame_subscribers.lock().unwrap().push(sender);
        Ok(receiver)
    }

    /// Creates the file-transfer service attached to this mesh.
    ///
    /// The mesh keeps ownership of the transport subscription and supplies the
    /// service with demultiplexed control frames and QUIC chunk streams.
    pub async fn file_transfer(
        &self,
        config: FileTransferConfig,
    ) -> Result<FileTransferServiceImpl, FileTransferError> {
        if config.max_concurrent_transfers == 0 {
            return Err(FileTransferError::Internal(
                "max_concurrent_transfers must be greater than zero".into(),
            ));
        }

        let chunk_receiver = self
            .inner
            .quic_backend
            .subscribe_file_transfer_streams()
            .await
            .map_err(|error| FileTransferError::Transport(error.to_string()))?;
        let frame_receiver = self
            .subscribe()
            .await
            .map_err(|error| FileTransferError::Transport(error.to_string()))?;
        let opener: Arc<dyn ChunkStreamOpener> = Arc::new(MeshChunkStreamOpener {
            backend: Arc::clone(&self.inner.quic_backend),
        });
        let service = FileTransferServiceImpl::new_with_receiver(
            self.inner.node_id,
            Arc::clone(&self.inner.backend),
            opener,
            config,
            frame_receiver,
        )
        .await?;

        spawn_file_transfer_stream_loop(
            chunk_receiver,
            service.receive_handler(),
            service.control(),
        );
        Ok(service)
    }

    /// ノード参加イベントハンドラを登録する。
    pub fn on_node_join<F>(&self, handler: F)
    where
        F: Fn(NodeId) + Send + Sync + 'static,
    {
        self.inner
            .join_handlers
            .lock()
            .unwrap()
            .push(Arc::new(handler));
    }

    /// ノード離脱イベントハンドラを登録する。
    pub fn on_node_leave<F>(&self, handler: F)
    where
        F: Fn(NodeId) + Send + Sync + 'static,
    {
        self.inner
            .leave_handlers
            .lock()
            .unwrap()
            .push(Arc::new(handler));
    }

    /// ステータス変更イベントハンドラを登録する。
    pub fn on_status_change<F>(&self, handler: F)
    where
        F: Fn(NodeId) + Send + Sync + 'static,
    {
        self.inner
            .status_handlers
            .lock()
            .unwrap()
            .push(Arc::new(handler));
    }

    /// 自ノードの `NodeId` を返す。
    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    /// 現在のメンバーシップビューを取得する。
    pub async fn membership(&self) -> MembershipView {
        let gossip = self.inner.gossip.lock().await;
        gossip.membership()
    }

    /// メッシュ内部のメトリクスをスナップショットで返す。
    pub fn metrics(&self) -> MeshMetricsSnapshot {
        self.inner.metrics_snapshot()
    }

    /// ノードのインカーネーション番号（永続化されたNodeIdに対応）を返す。
    pub fn incarnation(&self) -> u64 {
        self.inner.incarnation
    }

    /// Mesh 起動時に使用した設定を共有参照で返す。
    pub fn config(&self) -> Arc<NodeConfig> {
        Arc::clone(&self.inner.config)
    }
}

/// QUICトランスポートとSWIMゴシップを束ねるメッシュ本体。
type NodeHandler = Arc<dyn Fn(NodeId) + Send + Sync>;

#[cfg(feature = "multi-raft")]
type RaftIdentityHandler = Arc<dyn Fn(NodeId, u64) + Send + Sync>;

#[cfg(feature = "multi-raft")]
#[async_trait::async_trait]
pub(crate) trait RaftFrameHandler: Send + Sync {
    async fn handle_raft_frame(
        &self,
        source: NodeId,
        destination: NodeId,
        frame: Frame,
    ) -> Result<(), String>;
}

pub struct Mesh {
    pub(crate) node_id: NodeId,
    pub(crate) incarnation: u64,
    pub(crate) config: Arc<NodeConfig>,
    pub(crate) backend: Arc<dyn MessageBackend>,
    quic_backend: Arc<QuicBackend>,
    gossip: Arc<Mutex<GossipEngine>>,
    frame_subscribers: std::sync::Mutex<Vec<mpsc::Sender<(NodeId, Frame)>>>,
    #[cfg(feature = "multi-raft")]
    raft_handlers: std::sync::Mutex<Vec<Arc<dyn RaftFrameHandler>>>,
    #[cfg(feature = "multi-raft")]
    raft_identity_handlers: std::sync::Mutex<Vec<RaftIdentityHandler>>,
    join_handlers: std::sync::Mutex<Vec<NodeHandler>>,
    leave_handlers: std::sync::Mutex<Vec<NodeHandler>>,
    status_handlers: std::sync::Mutex<Vec<NodeHandler>>,
    _tasks: Vec<JoinHandle<()>>,
    metrics: Arc<MeshMetrics>,
    memory: Arc<MemoryManager>,
}

#[derive(Default)]
struct MeshMetrics {
    joins: AtomicU64,
    leaves: AtomicU64,
    status_events: AtomicU64,
    delivered_frames: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct MeshMetricsSnapshot {
    pub joins: u64,
    pub leaves: u64,
    pub status_events: u64,
    pub delivered_frames: u64,
}

impl Mesh {
    /// 指定された設定でメッシュを起動する。NodeId永続化→トランスポート→ゴシップの順に初期化する。
    pub async fn start(config: NodeConfig) -> Result<MeshHandle, MeshError> {
        #[cfg(feature = "hlc")]
        {
            Self::start_inner(config, None).await
        }
        #[cfg(not(feature = "hlc"))]
        {
            Self::start_inner(config).await
        }
    }

    /// Starts a mesh with HLC events wired to the shared Prometheus registry.
    #[cfg(feature = "hlc")]
    pub async fn start_with_metrics(
        config: NodeConfig,
        metrics: Arc<crate::raft::ChirpsMetricsCollector>,
    ) -> Result<MeshHandle, MeshError> {
        Self::start_inner(config, Some(metrics)).await
    }

    async fn start_inner(
        config: NodeConfig,
        #[cfg(feature = "hlc")] metrics: Option<Arc<crate::raft::ChirpsMetricsCollector>>,
    ) -> Result<MeshHandle, MeshError> {
        let config = Arc::new(config);
        let (node_id, incarnation) = load_or_create_node_id(&config.node_id_path)?;

        let quic_backend = Arc::new(
            QuicBackend::new(node_id, Arc::clone(&config))
                .await
                .map_err(|e| TransportError::Connection(e.to_string()))?,
        );
        let backend: Arc<dyn MessageBackend> = quic_backend.clone();

        let gossip_backend: Arc<dyn GossipTransport> = Arc::new(BackendAdapter {
            inner: Arc::clone(&backend),
        });

        let gossip_cfg = GossipConfig {
            ping_timeout: config.ping_timeout,
            indirect_ping_timeout: config.indirect_ping_timeout,
            suspect_to_dead_timeout: config.suspect_to_dead_timeout,
            gossip_interval: config.gossip_interval,
            fanout: config.fanout,
        };

        let membership = MembershipView::new();
        #[cfg(not(feature = "hlc"))]
        let gossip = Arc::new(Mutex::new(GossipEngine::new(
            node_id,
            gossip_cfg,
            gossip_backend,
            membership,
        )));
        #[cfg(feature = "hlc")]
        let local_hlc = match metrics {
            Some(metrics) => LocalHlc::with_metrics(config.max_clock_skew, metrics),
            None => LocalHlc::new(config.max_clock_skew),
        };
        #[cfg(feature = "hlc")]
        let gossip = Arc::new(Mutex::new(GossipEngine::new_with_hlc(
            node_id,
            gossip_cfg,
            gossip_backend,
            membership,
            local_hlc,
        )));

        let mesh = Arc::new(Mesh {
            node_id,
            incarnation,
            config,
            backend,
            quic_backend,
            gossip,
            frame_subscribers: std::sync::Mutex::new(Vec::new()),
            #[cfg(feature = "multi-raft")]
            raft_handlers: std::sync::Mutex::new(Vec::new()),
            #[cfg(feature = "multi-raft")]
            raft_identity_handlers: std::sync::Mutex::new(Vec::new()),
            join_handlers: std::sync::Mutex::new(Vec::new()),
            leave_handlers: std::sync::Mutex::new(Vec::new()),
            status_handlers: std::sync::Mutex::new(Vec::new()),
            _tasks: Vec::new(),
            metrics: Arc::new(MeshMetrics::default()),
            memory: Arc::new(
                MemoryManager::new(MemoryConfig::default()).expect("default memory config"),
            ),
        });

        let tasks = vec![
            spawn_tick_loop(Arc::clone(&mesh)),
            spawn_frame_loop(Arc::clone(&mesh)),
        ];

        if let Some(inner) = Arc::get_mut(&mut Arc::clone(&mesh)) {
            inner._tasks = tasks;
        }

        Ok(MeshHandle { inner: mesh })
    }

    fn emit_status(&self, change: StatusChange) {
        self.metrics.status_events.fetch_add(1, Ordering::Relaxed);
        if change.to == Status::Alive && change.from != Status::Alive {
            self.metrics.joins.fetch_add(1, Ordering::Relaxed);
            for h in self.join_handlers.lock().unwrap().iter() {
                h(change.node_id);
            }
        }
        if change.to == Status::Dead {
            self.metrics.leaves.fetch_add(1, Ordering::Relaxed);
            for h in self.leave_handlers.lock().unwrap().iter() {
                h(change.node_id);
            }
        }
        info!(
            peer = ?change.node_id,
            from = ?change.from,
            to = ?change.to,
            "status change emitted"
        );
        for h in self.status_handlers.lock().unwrap().iter() {
            h(change.node_id);
        }
    }

    fn metrics_snapshot(&self) -> MeshMetricsSnapshot {
        MeshMetricsSnapshot {
            joins: self.metrics.joins.load(Ordering::Relaxed),
            leaves: self.metrics.leaves.load(Ordering::Relaxed),
            status_events: self.metrics.status_events.load(Ordering::Relaxed),
            delivered_frames: self.metrics.delivered_frames.load(Ordering::Relaxed),
        }
    }
}

fn spawn_tick_loop(mesh: Arc<Mesh>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let changes = {
                let mut gossip = mesh.gossip.lock().await;
                gossip.tick().await.unwrap_or_default()
            };
            for change in changes {
                mesh.emit_status(change);
            }
        }
    })
}

fn spawn_frame_loop(mesh: Arc<Mesh>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut rx = match mesh.backend.subscribe().await {
            Ok(rx) => rx,
            Err(err) => {
                eprintln!("subscribe failed: {err}");
                return;
            }
        };
        while let Some((from, frame)) = rx.recv().await {
            mesh.metrics
                .delivered_frames
                .fetch_add(1, Ordering::Relaxed);
            let addr = mesh
                .backend
                .connected_peers()
                .into_iter()
                .find(|(id, _)| *id == from)
                .map(|(_, addr)| addr)
                .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
            match frame {
                Frame::Ping { seq, .. } => {
                    let mut gossip = mesh.gossip.lock().await;
                    gossip.handle_ping(from, seq, addr).await;
                }
                Frame::Ack { seq, .. } => {
                    let mut gossip = mesh.gossip.lock().await;
                    gossip.handle_ack(from, seq, addr);
                }
                Frame::PingReq {
                    seq,
                    from: requester,
                    target,
                } => {
                    let mut gossip = mesh.gossip.lock().await;
                    gossip.handle_ping_req(requester, seq, target, addr).await;
                }
                Frame::Gossip(msg) => {
                    let mut gossip = mesh.gossip.lock().await;
                    gossip.apply_membership_update(&msg.updates);
                }
                #[cfg(feature = "hlc")]
                Frame::HlcGossip(msg) => {
                    let mut gossip = mesh.gossip.lock().await;
                    if let Err(error) = gossip.apply_hlc_gossip(&msg) {
                        tracing::warn!(peer = ?from, %error, "rejected HLC gossip message");
                    }
                }
                Frame::User(message) => {
                    #[cfg(feature = "multi-raft")]
                    if let Some(raft_node_id) =
                        ChirpsRaftTransport::decode_identity_frame(&Frame::User(message.clone()))
                    {
                        let handlers = mesh
                            .raft_identity_handlers
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone();
                        for handler in handlers {
                            handler(from, raft_node_id);
                        }
                        continue;
                    }

                    let subscribers = {
                        let mut subscribers = mesh.frame_subscribers.lock().unwrap();
                        subscribers.retain(|sender| !sender.is_closed());
                        subscribers.clone()
                    };
                    for subscriber in subscribers {
                        let _ = subscriber.send((from, Frame::User(message.clone()))).await;
                    }
                }
                frame => {
                    #[cfg(feature = "multi-raft")]
                    if matches!(frame, Frame::Raft(_) | Frame::RaftSnapshot(_)) {
                        let handlers = mesh
                            .raft_handlers
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone();
                        if !handlers.is_empty() {
                            for handler in handlers {
                                if let Err(error) = handler
                                    .handle_raft_frame(from, mesh.node_id, frame.clone())
                                    .await
                                {
                                    tracing::warn!(peer = ?from, %error, "Raft frame dispatch failed");
                                }
                            }
                            continue;
                        }
                    }
                    let subscribers = {
                        let mut subscribers = mesh.frame_subscribers.lock().unwrap();
                        subscribers.retain(|sender| !sender.is_closed());
                        subscribers.clone()
                    };
                    for subscriber in subscribers {
                        let _ = subscriber.send((from, frame.clone())).await;
                    }
                }
            }
        }
    })
}

struct MeshChunkStreamOpener {
    backend: Arc<QuicBackend>,
}

#[async_trait::async_trait]
impl ChunkStreamOpener for MeshChunkStreamOpener {
    async fn open_chunk_stream(
        &self,
        target: NodeId,
    ) -> Result<quinn::SendStream, FileTransferError> {
        self.backend
            .open_file_transfer_stream(target)
            .await
            .map_err(|error| FileTransferError::Transport(error.to_string()))
    }
}

fn spawn_file_transfer_stream_loop(
    mut receiver: mpsc::Receiver<(NodeId, quinn::RecvStream)>,
    receive_handler: Arc<alopex_chirps_file_transfer::ops::ReceiveHandler>,
    control: Arc<alopex_chirps_file_transfer::ControlDispatcher>,
) {
    let receive_handler = Arc::downgrade(&receive_handler);
    let control = Arc::downgrade(&control);
    tokio::spawn(async move {
        while let Some((sender, mut stream)) = receiver.recv().await {
            let (Some(receive_handler), Some(control)) =
                (receive_handler.upgrade(), control.upgrade())
            else {
                break;
            };
            if let Err(error) = receive_handler
                .handle_chunk_stream(sender, &control, &mut stream)
                .await
            {
                tracing::warn!(peer = ?sender, %error, "file transfer chunk stream failed");
            }
        }
    });
}

/// Adapter to reuse the existing MessageBackend inside the gossip engine.
struct BackendAdapter {
    inner: Arc<dyn MessageBackend>,
}

#[async_trait::async_trait]
impl GossipTransport for BackendAdapter {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), GossipTransportError> {
        self.inner
            .send(target, frame)
            .await
            .map_err(|e| GossipTransportError::Send(e.to_string()))
    }

    async fn broadcast(&self, frame: Frame) -> Result<usize, GossipTransportError> {
        self.inner
            .broadcast(frame)
            .await
            .map_err(|e| GossipTransportError::Send(e.to_string()))
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, GossipTransportError> {
        self.inner
            .subscribe()
            .await
            .map_err(|e| GossipTransportError::Subscribe(e.to_string()))
    }

    async fn close(&self) -> Result<(), GossipTransportError> {
        self.inner
            .close()
            .await
            .map_err(|e| GossipTransportError::Io(e.to_string()))
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        self.inner.connected_peers()
    }
}
