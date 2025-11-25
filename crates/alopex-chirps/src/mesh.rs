use crate::backend::MessageBackend;
use crate::config::NodeConfig;
use crate::error::{MeshError, TransportError};
use crate::node_id::{load_or_create_node_id, NodeId};
use chirps_gossip_swim::engine::{GossipConfig, GossipEngine, Transport as GossipTransport, TransportError as GossipTransportError};
use chirps_gossip_swim::types::{MembershipView, Status};
use chirps_gossip_swim::util::StatusChange;
use chirps_transport_quic::QuicBackend;
use chirps_wire::frame::Frame;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

/// Public handle for interacting with the mesh.
#[derive(Clone)]
pub struct MeshHandle {
    inner: Arc<Mesh>,
}

impl MeshHandle {
    /// Sends a frame to a specific peer.
    pub async fn send_to(&self, target: NodeId, frame: Frame) -> Result<(), MeshError> {
        self.inner
            .backend
            .send(target, frame)
            .await
            .map_err(MeshError::from)
    }

    /// Broadcasts a frame to connected peers.
    pub async fn broadcast(&self, frame: Frame) -> Result<usize, MeshError> {
        self.inner
            .backend
            .broadcast(frame)
            .await
            .map_err(MeshError::from)
    }

    /// Subscribes to incoming frames; returns a receiver of `(from, frame)` tuples.
    pub async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, MeshError> {
        self.inner
            .backend
            .subscribe()
            .await
            .map_err(MeshError::from)
    }

    /// Registers a node join handler.
    pub fn on_node_join<F>(&self, handler: F)
    where
        F: Fn(NodeId) + Send + Sync + 'static,
    {
        self.inner.join_handlers.lock().unwrap().push(Arc::new(handler));
    }

    /// Registers a node leave handler.
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

    /// Registers a status change handler.
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
}

/// Mesh orchestrates transport and gossip subsystems.
pub struct Mesh {
    pub(crate) node_id: NodeId,
    pub(crate) incarnation: u64,
    pub(crate) config: Arc<NodeConfig>,
    pub(crate) backend: Arc<dyn MessageBackend>,
    gossip: Arc<Mutex<GossipEngine>>,
    join_handlers: std::sync::Mutex<Vec<Arc<dyn Fn(NodeId) + Send + Sync>>>,
    leave_handlers: std::sync::Mutex<Vec<Arc<dyn Fn(NodeId) + Send + Sync>>>,
    status_handlers: std::sync::Mutex<Vec<Arc<dyn Fn(NodeId) + Send + Sync>>>,
    _tasks: Vec<JoinHandle<()>>,
}

impl Mesh {
    /// Starts the mesh system with the provided configuration.
    pub async fn start(config: NodeConfig) -> Result<MeshHandle, MeshError> {
        let config = Arc::new(config);
        let (node_id, incarnation) = load_or_create_node_id(&config.node_id_path)?;

        let backend = Arc::new(
            QuicBackend::new(node_id, Arc::clone(&config))
                .await
                .map_err(|e| TransportError::Connection(e.to_string()))?,
        ) as Arc<dyn MessageBackend>;

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
        let gossip = Arc::new(Mutex::new(GossipEngine::new(
            node_id,
            gossip_cfg,
            gossip_backend,
            membership,
        )));

        let mesh = Arc::new(Mesh {
            node_id,
            incarnation,
            config,
            backend,
            gossip,
            join_handlers: std::sync::Mutex::new(Vec::new()),
            leave_handlers: std::sync::Mutex::new(Vec::new()),
            status_handlers: std::sync::Mutex::new(Vec::new()),
            _tasks: Vec::new(),
        });

        let mut tasks = Vec::new();
        tasks.push(spawn_tick_loop(Arc::clone(&mesh)));
        tasks.push(spawn_frame_loop(Arc::clone(&mesh)));

        if let Some(inner) = Arc::get_mut(&mut Arc::clone(&mesh)) {
            inner._tasks = tasks;
        }

        Ok(MeshHandle { inner: mesh })
    }

    fn emit_status(&self, change: StatusChange) {
        if change.to == Status::Alive && change.from != Status::Alive {
            for h in self.join_handlers.lock().unwrap().iter() {
                h(change.node_id);
            }
        }
        if change.to == Status::Dead {
            for h in self.leave_handlers.lock().unwrap().iter() {
                h(change.node_id);
            }
        }
        for h in self.status_handlers.lock().unwrap().iter() {
            h(change.node_id);
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
            match frame {
                Frame::Ping { seq, .. } => {
                    let mut gossip = mesh.gossip.lock().await;
                    gossip
                        .handle_ping(from, seq, SocketAddr::from(([0, 0, 0, 0], 0)))
                        .await;
                }
                Frame::Ack { seq, .. } => {
                    let mut gossip = mesh.gossip.lock().await;
                    gossip.handle_ack(from, seq, SocketAddr::from(([0, 0, 0, 0], 0)));
                }
                Frame::Gossip(msg) => {
                    let mut gossip = mesh.gossip.lock().await;
                    gossip.apply_membership_update(&msg.updates);
                }
                _ => {
                    // User or PingReq not handled yet.
                }
            }
        }
    })
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
