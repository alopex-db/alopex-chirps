use alopex_chirps::backend::MessageBackend;
use alopex_chirps::config::NodeConfig;
use alopex_chirps_gossip_swim::engine::{GossipConfig, GossipEngine, Transport, TransportError};
use alopex_chirps_gossip_swim::types::{MembershipView, Status};
use alopex_chirps_transport_quic::{QuicBackend, init_test_tracing};
use alopex_chirps_wire::frame::{
    Frame, GossipMessage, MemberStatus, MembershipUpdate, UserMessage,
};
use alopex_chirps_wire::node_id::NodeId;
use anyhow::Result;
use async_trait::async_trait;
use rcgen::generate_simple_self_signed;
use std::fs;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinHandle;

// Increased timeouts for CI environments where QUIC handshakes may be slower
const WAIT_SHORT: Duration = Duration::from_secs(10);
const WAIT_LONG: Duration = Duration::from_secs(15);

struct TestNode {
    id: NodeId,
    addr: SocketAddr,
    backend: Arc<QuicBackend>,
    engine: Arc<Mutex<GossipEngine>>,
    user_rx: mpsc::Receiver<(NodeId, Vec<u8>)>,
    shutdown: broadcast::Sender<()>,
    frame_task: JoinHandle<()>,
    tick_task: JoinHandle<()>,
}

struct MeshTls {
    _dir: TempDir,
    cert_paths: Vec<PathBuf>,
    key_paths: Vec<PathBuf>,
}

impl MeshTls {
    fn distinct_self_signed(nodes: usize) -> Result<Self> {
        Self::new(nodes, false)
    }

    fn shared_self_signed(nodes: usize) -> Result<Self> {
        Self::new(nodes, true)
    }

    fn new(nodes: usize, shared: bool) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let shared_material = if shared {
            let cert = generate_simple_self_signed(["alopex.local".to_string()])?;
            Some((cert.serialize_der()?, cert.serialize_private_key_der()))
        } else {
            None
        };
        let mut cert_paths = Vec::with_capacity(nodes);
        let mut key_paths = Vec::with_capacity(nodes);
        for index in 0..nodes {
            let (cert_der, key_der) = match &shared_material {
                Some((cert_der, key_der)) => (cert_der.clone(), key_der.clone()),
                None => {
                    let cert = generate_simple_self_signed(["alopex.local".to_string()])?;
                    (cert.serialize_der()?, cert.serialize_private_key_der())
                }
            };
            let cert_path = dir.path().join(format!("node-{index}.crt"));
            let key_path = dir.path().join(format!("node-{index}.key"));
            fs::write(&cert_path, cert_der)?;
            fs::write(&key_path, key_der)?;
            cert_paths.push(cert_path);
            key_paths.push(key_path);
        }
        Ok(Self {
            _dir: dir,
            cert_paths,
            key_paths,
        })
    }

    fn node_config(
        &self,
        node: usize,
        bind_addr: SocketAddr,
        seeds: Vec<SocketAddr>,
    ) -> NodeConfig {
        NodeConfig {
            bind_addr,
            seeds,
            cert_path: Some(self.cert_paths[node].clone()),
            key_path: Some(self.key_paths[node].clone()),
            trusted_cert_paths: self.cert_paths.clone(),
            ping_timeout: Duration::from_millis(200),
            indirect_ping_timeout: Duration::from_millis(400),
            suspect_to_dead_timeout: Duration::from_millis(800),
            gossip_interval: Duration::from_millis(80),
            ..Default::default()
        }
    }
}

impl TestNode {
    async fn start(id: NodeId, config: NodeConfig) -> Result<Self> {
        init_test_tracing();
        let config = Arc::new(config);
        let addr = config.bind_addr;
        let backend = QuicBackend::new(id, Arc::clone(&config)).await?;
        let backend = Arc::new(backend);

        let gossip_cfg = gossip_config(&config);
        let transport: Arc<dyn Transport> = Arc::new(BackendAdapter {
            inner: Arc::clone(&backend),
        });
        let membership = MembershipView::new();
        let engine = Arc::new(Mutex::new(GossipEngine::new(
            id, gossip_cfg, transport, membership,
        )));

        let (user_tx, user_rx) = mpsc::channel(64);
        let (shutdown, _) = broadcast::channel(4);

        let mut rx = backend.subscribe().await?;
        let mut frame_shutdown = shutdown.subscribe();
        let frame_engine = Arc::clone(&engine);
        let backend_for_loop = Arc::clone(&backend);
        let frame_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = frame_shutdown.recv() => break,
                    maybe = rx.recv() => match maybe {
                        Some((from, frame)) => {
                            let addr = backend_for_loop
                                .connected_peers()
                                .into_iter()
                                .find(|(id, _)| *id == from)
                                .map(|(_, addr)| addr)
                                .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
                            match frame {
                                Frame::Ping { seq, .. } => {
                                    let mut g = frame_engine.lock().await;
                                    g.handle_ping(from, seq, addr).await;
                                }
                                Frame::Ack { seq, .. } => {
                                    let mut g = frame_engine.lock().await;
                                    g.handle_ack(from, seq, addr);
                                }
                                Frame::PingReq { seq, from: requester, target } => {
                                    let mut g = frame_engine.lock().await;
                                    g.handle_ping_req(requester, seq, target, addr).await;
                                }
                                Frame::Gossip(msg) => {
                                    let mut g = frame_engine.lock().await;
                                    g.apply_membership_update(&msg.updates);
                                }
                                Frame::User(user) => {
                                    let _ = user_tx.send((from, user.payload)).await;
                                }
                                Frame::Raft(_) | Frame::RaftSnapshot(_) => {
                                    // Raftフレームはこのテストでは扱わない
                                }
                                Frame::FileTransfer(_) => {
                                    // FileTransferフレームはこのテストでは扱わない
                                }
                            }
                        }
                        None => break,
                    }
                }
            }
        });

        let mut tick_shutdown = shutdown.subscribe();
        let tick_engine = Arc::clone(&engine);
        let tick_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tick_shutdown.recv() => break,
                    res = async {
                        let mut g = tick_engine.lock().await;
                        g.tick().await
                    } => {
                        if let Err(err) = res {
                            // 重大ではないためロギングのみ（テスト失敗時にヒントを残す）。
                            eprintln!("tick処理中にエラー: {err}");
                        }
                    }
                }
            }
        });

        Ok(Self {
            id,
            addr,
            backend,
            engine,
            user_rx,
            shutdown,
            frame_task,
            tick_task,
        })
    }

    async fn wait_connected(&self, expected: usize, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.backend.connected_peers().len() >= expected {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                anyhow::bail!("接続待ちがタイムアウトしました (期待 {expected})");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn apply_members(
        &self,
        peers: &[(NodeId, SocketAddr)],
        incarnation: u64,
        status: Status,
    ) {
        let updates: Vec<MembershipUpdate> = peers
            .iter()
            .map(|(id, addr)| MembershipUpdate {
                node_id: *id,
                incarnation,
                addr: *addr,
                status: status_to_member(status.clone()),
            })
            .collect();
        let mut g = self.engine.lock().await;
        g.apply_membership_update(&updates);
    }

    async fn broadcast_user(&self, payload: &[u8]) -> Result<usize> {
        self.backend
            .broadcast(Frame::User(UserMessage {
                payload: payload.to_vec(),
            }))
            .await
            .map_err(Into::into)
    }

    /// Announces this node's new incarnation after reconnecting. A peer that
    /// marked this NodeId dead accepts an Alive update only when its
    /// incarnation is strictly newer than the old one.
    async fn announce_alive(&self, incarnation: u64) -> Result<()> {
        self.backend
            .broadcast(Frame::Gossip(GossipMessage {
                updates: vec![MembershipUpdate {
                    node_id: self.id,
                    incarnation,
                    addr: self.addr,
                    status: MemberStatus::Alive,
                }],
            }))
            .await?;
        Ok(())
    }

    async fn expect_user_from(
        &mut self,
        from: NodeId,
        body: &[u8],
        timeout: Duration,
    ) -> Result<()> {
        let (sender, payload) = tokio::time::timeout(timeout, self.user_rx.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("メッセージが届きませんでした"))?;
        anyhow::ensure!(
            sender == from,
            "送信元が一致しません: {sender:?} != {from:?}"
        );
        anyhow::ensure!(payload == body, "ペイロードが一致しません");
        Ok(())
    }

    async fn wait_membership_status(
        &self,
        target: NodeId,
        expected: Status,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let Some(status) = self.membership_status(target).await else {
                if tokio::time::Instant::now() > deadline {
                    anyhow::bail!("ステータス {expected:?} を待機中にタイムアウトしました");
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
                continue;
            };
            if status == expected {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                anyhow::bail!("ステータス {expected:?} を待機中にタイムアウトしました");
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }

    async fn membership_status(&self, target: NodeId) -> Option<Status> {
        let g = self.engine.lock().await;
        g.membership()
            .peers
            .get(&target)
            .map(|p| p.state.status.clone())
    }

    async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown.send(());
        self.backend.close().await?;
        self.frame_task.abort();
        self.tick_task.abort();
        let _ = self.frame_task.await;
        let _ = self.tick_task.await;
        Ok(())
    }
}

#[async_trait]
impl Transport for BackendAdapter {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError> {
        self.inner
            .send(target, frame)
            .await
            .map_err(|e| TransportError::Send(e.to_string()))
    }

    async fn broadcast(&self, frame: Frame) -> Result<usize, TransportError> {
        self.inner
            .broadcast(frame)
            .await
            .map_err(|e| TransportError::Send(e.to_string()))
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        self.inner
            .subscribe()
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))
    }

    async fn close(&self) -> Result<(), TransportError> {
        self.inner
            .close()
            .await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        self.inner.connected_peers()
    }
}

struct BackendAdapter {
    inner: Arc<QuicBackend>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "E2E test requires QUIC networking - run manually with --ignored"]
async fn three_node_mesh_self_signed_flow() -> Result<()> {
    run_three_node_mesh(MeshTls::distinct_self_signed(3)?).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "E2E test requires QUIC networking - run manually with --ignored"]
async fn three_node_mesh_with_explicit_cert() -> Result<()> {
    run_three_node_mesh(MeshTls::shared_self_signed(3)?).await
}

async fn run_three_node_mesh(tls: MeshTls) -> Result<()> {
    let addr_a = match free_addr() {
        Ok(a) => a,
        Err(e) if is_permission_error(&e) => {
            eprintln!("自己署名テストをスキップ: ソケット確保不可 ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    let addr_b = match free_addr() {
        Ok(a) => a,
        Err(e) if is_permission_error(&e) => {
            eprintln!("自己署名テストをスキップ: ソケット確保不可 ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    let addr_c = match free_addr() {
        Ok(a) => a,
        Err(e) if is_permission_error(&e) => {
            eprintln!("自己署名テストをスキップ: ソケット確保不可 ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let node_a_id = NodeId::new();
    let node_b_id = NodeId::new();
    let node_c_id = NodeId::new();

    let seeds_a = vec![];
    let seeds_b = vec![addr_a];
    let seeds_c = vec![addr_a, addr_b];

    let mut node_a = match TestNode::start(node_a_id, tls.node_config(0, addr_a, seeds_a)).await {
        Ok(n) => n,
        Err(e) if is_permission_error(&e) => {
            eprintln!("ノードA起動をスキップ: 権限不足 ({e})");
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let mut node_b = match TestNode::start(node_b_id, tls.node_config(1, addr_b, seeds_b)).await {
        Ok(n) => n,
        Err(e) if is_permission_error(&e) => {
            eprintln!("ノードB起動をスキップ: 権限不足 ({e})");
            node_a.shutdown().await?;
            return Ok(());
        }
        Err(e) => {
            node_a.shutdown().await?;
            return Err(e);
        }
    };
    let mut node_c = match TestNode::start(node_c_id, tls.node_config(2, addr_c, seeds_c)).await {
        Ok(n) => n,
        Err(e) if is_permission_error(&e) => {
            eprintln!("ノードC起動をスキップ: 権限不足 ({e})");
            node_a.shutdown().await?;
            node_b.shutdown().await?;
            return Ok(());
        }
        Err(e) => {
            node_a.shutdown().await?;
            node_b.shutdown().await?;
            return Err(e);
        }
    };

    node_a.wait_connected(2, WAIT_SHORT).await?;
    node_b.wait_connected(2, WAIT_SHORT).await?;
    node_c.wait_connected(2, WAIT_SHORT).await?;

    node_a
        .apply_members(
            &[(node_b_id, addr_b), (node_c_id, addr_c)],
            0,
            Status::Alive,
        )
        .await;
    node_b
        .apply_members(
            &[(node_a_id, addr_a), (node_c_id, addr_c)],
            0,
            Status::Alive,
        )
        .await;
    node_c
        .apply_members(
            &[(node_a_id, addr_a), (node_b_id, addr_b)],
            0,
            Status::Alive,
        )
        .await;

    tokio::time::sleep(Duration::from_millis(300)).await;

    node_a.broadcast_user(b"hello-mesh").await?;
    node_b
        .expect_user_from(node_a_id, b"hello-mesh", WAIT_SHORT)
        .await?;
    node_c
        .expect_user_from(node_a_id, b"hello-mesh", WAIT_SHORT)
        .await?;

    let old_b_id = node_b.id;
    node_b.shutdown().await?;

    node_a
        .wait_membership_status(old_b_id, Status::Suspect, WAIT_LONG)
        .await?;
    node_a
        .wait_membership_status(old_b_id, Status::Dead, WAIT_LONG)
        .await?;

    let addr_b2 = match free_addr() {
        Ok(a) => a,
        Err(e) if is_permission_error(&e) => {
            eprintln!("再接続用ポート確保失敗のためスキップ ({e})");
            node_a.shutdown().await?;
            node_c.shutdown().await?;
            return Ok(());
        }
        Err(e) => {
            node_a.shutdown().await?;
            node_c.shutdown().await?;
            return Err(e.into());
        }
    };
    let node_b2 =
        match TestNode::start(old_b_id, tls.node_config(1, addr_b2, vec![addr_a, addr_c])).await {
            Ok(n) => n,
            Err(e) if is_permission_error(&e) => {
                eprintln!("ノードB再起動をスキップ: 権限不足 ({e})");
                node_a.shutdown().await?;
                node_c.shutdown().await?;
                return Ok(());
            }
            Err(e) => {
                node_a.shutdown().await?;
                node_c.shutdown().await?;
                return Err(e);
            }
        };

    node_b2
        .apply_members(
            &[(node_a_id, addr_a), (node_c_id, addr_c)],
            1,
            Status::Alive,
        )
        .await;
    node_b2.wait_connected(2, WAIT_SHORT).await?;
    let rejoin_deadline = tokio::time::Instant::now() + WAIT_LONG;
    loop {
        // SWIM disseminates an Alive update repeatedly; a single control frame
        // is not a delivery acknowledgement for both peers in this real QUIC
        // harness. Keep announcing until both membership views converge.
        node_b2.announce_alive(1).await?;
        if node_a.membership_status(old_b_id).await == Some(Status::Alive)
            && node_c.membership_status(old_b_id).await == Some(Status::Alive)
        {
            break;
        }
        if tokio::time::Instant::now() > rejoin_deadline {
            anyhow::bail!("再参加ノードの Alive gossip が全 peer へ収束しませんでした");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    node_b2.broadcast_user(b"mesh-back-again").await?;
    node_a
        .expect_user_from(old_b_id, b"mesh-back-again", WAIT_SHORT)
        .await?;

    node_a.shutdown().await?;
    node_b2.shutdown().await?;
    node_c.shutdown().await?;
    Ok(())
}

fn gossip_config(cfg: &NodeConfig) -> GossipConfig {
    GossipConfig {
        ping_timeout: cfg.ping_timeout,
        indirect_ping_timeout: cfg.indirect_ping_timeout,
        suspect_to_dead_timeout: cfg.suspect_to_dead_timeout,
        gossip_interval: cfg.gossip_interval,
        fanout: cfg.fanout,
    }
}

fn free_addr() -> io::Result<SocketAddr> {
    TcpListener::bind("127.0.0.1:0").map(|l| l.local_addr().unwrap())
}

fn status_to_member(status: Status) -> MemberStatus {
    match status {
        Status::Alive => MemberStatus::Alive,
        Status::Suspect => MemberStatus::Suspect,
        Status::Dead => MemberStatus::Dead,
    }
}

fn is_permission_error<E: std::fmt::Display>(err: &E) -> bool {
    let msg = err.to_string();
    msg.contains("Permission denied") || msg.contains("Operation not permitted")
}
