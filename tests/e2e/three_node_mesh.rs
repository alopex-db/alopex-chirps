use alopex_chirps::backend::MessageBackend;
use alopex_chirps::config::NodeConfig;
use anyhow::Result;
use async_trait::async_trait;
use chirps_gossip_swim::engine::{GossipConfig, GossipEngine, Transport, TransportError};
use chirps_gossip_swim::types::{MembershipView, Status};
use chirps_transport_quic::QuicBackend;
use chirps_wire::frame::{Frame, MemberStatus, MembershipUpdate, UserMessage};
use chirps_wire::node_id::NodeId;
use rcgen::generate_simple_self_signed;
use std::fs::File;
use std::io::{self, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinHandle;

const WAIT_SHORT: Duration = Duration::from_secs(2);
const WAIT_LONG: Duration = Duration::from_secs(5);

struct TestNode {
    id: NodeId,
    backend: Arc<QuicBackend>,
    engine: Arc<Mutex<GossipEngine>>,
    user_rx: mpsc::Receiver<(NodeId, Vec<u8>)>,
    shutdown: broadcast::Sender<()>,
    frame_task: JoinHandle<()>,
    tick_task: JoinHandle<()>,
}

impl TestNode {
    async fn start(
        id: NodeId,
        addr: SocketAddr,
        seeds: Vec<SocketAddr>,
        cert: Option<(PathBuf, PathBuf)>,
    ) -> Result<Self> {
        let config = Arc::new(make_config(addr, seeds, cert));
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
            if let Some(status) = self.membership_status(target).await {
                if status == expected {
                    return Ok(());
                }
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
async fn three_node_mesh_self_signed_flow() -> Result<()> {
    run_three_node_mesh(None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_mesh_with_explicit_cert() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cert_path = dir.path().join("test.crt");
    let key_path = dir.path().join("test.key");
    write_cert_files(&cert_path, &key_path)?;
    run_three_node_mesh(Some((cert_path, key_path))).await
}

async fn run_three_node_mesh(certs: Option<(PathBuf, PathBuf)>) -> Result<()> {
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

    let mut node_a = match TestNode::start(node_a_id, addr_a, seeds_a, certs.clone()).await {
        Ok(n) => n,
        Err(e) if is_permission_error(&e) => {
            eprintln!("ノードA起動をスキップ: 権限不足 ({e})");
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let mut node_b = match TestNode::start(node_b_id, addr_b, seeds_b, certs.clone()).await {
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
    let mut node_c = match TestNode::start(node_c_id, addr_c, seeds_c, certs.clone()).await {
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
    let node_b2 = match TestNode::start(old_b_id, addr_b2, vec![addr_a, addr_c], certs).await {
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

    node_a
        .wait_membership_status(old_b_id, Status::Alive, WAIT_LONG)
        .await?;
    node_c
        .wait_membership_status(old_b_id, Status::Alive, WAIT_LONG)
        .await?;

    node_b2.broadcast_user(b"mesh-back-again").await?;
    node_a
        .expect_user_from(old_b_id, b"mesh-back-again", WAIT_SHORT)
        .await?;

    node_a.shutdown().await?;
    node_b2.shutdown().await?;
    node_c.shutdown().await?;
    Ok(())
}

fn make_config(
    addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    cert: Option<(PathBuf, PathBuf)>,
) -> NodeConfig {
    let mut cfg = NodeConfig::default();
    cfg.bind_addr = addr;
    cfg.seeds = seeds;
    cfg.ping_timeout = Duration::from_millis(200);
    cfg.indirect_ping_timeout = Duration::from_millis(400);
    cfg.suspect_to_dead_timeout = Duration::from_millis(800);
    cfg.gossip_interval = Duration::from_millis(80);
    if let Some((cert_path, key_path)) = cert {
        cfg.cert_path = Some(cert_path);
        cfg.key_path = Some(key_path);
    }
    cfg
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

fn write_cert_files(cert_path: &PathBuf, key_path: &PathBuf) -> Result<()> {
    let cert = generate_simple_self_signed(["alopex.local".to_string()])?;
    let cert_der = cert.serialize_der()?;
    let key_der = cert.serialize_private_key_der();

    let mut cert_file = File::create(cert_path)?;
    cert_file.write_all(&cert_der)?;
    let mut key_file = File::create(key_path)?;
    key_file.write_all(&key_der)?;
    Ok(())
}
