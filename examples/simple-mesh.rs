//! ローカルホスト上で3ノードのメッシュを起動し、イベントとメッセージ送受信を試す最小例。

use alopex_chirps::{start, Frame, MeshHandle, UserMessage};
use alopex_chirps::config::NodeConfig;
use std::fs;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

/// Credentials deliberately shared by the local development mesh.
///
/// Production nodes should instead use separate certificates issued by a
/// cluster CA and configure `trusted_cert_paths` with the CA certificate.
struct DevelopmentTls {
    _directory: tempfile::TempDir,
    cert_path: PathBuf,
    key_path: PathBuf,
}

impl DevelopmentTls {
    fn new() -> anyhow::Result<Self> {
        let certificate = rcgen::generate_simple_self_signed(["localhost".to_string()])?;
        let directory = tempfile::tempdir()?;
        let cert_path = directory.path().join("development-cert.der");
        let key_path = directory.path().join("development-key.der");
        fs::write(&cert_path, certificate.serialize_der()?)?;
        fs::write(&key_path, certificate.serialize_private_key_der())?;

        Ok(Self {
            _directory: directory,
            cert_path,
            key_path,
        })
    }

    fn node_id_path(&self, node_file: &str) -> PathBuf {
        self._directory.path().join(node_file)
    }
}

fn free_addr() -> io::Result<SocketAddr> {
    TcpListener::bind("127.0.0.1:0").map(|l| l.local_addr().unwrap())
}

fn config(
    bind_addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    node_file: &str,
    tls: &DevelopmentTls,
) -> NodeConfig {
    let mut cfg = NodeConfig::default();
    cfg.bind_addr = bind_addr;
    cfg.seeds = seeds;
    cfg.node_id_path = tls.node_id_path(node_file);
    cfg.cert_path = Some(tls.cert_path.clone());
    cfg.key_path = Some(tls.key_path.clone());
    cfg
}

async fn log_frames(label: &str, mesh: MeshHandle) {
    match mesh.subscribe().await {
        Ok(mut rx) => {
            while let Some((from, frame)) = rx.recv().await {
                println!("[{label}] frame from {from:?}: {frame:?}");
            }
        }
        Err(err) => eprintln!("[{label}] subscribe failed: {err}"),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    // 3ノードのアドレスを用意
    let addr_a = free_addr()?;
    let addr_b = free_addr()?;
    let addr_c = free_addr()?;
    // この例だけで用いる共有の自己署名資格情報を明示的に作成する。
    let tls = DevelopmentTls::new()?;

    // 参加ノードは A をシードとして接続
    let mesh_a = start(config(addr_a, vec![], "chirps_node_a.id", &tls)).await?;
    let mesh_b = start(config(addr_b, vec![addr_a], "chirps_node_b.id", &tls)).await?;
    let mesh_c = start(config(addr_c, vec![addr_a], "chirps_node_c.id", &tls)).await?;

    println!("node A = {:?}", mesh_a.node_id());
    println!("node B = {:?}", mesh_b.node_id());
    println!("node C = {:?}", mesh_c.node_id());

    // 現在のメンバーシップをスナップショットで確認
    let membership = mesh_a.membership().await?;
    println!("[A] membership snapshot:");
    for (id, peer) in membership.peers.iter() {
        println!("  - {id:?} @ {} status={:?}", peer.addr, peer.state.status);
    }

    // イベントハンドラ登録（join/leave/ステータス変化）
    mesh_a.on_node_join(|id| println!("[A] join: {id:?}"));
    mesh_a.on_node_leave(|id| println!("[A] leave: {id:?}"));
    mesh_a.on_status_change(|id| println!("[A] status change: {id:?}"));

    // 受信ログをそれぞれのノードで開始
    tokio::spawn(log_frames("A", mesh_a.clone()));
    tokio::spawn(log_frames("B", mesh_b.clone()));
    tokio::spawn(log_frames("C", mesh_c.clone()));

    // 接続安定を少し待つ
    sleep(Duration::from_millis(500)).await;

    // ブロードキャストで全員に通知
    mesh_a
        .broadcast(Frame::User(UserMessage {
            payload: b"hello from A".to_vec(),
        }))
        .await?;

    // send_to でCへ直接メッセージ
    mesh_b
        .send_to(
            mesh_c.node_id(),
            Frame::User(UserMessage {
                payload: b"direct from B to C".to_vec(),
            }),
        )
        .await?;

    // 少し待ってログを確認
    sleep(Duration::from_secs(2)).await;
    Ok(())
}
