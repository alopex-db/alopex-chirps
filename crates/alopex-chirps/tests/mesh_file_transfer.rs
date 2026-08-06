use alopex_chirps::config::NodeConfig;
use alopex_chirps::{
    FileTransferConfig, FileTransferService, Frame, MessageProfile, TransferOptions, UserMessage,
    start,
};
use rcgen::generate_simple_self_signed;
use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve loopback port")
        .local_addr()
        .expect("read loopback port")
}

struct TestCluster {
    _root: TempDir,
    cert_path: std::path::PathBuf,
    key_path: std::path::PathBuf,
    sender_path: std::path::PathBuf,
    receiver_path: std::path::PathBuf,
}

impl TestCluster {
    fn new() -> Self {
        let root = TempDir::new().expect("temporary cluster directory");
        let cert = generate_simple_self_signed(["alopex.local".to_string()])
            .expect("self-signed test certificate");
        let cert_path = root.path().join("cluster.crt");
        let key_path = root.path().join("cluster.key");
        fs::write(&cert_path, cert.serialize_der().expect("certificate DER"))
            .expect("write certificate");
        fs::write(&key_path, cert.serialize_private_key_der()).expect("write private key");
        let sender_path = root.path().join("sender");
        let receiver_path = root.path().join("receiver");
        fs::create_dir_all(&sender_path).expect("sender directory");
        fs::create_dir_all(&receiver_path).expect("receiver directory");
        Self {
            _root: root,
            cert_path,
            key_path,
            sender_path,
            receiver_path,
        }
    }

    fn node_config(
        &self,
        base_path: &Path,
        bind_addr: SocketAddr,
        seeds: Vec<SocketAddr>,
    ) -> NodeConfig {
        NodeConfig {
            bind_addr,
            seeds,
            cert_path: Some(self.cert_path.clone()),
            key_path: Some(self.key_path.clone()),
            trusted_cert_paths: vec![self.cert_path.clone()],
            node_id_path: base_path.join("node-id"),
            gossip_interval: Duration::from_millis(20),
            ..NodeConfig::default()
        }
    }

    fn transfer_config(&self, base_path: &Path) -> FileTransferConfig {
        FileTransferConfig::default()
            .with_base_path(base_path.to_path_buf())
            .with_temp_dir(Some(base_path.join("tmp")))
            .with_session_dir(Some(base_path.join("sessions")))
            .with_manifest_timeout(Duration::from_secs(3))
            .with_chunk_timeout(Duration::from_secs(3))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mesh_handle_constructs_and_runs_file_transfer() {
    let cluster = TestCluster::new();
    let receiver_addr = free_addr();
    let sender_addr = free_addr();

    let receiver_mesh =
        start(cluster.node_config(&cluster.receiver_path, receiver_addr, Vec::new()))
            .await
            .expect("start receiver mesh");
    let mut user_frames = receiver_mesh
        .subscribe()
        .await
        .expect("subscribe through MeshHandle");
    let receiver = receiver_mesh
        .file_transfer(cluster.transfer_config(&cluster.receiver_path))
        .await
        .expect("construct receiver service from MeshHandle");

    let sender_mesh =
        start(cluster.node_config(&cluster.sender_path, sender_addr, vec![receiver_addr]))
            .await
            .expect("start sender mesh");
    let sender = sender_mesh
        .file_transfer(cluster.transfer_config(&cluster.sender_path))
        .await
        .expect("construct sender service from MeshHandle");

    let mut connected = false;
    for _ in 0..150 {
        if sender_mesh
            .send_to_with_profile(
                receiver_mesh.node_id(),
                Frame::User(UserMessage {
                    payload: b"connection probe".to_vec(),
                }),
                MessageProfile::Ephemeral,
            )
            .await
            .is_ok()
        {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(connected, "logical loopback nodes did not connect");
    let (_, probe) = tokio::time::timeout(Duration::from_secs(1), user_frames.recv())
        .await
        .expect("user frame delivery timed out")
        .expect("user frame subscription closed");
    assert!(matches!(
        probe,
        Frame::User(UserMessage { payload }) if payload == b"connection probe"
    ));

    let payload = vec![0x5a; 128 * 1024];
    let source = cluster.sender_path.join("source.bin");
    tokio::fs::write(&source, &payload)
        .await
        .expect("write transfer source");

    tokio::time::timeout(
        Duration::from_secs(10),
        sender.send_file(
            receiver_mesh.node_id(),
            &source,
            Path::new("received.bin"),
            TransferOptions::default(),
        ),
    )
    .await
    .expect("file transfer timed out")
    .expect("file transfer failed");

    let received = tokio::fs::read(cluster.receiver_path.join("received.bin"))
        .await
        .expect("read received file");
    assert_eq!(received, payload);

    let duplicate = receiver_mesh
        .file_transfer(cluster.transfer_config(&cluster.receiver_path))
        .await;
    assert!(duplicate.is_err(), "chunk stream consumer must be unique");

    drop(sender);
    drop(receiver);
}
