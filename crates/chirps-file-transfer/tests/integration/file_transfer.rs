use alopex_chirps_file_transfer::ops::{
    ChunkStreamOpener, ControlDispatcher, ReceiveHandler, handle_exists_request,
    handle_list_request, handle_metadata_request, handle_remove_request,
};
use alopex_chirps_file_transfer::{
    CHUNK_STREAM_MAGIC, CompressionAlgorithm, ConflictResolution, FileTransferConfig,
    FileTransferError, FileTransferService, FileTransferServiceImpl, HashAlgorithm,
    IntegrityVerifier, ListOptions, RemoveOptions, SyncDirection, SyncOptions, TransferMode,
    TransferOptions, TransferSessionId,
};
use alopex_chirps_mock::{MockBackend, MockNetwork};
use alopex_chirps_wire::file_transfer::{FileTransferMessage, ManifestAck, TransferResponse};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use quinn::{ClientConfig, Connection, Endpoint, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, PrivateKey, RootCertStore};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

const SERVER_NAME: &str = "localhost";

fn build_tls_configs() -> (ServerConfig, ClientConfig) {
    let cert = generate_simple_self_signed([SERVER_NAME.to_string()]).expect("cert");
    let cert_der = cert.serialize_der().expect("cert der");
    let key_der = cert.serialize_private_key_der();
    let cert_chain = vec![Certificate(cert_der.clone())];
    let key = PrivateKey(key_der);

    let server_config = ServerConfig::with_single_cert(cert_chain, key).expect("server config");

    let mut roots = RootCertStore::empty();
    roots.add(&Certificate(cert_der)).expect("add cert");
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let client_config = ClientConfig::new(Arc::new(crypto));

    (server_config, client_config)
}

struct TestChunkNetwork {
    server_config: ServerConfig,
    client_config: ClientConfig,
    peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
    reverse_peers: Arc<RwLock<HashMap<SocketAddr, NodeId>>>,
}

impl TestChunkNetwork {
    fn new() -> Self {
        let (server_config, client_config) = build_tls_configs();
        TestChunkNetwork {
            server_config,
            client_config,
            peers: Arc::new(RwLock::new(HashMap::new())),
            reverse_peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn add_endpoint(&self, node_id: NodeId) -> Arc<TestChunkEndpoint> {
        let mut endpoint = Endpoint::server(
            self.server_config.clone(),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        )
        .expect("endpoint");
        endpoint.set_default_client_config(self.client_config.clone());
        let addr = endpoint.local_addr().expect("local addr");

        self.peers.write().await.insert(node_id, addr);
        self.reverse_peers.write().await.insert(addr, node_id);

        Arc::new(TestChunkEndpoint {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            peers: Arc::clone(&self.peers),
        })
    }
}

#[derive(Clone)]
struct TestChunkEndpoint {
    endpoint: Endpoint,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
}

#[async_trait]
impl ChunkStreamOpener for TestChunkEndpoint {
    async fn open_chunk_stream(
        &self,
        target: NodeId,
    ) -> Result<quinn::SendStream, FileTransferError> {
        if let Some(connection) = self.connections.read().await.get(&target).cloned() {
            return connection
                .open_uni()
                .await
                .map_err(|err| FileTransferError::Transport(err.to_string()));
        }

        let addr = {
            let peers = self.peers.read().await;
            peers
                .get(&target)
                .copied()
                .ok_or_else(|| FileTransferError::Internal("target not registered".to_string()))?
        };

        let connecting = self
            .endpoint
            .connect(addr, SERVER_NAME)
            .map_err(|err| FileTransferError::Transport(err.to_string()))?;
        let connection = connecting
            .await
            .map_err(|err| FileTransferError::Transport(err.to_string()))?;
        self.connections
            .write()
            .await
            .insert(target, connection.clone());

        connection
            .open_uni()
            .await
            .map_err(|err| FileTransferError::Transport(err.to_string()))
    }
}

struct FlakyChunkOpener {
    inner: Arc<dyn ChunkStreamOpener>,
    remaining_failures: AtomicUsize,
}

impl FlakyChunkOpener {
    fn new(inner: Arc<dyn ChunkStreamOpener>, failures: usize) -> Arc<Self> {
        Arc::new(FlakyChunkOpener {
            inner,
            remaining_failures: AtomicUsize::new(failures),
        })
    }

    fn remaining(&self) -> usize {
        self.remaining_failures.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ChunkStreamOpener for FlakyChunkOpener {
    async fn open_chunk_stream(
        &self,
        target: NodeId,
    ) -> Result<quinn::SendStream, FileTransferError> {
        if self
            .remaining_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current == 0 {
                    None
                } else {
                    Some(current - 1)
                }
            })
            .is_ok()
        {
            return Err(FileTransferError::Transport(
                "simulated stream failure".to_string(),
            ));
        }

        self.inner.open_chunk_stream(target).await
    }
}

#[derive(Clone)]
struct ProxyChunkOpener {
    endpoint: Endpoint,
    proxy_addr: SocketAddr,
    connection: Arc<RwLock<Option<Connection>>>,
}

impl ProxyChunkOpener {
    fn new(endpoint: Endpoint, proxy_addr: SocketAddr) -> Self {
        ProxyChunkOpener {
            endpoint,
            proxy_addr,
            connection: Arc::new(RwLock::new(None)),
        }
    }
}

#[async_trait]
impl ChunkStreamOpener for ProxyChunkOpener {
    async fn open_chunk_stream(
        &self,
        _target: NodeId,
    ) -> Result<quinn::SendStream, FileTransferError> {
        if let Some(connection) = self.connection.read().await.clone() {
            return connection
                .open_uni()
                .await
                .map_err(|error| FileTransferError::Transport(error.to_string()));
        }

        let connection = self
            .endpoint
            .connect(self.proxy_addr, SERVER_NAME)
            .map_err(|error| FileTransferError::Transport(error.to_string()))?
            .await
            .map_err(|error| FileTransferError::Transport(error.to_string()))?;
        self.connection.write().await.replace(connection.clone());
        connection
            .open_uni()
            .await
            .map_err(|error| FileTransferError::Transport(error.to_string()))
    }
}

struct CorruptingChunkProxy {
    endpoint: Endpoint,
    receiver_addr: SocketAddr,
    remaining_corruptions: Arc<AtomicUsize>,
    forwarded_streams: Arc<AtomicUsize>,
}

impl CorruptingChunkProxy {
    fn new(
        server_config: ServerConfig,
        client_config: ClientConfig,
        receiver_addr: SocketAddr,
        corruptions: usize,
    ) -> Self {
        let mut endpoint =
            Endpoint::server(server_config, SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .expect("proxy endpoint");
        endpoint.set_default_client_config(client_config);
        CorruptingChunkProxy {
            endpoint,
            receiver_addr,
            remaining_corruptions: Arc::new(AtomicUsize::new(corruptions)),
            forwarded_streams: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn addr(&self) -> SocketAddr {
        self.endpoint.local_addr().expect("proxy address")
    }

    fn remaining_corruptions(&self) -> usize {
        self.remaining_corruptions.load(Ordering::SeqCst)
    }

    fn forwarded_streams(&self) -> usize {
        self.forwarded_streams.load(Ordering::SeqCst)
    }

    fn spawn(&self) -> JoinHandle<()> {
        let endpoint = self.endpoint.clone();
        let receiver_addr = self.receiver_addr;
        let remaining_corruptions = Arc::clone(&self.remaining_corruptions);
        let forwarded_streams = Arc::clone(&self.forwarded_streams);

        tokio::spawn(async move {
            loop {
                let Some(connecting) = endpoint.accept().await else {
                    break;
                };
                let endpoint = endpoint.clone();
                let remaining_corruptions = Arc::clone(&remaining_corruptions);
                let forwarded_streams = Arc::clone(&forwarded_streams);
                tokio::spawn(async move {
                    let incoming = match connecting.await {
                        Ok(connection) => connection,
                        Err(_) => return,
                    };
                    let outgoing = match endpoint.connect(receiver_addr, SERVER_NAME) {
                        Ok(connecting) => match connecting.await {
                            Ok(connection) => connection,
                            Err(_) => return,
                        },
                        Err(_) => return,
                    };

                    loop {
                        let mut stream = match incoming.accept_uni().await {
                            Ok(stream) => stream,
                            Err(_) => break,
                        };
                        let mut frame = match stream.read_to_end(17 * 1024 * 1024).await {
                            Ok(frame) => frame,
                            Err(_) => break,
                        };
                        let corrupted = remaining_corruptions
                            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                                (remaining > 0).then_some(remaining - 1)
                            })
                            .is_ok();
                        if corrupted {
                            const PAYLOAD_OFFSET: usize = 1 + 16 + 4 + 4;
                            if let Some(byte) = frame.get_mut(PAYLOAD_OFFSET) {
                                *byte ^= 0xff;
                            }
                        }

                        let mut forward = match outgoing.open_uni().await {
                            Ok(stream) => stream,
                            Err(_) => break,
                        };
                        if forward.write_all(&frame).await.is_err()
                            || forward.finish().await.is_err()
                        {
                            break;
                        }
                        forwarded_streams.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        })
    }
}

struct TestCluster {
    network: MockNetwork,
    chunk_network: TestChunkNetwork,
}

impl TestCluster {
    fn new() -> Self {
        TestCluster {
            network: MockNetwork::new(),
            chunk_network: TestChunkNetwork::new(),
        }
    }

    async fn add_node(&self, node_id: NodeId, base_path: PathBuf) -> TestNode {
        self.add_node_with_controls(node_id, base_path, true).await
    }

    async fn add_node_with_controls(
        &self,
        node_id: NodeId,
        base_path: PathBuf,
        handle_transfer_messages: bool,
    ) -> TestNode {
        let endpoint = self.chunk_network.add_endpoint(node_id).await;
        self.add_node_with_endpoint(
            node_id,
            base_path,
            endpoint.clone(),
            endpoint,
            handle_transfer_messages,
        )
        .await
    }

    async fn add_node_with_endpoint(
        &self,
        node_id: NodeId,
        base_path: PathBuf,
        endpoint: Arc<TestChunkEndpoint>,
        opener: Arc<dyn ChunkStreamOpener>,
        handle_transfer_messages: bool,
    ) -> TestNode {
        let backend = self
            .network
            .add_node(node_id, MockBackend::ephemeral_addr())
            .await;
        let backend: Arc<dyn alopex_chirps_core::backend::MessageBackend> = Arc::new(backend);

        let session_dir = base_path.join("sessions");
        let temp_dir = base_path.join("tmp");
        let _ = tokio::fs::create_dir_all(&session_dir).await;
        let _ = tokio::fs::create_dir_all(&temp_dir).await;

        let config = FileTransferConfig::default()
            .with_base_path(base_path.clone())
            .with_session_dir(Some(session_dir))
            .with_temp_dir(Some(temp_dir));

        let service = FileTransferServiceImpl::new(node_id, backend, opener, config)
            .await
            .expect("service");
        let service = Arc::new(service);

        let (shutdown, _) = broadcast::channel(4);
        let control_task = spawn_control_responder(
            service.control(),
            service.receive_handler(),
            service.path_validator().clone(),
            shutdown.subscribe(),
            handle_transfer_messages,
        );
        let chunk_task = spawn_chunk_acceptor(
            endpoint.endpoint.clone(),
            Arc::clone(&self.chunk_network.reverse_peers),
            service.receive_handler(),
            service.control(),
            shutdown.subscribe(),
        );

        TestNode {
            service,
            shutdown,
            _control_task: control_task,
            _chunk_task: chunk_task,
        }
    }
}

struct TestNode {
    service: Arc<FileTransferServiceImpl>,
    shutdown: broadcast::Sender<()>,
    _control_task: JoinHandle<()>,
    _chunk_task: JoinHandle<()>,
}

impl TestNode {
    async fn shutdown(self) {
        let _ = self.shutdown.send(());
        self._control_task.abort();
        self._chunk_task.abort();
        let _ = self._control_task.await;
        let _ = self._chunk_task.await;
    }
}

fn spawn_control_responder(
    control: Arc<ControlDispatcher>,
    receive_handler: Arc<ReceiveHandler>,
    path_validator: alopex_chirps_file_transfer::PathValidator,
    mut shutdown: broadcast::Receiver<()>,
    handle_transfer_messages: bool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                message = control.recv_any_filtered(Duration::from_millis(200), |_, msg| {
                    let is_transfer = matches!(
                        msg,
                        FileTransferMessage::TransferRequest(_) | FileTransferMessage::Manifest(_)
                    );
                    let is_file_op = matches!(
                        msg,
                        FileTransferMessage::ExistsRequest(_)
                            | FileTransferMessage::RemoveRequest(_)
                            | FileTransferMessage::MetadataRequest(_)
                            | FileTransferMessage::ListRequest(_)
                    );
                    (handle_transfer_messages && is_transfer) || is_file_op
                }) => {
                    let (session_id, sender, message) = match message {
                        Ok(message) => message,
                        Err(FileTransferError::Timeout) => continue,
                        Err(_) => break,
                    };

                    match message {
                        FileTransferMessage::TransferRequest(request) => {
                            let response = match receive_handler
                                .handle_transfer_request(session_id, request)
                                .await
                            {
                                Ok(response) => response,
                                Err(err) => TransferResponse {
                                    accepted: false,
                                    rejection_reason: Some(err.to_string()),
                                    existing_chunks: Vec::new(),
                                },
                            };
                            let _ = control
                                .send_message(
                                    sender,
                                    session_id,
                                    FileTransferMessage::TransferResponse(response),
                                )
                                .await;
                        }
                        FileTransferMessage::Manifest(manifest) => {
                            let ack = match receive_handler.handle_manifest(sender, manifest).await {
                                Ok(ack) => ack,
                                Err(err) => ManifestAck {
                                    accepted: false,
                                    skip_chunks: Vec::new(),
                                    error: Some(err.to_string()),
                                },
                            };
                            let _ = control
                                .send_message(
                                    sender,
                                    session_id,
                                    FileTransferMessage::ManifestAck(ack),
                                )
                                .await;
                        }
                        FileTransferMessage::ExistsRequest(request) => {
                            if let Ok(response) = handle_exists_request(&path_validator, request).await {
                                let _ = control
                                    .send_message(
                                        sender,
                                        session_id,
                                        FileTransferMessage::ExistsResponse(response),
                                    )
                                    .await;
                            }
                        }
                        FileTransferMessage::RemoveRequest(request) => {
                            if let Ok(response) = handle_remove_request(&path_validator, request).await {
                                let _ = control
                                    .send_message(
                                        sender,
                                        session_id,
                                        FileTransferMessage::RemoveResponse(response),
                                    )
                                    .await;
                            }
                        }
                        FileTransferMessage::MetadataRequest(request) => {
                            if let Ok(response) = handle_metadata_request(&path_validator, request).await {
                                let _ = control
                                    .send_message(
                                        sender,
                                        session_id,
                                        FileTransferMessage::MetadataResponse(response),
                                    )
                                    .await;
                            }
                        }
                        FileTransferMessage::ListRequest(request) => {
                            if let Ok(response) = handle_list_request(&path_validator, request).await {
                                let _ = control
                                    .send_message(
                                        sender,
                                        session_id,
                                        FileTransferMessage::ListResponse(response),
                                    )
                                    .await;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    })
}

fn spawn_chunk_acceptor(
    endpoint: Endpoint,
    reverse_peers: Arc<RwLock<HashMap<SocketAddr, NodeId>>>,
    receive_handler: Arc<ReceiveHandler>,
    control: Arc<ControlDispatcher>,
    mut shutdown: broadcast::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                incoming = endpoint.accept() => {
                    let Some(connecting) = incoming else { break };
                    let reverse_peers = Arc::clone(&reverse_peers);
                    let receive_handler = Arc::clone(&receive_handler);
                    let control = Arc::clone(&control);
                    tokio::spawn(async move {
                        let connection = match connecting.await {
                            Ok(connection) => connection,
                            Err(_) => return,
                        };
                        let peer_addr = connection.remote_address();
                        let peer_id = { reverse_peers.read().await.get(&peer_addr).copied() };
                        let Some(peer_id) = peer_id else { return };

                        loop {
                            let mut recv = match connection.accept_uni().await {
                                Ok(recv) => recv,
                                Err(_) => break,
                            };
                            let mut magic = [0u8; 1];
                            if recv.read_exact(&mut magic).await.is_err() {
                                break;
                            }
                            if magic[0] != CHUNK_STREAM_MAGIC {
                                continue;
                            }
                            if let Err(err) = receive_handler
                                .handle_chunk_stream(peer_id, &control, &mut recv)
                                .await
                            {
                                eprintln!("chunk stream error: {err}");
                            }
                        }
                    });
                }
            }
        }
    })
}

async fn write_pattern_file(path: &Path, size: usize) -> std::io::Result<()> {
    let mut file = tokio::fs::File::create(path).await?;
    let mut remaining = size;
    let mut pattern = vec![0u8; 8192];
    let mut offset = 0u8;
    while remaining > 0 {
        for byte in &mut pattern {
            *byte = offset;
            offset = offset.wrapping_add(1);
        }
        let chunk = remaining.min(pattern.len());
        file.write_all(&pattern[..chunk]).await?;
        remaining -= chunk;
    }
    file.flush().await?;
    Ok(())
}

async fn assert_files_match(left: &Path, right: &Path) {
    let left_hash = IntegrityVerifier::compute_file_hash(left, HashAlgorithm::Sha256)
        .await
        .expect("left hash");
    let right_hash = IntegrityVerifier::compute_file_hash(right, HashAlgorithm::Sha256)
        .await
        .expect("right hash");
    assert_eq!(left_hash, right_hash);
}

async fn wait_for_session_id(node: &TestNode) -> TransferSessionId {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let sessions = node.service.active_transfers();
        if let Some(session) = sessions.first() {
            return session.id;
        }
        if Instant::now() > deadline {
            panic!("timed out waiting for session id");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_partial_completion(
    receive_handler: Arc<ReceiveHandler>,
    session_id: TransferSessionId,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(session) = receive_handler.session_snapshot(session_id).await
            && !session.chunk_tracker.completed.is_empty()
            && !session.chunk_tracker.is_complete()
        {
            return;
        }
        if Instant::now() > deadline {
            panic!("timed out waiting for partial transfer");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_send_file(cluster: &TestCluster, size: usize) {
    let sender_dir = TempDir::new().expect("sender dir");
    let receiver_dir = TempDir::new().expect("receiver dir");

    let sender_id = NodeId::new();
    let receiver_id = NodeId::new();

    let sender = cluster
        .add_node(sender_id, sender_dir.path().to_path_buf())
        .await;
    let receiver = cluster
        .add_node(receiver_id, receiver_dir.path().to_path_buf())
        .await;

    let source_path = sender_dir.path().join("source.bin");
    let dest_path = receiver_dir.path().join("dest.bin");
    write_pattern_file(&source_path, size)
        .await
        .expect("write source");

    let options = TransferOptions::default()
        .with_chunk_size(64 * 1024)
        .with_concurrency(2);
    let handle = sender
        .service
        .send_file(receiver_id, &source_path, &dest_path, options)
        .await
        .expect("send file");

    let progress = handle.progress().await;
    assert_eq!(progress.bytes_transferred, size as u64);
    assert_files_match(&source_path, &dest_path).await;

    drop(receiver);
    drop(sender);
}

async fn assert_compressed_send(compression: CompressionAlgorithm) {
    let cluster = TestCluster::new();
    let sender_dir = TempDir::new().expect("sender dir");
    let receiver_dir = TempDir::new().expect("receiver dir");
    let sender_id = NodeId::new();
    let receiver_id = NodeId::new();

    let sender = cluster
        .add_node(sender_id, sender_dir.path().to_path_buf())
        .await;
    let receiver = cluster
        .add_node(receiver_id, receiver_dir.path().to_path_buf())
        .await;

    let source_path = sender_dir.path().join("compressible.bin");
    let dest_path = receiver_dir.path().join("compressed.bin");
    let source_data = vec![b'x'; 128 * 1024];
    tokio::fs::write(&source_path, &source_data)
        .await
        .expect("write source");

    let options = TransferOptions::default()
        .with_chunk_size(64 * 1024)
        .with_compression(compression);
    let handle = sender
        .service
        .send_file(receiver_id, &source_path, &dest_path, options)
        .await
        .expect("send compressed file");

    let progress = handle.progress().await;
    assert_eq!(progress.bytes_transferred, source_data.len() as u64);
    assert_files_match(&source_path, &dest_path).await;

    receiver.shutdown().await;
    sender.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_file_transfers_small_file() {
    let cluster = TestCluster::new();
    assert_send_file(&cluster, 1024).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_file_transfers_medium_file() {
    let cluster = TestCluster::new();
    assert_send_file(&cluster, 10 * 1024 * 1024).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_file_transfers_large_file() {
    let cluster = TestCluster::new();
    assert_send_file(&cluster, 100 * 1024 * 1024).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_file_transfers_zstd_compressed_file() {
    assert_compressed_send(CompressionAlgorithm::Zstd).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_file_transfers_zstd_level_compressed_file() {
    assert_compressed_send(CompressionAlgorithm::ZstdLevel(9)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_file_move_removes_source() {
    let cluster = TestCluster::new();
    let sender_dir = TempDir::new().expect("sender dir");
    let receiver_dir = TempDir::new().expect("receiver dir");

    let sender_id = NodeId::new();
    let receiver_id = NodeId::new();

    let sender = cluster
        .add_node(sender_id, sender_dir.path().to_path_buf())
        .await;
    let _receiver = cluster
        .add_node(receiver_id, receiver_dir.path().to_path_buf())
        .await;

    let source_path = sender_dir.path().join("move-source.bin");
    let dest_path = receiver_dir.path().join("move-dest.bin");
    write_pattern_file(&source_path, 256 * 1024)
        .await
        .expect("write source");

    let options = TransferOptions::default().with_mode(TransferMode::Move);
    let algorithm = options.hash_algorithm;
    let expected_hash = IntegrityVerifier::compute_file_hash(&source_path, algorithm)
        .await
        .expect("hash");

    let handle = sender
        .service
        .send_file(receiver_id, &source_path, &dest_path, options)
        .await
        .expect("send file");

    let progress = handle.progress().await;
    assert_eq!(progress.bytes_transferred, 256 * 1024);
    assert!(tokio::fs::metadata(&source_path).await.is_err());

    let dest_hash = IntegrityVerifier::compute_file_hash(&dest_path, algorithm)
        .await
        .expect("dest hash");
    assert_eq!(dest_hash, expected_hash);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_file_retries_on_stream_failure() {
    let cluster = TestCluster::new();
    let sender_dir = TempDir::new().expect("sender dir");
    let receiver_dir = TempDir::new().expect("receiver dir");

    let sender_id = NodeId::new();
    let receiver_id = NodeId::new();

    let endpoint = cluster.chunk_network.add_endpoint(sender_id).await;
    let flaky = FlakyChunkOpener::new(endpoint.clone(), 1);

    let sender = cluster
        .add_node_with_endpoint(
            sender_id,
            sender_dir.path().to_path_buf(),
            endpoint.clone(),
            flaky.clone(),
            true,
        )
        .await;
    let _receiver = cluster
        .add_node(receiver_id, receiver_dir.path().to_path_buf())
        .await;

    let source_path = sender_dir.path().join("source.bin");
    let dest_path = receiver_dir.path().join("dest.bin");
    write_pattern_file(&source_path, 128 * 1024)
        .await
        .expect("write source");

    let handle = sender
        .service
        .send_file(
            receiver_id,
            &source_path,
            &dest_path,
            TransferOptions::default(),
        )
        .await
        .expect("send file");

    let progress = handle.progress().await;
    assert_eq!(progress.bytes_transferred, 128 * 1024);
    assert_eq!(flaky.remaining(), 0);
    assert_files_match(&source_path, &dest_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_file_retries_after_corrupted_chunk() {
    let cluster = TestCluster::new();
    let sender_dir = TempDir::new().expect("sender dir");
    let receiver_dir = TempDir::new().expect("receiver dir");
    let sender_id = NodeId::new();
    let receiver_id = NodeId::new();

    let receiver_endpoint = cluster.chunk_network.add_endpoint(receiver_id).await;
    let receiver_addr = cluster
        .chunk_network
        .peers
        .read()
        .await
        .get(&receiver_id)
        .copied()
        .expect("receiver address");
    let proxy = CorruptingChunkProxy::new(
        cluster.chunk_network.server_config.clone(),
        cluster.chunk_network.client_config.clone(),
        receiver_addr,
        1,
    );
    cluster
        .chunk_network
        .reverse_peers
        .write()
        .await
        .insert(proxy.addr(), sender_id);
    let proxy_task = proxy.spawn();

    let sender_endpoint = cluster.chunk_network.add_endpoint(sender_id).await;
    let sender_opener = Arc::new(ProxyChunkOpener::new(
        sender_endpoint.endpoint.clone(),
        proxy.addr(),
    ));
    let sender = cluster
        .add_node_with_endpoint(
            sender_id,
            sender_dir.path().to_path_buf(),
            sender_endpoint.clone(),
            sender_opener,
            true,
        )
        .await;
    let receiver = cluster
        .add_node_with_endpoint(
            receiver_id,
            receiver_dir.path().to_path_buf(),
            receiver_endpoint.clone(),
            receiver_endpoint,
            true,
        )
        .await;

    let source_path = sender_dir.path().join("source.bin");
    let dest_path = receiver_dir.path().join("dest.bin");
    write_pattern_file(&source_path, 64 * 1024)
        .await
        .expect("write source");

    let handle = sender
        .service
        .send_file(
            receiver_id,
            &source_path,
            &dest_path,
            TransferOptions::default().with_chunk_size(64 * 1024),
        )
        .await
        .expect("retry corrupted chunk");

    assert_eq!(handle.progress().await.bytes_transferred, 64 * 1024);
    assert_eq!(proxy.remaining_corruptions(), 0);
    assert!(
        proxy.forwarded_streams() >= 2,
        "a checksum NACK must cause the sender to retry the corrupted chunk"
    );
    assert_files_match(&source_path, &dest_path).await;

    proxy_task.abort();
    let _ = proxy_task.await;
    receiver.shutdown().await;
    sender.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_file_all_success() {
    let cluster = TestCluster::new();
    let sender_dir = TempDir::new().expect("sender dir");
    let receiver_dir = TempDir::new().expect("receiver dir");
    let third_dir = TempDir::new().expect("third dir");

    let sender_id = NodeId::new();
    let receiver_id = NodeId::new();
    let third_id = NodeId::new();

    let sender = cluster
        .add_node(sender_id, sender_dir.path().to_path_buf())
        .await;
    let receiver = cluster
        .add_node(receiver_id, receiver_dir.path().to_path_buf())
        .await;
    let third = cluster
        .add_node(third_id, third_dir.path().to_path_buf())
        .await;

    let source_path = sender_dir.path().join("source.bin");
    let dest_path = PathBuf::from("dest.bin");
    write_pattern_file(&source_path, 32 * 1024)
        .await
        .expect("write source");

    let handle = sender
        .service
        .broadcast_file(&source_path, &dest_path, TransferOptions::default())
        .await
        .expect("broadcast");

    let progress = handle.progress().await;
    for (node_id, status) in progress {
        if node_id == sender_id {
            continue;
        }
        assert!(status.error.is_none());
        assert!(matches!(
            status.state,
            alopex_chirps_file_transfer::TransferState::Completed
        ));
    }

    let receiver_dest = receiver_dir.path().join(&dest_path);
    let third_dest = third_dir.path().join(&dest_path);
    assert_files_match(&source_path, &receiver_dest).await;
    assert_files_match(&source_path, &third_dest).await;

    drop(receiver);
    drop(third);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_file_partial_failure_keeps_source_on_move() {
    let cluster = TestCluster::new();
    let sender_dir = TempDir::new().expect("sender dir");
    let receiver_dir = TempDir::new().expect("receiver dir");
    let third_dir = TempDir::new().expect("third dir");

    let sender_id = NodeId::new();
    let receiver_id = NodeId::new();
    let third_id = NodeId::new();

    let sender = cluster
        .add_node(sender_id, sender_dir.path().to_path_buf())
        .await;
    let receiver = cluster
        .add_node(receiver_id, receiver_dir.path().to_path_buf())
        .await;
    let third = cluster
        .add_node(third_id, third_dir.path().to_path_buf())
        .await;

    let source_path = sender_dir.path().join("source.bin");
    let dest_path = PathBuf::from("dest.bin");
    let receiver_dest = receiver_dir.path().join(&dest_path);
    let third_dest = third_dir.path().join(&dest_path);
    write_pattern_file(&source_path, 32 * 1024)
        .await
        .expect("write source");
    write_pattern_file(&third_dest, 4 * 1024)
        .await
        .expect("write existing");

    let options = TransferOptions::default().with_mode(TransferMode::Move);
    let handle = sender
        .service
        .broadcast_file(&source_path, &dest_path, options)
        .await
        .expect("broadcast");

    let progress = handle.progress().await;
    let third_status = progress.get(&third_id).expect("third status");
    assert!(matches!(
        third_status.state,
        alopex_chirps_file_transfer::TransferState::Failed
    ));

    assert!(tokio::fs::metadata(&source_path).await.is_ok());
    assert_files_match(&source_path, &receiver_dest).await;

    drop(receiver);
    drop(third);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_ops_round_trip() {
    let cluster = TestCluster::new();
    let sender_dir = TempDir::new().expect("sender dir");
    let receiver_dir = TempDir::new().expect("receiver dir");

    let sender_id = NodeId::new();
    let receiver_id = NodeId::new();

    let sender = cluster
        .add_node(sender_id, sender_dir.path().to_path_buf())
        .await;
    let _receiver = cluster
        .add_node(receiver_id, receiver_dir.path().to_path_buf())
        .await;

    let data_dir = receiver_dir.path().join("data");
    tokio::fs::create_dir_all(&data_dir).await.expect("mkdir");
    let data_path = data_dir.join("sample.txt");
    tokio::fs::write(&data_path, b"hello").await.expect("write");

    let exists = sender
        .service
        .exists(receiver_id, &data_path)
        .await
        .expect("exists");
    assert!(exists);

    let metadata = sender
        .service
        .metadata(receiver_id, &data_path)
        .await
        .expect("metadata");
    assert_eq!(metadata.size, Some(5));

    let listed = sender
        .service
        .list_files(receiver_id, &data_dir, ListOptions::default())
        .await
        .expect("list");
    assert!(listed.iter().any(|info| info.path.ends_with("sample.txt")));

    sender
        .service
        .remove(receiver_id, &data_path, RemoveOptions::default())
        .await
        .expect("remove");
    let exists = sender
        .service
        .exists(receiver_id, &data_path)
        .await
        .expect("exists");
    assert!(!exists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_push_transfers_to_remote() {
    let cluster = TestCluster::new();
    let local_dir = TempDir::new().expect("local dir");
    let remote_dir = TempDir::new().expect("remote dir");

    let local_id = NodeId::new();
    let remote_id = NodeId::new();

    let local = cluster
        .add_node_with_controls(local_id, local_dir.path().to_path_buf(), false)
        .await;
    let _remote = cluster
        .add_node(remote_id, remote_dir.path().to_path_buf())
        .await;

    let local_path = local_dir.path().join("sync.txt");
    let remote_path = remote_dir.path().join("sync.txt");
    write_pattern_file(&local_path, 12 * 1024)
        .await
        .expect("write local");

    let options = SyncOptions::default().with_direction(SyncDirection::Push);
    let handle = local
        .service
        .sync_file(&local_path, &remote_path, Some(vec![remote_id]), options)
        .await
        .expect("sync");

    let progress = handle.progress().await;
    assert_eq!(progress.bytes_transferred, 12 * 1024);
    assert_files_match(&local_path, &remote_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_pull_receives_from_remote() {
    let cluster = TestCluster::new();
    let local_dir = TempDir::new().expect("local dir");
    let remote_dir = TempDir::new().expect("remote dir");

    let local_id = NodeId::new();
    let remote_id = NodeId::new();

    let local = cluster
        .add_node(local_id, local_dir.path().to_path_buf())
        .await;
    let remote = cluster
        .add_node(remote_id, remote_dir.path().to_path_buf())
        .await;

    let local_path = local_dir.path().join("pull.txt");
    let remote_path = remote_dir.path().join("pull.txt");
    write_pattern_file(&remote_path, 8 * 1024)
        .await
        .expect("write remote");

    let sync_options = SyncOptions::default().with_direction(SyncDirection::Pull);

    let sync_task = {
        let local = Arc::clone(&local.service);
        let local_path = local_path.clone();
        let remote_path = remote_path.clone();
        tokio::spawn(async move {
            local
                .sync_file(
                    &local_path,
                    &remote_path,
                    Some(vec![remote_id]),
                    sync_options,
                )
                .await
        })
    };

    let send_task = {
        let remote = Arc::clone(&remote.service);
        let local_path = local_path.clone();
        let remote_path = remote_path.clone();
        tokio::spawn(async move {
            remote
                .send_file(
                    local_id,
                    &remote_path,
                    &local_path,
                    TransferOptions::default(),
                )
                .await
        })
    };

    let handle = sync_task.await.expect("sync task").expect("sync");
    send_task.await.expect("send task").expect("send");

    let progress = handle.progress().await;
    assert_eq!(progress.bytes_transferred, 8 * 1024);
    assert_files_match(&remote_path, &local_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_bidirectional_conflict_manual() {
    let cluster = TestCluster::new();
    let local_dir = TempDir::new().expect("local dir");
    let remote_dir = TempDir::new().expect("remote dir");

    let local_id = NodeId::new();
    let remote_id = NodeId::new();

    let local = cluster
        .add_node(local_id, local_dir.path().to_path_buf())
        .await;
    let _remote = cluster
        .add_node(remote_id, remote_dir.path().to_path_buf())
        .await;

    let local_path = local_dir.path().join("conflict.txt");
    let remote_path = remote_dir.path().join("conflict.txt");
    write_pattern_file(&local_path, 4 * 1024)
        .await
        .expect("write local");
    write_pattern_file(&remote_path, 6 * 1024)
        .await
        .expect("write remote");

    let options = SyncOptions::default()
        .with_direction(SyncDirection::Bidirectional)
        .with_conflict_resolution(ConflictResolution::Manual);

    let result = local
        .service
        .sync_file(&local_path, &remote_path, Some(vec![remote_id]), options)
        .await;
    assert!(matches!(
        result,
        Err(FileTransferError::SyncConflict { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_transfer_restores_progress() {
    let sender_dir = TempDir::new().expect("sender dir");
    let receiver_dir = TempDir::new().expect("receiver dir");
    let sender_base = sender_dir.path().to_path_buf();
    let receiver_base = receiver_dir.path().to_path_buf();

    let sender_id = NodeId::new();
    let receiver_id = NodeId::new();

    let source_path = sender_base.join("resume.bin");
    let dest_path = receiver_base.join("resume.bin");
    write_pattern_file(&source_path, 512 * 1024)
        .await
        .expect("write source");

    let cluster = TestCluster::new();
    let sender = cluster.add_node(sender_id, sender_base.clone()).await;
    let receiver = cluster.add_node(receiver_id, receiver_base.clone()).await;

    let options = TransferOptions::default()
        .with_chunk_size(8 * 1024)
        .with_concurrency(1)
        .with_bandwidth_limit(Some(16 * 1024))
        .with_resumable(true);

    let send_task = {
        let sender = Arc::clone(&sender.service);
        let source_path = source_path.clone();
        let dest_path = dest_path.clone();
        tokio::spawn(async move {
            sender
                .send_file(receiver_id, &source_path, &dest_path, options)
                .await
        })
    };

    let session_id = wait_for_session_id(&sender).await;
    wait_for_partial_completion(receiver.service.receive_handler(), session_id).await;

    sender
        .service
        .cancel_transfer(session_id)
        .await
        .expect("cancel");
    let send_result = send_task.await.expect("send task");
    assert!(send_result.is_err());

    sender.shutdown().await;
    receiver.shutdown().await;

    let cluster = TestCluster::new();
    let sender = cluster.add_node(sender_id, sender_base.clone()).await;
    let _receiver = cluster.add_node(receiver_id, receiver_base.clone()).await;

    let handle = sender
        .service
        .resume_transfer(session_id)
        .await
        .expect("resume");
    let progress = handle.progress().await;
    assert_eq!(progress.bytes_transferred, 512 * 1024);

    assert_files_match(&source_path, &dest_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a dedicated 1 Gbps performance runner; set CHIRPS_FILE_TRANSFER_PERF_1GBPS=1"]
async fn file_transfer_throughput_meets_v0_5_1_target() {
    assert_eq!(
        std::env::var("CHIRPS_FILE_TRANSFER_PERF_1GBPS").as_deref(),
        Ok("1"),
        "run this benchmark only on the dedicated 1 Gbps performance runner"
    );

    const FILE_SIZE: usize = 128 * 1024 * 1024;
    const MIN_THROUGHPUT_BYTES_PER_SEC: f64 = 100_000_000.0;

    let cluster = TestCluster::new();
    let sender_dir = TempDir::new().expect("sender dir");
    let receiver_dir = TempDir::new().expect("receiver dir");
    let sender_id = NodeId::new();
    let receiver_id = NodeId::new();
    let sender = cluster
        .add_node(sender_id, sender_dir.path().to_path_buf())
        .await;
    let receiver = cluster
        .add_node(receiver_id, receiver_dir.path().to_path_buf())
        .await;

    let source_path = sender_dir.path().join("throughput-source.bin");
    let dest_path = receiver_dir.path().join("throughput-dest.bin");
    write_pattern_file(&source_path, FILE_SIZE)
        .await
        .expect("write source");

    let started = Instant::now();
    let handle = sender
        .service
        .send_file(
            receiver_id,
            &source_path,
            &dest_path,
            TransferOptions::default()
                .with_chunk_size(1024 * 1024)
                .with_concurrency(4),
        )
        .await
        .expect("transfer source");
    let elapsed = started.elapsed();
    let throughput = FILE_SIZE as f64 / elapsed.as_secs_f64();

    assert_eq!(handle.progress().await.bytes_transferred, FILE_SIZE as u64);
    assert_files_match(&source_path, &dest_path).await;
    receiver.shutdown().await;
    sender.shutdown().await;

    assert!(
        throughput >= MIN_THROUGHPUT_BYTES_PER_SEC,
        "throughput was {throughput:.0} bytes/s, below the v0.5.1 target of {MIN_THROUGHPUT_BYTES_PER_SEC:.0} bytes/s"
    );
}
