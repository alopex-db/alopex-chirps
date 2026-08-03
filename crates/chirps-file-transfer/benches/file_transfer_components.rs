//! Local calibration benchmarks for the FileTransfer component boundaries.
//!
//! These measurements are intentionally not release assertions.  They let a
//! developer locate a regression in source preparation, chunk I/O,
//! compression, or the QUIC frame codec before running ft-1g-v1.

use alopex_chirps_file_transfer::{
    CHUNK_STREAM_MAGIC, ChunkManager, ChunkMeta, ChunkStreamCodec, ChunkTracker,
    CompressionAlgorithm, FileTransferConfig, HashAlgorithm, IntegrityVerifier, SessionPersistence,
    TransferKind, TransferManifest, TransferMode, TransferOptions, TransferSession,
    TransferSessionId, TransferState, compress_bytes,
};
use alopex_chirps_wire::node_id::NodeId;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use quinn::{ClientConfig, Connection, Endpoint, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, PrivateKey, RootCertStore};
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tempfile::TempDir;

const FILE_BYTES: usize = 128 * 1024 * 1024;
const CHUNK_BYTES: usize = 1024 * 1024;
const SERVER_NAME: &str = "localhost";

struct FileFixture {
    _directory: TempDir,
    path: std::path::PathBuf,
    chunk: Arc<Vec<u8>>,
}

impl FileFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("fixture directory");
        let path = directory.path().join("source-128mib.bin");
        let chunk = Arc::new(
            (0..CHUNK_BYTES)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let mut file = std::fs::File::create(&path).expect("fixture file");
        for _ in 0..(FILE_BYTES / CHUNK_BYTES) {
            file.write_all(chunk.as_slice()).expect("fixture payload");
        }
        file.sync_all().expect("sync fixture");
        Self {
            _directory: directory,
            path,
            chunk,
        }
    }
}

struct QuicFixture {
    _client_endpoint: Endpoint,
    _server_endpoint: Endpoint,
    client: Connection,
    server: Connection,
}

fn checkpoint_fixture() -> (TempDir, Arc<SessionPersistence>, TransferSession) {
    let directory = tempfile::tempdir().expect("checkpoint directory");
    let persistence = Arc::new(SessionPersistence::new(
        &FileTransferConfig::default()
            .with_base_path(directory.path().to_path_buf())
            .with_session_dir(Some(directory.path().join("sessions"))),
    ));
    let session_id = TransferSessionId::new();
    let chunk_count = (FILE_BYTES / CHUNK_BYTES) as u32;
    let chunks = (0..chunk_count)
        .map(|index| ChunkMeta {
            index,
            offset: index as u64 * CHUNK_BYTES as u64,
            size: CHUNK_BYTES as u32,
            checksum: 0,
        })
        .collect();
    let options = TransferOptions::default()
        .with_chunk_size(CHUNK_BYTES)
        .with_concurrency(4)
        .with_resumable(true);
    let manifest = TransferManifest {
        version: TransferManifest::CURRENT_VERSION,
        session_id,
        source_path: "source-128mib.bin".into(),
        dest_path: "destination-128mib.bin".into(),
        file_size: FILE_BYTES as u64,
        file_hash: vec![0; 32],
        hash_algorithm: HashAlgorithm::Sha256,
        chunk_size: CHUNK_BYTES as u32,
        chunk_count,
        chunks,
        metadata: None,
        options: options.clone(),
        created_at: 0,
    };
    let source_node = NodeId::new();
    let target_node = NodeId::new();
    let mut session = TransferSession::new(
        session_id,
        TransferKind::Send,
        TransferMode::Copy,
        source_node,
        vec![target_node],
        "source-128mib.bin".into(),
        "destination-128mib.bin".into(),
        manifest,
        ChunkTracker::new(chunk_count, options.retry_policy.max_retries),
        options,
    );
    session.state = TransferState::InProgress;
    (directory, persistence, session)
}

fn tls_configs() -> (ServerConfig, ClientConfig) {
    let certificate = generate_simple_self_signed([SERVER_NAME.to_string()]).expect("certificate");
    let certificate_der = certificate.serialize_der().expect("certificate DER");
    let key_der = certificate.serialize_private_key_der();
    let server = ServerConfig::with_single_cert(
        vec![Certificate(certificate_der.clone())],
        PrivateKey(key_der),
    )
    .expect("server config");

    let mut roots = RootCertStore::empty();
    roots
        .add(&Certificate(certificate_der))
        .expect("trusted certificate");
    let client = ClientConfig::new(Arc::new(
        rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));
    (server, client)
}

async fn quic_fixture() -> QuicFixture {
    let (server_config, client_config) = tls_configs();
    let server_endpoint =
        Endpoint::server(server_config, SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("server endpoint");
    let server_address = server_endpoint.local_addr().expect("server address");
    let mut client_endpoint =
        Endpoint::client(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("client endpoint");
    client_endpoint.set_default_client_config(client_config);

    let accept = {
        let server_endpoint = server_endpoint.clone();
        tokio::spawn(async move {
            server_endpoint
                .accept()
                .await
                .expect("connection")
                .await
                .expect("server connection")
        })
    };
    let client = client_endpoint
        .connect(server_address, SERVER_NAME)
        .expect("connect")
        .await
        .expect("client connection");
    let server = accept.await.expect("accept task");
    QuicFixture {
        _client_endpoint: client_endpoint,
        _server_endpoint: server_endpoint,
        client,
        server,
    }
}

fn benchmark_components(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let fixture = FileFixture::new();
    let path = fixture.path.clone();
    let chunk = Arc::clone(&fixture.chunk);
    let manager = Arc::new(ChunkManager::new(CHUNK_BYTES));
    let mut group = criterion.benchmark_group("file_transfer_components");

    group.throughput(Throughput::Bytes(FILE_BYTES as u64));
    group.bench_function("source_manifest_hash_128mib", |bench| {
        let path = path.clone();
        bench.to_async(&runtime).iter(|| {
            let path = path.clone();
            async move {
                black_box(
                    IntegrityVerifier::compute_file_hash_and_chunk_metas(
                        &path,
                        HashAlgorithm::Sha256,
                        CHUNK_BYTES,
                    )
                    .await
                    .expect("manifest and hash"),
                )
            }
        });
    });

    group.bench_function("receiver_finalize_hash_128mib", |bench| {
        let path = path.clone();
        bench.to_async(&runtime).iter(|| {
            let path = path.clone();
            async move {
                black_box(
                    IntegrityVerifier::compute_file_hash(&path, HashAlgorithm::Sha256)
                        .await
                        .expect("receiver final hash"),
                )
            }
        });
    });

    let (_checkpoint_directory, persistence, checkpoint_template) = checkpoint_fixture();
    group.bench_function("receiver_resume_checkpoints_128x1mib", |bench| {
        let persistence = Arc::clone(&persistence);
        let checkpoint_template = checkpoint_template.clone();
        bench.to_async(&runtime).iter(|| {
            let persistence = Arc::clone(&persistence);
            let mut session = checkpoint_template.clone();
            async move {
                for index in 0..session.manifest.chunk_count {
                    session.chunk_tracker.mark_completed(index);
                    persistence
                        .save(&session)
                        .await
                        .expect("persist receiver checkpoint");
                }
                black_box(session)
            }
        });
    });

    group.throughput(Throughput::Bytes(CHUNK_BYTES as u64));
    group.bench_function("source_open_and_read_1mib_chunk", |bench| {
        let path = path.clone();
        let manager = Arc::clone(&manager);
        bench.to_async(&runtime).iter(|| {
            let path = path.clone();
            let manager = Arc::clone(&manager);
            async move {
                let mut file = tokio::fs::File::open(path).await.expect("source open");
                black_box(manager.read_chunk(&mut file, 0).await.expect("source read"))
            }
        });
    });

    group.bench_function("compression_none_1mib", |bench| {
        let chunk = Arc::clone(&chunk);
        bench.iter(|| {
            black_box(
                compress_bytes(black_box(chunk.as_slice()), CompressionAlgorithm::None)
                    .expect("uncompressed payload"),
            )
        });
    });

    let quic = runtime.block_on(quic_fixture());
    group.bench_with_input(
        BenchmarkId::new("quic_codec_round_trip", "1mib"),
        &chunk,
        |bench, chunk| {
            let client = quic.client.clone();
            let server = quic.server.clone();
            bench.to_async(&runtime).iter(|| {
                let client = client.clone();
                let server = server.clone();
                let chunk = Arc::clone(chunk);
                async move {
                    let mut send = client.open_uni().await.expect("open stream");
                    ChunkStreamCodec::encode(&mut send, &TransferSessionId::new(), 0, &chunk)
                        .await
                        .expect("encode");
                    send.finish().await.expect("finish stream");

                    let mut receive = server.accept_uni().await.expect("accept stream");
                    let mut magic = [0u8; 1];
                    receive.read_exact(&mut magic).await.expect("read magic");
                    assert_eq!(magic[0], CHUNK_STREAM_MAGIC);
                    let (_, _, payload) = ChunkStreamCodec::decode(&mut receive)
                        .await
                        .expect("decode");
                    black_box(payload)
                }
            });
        },
    );
    group.finish();
}

criterion_group!(file_transfer_component_benches, benchmark_components);
criterion_main!(file_transfer_component_benches);
