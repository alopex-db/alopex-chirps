//! Physical two-host FileTransfer measurement harness.
//!
//! This is intentionally an example rather than a public `MeshHandle` API:
//! v0.5.2 does not expose FileTransfer construction from `MeshHandle`. It
//! exercises the public direct service constructor with a real Chirps QUIC
//! control backend and a separate real QUIC chunk endpoint on each host.

use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_core::config::NodeConfig;
use alopex_chirps_file_transfer::ops::ChunkStreamOpener;
use alopex_chirps_file_transfer::{
    CHUNK_STREAM_MAGIC, FileTransferConfig, FileTransferError, FileTransferService,
    FileTransferServiceImpl, HashAlgorithm, IntegrityVerifier, TransferOptions,
};
use alopex_chirps_transport_quic::{LogFormat, QuicBackend, TelemetryConfig, init_tracing};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use quinn::{ClientConfig, Connection, Endpoint, ServerConfig, TransportConfig};
use rustls::{Certificate, PrivateKey, RootCertStore};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep, timeout};

const SERVER_NAME: &str = "alopex.local";
const STREAM_WINDOW_BYTES: u32 = 16 * 1024 * 1024;
const CONNECTION_WINDOW_BYTES: u32 = 64 * 1024 * 1024;
const MAX_UNI_STREAMS: u32 = 256;

type DynError = Box<dyn Error + Send + Sync>;

#[derive(Debug)]
struct Arguments {
    role: String,
    node_id: NodeId,
    peer_id: NodeId,
    control_bind: SocketAddr,
    peer_control: SocketAddr,
    data_bind: SocketAddr,
    peer_data: SocketAddr,
    base_path: PathBuf,
    certificate: PathBuf,
    private_key: PathBuf,
    report: PathBuf,
    source_sha: String,
    scope: String,
    profile_id: Option<String>,
    image_digest: Option<String>,
    source: Option<PathBuf>,
    destination: PathBuf,
    expected_bytes: u64,
}

#[derive(Clone)]
struct DataStreamOpener {
    endpoint: Endpoint,
    peer_id: NodeId,
    peer_addr: SocketAddr,
    connection: Arc<Mutex<Option<Connection>>>,
}

#[async_trait]
impl ChunkStreamOpener for DataStreamOpener {
    async fn open_chunk_stream(
        &self,
        target: NodeId,
    ) -> Result<quinn::SendStream, FileTransferError> {
        if target != self.peer_id {
            return Err(FileTransferError::Transport(
                "unexpected chunk-stream peer".to_string(),
            ));
        }
        let connection = {
            let mut connection = self.connection.lock().await;
            if let Some(existing) = connection.as_ref() {
                existing.clone()
            } else {
                let connecting = self
                    .endpoint
                    .connect(self.peer_addr, SERVER_NAME)
                    .map_err(|error| FileTransferError::Transport(error.to_string()))?;
                let established = connecting
                    .await
                    .map_err(|error| FileTransferError::Transport(error.to_string()))?;
                connection.replace(established.clone());
                established
            }
        };
        connection
            .open_uni()
            .await
            .map_err(|error| FileTransferError::Transport(error.to_string()))
    }
}

fn usage() -> ! {
    eprintln!("Usage: two_node_transfer --role sender|receiver --node-id HEX --peer-id HEX");
    eprintln!("  --control-bind HOST:PORT --peer-control HOST:PORT --data-bind HOST:PORT");
    eprintln!("  --peer-data HOST:PORT --base-path DIR --cert CERT.DER --key KEY.DER");
    eprintln!(
        "  --report RESULT.ENV --source-sha GIT_SHA --scope SCOPE --destination RELATIVE_PATH"
    );
    eprintln!(
        "  [--profile-id ID --image-digest DIGEST] [--source RELATIVE_PATH] [--expected-bytes BYTES]"
    );
    std::process::exit(2);
}

fn input_error(message: impl Into<String>) -> DynError {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into()).into()
}

fn parse_node_id(value: &str) -> Result<NodeId, DynError> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(input_error(
            "node id must be exactly 32 hexadecimal characters",
        ));
    }
    let mut bytes = [0_u8; 16];
    for (index, output) in bytes.iter_mut().enumerate() {
        *output = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| input_error("invalid node id"))?;
    }
    Ok(NodeId::from(bytes))
}

fn parse_relative_path(value: &str, name: &str) -> Result<PathBuf, DynError> {
    let path = PathBuf::from(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(input_error(format!(
            "{name} must be a path relative to --base-path"
        )));
    }
    Ok(path)
}

fn required(values: &HashMap<String, String>, key: &str) -> Result<String, DynError> {
    values
        .get(key)
        .cloned()
        .ok_or_else(|| input_error(format!("missing required --{key}")))
}

fn parse_arguments() -> Result<Arguments, DynError> {
    let mut values = HashMap::new();
    let mut arguments = std::env::args().skip(1);
    while let Some(key) = arguments.next() {
        if matches!(key.as_str(), "-h" | "--help") {
            usage();
        }
        let Some(name) = key.strip_prefix("--") else {
            return Err(input_error(format!("unexpected argument {key}")));
        };
        let value = arguments
            .next()
            .ok_or_else(|| input_error(format!("missing value for --{name}")))?;
        if values.insert(name.to_string(), value).is_some() {
            return Err(input_error(format!("duplicate --{name}")));
        }
    }
    let role = required(&values, "role")?;
    if role != "sender" && role != "receiver" {
        return Err(input_error("--role must be sender or receiver"));
    }
    let expected_bytes = values
        .get("expected-bytes")
        .map(String::as_str)
        .unwrap_or("134217728")
        .parse::<u64>()?;
    if expected_bytes == 0 {
        return Err(input_error("--expected-bytes must be positive"));
    }
    let source = values
        .get("source")
        .map(|value| parse_relative_path(value, "source"))
        .transpose()?;
    if role == "sender" && source.is_none() {
        return Err(input_error("--source is required for sender"));
    }
    let scope = required(&values, "scope")?;
    if scope != "two-host-physical-network"
        && scope != "local-two-process"
        && scope != "two-container-controlled"
    {
        return Err(input_error(
            "--scope must be two-host-physical-network, local-two-process, or two-container-controlled",
        ));
    }
    let profile_id = values.get("profile-id").cloned();
    let image_digest = values.get("image-digest").cloned();
    if scope == "two-container-controlled" {
        let valid_profile = profile_id.as_deref().is_some_and(|value| {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        });
        if !valid_profile {
            return Err(input_error(
                "two-container-controlled requires a non-empty --profile-id containing only alphanumeric characters, '-', '_', or '.'",
            ));
        }
        if image_digest.as_deref().is_none_or(str::is_empty) {
            return Err(input_error(
                "two-container-controlled requires a non-empty --image-digest",
            ));
        }
    } else if profile_id.is_some() || image_digest.is_some() {
        return Err(input_error(
            "--profile-id and --image-digest are only valid with --scope two-container-controlled",
        ));
    }
    Ok(Arguments {
        role,
        node_id: parse_node_id(&required(&values, "node-id")?)?,
        peer_id: parse_node_id(&required(&values, "peer-id")?)?,
        control_bind: required(&values, "control-bind")?.parse()?,
        peer_control: required(&values, "peer-control")?.parse()?,
        data_bind: required(&values, "data-bind")?.parse()?,
        peer_data: required(&values, "peer-data")?.parse()?,
        base_path: PathBuf::from(required(&values, "base-path")?),
        certificate: PathBuf::from(required(&values, "cert")?),
        private_key: PathBuf::from(required(&values, "key")?),
        report: PathBuf::from(required(&values, "report")?),
        source_sha: required(&values, "source-sha")?,
        scope,
        profile_id,
        image_digest,
        source,
        destination: parse_relative_path(&required(&values, "destination")?, "destination")?,
        expected_bytes,
    })
}

fn performance_transport() -> Arc<TransportConfig> {
    let mut transport = TransportConfig::default();
    transport
        .stream_receive_window(STREAM_WINDOW_BYTES.into())
        .receive_window(CONNECTION_WINDOW_BYTES.into())
        .send_window(CONNECTION_WINDOW_BYTES as u64)
        .max_concurrent_uni_streams(MAX_UNI_STREAMS.into());
    Arc::new(transport)
}

fn data_tls_config(
    certificate: &Path,
    private_key: &Path,
) -> Result<(ServerConfig, ClientConfig), DynError> {
    let certificate_der = fs::read(certificate)?;
    let private_key_der = fs::read(private_key)?;
    let transport = performance_transport();
    let mut server = ServerConfig::with_single_cert(
        vec![Certificate(certificate_der.clone())],
        PrivateKey(private_key_der),
    )?;
    server.transport_config(Arc::clone(&transport));
    let mut roots = RootCertStore::empty();
    roots.add(&Certificate(certificate_der))?;
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut client = ClientConfig::new(Arc::new(crypto));
    client.transport_config(transport);
    Ok((server, client))
}

async fn serve_chunk_streams(
    endpoint: Endpoint,
    expected_peer_addr: SocketAddr,
    peer_id: NodeId,
    receive_handler: Arc<alopex_chirps_file_transfer::ops::ReceiveHandler>,
    control: Arc<alopex_chirps_file_transfer::ControlDispatcher>,
) {
    loop {
        let Some(connecting) = endpoint.accept().await else {
            break;
        };
        let receive_handler = Arc::clone(&receive_handler);
        let control = Arc::clone(&control);
        tokio::spawn(async move {
            let connection = match connecting.await {
                Ok(connection) => connection,
                Err(error) => {
                    eprintln!("chunk connection failed: {error}");
                    return;
                }
            };
            if connection.remote_address() != expected_peer_addr {
                eprintln!(
                    "rejected chunk connection from unexpected peer {}",
                    connection.remote_address()
                );
                return;
            }
            loop {
                let mut stream = match connection.accept_uni().await {
                    Ok(stream) => stream,
                    Err(_) => break,
                };
                let receive_handler = Arc::clone(&receive_handler);
                let control = Arc::clone(&control);
                tokio::spawn(async move {
                    let mut magic = [0_u8; 1];
                    if stream.read_exact(&mut magic).await.is_err()
                        || magic[0] != CHUNK_STREAM_MAGIC
                    {
                        return;
                    }
                    if let Err(error) = receive_handler
                        .handle_chunk_stream(peer_id, &control, &mut stream)
                        .await
                    {
                        eprintln!("chunk stream handling failed: {error}");
                    }
                });
            }
        });
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn write_report(path: &Path, values: &[(&str, String)]) -> Result<(), DynError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut text = String::new();
    for (key, value) in values {
        if value.contains('\n') || value.contains('\r') {
            return Err(input_error("report value contains a newline"));
        }
        text.push_str(key);
        text.push('=');
        text.push_str(value);
        text.push('\n');
    }
    fs::write(path, text)?;
    Ok(())
}

async fn wait_for_peer(backend: &QuicBackend, peer_id: NodeId) -> Result<(), DynError> {
    timeout(Duration::from_secs(30), async {
        loop {
            if backend
                .connected_peers()
                .iter()
                .any(|(id, _)| *id == peer_id)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| input_error("timed out waiting for Chirps QUIC control connection"))?;
    Ok(())
}

async fn run_sender(
    args: &Arguments,
    service: &FileTransferServiceImpl,
    backend: &QuicBackend,
) -> Result<(), DynError> {
    wait_for_peer(backend, args.peer_id).await?;
    let source_relative = args.source.as_ref().expect("validated sender source");
    let source = args.base_path.join(source_relative);
    let metadata = tokio::fs::metadata(&source).await?;
    if metadata.len() != args.expected_bytes {
        return Err(input_error(format!(
            "source size {} does not equal --expected-bytes {}",
            metadata.len(),
            args.expected_bytes
        )));
    }
    let started = Instant::now();
    let handle = service
        .send_file(
            args.peer_id,
            &source,
            &args.destination,
            TransferOptions::default()
                .with_chunk_size(1024 * 1024)
                .with_concurrency(4),
        )
        .await?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let progress = handle.progress().await;
    if progress.bytes_transferred != metadata.len() {
        return Err(input_error("sender reported incomplete transfer"));
    }
    let sha256 = hex(&IntegrityVerifier::compute_file_hash(&source, HashAlgorithm::Sha256).await?);
    write_report(
        &args.report,
        &[
            ("schema_version", "1".to_string()),
            ("kind", "chirps-file-transfer-two-node".to_string()),
            ("scope", args.scope.clone()),
            (
                "profile_id",
                args.profile_id
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "image_digest",
                args.image_digest
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            ("role", "sender".to_string()),
            ("source_sha", args.source_sha.clone()),
            ("control_plane", "chirps-quic".to_string()),
            ("data_plane", "quic-chunk-stream".to_string()),
            ("compression", "none".to_string()),
            ("file_bytes", metadata.len().to_string()),
            ("elapsed_seconds", format!("{elapsed_seconds:.9}")),
            (
                "end_to_end_bytes_per_second",
                format!("{:.0}", metadata.len() as f64 / elapsed_seconds),
            ),
            (
                "payload_bytes_per_second",
                format!("{:.0}", progress.throughput),
            ),
            ("sha256", sha256),
            ("completed", "true".to_string()),
        ],
    )
}

async fn run_receiver(args: &Arguments) -> Result<(), DynError> {
    let destination = args.base_path.join(&args.destination);
    timeout(Duration::from_secs(180), async {
        loop {
            if let Ok(metadata) = tokio::fs::metadata(&destination).await
                && metadata.len() == args.expected_bytes
            {
                let sha256 = hex(&IntegrityVerifier::compute_file_hash(
                    &destination,
                    HashAlgorithm::Sha256,
                )
                .await?);
                write_report(
                    &args.report,
                    &[
                        ("schema_version", "1".to_string()),
                        ("kind", "chirps-file-transfer-two-node".to_string()),
                        ("scope", args.scope.clone()),
                        (
                            "profile_id",
                            args.profile_id
                                .clone()
                                .unwrap_or_else(|| "none".to_string()),
                        ),
                        (
                            "image_digest",
                            args.image_digest
                                .clone()
                                .unwrap_or_else(|| "none".to_string()),
                        ),
                        ("role", "receiver".to_string()),
                        ("source_sha", args.source_sha.clone()),
                        ("control_plane", "chirps-quic".to_string()),
                        ("data_plane", "quic-chunk-stream".to_string()),
                        ("file_bytes", metadata.len().to_string()),
                        ("sha256", sha256),
                        ("completed", "true".to_string()),
                    ],
                )?;
                return Ok::<(), DynError>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| input_error("timed out waiting for atomically finalized destination"))??;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), DynError> {
    if std::env::var_os("CHIRPS_TWO_NODE_TRACE").is_some() {
        init_tracing(TelemetryConfig {
            log_format: LogFormat::Pretty,
            env_filter: "alopex_chirps_transport_quic=debug".to_string(),
            enable_ansi: false,
        });
    }
    let args = parse_arguments()?;
    fs::create_dir_all(&args.base_path)?;
    let (data_server, data_client) = data_tls_config(&args.certificate, &args.private_key)?;
    let mut data_endpoint = Endpoint::server(data_server, args.data_bind)?;
    data_endpoint.set_default_client_config(data_client);
    let opener: Arc<dyn ChunkStreamOpener> = Arc::new(DataStreamOpener {
        endpoint: data_endpoint.clone(),
        peer_id: args.peer_id,
        peer_addr: args.peer_data,
        connection: Arc::new(Mutex::new(None)),
    });

    let node_config = NodeConfig {
        bind_addr: args.control_bind,
        seeds: if args.role == "sender" {
            vec![args.peer_control]
        } else {
            Vec::new()
        },
        cert_path: Some(args.certificate.clone()),
        key_path: Some(args.private_key.clone()),
        trusted_cert_paths: vec![args.certificate.clone()],
        node_id_path: args.base_path.join("node-id"),
        ..NodeConfig::default()
    };
    let backend = Arc::new(QuicBackend::new(args.node_id, Arc::new(node_config)).await?);
    let backend_for_service: Arc<dyn MessageBackend> = backend.clone();
    let transfer_config = FileTransferConfig::default()
        .with_base_path(args.base_path.clone())
        .with_temp_dir(Some(args.base_path.join("tmp")))
        .with_session_dir(Some(args.base_path.join("sessions")));
    let service =
        FileTransferServiceImpl::new(args.node_id, backend_for_service, opener, transfer_config)
            .await?;
    let listener = tokio::spawn(serve_chunk_streams(
        data_endpoint,
        args.peer_data,
        args.peer_id,
        service.receive_handler(),
        service.control(),
    ));

    let result = if args.role == "sender" {
        run_sender(&args, &service, &backend).await
    } else {
        run_receiver(&args).await
    };
    listener.abort();
    let _ = listener.await;
    backend.close().await?;
    result
}
