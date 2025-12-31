use crate::TransferSessionId;
use crate::bandwidth::BandwidthThrottle;
use crate::chunk::ChunkManager;
use crate::config::FileTransferConfig;
use crate::error::FileTransferError;
use crate::integrity::IntegrityVerifier;
use crate::manifest::{FileMetadata, FileType, TransferManifest};
use crate::ops::conversions::to_wire_manifest;
use crate::ops::conversions::to_wire_transfer_options;
use crate::ops::{ChunkStreamOpener, ControlDispatcher};
use crate::options::TransferOptions;
use crate::progress::TransferHandle;
use crate::session::{TransferKind, TransferSession, TransferState};
use alopex_chirps_wire::file_transfer::{
    ChunkAck, FileTransferMessage, ManifestAck, TransferComplete, TransferRequest, TransferResponse,
};
use alopex_chirps_wire::node_id::NodeId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time::sleep;

pub struct SendFileResult {
    pub session: TransferSession,
    pub handle: TransferHandle,
}

#[allow(clippy::too_many_arguments)]
pub async fn send_file(
    control: Arc<ControlDispatcher>,
    stream_opener: Arc<dyn ChunkStreamOpener>,
    config: FileTransferConfig,
    source_node: NodeId,
    target: NodeId,
    source_path: &Path,
    dest_path: &Path,
    options: TransferOptions,
) -> Result<SendFileResult, FileTransferError> {
    send_file_with_cleanup(
        control,
        stream_opener,
        config,
        source_node,
        target,
        source_path,
        dest_path,
        options,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_file_with_cleanup(
    control: Arc<ControlDispatcher>,
    stream_opener: Arc<dyn ChunkStreamOpener>,
    config: FileTransferConfig,
    source_node: NodeId,
    target: NodeId,
    source_path: &Path,
    dest_path: &Path,
    options: TransferOptions,
    delete_source: bool,
) -> Result<SendFileResult, FileTransferError> {
    let metadata = fs::metadata(source_path).await?;
    if !metadata.is_file() {
        return Err(FileTransferError::FileNotFound(
            source_path.display().to_string(),
        ));
    }

    let file_size = metadata.len();
    let chunk_manager = ChunkManager::new(options.chunk_size);
    let chunk_count = chunk_manager.calculate_chunk_count(file_size);

    let file_hash =
        IntegrityVerifier::compute_file_hash(source_path, options.hash_algorithm).await?;

    let session_id = TransferSessionId::new();
    let manifest = build_manifest(
        source_path,
        dest_path,
        file_size,
        chunk_count,
        &chunk_manager,
        &options,
        file_hash,
        &metadata,
        session_id,
    )
    .await?;

    let mut session = TransferSession::new(
        session_id,
        TransferKind::Send,
        options.mode,
        source_node,
        vec![target],
        source_path.to_path_buf(),
        dest_path.to_path_buf(),
        manifest,
        crate::chunk::ChunkTracker::new(chunk_count, options.retry_policy.max_retries),
        options.clone(),
    );
    session.state = TransferState::InProgress;
    session.updated_at = SystemTime::now();

    let handle = TransferHandle::new(file_size);

    send_transfer_request(&control, target, &session).await?;
    let response =
        wait_for_transfer_response(&control, session.id, config.manifest_timeout).await?;
    if !response.accepted {
        return Err(FileTransferError::Rejected(
            response
                .rejection_reason
                .unwrap_or_else(|| "transfer rejected".into()),
        ));
    }

    send_manifest(&control, target, &session).await?;
    let manifest_ack = wait_for_manifest_ack(&control, session.id, config.manifest_timeout).await?;
    if !manifest_ack.accepted {
        return Err(FileTransferError::Rejected(
            manifest_ack
                .error
                .unwrap_or_else(|| "manifest rejected".into()),
        ));
    }

    let mut skip_chunks: HashSet<u32> = response.existing_chunks.into_iter().collect();
    skip_chunks.extend(manifest_ack.skip_chunks);
    for index in &skip_chunks {
        session.chunk_tracker.mark_completed(*index);
    }

    let throttle = build_throttle(&config, &options);
    let mut pending: VecDeque<u32> = (0..chunk_count)
        .filter(|index| !skip_chunks.contains(index))
        .collect();
    let mut in_flight: HashSet<u32> = HashSet::new();
    let mut retry_counts: HashMap<u32, u8> = HashMap::new();
    let concurrency = options.concurrency.max(1).min(config.max_concurrency);

    let (send_tx, mut send_rx) =
        mpsc::channel::<(u32, Result<(), FileTransferError>)>(concurrency.saturating_mul(2));
    let (retry_tx, mut retry_rx) = mpsc::channel::<u32>(concurrency.saturating_mul(2));

    let start_time = Instant::now();

    while !pending.is_empty() || !in_flight.is_empty() {
        while in_flight.len() < concurrency && !pending.is_empty() {
            let index = pending.pop_front().expect("pending checked");
            in_flight.insert(index);
            let send_tx = send_tx.clone();
            let opener = Arc::clone(&stream_opener);
            let throttle = throttle.clone();
            let source_path = source_path.to_path_buf();
            let session_id = session.id;
            let chunk_size = chunk_manager.chunk_size();
            tokio::spawn(async move {
                let result = send_chunk(
                    opener,
                    target,
                    source_path,
                    session_id,
                    index,
                    chunk_size,
                    throttle,
                )
                .await;
                let _ = send_tx.send((index, result)).await;
            });
        }

        tokio::select! {
            Some((index, result)) = send_rx.recv() => {
                if let Err(err) = result {
                    in_flight.remove(&index);
                    if let Some(attempt) =
                        should_retry(&mut retry_counts, index, options.retry_policy.max_retries)
                    {
                        schedule_retry(index, &options.retry_policy, attempt, &retry_tx);
                    } else {
                        return Err(err);
                    }
                }
            }
            Some(retry_index) = retry_rx.recv() => {
                if !session.chunk_tracker.completed.contains(&retry_index)
                    && !in_flight.contains(&retry_index)
                {
                    pending.push_back(retry_index);
                }
            }
            message = control.recv_any(session.id, config.chunk_timeout) => {
                let (_, message) = message?;
                match message {
                    FileTransferMessage::ChunkAck(ChunkAck { index, verified, error }) => {
                        if !in_flight.contains(&index) {
                            continue;
                        }
                        in_flight.remove(&index);
                        if verified {
                            session.chunk_tracker.mark_completed(index);
                            let meta = session.manifest.chunks.get(index as usize);
                            if let Some(meta) = meta {
                                handle.update_progress(meta.size as u64, 1).await;
                            }
                        } else {
                            session.chunk_tracker.mark_failed(index);
                            if let Some(attempt) =
                                should_retry(&mut retry_counts, index, options.retry_policy.max_retries)
                            {
                                schedule_retry(index, &options.retry_policy, attempt, &retry_tx);
                            } else {
                                return Err(FileTransferError::MaxRetriesExceeded { index });
                            }
                            if let Some(msg) = error {
                                session.fail(msg);
                            }
                        }
                    }
                    FileTransferMessage::Error(err) => {
                        return Err(FileTransferError::Transport(err.message));
                    }
                    FileTransferMessage::Cancel(_) => {
                        return Err(FileTransferError::Cancelled);
                    }
                    _ => {}
                }
            }
        }
    }

    if !session.chunk_tracker.is_complete() {
        return Err(FileTransferError::Internal(
            "transfer incomplete after sending chunks".into(),
        ));
    }

    let duration_ms = start_time.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let complete = TransferComplete {
        bytes_transferred: file_size,
        duration_ms,
        file_hash: session.manifest.file_hash.clone(),
        hash_algorithm: to_wire_hash_algorithm(session.manifest.hash_algorithm),
    };
    control
        .send_message(
            session.target_nodes[0],
            session.id,
            FileTransferMessage::Complete(complete),
        )
        .await?;

    if delete_source && matches!(options.mode, crate::options::TransferMode::Move) {
        fs::remove_file(source_path).await?;
    }

    session.state = TransferState::Completed;
    session.updated_at = SystemTime::now();

    Ok(SendFileResult { session, handle })
}

async fn send_transfer_request(
    control: &ControlDispatcher,
    target: NodeId,
    session: &TransferSession,
) -> Result<(), FileTransferError> {
    let request = TransferRequest {
        source_path: session.manifest.source_path.clone(),
        dest_path: session.manifest.dest_path.clone(),
        file_size: session.manifest.file_size,
        chunk_count: session.manifest.chunk_count,
        chunk_size: session.manifest.chunk_size,
        mode: to_wire_transfer_mode(session.options.mode),
        options: to_wire_transfer_options(&session.options),
        metadata: session
            .manifest
            .metadata
            .as_ref()
            .map(to_wire_file_metadata),
    };
    control
        .send_message(
            target,
            session.id,
            FileTransferMessage::TransferRequest(request),
        )
        .await
}

async fn send_manifest(
    control: &ControlDispatcher,
    target: NodeId,
    session: &TransferSession,
) -> Result<(), FileTransferError> {
    let manifest = to_wire_manifest(&session.manifest);
    control
        .send_message(target, session.id, FileTransferMessage::Manifest(manifest))
        .await
}

async fn wait_for_transfer_response(
    control: &ControlDispatcher,
    session_id: TransferSessionId,
    timeout: Duration,
) -> Result<TransferResponse, FileTransferError> {
    let (_, message) = control
        .recv_filtered(session_id, timeout, |msg| {
            matches!(msg, FileTransferMessage::TransferResponse(_))
        })
        .await?;
    match message {
        FileTransferMessage::TransferResponse(response) => Ok(response),
        _ => Err(FileTransferError::Internal(
            "unexpected transfer response message".into(),
        )),
    }
}

async fn wait_for_manifest_ack(
    control: &ControlDispatcher,
    session_id: TransferSessionId,
    timeout: Duration,
) -> Result<ManifestAck, FileTransferError> {
    let (_, message) = control
        .recv_filtered(session_id, timeout, |msg| {
            matches!(msg, FileTransferMessage::ManifestAck(_))
        })
        .await?;
    match message {
        FileTransferMessage::ManifestAck(ack) => Ok(ack),
        _ => Err(FileTransferError::Internal(
            "unexpected manifest ack message".into(),
        )),
    }
}

async fn send_chunk(
    opener: Arc<dyn ChunkStreamOpener>,
    target: NodeId,
    source_path: PathBuf,
    session_id: TransferSessionId,
    index: u32,
    chunk_size: usize,
    throttle: Option<Arc<BandwidthThrottle>>,
) -> Result<(), FileTransferError> {
    let mut file = fs::File::open(&source_path).await?;
    let chunk_manager = ChunkManager::new(chunk_size);
    let chunk = chunk_manager
        .read_chunk(&mut file, index)
        .await
        .map_err(FileTransferError::Io)?;

    if let Some(throttle) = throttle {
        throttle.acquire(chunk.data.len() as u64).await;
    }

    let mut stream = opener.open_chunk_stream(target).await?;
    crate::stream::ChunkStreamCodec::encode(&mut stream, &session_id, index, &chunk.data).await?;
    stream
        .finish()
        .await
        .map_err(|e| FileTransferError::Transport(e.to_string()))?;
    Ok(())
}

fn build_throttle(
    config: &FileTransferConfig,
    options: &TransferOptions,
) -> Option<Arc<BandwidthThrottle>> {
    let limit = options
        .bandwidth_limit
        .or(config.global_bandwidth_limit)
        .unwrap_or(0);
    if limit == 0 {
        None
    } else {
        Some(Arc::new(BandwidthThrottle::new(limit)))
    }
}

#[allow(clippy::too_many_arguments)]
async fn build_manifest(
    source_path: &Path,
    dest_path: &Path,
    file_size: u64,
    chunk_count: u32,
    chunk_manager: &ChunkManager,
    options: &TransferOptions,
    file_hash: Vec<u8>,
    metadata: &std::fs::Metadata,
    session_id: TransferSessionId,
) -> Result<TransferManifest, FileTransferError> {
    let mut file = fs::File::open(source_path).await?;
    let chunks = chunk_manager
        .generate_chunk_metas(&mut file, file_size)
        .await
        .map_err(FileTransferError::Io)?;
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs();
    let file_metadata = if options.preserve_metadata {
        Some(build_file_metadata(metadata))
    } else {
        None
    };

    Ok(TransferManifest {
        version: TransferManifest::CURRENT_VERSION,
        session_id,
        source_path: source_path.display().to_string(),
        dest_path: dest_path.display().to_string(),
        file_size,
        file_hash,
        hash_algorithm: options.hash_algorithm,
        chunk_size: chunk_manager.chunk_size() as u32,
        chunk_count,
        chunks,
        metadata: file_metadata,
        options: options.clone(),
        created_at,
    })
}

fn build_file_metadata(metadata: &std::fs::Metadata) -> FileMetadata {
    let file_type = if metadata.is_dir() {
        FileType::Directory
    } else if metadata.is_file() {
        FileType::File
    } else {
        FileType::Symlink
    };

    FileMetadata {
        created_at: to_unix_seconds(metadata.created()),
        modified_at: to_unix_seconds(metadata.modified()),
        permissions: permissions_to_u32(metadata),
        file_type,
    }
}

fn to_unix_seconds(time: Result<SystemTime, std::io::Error>) -> Option<u64> {
    time.ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
}

#[cfg(unix)]
fn permissions_to_u32(metadata: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(metadata.permissions().mode())
}

#[cfg(not(unix))]
fn permissions_to_u32(_metadata: &std::fs::Metadata) -> Option<u32> {
    None
}

fn schedule_retry(
    index: u32,
    policy: &crate::options::RetryPolicy,
    attempt: u8,
    tx: &mpsc::Sender<u32>,
) {
    let delay = retry_delay(attempt, policy);
    let tx = tx.clone();
    tokio::spawn(async move {
        sleep(delay).await;
        let _ = tx.send(index).await;
    });
}

fn retry_delay(attempt: u8, policy: &crate::options::RetryPolicy) -> Duration {
    let base = policy.initial_delay.as_millis().max(1) as f64;
    let delay = base * policy.backoff_multiplier.powi(attempt as i32);
    let delay = delay.min(policy.max_delay.as_millis() as f64);
    Duration::from_millis(delay as u64)
}

fn should_retry(retries: &mut HashMap<u32, u8>, index: u32, max_retries: u8) -> Option<u8> {
    let entry = retries.entry(index).or_insert(0);
    if *entry >= max_retries {
        return None;
    }
    *entry = entry.saturating_add(1);
    Some(*entry)
}

fn to_wire_hash_algorithm(
    algorithm: crate::options::HashAlgorithm,
) -> alopex_chirps_wire::file_transfer::HashAlgorithm {
    match algorithm {
        crate::options::HashAlgorithm::Sha256 => {
            alopex_chirps_wire::file_transfer::HashAlgorithm::Sha256
        }
        crate::options::HashAlgorithm::Blake3 => {
            alopex_chirps_wire::file_transfer::HashAlgorithm::Blake3
        }
        crate::options::HashAlgorithm::XxHash64 => {
            alopex_chirps_wire::file_transfer::HashAlgorithm::XxHash64
        }
    }
}

fn to_wire_transfer_mode(
    mode: crate::options::TransferMode,
) -> alopex_chirps_wire::file_transfer::TransferMode {
    match mode {
        crate::options::TransferMode::Copy => alopex_chirps_wire::file_transfer::TransferMode::Copy,
        crate::options::TransferMode::Move => alopex_chirps_wire::file_transfer::TransferMode::Move,
    }
}

fn to_wire_file_metadata(
    metadata: &FileMetadata,
) -> alopex_chirps_wire::file_transfer::FileMetadata {
    alopex_chirps_wire::file_transfer::FileMetadata {
        created_at: metadata.created_at,
        modified_at: metadata.modified_at,
        permissions: metadata.permissions,
        file_type: match metadata.file_type {
            FileType::File => alopex_chirps_wire::file_transfer::FileType::File,
            FileType::Directory => alopex_chirps_wire::file_transfer::FileType::Directory,
            FileType::Symlink => alopex_chirps_wire::file_transfer::FileType::Symlink,
        },
    }
}
