use crate::TransferSessionId;
use crate::bandwidth::BandwidthThrottle;
use crate::chunk::ChunkManager;
use crate::compression::compress_bytes;
use crate::config::FileTransferConfig;
use crate::error::FileTransferError;
use crate::integrity::IntegrityVerifier;
use crate::manifest::{FileMetadata, FileType, TransferManifest};
use crate::metrics::PrometheusMetrics;
use crate::ops::conversions::to_wire_manifest;
use crate::ops::conversions::to_wire_transfer_options;
use crate::ops::{ChunkStreamOpener, ControlDispatcher};
use crate::options::{CompressionAlgorithm, TransferOptions};
use crate::persistence::SessionPersistence;
use crate::progress::TransferHandle;
use crate::session::{TransferControlState, TransferKind, TransferSession, TransferState};
use alopex_chirps_wire::file_transfer::{
    ChunkAck, FileTransferMessage, ManifestAck, TransferComplete, TransferRequest, TransferResponse,
};
use alopex_chirps_wire::node_id::NodeId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::sync::{RwLock, mpsc};
use tokio::time::sleep;

/// Result of a send operation.
pub struct SendFileResult {
    pub session: TransferSession,
    pub handle: TransferHandle,
}

pub(crate) type SessionRegistry = Arc<RwLock<HashMap<TransferSessionId, TransferSession>>>;

#[allow(clippy::too_many_arguments)]
/// Sends a file to a target node.
///
/// # Errors
/// Returns `FileTransferError` when the source path is invalid, a local I/O operation
/// fails, the peer rejects the transfer, or control messages time out or fail to send.
///
/// # Panics
/// This function does not panic.
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
    send_file_with_context(
        control,
        stream_opener,
        config,
        source_node,
        target,
        source_path,
        dest_path,
        options,
        TransferKind::Send,
        None,
        None,
        None,
        None,
        None,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_file_with_context(
    control: Arc<ControlDispatcher>,
    stream_opener: Arc<dyn ChunkStreamOpener>,
    config: FileTransferConfig,
    source_node: NodeId,
    target: NodeId,
    source_path: &Path,
    dest_path: &Path,
    mut options: TransferOptions,
    kind: TransferKind,
    session_store: Option<SessionRegistry>,
    persistence: Option<Arc<SessionPersistence>>,
    metrics: Option<Arc<PrometheusMetrics>>,
    resume_session: Option<TransferSession>,
    requested_session_id: Option<TransferSessionId>,
    delete_source: bool,
) -> Result<SendFileResult, FileTransferError> {
    options = resolve_transfer_options(&config, options)?;
    let source_prepare_started = Instant::now();
    let metadata = fs::metadata(source_path).await?;
    if !metadata.is_file() {
        return Err(FileTransferError::FileNotFound(
            source_path.display().to_string(),
        ));
    }

    let file_size = metadata.len();
    let chunk_manager = ChunkManager::new(options.chunk_size);
    let chunk_count = chunk_manager.calculate_chunk_count(file_size);
    let source_path_str = source_path.display().to_string();
    let dest_path_str = dest_path.display().to_string();
    let session_id = if let Some(resume) = resume_session.as_ref() {
        if resume.kind != kind {
            return Err(FileTransferError::Internal(
                "resume session kind mismatch".into(),
            ));
        }
        if resume.source_node != source_node {
            return Err(FileTransferError::Internal(
                "resume source node mismatch".into(),
            ));
        }
        if resume.source_path.as_path() != source_path {
            return Err(FileTransferError::Internal(
                "resume source path mismatch".into(),
            ));
        }
        if resume.dest_path.as_path() != dest_path {
            return Err(FileTransferError::Internal(
                "resume destination path mismatch".into(),
            ));
        }
        if resume.target_nodes.first().copied() != Some(target) {
            return Err(FileTransferError::Internal("resume target mismatch".into()));
        }
        if !options.resumable || !resume.options.resumable {
            return Err(FileTransferError::Internal(
                "resume requested for non-resumable session".into(),
            ));
        }
        resume.id
    } else {
        requested_session_id.unwrap_or_default()
    };

    let (file_hash, chunks) = IntegrityVerifier::compute_file_hash_and_chunk_metas(
        source_path,
        options.hash_algorithm,
        chunk_manager.chunk_size(),
    )
    .await?;

    if let Some(resume) = resume_session.as_ref() {
        let manifest = &resume.manifest;
        if manifest.file_size != file_size
            || manifest.chunk_count != chunk_count
            || manifest.chunk_size != chunk_manager.chunk_size() as u32
            || manifest.hash_algorithm != options.hash_algorithm
            || manifest.file_hash != file_hash
            || manifest.source_path != source_path_str
            || manifest.dest_path != dest_path_str
        {
            return Err(FileTransferError::Internal(
                "resume manifest mismatch".into(),
            ));
        }
    }

    let manifest = build_manifest(
        source_path,
        dest_path,
        file_size,
        chunk_count,
        &chunk_manager,
        &options,
        file_hash,
        chunks,
        &metadata,
        session_id,
    )
    .await?;
    if let Some(metrics) = &metrics {
        metrics.observe_phase(
            "sender_source_prepare",
            source_prepare_started.elapsed(),
            file_size,
        );
    }

    let mut session = TransferSession::new(
        session_id,
        kind,
        options.mode,
        source_node,
        vec![target],
        source_path.to_path_buf(),
        dest_path.to_path_buf(),
        manifest,
        crate::chunk::ChunkTracker::new(chunk_count, options.retry_policy.max_retries),
        options.clone(),
    );
    if let Some(resume) = resume_session.as_ref() {
        session.created_at = resume.created_at;
        session.chunk_tracker.completed = resume.chunk_tracker.completed.clone();
    }
    session.state = TransferState::InProgress;
    session.updated_at = SystemTime::now();

    store_session(&session_store, &session).await;
    persist_session(&persistence, &session).await;
    if let Some(metrics) = &metrics {
        metrics.record_transfer(kind, "started");
        metrics.active_transfers.inc();
    }

    let handle = TransferHandle::new(file_size);
    let mut control_rx = session.control.subscribe();

    if let Err(err) = send_transfer_request(&control, target, &session).await {
        return Err(fail_session(
            &mut session,
            &session_store,
            &persistence,
            &metrics,
            kind,
            err,
        )
        .await);
    }
    let response =
        match wait_for_transfer_response(&control, session.id, config.manifest_timeout).await {
            Ok(response) => response,
            Err(err) => {
                return Err(fail_session(
                    &mut session,
                    &session_store,
                    &persistence,
                    &metrics,
                    kind,
                    err,
                )
                .await);
            }
        };
    if !response.accepted {
        let err = FileTransferError::Rejected(
            response
                .rejection_reason
                .unwrap_or_else(|| "transfer rejected".into()),
        );
        return Err(fail_session(
            &mut session,
            &session_store,
            &persistence,
            &metrics,
            kind,
            err,
        )
        .await);
    }

    if let Err(err) = send_manifest(&control, target, &session).await {
        return Err(fail_session(
            &mut session,
            &session_store,
            &persistence,
            &metrics,
            kind,
            err,
        )
        .await);
    }
    let manifest_ack =
        match wait_for_manifest_ack(&control, session.id, config.manifest_timeout).await {
            Ok(ack) => ack,
            Err(err) => {
                return Err(fail_session(
                    &mut session,
                    &session_store,
                    &persistence,
                    &metrics,
                    kind,
                    err,
                )
                .await);
            }
        };
    if !manifest_ack.accepted {
        let err = FileTransferError::Rejected(
            manifest_ack
                .error
                .unwrap_or_else(|| "manifest rejected".into()),
        );
        return Err(fail_session(
            &mut session,
            &session_store,
            &persistence,
            &metrics,
            kind,
            err,
        )
        .await);
    }

    let mut skip_chunks: HashSet<u32> = response.existing_chunks.into_iter().collect();
    skip_chunks.extend(manifest_ack.skip_chunks);
    for index in &skip_chunks {
        session.chunk_tracker.mark_completed(*index);
    }
    if !skip_chunks.is_empty() {
        let mut skipped_bytes = 0u64;
        let mut skipped_chunks = 0u32;
        for index in &skip_chunks {
            if let Some(meta) = session.manifest.chunks.get(*index as usize) {
                skipped_bytes = skipped_bytes.saturating_add(meta.size as u64);
                skipped_chunks = skipped_chunks.saturating_add(1);
            }
        }
        if skipped_bytes > 0 || skipped_chunks > 0 {
            handle.update_progress(skipped_bytes, skipped_chunks).await;
        }
    }

    let throttle = build_throttle(&config, &options);
    let mut pending: VecDeque<u32> = (0..chunk_count)
        .filter(|index| !skip_chunks.contains(index))
        .collect();
    let mut in_flight: HashMap<u32, Instant> = HashMap::new();
    let mut retry_counts: HashMap<u32, u8> = HashMap::new();
    let mut scheduled_retries = 0usize;
    let concurrency = options.concurrency.max(1).min(config.max_concurrency);

    let (send_tx, mut send_rx) =
        mpsc::channel::<(u32, Result<(), FileTransferError>)>(concurrency.saturating_mul(2));
    let (retry_tx, mut retry_rx) = mpsc::channel::<u32>(concurrency.saturating_mul(2));

    let start_time = Instant::now();

    while !pending.is_empty() || !in_flight.is_empty() || scheduled_retries != 0 {
        loop {
            let state = *control_rx.borrow();
            match state {
                TransferControlState::Running => break,
                TransferControlState::Paused => {
                    if control_rx.changed().await.is_err() {
                        return Err(cancel_session(
                            &mut session,
                            &session_store,
                            &persistence,
                            &metrics,
                            kind,
                        )
                        .await);
                    }
                }
                TransferControlState::Cancelled => {
                    return Err(cancel_session(
                        &mut session,
                        &session_store,
                        &persistence,
                        &metrics,
                        kind,
                    )
                    .await);
                }
            }
        }

        while in_flight.len() < concurrency && !pending.is_empty() {
            let index = pending.pop_front().expect("pending checked");
            in_flight.insert(index, Instant::now());
            update_chunks_in_flight(&metrics, in_flight.len());
            let send_tx = send_tx.clone();
            let opener = Arc::clone(&stream_opener);
            let throttle = throttle.clone();
            let source_path = source_path.to_path_buf();
            let session_id = session.id;
            let chunk_size = chunk_manager.chunk_size();
            let compression = options.compression;
            let metrics = metrics.clone();
            tokio::spawn(async move {
                let result = send_chunk(
                    opener,
                    target,
                    source_path,
                    session_id,
                    index,
                    chunk_size,
                    compression,
                    throttle,
                    metrics,
                )
                .await;
                let _ = send_tx.send((index, result)).await;
            });
        }

        tokio::select! {
            _ = control_rx.changed() => {
                if matches!(*control_rx.borrow(), TransferControlState::Cancelled) {
                    return Err(cancel_session(
                        &mut session,
                        &session_store,
                        &persistence,
                        &metrics,
                        kind,
                    )
                    .await);
                }
            }
            Some((index, result)) = send_rx.recv() => {
                if let Some(metrics) = &metrics
                    && result.is_ok()
                    && let Some(meta) = session.manifest.chunks.get(index as usize)
                {
                    metrics.record_chunk("sent", meta.size as u64);
                }
                if let Err(err) = result {
                    in_flight.remove(&index);
                    update_chunks_in_flight(&metrics, in_flight.len());
                    if let Some(attempt) =
                        should_retry(&mut retry_counts, index, options.retry_policy.max_retries)
                    {
                        if let Some(metrics) = &metrics {
                            metrics.record_retry();
                        }
                        schedule_retry(index, &options.retry_policy, attempt, &retry_tx);
                        scheduled_retries = scheduled_retries.saturating_add(1);
                    } else {
                        return Err(fail_session(
                            &mut session,
                            &session_store,
                            &persistence,
                            &metrics,
                            kind,
                            err,
                        )
                        .await);
                    }
                }
            }
            Some(retry_index) = retry_rx.recv() => {
                scheduled_retries = scheduled_retries.saturating_sub(1);
                if !session.chunk_tracker.completed.contains(&retry_index)
                    && !in_flight.contains_key(&retry_index)
                {
                    pending.push_back(retry_index);
                }
            }
            message = control.recv_any(session.id, config.chunk_timeout) => {
                let (_, message) = match message {
                    Ok(message) => message,
                    Err(err) => {
                        return Err(fail_session(
                            &mut session,
                            &session_store,
                            &persistence,
                            &metrics,
                            kind,
                            err,
                        )
                        .await);
                    }
                };
                match message {
                    FileTransferMessage::ChunkAck(ChunkAck { index, verified, error }) => {
                        let started = in_flight.remove(&index);
                        update_chunks_in_flight(&metrics, in_flight.len());
                        if started.is_none() {
                            continue;
                        }
                        if let Some(started) = started
                            && let Some(metrics) = &metrics
                        {
                            metrics.observe_chunk_latency(started.elapsed().as_secs_f64());
                        }
                        if verified {
                            session.chunk_tracker.mark_completed(index);
                            let meta = session.manifest.chunks.get(index as usize);
                            if let Some(meta) = meta {
                                handle.update_progress(meta.size as u64, 1).await;
                            }
                        } else {
                            session.chunk_tracker.mark_failed(index);
                            if let Some(metrics) = &metrics {
                                metrics.record_checksum_failure("chunk");
                            }
                            if let Some(attempt) =
                                should_retry(&mut retry_counts, index, options.retry_policy.max_retries)
                            {
                                if let Some(metrics) = &metrics {
                                    metrics.record_retry();
                                }
                                schedule_retry(index, &options.retry_policy, attempt, &retry_tx);
                                scheduled_retries = scheduled_retries.saturating_add(1);
                            } else {
                                return Err(fail_session(
                                    &mut session,
                                    &session_store,
                                    &persistence,
                                    &metrics,
                                    kind,
                                    FileTransferError::MaxRetriesExceeded { index },
                                )
                                .await);
                            }
                            if let Some(msg) = error {
                                session.fail(msg);
                            }
                        }
                    }
                    FileTransferMessage::Error(err) => {
                        return Err(fail_session(
                            &mut session,
                            &session_store,
                            &persistence,
                            &metrics,
                            kind,
                            FileTransferError::Transport(err.message),
                        )
                        .await);
                    }
                    FileTransferMessage::Cancel(_) => {
                        session.control.set_state(TransferControlState::Cancelled);
                        return Err(cancel_session(
                            &mut session,
                            &session_store,
                            &persistence,
                            &metrics,
                            kind,
                        )
                        .await);
                    }
                    _ => {}
                }
            }
        }
    }

    if !session.chunk_tracker.is_complete() {
        return Err(fail_session(
            &mut session,
            &session_store,
            &persistence,
            &metrics,
            kind,
            FileTransferError::Internal("transfer incomplete after sending chunks".into()),
        )
        .await);
    }

    // The receiver emits Complete only after it has verified the final hash,
    // restored requested metadata, and atomically renamed the temporary file.
    // Empty files are finalized while accepting the manifest, so its accepted
    // ManifestAck is already that completion proof.
    if skip_chunks.len() != chunk_count as usize
        && let Err(err) = wait_for_receiver_completion(
            &control,
            session.id,
            target,
            config.idle_timeout,
            &session.manifest.file_hash,
            session.manifest.hash_algorithm,
            file_size,
        )
        .await
    {
        return Err(fail_session(
            &mut session,
            &session_store,
            &persistence,
            &metrics,
            kind,
            err,
        )
        .await);
    }

    if delete_source
        && matches!(options.mode, crate::options::TransferMode::Move)
        && let Err(err) = fs::remove_file(source_path).await
    {
        return Err(fail_session(
            &mut session,
            &session_store,
            &persistence,
            &metrics,
            kind,
            FileTransferError::Io(err),
        )
        .await);
    }

    session.state = TransferState::Completed;
    session.updated_at = SystemTime::now();
    store_session(&session_store, &session).await;
    persist_session(&persistence, &session).await;
    if let Some(metrics) = &metrics {
        let duration_secs = start_time.elapsed().as_secs_f64();
        metrics.record_transfer(kind, "completed");
        metrics.active_transfers.dec();
        metrics.observe_transfer_duration(kind, duration_secs);
        if duration_secs > 0.0 {
            metrics.observe_throughput(kind, file_size as f64 / duration_secs);
        }
    }

    Ok(SendFileResult { session, handle })
}

async fn store_session(session_store: &Option<SessionRegistry>, session: &TransferSession) {
    if let Some(store) = session_store {
        let mut sessions = store.write().await;
        sessions.insert(session.id, session.clone());
    }
}

async fn persist_session(persistence: &Option<Arc<SessionPersistence>>, session: &TransferSession) {
    if let Some(persistence) = persistence
        && session.options.resumable
    {
        let _ = persistence.save(session).await;
    }
}

async fn fail_session(
    session: &mut TransferSession,
    session_store: &Option<SessionRegistry>,
    persistence: &Option<Arc<SessionPersistence>>,
    metrics: &Option<Arc<PrometheusMetrics>>,
    kind: TransferKind,
    err: FileTransferError,
) -> FileTransferError {
    session.fail(err.to_string());
    store_session(session_store, session).await;
    persist_session(persistence, session).await;
    if let Some(metrics) = metrics {
        metrics.record_transfer(kind, "failed");
        metrics.active_transfers.dec();
        metrics.set_chunks_in_flight(0);
    }
    err
}

async fn cancel_session(
    session: &mut TransferSession,
    session_store: &Option<SessionRegistry>,
    persistence: &Option<Arc<SessionPersistence>>,
    metrics: &Option<Arc<PrometheusMetrics>>,
    kind: TransferKind,
) -> FileTransferError {
    let mut was_active = true;
    if let Some(store) = session_store {
        let sessions = store.read().await;
        if let Some(existing) = sessions.get(&session.id) {
            was_active = matches!(
                existing.state,
                TransferState::Initializing
                    | TransferState::InProgress
                    | TransferState::Paused
                    | TransferState::Verifying
            );
        }
    }
    session.state = TransferState::Cancelled;
    session.error = Some("cancelled".into());
    session.updated_at = SystemTime::now();
    store_session(session_store, session).await;
    persist_session(persistence, session).await;
    if let Some(metrics) = metrics {
        if was_active {
            metrics.record_transfer(kind, "cancelled");
            metrics.active_transfers.dec();
        }
        metrics.set_chunks_in_flight(0);
    }
    FileTransferError::Cancelled
}

fn update_chunks_in_flight(metrics: &Option<Arc<PrometheusMetrics>>, in_flight: usize) {
    if let Some(metrics) = metrics {
        metrics.set_chunks_in_flight(in_flight as i64);
    }
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

#[allow(clippy::too_many_arguments)]
async fn wait_for_receiver_completion(
    control: &ControlDispatcher,
    session_id: TransferSessionId,
    target: NodeId,
    timeout: Duration,
    expected_hash: &[u8],
    expected_algorithm: crate::options::HashAlgorithm,
    expected_bytes: u64,
) -> Result<(), FileTransferError> {
    let (sender, message) = control
        .recv_filtered(session_id, timeout, |message| {
            matches!(
                message,
                FileTransferMessage::Complete(_)
                    | FileTransferMessage::Error(_)
                    | FileTransferMessage::Cancel(_)
            )
        })
        .await?;
    if sender != target {
        return Err(FileTransferError::Transport(
            "receiver completion came from an unexpected peer".into(),
        ));
    }

    match message {
        FileTransferMessage::Complete(TransferComplete {
            bytes_transferred,
            file_hash,
            hash_algorithm,
            ..
        }) if bytes_transferred == expected_bytes
            && file_hash == expected_hash
            && hash_algorithm == to_wire_hash_algorithm(expected_algorithm) =>
        {
            Ok(())
        }
        FileTransferMessage::Complete(_) => Err(FileTransferError::FileHashMismatch),
        FileTransferMessage::Error(error) => Err(FileTransferError::Transport(error.message)),
        FileTransferMessage::Cancel(_) => Err(FileTransferError::Cancelled),
        _ => Err(FileTransferError::Internal(
            "unexpected receiver completion message".into(),
        )),
    }
}

fn resolve_transfer_options(
    config: &FileTransferConfig,
    mut options: TransferOptions,
) -> Result<TransferOptions, FileTransferError> {
    let defaults = TransferOptions::default();
    if options.chunk_size == defaults.chunk_size {
        options.chunk_size = config.default_chunk_size;
    }
    if options.concurrency == defaults.concurrency {
        options.concurrency = config.default_concurrency;
    }
    if options.compression == defaults.compression {
        options.compression = config.default_compression;
    }

    if options.chunk_size == 0 || options.chunk_size > u32::MAX as usize {
        return Err(FileTransferError::Internal(
            "chunk size must be between 1 and u32::MAX".into(),
        ));
    }
    if config.max_concurrency == 0 || options.concurrency == 0 {
        return Err(FileTransferError::Internal(
            "transfer concurrency must be greater than zero".into(),
        ));
    }
    options.concurrency = options.concurrency.min(config.max_concurrency);
    Ok(options)
}

#[allow(clippy::too_many_arguments)]
async fn send_chunk(
    opener: Arc<dyn ChunkStreamOpener>,
    target: NodeId,
    source_path: PathBuf,
    session_id: TransferSessionId,
    index: u32,
    chunk_size: usize,
    compression: CompressionAlgorithm,
    throttle: Option<Arc<BandwidthThrottle>>,
    metrics: Option<Arc<PrometheusMetrics>>,
) -> Result<(), FileTransferError> {
    let read_started = Instant::now();
    let mut file = fs::File::open(&source_path).await?;
    let chunk_manager = ChunkManager::new(chunk_size);
    let chunk = chunk_manager
        .read_chunk(&mut file, index)
        .await
        .map_err(FileTransferError::Io)?;
    if let Some(metrics) = &metrics {
        metrics.observe_phase(
            "sender_chunk_read",
            read_started.elapsed(),
            chunk.data.len() as u64,
        );
    }

    let compression_started = Instant::now();
    let payload = compress_bytes(&chunk.data, compression)?;
    if let Some(metrics) = &metrics {
        metrics.observe_phase(
            "sender_chunk_compress",
            compression_started.elapsed(),
            payload.len() as u64,
        );
    }

    if let Some(throttle) = throttle {
        throttle.acquire(payload.len() as u64).await;
    }

    let stream_started = Instant::now();
    let mut stream = opener.open_chunk_stream(target).await?;
    crate::stream::ChunkStreamCodec::encode(&mut stream, &session_id, index, &payload).await?;
    stream
        .finish()
        .map_err(|e| FileTransferError::Transport(e.to_string()))?;
    // Do not wait for QUIC-level acknowledgement here. The transfer loop already
    // keeps this chunk in flight until the receiver verifies it and returns a
    // ChunkAck, so awaiting SendStream::stopped would duplicate that acknowledgement
    // and serialize every chunk on a transport round trip.
    if let Some(metrics) = &metrics {
        metrics.observe_phase(
            "sender_chunk_stream",
            stream_started.elapsed(),
            payload.len() as u64,
        );
    }
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
    chunks: Vec<crate::ChunkMeta>,
    metadata: &std::fs::Metadata,
    session_id: TransferSessionId,
) -> Result<TransferManifest, FileTransferError> {
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
        size: Some(metadata.len()),
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
        size: metadata.size,
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_transfer_options, send_chunk};
    use crate::compression::decompress_bytes;
    use crate::ops::ChunkStreamOpener;
    use crate::{
        CHUNK_STREAM_MAGIC, CompressionAlgorithm, FileTransferConfig, FileTransferError,
        TransferSessionId,
    };
    use alopex_chirps_wire::node_id::NodeId;
    use async_trait::async_trait;
    use quinn::{ClientConfig, Endpoint, ServerConfig};
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    const SERVER_NAME: &str = "localhost";

    #[test]
    fn config_defaults_are_applied_to_default_transfer_options() {
        let config = FileTransferConfig {
            default_chunk_size: 32 * 1024,
            default_concurrency: 3,
            default_compression: CompressionAlgorithm::ZstdLevel(5),
            max_concurrency: 4,
            ..FileTransferConfig::default()
        };
        let options = resolve_transfer_options(&config, crate::TransferOptions::default())
            .expect("resolve defaults");
        assert_eq!(options.chunk_size, 32 * 1024);
        assert_eq!(options.concurrency, 3);
        assert_eq!(options.compression, CompressionAlgorithm::ZstdLevel(5));
    }

    #[test]
    fn invalid_configured_transfer_defaults_are_rejected() {
        let config = FileTransferConfig {
            default_chunk_size: 0,
            ..FileTransferConfig::default()
        };
        assert!(resolve_transfer_options(&config, crate::TransferOptions::default()).is_err());
    }

    #[derive(Clone)]
    struct FixedChunkOpener {
        endpoint: Endpoint,
        target_addr: SocketAddr,
        connection: Arc<Mutex<Option<quinn::Connection>>>,
    }

    #[async_trait]
    impl ChunkStreamOpener for FixedChunkOpener {
        async fn open_chunk_stream(
            &self,
            _target: NodeId,
        ) -> Result<quinn::SendStream, FileTransferError> {
            let connection = {
                let mut connection = self.connection.lock().await;
                if let Some(existing) = connection.as_ref() {
                    existing.clone()
                } else {
                    let established = self
                        .endpoint
                        .connect(self.target_addr, SERVER_NAME)
                        .map_err(|error| FileTransferError::Transport(error.to_string()))?
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

    fn build_tls_configs() -> (ServerConfig, ClientConfig) {
        let cert = generate_simple_self_signed([SERVER_NAME.to_owned()]).expect("certificate");
        let cert_der = cert.serialize_der().expect("certificate DER");
        let key_der = cert.serialize_private_key_der();
        let cert_chain = vec![CertificateDer::from(cert_der.clone())];
        let key = PrivatePkcs8KeyDer::from(key_der).into();
        let server_config = ServerConfig::with_single_cert(cert_chain, key).expect("server TLS");

        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(cert_der))
            .expect("trusted certificate");
        let client_config =
            ClientConfig::with_root_certificates(Arc::new(roots)).expect("client config");
        (server_config, client_config)
    }

    #[tokio::test]
    async fn zstd_compresses_chunk_payload_on_the_wire() {
        let source = tempfile::tempdir().expect("source directory");
        let source_path = source.path().join("compressible.bin");
        let source_data = vec![b'x'; 64 * 1024];
        tokio::fs::write(&source_path, &source_data)
            .await
            .expect("source file");

        let (server_config, client_config) = build_tls_configs();
        let receiver = Endpoint::server(server_config, SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("receiver endpoint");
        let receiver_addr = receiver.local_addr().expect("receiver address");

        let mut sender =
            Endpoint::client(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("sender endpoint");
        sender.set_default_client_config(client_config);

        let received_payload = tokio::spawn(async move {
            let connection = receiver
                .accept()
                .await
                .expect("incoming connection")
                .await
                .expect("connection");
            let mut stream = connection.accept_uni().await.expect("chunk stream");
            let mut magic = [0u8; 1];
            stream.read_exact(&mut magic).await.expect("stream magic");
            assert_eq!(magic[0], CHUNK_STREAM_MAGIC);
            let (_, chunk_index, payload) = crate::stream::ChunkStreamCodec::decode(&mut stream)
                .await
                .expect("chunk frame");
            assert_eq!(chunk_index, 0);
            payload
        });

        let opener = Arc::new(FixedChunkOpener {
            endpoint: sender,
            target_addr: receiver_addr,
            connection: Arc::new(Mutex::new(None)),
        });
        send_chunk(
            opener.clone(),
            NodeId::new(),
            source_path,
            TransferSessionId::new(),
            0,
            64 * 1024,
            CompressionAlgorithm::Zstd,
            None,
            None,
        )
        .await
        .expect("send compressed chunk");

        let payload = received_payload.await.expect("receiver task");
        drop(opener);
        assert!(
            payload.len() < source_data.len(),
            "Zstd payload should be smaller than the original chunk"
        );
        assert_eq!(
            decompress_bytes(
                &payload,
                CompressionAlgorithm::Zstd,
                Some(source_data.len()),
            )
            .expect("decompress payload"),
            source_data
        );
    }
}
