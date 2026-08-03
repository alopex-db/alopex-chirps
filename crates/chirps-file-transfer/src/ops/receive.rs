use crate::TransferSessionId;
use crate::chunk::ChunkTracker;
use crate::compression::decompress_bytes;
use crate::config::FileTransferConfig;
use crate::error::FileTransferError;
use crate::integrity::{IncrementalFileHasher, IntegrityVerifier};
use crate::manifest::TransferManifest;
use crate::metrics::PrometheusMetrics;
use crate::ops::ControlDispatcher;
use crate::ops::conversions::from_wire_manifest;
use crate::options::TransferMode;
use crate::path::PathValidator;
use crate::persistence::SessionPersistence;
use crate::session::{TransferControlState, TransferKind, TransferSession, TransferState};
use alopex_chirps_wire::file_transfer::FileTransferMessage;
use alopex_chirps_wire::file_transfer::{
    ChunkAck, ManifestAck, TransferComplete, TransferErrorMessage, TransferRequest,
    TransferResponse,
};
use alopex_chirps_wire::node_id::NodeId;
use quinn::RecvStream;
use std::collections::{BTreeMap, HashMap, HashSet};
#[cfg(unix)]
use std::fs::OpenOptions as StdOpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::{self, OpenOptions};
#[cfg(not(unix))]
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};

use crate::stream::ChunkStreamCodec;

/// Handles incoming transfer control messages and chunk data.
pub struct ReceiveHandler {
    config: FileTransferConfig,
    _path_validator: PathValidator,
    sessions: Arc<RwLock<HashMap<TransferSessionId, TransferSession>>>,
    persistence: Option<Arc<SessionPersistence>>,
    metrics: Option<Arc<PrometheusMetrics>>,
    sync_sessions: Arc<RwLock<HashSet<TransferSessionId>>>,
    incremental_hashes: Mutex<HashMap<TransferSessionId, IncrementalReceiveHash>>,
}

struct IncrementalReceiveHash {
    next_index: u32,
    pending: BTreeMap<u32, Vec<u8>>,
    hasher: IncrementalFileHasher,
}

impl IncrementalReceiveHash {
    fn new(algorithm: crate::options::HashAlgorithm) -> Self {
        Self {
            next_index: 0,
            pending: BTreeMap::new(),
            hasher: IncrementalFileHasher::new(algorithm),
        }
    }

    fn record(&mut self, index: u32, data: Vec<u8>) -> u64 {
        if index < self.next_index || self.pending.contains_key(&index) {
            return 0;
        }
        self.pending.insert(index, data);
        let mut bytes_hashed = 0u64;
        while let Some(data) = self.pending.remove(&self.next_index) {
            bytes_hashed = bytes_hashed.saturating_add(data.len() as u64);
            self.hasher.update(&data);
            self.next_index = self.next_index.saturating_add(1);
        }
        bytes_hashed
    }
}

/// Outcome of processing an incoming chunk stream.
#[derive(Debug, Clone, Copy)]
pub struct ReceiveOutcome {
    pub session_id: TransferSessionId,
    pub chunk_index: u32,
    pub verified: bool,
    pub completed: bool,
}

impl ReceiveHandler {
    /// Creates a new receive handler.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn new(
        config: FileTransferConfig,
        path_validator: PathValidator,
        sessions: Arc<RwLock<HashMap<TransferSessionId, TransferSession>>>,
        persistence: Option<Arc<SessionPersistence>>,
        metrics: Option<Arc<PrometheusMetrics>>,
        sync_sessions: Arc<RwLock<HashSet<TransferSessionId>>>,
    ) -> Self {
        ReceiveHandler {
            config,
            _path_validator: path_validator,
            sessions,
            persistence,
            metrics,
            sync_sessions,
            incremental_hashes: Mutex::new(HashMap::new()),
        }
    }

    /// Returns a snapshot of a session if it is registered.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn session_snapshot(&self, session_id: TransferSessionId) -> Option<TransferSession> {
        let sessions = self.sessions.read().await;
        sessions.get(&session_id).cloned()
    }

    /// Marks a session as a sync transfer for lifecycle tracking.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn mark_sync_session(&self, session_id: TransferSessionId) {
        let mut sync_sessions = self.sync_sessions.write().await;
        sync_sessions.insert(session_id);
    }

    /// Discards non-persistent incremental state for a terminal session.
    pub(crate) async fn discard_incremental_hash(&self, session_id: TransferSessionId) {
        self.incremental_hashes.lock().await.remove(&session_id);
    }

    /// Validates an incoming transfer request and builds a response.
    ///
    /// # Errors
    /// Returns `FileTransferError` if path validation fails or persistence lookup fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn handle_transfer_request(
        &self,
        session_id: TransferSessionId,
        request: TransferRequest,
    ) -> Result<TransferResponse, FileTransferError> {
        let request_validator = PathValidator::new(
            self.config.base_path.clone(),
            request.options.follow_symlinks,
        );
        let dest_path = request_validator.validate(Path::new(&request.dest_path))?;

        if !request.options.overwrite && path_exists(&dest_path).await {
            return Ok(TransferResponse {
                accepted: false,
                rejection_reason: Some("destination already exists".into()),
                existing_chunks: Vec::new(),
            });
        }

        let mut existing_chunks = Vec::new();
        if request.options.resumable {
            let resume_session = match self.session_snapshot(session_id).await {
                Some(session) => Some(session),
                None => {
                    if let Some(persistence) = &self.persistence {
                        match persistence.load(session_id).await {
                            Ok(session) => Some(session),
                            Err(FileTransferError::SessionNotFound(_)) => None,
                            Err(err) => return Err(err),
                        }
                    } else {
                        None
                    }
                }
            };

            if let Some(session) = resume_session {
                if !session.options.resumable
                    || session.manifest.source_path != request.source_path
                    || session.manifest.dest_path != request.dest_path
                    || session.manifest.file_size != request.file_size
                    || session.manifest.chunk_count != request.chunk_count
                    || session.manifest.chunk_size != request.chunk_size
                {
                    return Ok(TransferResponse {
                        accepted: false,
                        rejection_reason: Some("resume session mismatch".into()),
                        existing_chunks: Vec::new(),
                    });
                }

                existing_chunks = session
                    .chunk_tracker
                    .completed
                    .iter()
                    .copied()
                    .filter(|index| *index < request.chunk_count)
                    .collect();
            }
        }

        Ok(TransferResponse {
            accepted: true,
            rejection_reason: None,
            existing_chunks,
        })
    }

    /// Validates and prepares to receive a transfer manifest.
    ///
    /// # Errors
    /// Returns `FileTransferError` if path validation or persistence access fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn handle_manifest(
        &self,
        sender: NodeId,
        manifest: alopex_chirps_wire::file_transfer::TransferManifest,
    ) -> Result<ManifestAck, FileTransferError> {
        let manifest = from_wire_manifest(manifest);
        if let Err(err) = manifest.validate() {
            return Ok(ManifestAck {
                accepted: false,
                skip_chunks: Vec::new(),
                error: Some(err.to_string()),
            });
        }

        let manifest_validator = PathValidator::new(
            self.config.base_path.clone(),
            manifest.options.follow_symlinks,
        );
        let dest_path = manifest_validator.validate(Path::new(&manifest.dest_path))?;
        let final_path = dest_path.clone();
        let is_empty = manifest.file_size == 0 || manifest.chunk_count == 0;
        let session_id = manifest.session_id;
        let resume_session = match self.session_snapshot(session_id).await {
            Some(session) => Some(session),
            None => {
                if manifest.options.resumable {
                    if let Some(persistence) = &self.persistence {
                        match persistence.load(session_id).await {
                            Ok(session) => Some(session),
                            Err(FileTransferError::SessionNotFound(_)) => None,
                            Err(err) => return Err(err),
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };
        if let Some(resume_session) = &resume_session {
            if !resume_session.options.resumable && !manifest.options.resumable {
                return Ok(ManifestAck {
                    accepted: false,
                    skip_chunks: Vec::new(),
                    error: Some("session already exists".into()),
                });
            }
            if resume_session.manifest.file_size != manifest.file_size
                || resume_session.manifest.chunk_count != manifest.chunk_count
                || resume_session.manifest.chunk_size != manifest.chunk_size
                || resume_session.manifest.file_hash != manifest.file_hash
                || resume_session.manifest.source_path != manifest.source_path
                || resume_session.manifest.dest_path != manifest.dest_path
            {
                return Ok(ManifestAck {
                    accepted: false,
                    skip_chunks: Vec::new(),
                    error: Some("resume manifest mismatch".into()),
                });
            }
        }

        // Do not create or resize a resumable transfer's temporary file until
        // the persisted manifest has been accepted.  A mismatched manifest
        // must be a read-only rejection of the existing resumable transfer.
        prepare_temp_file(&self.config, &final_path, session_id, manifest.file_size).await?;

        let chunk_tracker = ChunkTracker::new(
            manifest.chunk_count,
            manifest.options.retry_policy.max_retries,
        );
        let options = manifest.options.clone();
        let transfer_mode = options.mode;
        let mut sync_sessions = self.sync_sessions.write().await;
        let kind = if let Some(resume_session) = &resume_session {
            sync_sessions.remove(&session_id);
            resume_session.kind
        } else if sync_sessions.remove(&session_id) {
            TransferKind::Sync
        } else {
            TransferKind::Send
        };
        let manifest_clone = manifest.clone();
        let mut session = TransferSession::new(
            session_id,
            kind,
            transfer_mode,
            sender,
            Vec::new(),
            PathBuf::from(&manifest.source_path),
            dest_path,
            manifest_clone,
            chunk_tracker,
            options.clone(),
        );
        if let Some(resume_session) = &resume_session {
            session.created_at = resume_session.created_at;
            session.chunk_tracker.completed = resume_session
                .chunk_tracker
                .completed
                .iter()
                .copied()
                .filter(|index| *index < session.chunk_tracker.total_chunks)
                .collect();
        }
        session.state = TransferState::InProgress;
        session.updated_at = SystemTime::now();
        let skip_chunks: Vec<u32> = session
            .chunk_tracker
            .completed
            .iter()
            .copied()
            .filter(|index| *index < session.chunk_tracker.total_chunks)
            .collect();
        let mut sessions = self.sessions.write().await;
        sessions.insert(session.id, session);
        drop(sessions);

        let mut incremental_hashes = self.incremental_hashes.lock().await;
        incremental_hashes.remove(&session_id);
        if skip_chunks.is_empty() && !is_empty && options.verify_on_complete {
            incremental_hashes.insert(
                session_id,
                IncrementalReceiveHash::new(options.hash_algorithm),
            );
        }
        drop(incremental_hashes);

        if let Some(metrics) = &self.metrics {
            metrics.record_transfer(kind, "started");
            metrics.active_transfers.inc();
        }

        if let Some(persistence) = &self.persistence
            && options.resumable
            && let Some(session) = self.session_snapshot(session_id).await
        {
            let _ = persistence.save(&session).await;
        }

        let is_complete = if let Some(session) = self.session_snapshot(session_id).await {
            session.chunk_tracker.is_complete()
        } else {
            false
        };
        if is_empty || is_complete {
            if let Err(err) = verify_and_finalize(
                &temp_path_for(&self.config, &final_path, session_id),
                &final_path,
                &manifest,
                transfer_mode,
                None,
            )
            .await
            {
                let mut sessions = self.sessions.write().await;
                sessions.remove(&session_id);
                if let Some(metrics) = &self.metrics {
                    if matches!(err, FileTransferError::FileHashMismatch) {
                        metrics.record_checksum_failure("file");
                    }
                    metrics.record_transfer(kind, "failed");
                    metrics.active_transfers.dec();
                }
                return Ok(ManifestAck {
                    accepted: false,
                    skip_chunks: Vec::new(),
                    error: Some(err.to_string()),
                });
            }

            let mut sessions = self.sessions.write().await;
            if let Some(session) = sessions.get_mut(&session_id) {
                session.state = TransferState::Completed;
                session.updated_at = SystemTime::now();
            }
            if let Some(metrics) = &self.metrics {
                metrics.record_transfer(kind, "completed");
                metrics.active_transfers.dec();
                let created_at = UNIX_EPOCH.checked_add(Duration::from_secs(manifest.created_at));
                if let Some(created_at) = created_at
                    && let Ok(duration) = SystemTime::now().duration_since(created_at)
                {
                    metrics.observe_transfer_duration(kind, duration.as_secs_f64());
                    if duration.as_secs_f64() > 0.0 {
                        metrics.observe_throughput(
                            kind,
                            manifest.file_size as f64 / duration.as_secs_f64(),
                        );
                    }
                }
            }
        }

        Ok(ManifestAck {
            accepted: true,
            skip_chunks,
            error: None,
        })
    }

    /// Handles an incoming chunk stream and writes data to disk.
    ///
    /// # Errors
    /// Returns `FileTransferError` if chunk decoding fails, the session is not found,
    /// an I/O error occurs while writing, or the transfer is cancelled.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn handle_chunk_stream(
        &self,
        sender: NodeId,
        control: &ControlDispatcher,
        recv: &mut RecvStream,
    ) -> Result<ReceiveOutcome, FileTransferError> {
        let (session_id, chunk_index, payload) = ChunkStreamCodec::decode(recv).await?;
        let compression = {
            let sessions = self.sessions.read().await;
            sessions.get(&session_id).map(|session| {
                (
                    session.options.compression,
                    session
                        .manifest
                        .chunks
                        .get(chunk_index as usize)
                        .map(|chunk| chunk.size as usize),
                )
            })
        };

        let Some((compression, expected_size)) = compression else {
            return self
                .handle_chunk_data(sender, control, session_id, chunk_index, payload)
                .await;
        };
        let data = match decompress_bytes(&payload, compression, expected_size) {
            Ok(data) => data,
            Err(error) => {
                let _ = send_chunk_ack(
                    control,
                    sender,
                    session_id,
                    chunk_index,
                    false,
                    Some(format!("chunk decompression failed: {error}")),
                )
                .await;
                return Ok(ReceiveOutcome {
                    session_id,
                    chunk_index,
                    verified: false,
                    completed: false,
                });
            }
        };

        self.handle_chunk_data(sender, control, session_id, chunk_index, data)
            .await
    }

    async fn handle_chunk_data(
        &self,
        sender: NodeId,
        control: &ControlDispatcher,
        session_id: TransferSessionId,
        chunk_index: u32,
        data: Vec<u8>,
    ) -> Result<ReceiveOutcome, FileTransferError> {
        let (mut control_rx, kind) = {
            let sessions = self.sessions.read().await;
            let Some(session) = sessions.get(&session_id) else {
                let _ = send_chunk_ack(
                    control,
                    sender,
                    session_id,
                    chunk_index,
                    false,
                    Some("session not found".into()),
                )
                .await;
                return Err(FileTransferError::SessionNotFound(session_id));
            };
            (session.control.subscribe(), session.kind)
        };

        loop {
            let state = *control_rx.borrow();
            match state {
                TransferControlState::Running => break,
                TransferControlState::Paused => {
                    if control_rx.changed().await.is_err() {
                        return Err(FileTransferError::Cancelled);
                    }
                }
                TransferControlState::Cancelled => {
                    let mut sessions = self.sessions.write().await;
                    let mut was_active = false;
                    if let Some(session) = sessions.get_mut(&session_id) {
                        was_active = matches!(
                            session.state,
                            TransferState::Initializing
                                | TransferState::InProgress
                                | TransferState::Paused
                                | TransferState::Verifying
                        );
                        session.state = TransferState::Cancelled;
                        session.error = Some("cancelled".into());
                        session.updated_at = SystemTime::now();
                        if let Some(persistence) = &self.persistence
                            && session.options.resumable
                        {
                            let _ = persistence.save(session).await;
                        }
                    }
                    if was_active && let Some(metrics) = &self.metrics {
                        metrics.record_transfer(kind, "cancelled");
                        metrics.active_transfers.dec();
                    }
                    self.discard_incremental_hash(session_id).await;
                    return Err(FileTransferError::Cancelled);
                }
            }
        }

        // Snapshot only the immutable data needed for validation and disk I/O.
        // Stream handlers may write distinct offsets in parallel; holding the
        // session lock across an async file write would serialize every chunk.
        let (meta, dest_path) = {
            let sessions = self.sessions.read().await;
            let session = match sessions.get(&session_id) {
                Some(session) => session,
                None => {
                    let _ = send_chunk_ack(
                        control,
                        sender,
                        session_id,
                        chunk_index,
                        false,
                        Some("session not found".into()),
                    )
                    .await;
                    return Err(FileTransferError::SessionNotFound(session_id));
                }
            };
            let Some(meta) = session.manifest.chunks.get(chunk_index as usize).cloned() else {
                let _ = send_chunk_ack(
                    control,
                    sender,
                    session_id,
                    chunk_index,
                    false,
                    Some("invalid chunk index".into()),
                )
                .await;
                return Err(FileTransferError::ChunkChecksumMismatch { index: chunk_index });
            };
            (meta, session.dest_path.clone())
        };

        if data.len() != meta.size as usize {
            let _ = send_chunk_ack(
                control,
                sender,
                session_id,
                chunk_index,
                false,
                Some("chunk size mismatch".into()),
            )
            .await;
            return Err(FileTransferError::ChunkChecksumMismatch { index: chunk_index });
        }

        let verify_started = Instant::now();
        let checksum = IntegrityVerifier::compute_chunk_checksum(&data);
        if let Some(metrics) = &self.metrics {
            metrics.observe_phase(
                "receiver_chunk_verify",
                verify_started.elapsed(),
                data.len() as u64,
            );
        }
        if checksum != meta.checksum {
            let mut sessions = self.sessions.write().await;
            if let Some(session) = sessions.get_mut(&session_id) {
                session.chunk_tracker.mark_failed(chunk_index);
            }
            drop(sessions);
            if let Some(metrics) = &self.metrics {
                metrics.record_checksum_failure("chunk");
            }
            let _ = send_chunk_ack(
                control,
                sender,
                session_id,
                chunk_index,
                false,
                Some("checksum mismatch".into()),
            )
            .await;
            return Ok(ReceiveOutcome {
                session_id,
                chunk_index,
                verified: false,
                completed: false,
            });
        }

        let write_started = Instant::now();
        if let Err(error) =
            write_chunk_to_temp(&self.config, &dest_path, session_id, meta.offset, &data).await
        {
            self.discard_incremental_hash(session_id).await;
            return Err(error);
        }
        if let Some(metrics) = &self.metrics {
            metrics.observe_phase(
                "receiver_chunk_write",
                write_started.elapsed(),
                data.len() as u64,
            );
        }

        // Persist verified progress before acknowledging it to the sender. This
        // makes the receiver's persisted session the recovery source of truth
        // after either process is interrupted. Holding the session write lock
        // through the atomic save also prevents an older concurrent snapshot
        // from overwriting newer progress.
        //
        // Only the task that changes InProgress to Verifying owns finalization.
        // This keeps hash verification and rename single-shot even when streams
        // arrive concurrently and out of order.
        let data_len = data.len() as u64;
        let finalization = {
            let mut sessions = self.sessions.write().await;
            let session = match sessions.get_mut(&session_id) {
                Some(session) => session,
                None => return Err(FileTransferError::SessionNotFound(session_id)),
            };
            if matches!(
                session.state,
                TransferState::Cancelled | TransferState::Failed
            ) {
                return Err(FileTransferError::Cancelled);
            }
            session.chunk_tracker.mark_completed(chunk_index);
            session.updated_at = SystemTime::now();
            if session.options.resumable
                && let Some(persistence) = &self.persistence
                && let Err(error) = persistence.save(session).await
            {
                session.chunk_tracker.completed.remove(&chunk_index);
                session.chunk_tracker.mark_failed(chunk_index);
                session.updated_at = SystemTime::now();
                drop(sessions);
                send_chunk_ack(
                    control,
                    sender,
                    session_id,
                    chunk_index,
                    false,
                    Some(format!("failed to persist resume progress: {error}")),
                )
                .await?;
                return Ok(ReceiveOutcome {
                    session_id,
                    chunk_index,
                    verified: false,
                    completed: false,
                });
            }
            let hash_started = Instant::now();
            let bytes_hashed = self
                .incremental_hashes
                .lock()
                .await
                .get_mut(&session_id)
                .map(|state| state.record(chunk_index, data))
                .unwrap_or(0);
            if bytes_hashed > 0
                && let Some(metrics) = &self.metrics
            {
                metrics.observe_phase("receiver_file_hash", hash_started.elapsed(), bytes_hashed);
            }
            if session.chunk_tracker.is_complete() && session.state == TransferState::InProgress {
                session.state = TransferState::Verifying;
                Some((
                    session.dest_path.clone(),
                    session.manifest.clone(),
                    session.options.mode,
                ))
            } else {
                None
            }
        };

        if let Some(metrics) = &self.metrics {
            metrics.record_chunk("received", data_len);
        }
        send_chunk_ack(control, sender, session_id, chunk_index, true, None).await?;

        let Some((final_path, manifest, mode)) = finalization else {
            return Ok(ReceiveOutcome {
                session_id,
                chunk_index,
                verified: true,
                completed: false,
            });
        };

        let temp_path = temp_path_for(&self.config, &final_path, session_id);
        let incremental_hash = {
            let mut hashes = self.incremental_hashes.lock().await;
            hashes.remove(&session_id).and_then(|state| {
                (state.next_index == manifest.chunk_count && state.pending.is_empty())
                    .then(|| state.hasher.finalize())
            })
        };
        let finalize_started = Instant::now();
        if let Err(err) = verify_and_finalize(
            &temp_path,
            &final_path,
            &manifest,
            mode,
            incremental_hash.as_deref(),
        )
        .await
        {
            let persisted = {
                let mut sessions = self.sessions.write().await;
                let Some(session) = sessions.get_mut(&session_id) else {
                    return Err(FileTransferError::SessionNotFound(session_id));
                };
                session.fail(err.to_string());
                session.options.resumable.then(|| session.clone())
            };
            if let Some(persistence) = &self.persistence
                && let Some(session) = persisted.as_ref()
            {
                let _ = persistence.save(session).await;
            }
            if let Some(metrics) = &self.metrics {
                if matches!(err, FileTransferError::FileHashMismatch) {
                    metrics.record_checksum_failure("file");
                }
                metrics.record_transfer(kind, "failed");
                metrics.active_transfers.dec();
            }
            let _ = send_terminal_error(control, sender, session_id, &err).await;
            return Err(err);
        }
        if let Some(metrics) = &self.metrics {
            metrics.observe_phase(
                "receiver_finalize",
                finalize_started.elapsed(),
                manifest.file_size,
            );
        }

        let (persisted, duration_ms) = {
            let mut sessions = self.sessions.write().await;
            let Some(session) = sessions.get_mut(&session_id) else {
                return Err(FileTransferError::SessionNotFound(session_id));
            };
            session.state = TransferState::Completed;
            session.updated_at = SystemTime::now();
            let duration_ms = SystemTime::now()
                .duration_since(session.created_at)
                .unwrap_or(Duration::ZERO)
                .as_millis()
                .min(u64::MAX as u128) as u64;
            (
                session.options.resumable.then(|| session.clone()),
                duration_ms,
            )
        };
        if let Some(persistence) = &self.persistence
            && let Some(session) = persisted.as_ref()
        {
            let _ = persistence.save(session).await;
        }
        if let Some(metrics) = &self.metrics {
            metrics.record_transfer(kind, "completed");
            metrics.active_transfers.dec();
            if duration_ms > 0 {
                let seconds = duration_ms as f64 / 1_000.0;
                metrics.observe_transfer_duration(kind, seconds);
                metrics.observe_throughput(kind, manifest.file_size as f64 / seconds);
            }
        }
        control
            .send_message(
                sender,
                session_id,
                FileTransferMessage::Complete(TransferComplete {
                    bytes_transferred: manifest.file_size,
                    duration_ms,
                    file_hash: manifest.file_hash,
                    hash_algorithm: to_wire_hash_algorithm(manifest.hash_algorithm),
                }),
            )
            .await?;

        Ok(ReceiveOutcome {
            session_id,
            chunk_index,
            verified: true,
            completed: true,
        })
    }
}

async fn send_chunk_ack(
    control: &ControlDispatcher,
    sender: NodeId,
    session_id: TransferSessionId,
    chunk_index: u32,
    verified: bool,
    error: Option<String>,
) -> Result<(), FileTransferError> {
    control
        .send_message(
            sender,
            session_id,
            FileTransferMessage::ChunkAck(ChunkAck {
                index: chunk_index,
                verified,
                error,
            }),
        )
        .await
}

async fn send_terminal_error(
    control: &ControlDispatcher,
    sender: NodeId,
    session_id: TransferSessionId,
    error: &FileTransferError,
) -> Result<(), FileTransferError> {
    control
        .send_message(
            sender,
            session_id,
            FileTransferMessage::Error(TransferErrorMessage {
                code: error.code(),
                message: error.to_string(),
                recoverable: error.is_recoverable(),
            }),
        )
        .await
}

async fn verify_and_finalize(
    temp_path: &Path,
    dest_path: &Path,
    manifest: &TransferManifest,
    mode: TransferMode,
    precomputed_hash: Option<&[u8]>,
) -> Result<(), FileTransferError> {
    if manifest.options.verify_on_complete {
        let computed = match precomputed_hash {
            Some(hash) => hash.to_vec(),
            None => IntegrityVerifier::compute_file_hash(temp_path, manifest.hash_algorithm)
                .await
                .map_err(FileTransferError::Io)?,
        };
        if computed != manifest.file_hash {
            let _ = fs::remove_file(temp_path).await;
            return Err(FileTransferError::FileHashMismatch);
        }
    }

    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    if manifest.options.preserve_metadata
        && let Some(metadata) = &manifest.metadata
    {
        apply_preserved_metadata(temp_path, metadata)?;
    }

    if !manifest.options.overwrite && path_exists(dest_path).await {
        return Err(FileTransferError::FileAlreadyExists(
            dest_path.display().to_string(),
        ));
    }

    fs::rename(temp_path, dest_path).await?;

    if matches!(mode, TransferMode::Move) {
        // Receiver side never deletes source, but we keep mode for completeness.
    }

    Ok(())
}

#[cfg(unix)]
fn apply_preserved_metadata(
    path: &Path,
    metadata: &crate::manifest::FileMetadata,
) -> Result<(), FileTransferError> {
    use std::os::unix::fs::PermissionsExt;

    if let Some(mode) = metadata.permissions {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    }
    if let Some(modified_at) = metadata.modified_at {
        let seconds = i64::try_from(modified_at)
            .map_err(|_| FileTransferError::Internal("modified time does not fit in i64".into()))?;
        filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(seconds, 0))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn apply_preserved_metadata(
    _path: &Path,
    _metadata: &crate::manifest::FileMetadata,
) -> Result<(), FileTransferError> {
    Ok(())
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

async fn write_chunk_to_temp(
    config: &FileTransferConfig,
    dest_path: &Path,
    session_id: TransferSessionId,
    offset: u64,
    data: &[u8],
) -> Result<(), FileTransferError> {
    let temp_path = temp_path_for(config, dest_path, session_id);

    #[cfg(unix)]
    {
        let data = data.to_vec();
        let write_result = tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;

            let file = StdOpenOptions::new().write(true).open(&temp_path)?;
            let mut written = 0usize;
            while written < data.len() {
                let write_offset = offset.checked_add(written as u64).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "chunk offset overflow")
                })?;
                let bytes = file.write_at(&data[written..], write_offset)?;
                if bytes == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "chunk write made no progress",
                    ));
                }
                written += bytes;
            }
            Ok(())
        })
        .await
        .map_err(|error| {
            FileTransferError::Internal(format!("chunk write task failed: {error}"))
        })?;
        write_result.map_err(FileTransferError::Io)
    }

    #[cfg(not(unix))]
    {
        let mut file = OpenOptions::new().write(true).open(&temp_path).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        // Tokio requires a flush before drop when the next operation may read
        // the same file.  Unix uses positional blocking writes above, which
        // complete before the handler continues.
        file.flush().await?;
        Ok(())
    }
}

async fn prepare_temp_file(
    config: &FileTransferConfig,
    dest_path: &Path,
    session_id: TransferSessionId,
    file_size: u64,
) -> Result<(), FileTransferError> {
    let temp_path = temp_path_for(config, dest_path, session_id);
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(false)
        .open(&temp_path)
        .await?;
    file.set_len(file_size).await?;
    file.flush().await?;
    Ok(())
}

fn temp_path_for(
    config: &FileTransferConfig,
    dest_path: &Path,
    session_id: TransferSessionId,
) -> PathBuf {
    if let Some(temp_dir) = &config.temp_dir {
        return temp_dir.join(format!("session_{session_id}.tmp"));
    }

    let file_name = dest_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("transfer");
    dest_path.with_file_name(format!("{file_name}.{session_id}.part"))
}

async fn path_exists(path: &Path) -> bool {
    fs::metadata(path).await.is_ok()
}

#[cfg(test)]
mod tests {
    use super::IncrementalReceiveHash;
    use crate::options::HashAlgorithm;
    use sha2::{Digest, Sha256};

    #[test]
    fn incremental_receive_hash_reorders_chunks_and_ignores_duplicates() {
        let chunks = [b"first".to_vec(), b"second".to_vec(), b"third".to_vec()];
        let mut state = IncrementalReceiveHash::new(HashAlgorithm::Sha256);

        assert_eq!(state.record(1, chunks[1].clone()), 0);
        assert_eq!(state.record(1, chunks[1].clone()), 0);
        assert_eq!(
            state.record(0, chunks[0].clone()),
            (chunks[0].len() + chunks[1].len()) as u64
        );
        assert_eq!(state.record(2, chunks[2].clone()), chunks[2].len() as u64);
        assert_eq!(state.next_index, 3);
        assert!(state.pending.is_empty());

        let expected = Sha256::digest(chunks.concat()).to_vec();
        assert_eq!(state.hasher.finalize(), expected);
    }
}
