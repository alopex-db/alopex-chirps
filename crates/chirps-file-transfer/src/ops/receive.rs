use crate::TransferSessionId;
use crate::chunk::ChunkTracker;
use crate::compression::decompress_bytes;
use crate::config::FileTransferConfig;
use crate::error::FileTransferError;
use crate::integrity::IntegrityVerifier;
use crate::manifest::TransferManifest;
use crate::metrics::PrometheusMetrics;
use crate::ops::ControlDispatcher;
use crate::ops::conversions::from_wire_manifest;
use crate::options::TransferMode;
use crate::path::PathValidator;
use crate::persistence::SessionPersistence;
use crate::session::{TransferControlState, TransferKind, TransferSession, TransferState};
use alopex_chirps_wire::file_transfer::FileTransferMessage;
use alopex_chirps_wire::file_transfer::{ChunkAck, ManifestAck, TransferRequest, TransferResponse};
use alopex_chirps_wire::node_id::NodeId;
use quinn::RecvStream;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::stream::ChunkStreamCodec;

/// Handles incoming transfer control messages and chunk data.
pub struct ReceiveHandler {
    config: FileTransferConfig,
    path_validator: PathValidator,
    sessions: Arc<RwLock<HashMap<TransferSessionId, TransferSession>>>,
    persistence: Option<Arc<SessionPersistence>>,
    metrics: Option<Arc<PrometheusMetrics>>,
    sync_sessions: Arc<RwLock<HashSet<TransferSessionId>>>,
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
            path_validator,
            sessions,
            persistence,
            metrics,
            sync_sessions,
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
        let dest_path = self
            .path_validator
            .validate(Path::new(&request.dest_path))?;

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

        let dest_path = self
            .path_validator
            .validate(Path::new(&manifest.dest_path))?;
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
            if is_empty {
                write_chunk_to_temp(
                    &self.config,
                    &final_path,
                    session_id,
                    0,
                    &[],
                    manifest.file_size,
                )
                .await?;
            }
            if let Err(err) = verify_and_finalize(
                &temp_path_for(&self.config, &final_path, session_id),
                &final_path,
                &manifest,
                transfer_mode,
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
                    return Err(FileTransferError::Cancelled);
                }
            }
        }

        let mut sessions = self.sessions.write().await;
        let session = match sessions.get_mut(&session_id) {
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

        let Some(meta) = session.manifest.chunks.get(chunk_index as usize) else {
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

        let checksum = IntegrityVerifier::compute_chunk_checksum(&data);
        if checksum != meta.checksum {
            session.chunk_tracker.mark_failed(chunk_index);
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

        write_chunk_to_temp(
            &self.config,
            &session.dest_path,
            session_id,
            meta.offset,
            &data,
            session.manifest.file_size,
        )
        .await?;

        session.chunk_tracker.mark_completed(chunk_index);
        session.updated_at = SystemTime::now();
        if let Some(metrics) = &self.metrics {
            metrics.record_chunk("received", data.len() as u64);
        }

        send_chunk_ack(control, sender, session_id, chunk_index, true, None).await?;

        let completed = if session.chunk_tracker.is_complete() {
            let final_path = session.dest_path.clone();
            let temp_path = temp_path_for(&self.config, &session.dest_path, session_id);
            if let Err(err) = verify_and_finalize(
                &temp_path,
                &final_path,
                &session.manifest,
                session.options.mode,
            )
            .await
            {
                if let Some(metrics) = &self.metrics {
                    if matches!(err, FileTransferError::FileHashMismatch) {
                        metrics.record_checksum_failure("file");
                    }
                    metrics.record_transfer(kind, "failed");
                    metrics.active_transfers.dec();
                }
                return Err(err);
            }
            session.state = TransferState::Completed;
            if let Some(metrics) = &self.metrics {
                metrics.record_transfer(kind, "completed");
                metrics.active_transfers.dec();
                if let Ok(duration) = SystemTime::now().duration_since(session.created_at) {
                    metrics.observe_transfer_duration(kind, duration.as_secs_f64());
                    if duration.as_secs_f64() > 0.0 {
                        metrics.observe_throughput(
                            kind,
                            session.manifest.file_size as f64 / duration.as_secs_f64(),
                        );
                    }
                }
            }
            if let Some(persistence) = &self.persistence
                && session.options.resumable
            {
                let _ = persistence.save(session).await;
            }
            true
        } else {
            false
        };

        Ok(ReceiveOutcome {
            session_id,
            chunk_index,
            verified: true,
            completed,
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

async fn verify_and_finalize(
    temp_path: &Path,
    dest_path: &Path,
    manifest: &TransferManifest,
    mode: TransferMode,
) -> Result<(), FileTransferError> {
    if manifest.options.verify_on_complete {
        let computed = IntegrityVerifier::compute_file_hash(temp_path, manifest.hash_algorithm)
            .await
            .map_err(FileTransferError::Io)?;
        if computed != manifest.file_hash {
            let _ = fs::remove_file(temp_path).await;
            return Err(FileTransferError::FileHashMismatch);
        }
    }

    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent).await?;
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

async fn write_chunk_to_temp(
    config: &FileTransferConfig,
    dest_path: &Path,
    session_id: TransferSessionId,
    offset: u64,
    data: &[u8],
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

    let metadata = file.metadata().await?;
    if metadata.len() < file_size {
        file.set_len(file_size).await?;
    }

    file.seek(std::io::SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
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
