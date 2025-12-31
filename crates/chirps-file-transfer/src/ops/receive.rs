use crate::TransferSessionId;
use crate::chunk::ChunkTracker;
use crate::config::FileTransferConfig;
use crate::error::FileTransferError;
use crate::integrity::IntegrityVerifier;
use crate::manifest::TransferManifest;
use crate::ops::ControlDispatcher;
use crate::ops::conversions::from_wire_manifest;
use crate::options::TransferMode;
use crate::path::PathValidator;
use crate::session::{TransferKind, TransferSession, TransferState};
use alopex_chirps_wire::file_transfer::FileTransferMessage;
use alopex_chirps_wire::file_transfer::{ChunkAck, ManifestAck, TransferRequest, TransferResponse};
use alopex_chirps_wire::node_id::NodeId;
use quinn::RecvStream;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::stream::ChunkStreamCodec;

pub struct ReceiveHandler {
    config: FileTransferConfig,
    path_validator: PathValidator,
    sessions: Arc<RwLock<HashMap<TransferSessionId, TransferSession>>>,
}

#[derive(Debug, Clone, Copy)]
pub struct ReceiveOutcome {
    pub session_id: TransferSessionId,
    pub chunk_index: u32,
    pub verified: bool,
    pub completed: bool,
}

impl ReceiveHandler {
    pub fn new(
        config: FileTransferConfig,
        path_validator: PathValidator,
        sessions: Arc<RwLock<HashMap<TransferSessionId, TransferSession>>>,
    ) -> Self {
        ReceiveHandler {
            config,
            path_validator,
            sessions,
        }
    }

    pub async fn session_snapshot(&self, session_id: TransferSessionId) -> Option<TransferSession> {
        let sessions = self.sessions.read().await;
        sessions.get(&session_id).cloned()
    }

    pub async fn handle_transfer_request(
        &self,
        _session_id: TransferSessionId,
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

        Ok(TransferResponse {
            accepted: true,
            rejection_reason: None,
            existing_chunks: Vec::new(),
        })
    }

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
        let manifest_for_finalize = if is_empty {
            Some(manifest.clone())
        } else {
            None
        };

        let mut sessions = self.sessions.write().await;
        if sessions.contains_key(&manifest.session_id) {
            return Ok(ManifestAck {
                accepted: false,
                skip_chunks: Vec::new(),
                error: Some("session already exists".into()),
            });
        }

        let chunk_tracker = ChunkTracker::new(
            manifest.chunk_count,
            manifest.options.retry_policy.max_retries,
        );
        let options = manifest.options.clone();
        let transfer_mode = options.mode;
        let mut session = TransferSession::new(
            manifest.session_id,
            TransferKind::Send,
            transfer_mode,
            sender,
            Vec::new(),
            PathBuf::from(&manifest.source_path),
            dest_path,
            manifest,
            chunk_tracker,
            options,
        );
        session.state = TransferState::InProgress;
        sessions.insert(session.id, session);
        drop(sessions);

        if let Some(manifest) = manifest_for_finalize {
            write_chunk_to_temp(
                &self.config,
                &final_path,
                manifest.session_id,
                0,
                &[],
                manifest.file_size,
            )
            .await?;
            if let Err(err) = verify_and_finalize(
                &temp_path_for(&self.config, &final_path, manifest.session_id),
                &final_path,
                &manifest,
                transfer_mode,
            )
            .await
            {
                let mut sessions = self.sessions.write().await;
                sessions.remove(&manifest.session_id);
                return Ok(ManifestAck {
                    accepted: false,
                    skip_chunks: Vec::new(),
                    error: Some(err.to_string()),
                });
            }

            let mut sessions = self.sessions.write().await;
            if let Some(session) = sessions.get_mut(&manifest.session_id) {
                session.state = TransferState::Completed;
                session.updated_at = SystemTime::now();
            }
        }

        Ok(ManifestAck {
            accepted: true,
            skip_chunks: Vec::new(),
            error: None,
        })
    }

    pub async fn handle_chunk_stream(
        &self,
        sender: NodeId,
        control: &ControlDispatcher,
        recv: &mut RecvStream,
    ) -> Result<ReceiveOutcome, FileTransferError> {
        let (session_id, chunk_index, data) = ChunkStreamCodec::decode(recv).await?;
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

        send_chunk_ack(control, sender, session_id, chunk_index, true, None).await?;

        let completed = if session.chunk_tracker.is_complete() {
            let final_path = session.dest_path.clone();
            let temp_path = temp_path_for(&self.config, &session.dest_path, session_id);
            verify_and_finalize(
                &temp_path,
                &final_path,
                &session.manifest,
                session.options.mode,
            )
            .await?;
            session.state = TransferState::Completed;
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
