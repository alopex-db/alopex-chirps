use crate::TransferSessionId;
use crate::error::FileTransferError;
use crate::metrics::PrometheusMetrics;
use crate::ops::conversions::from_wire_file_metadata;
use crate::ops::send::{SessionRegistry, send_file_with_context};
use crate::ops::{ChunkStreamOpener, ControlDispatcher, ReceiveHandler};
use crate::options::{ConflictResolution, SyncDirection, SyncOptions};
use crate::persistence::SessionPersistence;
use crate::progress::SyncHandle;
use crate::session::{TransferKind, TransferSession, TransferState};
use alopex_chirps_wire::file_transfer::{FileTransferMessage, MetadataRequest, MetadataResponse};
use alopex_chirps_wire::node_id::NodeId;
use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::time::{Instant, sleep};

#[allow(clippy::too_many_arguments)]
/// Synchronizes a local path with a remote path.
///
/// # Errors
/// Returns `FileTransferError` when local I/O fails, remote metadata requests fail,
/// or conflict resolution rejects the sync.
///
/// # Panics
/// This function does not panic.
pub async fn sync_file(
    control: Arc<ControlDispatcher>,
    stream_opener: Arc<dyn ChunkStreamOpener>,
    receive_handler: Arc<ReceiveHandler>,
    config: crate::config::FileTransferConfig,
    source_node: NodeId,
    target: NodeId,
    local_path: &Path,
    remote_path: &Path,
    options: SyncOptions,
) -> Result<SyncHandle, FileTransferError> {
    sync_file_with_context(
        control,
        stream_opener,
        receive_handler,
        config,
        source_node,
        target,
        local_path,
        remote_path,
        options,
        None,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn sync_file_with_context(
    control: Arc<ControlDispatcher>,
    stream_opener: Arc<dyn ChunkStreamOpener>,
    receive_handler: Arc<ReceiveHandler>,
    config: crate::config::FileTransferConfig,
    source_node: NodeId,
    target: NodeId,
    local_path: &Path,
    remote_path: &Path,
    options: SyncOptions,
    session_store: Option<SessionRegistry>,
    persistence: Option<Arc<SessionPersistence>>,
    metrics: Option<Arc<PrometheusMetrics>>,
) -> Result<SyncHandle, FileTransferError> {
    let local_metadata = fs::metadata(local_path).await.ok();
    let local_modified = local_metadata
        .as_ref()
        .and_then(|meta| meta.modified().ok())
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs());
    let local_size = local_metadata.as_ref().map(|meta| meta.len()).unwrap_or(0);

    let remote_metadata = request_remote_metadata(&control, target, remote_path).await?;
    let remote_modified = remote_metadata
        .metadata
        .as_ref()
        .and_then(|meta| meta.modified_at);

    let ordering = compare_modified(
        local_modified,
        remote_modified,
        options.clock_skew_tolerance,
    );

    let action = decide_action(
        options.direction,
        ordering,
        local_metadata.is_some(),
        remote_metadata.metadata.is_some(),
        options.conflict_resolution,
        local_path,
    )?;

    match action {
        SyncAction::None => {
            let handle = SyncHandle::new(local_size);
            handle.update_progress(local_size, 0).await;
            Ok(handle)
        }
        SyncAction::Push => {
            let result = send_file_with_context(
                control,
                stream_opener,
                config,
                source_node,
                target,
                local_path,
                remote_path,
                options.transfer.clone(),
                TransferKind::Sync,
                session_store.clone(),
                persistence.clone(),
                metrics.clone(),
                None,
                true,
            )
            .await?;
            let handle = SyncHandle::new(local_size);
            let progress = result.handle.progress().await;
            handle
                .update_progress(progress.bytes_transferred, progress.chunks_completed)
                .await;
            Ok(handle)
        }
        SyncAction::Pull => {
            wait_for_pull_transfer(
                &control,
                &receive_handler,
                target,
                local_path,
                remote_path,
                &config,
            )
            .await
        }
    }
}

fn decide_action(
    direction: SyncDirection,
    ordering: Option<Ordering>,
    local_exists: bool,
    remote_exists: bool,
    resolution: ConflictResolution,
    path: &Path,
) -> Result<SyncAction, FileTransferError> {
    match direction {
        SyncDirection::Push => decide_push(ordering, local_exists, remote_exists, resolution, path),
        SyncDirection::Pull => decide_pull(ordering, local_exists, remote_exists, resolution, path),
        SyncDirection::Bidirectional => {
            decide_bidirectional(ordering, local_exists, remote_exists, resolution, path)
        }
    }
}

fn decide_push(
    ordering: Option<Ordering>,
    local_exists: bool,
    remote_exists: bool,
    resolution: ConflictResolution,
    path: &Path,
) -> Result<SyncAction, FileTransferError> {
    if !local_exists {
        return Err(FileTransferError::FileNotFound(path.display().to_string()));
    }
    if !remote_exists {
        return Ok(SyncAction::Push);
    }
    match ordering {
        Some(Ordering::Greater) => Ok(SyncAction::Push),
        Some(Ordering::Less) => resolve_push_conflict(resolution, path),
        Some(Ordering::Equal) | None => Ok(SyncAction::None),
    }
}

fn decide_pull(
    ordering: Option<Ordering>,
    local_exists: bool,
    remote_exists: bool,
    resolution: ConflictResolution,
    path: &Path,
) -> Result<SyncAction, FileTransferError> {
    if !remote_exists {
        return Ok(SyncAction::None);
    }
    if !local_exists {
        return Ok(SyncAction::Pull);
    }
    match ordering {
        Some(Ordering::Less) => Ok(SyncAction::Pull),
        Some(Ordering::Greater) => resolve_pull_conflict(resolution, path),
        Some(Ordering::Equal) | None => Ok(SyncAction::None),
    }
}

fn decide_bidirectional(
    ordering: Option<Ordering>,
    local_exists: bool,
    remote_exists: bool,
    resolution: ConflictResolution,
    path: &Path,
) -> Result<SyncAction, FileTransferError> {
    match (local_exists, remote_exists) {
        (true, false) => Ok(SyncAction::Push),
        (false, true) => Ok(SyncAction::Pull),
        (false, false) => Ok(SyncAction::None),
        (true, true) => match resolution {
            ConflictResolution::Manual => Err(FileTransferError::SyncConflict {
                path: path.display().to_string(),
            }),
            ConflictResolution::SourceWins => Ok(SyncAction::Push),
            ConflictResolution::TargetWins => Ok(SyncAction::Pull),
            ConflictResolution::NewerWins => match ordering {
                Some(Ordering::Greater) => Ok(SyncAction::Push),
                Some(Ordering::Less) => Ok(SyncAction::Pull),
                Some(Ordering::Equal) | None => Ok(SyncAction::None),
            },
        },
    }
}

fn resolve_push_conflict(
    resolution: ConflictResolution,
    path: &Path,
) -> Result<SyncAction, FileTransferError> {
    match resolution {
        ConflictResolution::NewerWins | ConflictResolution::TargetWins => Ok(SyncAction::None),
        ConflictResolution::SourceWins => Ok(SyncAction::Push),
        ConflictResolution::Manual => Err(FileTransferError::SyncConflict {
            path: path.display().to_string(),
        }),
    }
}

fn resolve_pull_conflict(
    resolution: ConflictResolution,
    path: &Path,
) -> Result<SyncAction, FileTransferError> {
    match resolution {
        ConflictResolution::NewerWins | ConflictResolution::TargetWins => Ok(SyncAction::None),
        ConflictResolution::SourceWins => Ok(SyncAction::Pull),
        ConflictResolution::Manual => Err(FileTransferError::SyncConflict {
            path: path.display().to_string(),
        }),
    }
}

fn compare_modified(
    local: Option<u64>,
    remote: Option<u64>,
    tolerance: Duration,
) -> Option<Ordering> {
    let (Some(local), Some(remote)) = (local, remote) else {
        return None;
    };

    let tolerance = tolerance.as_secs();
    if local.abs_diff(remote) <= tolerance {
        None
    } else {
        Some(local.cmp(&remote))
    }
}

async fn request_remote_metadata(
    control: &ControlDispatcher,
    target: NodeId,
    remote_path: &Path,
) -> Result<RemoteMetadata, FileTransferError> {
    let session_id = TransferSessionId::new();
    control
        .send_message(
            target,
            session_id,
            FileTransferMessage::MetadataRequest(MetadataRequest {
                path: remote_path.display().to_string(),
            }),
        )
        .await?;
    let (_, message) = control
        .recv_filtered(session_id, Duration::from_secs(10), |msg| {
            matches!(msg, FileTransferMessage::MetadataResponse(_))
        })
        .await?;
    match message {
        FileTransferMessage::MetadataResponse(MetadataResponse {
            found, metadata, ..
        }) => {
            if found {
                Ok(RemoteMetadata {
                    metadata: metadata.map(from_wire_file_metadata),
                })
            } else {
                Ok(RemoteMetadata { metadata: None })
            }
        }
        _ => Err(FileTransferError::Internal(
            "unexpected metadata response".into(),
        )),
    }
}

async fn wait_for_pull_transfer(
    control: &ControlDispatcher,
    receive_handler: &ReceiveHandler,
    target: NodeId,
    local_path: &Path,
    remote_path: &Path,
    config: &crate::config::FileTransferConfig,
) -> Result<SyncHandle, FileTransferError> {
    let local_path_str = local_path.display().to_string();
    let remote_path_str = remote_path.display().to_string();
    let (session_id, sender, message) = control
        .recv_any_filtered(config.manifest_timeout, move |sender, msg| {
            if sender != target {
                return false;
            }
            match msg {
                FileTransferMessage::TransferRequest(request) => {
                    request.source_path.as_str() == remote_path_str.as_str()
                        && request.dest_path.as_str() == local_path_str.as_str()
                }
                _ => false,
            }
        })
        .await?;
    if sender != target {
        return Err(FileTransferError::Internal(
            "unexpected transfer request sender".into(),
        ));
    }
    let FileTransferMessage::TransferRequest(request) = message else {
        return Err(FileTransferError::Internal(
            "unexpected transfer request message".into(),
        ));
    };
    let response = receive_handler
        .handle_transfer_request(session_id, request)
        .await?;
    control
        .send_message(
            target,
            session_id,
            FileTransferMessage::TransferResponse(response.clone()),
        )
        .await?;
    if !response.accepted {
        return Err(FileTransferError::Rejected(
            response
                .rejection_reason
                .unwrap_or_else(|| "pull transfer rejected".into()),
        ));
    }
    receive_handler.mark_sync_session(session_id).await;

    let (sender, message) = control
        .recv_filtered(session_id, config.manifest_timeout, |msg| {
            matches!(msg, FileTransferMessage::Manifest(_))
        })
        .await?;
    if sender != target {
        return Err(FileTransferError::Internal(
            "unexpected pull manifest sender".into(),
        ));
    }
    let FileTransferMessage::Manifest(manifest) = message else {
        return Err(FileTransferError::Internal(
            "unexpected pull manifest message".into(),
        ));
    };
    let file_size = manifest.file_size;
    let manifest_ack = receive_handler.handle_manifest(sender, manifest).await?;
    let ack_accepted = manifest_ack.accepted;
    let ack_error = manifest_ack.error.clone();
    control
        .send_message(
            target,
            session_id,
            FileTransferMessage::ManifestAck(manifest_ack),
        )
        .await?;
    if !ack_accepted {
        return Err(FileTransferError::Rejected(
            ack_error.unwrap_or_else(|| "manifest rejected".into()),
        ));
    }

    let handle = SyncHandle::new(file_size);
    wait_for_completion(receive_handler, session_id, &handle, config.idle_timeout).await?;
    Ok(handle)
}

async fn wait_for_completion(
    receive_handler: &ReceiveHandler,
    session_id: TransferSessionId,
    handle: &SyncHandle,
    idle_timeout: Duration,
) -> Result<(), FileTransferError> {
    let mut last_bytes = 0u64;
    let mut last_chunks = 0u32;
    let mut last_activity = Instant::now();

    loop {
        let session = receive_handler
            .session_snapshot(session_id)
            .await
            .ok_or(FileTransferError::SessionNotFound(session_id))?;
        let (bytes, chunks) = progress_from_session(&session);

        if bytes != last_bytes || chunks != last_chunks {
            handle
                .update_progress(bytes - last_bytes, chunks - last_chunks)
                .await;
            last_bytes = bytes;
            last_chunks = chunks;
            last_activity = Instant::now();
        }

        match session.state {
            TransferState::Completed => {
                if bytes < session.manifest.file_size {
                    handle
                        .update_progress(session.manifest.file_size - bytes, 0)
                        .await;
                }
                return Ok(());
            }
            TransferState::Failed => {
                return Err(FileTransferError::Internal(
                    session.error.unwrap_or_else(|| "transfer failed".into()),
                ));
            }
            TransferState::Cancelled => return Err(FileTransferError::Cancelled),
            _ => {}
        }

        if last_activity.elapsed() > idle_timeout {
            return Err(FileTransferError::Timeout);
        }

        sleep(Duration::from_millis(200)).await;
    }
}

fn progress_from_session(session: &TransferSession) -> (u64, u32) {
    let chunks = session.chunk_tracker.completed.len() as u32;
    if session.chunk_tracker.is_complete() {
        return (session.manifest.file_size, chunks);
    }
    let bytes = session
        .chunk_tracker
        .completed
        .iter()
        .filter_map(|index| session.manifest.chunks.get(*index as usize))
        .map(|meta| meta.size as u64)
        .sum();
    (bytes, chunks)
}

#[derive(Debug, Clone)]
struct RemoteMetadata {
    metadata: Option<crate::manifest::FileMetadata>,
}

#[derive(Debug, Clone, Copy)]
enum SyncAction {
    Push,
    Pull,
    None,
}
