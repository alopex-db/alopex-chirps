use crate::config::FileTransferConfig;
use crate::error::FileTransferError;
use crate::metrics::PrometheusMetrics;
use crate::ops::send::{SessionRegistry, send_file_with_context};
use crate::ops::{ChunkStreamOpener, ControlDispatcher};
use crate::options::{TransferMode, TransferOptions};
use crate::persistence::SessionPersistence;
use crate::progress::BroadcastHandle;
use crate::session::{TransferKind, TransferSession, TransferState};
use alopex_chirps_wire::node_id::NodeId;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Result of a broadcast transfer request.
pub struct BroadcastResult {
    pub handle: BroadcastHandle,
    pub sessions: Vec<TransferSession>,
    pub errors: Vec<(NodeId, FileTransferError)>,
}

#[allow(clippy::too_many_arguments)]
/// Broadcasts a file to multiple targets.
///
/// # Errors
/// Returns `FileTransferError` if the source metadata cannot be read or if
/// removing the source file fails after a successful move.
///
/// # Panics
/// This function does not panic.
pub async fn broadcast_file(
    control: Arc<ControlDispatcher>,
    stream_opener: Arc<dyn ChunkStreamOpener>,
    config: FileTransferConfig,
    source_node: NodeId,
    targets: Vec<NodeId>,
    source_path: &Path,
    dest_path: &Path,
    options: TransferOptions,
) -> Result<BroadcastResult, FileTransferError> {
    broadcast_file_with_context(
        control,
        stream_opener,
        config,
        source_node,
        targets,
        source_path,
        dest_path,
        options,
        None,
        None,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn broadcast_file_with_context(
    control: Arc<ControlDispatcher>,
    stream_opener: Arc<dyn ChunkStreamOpener>,
    config: FileTransferConfig,
    source_node: NodeId,
    targets: Vec<NodeId>,
    source_path: &Path,
    dest_path: &Path,
    options: TransferOptions,
    session_store: Option<SessionRegistry>,
    persistence: Option<Arc<SessionPersistence>>,
    metrics: Option<Arc<PrometheusMetrics>>,
    transfer_slots: Option<Arc<Semaphore>>,
) -> Result<BroadcastResult, FileTransferError> {
    let metadata = fs::metadata(source_path).await?;
    let file_size = metadata.len();
    let handle = BroadcastHandle::new(targets.clone(), file_size);

    let mut join_set = JoinSet::new();
    for target in targets.clone() {
        let control = Arc::clone(&control);
        let stream_opener = Arc::clone(&stream_opener);
        let config = config.clone();
        let source_path = source_path.to_path_buf();
        let dest_path = dest_path.to_path_buf();
        let handle = handle.clone();
        let per_target_options = options.clone();
        let session_store = session_store.clone();
        let persistence = persistence.clone();
        let metrics = metrics.clone();
        let transfer_slots = transfer_slots.clone();
        join_set.spawn(async move {
            let _transfer_slot = match transfer_slots {
                Some(slots) => Some(slots.acquire_owned().await.map_err(|_| {
                    (
                        target,
                        FileTransferError::Internal("transfer slots are closed".into()),
                    )
                })?),
                None => None,
            };
            let result = send_file_with_context(
                control,
                stream_opener,
                config,
                source_node,
                target,
                &source_path,
                &dest_path,
                per_target_options,
                TransferKind::Broadcast,
                session_store,
                persistence,
                metrics,
                None,
                None,
                false,
            )
            .await;
            match result {
                Ok(res) => {
                    handle
                        .set_node_state(target, TransferState::Completed, None)
                        .await;
                    Ok(res.session)
                }
                Err(err) => {
                    handle
                        .set_node_state(target, TransferState::Failed, Some(err.to_string()))
                        .await;
                    Err((target, err))
                }
            }
        });
    }

    let mut sessions = Vec::new();
    let mut errors = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(session)) => sessions.push(session),
            Ok(Err(err)) => errors.push(err),
            Err(err) => {
                errors.push((
                    source_node,
                    FileTransferError::Internal(format!("broadcast task failed: {err}")),
                ));
            }
        }
    }

    if errors.is_empty() && matches!(options.mode, TransferMode::Move) {
        fs::remove_file(source_path).await?;
    }

    Ok(BroadcastResult {
        handle,
        sessions,
        errors,
    })
}
