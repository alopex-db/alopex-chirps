use crate::TransferSessionId;
use crate::config::FileTransferConfig;
use crate::error::FileTransferError;
use crate::manifest::FileMetadata;
use crate::metrics::PrometheusMetrics;
use crate::ops::ChunkStreamOpener;
use crate::ops::broadcast::broadcast_file_with_context;
use crate::ops::conversions::from_wire_file_metadata;
use crate::ops::send::send_file_with_context;
use crate::ops::sync::sync_file_with_context;
use crate::ops::{ControlDispatcher, ReceiveHandler};
use crate::options::{ListOptions, RemoveOptions, SortBy, SyncOptions, TransferOptions};
use crate::path::PathValidator;
use crate::persistence::SessionPersistence;
use crate::progress::{BroadcastHandle, SyncHandle, TransferHandle};
use crate::session::{TransferControlState, TransferSession, TransferSessionInfo, TransferState};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_wire::file_transfer::{
    CancelRequest, ExistsRequest, FileInfo, FileTransferMessage, ListRequest, MetadataRequest,
    RemoveRequest,
};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait FileTransferService: Send + Sync {
    async fn send_file(
        &self,
        target: NodeId,
        source_path: &Path,
        dest_path: &Path,
        options: TransferOptions,
    ) -> Result<TransferHandle, FileTransferError>;

    async fn broadcast_file(
        &self,
        source_path: &Path,
        dest_path: &Path,
        options: TransferOptions,
    ) -> Result<BroadcastHandle, FileTransferError>;

    async fn sync_file(
        &self,
        local_path: &Path,
        remote_path: &Path,
        targets: Option<Vec<NodeId>>,
        options: SyncOptions,
    ) -> Result<SyncHandle, FileTransferError>;

    async fn exists(&self, target: NodeId, path: &Path) -> Result<bool, FileTransferError>;
    async fn remove(
        &self,
        target: NodeId,
        path: &Path,
        options: RemoveOptions,
    ) -> Result<(), FileTransferError>;
    async fn metadata(
        &self,
        target: NodeId,
        path: &Path,
    ) -> Result<FileMetadata, FileTransferError>;
    async fn list_files(
        &self,
        target: NodeId,
        dir_path: &Path,
        options: ListOptions,
    ) -> Result<Vec<FileInfo>, FileTransferError>;

    async fn cancel_transfer(&self, session_id: TransferSessionId)
    -> Result<(), FileTransferError>;
    async fn pause_transfer(&self, session_id: TransferSessionId) -> Result<(), FileTransferError>;
    async fn resume_transfer(
        &self,
        session_id: TransferSessionId,
    ) -> Result<TransferHandle, FileTransferError>;
    fn active_transfers(&self) -> Vec<TransferSessionInfo>;
}

pub struct FileTransferServiceImpl {
    source_node: NodeId,
    backend: Arc<dyn MessageBackend>,
    control: Arc<ControlDispatcher>,
    stream_opener: Arc<dyn ChunkStreamOpener>,
    receive_handler: Arc<ReceiveHandler>,
    config: FileTransferConfig,
    path_validator: PathValidator,
    sessions: Arc<RwLock<HashMap<TransferSessionId, TransferSession>>>,
    persistence: Arc<SessionPersistence>,
    metrics: Option<Arc<PrometheusMetrics>>,
}

impl FileTransferServiceImpl {
    pub async fn new(
        source_node: NodeId,
        backend: Arc<dyn MessageBackend>,
        stream_opener: Arc<dyn ChunkStreamOpener>,
        config: FileTransferConfig,
    ) -> Result<Self, FileTransferError> {
        let receiver = backend
            .subscribe()
            .await
            .map_err(|err| FileTransferError::Transport(err.to_string()))?;
        let control = ControlDispatcher::new(Arc::clone(&backend), receiver);
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        let path_validator = PathValidator::new(config.base_path.clone(), false);
        let persistence = Arc::new(SessionPersistence::new(&config));
        let metrics = PrometheusMetrics::register().ok().map(Arc::new);
        let sync_sessions = Arc::new(RwLock::new(HashSet::new()));
        let receive_handler = Arc::new(ReceiveHandler::new(
            config.clone(),
            path_validator.clone(),
            Arc::clone(&sessions),
            Some(Arc::clone(&persistence)),
            metrics.clone(),
            Arc::clone(&sync_sessions),
        ));

        let cancel_control = Arc::clone(&control);
        let cancel_sessions = Arc::clone(&sessions);
        let cancel_persistence = Arc::clone(&persistence);
        let cancel_metrics = metrics.clone();
        let cancel_wait = config.idle_timeout;
        tokio::spawn(async move {
            loop {
                let message = cancel_control
                    .recv_any_filtered(cancel_wait, |_, msg| {
                        matches!(msg, FileTransferMessage::Cancel(_))
                    })
                    .await;
                let (session_id, _, message) = match message {
                    Ok(message) => message,
                    Err(FileTransferError::Timeout) => continue,
                    Err(FileTransferError::Transport(_)) => break,
                    Err(_) => continue,
                };
                let FileTransferMessage::Cancel(request) = message else {
                    continue;
                };
                let mut sessions = cancel_sessions.write().await;
                if let Some(session) = sessions.get_mut(&session_id) {
                    let was_active = matches!(
                        session.state,
                        TransferState::Initializing
                            | TransferState::InProgress
                            | TransferState::Paused
                            | TransferState::Verifying
                    );
                    session.state = TransferState::Cancelled;
                    session.error = Some(request.reason);
                    session.updated_at = std::time::SystemTime::now();
                    session.control.set_state(TransferControlState::Cancelled);
                    if session.options.resumable {
                        let _ = cancel_persistence.save(session).await;
                    }
                    if let Some(metrics) = &cancel_metrics {
                        metrics.record_transfer(session.kind, "cancelled");
                        if was_active {
                            metrics.active_transfers.dec();
                        }
                    }
                }
            }
        });

        Ok(FileTransferServiceImpl {
            source_node,
            backend,
            control,
            stream_opener,
            receive_handler,
            config,
            path_validator,
            sessions,
            persistence,
            metrics,
        })
    }

    pub fn control(&self) -> Arc<ControlDispatcher> {
        Arc::clone(&self.control)
    }

    pub fn receive_handler(&self) -> Arc<ReceiveHandler> {
        Arc::clone(&self.receive_handler)
    }

    pub fn path_validator(&self) -> &PathValidator {
        &self.path_validator
    }

    async fn record_session(&self, session: TransferSession) {
        let mut sessions = self.sessions.write().await;
        sessions.insert(session.id, session);
    }

    async fn load_session(
        &self,
        session_id: TransferSessionId,
    ) -> Result<TransferSession, FileTransferError> {
        if let Some(session) = self.sessions.read().await.get(&session_id).cloned() {
            return Ok(session);
        }
        let session = self.persistence.load(session_id).await?;
        self.record_session(session.clone()).await;
        Ok(session)
    }

    fn select_target(&self, targets: Option<Vec<NodeId>>) -> Result<NodeId, FileTransferError> {
        let mut candidates = match targets {
            Some(list) => list,
            None => self
                .backend
                .connected_peers()
                .into_iter()
                .map(|(node_id, _)| node_id)
                .collect(),
        };
        candidates.retain(|node_id| *node_id != self.source_node);
        match candidates.len() {
            1 => Ok(candidates[0]),
            0 => Err(FileTransferError::Internal(
                "no target nodes available for sync".into(),
            )),
            _ => Err(FileTransferError::Internal(
                "sync requires exactly one target node".into(),
            )),
        }
    }

    fn list_filter_match(entry: &FileInfo, options: &ListOptions) -> bool {
        if options.files_only
            && !matches!(
                entry.file_type,
                alopex_chirps_wire::file_transfer::FileType::File
            )
        {
            return false;
        }
        if options.directories_only
            && !matches!(
                entry.file_type,
                alopex_chirps_wire::file_transfer::FileType::Directory
            )
        {
            return false;
        }
        if let Some(pattern) = &options.pattern {
            let name = Path::new(&entry.path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if !name.contains(pattern) {
                return false;
            }
        }
        true
    }

    fn sort_files(files: &mut [FileInfo], sort_by: SortBy) {
        match sort_by {
            SortBy::Name => files.sort_by(|a, b| a.path.cmp(&b.path)),
            SortBy::Size => files.sort_by_key(|entry| entry.size),
            SortBy::ModifiedTime => files.sort_by_key(|entry| entry.modified_at),
        }
    }

    async fn collect_active_transfers(&self) -> Vec<TransferSessionInfo> {
        let sessions = self.sessions.read().await;
        sessions
            .values()
            .filter(|session| {
                matches!(
                    session.state,
                    TransferState::Initializing | TransferState::InProgress | TransferState::Paused
                )
            })
            .map(TransferSessionInfo::from)
            .collect()
    }
}

#[async_trait]
impl FileTransferService for FileTransferServiceImpl {
    async fn send_file(
        &self,
        target: NodeId,
        source_path: &Path,
        dest_path: &Path,
        options: TransferOptions,
    ) -> Result<TransferHandle, FileTransferError> {
        let result = send_file_with_context(
            Arc::clone(&self.control),
            Arc::clone(&self.stream_opener),
            self.config.clone(),
            self.source_node,
            target,
            source_path,
            dest_path,
            options.clone(),
            crate::session::TransferKind::Send,
            Some(Arc::clone(&self.sessions)),
            Some(Arc::clone(&self.persistence)),
            self.metrics.clone(),
            None,
            true,
        )
        .await?;
        Ok(result.handle)
    }

    async fn broadcast_file(
        &self,
        source_path: &Path,
        dest_path: &Path,
        options: TransferOptions,
    ) -> Result<BroadcastHandle, FileTransferError> {
        let targets: Vec<NodeId> = self
            .backend
            .connected_peers()
            .into_iter()
            .map(|(node_id, _)| node_id)
            .filter(|node_id| *node_id != self.source_node)
            .collect();
        if targets.is_empty() {
            return Err(FileTransferError::Internal(
                "no connected peers available for broadcast".into(),
            ));
        }
        let result = broadcast_file_with_context(
            Arc::clone(&self.control),
            Arc::clone(&self.stream_opener),
            self.config.clone(),
            self.source_node,
            targets,
            source_path,
            dest_path,
            options.clone(),
            Some(Arc::clone(&self.sessions)),
            Some(Arc::clone(&self.persistence)),
            self.metrics.clone(),
        )
        .await?;
        Ok(result.handle)
    }

    async fn sync_file(
        &self,
        local_path: &Path,
        remote_path: &Path,
        targets: Option<Vec<NodeId>>,
        options: SyncOptions,
    ) -> Result<SyncHandle, FileTransferError> {
        let target = self.select_target(targets)?;
        sync_file_with_context(
            Arc::clone(&self.control),
            Arc::clone(&self.stream_opener),
            Arc::clone(&self.receive_handler),
            self.config.clone(),
            self.source_node,
            target,
            local_path,
            remote_path,
            options,
            Some(Arc::clone(&self.sessions)),
            Some(Arc::clone(&self.persistence)),
            self.metrics.clone(),
        )
        .await
    }

    async fn exists(&self, target: NodeId, path: &Path) -> Result<bool, FileTransferError> {
        let session_id = TransferSessionId::new();
        self.control
            .send_message(
                target,
                session_id,
                FileTransferMessage::ExistsRequest(ExistsRequest {
                    path: path.display().to_string(),
                }),
            )
            .await?;
        let (_, message) = self
            .control
            .recv_filtered(session_id, self.config.manifest_timeout, |msg| {
                matches!(msg, FileTransferMessage::ExistsResponse(_))
            })
            .await?;
        match message {
            FileTransferMessage::ExistsResponse(response) => Ok(response.exists),
            _ => Err(FileTransferError::Internal(
                "unexpected exists response message".into(),
            )),
        }
    }

    async fn remove(
        &self,
        target: NodeId,
        path: &Path,
        options: RemoveOptions,
    ) -> Result<(), FileTransferError> {
        let session_id = TransferSessionId::new();
        self.control
            .send_message(
                target,
                session_id,
                FileTransferMessage::RemoveRequest(RemoveRequest {
                    path: path.display().to_string(),
                    recursive: options.recursive,
                }),
            )
            .await?;
        let (_, message) = self
            .control
            .recv_filtered(session_id, self.config.manifest_timeout, |msg| {
                matches!(msg, FileTransferMessage::RemoveResponse(_))
            })
            .await?;
        match message {
            FileTransferMessage::RemoveResponse(response) => {
                if response.success {
                    Ok(())
                } else {
                    Err(FileTransferError::Internal(
                        response.error.unwrap_or_else(|| "remove failed".into()),
                    ))
                }
            }
            _ => Err(FileTransferError::Internal(
                "unexpected remove response message".into(),
            )),
        }
    }

    async fn metadata(
        &self,
        target: NodeId,
        path: &Path,
    ) -> Result<FileMetadata, FileTransferError> {
        let session_id = TransferSessionId::new();
        self.control
            .send_message(
                target,
                session_id,
                FileTransferMessage::MetadataRequest(MetadataRequest {
                    path: path.display().to_string(),
                }),
            )
            .await?;
        let (_, message) = self
            .control
            .recv_filtered(session_id, self.config.manifest_timeout, |msg| {
                matches!(msg, FileTransferMessage::MetadataResponse(_))
            })
            .await?;
        match message {
            FileTransferMessage::MetadataResponse(response) => {
                if let Some(error) = response.error {
                    return Err(FileTransferError::Internal(error));
                }
                if !response.found {
                    return Err(FileTransferError::FileNotFound(path.display().to_string()));
                }
                let metadata = response.metadata.ok_or_else(|| {
                    FileTransferError::Internal("metadata response missing metadata".into())
                })?;
                let mut metadata = from_wire_file_metadata(metadata);
                if metadata.size.is_none() {
                    metadata.size = response.size;
                }
                Ok(metadata)
            }
            _ => Err(FileTransferError::Internal(
                "unexpected metadata response message".into(),
            )),
        }
    }

    async fn list_files(
        &self,
        target: NodeId,
        dir_path: &Path,
        options: ListOptions,
    ) -> Result<Vec<FileInfo>, FileTransferError> {
        let session_id = TransferSessionId::new();
        self.control
            .send_message(
                target,
                session_id,
                FileTransferMessage::ListRequest(ListRequest {
                    path: dir_path.display().to_string(),
                    recursive: options.recursive,
                    include_hidden: options.include_hidden,
                }),
            )
            .await?;
        let (_, message) = self
            .control
            .recv_filtered(session_id, self.config.manifest_timeout, |msg| {
                matches!(msg, FileTransferMessage::ListResponse(_))
            })
            .await?;
        let mut files = match message {
            FileTransferMessage::ListResponse(response) => {
                if let Some(error) = response.error {
                    return Err(FileTransferError::Internal(error));
                }
                response.files
            }
            _ => {
                return Err(FileTransferError::Internal(
                    "unexpected list response message".into(),
                ));
            }
        };

        files.retain(|entry| Self::list_filter_match(entry, &options));
        Self::sort_files(&mut files, options.sort_by);
        if options.limit > 0 && files.len() > options.limit {
            files.truncate(options.limit);
        }
        Ok(files)
    }

    async fn cancel_transfer(
        &self,
        session_id: TransferSessionId,
    ) -> Result<(), FileTransferError> {
        let session = self.load_session(session_id).await?;
        let targets = if session.target_nodes.is_empty() {
            vec![session.source_node]
        } else {
            session.target_nodes.clone()
        };
        for target in targets {
            self.control
                .send_message(
                    target,
                    session_id,
                    FileTransferMessage::Cancel(CancelRequest {
                        reason: "cancelled by user".into(),
                    }),
                )
                .await?;
        }

        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(&session_id) {
            let was_active = matches!(
                session.state,
                TransferState::Initializing
                    | TransferState::InProgress
                    | TransferState::Paused
                    | TransferState::Verifying
            );
            if session.state != TransferState::Cancelled {
                session.transition_to(TransferState::Cancelled)?;
            }
            session.control.set_state(TransferControlState::Cancelled);
            if session.options.resumable {
                self.persistence.save(session).await?;
            }
            if was_active && let Some(metrics) = &self.metrics {
                metrics.record_transfer(session.kind, "cancelled");
                metrics.active_transfers.dec();
            }
        }
        Ok(())
    }

    async fn pause_transfer(&self, session_id: TransferSessionId) -> Result<(), FileTransferError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(&session_id)
            .ok_or(FileTransferError::SessionNotFound(session_id))?;
        if session.state != TransferState::Paused {
            session.transition_to(TransferState::Paused)?;
            session.control.set_state(TransferControlState::Paused);
            if session.options.resumable {
                self.persistence.save(session).await?;
            }
        }
        Ok(())
    }

    async fn resume_transfer(
        &self,
        session_id: TransferSessionId,
    ) -> Result<TransferHandle, FileTransferError> {
        if self.sessions.read().await.contains_key(&session_id) {
            let mut sessions = self.sessions.write().await;
            let session = sessions
                .get_mut(&session_id)
                .ok_or(FileTransferError::SessionNotFound(session_id))?;
            if session.state == TransferState::Paused {
                session.transition_to(TransferState::InProgress)?;
                session.control.set_state(TransferControlState::Running);
            }

            let handle = TransferHandle::new(session.manifest.file_size);
            let completed_bytes = session
                .chunk_tracker
                .completed
                .iter()
                .filter_map(|index| session.manifest.chunks.get(*index as usize))
                .map(|meta| meta.size as u64)
                .sum();
            let completed_chunks = session.chunk_tracker.completed.len() as u32;
            handle
                .update_progress(completed_bytes, completed_chunks)
                .await;

            if session.options.resumable {
                self.persistence.save(session).await?;
            }

            return Ok(handle);
        }

        let session = self.persistence.load(session_id).await?;
        if session.source_node != self.source_node {
            return Err(FileTransferError::Internal(
                "resume is only supported for sender sessions".into(),
            ));
        }
        let target =
            session.target_nodes.first().copied().ok_or_else(|| {
                FileTransferError::Internal("resume session missing target".into())
            })?;
        let source_path = session.source_path.clone();
        let dest_path = session.dest_path.clone();
        let result = send_file_with_context(
            Arc::clone(&self.control),
            Arc::clone(&self.stream_opener),
            self.config.clone(),
            session.source_node,
            target,
            &source_path,
            &dest_path,
            session.options.clone(),
            session.kind,
            Some(Arc::clone(&self.sessions)),
            Some(Arc::clone(&self.persistence)),
            self.metrics.clone(),
            Some(session),
            true,
        )
        .await?;

        Ok(result.handle)
    }

    fn active_transfers(&self) -> Vec<TransferSessionInfo> {
        if let Ok(handle) = tokio::runtime::Handle::try_current()
            && matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            )
        {
            return tokio::task::block_in_place(|| {
                handle.block_on(self.collect_active_transfers())
            });
        }

        if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            return runtime.block_on(self.collect_active_transfers());
        }

        Vec::new()
    }
}
