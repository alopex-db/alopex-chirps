use crate::TransferSessionId;
use crate::chunk::ChunkTracker;
use crate::error::FileTransferError;
use crate::manifest::TransferManifest;
use crate::options::{TransferMode, TransferOptions};
use alopex_chirps_wire::node_id::NodeId;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferKind {
    Send,
    Broadcast,
    Sync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferState {
    Initializing,
    InProgress,
    Paused,
    Verifying,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferSession {
    pub id: TransferSessionId,
    pub kind: TransferKind,
    pub mode: TransferMode,
    pub source_node: NodeId,
    pub target_nodes: Vec<NodeId>,
    pub source_path: PathBuf,
    pub dest_path: PathBuf,
    pub state: TransferState,
    pub manifest: TransferManifest,
    pub chunk_tracker: ChunkTracker,
    pub options: TransferOptions,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub error: Option<String>,
}

impl TransferSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: TransferSessionId,
        kind: TransferKind,
        mode: TransferMode,
        source_node: NodeId,
        target_nodes: Vec<NodeId>,
        source_path: PathBuf,
        dest_path: PathBuf,
        manifest: TransferManifest,
        chunk_tracker: ChunkTracker,
        options: TransferOptions,
    ) -> Self {
        let now = SystemTime::now();
        TransferSession {
            id,
            kind,
            mode,
            source_node,
            target_nodes,
            source_path,
            dest_path,
            state: TransferState::Initializing,
            manifest,
            chunk_tracker,
            options,
            created_at: now,
            updated_at: now,
            error: None,
        }
    }

    pub fn transition_to(&mut self, next: TransferState) -> Result<(), FileTransferError> {
        if !Self::can_transition(self.state, next) {
            return Err(FileTransferError::InvalidState {
                expected: format!("{:?}", self.state),
                actual: format!("{:?}", next),
            });
        }
        self.state = next;
        self.updated_at = SystemTime::now();
        if !matches!(next, TransferState::Failed) {
            self.error = None;
        }
        Ok(())
    }

    pub fn fail(&mut self, message: impl Into<String>) {
        self.state = TransferState::Failed;
        self.error = Some(message.into());
        self.updated_at = SystemTime::now();
    }

    fn can_transition(current: TransferState, next: TransferState) -> bool {
        match current {
            TransferState::Initializing => matches!(
                next,
                TransferState::InProgress | TransferState::Failed | TransferState::Cancelled
            ),
            TransferState::InProgress => matches!(
                next,
                TransferState::Paused
                    | TransferState::Verifying
                    | TransferState::Completed
                    | TransferState::Failed
                    | TransferState::Cancelled
            ),
            TransferState::Paused => matches!(
                next,
                TransferState::InProgress | TransferState::Failed | TransferState::Cancelled
            ),
            TransferState::Verifying => {
                matches!(next, TransferState::Completed | TransferState::Failed)
            }
            TransferState::Completed | TransferState::Failed | TransferState::Cancelled => false,
        }
    }
}
