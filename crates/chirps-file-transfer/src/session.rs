use crate::TransferSessionId;
use crate::chunk::ChunkTracker;
use crate::error::FileTransferError;
use crate::manifest::TransferManifest;
use crate::options::{TransferMode, TransferOptions};
use alopex_chirps_wire::node_id::NodeId;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::sync::watch;

/// Kind of transfer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferKind {
    /// Single target send.
    Send,
    /// Broadcast to multiple targets.
    Broadcast,
    /// Sync operation.
    Sync,
}

/// State machine for a transfer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferState {
    /// Session is being initialized.
    Initializing,
    /// Transfer is in progress.
    InProgress,
    /// Transfer is paused.
    Paused,
    /// Transfer is verifying integrity.
    Verifying,
    /// Transfer completed successfully.
    Completed,
    /// Transfer failed.
    Failed,
    /// Transfer was cancelled.
    Cancelled,
}

/// Persistent record of an in-progress or completed transfer.
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
    #[serde(skip)]
    pub control: TransferControl,
}

/// Control state exposed to transfer loops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferControlState {
    /// Transfer should continue.
    Running,
    /// Transfer should wait without progressing.
    Paused,
    /// Transfer should abort.
    Cancelled,
}

/// Control channel for pausing or cancelling a transfer.
#[derive(Debug, Clone)]
pub struct TransferControl {
    state: watch::Sender<TransferControlState>,
}

impl Default for TransferControl {
    fn default() -> Self {
        let (state, _) = watch::channel(TransferControlState::Running);
        TransferControl { state }
    }
}

impl TransferControl {
    /// Subscribes to control state changes.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn subscribe(&self) -> watch::Receiver<TransferControlState> {
        self.state.subscribe()
    }

    /// Returns the current control state.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn state(&self) -> TransferControlState {
        *self.state.borrow()
    }

    /// Updates the control state.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn set_state(&self, state: TransferControlState) {
        let _ = self.state.send(state);
    }
}

/// Public snapshot of a transfer session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferSessionInfo {
    pub id: TransferSessionId,
    pub kind: TransferKind,
    pub mode: TransferMode,
    pub source_node: NodeId,
    pub target_nodes: Vec<NodeId>,
    pub source_path: PathBuf,
    pub dest_path: PathBuf,
    pub state: TransferState,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub error: Option<String>,
}

impl TransferSession {
    #[allow(clippy::too_many_arguments)]
    /// Creates a new transfer session in the initializing state.
    ///
    /// # Panics
    /// This method does not panic.
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
            control: TransferControl::default(),
        }
    }

    /// Transitions the session to a new state if allowed.
    ///
    /// # Errors
    /// Returns `FileTransferError::InvalidState` if the transition is not allowed.
    ///
    /// # Panics
    /// This method does not panic.
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

    /// Marks the session as failed with an error message.
    ///
    /// # Panics
    /// This method does not panic.
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

impl From<&TransferSession> for TransferSessionInfo {
    fn from(session: &TransferSession) -> Self {
        TransferSessionInfo {
            id: session.id,
            kind: session.kind,
            mode: session.mode,
            source_node: session.source_node,
            target_nodes: session.target_nodes.clone(),
            source_path: session.source_path.clone(),
            dest_path: session.dest_path.clone(),
            state: session.state,
            created_at: session.created_at,
            updated_at: session.updated_at,
            error: session.error.clone(),
        }
    }
}
