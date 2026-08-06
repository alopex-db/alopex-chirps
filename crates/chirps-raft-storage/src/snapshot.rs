use crate::types::{ChirpsNodeId, GroupId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotCompletionKind {
    Built,
    Installed,
}

/// Emitted only after snapshot bytes have reached their durable checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotCompletionEvent {
    pub group_id: GroupId,
    pub node_id: ChirpsNodeId,
    pub snapshot_id: String,
    pub size_bytes: u64,
    pub digest: [u8; 32],
    pub kind: SnapshotCompletionKind,
}

pub trait SnapshotCompletionHook: Send + Sync + 'static {
    fn completed(&self, event: SnapshotCompletionEvent);
}

#[derive(Debug, Default)]
pub struct NoopSnapshotCompletionHook;

impl SnapshotCompletionHook for NoopSnapshotCompletionHook {
    fn completed(&self, _event: SnapshotCompletionEvent) {}
}

pub(crate) fn noop_completion_hook() -> Arc<dyn SnapshotCompletionHook> {
    Arc::new(NoopSnapshotCompletionHook)
}
