use crate::session::TransferState;
use alopex_chirps_wire::node_id::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::Mutex;

/// Snapshot of transfer progress and throughput.
#[derive(Debug, Clone, Copy, Default)]
pub struct TransferProgress {
    pub bytes_transferred: u64,
    pub total_bytes: u64,
    pub chunks_completed: u32,
    pub throughput: f64,
}

impl TransferProgress {
    /// Returns completion ratio in `[0.0, 1.0]`.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn completion_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            1.0
        } else {
            self.bytes_transferred as f64 / self.total_bytes as f64
        }
    }
}

#[derive(Debug)]
struct ProgressState {
    progress: TransferProgress,
    started_at: Instant,
}

impl ProgressState {
    fn new(total_bytes: u64) -> Self {
        ProgressState {
            progress: TransferProgress {
                total_bytes,
                ..TransferProgress::default()
            },
            started_at: Instant::now(),
        }
    }

    fn update(&mut self, bytes_delta: u64, chunks_delta: u32) {
        self.progress.bytes_transferred =
            self.progress.bytes_transferred.saturating_add(bytes_delta);
        self.progress.chunks_completed =
            self.progress.chunks_completed.saturating_add(chunks_delta);
        self.update_throughput();
    }

    fn set_total_bytes(&mut self, total_bytes: u64) {
        self.progress.total_bytes = total_bytes;
        self.update_throughput();
    }

    fn snapshot(&self) -> TransferProgress {
        self.progress
    }

    fn update_throughput(&mut self) {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        self.progress.throughput = if elapsed > 0.0 {
            self.progress.bytes_transferred as f64 / elapsed
        } else {
            0.0
        };
    }
}

/// Handle for tracking and controlling a single transfer.
#[derive(Debug, Clone)]
pub struct TransferHandle {
    state: Arc<Mutex<ProgressState>>,
    cancelled: Arc<AtomicBool>,
}

impl TransferHandle {
    /// Creates a new handle with the provided total byte count.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn new(total_bytes: u64) -> Self {
        TransferHandle {
            state: Arc::new(Mutex::new(ProgressState::new(total_bytes))),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the latest progress snapshot.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn progress(&self) -> TransferProgress {
        let state = self.state.lock().await;
        state.snapshot()
    }

    /// Applies a progress delta for bytes and chunks.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn update_progress(&self, bytes_delta: u64, chunks_delta: u32) {
        let mut state = self.state.lock().await;
        state.update(bytes_delta, chunks_delta);
    }

    /// Updates the total bytes for the transfer.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn set_total_bytes(&self, total_bytes: u64) {
        let mut state = self.state.lock().await;
        state.set_total_bytes(total_bytes);
    }

    /// Requests cancellation of the transfer.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Returns true when cancellation has been requested.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

/// Per-node status snapshot for broadcast transfers.
#[derive(Debug, Clone)]
pub struct NodeTransferStatus {
    pub progress: TransferProgress,
    pub state: TransferState,
    pub error: Option<String>,
}

#[derive(Debug)]
struct NodeProgressState {
    progress: ProgressState,
    state: TransferState,
    error: Option<String>,
}

impl NodeProgressState {
    fn new(total_bytes: u64) -> Self {
        NodeProgressState {
            progress: ProgressState::new(total_bytes),
            state: TransferState::Initializing,
            error: None,
        }
    }
}

/// Handle for tracking broadcast progress across multiple nodes.
#[derive(Debug, Clone)]
pub struct BroadcastHandle {
    state: Arc<Mutex<HashMap<NodeId, NodeProgressState>>>,
    cancelled: Arc<AtomicBool>,
}

impl BroadcastHandle {
    /// Creates a broadcast handle for the target list.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn new(targets: Vec<NodeId>, total_bytes: u64) -> Self {
        let mut map = HashMap::new();
        for node_id in targets {
            map.insert(node_id, NodeProgressState::new(total_bytes));
        }
        BroadcastHandle {
            state: Arc::new(Mutex::new(map)),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns a snapshot of progress for all targets.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn progress(&self) -> HashMap<NodeId, NodeTransferStatus> {
        let state = self.state.lock().await;
        state
            .iter()
            .map(|(node_id, node_state)| {
                (
                    *node_id,
                    NodeTransferStatus {
                        progress: node_state.progress.snapshot(),
                        state: node_state.state,
                        error: node_state.error.clone(),
                    },
                )
            })
            .collect()
    }

    /// Updates progress for a specific target.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn update_node_progress(&self, node_id: NodeId, bytes_delta: u64, chunks_delta: u32) {
        let mut state = self.state.lock().await;
        if let Some(node_state) = state.get_mut(&node_id) {
            node_state.progress.update(bytes_delta, chunks_delta);
        }
    }

    /// Updates the transfer state for a specific target.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn set_node_state(
        &self,
        node_id: NodeId,
        transfer_state: TransferState,
        error: Option<String>,
    ) {
        let mut state = self.state.lock().await;
        if let Some(node_state) = state.get_mut(&node_id) {
            node_state.state = transfer_state;
            node_state.error = error;
        }
    }

    /// Requests cancellation of the broadcast operation.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Returns true when cancellation has been requested.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

/// Handle for tracking sync progress.
#[derive(Debug, Clone)]
pub struct SyncHandle {
    state: Arc<Mutex<ProgressState>>,
    cancelled: Arc<AtomicBool>,
}

impl SyncHandle {
    /// Creates a new sync handle with the provided total byte count.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn new(total_bytes: u64) -> Self {
        SyncHandle {
            state: Arc::new(Mutex::new(ProgressState::new(total_bytes))),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the latest progress snapshot.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn progress(&self) -> TransferProgress {
        let state = self.state.lock().await;
        state.snapshot()
    }

    /// Applies a progress delta for bytes and chunks.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn update_progress(&self, bytes_delta: u64, chunks_delta: u32) {
        let mut state = self.state.lock().await;
        state.update(bytes_delta, chunks_delta);
    }

    /// Updates the total bytes for the sync.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn set_total_bytes(&self, total_bytes: u64) {
        let mut state = self.state.lock().await;
        state.set_total_bytes(total_bytes);
    }

    /// Requests cancellation of the sync.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Returns true when cancellation has been requested.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}
