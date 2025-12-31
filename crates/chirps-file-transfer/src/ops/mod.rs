use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_wire::file_transfer::{FileTransferFrame, FileTransferMessage};
use alopex_chirps_wire::frame::Frame;
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::{Duration, Instant};

use crate::TransferSessionId;
use crate::error::FileTransferError;

pub mod broadcast;
mod conversions;
pub mod file_ops;
pub mod receive;
pub mod send;
pub mod sync;

pub use broadcast::{BroadcastResult, broadcast_file};
pub use file_ops::{
    exists, handle_exists_request, handle_list_request, handle_metadata_request,
    handle_remove_request, list_files, metadata, remove,
};
pub use receive::{ReceiveHandler, ReceiveOutcome};
pub use send::{SendFileResult, send_file};
pub use sync::sync_file;

#[async_trait]
pub trait ChunkStreamOpener: Send + Sync {
    async fn open_chunk_stream(
        &self,
        target: NodeId,
    ) -> Result<quinn::SendStream, FileTransferError>;
}

pub struct ControlDispatcher {
    backend: Arc<dyn MessageBackend>,
    inbox: Mutex<HashMap<TransferSessionId, VecDeque<(NodeId, FileTransferMessage)>>>,
    notify: Notify,
    closed: AtomicBool,
}

impl ControlDispatcher {
    pub fn new(
        backend: Arc<dyn MessageBackend>,
        mut receiver: mpsc::Receiver<(NodeId, Frame)>,
    ) -> Arc<Self> {
        let dispatcher = Arc::new(ControlDispatcher {
            backend,
            inbox: Mutex::new(HashMap::new()),
            notify: Notify::new(),
            closed: AtomicBool::new(false),
        });

        let task_dispatcher = Arc::clone(&dispatcher);
        tokio::spawn(async move {
            while let Some((sender, frame)) = receiver.recv().await {
                let Frame::FileTransfer(frame) = frame else {
                    continue;
                };
                let mut inbox = task_dispatcher.inbox.lock().await;
                inbox
                    .entry(frame.session_id)
                    .or_default()
                    .push_back((sender, frame.message));
                task_dispatcher.notify.notify_waiters();
            }
            task_dispatcher.closed.store(true, Ordering::Relaxed);
            task_dispatcher.notify.notify_waiters();
        });

        dispatcher
    }

    pub async fn send_message(
        &self,
        target: NodeId,
        session_id: TransferSessionId,
        message: FileTransferMessage,
    ) -> Result<(), FileTransferError> {
        self.backend
            .send(
                target,
                Frame::FileTransfer(FileTransferFrame {
                    session_id,
                    message,
                }),
            )
            .await
            .map_err(|err| FileTransferError::Transport(err.to_string()))
    }

    pub async fn broadcast_message(
        &self,
        session_id: TransferSessionId,
        message: FileTransferMessage,
    ) -> Result<usize, FileTransferError> {
        self.backend
            .broadcast(Frame::FileTransfer(FileTransferFrame {
                session_id,
                message,
            }))
            .await
            .map_err(|err| FileTransferError::Transport(err.to_string()))
    }

    pub async fn recv_any(
        &self,
        session_id: TransferSessionId,
        wait: Duration,
    ) -> Result<(NodeId, FileTransferMessage), FileTransferError> {
        self.recv_filtered(session_id, wait, |_| true).await
    }

    pub async fn recv_any_filtered<F>(
        &self,
        wait: Duration,
        matcher: F,
    ) -> Result<(TransferSessionId, NodeId, FileTransferMessage), FileTransferError>
    where
        F: Fn(NodeId, &FileTransferMessage) -> bool + Send + Sync,
    {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(message) = self.pop_matching_any(&matcher).await {
                return Ok(message);
            }

            if self.closed.load(Ordering::Relaxed) {
                return Err(FileTransferError::Transport(
                    "control channel closed".into(),
                ));
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(FileTransferError::Timeout);
            }
            let remaining = deadline - now;
            if tokio::time::timeout(remaining, self.notify.notified())
                .await
                .is_err()
            {
                return Err(FileTransferError::Timeout);
            }
        }
    }

    pub async fn recv_filtered<F>(
        &self,
        session_id: TransferSessionId,
        wait: Duration,
        matcher: F,
    ) -> Result<(NodeId, FileTransferMessage), FileTransferError>
    where
        F: Fn(&FileTransferMessage) -> bool + Send + Sync,
    {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(message) = self.pop_matching(session_id, &matcher).await {
                return Ok(message);
            }

            if self.closed.load(Ordering::Relaxed) {
                return Err(FileTransferError::Transport(
                    "control channel closed".into(),
                ));
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(FileTransferError::Timeout);
            }
            let remaining = deadline - now;
            if tokio::time::timeout(remaining, self.notify.notified())
                .await
                .is_err()
            {
                return Err(FileTransferError::Timeout);
            }
        }
    }

    async fn pop_matching<F>(
        &self,
        session_id: TransferSessionId,
        matcher: &F,
    ) -> Option<(NodeId, FileTransferMessage)>
    where
        F: Fn(&FileTransferMessage) -> bool,
    {
        let mut inbox = self.inbox.lock().await;
        let queue = inbox.get_mut(&session_id)?;
        let index = queue.iter().position(|(_, msg)| matcher(msg))?;
        queue.remove(index)
    }

    async fn pop_matching_any<F>(
        &self,
        matcher: &F,
    ) -> Option<(TransferSessionId, NodeId, FileTransferMessage)>
    where
        F: Fn(NodeId, &FileTransferMessage) -> bool,
    {
        let mut inbox = self.inbox.lock().await;
        for (session_id, queue) in inbox.iter_mut() {
            if let Some(index) = queue.iter().position(|(sender, msg)| matcher(*sender, msg))
                && let Some((sender, message)) = queue.remove(index)
            {
                return Some((*session_id, sender, message));
            }
        }
        None
    }
}
