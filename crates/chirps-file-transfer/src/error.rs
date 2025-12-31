use crate::{ChunkIndex, TransferSessionId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FileTransferError {
    // Session errors (1xxx)
    #[error("session not found: {0}")]
    SessionNotFound(TransferSessionId),
    #[error("session already exists: {0}")]
    SessionAlreadyExists(TransferSessionId),
    #[error("invalid session state")]
    InvalidState { expected: String, actual: String },

    // File errors (2xxx)
    #[error("file not found: {0}")]
    FileNotFound(String),
    #[error("file already exists: {0}")]
    FileAlreadyExists(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("disk full")]
    DiskFull,
    #[error("path traversal attack detected: {0}")]
    PathTraversal(String),

    // Transfer errors (3xxx)
    #[error("chunk checksum mismatch at index {index}")]
    ChunkChecksumMismatch { index: ChunkIndex },
    #[error("file hash mismatch")]
    FileHashMismatch,
    #[error("transfer timeout")]
    Timeout,
    #[error("transfer cancelled")]
    Cancelled,
    #[error("max retries exceeded for chunk {index}")]
    MaxRetriesExceeded { index: ChunkIndex },

    // Sync errors (4xxx)
    #[error("sync conflict: {path}")]
    SyncConflict { path: String },

    // Internal errors (9xxx)
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("peer rejected: {0}")]
    Rejected(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("compression error: {0}")]
    Compression(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl FileTransferError {
    pub fn code(&self) -> u32 {
        match self {
            FileTransferError::SessionNotFound(_) => 1001,
            FileTransferError::SessionAlreadyExists(_) => 1002,
            FileTransferError::InvalidState { .. } => 1003,
            FileTransferError::FileNotFound(_) => 2001,
            FileTransferError::FileAlreadyExists(_) => 2002,
            FileTransferError::PermissionDenied(_) => 2003,
            FileTransferError::DiskFull => 2004,
            FileTransferError::PathTraversal(_) => 2005,
            FileTransferError::ChunkChecksumMismatch { .. } => 3001,
            FileTransferError::FileHashMismatch => 3002,
            FileTransferError::Timeout => 3003,
            FileTransferError::Cancelled => 3004,
            FileTransferError::MaxRetriesExceeded { .. } => 3005,
            FileTransferError::SyncConflict { .. } => 4001,
            FileTransferError::Io(_)
            | FileTransferError::Transport(_)
            | FileTransferError::Rejected(_)
            | FileTransferError::Serialization(_)
            | FileTransferError::Compression(_)
            | FileTransferError::Internal(_) => 9001,
        }
    }

    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            FileTransferError::ChunkChecksumMismatch { .. }
                | FileTransferError::Timeout
                | FileTransferError::Transport(_)
        )
    }
}
