//! File transfer APIs for Chirps.

pub use alopex_chirps_wire::file_transfer::TransferSessionId;
pub type ChunkIndex = u32;

pub mod config;
pub mod error;
pub mod options;

pub use config::FileTransferConfig;
pub use error::FileTransferError;
pub use options::{
    CompressionAlgorithm, ConflictResolution, HashAlgorithm, ListOptions, RemoveOptions,
    RetryPolicy, SortBy, SyncDirection, SyncOptions, TransferMode, TransferOptions,
};
