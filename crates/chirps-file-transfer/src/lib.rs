//! File transfer APIs for Chirps.

pub use alopex_chirps_wire::file_transfer::TransferSessionId;
pub type ChunkIndex = u32;
pub type ChunkChecksum = u64;

pub mod bandwidth;
pub mod chunk;
pub mod compression;
pub mod config;
pub mod error;
pub mod integrity;
pub mod manifest;
pub mod ops;
pub mod options;
pub mod path;
pub mod persistence;
pub mod progress;
pub mod session;
pub mod stream;
pub mod wire;

pub use bandwidth::BandwidthThrottle;
pub use chunk::{Chunk, ChunkManager, ChunkMeta, ChunkTracker};
pub use compression::{compress_bytes, compress_reader, decompress_bytes, decompress_reader};
pub use config::FileTransferConfig;
pub use error::FileTransferError;
pub use integrity::IntegrityVerifier;
pub use manifest::{FileMetadata, FileType, ManifestError, TransferManifest};
pub use ops::{ChunkStreamOpener, ControlDispatcher, broadcast_file, send_file, sync_file};
pub use options::{
    CompressionAlgorithm, ConflictResolution, HashAlgorithm, ListOptions, RemoveOptions,
    RetryPolicy, SortBy, SyncDirection, SyncOptions, TransferMode, TransferOptions,
};
pub use path::PathValidator;
pub use persistence::SessionPersistence;
pub use progress::{
    BroadcastHandle, NodeTransferStatus, SyncHandle, TransferHandle, TransferProgress,
};
pub use session::{TransferKind, TransferSession, TransferState};
pub use stream::{CHUNK_STREAM_MAGIC, ChunkStreamCodec};
