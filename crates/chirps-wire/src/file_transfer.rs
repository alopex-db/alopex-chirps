use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

pub type ChunkIndex = u32;
pub type ChunkChecksum = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TransferSessionId([u8; 16]);

impl TransferSessionId {
    pub fn new() -> Self {
        TransferSessionId(*uuid::Uuid::new_v4().as_bytes())
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != 16 {
            return Err("TransferSessionId must be 16 bytes long");
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(bytes);
        Ok(TransferSessionId(arr))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Default for TransferSessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<[u8; 16]> for TransferSessionId {
    fn from(bytes: [u8; 16]) -> Self {
        TransferSessionId(bytes)
    }
}

impl std::fmt::Display for TransferSessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let uuid = uuid::Uuid::from_bytes(self.0);
        write!(f, "{uuid}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompressionAlgorithm {
    #[default]
    None,
    Zstd,
    ZstdLevel(i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum HashAlgorithm {
    #[default]
    Sha256,
    Blake3,
    XxHash64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TransferMode {
    #[default]
    Copy,
    Move,
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
pub struct RetryPolicy {
    pub max_retries: u8,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferOptions {
    pub chunk_size: usize,
    pub concurrency: usize,
    pub compression: CompressionAlgorithm,
    pub bandwidth_limit: Option<u64>,
    pub retry_policy: RetryPolicy,
    pub verify_on_complete: bool,
    pub hash_algorithm: HashAlgorithm,
    pub resumable: bool,
    pub overwrite: bool,
    pub mode: TransferMode,
    pub preserve_metadata: bool,
    pub follow_symlinks: bool,
}

impl Default for TransferOptions {
    fn default() -> Self {
        TransferOptions {
            chunk_size: DEFAULT_CHUNK_SIZE,
            concurrency: 4,
            compression: CompressionAlgorithm::None,
            bandwidth_limit: None,
            retry_policy: RetryPolicy::default(),
            verify_on_complete: true,
            hash_algorithm: HashAlgorithm::Sha256,
            resumable: true,
            overwrite: false,
            mode: TransferMode::Copy,
            preserve_metadata: true,
            follow_symlinks: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub index: ChunkIndex,
    pub offset: u64,
    pub size: u32,
    pub checksum: ChunkChecksum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferManifest {
    pub version: u16,
    pub session_id: TransferSessionId,
    pub source_path: String,
    pub dest_path: String,
    pub file_size: u64,
    pub file_hash: Vec<u8>,
    pub hash_algorithm: HashAlgorithm,
    pub chunk_size: u32,
    pub chunk_count: u32,
    pub chunks: Vec<ChunkMeta>,
    pub metadata: Option<FileMetadata>,
    pub options: TransferOptions,
    pub created_at: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileTransferFrame {
    pub session_id: TransferSessionId,
    pub message: FileTransferMessage,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FileTransferMessage {
    TransferRequest(TransferRequest),
    TransferResponse(TransferResponse),
    Manifest(TransferManifest),
    ManifestAck(ManifestAck),
    ChunkAck(ChunkAck),
    ChunkRequest(ChunkRequest),
    Progress(ProgressUpdate),
    Cancel(CancelRequest),
    Complete(TransferComplete),
    Error(TransferErrorMessage),
    ExistsRequest(ExistsRequest),
    ExistsResponse(ExistsResponse),
    RemoveRequest(RemoveRequest),
    RemoveResponse(RemoveResponse),
    MetadataRequest(MetadataRequest),
    MetadataResponse(MetadataResponse),
    ListRequest(ListRequest),
    ListResponse(ListResponse),
    /// Requests that the peer initiate the reverse direction of a sync using
    /// this frame's session id.
    SyncRequest(SyncRequest),
    /// Supplies the final whole-file hash after all chunk acknowledgements.
    /// Appended for manifest v2 so existing bincode variant indices stay stable.
    FinalizeRequest(TransferComplete),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransferRequest {
    pub source_path: String,
    pub dest_path: String,
    pub file_size: u64,
    pub chunk_count: u32,
    pub chunk_size: u32,
    pub mode: TransferMode,
    pub options: TransferOptions,
    pub metadata: Option<FileMetadata>,
}

/// Requests a peer to send a file back to the caller for Pull/Bidirectional
/// synchronization. The enclosing frame session id is reused for the reverse
/// transfer so the requester can await one receiving session.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SyncRequest {
    pub source_path: String,
    pub dest_path: String,
    pub options: TransferOptions,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransferResponse {
    pub accepted: bool,
    pub rejection_reason: Option<String>,
    pub existing_chunks: Vec<ChunkIndex>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManifestAck {
    pub accepted: bool,
    pub skip_chunks: Vec<ChunkIndex>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkAck {
    pub index: ChunkIndex,
    pub verified: bool,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkRequest {
    pub indices: Vec<ChunkIndex>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProgressUpdate {
    pub chunks_completed: u32,
    pub bytes_transferred: u64,
    pub state: TransferState,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CancelRequest {
    pub reason: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransferComplete {
    pub bytes_transferred: u64,
    pub duration_ms: u64,
    pub file_hash: Vec<u8>,
    pub hash_algorithm: HashAlgorithm,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransferErrorMessage {
    pub code: u32,
    pub message: String,
    pub recoverable: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExistsRequest {
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExistsResponse {
    pub exists: bool,
    pub is_file: bool,
    pub is_directory: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RemoveRequest {
    pub path: String,
    pub recursive: bool,
    pub ignore_not_found: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RemoveResponse {
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetadataRequest {
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetadataResponse {
    pub found: bool,
    pub metadata: Option<FileMetadata>,
    pub size: Option<u64>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListRequest {
    pub path: String,
    pub recursive: bool,
    pub include_hidden: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListResponse {
    pub files: Vec<FileInfo>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub created_at: Option<u64>,
    pub modified_at: Option<u64>,
    pub permissions: Option<u32>,
    pub file_type: FileType,
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
    pub modified_at: u64,
    pub file_type: FileType,
}
