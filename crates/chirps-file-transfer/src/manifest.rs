use crate::TransferSessionId;
use crate::chunk::ChunkMeta;
use crate::options::{HashAlgorithm, TransferOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Transfer manifest describing a file and its chunks.
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

impl TransferManifest {
    pub const CURRENT_VERSION: u16 = 1;

    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.version > Self::CURRENT_VERSION {
            return Err(ManifestError::UnsupportedVersion(self.version));
        }

        if self.chunks.len() != self.chunk_count as usize {
            return Err(ManifestError::ChunkCountMismatch {
                expected: self.chunk_count,
                actual: self.chunks.len() as u32,
            });
        }

        let mut total_size = 0u64;
        for (i, chunk) in self.chunks.iter().enumerate() {
            if chunk.index != i as u32 {
                return Err(ManifestError::ChunkIndexMismatch {
                    expected: i as u32,
                    actual: chunk.index,
                });
            }
            total_size += chunk.size as u64;
        }

        if total_size != self.file_size {
            return Err(ManifestError::SizeMismatch {
                expected: self.file_size,
                actual: total_size,
            });
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("unsupported manifest version {0}")]
    UnsupportedVersion(u16),
    #[error("chunk count mismatch: expected {expected}, got {actual}")]
    ChunkCountMismatch { expected: u32, actual: u32 },
    #[error("chunk index mismatch: expected {expected}, got {actual}")]
    ChunkIndexMismatch { expected: u32, actual: u32 },
    #[error("size mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },
}

/// File metadata for transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub created_at: Option<u64>,
    pub modified_at: Option<u64>,
    pub permissions: Option<u32>,
    pub file_type: FileType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}
