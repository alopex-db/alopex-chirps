use crate::ChunkChecksum;
use crate::options::HashAlgorithm;
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use xxhash_rust::xxh64::Xxh64;

pub struct IntegrityVerifier;

impl IntegrityVerifier {
    pub fn compute_chunk_checksum(data: &[u8]) -> ChunkChecksum {
        xxhash_rust::xxh64::xxh64(data, 0)
    }

    pub fn verify_chunk_checksum(data: &[u8], expected: ChunkChecksum) -> bool {
        Self::compute_chunk_checksum(data) == expected
    }

    pub async fn compute_file_hash(
        path: &std::path::Path,
        algorithm: HashAlgorithm,
    ) -> std::io::Result<Vec<u8>> {
        let mut file = File::open(path).await?;
        let mut buffer = vec![0u8; 64 * 1024];

        match algorithm {
            HashAlgorithm::Sha256 => {
                let mut hasher = Sha256::new();
                loop {
                    let bytes_read = file.read(&mut buffer).await?;
                    if bytes_read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..bytes_read]);
                }
                Ok(hasher.finalize().to_vec())
            }
            HashAlgorithm::Blake3 => {
                let mut hasher = blake3::Hasher::new();
                loop {
                    let bytes_read = file.read(&mut buffer).await?;
                    if bytes_read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..bytes_read]);
                }
                Ok(hasher.finalize().as_bytes().to_vec())
            }
            HashAlgorithm::XxHash64 => {
                let mut hasher = Xxh64::new(0);
                loop {
                    let bytes_read = file.read(&mut buffer).await?;
                    if bytes_read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..bytes_read]);
                }
                Ok(hasher.digest().to_be_bytes().to_vec())
            }
        }
    }

    pub async fn verify_file_hash(
        path: &std::path::Path,
        expected: &[u8],
        algorithm: HashAlgorithm,
    ) -> std::io::Result<bool> {
        let actual = Self::compute_file_hash(path, algorithm).await?;
        Ok(actual == expected)
    }
}
