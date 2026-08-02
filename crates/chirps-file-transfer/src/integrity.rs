use crate::options::HashAlgorithm;
use crate::{ChunkChecksum, ChunkMeta};
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use xxhash_rust::xxh64::Xxh64;

/// Utilities for computing and verifying checksums and hashes.
pub struct IntegrityVerifier;

impl IntegrityVerifier {
    /// Computes an XXHash64 checksum for a chunk payload.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn compute_chunk_checksum(data: &[u8]) -> ChunkChecksum {
        xxhash_rust::xxh64::xxh64(data, 0)
    }

    /// Verifies a chunk checksum against an expected value.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn verify_chunk_checksum(data: &[u8], expected: ChunkChecksum) -> bool {
        Self::compute_chunk_checksum(data) == expected
    }

    /// Computes a file hash using the requested algorithm.
    ///
    /// # Errors
    /// Returns an `io::Error` if opening or reading the file fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn compute_file_hash(
        path: &std::path::Path,
        algorithm: HashAlgorithm,
    ) -> std::io::Result<Vec<u8>> {
        let mut file = File::open(path).await?;
        let mut buffer = vec![0u8; 64 * 1024];

        let mut hasher = FileHasher::new(algorithm);
        loop {
            let bytes_read = file.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }
        Ok(hasher.finalize())
    }

    /// Computes the requested file hash and all fixed-size chunk metadata in
    /// one sequential scan.
    ///
    /// # Errors
    /// Returns an I/O error if opening, reading, or inspecting the file fails,
    /// or if `chunk_size` is zero.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn compute_file_hash_and_chunk_metas(
        path: &std::path::Path,
        algorithm: HashAlgorithm,
        chunk_size: usize,
    ) -> std::io::Result<(Vec<u8>, Vec<ChunkMeta>)> {
        if chunk_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "chunk_size must be greater than zero",
            ));
        }

        let mut file = File::open(path).await?;
        let file_size = file.metadata().await?.len();
        let mut hasher = FileHasher::new(algorithm);
        let mut chunks = Vec::with_capacity(file_size.div_ceil(chunk_size as u64) as usize);
        let mut offset = 0u64;
        let mut index = 0u32;

        while offset < file_size {
            let size = (file_size - offset).min(chunk_size as u64) as usize;
            let mut data = vec![0u8; size];
            file.read_exact(&mut data).await?;
            hasher.update(&data);
            chunks.push(ChunkMeta {
                index,
                offset,
                size: size as u32,
                checksum: Self::compute_chunk_checksum(&data),
            });
            offset = offset.saturating_add(size as u64);
            index = index.saturating_add(1);
        }

        Ok((hasher.finalize(), chunks))
    }

    /// Verifies a file hash against an expected value.
    ///
    /// # Errors
    /// Returns an `io::Error` if opening or reading the file fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn verify_file_hash(
        path: &std::path::Path,
        expected: &[u8],
        algorithm: HashAlgorithm,
    ) -> std::io::Result<bool> {
        let actual = Self::compute_file_hash(path, algorithm).await?;
        Ok(actual == expected)
    }
}

enum FileHasher {
    Sha256(Sha256),
    Blake3(Box<blake3::Hasher>),
    XxHash64(Xxh64),
}

impl FileHasher {
    fn new(algorithm: HashAlgorithm) -> Self {
        match algorithm {
            HashAlgorithm::Sha256 => Self::Sha256(Sha256::new()),
            HashAlgorithm::Blake3 => Self::Blake3(Box::new(blake3::Hasher::new())),
            HashAlgorithm::XxHash64 => Self::XxHash64(Xxh64::new(0)),
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Sha256(hasher) => {
                hasher.update(data);
            }
            Self::Blake3(hasher) => {
                hasher.update(data);
            }
            Self::XxHash64(hasher) => {
                hasher.update(data);
            }
        }
    }

    fn finalize(self) -> Vec<u8> {
        match self {
            Self::Sha256(hasher) => hasher.finalize().to_vec(),
            Self::Blake3(hasher) => (*hasher).finalize().as_bytes().to_vec(),
            Self::XxHash64(hasher) => hasher.digest().to_be_bytes().to_vec(),
        }
    }
}
