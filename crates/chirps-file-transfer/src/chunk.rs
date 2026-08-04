use crate::options::{MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use crate::{ChunkChecksum, ChunkIndex};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// Writes an owned chunk at a fixed file offset and returns the same buffer.
///
/// Returning ownership lets the receiver feed verified bytes into its
/// incremental file hash without copying the payload around blocking I/O.
///
/// # Errors
/// Returns an I/O error when the positional write fails or makes no progress.
pub async fn write_owned_chunk_at(
    path: PathBuf,
    offset: u64,
    data: Vec<u8>,
) -> std::io::Result<Vec<u8>> {
    #[cfg(unix)]
    {
        tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;

            let file = std::fs::OpenOptions::new().write(true).open(path)?;
            let mut written = 0usize;
            while written < data.len() {
                let write_offset = offset.checked_add(written as u64).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "chunk offset overflow")
                })?;
                let bytes = file.write_at(&data[written..], write_offset)?;
                if bytes == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "chunk write made no progress",
                    ));
                }
                written += bytes;
            }
            Ok(data)
        })
        .await
        .map_err(|error| std::io::Error::other(format!("chunk write task failed: {error}")))?
    }

    #[cfg(not(unix))]
    {
        let mut file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(&data).await?;
        file.flush().await?;
        Ok(data)
    }
}

/// A chunk payload with checksum metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub index: ChunkIndex,
    pub offset: u64,
    pub size: u32,
    pub checksum: ChunkChecksum,
    pub data: Vec<u8>,
}

/// Metadata describing a chunk without its payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub index: ChunkIndex,
    pub offset: u64,
    pub size: u32,
    pub checksum: ChunkChecksum,
}

/// Helper for chunk sizing and chunk I/O.
pub struct ChunkManager {
    chunk_size: usize,
}

impl ChunkManager {
    /// Creates a chunk manager, clamping size to `[MIN_CHUNK_SIZE, MAX_CHUNK_SIZE]`.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn new(chunk_size: usize) -> Self {
        let size = chunk_size.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
        ChunkManager { chunk_size: size }
    }

    /// Returns the effective chunk size.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Calculates how many chunks are needed for a file size.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn calculate_chunk_count(&self, file_size: u64) -> u32 {
        file_size.div_ceil(self.chunk_size as u64) as u32
    }

    /// Reads a chunk at the given index from an open file handle.
    ///
    /// # Errors
    /// Returns an `io::Error` if seeking to the chunk offset or reading from the file fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn read_chunk(&self, file: &mut File, index: ChunkIndex) -> std::io::Result<Chunk> {
        let offset = index as u64 * self.chunk_size as u64;
        file.seek(SeekFrom::Start(offset)).await?;

        let file_size = file.metadata().await?.len();
        let bytes_to_read = file_size.saturating_sub(offset).min(self.chunk_size as u64) as usize;
        let mut data = vec![0u8; bytes_to_read];
        file.read_exact(&mut data).await?;

        let checksum = xxhash_rust::xxh64::xxh64(&data, 0);

        Ok(Chunk {
            index,
            offset,
            size: bytes_to_read as u32,
            checksum,
            data,
        })
    }

    /// Writes a chunk payload to the file at the chunk offset.
    ///
    /// # Errors
    /// Returns an `io::Error` if seeking to the chunk offset or writing to the file fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn write_chunk(&self, file: &mut File, chunk: &Chunk) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(chunk.offset)).await?;
        file.write_all(&chunk.data).await?;
        Ok(())
    }

    /// Verifies that a chunk payload matches its checksum.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn verify_chunk(&self, chunk: &Chunk) -> bool {
        let computed = xxhash_rust::xxh64::xxh64(&chunk.data, 0);
        computed == chunk.checksum
    }

    /// Generates chunk metadata for a file by reading each chunk.
    ///
    /// # Errors
    /// Returns an `io::Error` if seeking to a chunk offset or reading chunk data fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn generate_chunk_metas(
        &self,
        file: &mut File,
        file_size: u64,
    ) -> std::io::Result<Vec<ChunkMeta>> {
        let chunk_count = self.calculate_chunk_count(file_size);
        let mut metas = Vec::with_capacity(chunk_count as usize);

        for idx in 0..chunk_count {
            let offset = idx as u64 * self.chunk_size as u64;
            file.seek(SeekFrom::Start(offset)).await?;

            let remaining = file_size.saturating_sub(offset);
            let size = remaining.min(self.chunk_size as u64) as u32;

            let mut data = vec![0u8; size as usize];
            file.read_exact(&mut data).await?;

            let checksum = xxhash_rust::xxh64::xxh64(&data, 0);

            metas.push(ChunkMeta {
                index: idx,
                offset,
                size,
                checksum,
            });
        }

        Ok(metas)
    }
}

/// Tracks chunk completion, failures, and retry state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkTracker {
    pub total_chunks: u32,
    pub completed: BTreeSet<ChunkIndex>,
    pub in_flight: BTreeSet<ChunkIndex>,
    pub failed: BTreeMap<ChunkIndex, u8>,
    pub max_retries: u8,
}

impl ChunkTracker {
    /// Creates a tracker for a transfer with retry limits.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn new(total_chunks: u32, max_retries: u8) -> Self {
        ChunkTracker {
            total_chunks,
            completed: BTreeSet::new(),
            in_flight: BTreeSet::new(),
            failed: BTreeMap::new(),
            max_retries,
        }
    }

    /// Returns the next set of chunk indices to send.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn next_chunks(&self, count: usize) -> Vec<ChunkIndex> {
        let mut result = Vec::with_capacity(count);

        for (&idx, &retries) in &self.failed {
            if retries < self.max_retries && !self.in_flight.contains(&idx) {
                result.push(idx);
                if result.len() >= count {
                    return result;
                }
            }
        }

        for idx in 0..self.total_chunks {
            if !self.completed.contains(&idx)
                && !self.in_flight.contains(&idx)
                && !self.failed.contains_key(&idx)
            {
                result.push(idx);
                if result.len() >= count {
                    return result;
                }
            }
        }

        result
    }

    /// Marks a chunk as in-flight.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn mark_in_flight(&mut self, index: ChunkIndex) {
        self.in_flight.insert(index);
    }

    /// Marks a chunk as completed and clears any failure state.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn mark_completed(&mut self, index: ChunkIndex) {
        self.in_flight.remove(&index);
        self.failed.remove(&index);
        self.completed.insert(index);
    }

    /// Marks a chunk as failed and increments its retry count.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn mark_failed(&mut self, index: ChunkIndex) {
        self.in_flight.remove(&index);
        let retries = self.failed.entry(index).or_insert(0);
        *retries = retries.saturating_add(1);
    }

    /// Returns true when all chunks have completed.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn is_complete(&self) -> bool {
        self.completed.len() == self.total_chunks as usize
    }

    /// Returns completion ratio in `[0.0, 1.0]`.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn completion_ratio(&self) -> f64 {
        if self.total_chunks == 0 {
            return 1.0;
        }
        self.completed.len() as f64 / self.total_chunks as f64
    }

    /// Returns chunk indices that exceeded retry limits.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn permanently_failed(&self) -> Vec<ChunkIndex> {
        self.failed
            .iter()
            .filter(|&(_, &retries)| retries >= self.max_retries)
            .map(|(&idx, _)| idx)
            .collect()
    }
}
