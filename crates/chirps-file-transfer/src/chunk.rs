use crate::options::{MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use crate::{ChunkChecksum, ChunkIndex};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub index: ChunkIndex,
    pub offset: u64,
    pub size: u32,
    pub checksum: ChunkChecksum,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub index: ChunkIndex,
    pub offset: u64,
    pub size: u32,
    pub checksum: ChunkChecksum,
}

pub struct ChunkManager {
    chunk_size: usize,
}

impl ChunkManager {
    pub fn new(chunk_size: usize) -> Self {
        let size = chunk_size.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
        ChunkManager { chunk_size: size }
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn calculate_chunk_count(&self, file_size: u64) -> u32 {
        file_size.div_ceil(self.chunk_size as u64) as u32
    }

    pub async fn read_chunk(&self, file: &mut File, index: ChunkIndex) -> std::io::Result<Chunk> {
        let offset = index as u64 * self.chunk_size as u64;
        file.seek(SeekFrom::Start(offset)).await?;

        let mut data = vec![0u8; self.chunk_size];
        let bytes_read = file.read(&mut data).await?;
        data.truncate(bytes_read);

        let checksum = xxhash_rust::xxh64::xxh64(&data, 0);

        Ok(Chunk {
            index,
            offset,
            size: bytes_read as u32,
            checksum,
            data,
        })
    }

    pub async fn write_chunk(&self, file: &mut File, chunk: &Chunk) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(chunk.offset)).await?;
        file.write_all(&chunk.data).await?;
        Ok(())
    }

    pub fn verify_chunk(&self, chunk: &Chunk) -> bool {
        let computed = xxhash_rust::xxh64::xxh64(&chunk.data, 0);
        computed == chunk.checksum
    }

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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkTracker {
    pub total_chunks: u32,
    pub completed: BTreeSet<ChunkIndex>,
    pub in_flight: BTreeSet<ChunkIndex>,
    pub failed: BTreeMap<ChunkIndex, u8>,
    pub max_retries: u8,
}

impl ChunkTracker {
    pub fn new(total_chunks: u32, max_retries: u8) -> Self {
        ChunkTracker {
            total_chunks,
            completed: BTreeSet::new(),
            in_flight: BTreeSet::new(),
            failed: BTreeMap::new(),
            max_retries,
        }
    }

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

    pub fn mark_in_flight(&mut self, index: ChunkIndex) {
        self.in_flight.insert(index);
    }

    pub fn mark_completed(&mut self, index: ChunkIndex) {
        self.in_flight.remove(&index);
        self.failed.remove(&index);
        self.completed.insert(index);
    }

    pub fn mark_failed(&mut self, index: ChunkIndex) {
        self.in_flight.remove(&index);
        let retries = self.failed.entry(index).or_insert(0);
        *retries = retries.saturating_add(1);
    }

    pub fn is_complete(&self) -> bool {
        self.completed.len() == self.total_chunks as usize
    }

    pub fn completion_ratio(&self) -> f64 {
        if self.total_chunks == 0 {
            return 1.0;
        }
        self.completed.len() as f64 / self.total_chunks as f64
    }

    pub fn permanently_failed(&self) -> Vec<ChunkIndex> {
        self.failed
            .iter()
            .filter(|&(_, &retries)| retries >= self.max_retries)
            .map(|(&idx, _)| idx)
            .collect()
    }
}
