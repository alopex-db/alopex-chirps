//! Verified, bounded-concurrency transport primitives for Raft snapshots.
//!
//! The transport owns retry and integrity policy. Installation remains a Raft
//! responsibility: a receiver may hand bytes to OpenRaft only after every
//! chunk and the whole-snapshot digest have been verified.

use alopex_chirps_raft_storage::types::{BasicNode, ChirpsNodeId, SnapshotMeta, Vote};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;

pub const DEFAULT_CHUNK_THRESHOLD: usize = 10 * 1024 * 1024;
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;
pub const DEFAULT_MAX_CONCURRENT_CHUNKS: usize = 4;
pub const DEFAULT_MAX_RETRIES: usize = 3;
pub const DEFAULT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTransferConfig {
    pub chunk_threshold: usize,
    pub chunk_size: usize,
    pub max_concurrent_chunks: usize,
    /// Number of retries after the first attempt, per chunk.
    pub max_retries: usize,
    /// Maximum wall-clock time for the complete transfer, including retries.
    pub transfer_timeout: Duration,
}

impl Default for SnapshotTransferConfig {
    fn default() -> Self {
        Self {
            chunk_threshold: DEFAULT_CHUNK_THRESHOLD,
            chunk_size: DEFAULT_CHUNK_SIZE,
            max_concurrent_chunks: DEFAULT_MAX_CONCURRENT_CHUNKS,
            max_retries: DEFAULT_MAX_RETRIES,
            transfer_timeout: DEFAULT_TRANSFER_TIMEOUT,
        }
    }
}

impl SnapshotTransferConfig {
    pub fn validate(self) -> Result<Self, SnapshotTransferError> {
        if self.chunk_threshold == 0 {
            return Err(SnapshotTransferError::InvalidConfig(
                "chunk_threshold must be greater than zero".into(),
            ));
        }
        if self.chunk_size == 0 {
            return Err(SnapshotTransferError::InvalidConfig(
                "chunk_size must be greater than zero".into(),
            ));
        }
        if self.max_concurrent_chunks == 0 {
            return Err(SnapshotTransferError::InvalidConfig(
                "max_concurrent_chunks must be greater than zero".into(),
            ));
        }
        if self.transfer_timeout.is_zero() {
            return Err(SnapshotTransferError::InvalidConfig(
                "transfer_timeout must be greater than zero".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub snapshot_id: String,
    pub total_len: u64,
    pub chunk_size: u32,
    pub chunk_count: u32,
    pub digest: [u8; 32],
}

impl SnapshotManifest {
    pub fn from_bytes(
        snapshot_id: impl Into<String>,
        bytes: &[u8],
        chunk_size: usize,
    ) -> Result<Self, SnapshotTransferError> {
        if chunk_size == 0 || chunk_size > u32::MAX as usize {
            return Err(SnapshotTransferError::InvalidConfig(
                "chunk_size must fit in a non-zero u32".into(),
            ));
        }
        let count = bytes.len().max(1).div_ceil(chunk_size);
        let chunk_count = u32::try_from(count).map_err(|_| {
            SnapshotTransferError::InvalidManifest("too many snapshot chunks".into())
        })?;
        Ok(Self {
            snapshot_id: snapshot_id.into(),
            total_len: bytes.len() as u64,
            chunk_size: chunk_size as u32,
            chunk_count,
            digest: sha256(bytes),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotChunk {
    pub snapshot_id: String,
    pub index: u32,
    pub offset: u64,
    pub data: Vec<u8>,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedSnapshot {
    pub snapshot_id: String,
    pub bytes: Vec<u8>,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTransferReceipt {
    pub installed: bool,
    pub size_bytes: u64,
}

impl SnapshotTransferReceipt {
    pub fn installed(size_bytes: u64) -> Self {
        Self {
            installed: true,
            size_bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotProgress {
    pub snapshot_id: String,
    pub verified_chunks: u32,
    pub total_chunks: u32,
    pub transferred_bytes: u64,
    pub retries: u64,
    pub in_flight: usize,
    pub max_observed_concurrency: usize,
    pub installed: bool,
}

impl SnapshotProgress {
    pub fn percent(&self) -> u8 {
        if self.total_chunks == 0 {
            return 0;
        }
        ((u64::from(self.verified_chunks) * 100) / u64::from(self.total_chunks)) as u8
    }
}

pub trait SnapshotProgressObserver: Send + Sync + 'static {
    fn observe(&self, progress: SnapshotProgress);
}

#[derive(Debug, Default)]
pub struct NoopSnapshotProgressObserver;

impl SnapshotProgressObserver for NoopSnapshotProgressObserver {
    fn observe(&self, _progress: SnapshotProgress) {}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftSnapshotBegin {
    pub vote: Vote<ChirpsNodeId>,
    pub meta: SnapshotMeta<ChirpsNodeId, BasicNode>,
    pub manifest: SnapshotManifest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftSnapshotRequest {
    Begin(RaftSnapshotBegin),
    Chunk(SnapshotChunk),
    Finish { snapshot_id: String },
    Abort { snapshot_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftSnapshotStatus {
    Accepted,
    ChunkVerified { index: u32 },
    Installed { size_bytes: u64 },
    RetryChunk { index: u32, reason: String },
    Rejected { reason: String },
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftSnapshotResponse {
    pub vote: Vote<ChirpsNodeId>,
    pub snapshot_id: String,
    pub status: RaftSnapshotStatus,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotTransferError {
    #[error("invalid snapshot transfer configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid snapshot manifest: {0}")]
    InvalidManifest(String),
    #[error("snapshot integrity check failed: {0}")]
    Integrity(String),
    #[error("retryable snapshot transfer failure: {0}")]
    Retryable(String),
    #[error("terminal snapshot transfer failure: {0}")]
    Terminal(String),
    #[error("snapshot transfer timed out")]
    Timeout,
}

impl SnapshotTransferError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable(message.into())
    }

    pub fn terminal(message: impl Into<String>) -> Self {
        Self::Terminal(message.into())
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

#[async_trait]
pub trait SnapshotChunkSink: Send + Sync + 'static {
    async fn begin(&self, manifest: SnapshotManifest) -> Result<(), SnapshotTransferError>;
    async fn send_chunk(&self, chunk: SnapshotChunk) -> Result<(), SnapshotTransferError>;
    async fn finish(
        &self,
        snapshot_id: &str,
    ) -> Result<SnapshotTransferReceipt, SnapshotTransferError>;
    async fn abort(&self, snapshot_id: &str);
}

pub struct SnapshotSender {
    config: SnapshotTransferConfig,
    observer: Arc<dyn SnapshotProgressObserver>,
}

#[derive(Default)]
struct SenderProgressState {
    verified: u64,
    transferred: u64,
    retries: u64,
    active: usize,
    max_active: usize,
}

impl SnapshotSender {
    pub fn new(
        config: SnapshotTransferConfig,
        observer: Arc<dyn SnapshotProgressObserver>,
    ) -> Result<Self, SnapshotTransferError> {
        Ok(Self {
            config: config.validate()?,
            observer,
        })
    }

    pub async fn transfer<S: SnapshotChunkSink>(
        &self,
        snapshot_id: impl Into<String>,
        bytes: Vec<u8>,
        sink: Arc<S>,
    ) -> Result<SnapshotTransferReceipt, SnapshotTransferError> {
        let snapshot_id = snapshot_id.into();
        match timeout(
            self.config.transfer_timeout,
            self.transfer_inner(snapshot_id.clone(), bytes, Arc::clone(&sink)),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                sink.abort(&snapshot_id).await;
                Err(SnapshotTransferError::Timeout)
            }
        }
    }

    async fn transfer_inner<S: SnapshotChunkSink>(
        &self,
        snapshot_id: String,
        bytes: Vec<u8>,
        sink: Arc<S>,
    ) -> Result<SnapshotTransferReceipt, SnapshotTransferError> {
        let chunk_size = if bytes.len() > self.config.chunk_threshold {
            self.config.chunk_size
        } else {
            bytes.len().max(1)
        };
        let manifest = SnapshotManifest::from_bytes(snapshot_id.clone(), &bytes, chunk_size)?;
        sink.begin(manifest.clone()).await?;

        let bytes = Arc::new(bytes);
        let state = Arc::new(Mutex::new(SenderProgressState::default()));
        self.observer
            .observe(progress(&manifest, 0, 0, 0, 0, 0, false));

        let result: Result<Vec<()>, SnapshotTransferError> = stream::iter(0..manifest.chunk_count)
            .map(|index| {
                let sink = Arc::clone(&sink);
                let observer = Arc::clone(&self.observer);
                let manifest = manifest.clone();
                let state = Arc::clone(&state);
                let bytes = Arc::clone(&bytes);
                async move {
                    let chunk = build_chunk(&manifest, &bytes, index)?;
                    for attempt in 0..=self.config.max_retries {
                        {
                            let mut state = state
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            state.active += 1;
                            state.max_active = state.max_active.max(state.active);
                            state.transferred += chunk.data.len() as u64;
                        }
                        let result = sink.send_chunk(chunk.clone()).await;
                        match result {
                            Ok(()) => {
                                let mut state = state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                state.active -= 1;
                                state.verified += 1;
                                observer.observe(progress(
                                    &manifest,
                                    state.verified,
                                    state.transferred,
                                    state.retries,
                                    state.active,
                                    state.max_active,
                                    false,
                                ));
                                return Ok(());
                            }
                            Err(error)
                                if error.is_retryable() && attempt < self.config.max_retries =>
                            {
                                let mut state = state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                state.active -= 1;
                                state.retries += 1;
                                observer.observe(progress(
                                    &manifest,
                                    state.verified,
                                    state.transferred,
                                    state.retries,
                                    state.active,
                                    state.max_active,
                                    false,
                                ));
                            }
                            Err(error) => {
                                state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .active -= 1;
                                return Err(error);
                            }
                        }
                    }
                    unreachable!("bounded retry loop always returns")
                }
            })
            .buffer_unordered(self.config.max_concurrent_chunks)
            .try_collect()
            .await;

        if let Err(error) = result {
            sink.abort(&snapshot_id).await;
            return Err(error);
        }

        let receipt = match sink.finish(&snapshot_id).await {
            Ok(receipt) if receipt.installed => receipt,
            Ok(_) => {
                sink.abort(&snapshot_id).await;
                return Err(SnapshotTransferError::Terminal(
                    "receiver completed without installing the snapshot".into(),
                ));
            }
            Err(error) => {
                sink.abort(&snapshot_id).await;
                return Err(error);
            }
        };
        let state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.observer.observe(progress(
            &manifest,
            state.verified,
            state.transferred,
            state.retries,
            state.active,
            state.max_active,
            true,
        ));
        Ok(receipt)
    }
}

pub struct SnapshotReceiver {
    manifest: SnapshotManifest,
    bytes: Vec<u8>,
    received: Vec<bool>,
    verified_chunks: u32,
    verified_bytes: u64,
    observer: Arc<dyn SnapshotProgressObserver>,
}

impl SnapshotReceiver {
    pub fn new(
        manifest: SnapshotManifest,
        observer: Arc<dyn SnapshotProgressObserver>,
    ) -> Result<Self, SnapshotTransferError> {
        validate_manifest(&manifest)?;
        let total_len = usize::try_from(manifest.total_len).map_err(|_| {
            SnapshotTransferError::InvalidManifest(
                "snapshot length does not fit this platform".into(),
            )
        })?;
        let bytes = vec![0; total_len];
        let received = vec![false; manifest.chunk_count as usize];
        observer.observe(progress(&manifest, 0, 0, 0, 0, 0, false));
        Ok(Self {
            manifest,
            bytes,
            received,
            verified_chunks: 0,
            verified_bytes: 0,
            observer,
        })
    }

    pub fn accept(&mut self, chunk: SnapshotChunk) -> Result<(), SnapshotTransferError> {
        validate_chunk(&self.manifest, &chunk)?;
        let index = chunk.index as usize;
        let start = chunk.offset as usize;
        let end = start + chunk.data.len();
        if self.received[index] {
            if self.bytes[start..end] == chunk.data {
                return Ok(());
            }
            return Err(SnapshotTransferError::Integrity(format!(
                "conflicting duplicate chunk {}",
                chunk.index
            )));
        }
        self.verified_chunks += 1;
        self.verified_bytes += chunk.data.len() as u64;
        self.bytes[start..end].copy_from_slice(&chunk.data);
        self.received[index] = true;
        self.observer.observe(progress(
            &self.manifest,
            u64::from(self.verified_chunks),
            self.verified_bytes,
            0,
            0,
            0,
            false,
        ));
        Ok(())
    }

    pub fn verify(&self, snapshot_id: &str) -> Result<VerifiedSnapshot, SnapshotTransferError> {
        self.verify_complete(snapshot_id)?;
        Ok(VerifiedSnapshot {
            snapshot_id: self.manifest.snapshot_id.clone(),
            bytes: self.bytes.clone(),
            digest: self.manifest.digest,
        })
    }

    pub fn into_verified(
        self,
        snapshot_id: &str,
    ) -> Result<VerifiedSnapshot, SnapshotTransferError> {
        self.verify_complete(snapshot_id)?;
        Ok(VerifiedSnapshot {
            snapshot_id: self.manifest.snapshot_id.clone(),
            bytes: self.bytes,
            digest: self.manifest.digest,
        })
    }

    fn verify_complete(&self, snapshot_id: &str) -> Result<(), SnapshotTransferError> {
        if snapshot_id != self.manifest.snapshot_id {
            return Err(SnapshotTransferError::InvalidManifest(
                "snapshot id changed before completion".into(),
            ));
        }
        if self.verified_chunks != self.manifest.chunk_count {
            return Err(SnapshotTransferError::InvalidManifest(format!(
                "snapshot is incomplete: verified {} of {} chunks",
                self.verified_chunks, self.manifest.chunk_count
            )));
        }
        if self.bytes.len() as u64 != self.manifest.total_len {
            return Err(SnapshotTransferError::Integrity(
                "reassembled length does not match manifest".into(),
            ));
        }
        if sha256(&self.bytes) != self.manifest.digest {
            return Err(SnapshotTransferError::Integrity(
                "whole snapshot digest mismatch".into(),
            ));
        }
        Ok(())
    }
}

fn build_chunk(
    manifest: &SnapshotManifest,
    bytes: &[u8],
    index: u32,
) -> Result<SnapshotChunk, SnapshotTransferError> {
    let chunk_size = manifest.chunk_size as usize;
    if bytes.is_empty() {
        return Ok(SnapshotChunk {
            snapshot_id: manifest.snapshot_id.clone(),
            index: 0,
            offset: 0,
            data: Vec::new(),
            digest: sha256(&[]),
        });
    }
    let start = index as usize * chunk_size;
    let end = (start + chunk_size).min(bytes.len());
    let data = bytes.get(start..end).ok_or_else(|| {
        SnapshotTransferError::InvalidManifest("chunk index is out of bounds".into())
    })?;
    Ok(SnapshotChunk {
        snapshot_id: manifest.snapshot_id.clone(),
        index,
        offset: u64::from(index) * manifest.chunk_size as u64,
        data: data.to_vec(),
        digest: sha256(data),
    })
}

fn validate_manifest(manifest: &SnapshotManifest) -> Result<(), SnapshotTransferError> {
    if manifest.snapshot_id.is_empty() || manifest.chunk_size == 0 || manifest.chunk_count == 0 {
        return Err(SnapshotTransferError::InvalidManifest(
            "snapshot id, chunk size, and chunk count must be non-zero".into(),
        ));
    }
    let expected = (manifest.total_len.max(1)).div_ceil(u64::from(manifest.chunk_size));
    if expected != u64::from(manifest.chunk_count) {
        return Err(SnapshotTransferError::InvalidManifest(
            "chunk count does not cover the declared snapshot length".into(),
        ));
    }
    Ok(())
}

fn validate_chunk(
    manifest: &SnapshotManifest,
    chunk: &SnapshotChunk,
) -> Result<(), SnapshotTransferError> {
    if chunk.snapshot_id != manifest.snapshot_id || chunk.index >= manifest.chunk_count {
        return Err(SnapshotTransferError::InvalidManifest(
            "chunk does not belong to this snapshot".into(),
        ));
    }
    let expected_offset = u64::from(chunk.index) * u64::from(manifest.chunk_size);
    if chunk.offset != expected_offset {
        return Err(SnapshotTransferError::InvalidManifest(format!(
            "chunk {} has offset {}, expected {}",
            chunk.index, chunk.offset, expected_offset
        )));
    }
    let remaining = manifest.total_len.saturating_sub(expected_offset);
    let expected_len = remaining.min(u64::from(manifest.chunk_size)) as usize;
    if chunk.data.len() != expected_len {
        return Err(SnapshotTransferError::InvalidManifest(format!(
            "chunk {} has length {}, expected {}",
            chunk.index,
            chunk.data.len(),
            expected_len
        )));
    }
    if sha256(&chunk.data) != chunk.digest {
        return Err(SnapshotTransferError::Integrity(format!(
            "chunk {} digest mismatch",
            chunk.index
        )));
    }
    Ok(())
}

fn progress(
    manifest: &SnapshotManifest,
    verified_chunks: u64,
    transferred_bytes: u64,
    retries: u64,
    in_flight: usize,
    max_observed_concurrency: usize,
    installed: bool,
) -> SnapshotProgress {
    SnapshotProgress {
        snapshot_id: manifest.snapshot_id.clone(),
        verified_chunks: verified_chunks as u32,
        total_chunks: manifest.chunk_count,
        transferred_bytes,
        retries,
        in_flight,
        max_observed_concurrency,
        installed,
    }
}

pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
