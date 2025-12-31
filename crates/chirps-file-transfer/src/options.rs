use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;
pub const MIN_CHUNK_SIZE: usize = 64 * 1024;
pub const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_CONCURRENCY: usize = 4;
const DEFAULT_CLOCK_SKEW_TOLERANCE_SECS: u64 = 2;

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
pub enum SyncDirection {
    Push,
    Pull,
    Bidirectional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ConflictResolution {
    #[default]
    NewerWins,
    SourceWins,
    TargetWins,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SortBy {
    #[default]
    Name,
    Size,
    ModifiedTime,
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
}

impl Default for TransferOptions {
    fn default() -> Self {
        TransferOptions {
            chunk_size: DEFAULT_CHUNK_SIZE,
            concurrency: DEFAULT_CONCURRENCY,
            compression: CompressionAlgorithm::None,
            bandwidth_limit: None,
            retry_policy: RetryPolicy::default(),
            verify_on_complete: true,
            hash_algorithm: HashAlgorithm::Sha256,
            resumable: true,
            overwrite: false,
            mode: TransferMode::Copy,
            preserve_metadata: true,
        }
    }
}

impl TransferOptions {
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_bandwidth_limit(mut self, bandwidth_limit: Option<u64>) -> Self {
        self.bandwidth_limit = bandwidth_limit;
        self
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub fn with_verify_on_complete(mut self, verify_on_complete: bool) -> Self {
        self.verify_on_complete = verify_on_complete;
        self
    }

    pub fn with_hash_algorithm(mut self, hash_algorithm: HashAlgorithm) -> Self {
        self.hash_algorithm = hash_algorithm;
        self
    }

    pub fn with_resumable(mut self, resumable: bool) -> Self {
        self.resumable = resumable;
        self
    }

    pub fn with_overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    pub fn with_mode(mut self, mode: TransferMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_preserve_metadata(mut self, preserve_metadata: bool) -> Self {
        self.preserve_metadata = preserve_metadata;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncOptions {
    pub transfer: TransferOptions,
    pub direction: SyncDirection,
    pub conflict_resolution: ConflictResolution,
    pub follow_symlinks: bool,
    pub clock_skew_tolerance: Duration,
}

impl Default for SyncOptions {
    fn default() -> Self {
        SyncOptions {
            transfer: TransferOptions::default(),
            direction: SyncDirection::Push,
            conflict_resolution: ConflictResolution::NewerWins,
            follow_symlinks: false,
            clock_skew_tolerance: Duration::from_secs(DEFAULT_CLOCK_SKEW_TOLERANCE_SECS),
        }
    }
}

impl SyncOptions {
    pub fn with_transfer(mut self, transfer: TransferOptions) -> Self {
        self.transfer = transfer;
        self
    }

    pub fn with_direction(mut self, direction: SyncDirection) -> Self {
        self.direction = direction;
        self
    }

    pub fn with_conflict_resolution(mut self, conflict_resolution: ConflictResolution) -> Self {
        self.conflict_resolution = conflict_resolution;
        self
    }

    pub fn with_follow_symlinks(mut self, follow_symlinks: bool) -> Self {
        self.follow_symlinks = follow_symlinks;
        self
    }

    pub fn with_clock_skew_tolerance(mut self, clock_skew_tolerance: Duration) -> Self {
        self.clock_skew_tolerance = clock_skew_tolerance;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RemoveOptions {
    pub recursive: bool,
    pub ignore_not_found: bool,
}

impl RemoveOptions {
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    pub fn with_ignore_not_found(mut self, ignore_not_found: bool) -> Self {
        self.ignore_not_found = ignore_not_found;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListOptions {
    pub recursive: bool,
    pub include_hidden: bool,
    pub files_only: bool,
    pub directories_only: bool,
    pub pattern: Option<String>,
    pub limit: usize,
    pub sort_by: SortBy,
}

impl Default for ListOptions {
    fn default() -> Self {
        ListOptions {
            recursive: false,
            include_hidden: false,
            files_only: false,
            directories_only: false,
            pattern: None,
            limit: 0,
            sort_by: SortBy::Name,
        }
    }
}

impl ListOptions {
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    pub fn with_include_hidden(mut self, include_hidden: bool) -> Self {
        self.include_hidden = include_hidden;
        self
    }

    pub fn with_files_only(mut self, files_only: bool) -> Self {
        self.files_only = files_only;
        self
    }

    pub fn with_directories_only(mut self, directories_only: bool) -> Self {
        self.directories_only = directories_only;
        self
    }

    pub fn with_pattern(mut self, pattern: Option<String>) -> Self {
        self.pattern = pattern;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_sort_by(mut self, sort_by: SortBy) -> Self {
        self.sort_by = sort_by;
        self
    }
}
