use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default chunk size in bytes (1 MiB).
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;
/// Minimum supported chunk size in bytes.
pub const MIN_CHUNK_SIZE: usize = 64 * 1024;
/// Maximum supported chunk size in bytes.
pub const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_CONCURRENCY: usize = 4;
const DEFAULT_CLOCK_SKEW_TOLERANCE_SECS: u64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
/// Compression options for transfer payloads.
pub enum CompressionAlgorithm {
    /// Do not compress payloads.
    #[default]
    None,
    /// Compress payloads with Zstd at the default level.
    Zstd,
    /// Compress payloads with Zstd at the provided level.
    ZstdLevel(i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
/// Hash algorithms supported for integrity verification.
pub enum HashAlgorithm {
    /// SHA-256 hash.
    #[default]
    Sha256,
    /// Blake3 hash.
    Blake3,
    /// XXHash64 hash.
    XxHash64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
/// Behavior for what happens to the source after a successful transfer.
pub enum TransferMode {
    /// Keep the source and copy the data.
    #[default]
    Copy,
    /// Remove the source after a successful transfer.
    Move,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Direction to synchronize files between local and remote.
pub enum SyncDirection {
    /// Local to remote.
    Push,
    /// Remote to local.
    Pull,
    /// Synchronize in both directions.
    Bidirectional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
/// Strategy for resolving conflicts when both sides differ.
pub enum ConflictResolution {
    /// Prefer the newer file based on modification time.
    #[default]
    NewerWins,
    /// Prefer the local/source copy.
    SourceWins,
    /// Prefer the remote/target copy.
    TargetWins,
    /// Require manual resolution.
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
/// Sort key for file listings.
pub enum SortBy {
    /// Sort by file name.
    #[default]
    Name,
    /// Sort by file size.
    Size,
    /// Sort by last modification timestamp.
    ModifiedTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Retry policy for transient transfer failures.
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
/// Options that control how a transfer is performed.
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
    /// Returns options with the chunk size updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Returns options with the concurrency updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Returns options with the compression algorithm updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.compression = compression;
        self
    }

    /// Returns options with the bandwidth limit updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_bandwidth_limit(mut self, bandwidth_limit: Option<u64>) -> Self {
        self.bandwidth_limit = bandwidth_limit;
        self
    }

    /// Returns options with the retry policy updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Returns options with the verify-on-complete flag updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_verify_on_complete(mut self, verify_on_complete: bool) -> Self {
        self.verify_on_complete = verify_on_complete;
        self
    }

    /// Returns options with the hash algorithm updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_hash_algorithm(mut self, hash_algorithm: HashAlgorithm) -> Self {
        self.hash_algorithm = hash_algorithm;
        self
    }

    /// Returns options with the resumable flag updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_resumable(mut self, resumable: bool) -> Self {
        self.resumable = resumable;
        self
    }

    /// Returns options with the overwrite flag updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Returns options with the transfer mode updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_mode(mut self, mode: TransferMode) -> Self {
        self.mode = mode;
        self
    }

    /// Returns options with metadata preservation updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_preserve_metadata(mut self, preserve_metadata: bool) -> Self {
        self.preserve_metadata = preserve_metadata;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Options that control a sync operation.
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
    /// Returns sync options with transfer options updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_transfer(mut self, transfer: TransferOptions) -> Self {
        self.transfer = transfer;
        self
    }

    /// Returns sync options with the sync direction updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_direction(mut self, direction: SyncDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Returns sync options with the conflict resolution updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_conflict_resolution(mut self, conflict_resolution: ConflictResolution) -> Self {
        self.conflict_resolution = conflict_resolution;
        self
    }

    /// Returns sync options with follow-symlinks updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_follow_symlinks(mut self, follow_symlinks: bool) -> Self {
        self.follow_symlinks = follow_symlinks;
        self
    }

    /// Returns sync options with the clock skew tolerance updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_clock_skew_tolerance(mut self, clock_skew_tolerance: Duration) -> Self {
        self.clock_skew_tolerance = clock_skew_tolerance;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Options for file and directory removal.
pub struct RemoveOptions {
    pub recursive: bool,
    pub ignore_not_found: bool,
}

impl RemoveOptions {
    /// Returns options with recursive removal enabled or disabled.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Returns options with ignore-not-found enabled or disabled.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_ignore_not_found(mut self, ignore_not_found: bool) -> Self {
        self.ignore_not_found = ignore_not_found;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Options for listing files and directories.
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
    /// Returns options with recursive traversal enabled or disabled.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Returns options with hidden files included or excluded.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_include_hidden(mut self, include_hidden: bool) -> Self {
        self.include_hidden = include_hidden;
        self
    }

    /// Returns options with files-only filtering enabled or disabled.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_files_only(mut self, files_only: bool) -> Self {
        self.files_only = files_only;
        self
    }

    /// Returns options with directories-only filtering enabled or disabled.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_directories_only(mut self, directories_only: bool) -> Self {
        self.directories_only = directories_only;
        self
    }

    /// Returns options with an optional name pattern filter.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_pattern(mut self, pattern: Option<String>) -> Self {
        self.pattern = pattern;
        self
    }

    /// Returns options with the listing limit updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Returns options with the sort key updated.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_sort_by(mut self, sort_by: SortBy) -> Self {
        self.sort_by = sort_by;
        self
    }
}
