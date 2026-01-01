use crate::options::{CompressionAlgorithm, DEFAULT_CHUNK_SIZE};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_CONCURRENCY: usize = 4;
const MAX_CONCURRENCY: usize = 16;
const DEFAULT_MAX_TRANSFERS: usize = 32;
const DEFAULT_SESSION_RETENTION_HOURS: u64 = 24;
const DEFAULT_MAX_SESSIONS: usize = 100;

/// Global configuration for file transfer operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileTransferConfig {
    pub default_chunk_size: usize,
    pub default_concurrency: usize,
    pub max_concurrency: usize,
    pub default_compression: CompressionAlgorithm,
    pub global_bandwidth_limit: Option<u64>,
    pub max_concurrent_transfers: usize,
    pub chunk_timeout: Duration,
    pub manifest_timeout: Duration,
    pub idle_timeout: Duration,
    pub retry: RetryConfig,
    pub base_path: PathBuf,
    pub temp_dir: Option<PathBuf>,
    pub session_dir: Option<PathBuf>,
    pub session_retention: Duration,
    pub max_sessions: usize,
}

impl Default for FileTransferConfig {
    fn default() -> Self {
        FileTransferConfig {
            default_chunk_size: DEFAULT_CHUNK_SIZE,
            default_concurrency: DEFAULT_CONCURRENCY,
            max_concurrency: MAX_CONCURRENCY,
            default_compression: CompressionAlgorithm::None,
            global_bandwidth_limit: None,
            max_concurrent_transfers: DEFAULT_MAX_TRANSFERS,
            chunk_timeout: Duration::from_secs(30),
            manifest_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            retry: RetryConfig::default(),
            base_path: PathBuf::from("."),
            temp_dir: None,
            session_dir: None,
            session_retention: Duration::from_secs(DEFAULT_SESSION_RETENTION_HOURS * 60 * 60),
            max_sessions: DEFAULT_MAX_SESSIONS,
        }
    }
}

impl FileTransferConfig {
    /// Sets the default chunk size for transfers.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_default_chunk_size(mut self, default_chunk_size: usize) -> Self {
        self.default_chunk_size = default_chunk_size;
        self
    }

    /// Sets the default concurrency used for chunk uploads.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_default_concurrency(mut self, default_concurrency: usize) -> Self {
        self.default_concurrency = default_concurrency;
        self
    }

    /// Sets the maximum allowed concurrency for transfers.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency;
        self
    }

    /// Sets the default compression algorithm.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_default_compression(mut self, default_compression: CompressionAlgorithm) -> Self {
        self.default_compression = default_compression;
        self
    }

    /// Sets a global bandwidth limit (bytes/sec) across transfers.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_global_bandwidth_limit(mut self, global_bandwidth_limit: Option<u64>) -> Self {
        self.global_bandwidth_limit = global_bandwidth_limit;
        self
    }

    /// Sets the maximum concurrent transfers.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_max_concurrent_transfers(mut self, max_concurrent_transfers: usize) -> Self {
        self.max_concurrent_transfers = max_concurrent_transfers;
        self
    }

    /// Sets the per-chunk acknowledgement timeout.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_chunk_timeout(mut self, chunk_timeout: Duration) -> Self {
        self.chunk_timeout = chunk_timeout;
        self
    }

    /// Sets the timeout for manifest exchange.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_manifest_timeout(mut self, manifest_timeout: Duration) -> Self {
        self.manifest_timeout = manifest_timeout;
        self
    }

    /// Sets the idle timeout for transfer progress.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets retry configuration used for transfers.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Sets the base path for file operations.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_base_path(mut self, base_path: PathBuf) -> Self {
        self.base_path = base_path;
        self
    }

    /// Sets the directory for temporary files.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_temp_dir(mut self, temp_dir: Option<PathBuf>) -> Self {
        self.temp_dir = temp_dir;
        self
    }

    /// Sets the directory for persisted sessions.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_session_dir(mut self, session_dir: Option<PathBuf>) -> Self {
        self.session_dir = session_dir;
        self
    }

    /// Sets how long to retain persisted sessions.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_session_retention(mut self, session_retention: Duration) -> Self {
        self.session_retention = session_retention;
        self
    }

    /// Sets the maximum number of persisted sessions to keep.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = max_sessions;
        self
    }
}

/// Retry configuration for chunk transmissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u8,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

impl RetryConfig {
    /// Sets the maximum retry count for a chunk.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_max_retries(mut self, max_retries: u8) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the initial retry backoff delay.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_initial_delay(mut self, initial_delay: Duration) -> Self {
        self.initial_delay = initial_delay;
        self
    }

    /// Sets the maximum retry backoff delay.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Sets the retry backoff multiplier.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn with_backoff_multiplier(mut self, backoff_multiplier: f64) -> Self {
        self.backoff_multiplier = backoff_multiplier;
        self
    }
}
