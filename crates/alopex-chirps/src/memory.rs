//! Node-wide logical memory accounting and runtime budget controls.
//!
//! The manager accounts for Chirps-owned allocations. It deliberately does not
//! claim to be an RSS/cgroup sampler; those measurements belong to the v0.6.3
//! stability evidence tooling.

use std::sync::Mutex;
use thiserror::Error;

const DEFAULT_TOTAL_BUDGET: usize = 256 * 1024 * 1024;
const DEFAULT_MESSAGE_BUFFER_LIMIT: usize = 64 * 1024 * 1024;
const DEFAULT_RAFT_LOG_CACHE_LIMIT: usize = 32 * 1024 * 1024;
const DEFAULT_CONNECTION_POOL_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryComponent {
    MessageBuffer,
    RaftLogCache,
    ConnectionPool,
    BlockCache,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MemoryConfig {
    pub total_budget: usize,
    pub message_buffer_limit: usize,
    pub raft_log_cache_limit: usize,
    pub connection_pool_limit: usize,
    pub backpressure_threshold: f32,
    pub emergency_threshold: f32,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            total_budget: DEFAULT_TOTAL_BUDGET,
            message_buffer_limit: DEFAULT_MESSAGE_BUFFER_LIMIT,
            raft_log_cache_limit: DEFAULT_RAFT_LOG_CACHE_LIMIT,
            connection_pool_limit: DEFAULT_CONNECTION_POOL_LIMIT,
            backpressure_threshold: 0.80,
            emergency_threshold: 0.95,
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum MemoryError {
    #[error("memory total_budget must be greater than zero")]
    ZeroBudget,
    #[error("memory threshold must be finite and between zero and one")]
    InvalidThreshold,
    #[error("memory component limit exceeds total budget")]
    ComponentLimitExceedsBudget,
    #[error("at least one memory sample is required")]
    EmptySamples,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryStats {
    pub total_budget: usize,
    pub current_usage: usize,
    pub message_buffer_bytes: usize,
    pub raft_log_cache_bytes: usize,
    pub connection_pool_bytes: usize,
    pub block_cache_bytes: usize,
    pub budget_exceeded: bool,
}

/// One best-effort process/cgroup memory observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryMeasurement {
    pub rss_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    pub cgroup_limit_bytes: Option<u64>,
}

impl MemoryMeasurement {
    pub const fn from_values(
        rss_bytes: Option<u64>,
        cgroup_current_bytes: Option<u64>,
        cgroup_limit_bytes: Option<u64>,
    ) -> Self {
        Self {
            rss_bytes,
            cgroup_current_bytes,
            cgroup_limit_bytes,
        }
    }

    /// Selects RSS when available and otherwise falls back to cgroup usage.
    pub fn observed_bytes(self) -> Option<u64> {
        self.rss_bytes.or(self.cgroup_current_bytes)
    }

    /// Captures the process RSS and common cgroup v2/v1 files when present.
    pub fn capture() -> Self {
        let rss_bytes = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|line| {
                    let value = line.strip_prefix("VmRSS:")?.split_whitespace().next()?;
                    value
                        .parse::<u64>()
                        .ok()
                        .map(|kib| kib.saturating_mul(1024))
                })
            });
        let cgroup_current_bytes = read_first_number(&[
            "/sys/fs/cgroup/memory.current",
            "/sys/fs/cgroup/memory/memory.usage_in_bytes",
        ]);
        let cgroup_limit_bytes = read_first_number(&[
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ]);
        Self::from_values(rss_bytes, cgroup_current_bytes, cgroup_limit_bytes)
    }
}

/// Summary used by the high-load memory stability measurement contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryStabilityReport {
    pub peak_bytes: u64,
    pub final_bytes: u64,
    pub growth_bytes: u64,
    pub within_budget: bool,
    pub stable: bool,
}

impl MemoryStabilityReport {
    pub fn from_samples(
        samples: &[MemoryMeasurement],
        budget_bytes: u64,
        allowed_growth_bytes: u64,
    ) -> Result<Self, MemoryError> {
        let values: Vec<u64> = samples
            .iter()
            .copied()
            .filter_map(MemoryMeasurement::observed_bytes)
            .collect();
        let Some((&first, rest)) = values.split_first() else {
            return Err(MemoryError::EmptySamples);
        };
        let peak_bytes = values.iter().copied().max().unwrap_or(first);
        let final_bytes = values.last().copied().unwrap_or(first);
        let growth_bytes = final_bytes.saturating_sub(first);
        Ok(Self {
            peak_bytes,
            final_bytes,
            growth_bytes,
            within_budget: peak_bytes <= budget_bytes,
            stable: growth_bytes <= allowed_growth_bytes
                && rest
                    .iter()
                    .all(|value| value.saturating_sub(first) <= allowed_growth_bytes),
        })
    }
}

fn read_first_number(paths: &[&str]) -> Option<u64> {
    paths.iter().find_map(|path| {
        let value = std::fs::read_to_string(path).ok()?.trim().to_owned();
        if value == "max" {
            None
        } else {
            value.parse().ok()
        }
    })
}

#[derive(Debug)]
pub struct MemoryManager {
    state: Mutex<MemoryState>,
}

#[derive(Debug)]
struct MemoryState {
    config: MemoryConfig,
    stats: MemoryStats,
}

impl MemoryManager {
    pub fn new(config: MemoryConfig) -> Result<Self, MemoryError> {
        validate_config(&config)?;
        Ok(Self {
            state: Mutex::new(MemoryState {
                stats: MemoryStats {
                    total_budget: config.total_budget,
                    ..MemoryStats::default()
                },
                config,
            }),
        })
    }

    pub fn resize_memory_budget(&self, new_budget: usize) -> Result<(), MemoryError> {
        if new_budget == 0 {
            return Err(MemoryError::ZeroBudget);
        }
        let mut state = self.state.lock().expect("memory manager lock poisoned");
        state.config.total_budget = new_budget;
        state.stats.total_budget = new_budget;
        refresh_budget_flag(&mut state.stats);
        Ok(())
    }

    pub fn set_component_usage(&self, component: MemoryComponent, bytes: usize) {
        let mut state = self.state.lock().expect("memory manager lock poisoned");
        let slot = match component {
            MemoryComponent::MessageBuffer => &mut state.stats.message_buffer_bytes,
            MemoryComponent::RaftLogCache => &mut state.stats.raft_log_cache_bytes,
            MemoryComponent::ConnectionPool => &mut state.stats.connection_pool_bytes,
            MemoryComponent::BlockCache => &mut state.stats.block_cache_bytes,
        };
        *slot = bytes;
        refresh_budget_flag(&mut state.stats);
    }

    /// Applies configured component caps and gives any remaining budget to the
    /// block cache. This is the explicit, synchronous eviction boundary exposed
    /// to operators; transport/WAL owners can call it before releasing storage.
    pub fn trigger_gc(&self) -> Result<(), MemoryError> {
        let mut state = self.state.lock().expect("memory manager lock poisoned");
        state.stats.message_buffer_bytes = state
            .stats
            .message_buffer_bytes
            .min(state.config.message_buffer_limit);
        state.stats.raft_log_cache_bytes = state
            .stats
            .raft_log_cache_bytes
            .min(state.config.raft_log_cache_limit);
        state.stats.connection_pool_bytes = state
            .stats
            .connection_pool_bytes
            .min(state.config.connection_pool_limit);
        let fixed = state
            .stats
            .message_buffer_bytes
            .saturating_add(state.stats.raft_log_cache_bytes)
            .saturating_add(state.stats.connection_pool_bytes);
        let remaining = state.config.total_budget.saturating_sub(fixed);
        state.stats.block_cache_bytes = state.stats.block_cache_bytes.min(remaining);
        refresh_budget_flag(&mut state.stats);
        Ok(())
    }

    pub fn get_memory_stats(&self) -> MemoryStats {
        self.state
            .lock()
            .expect("memory manager lock poisoned")
            .stats
    }
}

fn validate_config(config: &MemoryConfig) -> Result<(), MemoryError> {
    if config.total_budget == 0 {
        return Err(MemoryError::ZeroBudget);
    }
    if config.message_buffer_limit > config.total_budget
        || config.raft_log_cache_limit > config.total_budget
        || config.connection_pool_limit > config.total_budget
    {
        return Err(MemoryError::ComponentLimitExceedsBudget);
    }
    if !config.backpressure_threshold.is_finite()
        || !config.emergency_threshold.is_finite()
        || !(0.0..=1.0).contains(&config.backpressure_threshold)
        || !(0.0..=1.0).contains(&config.emergency_threshold)
        || config.backpressure_threshold > config.emergency_threshold
    {
        return Err(MemoryError::InvalidThreshold);
    }
    Ok(())
}

fn refresh_budget_flag(stats: &mut MemoryStats) {
    stats.current_usage = stats
        .message_buffer_bytes
        .saturating_add(stats.raft_log_cache_bytes)
        .saturating_add(stats.connection_pool_bytes)
        .saturating_add(stats.block_cache_bytes);
    stats.budget_exceeded = stats.current_usage > stats.total_budget;
}
