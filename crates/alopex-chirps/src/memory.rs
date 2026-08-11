//! Node-wide logical memory accounting and runtime budget controls.
//!
//! The manager accounts for Chirps-owned allocations. It deliberately does not
//! claim to be an RSS/cgroup sampler; those measurements belong to the v0.6.3
//! stability evidence tooling.

use crate::buffer::MessageBuffer;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
    #[error("allocation ratios must be finite, non-negative, and sum to one")]
    InvalidAllocationRatio,
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

/// Dynamic shares of the node-wide memory budget.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AllocationRatio {
    pub message_buffer: f32,
    pub raft_cache: f32,
    pub connection_pool: f32,
    pub block_cache: f32,
}

impl Default for AllocationRatio {
    fn default() -> Self {
        Self {
            message_buffer: 0.30,
            raft_cache: 0.20,
            connection_pool: 0.10,
            block_cache: 0.40,
        }
    }
}

impl AllocationRatio {
    fn validate(self) -> Result<(), MemoryError> {
        let values = [
            self.message_buffer,
            self.raft_cache,
            self.connection_pool,
            self.block_cache,
        ];
        if values
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
            || (values.iter().sum::<f32>() - 1.0).abs() > 0.0001
        {
            return Err(MemoryError::InvalidAllocationRatio);
        }
        Ok(())
    }
}

/// Workload weights used by [`IntegratedCacheManager::rebalance`].
pub type WorkloadProfile = AllocationRatio;

/// A bounded LRU cache for recently used Raft log payloads.
#[derive(Debug)]
pub struct RaftLogCache {
    max_bytes: usize,
    used_bytes: usize,
    entries: VecDeque<(u64, Vec<u8>)>,
}

impl RaftLogCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            used_bytes: 0,
            entries: VecDeque::new(),
        }
    }

    pub fn insert(&mut self, index: u64, payload: Vec<u8>) {
        if let Some(position) = self.entries.iter().position(|(key, _)| *key == index)
            && let Some((_, old)) = self.entries.remove(position)
        {
            self.used_bytes = self.used_bytes.saturating_sub(old.len());
        }
        if payload.len() > self.max_bytes {
            return;
        }
        while self.used_bytes.saturating_add(payload.len()) > self.max_bytes {
            let Some((_, evicted)) = self.entries.pop_front() else {
                break;
            };
            self.used_bytes = self.used_bytes.saturating_sub(evicted.len());
        }
        self.used_bytes = self.used_bytes.saturating_add(payload.len());
        self.entries.push_back((index, payload));
    }

    pub fn get(&mut self, index: u64) -> Option<Vec<u8>> {
        let position = self.entries.iter().position(|(key, _)| *key == index)?;
        let entry = self.entries.remove(position)?;
        let payload = entry.1.clone();
        self.entries.push_back(entry);
        Some(payload)
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    fn evict_bytes(&mut self, target_bytes: usize) -> usize {
        let mut evicted = 0;
        while evicted < target_bytes {
            let Some((_, payload)) = self.entries.pop_front() else {
                break;
            };
            evicted = evicted.saturating_add(payload.len());
            self.used_bytes = self.used_bytes.saturating_sub(payload.len());
        }
        evicted
    }
}

/// Usage adapter for alopex-core's block cache.
///
/// alopex-core 0.3 does not expose a public BlockCache type. This adapter is
/// intentionally limited to the accounting/eviction contract and can wrap a
/// concrete core cache when that API becomes available without changing the
/// Chirps wire or persistence formats.
#[derive(Debug)]
pub struct BlockCacheHandle {
    capacity: AtomicUsize,
    used_bytes: AtomicUsize,
}

impl BlockCacheHandle {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: AtomicUsize::new(capacity),
            used_bytes: AtomicUsize::new(0),
        }
    }

    pub fn set_capacity(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Relaxed);
    }

    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    pub fn set_used_bytes(&self, bytes: usize) {
        self.used_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed)
    }

    fn evict_bytes(&self, target_bytes: usize) -> usize {
        loop {
            let current = self.used_bytes();
            let evicted = current.min(target_bytes);
            if self
                .used_bytes
                .compare_exchange(
                    current,
                    current - evicted,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return evicted;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnifiedMemoryMetrics {
    pub total_budget: usize,
    pub current_usage: usize,
    pub message_buffer_bytes: usize,
    pub raft_cache_bytes: usize,
    pub connection_pool_bytes: usize,
    pub block_cache_bytes: usize,
    pub evicted_bytes: usize,
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

    fn set_component_limit(&self, component: MemoryComponent, bytes: usize) {
        let mut state = self.state.lock().expect("memory manager lock poisoned");
        let limit = bytes.min(state.config.total_budget);
        match component {
            MemoryComponent::MessageBuffer => state.config.message_buffer_limit = limit,
            MemoryComponent::RaftLogCache => state.config.raft_log_cache_limit = limit,
            MemoryComponent::ConnectionPool => state.config.connection_pool_limit = limit,
            MemoryComponent::BlockCache => {}
        }
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

/// Coordinates the memory owned by Chirps subsystems under the existing
/// [`MemoryManager`] budget.
pub struct IntegratedCacheManager {
    pub message_buffer: MessageBuffer,
    pub raft_cache: RaftLogCache,
    pub block_cache: Arc<BlockCacheHandle>,
    pub total_budget: usize,
    pub allocation_ratio: AllocationRatio,
    memory: Arc<MemoryManager>,
    connection_pool_bytes: usize,
    evicted_bytes: usize,
}

impl IntegratedCacheManager {
    pub fn new(config: MemoryConfig) -> Result<Self, MemoryError> {
        Self::with_block_cache(
            config,
            Arc::new(BlockCacheHandle::new(
                (config.total_budget as f32 * AllocationRatio::default().block_cache) as usize,
            )),
        )
    }

    pub fn with_block_cache(
        config: MemoryConfig,
        block_cache: Arc<BlockCacheHandle>,
    ) -> Result<Self, MemoryError> {
        let memory = Arc::new(MemoryManager::new(config)?);
        let allocation_ratio = AllocationRatio::default();
        allocation_ratio.validate()?;
        let manager = Self {
            message_buffer: MessageBuffer::new(
                config.message_buffer_limit,
                config.backpressure_threshold,
                config.emergency_threshold,
            ),
            raft_cache: RaftLogCache::new(config.raft_log_cache_limit),
            block_cache,
            total_budget: config.total_budget,
            allocation_ratio,
            memory,
            connection_pool_bytes: 0,
            evicted_bytes: 0,
        };
        manager.sync_usage();
        Ok(manager)
    }

    /// Reallocates component caps and evicts immediately when a new cap is
    /// below current usage. The shared MemoryManager remains the accounting
    /// source of truth used by MeshHandle.
    pub fn rebalance(&mut self, workload: WorkloadProfile) {
        if workload.validate().is_err() {
            return;
        }
        self.allocation_ratio = workload;
        let budget = self.total_budget;
        let message_limit = (budget as f32 * workload.message_buffer) as usize;
        let raft_limit = (budget as f32 * workload.raft_cache) as usize;
        let connection_limit = (budget as f32 * workload.connection_pool) as usize;
        let block_limit = (budget as f32 * workload.block_cache) as usize;
        self.memory
            .set_component_limit(MemoryComponent::MessageBuffer, message_limit);
        self.memory
            .set_component_limit(MemoryComponent::RaftLogCache, raft_limit);
        self.memory
            .set_component_limit(MemoryComponent::ConnectionPool, connection_limit);
        self.block_cache.set_capacity(block_limit);
        self.evicted_bytes = self.evicted_bytes.saturating_add(
            self.evict_message_bytes(
                self.message_buffer
                    .used_bytes()
                    .saturating_sub(message_limit),
            ),
        );
        self.evicted_bytes = self.evicted_bytes.saturating_add(
            self.raft_cache
                .evict_bytes(self.raft_cache.used_bytes().saturating_sub(raft_limit)),
        );
        self.evicted_bytes = self.evicted_bytes.saturating_add(
            self.block_cache
                .evict_bytes(self.block_cache.used_bytes().saturating_sub(block_limit)),
        );
        self.sync_usage();
    }

    /// Frees at least as much as possible from the least durable in-memory
    /// layers and returns the number of bytes actually released.
    pub fn emergency_evict(&mut self, target_bytes: usize) -> usize {
        let mut remaining = target_bytes;
        let mut evicted = 0;
        let released = self.block_cache.evict_bytes(remaining);
        remaining = remaining.saturating_sub(released);
        evicted += released;
        if remaining > 0 {
            let released = self.raft_cache.evict_bytes(remaining);
            remaining = remaining.saturating_sub(released);
            evicted += released;
        }
        if remaining > 0 {
            evicted += self.evict_message_bytes(remaining);
        }
        self.evicted_bytes = self.evicted_bytes.saturating_add(evicted);
        self.sync_usage();
        evicted
    }

    pub fn get_unified_metrics(&self) -> UnifiedMemoryMetrics {
        self.sync_usage();
        let stats = self.memory.get_memory_stats();
        UnifiedMemoryMetrics {
            total_budget: stats.total_budget,
            current_usage: stats.current_usage,
            message_buffer_bytes: stats.message_buffer_bytes,
            raft_cache_bytes: stats.raft_log_cache_bytes,
            connection_pool_bytes: stats.connection_pool_bytes,
            block_cache_bytes: stats.block_cache_bytes,
            evicted_bytes: self.evicted_bytes,
        }
    }

    /// Records bytes owned by the QUIC connection/session layer in the same
    /// budget. The transport can update this at its own sampling boundary.
    pub fn set_connection_pool_usage(&mut self, bytes: usize) {
        self.connection_pool_bytes = bytes;
        self.sync_usage();
    }

    pub fn memory_manager(&self) -> Arc<MemoryManager> {
        Arc::clone(&self.memory)
    }

    fn evict_message_bytes(&mut self, target_bytes: usize) -> usize {
        let mut evicted = 0;
        while evicted < target_bytes {
            let Some(message) = self.message_buffer.pop() else {
                break;
            };
            evicted = evicted.saturating_add(message.payload.len());
        }
        evicted
    }

    fn sync_usage(&self) {
        self.memory.set_component_usage(
            MemoryComponent::MessageBuffer,
            self.message_buffer.used_bytes(),
        );
        self.memory
            .set_component_usage(MemoryComponent::RaftLogCache, self.raft_cache.used_bytes());
        self.memory
            .set_component_usage(MemoryComponent::ConnectionPool, self.connection_pool_bytes);
        self.memory
            .set_component_usage(MemoryComponent::BlockCache, self.block_cache.used_bytes());
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
