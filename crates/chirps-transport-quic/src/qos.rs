use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bincode::serialized_size;
use chirps_wire::frame::Frame;

use crate::{StreamKind, priority::Priority};

const SOFT_LIMIT_RATIO: f32 = 0.90;
const HARD_LIMIT_RATIO: f32 = 1.00;
const RECOVERY_RATIO: f32 = 0.80;

#[derive(Clone, Debug)]
pub struct QosConfig {
    pub enabled: bool,
    pub bandwidth: BandwidthConfig,
    pub queue_limits: QueueLimits,
}

impl Default for QosConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bandwidth: BandwidthConfig::default(),
            queue_limits: QueueLimits::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueueLimits {
    pub raft_max_bytes: usize,
    pub raft_max_items: usize,
    pub user_max_bytes: usize,
    pub user_max_items: usize,
    pub gossip_max_bytes: usize,
    pub gossip_max_items: usize,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            raft_max_bytes: 16 * 1024 * 1024,
            raft_max_items: 10_000,
            user_max_bytes: 64 * 1024 * 1024,
            user_max_items: 50_000,
            gossip_max_bytes: 8 * 1024 * 1024,
            gossip_max_items: 5_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BandwidthConfig {
    pub raft_ratio: f32,
    pub user_ratio: f32,
    pub gossip_ratio: f32,
    pub total_bandwidth: Option<u64>,
    pub snapshot_bandwidth_limit: u64,
    pub throttle_timeout: Duration,
}

impl Default for BandwidthConfig {
    fn default() -> Self {
        Self {
            raft_ratio: 0.40,
            user_ratio: 0.50,
            gossip_ratio: 0.10,
            total_bandwidth: None,
            snapshot_bandwidth_limit: 50 * 1024 * 1024,
            throttle_timeout: Duration::from_secs(5),
        }
    }
}

pub struct QosMetrics {
    pub backpressure_triggered_total: AtomicU64,
    pub queue_overflow_total: AtomicU64,
    pub queue_utilization: HashMap<StreamKind, AtomicU64>,
    pub snapshot_throttle_wait_ms: AtomicU64,
}

impl Default for QosMetrics {
    fn default() -> Self {
        QosMetrics {
            backpressure_triggered_total: AtomicU64::new(0),
            queue_overflow_total: AtomicU64::new(0),
            queue_utilization: HashMap::new(),
            snapshot_throttle_wait_ms: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
pub enum QosError {
    QueueFull {
        kind: StreamKind,
        size: usize,
        limit: usize,
    },
    ThrottleTimeout,
    InvalidStreamKind,
    Serialize(String),
}

pub struct QosController {
    queues: HashMap<StreamKind, StreamQueue>,
    metrics: QosMetrics,
    config: QosConfig,
    snapshot_bucket: TokenBucket,
}

impl QosController {
    pub fn new(config: QosConfig) -> Self {
        let mut queues = HashMap::new();
        let limits = &config.queue_limits;

        let mut metrics = QosMetrics::default();
        for kind in [
            StreamKind::Control,
            StreamKind::Raft,
            StreamKind::RaftSnapshot,
            StreamKind::Gossip,
            StreamKind::User,
        ] {
            metrics.queue_utilization.insert(kind, AtomicU64::new(0));
            let (max_bytes, max_items) = queue_limits_for_kind(limits, kind);
            queues.insert(
                kind,
                StreamQueue::new(max_bytes, max_items, kind.priority()),
            );
        }

        let bucket = TokenBucket::new(
            config.bandwidth.snapshot_bandwidth_limit,
            config.bandwidth.snapshot_bandwidth_limit,
        );

        QosController {
            queues,
            metrics,
            config,
            snapshot_bucket: bucket,
        }
    }

    pub fn enqueue(&mut self, kind: StreamKind, frame: Frame) -> Result<(), QosError> {
        let queue = self
            .queues
            .get_mut(&kind)
            .ok_or(QosError::InvalidStreamKind)?;

        let size = serialized_size(&frame)
            .map(|s| s as usize)
            .map_err(|e| QosError::Serialize(e.to_string()))?;

        let prospective_bytes = queue.current_bytes.saturating_add(size);
        let prospective_items = queue.current_items + 1;
        let util = utilization_ratio(prospective_bytes, prospective_items, queue);

        if util >= HARD_LIMIT_RATIO {
            self.metrics
                .queue_overflow_total
                .fetch_add(1, Ordering::Relaxed);
            queue.backpressured = true;
            self.metrics
                .queue_utilization
                .get(&kind)
                .map(|m| m.store((util * 100.0) as u64, Ordering::Relaxed));
            return Err(QosError::QueueFull {
                kind,
                size: prospective_items,
                limit: queue.max_items,
            });
        }

        if util >= SOFT_LIMIT_RATIO || queue.backpressured {
            queue.backpressured = true;
            self.metrics
                .backpressure_triggered_total
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .queue_utilization
                .get(&kind)
                .map(|m| m.store((util * 100.0) as u64, Ordering::Relaxed));
            return Err(QosError::QueueFull {
                kind,
                size: prospective_items,
                limit: queue.max_items,
            });
        }

        queue.current_bytes = prospective_bytes;
        queue.current_items = prospective_items;
        queue.items.push_back(frame);

        let util = utilization_ratio(queue.current_bytes, queue.current_items, queue);
        self.metrics
            .queue_utilization
            .get(&kind)
            .map(|m| m.store((util * 100.0) as u64, Ordering::Relaxed));

        if util < RECOVERY_RATIO {
            queue.backpressured = false;
        }

        Ok(())
    }

    pub fn dequeue(&mut self) -> Option<(StreamKind, Frame)> {
        // Priority ordering: Raft/Control > RaftSnapshot/Gossip > User
        let order = [
            StreamKind::Raft,
            StreamKind::Control,
            StreamKind::RaftSnapshot,
            StreamKind::Gossip,
            StreamKind::User,
        ];

        for kind in order {
            if let Some(queue) = self.queues.get_mut(&kind) {
                if let Some(frame) = queue.items.pop_front() {
                    queue.current_items = queue.current_items.saturating_sub(1);
                    let size = serialized_size(&frame).unwrap_or(0) as usize;
                    queue.current_bytes = queue.current_bytes.saturating_sub(size);

                    let util = utilization_ratio(queue.current_bytes, queue.current_items, queue);
                    self.metrics
                        .queue_utilization
                        .get(&kind)
                        .map(|m| m.store((util * 100.0) as u64, Ordering::Relaxed));
                    if util < RECOVERY_RATIO {
                        queue.backpressured = false;
                    }

                    return Some((kind, frame));
                }
            }
        }
        None
    }

    pub fn is_backpressured(&self, kind: StreamKind) -> bool {
        self.queues
            .get(&kind)
            .map(|q| q.backpressured)
            .unwrap_or(false)
    }

    pub fn utilization(&self, kind: StreamKind) -> f32 {
        self.queues
            .get(&kind)
            .map(|q| utilization_ratio(q.current_bytes, q.current_items, q))
            .unwrap_or(0.0)
    }

    pub async fn throttle_snapshot(&self, size: usize) -> Result<(), QosError> {
        // Placeholder: will be fully implemented with retry/timeout in Task 6.
        if let Err(wait) = self.snapshot_bucket.try_consume(size as u64) {
            // Simulate throttle by sleeping once for the required wait duration.
            if wait > self.config.bandwidth.throttle_timeout {
                return Err(QosError::ThrottleTimeout);
            }
            tokio::time::sleep(wait).await;
            self.snapshot_bucket
                .try_consume(size as u64)
                .map_err(|_| QosError::ThrottleTimeout)?;
        }
        Ok(())
    }

    pub fn metrics(&self) -> &QosMetrics {
        &self.metrics
    }
}

struct StreamQueue {
    items: VecDeque<Frame>,
    current_bytes: usize,
    current_items: usize,
    max_bytes: usize,
    max_items: usize,
    backpressured: bool,
    priority: Priority,
}

impl StreamQueue {
    fn new(max_bytes: usize, max_items: usize, priority: Priority) -> Self {
        StreamQueue {
            items: VecDeque::new(),
            current_bytes: 0,
            current_items: 0,
            max_bytes,
            max_items,
            backpressured: false,
            priority,
        }
    }
}

fn queue_limits_for_kind(limits: &QueueLimits, kind: StreamKind) -> (usize, usize) {
    match kind {
        StreamKind::Control | StreamKind::Raft | StreamKind::RaftSnapshot => {
            (limits.raft_max_bytes, limits.raft_max_items)
        }
        StreamKind::User => (limits.user_max_bytes, limits.user_max_items),
        StreamKind::Gossip => (limits.gossip_max_bytes, limits.gossip_max_items),
    }
}

fn utilization_ratio(bytes: usize, items: usize, queue: &StreamQueue) -> f32 {
    let byte_ratio = bytes as f32 / queue.max_bytes as f32;
    let item_ratio = items as f32 / queue.max_items as f32;
    byte_ratio.max(item_ratio)
}

/// Simple token bucket used for snapshot throttling (expanded in Task 6).
pub struct TokenBucket {
    tokens: AtomicU64,
    capacity: u64,
    refill_rate: u64,
    last_refill: Mutex<Instant>,
}

impl TokenBucket {
    pub fn new(capacity: u64, refill_rate: u64) -> Self {
        TokenBucket {
            tokens: AtomicU64::new(capacity),
            capacity,
            refill_rate,
            last_refill: Mutex::new(Instant::now()),
        }
    }

    pub fn try_consume(&self, amount: u64) -> Result<(), Duration> {
        self.refill();
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < amount {
                let deficit = amount - current;
                let wait_secs = deficit as f64 / self.refill_rate as f64;
                let wait = Duration::from_secs_f64(wait_secs);
                return Err(wait);
            }
            let new = current - amount;
            if self
                .tokens
                .compare_exchange(current, new, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    pub fn refill(&self) {
        let mut guard = self
            .last_refill
            .lock()
            .expect("token bucket mutex poisoned");
        let now = Instant::now();
        let elapsed = now.duration_since(*guard);
        if elapsed.is_zero() {
            return;
        }
        let add = (self.refill_rate as u128 * elapsed.as_micros() / 1_000_000) as u64;
        if add > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let new = (current + add).min(self.capacity);
            self.tokens.store(new, Ordering::Relaxed);
            *guard = now;
        }
    }
}
