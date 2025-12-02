use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bincode::serialized_size;
use chirps_wire::frame::Frame;

use crate::{
    StreamKind,
    config::{QosConfig, QueueLimits},
    events::{TransportEvent, emit_event},
    priority::{PriorityScheduler, ScheduledMessage, SchedulerConfig},
};
use tracing::debug;

const SOFT_LIMIT_RATIO: f32 = 0.90;
const HARD_LIMIT_RATIO: f32 = 1.00;
const RECOVERY_RATIO: f32 = 0.80;

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

/// Per-stream QoS manager that applies backpressure, snapshot throttling, and priority scheduling.
pub struct QosController {
    queues: HashMap<StreamKind, StreamQueue>,
    metrics: QosMetrics,
    config: QosConfig,
    snapshot_bucket: TokenBucket,
    scheduler: PriorityScheduler<QueuedFrame>,
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
            queues.insert(kind, StreamQueue::new(max_bytes, max_items));
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
            scheduler: PriorityScheduler::new(SchedulerConfig::default()),
        }
    }

    pub async fn enqueue(&mut self, kind: StreamKind, frame: Frame) -> Result<(), QosError> {
        let size = frame_size(&frame)?;

        if kind == StreamKind::RaftSnapshot {
            self.throttle_snapshot(size).await?;
        }

        let queue = self
            .queues
            .get_mut(&kind)
            .ok_or(QosError::InvalidStreamKind)?;

        let prospective_bytes = queue.current_bytes.saturating_add(size);
        let prospective_items = queue.current_items + 1;
        let util = utilization_ratio(prospective_bytes, prospective_items, queue);
        let was_backpressured = queue.backpressured;

        if util >= HARD_LIMIT_RATIO {
            self.metrics
                .queue_overflow_total
                .fetch_add(1, Ordering::Relaxed);
            queue.backpressured = true;
            self.metrics
                .queue_utilization
                .get(&kind)
                .map(|m| m.store((util * 100.0) as u64, Ordering::Relaxed));
            if !was_backpressured {
                emit_event(TransportEvent::BackpressureTriggered {
                    stream_kind: format!("{kind:?}"),
                    queue_size: prospective_items,
                    queue_limit: queue.max_items,
                });
            }
            debug!(
                event = "backpressure",
                stream_kind = ?kind,
                utilization = util,
                queue_size = prospective_items,
                limit_items = queue.max_items,
                limit_bytes = queue.max_bytes,
                hard = true,
                "backpressure_triggered"
            );
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
            if !was_backpressured {
                emit_event(TransportEvent::BackpressureTriggered {
                    stream_kind: format!("{kind:?}"),
                    queue_size: prospective_items,
                    queue_limit: queue.max_items,
                });
            }
            debug!(
                event = "backpressure",
                stream_kind = ?kind,
                utilization = util,
                queue_size = prospective_items,
                limit_items = queue.max_items,
                limit_bytes = queue.max_bytes,
                hard = false,
                "backpressure_triggered"
            );
            return Err(QosError::QueueFull {
                kind,
                size: prospective_items,
                limit: queue.max_items,
            });
        }

        queue.current_bytes = prospective_bytes;
        queue.current_items = prospective_items;

        let priority = kind.priority();
        self.scheduler.enqueue(
            ScheduledMessage::new(priority, size, QueuedFrame { kind, frame }),
            priority,
        );

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
        if let Some(next) = self.scheduler.dequeue() {
            let kind = next.payload.kind;
            if let Some(queue) = self.queues.get_mut(&kind) {
                queue.current_items = queue.current_items.saturating_sub(1);
                queue.current_bytes = queue.current_bytes.saturating_sub(next.size_bytes);

                let util = utilization_ratio(queue.current_bytes, queue.current_items, queue);
                self.metrics
                    .queue_utilization
                    .get(&kind)
                    .map(|m| m.store((util * 100.0) as u64, Ordering::Relaxed));
                if util < RECOVERY_RATIO {
                    queue.backpressured = false;
                }
            }
            return Some((kind, next.payload.frame));
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
        let mut attempts = 0;
        let mut waited = Duration::ZERO;
        loop {
            match self.snapshot_bucket.try_consume(size as u64) {
                Ok(_) => {
                    self.metrics
                        .snapshot_throttle_wait_ms
                        .fetch_add(waited.as_millis() as u64, Ordering::Relaxed);
                    debug!(
                        event = "snapshot_throttle",
                        size_bytes = size,
                        waited_ms = waited.as_millis(),
                        attempts,
                        "throttle_snapshot_ok"
                    );
                    return Ok(());
                }
                Err(delay) => {
                    attempts += 1;
                    waited += delay;
                    if waited > self.config.bandwidth.throttle_timeout || attempts >= 3 {
                        self.metrics
                            .snapshot_throttle_wait_ms
                            .fetch_add(waited.as_millis() as u64, Ordering::Relaxed);
                        debug!(
                            event = "snapshot_throttle",
                            size_bytes = size,
                            waited_ms = waited.as_millis(),
                            attempts,
                            "throttle_snapshot_timeout"
                        );
                        return Err(QosError::ThrottleTimeout);
                    }
                    debug!(
                        event = "snapshot_throttle",
                        size_bytes = size,
                        wait_ms = delay.as_millis(),
                        attempts,
                        "throttle_wait"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub fn metrics(&self) -> &QosMetrics {
        &self.metrics
    }
}

struct StreamQueue {
    current_bytes: usize,
    current_items: usize,
    max_bytes: usize,
    max_items: usize,
    backpressured: bool,
}

impl StreamQueue {
    fn new(max_bytes: usize, max_items: usize) -> Self {
        StreamQueue {
            current_bytes: 0,
            current_items: 0,
            max_bytes,
            max_items,
            backpressured: false,
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

#[derive(Clone)]
struct QueuedFrame {
    kind: StreamKind,
    frame: Frame,
}

fn frame_size(frame: &Frame) -> Result<usize, QosError> {
    serialized_size(frame)
        .map(|s| s as usize)
        .map_err(|e| QosError::Serialize(e.to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use chirps_wire::frame::{Frame, UserMessage};
    use crate::BandwidthConfig;
    use tracing_test::traced_test;

    fn user_frame(len: usize) -> Frame {
        Frame::User(UserMessage {
            payload: vec![0u8; len],
        })
    }

    fn user_limits(bytes: usize, items: usize) -> QueueLimits {
        QueueLimits {
            raft_max_bytes: 1_000_000,
            raft_max_items: 1000,
            user_max_bytes: bytes,
            user_max_items: items,
            gossip_max_bytes: 1_000_000,
            gossip_max_items: 1000,
        }
    }

    fn qos_with_limits(bytes: usize, items: usize, bandwidth: BandwidthConfig) -> QosController {
        let mut cfg = QosConfig::default();
        cfg.queue_limits = user_limits(bytes, items);
        cfg.bandwidth = bandwidth;
        QosController::new(cfg)
    }

    #[traced_test]
    #[tokio::test]
    async fn soft_backpressure_triggers_metrics_and_logs() {
        let frame = user_frame(32);
        let size = serialized_size(&frame).unwrap() as usize;
        let max_bytes = ((size as f32) * 1.05) as usize; // ~95% utilization on first enqueue
        let mut qos = qos_with_limits(max_bytes, 100, BandwidthConfig::default());

        let err = qos.enqueue(StreamKind::User, frame).await.unwrap_err();
        match err {
            QosError::QueueFull { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }

        assert_eq!(
            qos.metrics.backpressure_triggered_total.load(Ordering::Relaxed),
            1
        );
        assert!(qos.is_backpressured(StreamKind::User));
        assert!(logs_contain("backpressure_triggered"));
    }

    #[traced_test]
    #[tokio::test]
    async fn hard_overflow_increments_overflow_metrics_and_logs() {
        let frame = user_frame(16);
        let size = serialized_size(&frame).unwrap() as usize;
        let max_bytes = size; // hard limit hit on first enqueue
        let mut qos = qos_with_limits(max_bytes, 1, BandwidthConfig::default());

        let err = qos.enqueue(StreamKind::User, frame).await.unwrap_err();
        match err {
            QosError::QueueFull { limit, .. } => assert_eq!(limit, 1),
            other => panic!("unexpected error: {other:?}"),
        }

        assert_eq!(
            qos.metrics.queue_overflow_total.load(Ordering::Relaxed),
            1
        );
        assert!(logs_contain("backpressure_triggered"));
    }

    #[traced_test]
    #[tokio::test]
    async fn hysteresis_recovers_after_dequeue() {
        let frame = user_frame(8);
        let size = serialized_size(&frame).unwrap() as usize;
        let max_bytes = size * 3;
        let mut qos = qos_with_limits(max_bytes, 10, BandwidthConfig::default());

        qos.enqueue(StreamKind::User, frame.clone()).await.unwrap();
        qos.enqueue(StreamKind::User, frame.clone()).await.unwrap();
        assert!(qos.enqueue(StreamKind::User, frame.clone()).await.is_err());
        assert!(qos.is_backpressured(StreamKind::User));

        let _ = qos.dequeue();
        assert!(!qos.is_backpressured(StreamKind::User));
    }

    #[test]
    fn token_bucket_returns_wait_duration() {
        let bucket = TokenBucket::new(100, 50);
        bucket.try_consume(60).expect("should have capacity");
        let err = bucket.try_consume(100).unwrap_err();
        assert!(err > Duration::ZERO);
    }

    #[traced_test]
    #[tokio::test]
    async fn throttle_snapshot_timeout_emits_log_and_metrics() {
        let mut bandwidth = BandwidthConfig::default();
        bandwidth.snapshot_bandwidth_limit = 100;
        bandwidth.throttle_timeout = Duration::from_millis(5);
        let mut qos = qos_with_limits(10_000, 10, bandwidth);

        let res = qos.throttle_snapshot(500).await;
        assert!(matches!(res, Err(QosError::ThrottleTimeout)));
        assert!(
            qos.metrics
                .snapshot_throttle_wait_ms
                .load(Ordering::Relaxed)
                > 0
        );
        assert!(logs_contain("throttle_snapshot_timeout"));
    }
}
