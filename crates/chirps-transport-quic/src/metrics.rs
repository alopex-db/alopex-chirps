use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{StreamKind, telemetry::ensure_metrics_recorder};

const MAX_SAMPLES: usize = 1000;

#[derive(Debug, Default, Clone)]
pub struct LatencySnapshot {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub count: usize,
}

pub struct LatencyHistogram {
    p50: AtomicU64,
    p95: AtomicU64,
    p99: AtomicU64,
    samples: RwLock<Vec<u64>>,
    max_samples: usize,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        LatencyHistogram {
            p50: AtomicU64::new(0),
            p95: AtomicU64::new(0),
            p99: AtomicU64::new(0),
            samples: RwLock::new(Vec::new()),
            max_samples: MAX_SAMPLES,
        }
    }
}

impl LatencyHistogram {
    pub fn add_sample(&self, value: u64) {
        let mut guard = self.samples.write().expect("latency lock poisoned");
        if guard.len() >= self.max_samples {
            guard.remove(0);
        }
        guard.push(value);
        if guard.len() % 100 == 0 {
            self.update_percentiles(&guard);
        }
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let guard = self.samples.read().expect("latency lock poisoned");
        if guard.is_empty() {
            return LatencySnapshot::default();
        }
        let mut sorted = guard.clone();
        sorted.sort_unstable();
        let p50_idx = (sorted.len() as f32 * 0.50) as usize;
        let p95_idx = (sorted.len() as f32 * 0.95) as usize;
        let p99_idx = (sorted.len() as f32 * 0.99) as usize;

        let snapshot = LatencySnapshot {
            p50: *sorted.get(p50_idx.min(sorted.len() - 1)).unwrap_or(&0),
            p95: *sorted.get(p95_idx.min(sorted.len() - 1)).unwrap_or(&0),
            p99: *sorted.get(p99_idx.min(sorted.len() - 1)).unwrap_or(&0),
            count: sorted.len(),
        };

        self.p50.store(snapshot.p50, Ordering::Relaxed);
        self.p95.store(snapshot.p95, Ordering::Relaxed);
        self.p99.store(snapshot.p99, Ordering::Relaxed);
        snapshot
    }

    fn update_percentiles(&self, samples: &[u64]) {
        if samples.is_empty() {
            return;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let p50_idx = (sorted.len() as f32 * 0.50) as usize;
        let p95_idx = (sorted.len() as f32 * 0.95) as usize;
        let p99_idx = (sorted.len() as f32 * 0.99) as usize;
        self.p50.store(
            *sorted.get(p50_idx.min(sorted.len() - 1)).unwrap_or(&0),
            Ordering::Relaxed,
        );
        self.p95.store(
            *sorted.get(p95_idx.min(sorted.len() - 1)).unwrap_or(&0),
            Ordering::Relaxed,
        );
        self.p99.store(
            *sorted.get(p99_idx.min(sorted.len() - 1)).unwrap_or(&0),
            Ordering::Relaxed,
        );
    }
}

#[derive(Debug, Default, Clone)]
pub struct MetricsSnapshot {
    pub stream_sent: HashMap<StreamKind, u64>,
    pub stream_received: HashMap<StreamKind, u64>,
    pub retransmission_total: u64,
    pub retransmission_buffer_bytes: u64,
    pub message_dropped_total: u64,
    pub backpressure_triggered_total: u64,
    pub queue_overflow_total: u64,
    pub duplicate_received_total: u64,
    pub queue_utilization: HashMap<StreamKind, u64>,
    pub snapshot_throttle_wait_ms: u64,
    pub latency: HashMap<StreamKind, LatencySnapshot>,
}

pub struct ExtendedTransportMetrics {
    pub stream_sent: HashMap<StreamKind, AtomicU64>,
    pub stream_received: HashMap<StreamKind, AtomicU64>,
    pub retransmission_total: AtomicU64,
    pub retransmission_buffer_bytes: AtomicU64,
    pub message_dropped_total: AtomicU64,
    pub backpressure_triggered_total: AtomicU64,
    pub queue_overflow_total: AtomicU64,
    pub duplicate_received_total: AtomicU64,
    pub queue_utilization: HashMap<StreamKind, AtomicU64>,
    pub snapshot_throttle_wait_ms: AtomicU64,
    pub stream_latency: HashMap<StreamKind, LatencyHistogram>,
}

impl ExtendedTransportMetrics {
    pub fn new() -> Self {
        // Ensure a recorder exists even if caller forgot to init_metrics.
        ensure_metrics_recorder();

        let mut stream_sent = HashMap::new();
        let mut stream_received = HashMap::new();
        let mut queue_utilization = HashMap::new();
        let mut stream_latency = HashMap::new();
        for kind in [
            StreamKind::Control,
            StreamKind::Raft,
            StreamKind::RaftSnapshot,
            StreamKind::Gossip,
            StreamKind::User,
        ] {
            stream_sent.insert(kind, AtomicU64::new(0));
            stream_received.insert(kind, AtomicU64::new(0));
            queue_utilization.insert(kind, AtomicU64::new(0));
            stream_latency.insert(kind, LatencyHistogram::default());
        }

        ExtendedTransportMetrics {
            stream_sent,
            stream_received,
            retransmission_total: AtomicU64::new(0),
            retransmission_buffer_bytes: AtomicU64::new(0),
            message_dropped_total: AtomicU64::new(0),
            backpressure_triggered_total: AtomicU64::new(0),
            queue_overflow_total: AtomicU64::new(0),
            duplicate_received_total: AtomicU64::new(0),
            queue_utilization,
            snapshot_throttle_wait_ms: AtomicU64::new(0),
            stream_latency,
        }
    }

    pub fn record_send(&self, kind: StreamKind, latency_us: Option<u64>) {
        if let Some(counter) = self.stream_sent.get(&kind) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        ensure_metrics_recorder();
        match kind {
            StreamKind::Control => {
                metrics::counter!("chirps_stream_sent_total", "kind" => "Control").increment(1);
            }
            StreamKind::Raft => {
                metrics::counter!("chirps_stream_sent_total", "kind" => "Raft").increment(1);
            }
            StreamKind::RaftSnapshot => {
                metrics::counter!("chirps_stream_sent_total", "kind" => "RaftSnapshot")
                    .increment(1);
            }
            StreamKind::Gossip => {
                metrics::counter!("chirps_stream_sent_total", "kind" => "Gossip").increment(1);
            }
            StreamKind::User => {
                metrics::counter!("chirps_stream_sent_total", "kind" => "User").increment(1);
            }
        }
        if let Some(latency) = latency_us {
            if let Some(hist) = self.stream_latency.get(&kind) {
                hist.add_sample(latency);
            }
        }
    }

    pub fn record_receive(&self, kind: StreamKind, latency_us: Option<u64>) {
        if let Some(counter) = self.stream_received.get(&kind) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        ensure_metrics_recorder();
        match kind {
            StreamKind::Control => {
                metrics::counter!("chirps_stream_received_total", "kind" => "Control").increment(1);
            }
            StreamKind::Raft => {
                metrics::counter!("chirps_stream_received_total", "kind" => "Raft").increment(1);
            }
            StreamKind::RaftSnapshot => {
                metrics::counter!("chirps_stream_received_total", "kind" => "RaftSnapshot")
                    .increment(1);
            }
            StreamKind::Gossip => {
                metrics::counter!("chirps_stream_received_total", "kind" => "Gossip").increment(1);
            }
            StreamKind::User => {
                metrics::counter!("chirps_stream_received_total", "kind" => "User").increment(1);
            }
        }
        if let Some(latency) = latency_us {
            if let Some(hist) = self.stream_latency.get(&kind) {
                hist.add_sample(latency);
            }
        }
    }
    pub fn record_retransmit(&self, count: u64, buffer_bytes: Option<u64>) {
        self.retransmission_total
            .fetch_add(count, Ordering::Relaxed);
        ensure_metrics_recorder();
        metrics::counter!("chirps_retransmission_total").increment(count);
        if let Some(bytes) = buffer_bytes {
            self.retransmission_buffer_bytes
                .store(bytes, Ordering::Relaxed);
        }
    }

    pub fn record_drop(&self) {
        self.message_dropped_total.fetch_add(1, Ordering::Relaxed);
        ensure_metrics_recorder();
        metrics::counter!("chirps_message_dropped_total").increment(1);
    }

    pub fn record_backpressure(&self) {
        self.backpressure_triggered_total
            .fetch_add(1, Ordering::Relaxed);
        ensure_metrics_recorder();
        metrics::counter!("chirps_backpressure_triggered_total").increment(1);
    }

    pub fn record_queue_overflow(&self) {
        self.queue_overflow_total.fetch_add(1, Ordering::Relaxed);
        ensure_metrics_recorder();
        metrics::counter!("chirps_queue_overflow_total").increment(1);
    }

    pub fn record_duplicate(&self) {
        self.duplicate_received_total
            .fetch_add(1, Ordering::Relaxed);
        ensure_metrics_recorder();
        metrics::counter!("chirps_duplicate_received_total").increment(1);
    }

    pub fn update_queue_utilization(&self, kind: StreamKind, percent: u64) {
        if let Some(g) = self.queue_utilization.get(&kind) {
            g.store(percent.min(100), Ordering::Relaxed);
        }
    }

    pub fn add_throttle_wait(&self, millis: u64) {
        self.snapshot_throttle_wait_ms
            .fetch_add(millis, Ordering::Relaxed);
        ensure_metrics_recorder();
        metrics::counter!("chirps_snapshot_throttle_wait_ms").increment(millis);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut snap = MetricsSnapshot::default();
        for (k, v) in &self.stream_sent {
            snap.stream_sent.insert(*k, v.load(Ordering::Relaxed));
        }
        for (k, v) in &self.stream_received {
            snap.stream_received.insert(*k, v.load(Ordering::Relaxed));
        }
        for (k, v) in &self.queue_utilization {
            snap.queue_utilization.insert(*k, v.load(Ordering::Relaxed));
        }
        for (k, hist) in &self.stream_latency {
            snap.latency.insert(*k, hist.snapshot());
        }
        snap.retransmission_total = self.retransmission_total.load(Ordering::Relaxed);
        snap.retransmission_buffer_bytes = self.retransmission_buffer_bytes.load(Ordering::Relaxed);
        snap.message_dropped_total = self.message_dropped_total.load(Ordering::Relaxed);
        snap.backpressure_triggered_total =
            self.backpressure_triggered_total.load(Ordering::Relaxed);
        snap.queue_overflow_total = self.queue_overflow_total.load(Ordering::Relaxed);
        snap.duplicate_received_total = self.duplicate_received_total.load(Ordering::Relaxed);
        snap.snapshot_throttle_wait_ms = self.snapshot_throttle_wait_ms.load(Ordering::Relaxed);
        snap
    }
}
