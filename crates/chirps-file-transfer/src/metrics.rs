use crate::session::TransferKind;
use prometheus::{
    Counter, CounterVec, Error as PrometheusError, Gauge, HistogramVec, register_counter,
    register_counter_vec, register_gauge, register_histogram_vec,
};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct FileTransferMetrics {
    pub transfers_started: AtomicU64,
    pub transfers_completed: AtomicU64,
    pub transfers_failed: AtomicU64,
    pub transfers_cancelled: AtomicU64,
    pub chunks_sent: AtomicU64,
    pub chunks_received: AtomicU64,
    pub chunks_retried: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub checksum_verifications: AtomicU64,
    pub checksum_failures: AtomicU64,
    pub active_transfers: AtomicU64,
    pub chunks_in_flight: AtomicU64,
}

impl Default for FileTransferMetrics {
    fn default() -> Self {
        FileTransferMetrics {
            transfers_started: AtomicU64::new(0),
            transfers_completed: AtomicU64::new(0),
            transfers_failed: AtomicU64::new(0),
            transfers_cancelled: AtomicU64::new(0),
            chunks_sent: AtomicU64::new(0),
            chunks_received: AtomicU64::new(0),
            chunks_retried: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            checksum_verifications: AtomicU64::new(0),
            checksum_failures: AtomicU64::new(0),
            active_transfers: AtomicU64::new(0),
            chunks_in_flight: AtomicU64::new(0),
        }
    }
}

impl FileTransferMetrics {
    pub fn record_transfer_start(&self) {
        self.transfers_started.fetch_add(1, Ordering::Relaxed);
        self.active_transfers.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_transfer_complete(&self, bytes: u64) {
        self.transfers_completed.fetch_add(1, Ordering::Relaxed);
        self.active_transfers.fetch_sub(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_transfer_failed(&self) {
        self.transfers_failed.fetch_add(1, Ordering::Relaxed);
        self.active_transfers.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_transfer_cancelled(&self) {
        self.transfers_cancelled.fetch_add(1, Ordering::Relaxed);
        self.active_transfers.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_chunk_sent(&self, size: u64) {
        self.chunks_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(size, Ordering::Relaxed);
    }

    pub fn record_chunk_received(&self, size: u64) {
        self.chunks_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(size, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.chunks_retried.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_checksum_verification(&self, success: bool) {
        self.checksum_verifications.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.checksum_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn set_active_transfers(&self, value: u64) {
        self.active_transfers.store(value, Ordering::Relaxed);
    }

    pub fn set_chunks_in_flight(&self, value: u64) {
        self.chunks_in_flight.store(value, Ordering::Relaxed);
    }
}

pub struct PrometheusMetrics {
    pub transfers_total: CounterVec,
    pub chunks_total: CounterVec,
    pub bytes_total: CounterVec,
    pub checksum_failures_total: CounterVec,
    pub retries_total: Counter,
    pub active_transfers: Gauge,
    pub chunks_in_flight: Gauge,
    pub transfer_duration: HistogramVec,
    pub chunk_latency: HistogramVec,
    pub throughput: HistogramVec,
}

impl PrometheusMetrics {
    pub fn register() -> Result<Self, PrometheusError> {
        Ok(PrometheusMetrics {
            transfers_total: register_counter_vec!(
                "chirps_ft_transfers_total",
                "Total number of file transfers",
                &["kind", "status"]
            )?,
            chunks_total: register_counter_vec!(
                "chirps_ft_chunks_total",
                "Total number of chunks transferred",
                &["direction"]
            )?,
            bytes_total: register_counter_vec!(
                "chirps_ft_bytes_total",
                "Total bytes transferred",
                &["direction"]
            )?,
            checksum_failures_total: register_counter_vec!(
                "chirps_ft_checksum_failures_total",
                "Total checksum verification failures",
                &["level"]
            )?,
            retries_total: register_counter!(
                "chirps_ft_retries_total",
                "Total number of chunk retries"
            )?,
            active_transfers: register_gauge!(
                "chirps_ft_active_transfers",
                "Number of active transfers"
            )?,
            chunks_in_flight: register_gauge!(
                "chirps_ft_chunks_in_flight",
                "Number of chunks currently in flight"
            )?,
            transfer_duration: register_histogram_vec!(
                "chirps_ft_transfer_duration_seconds",
                "Transfer duration in seconds",
                &["kind"],
                vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
            )?,
            chunk_latency: register_histogram_vec!(
                "chirps_ft_chunk_latency_seconds",
                "Chunk transfer latency in seconds",
                &[],
                vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
            )?,
            throughput: register_histogram_vec!(
                "chirps_ft_throughput_bytes_per_sec",
                "Transfer throughput in bytes per second",
                &["kind"],
                vec![1e6, 1e7, 5e7, 1e8, 5e8, 1e9]
            )?,
        })
    }

    pub fn record_transfer(&self, kind: TransferKind, status: &str) {
        self.transfers_total
            .with_label_values(&[kind_label(kind), status])
            .inc();
    }

    pub fn record_chunk(&self, direction: &str, bytes: u64) {
        self.chunks_total.with_label_values(&[direction]).inc();
        self.bytes_total
            .with_label_values(&[direction])
            .inc_by(bytes as f64);
    }

    pub fn record_checksum_failure(&self, level: &str) {
        self.checksum_failures_total
            .with_label_values(&[level])
            .inc();
    }

    pub fn record_retry(&self) {
        self.retries_total.inc();
    }

    pub fn set_active_transfers(&self, value: i64) {
        self.active_transfers.set(value as f64);
    }

    pub fn set_chunks_in_flight(&self, value: i64) {
        self.chunks_in_flight.set(value as f64);
    }

    pub fn observe_transfer_duration(&self, kind: TransferKind, seconds: f64) {
        self.transfer_duration
            .with_label_values(&[kind_label(kind)])
            .observe(seconds);
    }

    pub fn observe_chunk_latency(&self, seconds: f64) {
        self.chunk_latency.with_label_values(&[]).observe(seconds);
    }

    pub fn observe_throughput(&self, kind: TransferKind, bytes_per_sec: f64) {
        self.throughput
            .with_label_values(&[kind_label(kind)])
            .observe(bytes_per_sec);
    }
}

fn kind_label(kind: TransferKind) -> &'static str {
    match kind {
        TransferKind::Send => "Send",
        TransferKind::Broadcast => "Broadcast",
        TransferKind::Sync => "Sync",
    }
}
