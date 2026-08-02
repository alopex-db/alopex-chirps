use crate::session::TransferKind;
use prometheus::{
    Counter, CounterVec, Error as PrometheusError, Gauge, HistogramOpts, HistogramVec, Opts,
    Registry,
};
use std::sync::atomic::{AtomicU64, Ordering};

/// In-memory counters for file transfer activity.
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
    /// Records a transfer start.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_transfer_start(&self) {
        self.transfers_started.fetch_add(1, Ordering::Relaxed);
        self.active_transfers.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a transfer completion and updates bytes sent.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_transfer_complete(&self, bytes: u64) {
        self.transfers_completed.fetch_add(1, Ordering::Relaxed);
        self.active_transfers.fetch_sub(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a failed transfer.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_transfer_failed(&self) {
        self.transfers_failed.fetch_add(1, Ordering::Relaxed);
        self.active_transfers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records a cancelled transfer.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_transfer_cancelled(&self) {
        self.transfers_cancelled.fetch_add(1, Ordering::Relaxed);
        self.active_transfers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records a sent chunk and adds to bytes sent.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_chunk_sent(&self, size: u64) {
        self.chunks_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(size, Ordering::Relaxed);
    }

    /// Records a received chunk and adds to bytes received.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_chunk_received(&self, size: u64) {
        self.chunks_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(size, Ordering::Relaxed);
    }

    /// Records a chunk retry.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_retry(&self) {
        self.chunks_retried.fetch_add(1, Ordering::Relaxed);
    }

    /// Records checksum verification and failure if applicable.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_checksum_verification(&self, success: bool) {
        self.checksum_verifications.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.checksum_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Sets the active transfer count.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn set_active_transfers(&self, value: u64) {
        self.active_transfers.store(value, Ordering::Relaxed);
    }

    /// Sets the number of chunks currently in flight.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn set_chunks_in_flight(&self, value: u64) {
        self.chunks_in_flight.store(value, Ordering::Relaxed);
    }
}

/// Prometheus metric set for file transfer operations.
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
    /// Registers and returns a metrics set owned by `registry`.
    ///
    /// # Errors
    /// Returns `PrometheusError` if metric registration fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn register(registry: &Registry) -> Result<Self, PrometheusError> {
        let transfers_total = CounterVec::new(
            Opts::new(
                "chirps_ft_transfers_total",
                "Total number of file transfers",
            ),
            &["kind", "status"],
        )?;
        let chunks_total = CounterVec::new(
            Opts::new(
                "chirps_ft_chunks_total",
                "Total number of chunks transferred",
            ),
            &["direction"],
        )?;
        let bytes_total = CounterVec::new(
            Opts::new("chirps_ft_bytes_total", "Total bytes transferred"),
            &["direction"],
        )?;
        let checksum_failures_total = CounterVec::new(
            Opts::new(
                "chirps_ft_checksum_failures_total",
                "Total checksum verification failures",
            ),
            &["level"],
        )?;
        let retries_total = Counter::with_opts(Opts::new(
            "chirps_ft_retries_total",
            "Total number of chunk retries",
        ))?;
        let active_transfers = Gauge::with_opts(Opts::new(
            "chirps_ft_active_transfers",
            "Number of active transfers",
        ))?;
        let chunks_in_flight = Gauge::with_opts(Opts::new(
            "chirps_ft_chunks_in_flight",
            "Number of chunks currently in flight",
        ))?;
        let transfer_duration = HistogramVec::new(
            HistogramOpts::new(
                "chirps_ft_transfer_duration_seconds",
                "Transfer duration in seconds",
            )
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]),
            &["kind"],
        )?;
        let chunk_latency = HistogramVec::new(
            HistogramOpts::new(
                "chirps_ft_chunk_latency_seconds",
                "Chunk transfer latency in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
            &[],
        )?;
        let throughput = HistogramVec::new(
            HistogramOpts::new(
                "chirps_ft_throughput_bytes_per_sec",
                "Transfer throughput in bytes per second",
            )
            .buckets(vec![1e6, 1e7, 5e7, 1e8, 5e8, 1e9]),
            &["kind"],
        )?;

        registry.register(Box::new(transfers_total.clone()))?;
        registry.register(Box::new(chunks_total.clone()))?;
        registry.register(Box::new(bytes_total.clone()))?;
        registry.register(Box::new(checksum_failures_total.clone()))?;
        registry.register(Box::new(retries_total.clone()))?;
        registry.register(Box::new(active_transfers.clone()))?;
        registry.register(Box::new(chunks_in_flight.clone()))?;
        registry.register(Box::new(transfer_duration.clone()))?;
        registry.register(Box::new(chunk_latency.clone()))?;
        registry.register(Box::new(throughput.clone()))?;

        Ok(PrometheusMetrics {
            transfers_total,
            chunks_total,
            bytes_total,
            checksum_failures_total,
            retries_total,
            active_transfers,
            chunks_in_flight,
            transfer_duration,
            chunk_latency,
            throughput,
        })
    }

    /// Records a transfer event with kind and status labels.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_transfer(&self, kind: TransferKind, status: &str) {
        self.transfers_total
            .with_label_values(&[kind_label(kind), status])
            .inc();
    }

    /// Records a chunk and bytes count for the given direction.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_chunk(&self, direction: &str, bytes: u64) {
        self.chunks_total.with_label_values(&[direction]).inc();
        self.bytes_total
            .with_label_values(&[direction])
            .inc_by(bytes as f64);
    }

    /// Records a checksum failure at the given level.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_checksum_failure(&self, level: &str) {
        self.checksum_failures_total
            .with_label_values(&[level])
            .inc();
    }

    /// Records a retry event.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn record_retry(&self) {
        self.retries_total.inc();
    }

    /// Sets the active transfer gauge.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn set_active_transfers(&self, value: i64) {
        self.active_transfers.set(value as f64);
    }

    /// Sets the chunks in flight gauge.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn set_chunks_in_flight(&self, value: i64) {
        self.chunks_in_flight.set(value as f64);
    }

    /// Observes a transfer duration for the given kind.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn observe_transfer_duration(&self, kind: TransferKind, seconds: f64) {
        self.transfer_duration
            .with_label_values(&[kind_label(kind)])
            .observe(seconds);
    }

    /// Observes a chunk latency value.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn observe_chunk_latency(&self, seconds: f64) {
        self.chunk_latency.with_label_values(&[]).observe(seconds);
    }

    /// Observes throughput in bytes/sec for the given kind.
    ///
    /// # Panics
    /// This method does not panic.
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
