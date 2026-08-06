use alopex_chirps_wire::hlc::HybridTimestamp;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Injectable physical clock used by [`LocalHlc`].
pub trait Clock: Send + Sync {
    fn now_millis(&self) -> u64;
}

/// Bounded classification of how an HLC operation advanced local time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HlcAdvance {
    Physical,
    Logical,
}

/// Bounded result label for an HLC receive operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HlcReceiveResult {
    Success,
    SkewError,
}

/// Optional observation sink for real HLC operations.
///
/// Implementations must keep labels bounded; operation-specific data is
/// represented by enums and numeric values rather than arbitrary strings.
pub trait HlcMetricsSink: Send + Sync {
    fn record_tick(&self, advance: HlcAdvance);

    fn record_receive(
        &self,
        result: HlcReceiveResult,
        clock_skew: Duration,
        advance: Option<HlcAdvance>,
    );
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HlcError {
    #[error(
        "remote physical clock {remote_physical}ms exceeds wall clock {wall_physical}ms by more than {max_skew_millis}ms"
    )]
    ClockSkewTooLarge {
        remote_physical: u64,
        wall_physical: u64,
        max_skew_millis: u64,
    },
}

/// Per-node Hybrid Logical Clock with a bounded future-skew policy.
pub struct LocalHlc {
    current: HybridTimestamp,
    max_clock_skew: Duration,
    clock: Arc<dyn Clock>,
    metrics: Option<Arc<dyn HlcMetricsSink>>,
}

impl LocalHlc {
    pub fn new(max_clock_skew: Duration) -> Self {
        Self::with_clock(max_clock_skew, Arc::new(SystemClock))
    }

    pub fn with_clock(max_clock_skew: Duration, clock: Arc<dyn Clock>) -> Self {
        Self::from_parts(max_clock_skew, clock, None)
    }

    /// Creates a system-clock HLC whose real operations update `metrics`.
    pub fn with_metrics(max_clock_skew: Duration, metrics: Arc<dyn HlcMetricsSink>) -> Self {
        Self::with_clock_and_metrics(max_clock_skew, Arc::new(SystemClock), metrics)
    }

    /// Creates an instrumented HLC with an injectable physical clock.
    pub fn with_clock_and_metrics(
        max_clock_skew: Duration,
        clock: Arc<dyn Clock>,
        metrics: Arc<dyn HlcMetricsSink>,
    ) -> Self {
        Self::from_parts(max_clock_skew, clock, Some(metrics))
    }

    fn from_parts(
        max_clock_skew: Duration,
        clock: Arc<dyn Clock>,
        metrics: Option<Arc<dyn HlcMetricsSink>>,
    ) -> Self {
        Self {
            current: HybridTimestamp::new(clock.now_millis(), 0),
            max_clock_skew,
            clock,
            metrics,
        }
    }

    pub fn tick(&mut self) -> HybridTimestamp {
        let previous = self.current;
        let wall = self.clock.now_millis();
        self.current = if wall > self.current.physical {
            HybridTimestamp::new(wall, 0)
        } else {
            self.current.next()
        };
        if let Some(metrics) = &self.metrics {
            metrics.record_tick(classify_advance(previous, self.current));
        }
        self.current
    }

    pub fn receive(&mut self, remote: HybridTimestamp) -> Result<HybridTimestamp, HlcError> {
        let wall = self.clock.now_millis();
        let max_skew_millis = self.max_clock_skew.as_millis().min(u128::from(u64::MAX)) as u64;
        if remote.physical > wall.saturating_add(max_skew_millis) {
            if let Some(metrics) = &self.metrics {
                metrics.record_receive(
                    HlcReceiveResult::SkewError,
                    Duration::from_millis(remote.physical.abs_diff(wall)),
                    None,
                );
            }
            return Err(HlcError::ClockSkewTooLarge {
                remote_physical: remote.physical,
                wall_physical: wall,
                max_skew_millis,
            });
        }

        let local = self.current;
        let merged_physical = local.physical.max(wall).max(remote.physical);
        self.current = if merged_physical == local.physical && merged_physical == remote.physical {
            HybridTimestamp::new(merged_physical, local.logical.max(remote.logical)).next()
        } else if merged_physical == local.physical {
            local.next()
        } else if merged_physical == remote.physical {
            remote.next()
        } else {
            HybridTimestamp::new(merged_physical, 0)
        };
        if let Some(metrics) = &self.metrics {
            metrics.record_receive(
                HlcReceiveResult::Success,
                Duration::from_millis(remote.physical.abs_diff(wall)),
                Some(classify_advance(local, self.current)),
            );
        }
        Ok(self.current)
    }

    pub fn current(&self) -> HybridTimestamp {
        self.current
    }
}

fn classify_advance(before: HybridTimestamp, after: HybridTimestamp) -> HlcAdvance {
    if after.physical > before.physical {
        HlcAdvance::Physical
    } else {
        HlcAdvance::Logical
    }
}
