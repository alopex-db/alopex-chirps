use alopex_chirps_wire::hlc::HybridTimestamp;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Injectable physical clock used by [`LocalHlc`].
pub trait Clock: Send + Sync {
    fn now_millis(&self) -> u64;
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
}

impl LocalHlc {
    pub fn new(max_clock_skew: Duration) -> Self {
        Self::with_clock(max_clock_skew, Arc::new(SystemClock))
    }

    pub fn with_clock(max_clock_skew: Duration, clock: Arc<dyn Clock>) -> Self {
        Self {
            current: HybridTimestamp::new(clock.now_millis(), 0),
            max_clock_skew,
            clock,
        }
    }

    pub fn tick(&mut self) -> HybridTimestamp {
        let wall = self.clock.now_millis();
        self.current = if wall > self.current.physical {
            HybridTimestamp::new(wall, 0)
        } else {
            self.current.next()
        };
        self.current
    }

    pub fn receive(&mut self, remote: HybridTimestamp) -> Result<HybridTimestamp, HlcError> {
        let wall = self.clock.now_millis();
        let max_skew_millis = self.max_clock_skew.as_millis().min(u128::from(u64::MAX)) as u64;
        if remote.physical > wall.saturating_add(max_skew_millis) {
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
        Ok(self.current)
    }

    pub fn current(&self) -> HybridTimestamp {
        self.current
    }
}
