use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Injectable physical and lease clocks.
///
/// `lease_millis` must be nondecreasing and comparable across cluster nodes;
/// `physical_millis` may move backwards and is repaired by the replicated HLC
/// logical floor.
pub trait Clock: Send + Sync + 'static {
    fn physical_millis(&self) -> u64;
    fn lease_millis(&self) -> u64;
}

#[derive(Clone, Debug)]
pub struct SystemClock {
    lease_epoch_ms: u64,
    lease_origin: Instant,
}

impl Default for SystemClock {
    fn default() -> Self {
        Self {
            lease_epoch_ms: unix_epoch_millis(),
            lease_origin: Instant::now(),
        }
    }
}

impl Clock for SystemClock {
    fn physical_millis(&self) -> u64 {
        unix_epoch_millis()
    }

    fn lease_millis(&self) -> u64 {
        let elapsed: u64 = self
            .lease_origin
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        self.lease_epoch_ms.saturating_add(elapsed)
    }
}

fn unix_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
