use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// A totally ordered Hybrid Logical Clock timestamp.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct HybridTimestamp {
    /// Unix time in milliseconds.
    pub physical: u64,
    /// Causal order within the same physical millisecond.
    pub logical: u32,
}

impl HybridTimestamp {
    pub const fn new(physical: u64, logical: u32) -> Self {
        Self { physical, logical }
    }

    pub fn now() -> Self {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        Self::new(physical, 0)
    }

    /// Return the next representable timestamp while preserving total order.
    pub fn next(self) -> Self {
        match self.logical.checked_add(1) {
            Some(logical) => Self::new(self.physical, logical),
            None => Self::new(self.physical.saturating_add(1), 0),
        }
    }
}
