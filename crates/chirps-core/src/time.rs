use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable Hybrid Logical Clock value shared by TSO and Gossip users.
#[derive(
    Clone, Copy, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct HybridTimestamp {
    /// Unix epoch milliseconds supplied by an injected wall clock.
    pub physical: u64,
    /// Total ordering within one physical millisecond.
    pub logical: u32,
}

impl HybridTimestamp {
    pub const fn new(physical: u64, logical: u32) -> Self {
        Self { physical, logical }
    }

    pub fn checked_next(self) -> Result<Self, TimestampError> {
        self.checked_add(1)
    }

    pub fn checked_add(self, delta: u64) -> Result<Self, TimestampError> {
        const LOGICAL_CARDINALITY: u64 = u32::MAX as u64 + 1;
        let logical = self.logical as u64 + delta;
        let physical_delta = logical / LOGICAL_CARDINALITY;
        let physical = self
            .physical
            .checked_add(physical_delta)
            .ok_or(TimestampError::Overflow)?;
        Ok(Self {
            physical,
            logical: (logical % LOGICAL_CARDINALITY) as u32,
        })
    }
}

/// Inclusive, contiguous timestamp range committed by the TSO state machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampRange {
    pub start: HybridTimestamp,
    pub end: HybridTimestamp,
    pub count: u32,
}

impl TimestampRange {
    pub fn from_start(start: HybridTimestamp, count: u32) -> Result<Self, TimestampError> {
        if count == 0 {
            return Err(TimestampError::EmptyRange);
        }
        let end = start.checked_add(u64::from(count - 1))?;
        Ok(Self { start, end, count })
    }

    pub fn at(self, offset: u32) -> Option<HybridTimestamp> {
        if offset >= self.count {
            return None;
        }
        self.start.checked_add(u64::from(offset)).ok()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum TimestampError {
    #[error("timestamp range must contain at least one value")]
    EmptyRange,
    #[error("hybrid timestamp overflow")]
    Overflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_order_and_range_cross_logical_overflow() {
        let start = HybridTimestamp::new(9, u32::MAX);
        let range = TimestampRange::from_start(start, 2).unwrap();
        assert_eq!(range.end, HybridTimestamp::new(10, 0));
        assert!(range.start < range.end);
    }

    #[test]
    fn timestamp_rejects_physical_overflow() {
        assert_eq!(
            HybridTimestamp::new(u64::MAX, u32::MAX).checked_next(),
            Err(TimestampError::Overflow)
        );
    }
}
