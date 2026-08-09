//! Public Hybrid Logical Clock API.

pub use alopex_chirps_gossip_swim::hlc::{
    Clock, HlcAdvance, HlcError, HlcMetricsSink, HlcReceiveResult, LocalHlc, SystemClock,
};
pub use alopex_chirps_wire::hlc::HybridTimestamp;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    struct ManualClock(AtomicU64);

    impl ManualClock {
        fn new(millis: u64) -> Self {
            Self(AtomicU64::new(millis))
        }

        fn set(&self, millis: u64) {
            self.0.store(millis, Ordering::SeqCst);
        }
    }

    impl Clock for ManualClock {
        fn now_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn tick_is_monotonic_across_wall_clock_rollback() {
        let clock = Arc::new(ManualClock::new(100));
        let mut hlc = LocalHlc::with_clock(Duration::from_millis(50), clock.clone());
        let first = hlc.tick();
        clock.set(90);
        let second = hlc.tick();
        assert!(second > first);
        assert_eq!(second, HybridTimestamp::new(100, first.logical + 1));
    }

    #[test]
    fn receive_applies_canonical_merge_rule() {
        let clock = Arc::new(ManualClock::new(100));
        let mut hlc = LocalHlc::with_clock(Duration::from_millis(100), clock.clone());
        assert_eq!(hlc.tick(), HybridTimestamp::new(100, 1));
        assert_eq!(
            hlc.receive(HybridTimestamp::new(100, 5)).unwrap(),
            HybridTimestamp::new(100, 6)
        );
        assert_eq!(
            hlc.receive(HybridTimestamp::new(110, 2)).unwrap(),
            HybridTimestamp::new(110, 3)
        );
        clock.set(120);
        assert_eq!(
            hlc.receive(HybridTimestamp::new(115, 9)).unwrap(),
            HybridTimestamp::new(120, 0)
        );
    }

    #[test]
    fn future_timestamp_beyond_max_skew_is_rejected_without_mutation() {
        let clock = Arc::new(ManualClock::new(100));
        let mut hlc = LocalHlc::with_clock(Duration::from_millis(10), clock);
        let before = hlc.current();
        let error = hlc.receive(HybridTimestamp::new(111, 0)).unwrap_err();
        assert!(matches!(error, HlcError::ClockSkewTooLarge { .. }));
        assert_eq!(hlc.current(), before);
    }
}
