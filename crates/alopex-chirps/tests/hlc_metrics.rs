use alopex_chirps::{
    ChirpsMetricsCollector, Clock, HlcError, HybridTimestamp, LocalHlc, NodeConfig,
    start_with_metrics,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

struct FakeClock(AtomicU64);

impl FakeClock {
    fn new(now_millis: u64) -> Self {
        Self(AtomicU64::new(now_millis))
    }

    fn set(&self, now_millis: u64) {
        self.0.store(now_millis, Ordering::Relaxed);
    }
}

impl Clock for FakeClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[test]
fn public_mesh_wiring_api_accepts_the_shared_registry() {
    fn assert_signature<F, Fut>(_start: F)
    where
        F: Fn(NodeConfig, Arc<ChirpsMetricsCollector>) -> Fut,
    {
    }

    assert_signature(start_with_metrics);
}

#[test]
fn real_hlc_operations_are_exported_by_the_chirps_registry() {
    let clock = Arc::new(FakeClock::new(100));
    let collector = Arc::new(ChirpsMetricsCollector::new());
    let mut hlc = LocalHlc::with_clock_and_metrics(
        Duration::from_millis(10),
        clock.clone(),
        collector.clone(),
    );

    clock.set(101);
    assert_eq!(hlc.tick(), HybridTimestamp::new(101, 0));
    clock.set(99);
    assert_eq!(hlc.tick(), HybridTimestamp::new(101, 1));

    clock.set(102);
    assert_eq!(
        hlc.receive(HybridTimestamp::new(103, 0)).unwrap(),
        HybridTimestamp::new(103, 1)
    );
    let before_rejection = hlc.current();
    assert!(matches!(
        hlc.receive(HybridTimestamp::new(113, 0)),
        Err(HlcError::ClockSkewTooLarge { .. })
    ));
    assert_eq!(hlc.current(), before_rejection);

    let body = collector
        .encode()
        .expect("Prometheus encoding must succeed");
    for expected in [
        "chirps_hlc_ticks_total 2",
        "chirps_hlc_receives_total{result=\"success\"} 1",
        "chirps_hlc_receives_total{result=\"skew_error\"} 1",
        "chirps_hlc_logical_advances_total 1",
        "chirps_hlc_physical_advances_total 2",
        "chirps_hlc_clock_skew_seconds_count 2",
    ] {
        assert!(
            body.contains(expected),
            "missing metric: {expected}\n{body}"
        );
    }
    let receive_series: Vec<_> = body
        .lines()
        .filter(|line| line.starts_with("chirps_hlc_receives_total{"))
        .collect();
    assert_eq!(receive_series.len(), 2, "unexpected result label: {body}");
}
