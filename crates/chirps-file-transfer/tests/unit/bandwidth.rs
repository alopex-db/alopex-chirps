use alopex_chirps_file_transfer::BandwidthThrottle;
use std::time::Duration;

#[tokio::test]
async fn bandwidth_unlimited_returns_quickly() {
    let throttle = BandwidthThrottle::unlimited();
    let result = tokio::time::timeout(Duration::from_millis(10), throttle.acquire(10_000)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn bandwidth_limit_enforces_wait() {
    let throttle = BandwidthThrottle::new(1_000);
    let result = tokio::time::timeout(Duration::from_millis(5), throttle.acquire(10_000)).await;
    assert!(result.is_err());
}
