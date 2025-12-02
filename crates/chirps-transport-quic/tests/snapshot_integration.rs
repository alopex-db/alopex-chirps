use chirps_transport_quic::{QosConfig, QosController, StreamKind};
use chirps_wire::frame::Frame;
use chirps_wire::node_id::NodeId;
use std::time::Instant;

fn raft_frame(seq: u64, from: NodeId) -> Frame {
    Frame::Ping { seq, from }
}

fn p99(latencies: &[u64]) -> u64 {
    if latencies.is_empty() {
        return 0;
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len() as f64) * 0.99).floor() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn qos_with_limit(limit_bytes_per_s: u64, timeout: std::time::Duration) -> QosController {
    let mut cfg = QosConfig::default();
    cfg.bandwidth.snapshot_bandwidth_limit = limit_bytes_per_s;
    cfg.bandwidth.throttle_timeout = timeout;
    QosController::new(cfg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_bandwidth_limit_enforced_and_metrics_recorded() {
    // Use small limit to keep test runtime reasonable.
    let limit_bps = 10 * 1024 * 1024; // 10 MB/s
    let qos = qos_with_limit(limit_bps, std::time::Duration::from_secs(5));

    // 20MB snapshot should be throttled by 10MB/s bucket -> ~2s wait.
    let size_bytes: u64 = 20 * 1024 * 1024;
    let limit = limit_bps as f64;
    let expected_wait_s = (size_bytes.saturating_sub(limit_bps) as f64) / limit;

    let start = Instant::now();
    qos.throttle_snapshot(size_bytes as usize)
        .await
        .expect("throttle ok");
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_secs_f64() >= expected_wait_s * 0.8,
        "throttle wait too short: {:?} vs expected >= {:.3}s",
        elapsed,
        expected_wait_s
    );

    // Effective throughput should respect 50MB/s (allow small slack for timer quantization).
    let rate = size_bytes as f64 / elapsed.as_secs_f64();
    assert!(
        rate <= limit * 1.05,
        "snapshot rate exceeded limit: {:.2} MB/s (limit {:.2})",
        rate / 1024.0 / 1024.0,
        limit / 1024.0 / 1024.0
    );

    let waited = qos
        .metrics()
        .snapshot_throttle_wait_ms
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(waited > 0, "snapshot_throttle_wait_ms should record wait");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_p99_latency_not_degraded_by_snapshot_throttle() {
    let mut qos = qos_with_limit(10 * 1024 * 1024, std::time::Duration::from_secs(5));
    let from = NodeId::new();

    // Baseline Raft latency (enqueue + dequeue)
    let mut baseline = Vec::new();
    for i in 0..200 {
        let start = Instant::now();
        qos.enqueue(StreamKind::Raft, raft_frame(i, from)).await.unwrap();
        let _ = qos.dequeue().unwrap();
        baseline.push(start.elapsed().as_micros() as u64);
    }
    let baseline_p99 = p99(&baseline);

    // Simulate snapshot throttling before next Raft burst.
    qos.throttle_snapshot(100 * 1024 * 1024)
        .await
        .expect("throttle ok");

    let mut loaded = Vec::new();
    for i in 200..400 {
        let start = Instant::now();
        qos.enqueue(StreamKind::RaftSnapshot, raft_frame(i, from))
            .await
            .unwrap();
        qos.enqueue(StreamKind::Raft, raft_frame(i, from)).await.unwrap();
        let _ = qos.dequeue().unwrap();
        loaded.push(start.elapsed().as_micros() as u64);
    }
    let loaded_p99 = p99(&loaded);

    assert!(
        loaded_p99 as f64 <= baseline_p99 as f64,
        "Raft p99 latency should not degrade during snapshot throttling: baseline {baseline_p99}µs vs loaded {loaded_p99}µs"
    );
}
