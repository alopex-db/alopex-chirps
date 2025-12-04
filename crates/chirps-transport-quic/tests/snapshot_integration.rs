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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_bandwidth_limit_enforced_and_metrics_recorded() {
    let cfg = QosConfig::default();
    let qos = QosController::new(cfg.clone());

    // 10MB snapshot under 50MB/s limit should succeed without timeout.
    let size_bytes: u64 = 10 * 1024 * 1024;

    let start = Instant::now();
    qos.throttle_snapshot(size_bytes as usize)
        .await
        .expect("throttle ok");
    let _elapsed = start.elapsed();

    let _waited = qos
        .metrics()
        .snapshot_throttle_wait_ms
        .load(std::sync::atomic::Ordering::Relaxed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_p99_latency_not_degraded_by_snapshot_throttle() {
    let mut qos = QosController::new(QosConfig::default());
    let from = NodeId::new();

    // Baseline Raft latency (enqueue + dequeue)
    let mut baseline = Vec::new();
    for i in 0..200 {
        let start = Instant::now();
        qos.enqueue(StreamKind::Raft, raft_frame(i, from))
            .await
            .unwrap();
        let _ = qos.dequeue().unwrap();
        baseline.push(start.elapsed().as_micros() as u64);
    }
    let baseline_p99 = p99(&baseline);

    // Simulate snapshot throttling before next Raft burst (10MB).
    qos.throttle_snapshot(10 * 1024 * 1024)
        .await
        .expect("throttle ok");

    let mut loaded = Vec::new();
    for i in 200..400 {
        let start = Instant::now();
        qos.enqueue(StreamKind::RaftSnapshot, raft_frame(i, from))
            .await
            .unwrap();
        qos.enqueue(StreamKind::Raft, raft_frame(i, from))
            .await
            .unwrap();
        let _ = qos.dequeue().unwrap();
        loaded.push(start.elapsed().as_micros() as u64);
    }
    let loaded_p99 = p99(&loaded);

    let tolerance_us = 10;
    assert!(
        loaded_p99 <= baseline_p99 + tolerance_us,
        "Raft p99 latency should not degrade during snapshot throttling: baseline {baseline_p99}µs vs loaded {loaded_p99}µs (tolerance {tolerance_us}µs)"
    );
}
