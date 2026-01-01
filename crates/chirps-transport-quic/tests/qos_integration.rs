use alopex_chirps_transport_quic::{QosConfig, QosController, StreamKind};
use alopex_chirps_wire::frame::{Frame, UserMessage};
use alopex_chirps_wire::node_id::NodeId;
use std::time::Instant;

fn user_frame() -> Frame {
    Frame::User(UserMessage {
        payload: b"user".to_vec(),
    })
}

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
async fn raft_p99_latency_under_user_load_stays_within_10pct() {
    let mut qos = QosController::new(QosConfig::default());
    let from = NodeId::new();

    // Baseline: Raft-only
    let mut baseline_lat = Vec::new();
    for i in 0..500 {
        let start = Instant::now();
        qos.enqueue(StreamKind::Raft, raft_frame(i, from))
            .await
            .unwrap();
        let _ = qos.dequeue().unwrap();
        baseline_lat.push(start.elapsed().as_micros() as u64);
    }
    let baseline_p99 = p99(&baseline_lat).max(1);

    // Loaded: heavy User backlog plus Raft messages
    let mut loaded_lat = Vec::new();
    for _ in 0..2_000 {
        qos.enqueue(StreamKind::User, user_frame()).await.unwrap();
    }
    for i in 0..500 {
        let start = Instant::now();
        qos.enqueue(StreamKind::Raft, raft_frame(i, from))
            .await
            .unwrap();
        let _ = qos.dequeue().unwrap();
        loaded_lat.push(start.elapsed().as_micros() as u64);
    }

    let loaded_p99 = p99(&loaded_lat);
    assert!(
        loaded_p99 as f64 <= baseline_p99 as f64 * 1.10 + 1.0,
        "Raft p99 latency degraded more than 10%: baseline {baseline_p99}µs vs loaded {loaded_p99}µs"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_throughput_degrades_less_than_10pct_under_raft_load() {
    let mut qos = QosController::new(QosConfig::default());
    let from = NodeId::new();

    // Baseline user-only drain time
    let user_count = 2_000;
    for _ in 0..user_count {
        qos.enqueue(StreamKind::User, user_frame()).await.unwrap();
    }
    let start = Instant::now();
    while let Some((_kind, _)) = qos.dequeue() {}
    let baseline_time = start.elapsed();

    // Mix with Raft load
    for _ in 0..user_count {
        qos.enqueue(StreamKind::User, user_frame()).await.unwrap();
    }
    for i in 0..1_000 {
        qos.enqueue(StreamKind::Raft, raft_frame(i, from))
            .await
            .unwrap();
    }
    let start = Instant::now();
    let mut drained_users = 0;
    while let Some((kind, _)) = qos.dequeue() {
        if kind == StreamKind::User {
            drained_users += 1;
        }
        if drained_users == user_count {
            break;
        }
    }
    let mixed_time = start.elapsed();

    assert!(
        mixed_time.as_secs_f64() <= baseline_time.as_secs_f64() * 1.10 + 0.001,
        "User throughput degraded >10%: baseline {:?}, mixed {:?}",
        baseline_time,
        mixed_time
    );
}
