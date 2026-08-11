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
async fn raft_p99_dispatch_turns_under_user_load_are_bounded() {
    const SAMPLE_COUNT: usize = 4_096;
    const USER_BACKLOG: usize = 8_192;
    let from = NodeId::new();

    // This is an in-memory scheduler: wall-clock enqueue/dequeue time is
    // below useful resolution and varies with workspace load. The contract
    // under test is logical service latency, measured in dequeue turns, over
    // enough samples and a substantial sustained User backlog.
    let mut baseline = QosController::new(QosConfig::default());
    let mut baseline_turns = Vec::with_capacity(SAMPLE_COUNT);
    for i in 0..SAMPLE_COUNT {
        baseline
            .enqueue(StreamKind::Raft, raft_frame(i as u64, from))
            .await
            .unwrap();
        let mut turns = 0;
        loop {
            turns += 1;
            if baseline.dequeue().expect("baseline message").0 == StreamKind::Raft {
                break;
            }
        }
        baseline_turns.push(turns);
    }
    let baseline_p99 = p99(&baseline_turns);

    let mut loaded = QosController::new(QosConfig::default());
    for _ in 0..USER_BACKLOG {
        loaded
            .enqueue(StreamKind::User, user_frame())
            .await
            .unwrap();
    }
    let mut loaded_turns = Vec::with_capacity(SAMPLE_COUNT);
    for i in 0..SAMPLE_COUNT {
        loaded
            .enqueue(StreamKind::Raft, raft_frame(i as u64, from))
            .await
            .unwrap();
        let mut turns = 0;
        loop {
            turns += 1;
            if loaded.dequeue().expect("loaded message").0 == StreamKind::Raft {
                break;
            }
        }
        loaded_turns.push(turns);
    }

    let loaded_p99 = p99(&loaded_turns);
    let mut users_served = 0;
    while let Some((kind, _)) = loaded.dequeue() {
        if kind == StreamKind::User {
            users_served += 1;
        }
    }
    assert!(
        baseline_p99 >= 1,
        "baseline must contain measurable dispatch"
    );
    assert!(
        loaded_p99 <= baseline_p99 + 1,
        "Raft dispatch p99 exceeded one scheduler turn over baseline: baseline {baseline_p99} turns vs loaded {loaded_p99} turns"
    );
    assert!(users_served > 0, "User traffic must still make progress");
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
