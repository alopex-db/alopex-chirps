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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_queueing_delay_is_bounded_under_user_load() {
    let mut qos = QosController::new(QosConfig::default());
    let from = NodeId::new();

    // Baseline: every Raft frame is ready on the next scheduler turn.
    for i in 0..500 {
        qos.enqueue(StreamKind::Raft, raft_frame(i, from))
            .await
            .unwrap();
        let (kind, _) = qos.dequeue().unwrap();
        assert_eq!(kind, StreamKind::Raft);
    }

    // Loaded: DWRR may serve one User frame to prevent starvation, but Raft
    // must be selected by the following scheduler turn. Queue turns are
    // deterministic; wall-clock microbenchmarks belong in the performance
    // evidence lane, not in a scheduler correctness test.
    for _ in 0..2_000 {
        qos.enqueue(StreamKind::User, user_frame()).await.unwrap();
    }
    let mut observed_user_turn = false;
    for i in 0..500 {
        qos.enqueue(StreamKind::Raft, raft_frame(i, from))
            .await
            .unwrap();
        let mut user_turns = 0;
        loop {
            let (kind, _) = qos.dequeue().unwrap();
            if kind == StreamKind::Raft {
                break;
            }
            assert_eq!(kind, StreamKind::User);
            user_turns += 1;
            observed_user_turn = true;
            assert!(
                user_turns <= 1,
                "Raft waited behind more than one lower-priority scheduler turn"
            );
        }
    }
    assert!(
        observed_user_turn,
        "the loaded lane must exercise DWRR fairness"
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
