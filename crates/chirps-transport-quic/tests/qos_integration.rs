use alopex_chirps_transport_quic::{QosConfig, QosController, StreamKind};
use alopex_chirps_wire::frame::{Frame, UserMessage};
use alopex_chirps_wire::node_id::NodeId;

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
