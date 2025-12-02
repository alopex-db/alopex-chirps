use chirps_transport_quic::{QosConfig, QosController, StreamKind};
use chirps_wire::frame::{Frame, UserMessage};
use chirps_wire::node_id::NodeId;

fn user_frame() -> Frame {
    Frame::User(UserMessage {
        payload: b"u".to_vec(),
    })
}

fn raft_frame(seq: u64, from: NodeId) -> Frame {
    Frame::Ping { seq, from }
}

fn qos() -> QosController {
    QosController::new(QosConfig::default())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_messages_dequeue_before_user_under_load() {
    let mut qos = qos();
    let from = NodeId::new();

    // Saturate with user messages first.
    for _ in 0..30 {
        qos.enqueue(StreamKind::User, user_frame()).await.unwrap();
    }
    // Enqueue Raft messages after users; scheduler should still serve Raft first.
    for i in 0..5 {
        qos.enqueue(StreamKind::Raft, raft_frame(i, from))
            .await
            .unwrap();
    }

    let mut first_five = Vec::new();
    for _ in 0..5 {
        let (kind, _) = qos.dequeue().expect("should dequeue");
        first_five.push(kind);
    }

    assert!(
        first_five.iter().all(|k| *k == StreamKind::Raft),
        "Raft frames should be prioritized even when enqueued after user backlog: {:?}",
        first_five
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_messages_still_progress_under_raft_load() {
    let mut qos = qos();
    let from = NodeId::new();

    for _ in 0..15 {
        qos.enqueue(StreamKind::Raft, raft_frame(1, from))
            .await
            .unwrap();
    }
    qos.enqueue(StreamKind::User, user_frame()).await.unwrap();

    let mut saw_user = false;
    for _ in 0..20 {
        if let Some((kind, _)) = qos.dequeue() {
            if kind == StreamKind::User {
                saw_user = true;
                break;
            }
        }
    }

    assert!(
        saw_user,
        "User traffic should make progress even under sustained Raft load"
    );
}
