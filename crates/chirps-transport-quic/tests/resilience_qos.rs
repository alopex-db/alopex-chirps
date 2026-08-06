use alopex_chirps_transport_quic::{QosConfig, QosController, QosError, StreamKind};
use alopex_chirps_wire::frame::{Frame, UserMessage};
use std::sync::atomic::Ordering;

fn frame(byte: u8) -> Frame {
    Frame::User(UserMessage {
        payload: vec![byte; 32],
    })
}

#[tokio::test]
async fn mixed_traffic_serves_control_without_starving_user() {
    let mut qos = QosController::new(QosConfig::default());
    for value in 0..7 {
        qos.enqueue(StreamKind::Raft, frame(value)).await.unwrap();
    }
    for value in 10..13 {
        qos.enqueue(StreamKind::FileTransfer, frame(value))
            .await
            .unwrap();
    }
    qos.enqueue(StreamKind::User, frame(20)).await.unwrap();

    let mut order = Vec::new();
    while let Some((kind, _)) = qos.dequeue() {
        order.push(kind);
    }
    assert_eq!(order.first(), Some(&StreamKind::Raft));
    assert!(
        order.iter().take(8).any(|kind| *kind == StreamKind::User),
        "weighted scheduling must serve user traffic within one complete round"
    );
    assert_eq!(order.len(), 11);
}

#[tokio::test]
async fn queue_capacity_is_fail_closed_and_observable() {
    let mut config = QosConfig::default();
    config.queue_limits.user_max_items = 4;
    config.queue_limits.user_max_bytes = 1_000_000;
    let mut qos = QosController::new(config);

    let mut accepted = 0usize;
    let mut rejected = 0usize;
    for value in 0..20 {
        match qos.enqueue(StreamKind::User, frame(value)).await {
            Ok(()) => accepted += 1,
            Err(QosError::QueueFull { kind, limit, .. }) => {
                assert_eq!(kind, StreamKind::User);
                assert_eq!(limit, 4);
                rejected += 1;
            }
            Err(error) => panic!("unexpected QoS error: {error:?}"),
        }
    }

    assert!(accepted <= 4);
    assert!(rejected > 0);
    assert!(qos.utilization(StreamKind::User) <= 1.0);
    assert!(
        qos.metrics()
            .backpressure_triggered_total
            .load(Ordering::Relaxed)
            + qos.metrics().queue_overflow_total.load(Ordering::Relaxed)
            > 0
    );
    while qos.dequeue().is_some() {}
    assert_eq!(qos.utilization(StreamKind::User), 0.0);
}
