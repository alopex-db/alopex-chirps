use alopex_chirps::MessageProfile;
use alopex_chirps::buffer::{BackpressureLevel, BufferError, MessageBuffer};

#[test]
fn profile_queues_apply_staged_backpressure_before_global_rejection() {
    let mut buffer = MessageBuffer::new(100, 0.60, 0.80);

    buffer
        .push(MessageProfile::Ephemeral, vec![0; 60])
        .expect("warning level must still accept a message");
    assert_eq!(buffer.backpressure_level(), BackpressureLevel::Warning);

    let error = buffer
        .push(MessageProfile::Ephemeral, vec![0; 20])
        .expect_err("limited pressure must reject ephemeral traffic first");
    assert!(matches!(
        error,
        BufferError::Backpressure {
            level: BackpressureLevel::Limited,
            ..
        }
    ));

    buffer
        .push(MessageProfile::Control, vec![0; 20])
        .expect("control traffic must remain available under limited pressure");
    assert_eq!(buffer.bytes_for(MessageProfile::Control), 20);

    buffer
        .push(MessageProfile::Control, vec![0; 20])
        .expect("control traffic may consume the remaining budget");

    let error = buffer
        .push(MessageProfile::Durable, vec![0; 1])
        .expect_err("a full buffer must reject new traffic");
    assert!(matches!(
        error,
        BufferError::Backpressure {
            level: BackpressureLevel::Reject,
            ..
        }
    ));
}
