use chirps_transport_quic::{RetransmissionBuffer, RetransmitConfig};
use chirps_wire::frame::Frame;
use chirps_wire::node_id::NodeId;
use std::collections::HashSet;
use std::time::Instant;

fn ping(seq: u64, from: NodeId) -> Frame {
    Frame::Ping { seq, from }
}

#[test]
fn retransmit_replays_unacked_in_order_without_duplicates() {
    let config = RetransmitConfig {
        max_buffer_bytes: 8 * 1024 * 1024,
        max_messages_per_peer: 2_000,
        message_ttl: std::time::Duration::from_secs(60),
    };
    let mut buffer = RetransmissionBuffer::new(config);
    let mut dedup = HashSet::new();
    let peer = NodeId::new();

    // Buffer 1,000 messages to simulate in-flight frames before disconnect.
    for i in 0..1_000 {
        let _ = buffer.buffer(peer, ping(i, peer)).unwrap();
    }

    // Simulate reconnect: drain unacked messages for retransmit.
    let start = Instant::now();
    let drained = buffer.drain_for_retransmit(peer);
    let elapsed = start.elapsed();

    // Should finish well under 100ms for 1,000 messages.
    assert!(
        elapsed.as_millis() <= 100,
        "drain took too long: {}ms",
        elapsed.as_millis()
    );

    // Deliver drained messages through deduplication to ensure no duplicates.
    for msg in &drained {
        assert!(dedup.insert(msg.seq), "seq {} should be new", msg.seq);
    }

    // Replaying the same drained set should be treated as duplicates.
    for msg in &drained {
        assert!(
            !dedup.insert(msg.seq),
            "seq {} should be seen as duplicate on second replay",
            msg.seq
        );
    }

    // Order must be preserved.
    let seqs: Vec<u64> = drained.iter().map(|m| m.seq).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted, "retransmit order must follow seq ascending");
}
