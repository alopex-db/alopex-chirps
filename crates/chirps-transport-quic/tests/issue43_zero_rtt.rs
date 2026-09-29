use alopex_chirps_transport_quic::{EarlyDataPolicy, StreamKind};

#[test]
fn replay_sensitive_operations_are_never_sent_as_zero_rtt_early_data() {
    for kind in [
        StreamKind::Control,
        StreamKind::Gossip,
        StreamKind::User,
        StreamKind::Raft,
        StreamKind::RaftSnapshot,
        StreamKind::FileTransfer,
    ] {
        assert_eq!(EarlyDataPolicy::for_stream(kind), EarlyDataPolicy::Disabled);
    }
}
