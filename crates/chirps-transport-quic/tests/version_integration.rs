use alopex_chirps_transport_quic::{
    HandshakeError, HandshakeMessage, MIN_COMPATIBLE_VERSION, PROTOCOL_VERSION, negotiate,
};
use alopex_chirps_wire::node_id::NodeId;
use tracing_test::traced_test;

#[traced_test]
#[test]
fn v0_4_to_v0_4_connects_with_capabilities() {
    let local = HandshakeMessage::new(NodeId::new());
    let remote = HandshakeMessage::new(NodeId::new());

    let negotiated = negotiate(&local, &remote).expect("v0.4 should be compatible");
    assert!(negotiated.priority_streams);
    assert!(negotiated.retransmission);
    assert!(negotiated.qos);
    assert!(
        !logs_contain("version_mismatch"),
        "should not emit version_mismatch for compatible peers"
    );
}

#[traced_test]
#[test]
fn v0_3_rejected_with_version_mismatch_log() {
    let local = HandshakeMessage::new(NodeId::new());
    let mut remote = HandshakeMessage::new(NodeId::new());
    remote.version = MIN_COMPATIBLE_VERSION - 1; // v0.3

    let res = negotiate(&local, &remote);
    match res {
        Err(HandshakeError::VersionMismatch { local, remote }) => {
            assert_eq!(local, PROTOCOL_VERSION);
            assert_eq!(remote, MIN_COMPATIBLE_VERSION - 1);
        }
        other => panic!("expected VersionMismatch, got {other:?}"),
    }
}
