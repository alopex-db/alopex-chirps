#![cfg(feature = "multi-raft")]

use alopex_chirps::raft::RaftFramePayload;
use alopex_chirps::{ChirpsRaftTransport, RaftMessage};
use alopex_chirps_raft_storage::types::{GroupId, Vote, VoteRequest};
use alopex_chirps_transport_quic::{HandshakeError, HandshakeMessage, PROTOCOL_VERSION, negotiate};
use alopex_chirps_wire::frame::{Frame, RaftFrame};
use alopex_chirps_wire::node_id::NodeId;

fn vote_payload(group_id: GroupId, correlation_id: u64) -> RaftFramePayload {
    RaftFramePayload {
        correlation_id,
        message: RaftMessage::Vote {
            group_id,
            request: VoteRequest {
                vote: Vote::new(1, 2),
                last_log_id: None,
            },
        },
    }
}

#[test]
fn outer_and_payload_group_must_match_before_routing() {
    let payload = vote_payload(GroupId(7), 41);
    let frame = Frame::Raft(RaftFrame {
        group_id: 8,
        payload: bincode::serialize(&payload).unwrap(),
    });

    assert!(ChirpsRaftTransport::decode_frame(frame).is_none());
}

#[test]
fn only_exact_v0_6_peers_negotiate_multi_raft() {
    let local = HandshakeMessage::new(NodeId::new());
    for incompatible in [PROTOCOL_VERSION - 1, PROTOCOL_VERSION + 1] {
        let mut remote = HandshakeMessage::new(NodeId::new());
        remote.version = incompatible;
        assert!(matches!(
            negotiate(&local, &remote),
            Err(HandshakeError::VersionMismatch { .. })
        ));
    }

    let negotiated = negotiate(&local, &HandshakeMessage::new(NodeId::new())).unwrap();
    assert!(negotiated.multi_raft);
}
