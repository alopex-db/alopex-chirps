#![cfg(feature = "multi-raft")]

use alopex_chirps::multi_raft::{MultiRaftError, MultiRaftManager, WalRaftStorageFactory};
use alopex_chirps::raft::RaftFramePayload;
use alopex_chirps::{ChirpsRaftTransport, RaftConfig, RaftMessage};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_mock::{MockBackend, MockNetwork};
use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
use alopex_chirps_raft_storage::types::{GroupId, LogId, Vote, VoteRequest, VoteResponse};
use alopex_chirps_raft_storage::wal_storage::WalStorageConfig;
use alopex_chirps_transport_quic::{HandshakeError, HandshakeMessage, PROTOCOL_VERSION, negotiate};
use alopex_chirps_wire::frame::{Frame, RaftFrame};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;

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

#[tokio::test]
async fn multi_node_raft_frame_is_routed_without_manual_node_registration() {
    let root = tempfile::tempdir().unwrap();
    let network = MockNetwork::new();
    let local_wire_id = wire_node_id(1);
    let peer_wire_id = NodeId::new();
    let backend = network
        .add_node(local_wire_id, MockBackend::ephemeral_addr())
        .await;
    let peer_backend = network
        .add_node(peer_wire_id, MockBackend::ephemeral_addr())
        .await;
    let mut local_rx = backend.subscribe().await.unwrap();
    let mut peer_rx = peer_backend.subscribe().await.unwrap();
    let backend: Arc<dyn MessageBackend> = Arc::new(backend);
    let transport = Arc::new(ChirpsRaftTransport::new(backend, GroupId(41), 1));
    let factory = Arc::new(WalRaftStorageFactory::new(
        WalStorageConfig {
            wal_dir: root.path().join("wal"),
            snapshot_dir: root.path().join("snapshot"),
            ..WalStorageConfig::default()
        },
        1,
    ));
    let manager = MultiRaftManager::new(transport, factory, RaftConfig::default());
    manager
        .create_group(GroupId(41), BTreeSet::from([1]), EchoStateMachine)
        .await
        .unwrap();

    let frame = ChirpsRaftTransport::encode_group_frame(vote_payload(GroupId(41), 7)).unwrap();
    peer_backend.send(local_wire_id, frame).await.unwrap();
    let (source, received_frame) =
        tokio::time::timeout(std::time::Duration::from_secs(1), local_rx.recv())
            .await
            .unwrap()
            .unwrap();
    let response = manager
        .dispatch_frame(source, local_wire_id, received_frame)
        .await
        .expect("peer Raft frame must be routed to the local group")
        .expect("vote request must produce a response");
    assert_eq!(response.destination, 2);
    manager
        .send_routed_response(response)
        .await
        .expect("response must use the learned peer NodeId route");
    let (_, response_frame) =
        tokio::time::timeout(std::time::Duration::from_secs(1), peer_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert!(ChirpsRaftTransport::decode_frame(response_frame).is_some());
    manager.shutdown_all().await.unwrap();
}

#[derive(Default)]
struct EchoStateMachine;

#[async_trait]
impl StateMachine for EchoStateMachine {
    type Command = Vec<u8>;
    type Response = Vec<u8>;

    async fn apply(
        &mut self,
        _log_id: LogId<u64>,
        command: Self::Command,
    ) -> StateMachineResult<Self::Response> {
        Ok(command)
    }

    async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn restore(&mut self, _snapshot: Box<dyn AsyncSnapshotData>) -> StateMachineResult<()> {
        Ok(())
    }
}

fn wire_node_id(node_id: u64) -> NodeId {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&node_id.to_be_bytes());
    NodeId::from(bytes)
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

#[tokio::test]
async fn unknown_correlated_responses_do_not_mutate_group_state() {
    let root = tempfile::tempdir().unwrap();
    let network = MockNetwork::new();
    let backend = network
        .add_node(wire_node_id(1), MockBackend::ephemeral_addr())
        .await;
    let backend: Arc<dyn MessageBackend> = Arc::new(backend);
    let transport = Arc::new(ChirpsRaftTransport::new(backend, GroupId(0), 1));
    let factory = Arc::new(WalRaftStorageFactory::<EchoStateMachine>::new(
        WalStorageConfig {
            wal_dir: root.path().join("wal"),
            snapshot_dir: root.path().join("snapshot"),
            ..WalStorageConfig::default()
        },
        1,
    ));
    let manager = MultiRaftManager::new(transport, factory, RaftConfig::default());
    manager
        .create_group(GroupId(4), BTreeSet::from([1]), EchoStateMachine)
        .await
        .unwrap();
    let before = manager.get_group(GroupId(4)).unwrap().metrics();

    let response_frame = |correlation_id| {
        ChirpsRaftTransport::encode_group_frame(RaftFramePayload {
            correlation_id,
            message: RaftMessage::VoteResponse {
                group_id: GroupId(4),
                response: VoteResponse {
                    vote: Vote::new(1, 2),
                    vote_granted: true,
                    last_log_id: None,
                },
            },
        })
        .unwrap()
    };

    // Neither a wrong peer nor an unknown correlation may be consumed or
    // routed to RaftNode as a fresh request.
    assert!(
        manager
            .dispatch_frame(wire_node_id(3), wire_node_id(1), response_frame(7))
            .await
            .is_err()
    );
    assert!(
        manager
            .dispatch_frame(wire_node_id(2), wire_node_id(1), response_frame(8))
            .await
            .is_err()
    );
    assert!(matches!(
        manager
            .route_frame(wire_node_id(2), wire_node_id(1), response_frame(8))
            .await,
        Err(MultiRaftError::InvalidTransportRoute { .. })
    ));
    let after = manager.get_group(GroupId(4)).unwrap().metrics();
    assert_eq!(after.last_applied, before.last_applied);
    assert_eq!(after.current_term, before.current_term);
}
