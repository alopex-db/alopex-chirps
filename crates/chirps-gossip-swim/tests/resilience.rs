use alopex_chirps_gossip_swim::engine::{GossipConfig, GossipEngine, Transport, TransportError};
use alopex_chirps_gossip_swim::types::{MembershipView, Peer, PeerState, Status};
use alopex_chirps_wire::frame::{Frame, MemberStatus, MembershipUpdate};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Default)]
struct RecordingTransport {
    peers: Vec<(NodeId, SocketAddr)>,
    sent: Mutex<Vec<(NodeId, Frame)>>,
}

#[async_trait]
impl Transport for RecordingTransport {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError> {
        self.sent.lock().unwrap().push((target, frame));
        Ok(())
    }

    async fn broadcast(&self, _frame: Frame) -> Result<usize, TransportError> {
        Ok(self.peers.len())
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(rx)
    }

    async fn close(&self) -> Result<(), TransportError> {
        Ok(())
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        self.peers.clone()
    }
}

fn id(byte: u8) -> NodeId {
    NodeId::from([byte; 16])
}

fn config() -> GossipConfig {
    GossipConfig {
        ping_timeout: Duration::from_millis(10),
        indirect_ping_timeout: Duration::from_millis(20),
        suspect_to_dead_timeout: Duration::from_millis(40),
        gossip_interval: Duration::from_millis(5),
        fanout: Some(2),
    }
}

fn membership(peers: &[(NodeId, SocketAddr)]) -> MembershipView {
    let mut view = MembershipView::new();
    for (node_id, addr) in peers {
        view.peers.insert(
            *node_id,
            Peer {
                node_id: *node_id,
                addr: *addr,
                state: PeerState::new(5, Status::Alive),
            },
        );
    }
    view
}

#[tokio::test(start_paused = true)]
async fn simulated_clock_replays_direct_indirect_suspect_and_recovery() {
    let target = (id(2), "127.0.0.1:9102".parse().unwrap());
    let helper = (id(3), "127.0.0.1:9103".parse().unwrap());
    let peers = vec![target, helper];
    let transport = Arc::new(RecordingTransport {
        peers: peers.clone(),
        sent: Mutex::new(Vec::new()),
    });
    let mut engine = GossipEngine::new(id(1), config(), transport.clone(), membership(&peers));

    engine.tick().await.unwrap();
    tokio::time::advance(Duration::from_millis(11)).await;
    engine.tick().await.unwrap();
    tokio::time::advance(Duration::from_millis(21)).await;
    let changes = engine.tick().await.unwrap();
    assert!(changes.iter().any(|change| change.to == Status::Suspect));
    assert!(
        transport
            .sent
            .lock()
            .unwrap()
            .iter()
            .any(|(_, frame)| matches!(frame, Frame::PingReq { .. }))
    );

    engine.handle_ack(target.0, 1, target.1);
    assert_eq!(
        engine.membership().peers[&target.0].state.status,
        Status::Alive
    );
}

#[tokio::test]
async fn stale_incarnations_and_reordered_gossip_cannot_override_rejoin() {
    let peer = id(2);
    let addr: SocketAddr = "127.0.0.1:9202".parse().unwrap();
    let peers = vec![(peer, addr)];
    let transport = Arc::new(RecordingTransport {
        peers: peers.clone(),
        sent: Mutex::new(Vec::new()),
    });
    let mut engine = GossipEngine::new(id(1), config(), transport, membership(&peers));

    engine.apply_membership_update(&[MembershipUpdate {
        node_id: peer,
        incarnation: 5,
        addr,
        status: MemberStatus::Suspect,
    }]);
    assert_eq!(
        engine.membership().peers[&peer].state.status,
        Status::Suspect
    );

    engine.apply_membership_update(&[MembershipUpdate {
        node_id: peer,
        incarnation: 6,
        addr,
        status: MemberStatus::Alive,
    }]);
    engine.apply_membership_update(&[
        MembershipUpdate {
            node_id: peer,
            incarnation: 5,
            addr,
            status: MemberStatus::Dead,
        },
        MembershipUpdate {
            node_id: peer,
            incarnation: 6,
            addr,
            status: MemberStatus::Suspect,
        },
    ]);

    let current = engine.membership();
    assert_eq!(current.peers[&peer].state.incarnation, 6);
    assert_eq!(current.peers[&peer].state.status, Status::Suspect);
    engine.apply_membership_update(&[MembershipUpdate {
        node_id: peer,
        incarnation: 7,
        addr,
        status: MemberStatus::Alive,
    }]);
    let rejoined = engine.membership();
    assert_eq!(rejoined.peers[&peer].state.incarnation, 7);
    assert_eq!(rejoined.peers[&peer].state.status, Status::Alive);
}
