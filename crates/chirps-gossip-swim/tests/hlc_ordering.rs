#![cfg(feature = "hlc")]

use alopex_chirps_gossip_swim::engine::{
    GossipConfig, GossipEngine, GossipError, Transport, TransportError,
};
use alopex_chirps_gossip_swim::hlc::{Clock, HlcError, LocalHlc};
use alopex_chirps_gossip_swim::types::{MembershipView, Peer, PeerState, Status};
use alopex_chirps_wire::frame::{
    Frame, HlcEventId, HlcGossipMessage, MemberStatus, MembershipUpdate, StampedMembershipUpdate,
};
use alopex_chirps_wire::hlc::HybridTimestamp;
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

struct ManualClock(AtomicU64);

impl ManualClock {
    fn new(millis: u64) -> Self {
        Self(AtomicU64::new(millis))
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct CapturingTransport {
    frames: Mutex<Vec<(NodeId, Frame)>>,
}

#[async_trait]
impl Transport for CapturingTransport {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError> {
        self.frames.lock().unwrap().push((target, frame));
        Ok(())
    }

    async fn broadcast(&self, _frame: Frame) -> Result<usize, TransportError> {
        Ok(0)
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(rx)
    }

    async fn close(&self) -> Result<(), TransportError> {
        Ok(())
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        Vec::new()
    }
}

fn config() -> GossipConfig {
    GossipConfig {
        ping_timeout: Duration::from_millis(100),
        indirect_ping_timeout: Duration::from_millis(200),
        suspect_to_dead_timeout: Duration::from_millis(400),
        gossip_interval: Duration::from_millis(1),
        fanout: Some(1),
    }
}

fn update(
    source: NodeId,
    sequence: u64,
    timestamp: HybridTimestamp,
    peer: NodeId,
    status: MemberStatus,
) -> StampedMembershipUpdate {
    StampedMembershipUpdate {
        event_id: HlcEventId { source, sequence },
        timestamp,
        update: MembershipUpdate {
            node_id: peer,
            incarnation: 1,
            addr: "127.0.0.1:9001".parse().unwrap(),
            status,
        },
    }
}

#[tokio::test]
async fn swim_and_gossip_messages_include_sender_hlc() {
    let node = NodeId::new();
    let peer = NodeId::new();
    let mut membership = MembershipView::new();
    membership.peers.insert(
        peer,
        Peer {
            node_id: peer,
            addr: "127.0.0.1:9001".parse().unwrap(),
            state: PeerState::new(1, Status::Alive),
        },
    );
    let transport = Arc::new(CapturingTransport::default());
    let clock = Arc::new(ManualClock::new(100));
    let mut engine = GossipEngine::new_with_hlc(
        node,
        config(),
        transport.clone(),
        membership,
        LocalHlc::with_clock(Duration::from_millis(10), clock),
    );

    engine.tick().await.unwrap();

    let frames = transport.frames.lock().unwrap();
    let message = frames
        .iter()
        .find_map(|(_, frame)| match frame {
            Frame::HlcGossip(message) => Some(message),
            _ => None,
        })
        .expect("HLC gossip frame");
    assert_eq!(message.timestamp.physical, 100);
    assert!(!message.updates.is_empty());
    assert!(
        message
            .updates
            .iter()
            .all(|event| event.timestamp <= message.timestamp)
    );
}

#[tokio::test]
async fn reordered_and_duplicate_delivery_preserves_receiver_order() {
    let sender = NodeId::new();
    let receiver = NodeId::new();
    let peer = NodeId::new();
    let transport = Arc::new(CapturingTransport::default());
    let clock = Arc::new(ManualClock::new(100));
    let mut engine = GossipEngine::new_with_hlc(
        receiver,
        config(),
        transport,
        MembershipView::new(),
        LocalHlc::with_clock(Duration::from_millis(10), clock),
    );
    let later = HlcGossipMessage {
        event_id: HlcEventId {
            source: sender,
            sequence: 20,
        },
        timestamp: HybridTimestamp::new(100, 4),
        updates: vec![update(
            sender,
            2,
            HybridTimestamp::new(100, 3),
            peer,
            MemberStatus::Alive,
        )],
    };
    let earlier = HlcGossipMessage {
        event_id: HlcEventId {
            source: sender,
            sequence: 10,
        },
        timestamp: HybridTimestamp::new(100, 2),
        updates: vec![update(
            sender,
            1,
            HybridTimestamp::new(100, 1),
            peer,
            MemberStatus::Dead,
        )],
    };

    assert_eq!(engine.apply_hlc_gossip(&later).unwrap(), 1);
    let after_later = engine.hlc_current();
    assert_eq!(engine.apply_hlc_gossip(&earlier).unwrap(), 0);
    assert_eq!(engine.apply_hlc_gossip(&later).unwrap(), 0);

    let member = engine.membership().peers.remove(&peer).unwrap();
    assert_eq!(member.state.status, Status::Alive);
    assert!(engine.hlc_current() > after_later);
}

#[tokio::test]
async fn skewed_gossip_is_rejected_without_clock_or_membership_mutation() {
    let sender = NodeId::new();
    let receiver = NodeId::new();
    let peer = NodeId::new();
    let clock = Arc::new(ManualClock::new(100));
    let mut engine = GossipEngine::new_with_hlc(
        receiver,
        config(),
        Arc::new(CapturingTransport::default()),
        MembershipView::new(),
        LocalHlc::with_clock(Duration::from_millis(10), clock),
    );
    let before = engine.hlc_current();
    let skewed = HlcGossipMessage {
        event_id: HlcEventId {
            source: sender,
            sequence: 1,
        },
        timestamp: HybridTimestamp::new(111, 0),
        updates: vec![update(
            sender,
            1,
            HybridTimestamp::new(111, 0),
            peer,
            MemberStatus::Alive,
        )],
    };

    let error = engine.apply_hlc_gossip(&skewed).unwrap_err();

    assert!(matches!(
        error,
        GossipError::Hlc(HlcError::ClockSkewTooLarge { .. })
    ));
    assert_eq!(engine.hlc_current(), before);
    assert!(engine.membership().peers.is_empty());
}
