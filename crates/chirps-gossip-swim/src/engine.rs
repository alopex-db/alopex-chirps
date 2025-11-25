use crate::types::{MembershipView, Peer, PeerState, Status};
use crate::util::{calculate_fanout, check_timeouts, StatusChange};
use async_trait::async_trait;
use chirps_wire::frame::{Frame, MembershipUpdate, MemberStatus};
use chirps_wire::node_id::NodeId;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{interval, Interval};
use tracing::warn;

/// Transport abstraction used by the gossip engine.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError>;
    async fn broadcast(&self, frame: Frame) -> Result<usize, TransportError>;
    async fn subscribe(&self) -> Result<tokio::sync::mpsc::Receiver<(NodeId, Frame)>, TransportError>;
    async fn close(&self) -> Result<(), TransportError>;
    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)>;
}

/// Minimal configuration needed by gossip.
#[derive(Clone)]
pub struct GossipConfig {
    pub ping_timeout: Duration,
    pub indirect_ping_timeout: Duration,
    pub suspect_to_dead_timeout: Duration,
    pub gossip_interval: Duration,
    pub fanout: Option<usize>,
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("connection: {0}")]
    Connection(String),
    #[error("send: {0}")]
    Send(String),
    #[error("subscribe: {0}")]
    Subscribe(String),
    #[error("io: {0}")]
    Io(String),
}

#[derive(Debug, thiserror::Error)]
pub enum GossipError {
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
}

/// Tracks outstanding ping requests.
#[derive(Debug)]
struct PendingPing {
    target: NodeId,
    sent_at: Instant,
}

/// SWIM-style gossip engine.
pub struct GossipEngine {
    node_id: NodeId,
    config: GossipConfig,
    backend: Arc<dyn Transport>,
    membership: MembershipView,
    pending: HashMap<u64, PendingPing>,
    seq: u64,
    ticker: Interval,
}

impl GossipEngine {
    pub fn new(
        node_id: NodeId,
        config: GossipConfig,
        backend: Arc<dyn Transport>,
        membership: MembershipView,
    ) -> Self {
        let mut ticker = interval(config.gossip_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self {
            node_id,
            config,
            backend,
            membership,
            pending: HashMap::new(),
            seq: 0,
            ticker,
        }
    }

    /// Periodic tick: apply timeouts then send pings/gossip.
    pub async fn tick(&mut self) -> Result<Vec<StatusChange>, GossipError> {
        self.ticker.tick().await;
        let now = Instant::now();
        let mut changes = self.detect_ping_timeouts(now);
        changes.extend(self.apply_timeouts(now));
        self.send_pings().await?;
        Ok(changes)
    }

    /// Handle an incoming Ping: reply Ack and update last_seen.
    pub async fn handle_ping(&mut self, from: NodeId, seq: u64, addr: SocketAddr) {
        self.touch_peer(from, addr, Status::Alive);
        if let Err(err) = self
            .backend
            .send(from, Frame::Ack { seq, from: self.node_id })
            .await
        {
            warn!("failed to send ack to {from:?}: {err}");
        }
    }

    /// Handle an incoming Ack: mark peer alive and clear pending if present.
    pub fn handle_ack(&mut self, from: NodeId, seq: u64, addr: SocketAddr) {
        if let Some(peer) = self.membership.peers.get_mut(&from) {
            if matches!(peer.state.status, Status::Dead) {
                return; // ignore late ack from dead peers
            }
            peer.state.status = Status::Alive;
            peer.state.last_seen = Instant::now();
            peer.state.suspect_since = None;
        } else {
            self.touch_peer(from, addr, Status::Alive);
        }
        self.pending.remove(&seq);
    }

    /// Apply membership updates learned from gossip.
    pub fn apply_membership_update(&mut self, updates: &[MembershipUpdate]) {
        for upd in updates {
            let status: Status = upd.status.clone().into();
            let entry = self.membership.peers.entry(upd.node_id).or_insert_with(|| Peer {
                node_id: upd.node_id,
                addr: upd.addr,
                state: PeerState::new(upd.incarnation, status.clone()),
            });
            if upd.incarnation >= entry.state.incarnation {
                entry.state.incarnation = upd.incarnation;
                entry.state.status = status;
                entry.addr = upd.addr;
                entry.state.last_seen = Instant::now();
            }
        }
    }

    fn detect_ping_timeouts(&mut self, now: Instant) -> Vec<StatusChange> {
        let mut changes = Vec::new();
        let timeout = self.config.ping_timeout;
        let mut expired = Vec::new();
        for (seq, pending) in self.pending.iter() {
            if now.saturating_duration_since(pending.sent_at) >= timeout {
                expired.push(*seq);
                if let Some(peer) = self.membership.peers.get_mut(&pending.target) {
                    if peer.state.status == Status::Alive {
                        peer.state.status = Status::Suspect;
                        peer.state.suspect_since = Some(pending.sent_at);
                        changes.push(StatusChange::new(
                            pending.target,
                            Status::Alive,
                            Status::Suspect,
                            peer.state.incarnation,
                        ));
                    }
                }
            }
        }
        for seq in expired {
            self.pending.remove(&seq);
        }
        changes
    }

    fn apply_timeouts(&mut self, now: Instant) -> Vec<StatusChange> {
        check_timeouts(
            &mut self.membership.peers,
            now,
            self.config.indirect_ping_timeout,
            self.config.suspect_to_dead_timeout,
        )
    }

    async fn send_pings(&mut self) -> Result<(), GossipError> {
        let targets = self.select_targets();
        for target in targets {
            self.seq = self.seq.wrapping_add(1);
            let seq = self.seq;
            let frame = Frame::Ping {
                seq,
                from: self.node_id,
            };
            if let Err(err) = self.backend.send(target, frame).await {
                warn!("failed to send ping to {target:?}: {err}");
                continue;
            }
            self.pending.insert(
                seq,
                PendingPing {
                    target,
                    sent_at: Instant::now(),
                },
            );
            if let Some(peer) = self.membership.peers.get_mut(&target) {
                peer.state.last_seen = Instant::now();
            }
            // Gossip membership to the same target
            let updates: Vec<MembershipUpdate> = self
                .membership
                .peers
                .values()
                .map(|peer| MembershipUpdate {
                    node_id: peer.node_id,
                    incarnation: peer.state.incarnation,
                    addr: peer.addr,
                    status: member_status(&peer.state.status),
                })
                .collect();
            if !updates.is_empty() {
                let _ = self
                    .backend
                    .send(
                        target,
                        Frame::Gossip(chirps_wire::frame::GossipMessage { updates }),
                    )
                    .await;
            }
        }
        Ok(())
    }

    fn select_targets(&self) -> Vec<NodeId> {
        let total_nodes = self.membership.peers.len() + 1; // include self
        let fanout = calculate_fanout(total_nodes, self.config.fanout);
        let mut rng = rand::thread_rng();
        self.membership
            .peers
            .keys()
            .cloned()
            .choose_multiple(&mut rng, fanout)
    }

    fn touch_peer(&mut self, node_id: NodeId, addr: SocketAddr, status: Status) {
        let entry = self
            .membership
            .peers
            .entry(node_id)
            .or_insert_with(|| Peer {
                node_id,
                addr,
                state: PeerState::new(0, status.clone()),
            });
        entry.addr = addr;
        entry.state.status = status;
        entry.state.last_seen = Instant::now();
    }

    /// Returns current membership view (read-only clone).
    pub fn membership(&self) -> MembershipView {
        self.membership.clone()
    }
}

fn member_status(status: &Status) -> MemberStatus {
    match status {
        Status::Alive => MemberStatus::Alive,
        Status::Suspect => MemberStatus::Suspect,
        Status::Dead => MemberStatus::Dead,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    struct MockBackend {
        sent: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Transport for MockBackend {
        async fn send(&self, _target: NodeId, _frame: Frame) -> Result<(), TransportError> {
            self.sent.fetch_add(1, Ordering::SeqCst);
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
            gossip_interval: Duration::from_millis(10),
            fanout: Some(1),
        }
    }

    #[tokio::test]
    async fn tick_sends_ping() {
        let node = NodeId::new();
        let peer_id = NodeId::new();
        let membership = {
            let mut m = MembershipView::new();
            m.peers.insert(
                peer_id,
                Peer {
                    node_id: peer_id,
                    addr: "127.0.0.1:9000".parse().unwrap(),
                    state: PeerState::new(0, Status::Alive),
                },
            );
            m
        };
        let backend = Arc::new(MockBackend {
            sent: Arc::new(AtomicUsize::new(0)),
        });
        let mut engine = GossipEngine::new(node, config(), backend.clone(), membership);
        let _ = engine.tick().await.unwrap();
        assert!(backend.sent.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn timeouts_mark_suspect_and_dead() {
        let node = NodeId::new();
        let peer_id = NodeId::new();
        let mut membership = MembershipView::new();
        membership.peers.insert(
            peer_id,
            Peer {
                node_id: peer_id,
                addr: "127.0.0.1:9000".parse().unwrap(),
                state: PeerState::new(0, Status::Alive),
            },
        );
        let backend = Arc::new(MockBackend {
            sent: Arc::new(AtomicUsize::new(0)),
        });
        let mut engine = GossipEngine::new(node, config(), backend, membership);

        // simulate one tick that records a pending ping
        engine.send_pings().await.unwrap();
        // force suspect to dead by adjusting state timestamps
        engine.pending.clear();
        for peer in engine.membership.peers.values_mut() {
            peer.state.status = Status::Suspect;
            peer.state.suspect_since = Some(Instant::now() - Duration::from_millis(500));
            peer.state.last_seen = Instant::now() - Duration::from_millis(500);
        }
        let changes = engine.tick().await.unwrap();
        assert!(changes.iter().any(|c| c.to == Status::Dead));
    }
}
