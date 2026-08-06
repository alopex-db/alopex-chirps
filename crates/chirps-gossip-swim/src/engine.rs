#[cfg(feature = "hlc")]
use crate::hlc::{HlcError, LocalHlc};
use crate::types::{MembershipView, Peer, PeerState, Status};
use crate::util::{StatusChange, calculate_fanout, check_timeouts};
use alopex_chirps_wire::frame::{Frame, MemberStatus, MembershipUpdate};
#[cfg(feature = "hlc")]
use alopex_chirps_wire::frame::{HlcEventId, HlcGossipMessage, StampedMembershipUpdate};
#[cfg(feature = "hlc")]
use alopex_chirps_wire::hlc::HybridTimestamp;
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
#[cfg(feature = "hlc")]
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::time::{Interval, interval};
use tracing::{debug, info, warn};

/// Transport abstraction used by the gossip engine.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError>;
    async fn broadcast(&self, frame: Frame) -> Result<usize, TransportError>;
    async fn subscribe(
        &self,
    ) -> Result<tokio::sync::mpsc::Receiver<(NodeId, Frame)>, TransportError>;
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

#[derive(Default)]
struct GossipMetrics {
    sent: AtomicU64,
    received: AtomicU64,
    status_events: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct GossipMetricsSnapshot {
    pub sent: u64,
    pub received: u64,
    pub status_events: u64,
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
    #[cfg(feature = "hlc")]
    #[error("hlc: {0}")]
    Hlc(#[from] HlcError),
    #[cfg(feature = "hlc")]
    #[error("invalid HLC gossip envelope: {0}")]
    InvalidHlcEnvelope(&'static str),
}

/// Tracks outstanding ping requests.
#[derive(Debug)]
struct PendingPing {
    target: NodeId,
    stage: PingStage,
}

#[derive(Debug, Clone, Copy)]
enum PingStage {
    Direct { sent_at: Instant },
    Indirect { requested_at: Instant },
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
    metrics: Arc<GossipMetrics>,
    #[cfg(feature = "hlc")]
    hlc: LocalHlc,
    #[cfg(feature = "hlc")]
    event_sequence: u64,
    #[cfg(feature = "hlc")]
    seen_messages: HashSet<HlcEventId>,
    #[cfg(feature = "hlc")]
    seen_membership_events: HashSet<HlcEventId>,
    #[cfg(feature = "hlc")]
    latest_membership_timestamps: HashMap<NodeId, HybridTimestamp>,
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
            metrics: Arc::new(GossipMetrics::default()),
            #[cfg(feature = "hlc")]
            hlc: LocalHlc::new(Duration::from_secs(1)),
            #[cfg(feature = "hlc")]
            event_sequence: 0,
            #[cfg(feature = "hlc")]
            seen_messages: HashSet::new(),
            #[cfg(feature = "hlc")]
            seen_membership_events: HashSet::new(),
            #[cfg(feature = "hlc")]
            latest_membership_timestamps: HashMap::new(),
        }
    }

    #[cfg(feature = "hlc")]
    pub fn new_with_hlc(
        node_id: NodeId,
        config: GossipConfig,
        backend: Arc<dyn Transport>,
        membership: MembershipView,
        hlc: LocalHlc,
    ) -> Self {
        let mut engine = Self::new(node_id, config, backend, membership);
        engine.hlc = hlc;
        engine
    }

    /// Periodic tick: apply timeouts then send pings/gossip.
    pub async fn tick(&mut self) -> Result<Vec<StatusChange>, GossipError> {
        self.ticker.tick().await;
        let now = Instant::now();
        let mut changes = self.process_pending(now).await?;
        changes.extend(self.apply_timeouts(now));
        self.send_pings().await?;
        Ok(changes)
    }

    /// Handle an incoming Ping: reply Ack and update last_seen.
    pub async fn handle_ping(&mut self, from: NodeId, seq: u64, addr: SocketAddr) {
        self.metrics.received.fetch_add(1, Ordering::Relaxed);
        debug!(from = ?from, seq, "received ping");
        self.touch_peer(from, addr, Status::Alive);
        if let Err(err) = self
            .backend
            .send(
                from,
                Frame::Ack {
                    seq,
                    from: self.node_id,
                },
            )
            .await
        {
            warn!("failed to send ack to {from:?}: {err}");
        } else {
            self.metrics.sent.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Handle an incoming Ack: mark peer alive and clear pending if present.
    pub fn handle_ack(&mut self, from: NodeId, seq: u64, addr: SocketAddr) {
        self.metrics.received.fetch_add(1, Ordering::Relaxed);
        debug!(from = ?from, seq, "received ack");
        let _changed = if let Some(peer) = self.membership.peers.get_mut(&from) {
            if matches!(peer.state.status, Status::Dead) {
                return; // ignore late ack from dead peers
            }
            let changed = peer.state.status != Status::Alive;
            peer.state.status = Status::Alive;
            peer.state.last_seen = Instant::now();
            peer.state.suspect_since = None;
            changed
        } else {
            self.touch_peer(from, addr, Status::Alive);
            false // touch_peer stamps a newly observed peer
        };
        #[cfg(feature = "hlc")]
        if _changed {
            self.stamp_membership_event(from);
        }
        self.pending.remove(&seq);
    }

    /// Handle an incoming PingReq: forward Ping to target on behalf of the requester.
    pub async fn handle_ping_req(
        &mut self,
        from: NodeId,
        seq: u64,
        target: NodeId,
        addr: SocketAddr,
    ) {
        self.metrics.received.fetch_add(1, Ordering::Relaxed);
        self.touch_peer(from, addr, Status::Alive);
        if let Err(err) = self.backend.send(target, Frame::Ping { seq, from }).await {
            warn!("failed to forward ping to {target:?} for {from:?}: {err}");
        } else {
            self.metrics.sent.fetch_add(1, Ordering::Relaxed);
            debug!(target = ?target, helper = ?from, seq, "forwarded ping");
        }
    }

    /// Apply membership updates learned from gossip.
    pub fn apply_membership_update(&mut self, updates: &[MembershipUpdate]) {
        for upd in updates {
            if self.apply_one_membership_update(upd) {
                #[cfg(feature = "hlc")]
                self.stamp_membership_event(upd.node_id);
            }
        }
    }

    #[cfg(feature = "hlc")]
    pub fn apply_hlc_gossip(&mut self, message: &HlcGossipMessage) -> Result<usize, GossipError> {
        if message
            .updates
            .iter()
            .any(|event| event.timestamp > message.timestamp)
        {
            return Err(GossipError::InvalidHlcEnvelope(
                "membership timestamp exceeds envelope timestamp",
            ));
        }

        // A duplicate is still a receive event and advances the local HLC,
        // but it must not reapply membership state.
        self.hlc.receive(message.timestamp)?;
        if !self.seen_messages.insert(message.event_id) {
            return Ok(0);
        }

        let mut applied = 0;
        for event in &message.updates {
            if !self.seen_membership_events.insert(event.event_id) {
                continue;
            }
            let is_stale = self
                .latest_membership_timestamps
                .get(&event.update.node_id)
                .is_some_and(|current| event.timestamp <= *current);
            if is_stale {
                continue;
            }
            self.latest_membership_timestamps
                .insert(event.update.node_id, event.timestamp);
            if self.apply_one_membership_update(&event.update) {
                if let Some(peer) = self.membership.peers.get_mut(&event.update.node_id) {
                    peer.state.event_id = Some(event.event_id);
                    peer.state.timestamp = Some(event.timestamp);
                }
                applied += 1;
            }
        }
        Ok(applied)
    }

    fn apply_one_membership_update(&mut self, upd: &MembershipUpdate) -> bool {
        let status: Status = upd.status.clone().into();
        let received_at = Instant::now();
        let Some(entry) = self.membership.peers.get_mut(&upd.node_id) else {
            let mut state = PeerState::new(upd.incarnation, status.clone());
            if matches!(status, Status::Suspect) {
                state.suspect_since = Some(received_at);
            }
            self.membership.peers.insert(
                upd.node_id,
                Peer {
                    node_id: upd.node_id,
                    addr: upd.addr,
                    state,
                },
            );
            return true;
        };

        // SWIM orders gossiped membership by incarnation first. At an equal
        // incarnation only a stronger status can replace the current state.
        let is_newer_incarnation = upd.incarnation > entry.state.incarnation;
        let has_stronger_status = upd.incarnation == entry.state.incarnation
            && status_rank(&status) > status_rank(&entry.state.status);
        if !is_newer_incarnation && !has_stronger_status {
            return false;
        }

        let previous = entry.state.status.clone();
        if is_newer_incarnation {
            entry.state.incarnation = upd.incarnation;
            entry.state.last_seen = received_at;
        }
        entry.state.status = status.clone();
        entry.addr = upd.addr;
        entry.state.suspect_since = matches!(status, Status::Suspect).then_some(received_at);
        if previous != entry.state.status {
            self.metrics.status_events.fetch_add(1, Ordering::Relaxed);
            info!(
                peer = ?upd.node_id,
                from = ?previous,
                to = ?entry.state.status,
                "status updated via gossip"
            );
        }
        true
    }

    fn apply_timeouts(&mut self, now: Instant) -> Vec<StatusChange> {
        let changes = check_timeouts(
            &mut self.membership.peers,
            now,
            self.config.indirect_ping_timeout,
            self.config.suspect_to_dead_timeout,
        );
        for change in &changes {
            self.metrics.status_events.fetch_add(1, Ordering::Relaxed);
            info!(
                peer = ?change.node_id,
                from = ?change.from,
                to = ?change.to,
                "status transitioned by timeout"
            );
        }
        #[cfg(feature = "hlc")]
        for change in &changes {
            self.stamp_membership_event(change.node_id);
        }
        changes
    }

    async fn send_pings(&mut self) -> Result<(), GossipError> {
        let targets = self.select_targets();
        for target in targets {
            if self
                .pending
                .values()
                .any(|pending| pending.target == target)
            {
                continue; // already awaiting a response
            }
            self.seq = self.seq.wrapping_add(1);
            let seq = self.seq;
            let sent_at = Instant::now();
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
                    stage: PingStage::Direct { sent_at },
                },
            );
            // Gossip membership to the same target.
            #[cfg(not(feature = "hlc"))]
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
            #[cfg(not(feature = "hlc"))]
            if !updates.is_empty() {
                let _ = self
                    .backend
                    .send(
                        target,
                        Frame::Gossip(alopex_chirps_wire::frame::GossipMessage { updates }),
                    )
                    .await;
            }
            #[cfg(feature = "hlc")]
            {
                self.ensure_membership_stamps();
                let updates: Vec<StampedMembershipUpdate> = self
                    .membership
                    .peers
                    .values()
                    .map(|peer| StampedMembershipUpdate {
                        event_id: peer.state.event_id.expect("membership event stamped"),
                        timestamp: peer.state.timestamp.expect("membership event stamped"),
                        update: MembershipUpdate {
                            node_id: peer.node_id,
                            incarnation: peer.state.incarnation,
                            addr: peer.addr,
                            status: member_status(&peer.state.status),
                        },
                    })
                    .collect();
                if !updates.is_empty() {
                    let timestamp = self.hlc.tick();
                    let event_id = self.next_event_id();
                    let _ = self
                        .backend
                        .send(
                            target,
                            Frame::HlcGossip(HlcGossipMessage {
                                event_id,
                                timestamp,
                                updates,
                            }),
                        )
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn process_pending(&mut self, now: Instant) -> Result<Vec<StatusChange>, GossipError> {
        let mut to_request = Vec::new();
        let mut to_suspect = Vec::new();

        for (seq, pending) in self.pending.iter_mut() {
            match pending.stage {
                PingStage::Direct { sent_at } => {
                    if now.saturating_duration_since(sent_at) >= self.config.ping_timeout {
                        to_request.push((*seq, pending.target));
                        pending.stage = PingStage::Indirect { requested_at: now };
                    }
                }
                PingStage::Indirect { requested_at } => {
                    if now.saturating_duration_since(requested_at)
                        >= self.config.indirect_ping_timeout
                    {
                        to_suspect.push((*seq, pending.target));
                    }
                }
            }
        }

        for (seq, target) in to_request {
            self.send_ping_req(seq, target).await?;
        }

        let mut changes = Vec::new();
        for (seq, target) in to_suspect {
            let mut _changed = false;
            if let Some(peer) = self.membership.peers.get_mut(&target)
                && peer.state.status == Status::Alive
            {
                peer.state.status = Status::Suspect;
                peer.state.suspect_since = Some(now);
                changes.push(StatusChange::new(
                    target,
                    Status::Alive,
                    Status::Suspect,
                    peer.state.incarnation,
                ));
                _changed = true;
            }
            #[cfg(feature = "hlc")]
            if _changed {
                self.stamp_membership_event(target);
            }
            self.pending.remove(&seq);
        }

        Ok(changes)
    }

    fn select_targets(&self) -> Vec<NodeId> {
        let candidate_ids: Vec<NodeId> = self
            .membership
            .peers
            .iter()
            .filter(|(_, peer)| !matches!(peer.state.status, Status::Dead))
            .map(|(id, _)| *id)
            .collect();
        let total_nodes = candidate_ids.len() + 1; // include self
        let fanout = calculate_fanout(total_nodes, self.config.fanout);
        let mut rng = rand::thread_rng();
        candidate_ids.into_iter().choose_multiple(&mut rng, fanout)
    }

    fn touch_peer(&mut self, node_id: NodeId, addr: SocketAddr, status: Status) {
        let _changed = self
            .membership
            .peers
            .get(&node_id)
            .is_none_or(|peer| peer.state.status != status);
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
        #[cfg(feature = "hlc")]
        if _changed {
            self.stamp_membership_event(node_id);
        }
    }

    /// Returns current membership view (read-only clone).
    pub fn membership(&self) -> MembershipView {
        self.membership.clone()
    }

    #[cfg(feature = "hlc")]
    pub fn hlc_current(&self) -> HybridTimestamp {
        self.hlc.current()
    }

    #[cfg(feature = "hlc")]
    fn next_event_id(&mut self) -> HlcEventId {
        self.event_sequence = self.event_sequence.wrapping_add(1);
        if self.event_sequence == 0 {
            self.event_sequence = 1;
        }
        HlcEventId {
            source: self.node_id,
            sequence: self.event_sequence,
        }
    }

    #[cfg(feature = "hlc")]
    fn stamp_membership_event(&mut self, node_id: NodeId) {
        let timestamp = self.hlc.tick();
        let event_id = self.next_event_id();
        if let Some(peer) = self.membership.peers.get_mut(&node_id) {
            peer.state.timestamp = Some(timestamp);
            peer.state.event_id = Some(event_id);
            self.latest_membership_timestamps.insert(node_id, timestamp);
        }
    }

    #[cfg(feature = "hlc")]
    fn ensure_membership_stamps(&mut self) {
        let unstamped: Vec<NodeId> = self
            .membership
            .peers
            .iter()
            .filter_map(|(node_id, peer)| peer.state.timestamp.is_none().then_some(*node_id))
            .collect();
        for node_id in unstamped {
            self.stamp_membership_event(node_id);
        }
    }

    /// 現在のメトリクスを取得する。
    pub fn metrics(&self) -> GossipMetricsSnapshot {
        GossipMetricsSnapshot {
            sent: self.metrics.sent.load(Ordering::Relaxed),
            received: self.metrics.received.load(Ordering::Relaxed),
            status_events: self.metrics.status_events.load(Ordering::Relaxed),
        }
    }

    async fn send_ping_req(&self, seq: u64, target: NodeId) -> Result<(), GossipError> {
        let helpers = self.select_indirect_targets(target);
        for helper in helpers {
            let frame = Frame::PingReq {
                seq,
                from: self.node_id,
                target,
            };
            if let Err(err) = self.backend.send(helper, frame).await {
                warn!("failed to send pingreq to {helper:?} for {target:?}: {err}");
            }
        }
        Ok(())
    }

    fn select_indirect_targets(&self, target: NodeId) -> Vec<NodeId> {
        let candidates: Vec<NodeId> = self
            .membership
            .peers
            .iter()
            .filter_map(|(id, peer)| {
                if *id != target && !matches!(peer.state.status, Status::Dead) {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        let total_nodes = candidates.len() + 1;
        let fanout = calculate_fanout(total_nodes, self.config.fanout);
        let mut rng = rand::thread_rng();
        candidates.into_iter().choose_multiple(&mut rng, fanout)
    }
}

fn member_status(status: &Status) -> MemberStatus {
    match status {
        Status::Alive => MemberStatus::Alive,
        Status::Suspect => MemberStatus::Suspect,
        Status::Dead => MemberStatus::Dead,
    }
}

fn status_rank(status: &Status) -> u8 {
    match status {
        Status::Alive => 0,
        Status::Suspect => 1,
        Status::Dead => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    struct MockBackend {
        sent: Arc<AtomicUsize>,
        frames: Arc<Mutex<Vec<(NodeId, Frame)>>>,
    }

    #[async_trait]
    impl Transport for MockBackend {
        async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError> {
            self.sent.fetch_add(1, Ordering::SeqCst);
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
            frames: Arc::new(Mutex::new(Vec::new())),
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
            frames: Arc::new(Mutex::new(Vec::new())),
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
        let metrics = engine.metrics();
        assert!(
            metrics.status_events >= 1,
            "status events should be counted"
        );
    }

    #[tokio::test]
    #[cfg_attr(windows, ignore)] // Windows timer resolution (15.6ms) causes flaky timing tests
    async fn ping_timeout_requests_indirect_and_marks_suspect() {
        let node = NodeId::new();
        let target = NodeId::new();
        let helper = NodeId::new();
        let mut membership = MembershipView::new();
        membership.peers.insert(
            target,
            Peer {
                node_id: target,
                addr: "127.0.0.1:9100".parse().unwrap(),
                state: PeerState::new(0, Status::Alive),
            },
        );
        membership.peers.insert(
            helper,
            Peer {
                node_id: helper,
                addr: "127.0.0.1:9101".parse().unwrap(),
                state: PeerState::new(0, Status::Alive),
            },
        );

        let cfg = GossipConfig {
            ping_timeout: Duration::from_millis(5),
            indirect_ping_timeout: Duration::from_millis(15),
            suspect_to_dead_timeout: Duration::from_millis(40),
            gossip_interval: Duration::from_millis(5),
            fanout: Some(2),
        };

        let backend = Arc::new(MockBackend {
            sent: Arc::new(AtomicUsize::new(0)),
            frames: Arc::new(Mutex::new(Vec::new())),
        });
        let mut engine = GossipEngine::new(node, cfg, backend.clone(), membership);

        // first tick -> send direct ping
        engine.tick().await.unwrap();

        tokio::time::sleep(Duration::from_millis(7)).await; // exceed ping_timeout
        engine.tick().await.unwrap(); // trigger PingReq to helper

        tokio::time::sleep(Duration::from_millis(20)).await; // exceed indirect timeout
        let changes = engine.tick().await.unwrap(); // mark suspect

        let frames = backend.frames.lock().unwrap();
        assert!(
            frames.iter().any(|(to, frame)| *to == helper
                && matches!(frame, Frame::PingReq { target: t, .. } if *t == target)),
            "expected PingReq to helper"
        );
        assert!(
            changes
                .iter()
                .any(|c| c.node_id == target && c.to == Status::Suspect)
        );
    }

    #[tokio::test]
    async fn handle_ping_req_forwards_ping_from_requester() {
        let node = NodeId::new();
        let requester = NodeId::new();
        let target = NodeId::new();
        let mut membership = MembershipView::new();
        membership.peers.insert(
            target,
            Peer {
                node_id: target,
                addr: "127.0.0.1:9200".parse().unwrap(),
                state: PeerState::new(0, Status::Alive),
            },
        );

        let backend = Arc::new(MockBackend {
            sent: Arc::new(AtomicUsize::new(0)),
            frames: Arc::new(Mutex::new(Vec::new())),
        });
        let mut engine = GossipEngine::new(node, config(), backend.clone(), membership);

        engine
            .handle_ping_req(requester, 10, target, "127.0.0.1:9300".parse().unwrap())
            .await;

        let frames = backend.frames.lock().unwrap();
        assert!(
            frames.iter().any(|(to, frame)| *to == target
                && matches!(frame, Frame::Ping { seq, from } if *seq == 10 && *from == requester)),
            "expected forwarded ping to target with requester as source"
        );
    }

    #[tokio::test]
    async fn equal_incarnation_alive_gossip_does_not_refresh_liveness() {
        let node = NodeId::new();
        let peer_id = NodeId::new();
        let last_seen = Instant::now() - Duration::from_secs(1);
        let mut membership = MembershipView::new();
        membership.peers.insert(
            peer_id,
            Peer {
                node_id: peer_id,
                addr: "127.0.0.1:9400".parse().unwrap(),
                state: PeerState {
                    incarnation: 7,
                    status: Status::Alive,
                    last_seen,
                    suspect_since: None,
                    #[cfg(feature = "hlc")]
                    event_id: None,
                    #[cfg(feature = "hlc")]
                    timestamp: None,
                },
            },
        );
        let backend = Arc::new(MockBackend {
            sent: Arc::new(AtomicUsize::new(0)),
            frames: Arc::new(Mutex::new(Vec::new())),
        });
        let mut engine = GossipEngine::new(node, config(), backend, membership);

        engine.apply_membership_update(&[MembershipUpdate {
            node_id: peer_id,
            incarnation: 7,
            addr: "127.0.0.1:9400".parse().unwrap(),
            status: MemberStatus::Alive,
        }]);

        let peer = engine.membership.peers.get(&peer_id).unwrap();
        assert_eq!(peer.state.status, Status::Alive);
        assert_eq!(peer.state.last_seen, last_seen);

        let changes = engine.apply_timeouts(Instant::now());
        assert!(
            changes
                .iter()
                .any(|change| change.node_id == peer_id && change.to == Status::Suspect),
            "stale Alive gossip must not keep an unreachable peer alive"
        );
    }

    #[tokio::test]
    async fn only_higher_incarnation_alive_gossip_refutes_suspicion() {
        let node = NodeId::new();
        let peer_id = NodeId::new();
        let last_seen = Instant::now() - Duration::from_secs(1);
        let mut membership = MembershipView::new();
        membership.peers.insert(
            peer_id,
            Peer {
                node_id: peer_id,
                addr: "127.0.0.1:9500".parse().unwrap(),
                state: PeerState {
                    incarnation: 3,
                    status: Status::Suspect,
                    last_seen,
                    suspect_since: Some(last_seen),
                    #[cfg(feature = "hlc")]
                    event_id: None,
                    #[cfg(feature = "hlc")]
                    timestamp: None,
                },
            },
        );
        let backend = Arc::new(MockBackend {
            sent: Arc::new(AtomicUsize::new(0)),
            frames: Arc::new(Mutex::new(Vec::new())),
        });
        let mut engine = GossipEngine::new(node, config(), backend, membership);

        engine.apply_membership_update(&[MembershipUpdate {
            node_id: peer_id,
            incarnation: 3,
            addr: "127.0.0.1:9500".parse().unwrap(),
            status: MemberStatus::Alive,
        }]);
        assert_eq!(
            engine.membership.peers.get(&peer_id).unwrap().state.status,
            Status::Suspect
        );

        engine.apply_membership_update(&[MembershipUpdate {
            node_id: peer_id,
            incarnation: 4,
            addr: "127.0.0.1:9500".parse().unwrap(),
            status: MemberStatus::Alive,
        }]);
        let peer = engine.membership.peers.get(&peer_id).unwrap();
        assert_eq!(peer.state.incarnation, 4);
        assert_eq!(peer.state.status, Status::Alive);
        assert!(peer.state.suspect_since.is_none());
        assert!(peer.state.last_seen > last_seen);
    }
}
