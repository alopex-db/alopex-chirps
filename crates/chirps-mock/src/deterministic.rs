//! Seeded, single-step network simulation for reproducible distributed tests.

use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_core::error::TransportError;
use alopex_chirps_wire::frame::Frame;
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, mpsc};

pub const SCHEDULE_SCHEMA: &str = "chirps.deterministic-network-schedule/v1";
pub const GENERATOR_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DirectedLink {
    pub source: NodeId,
    pub target: NodeId,
}

impl DirectedLink {
    pub const fn new(source: NodeId, target: NodeId) -> Self {
        Self { source, target }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FaultEffect {
    Delay {
        ticks: u64,
    },
    Drop,
    /// Total number of copies to enqueue, including the original.
    Duplicate {
        copies: u16,
    },
    Reorder {
        extra_ticks: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultRule {
    pub link: DirectedLink,
    pub occurrence: u64,
    pub effects: Vec<FaultEffect>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultSchedule {
    pub schema: String,
    pub seed: u64,
    pub generator_version: u32,
    rules: Vec<FaultRule>,
}

impl FaultSchedule {
    pub fn empty(seed: u64) -> Self {
        Self {
            schema: SCHEDULE_SCHEMA.to_owned(),
            seed,
            generator_version: GENERATOR_VERSION,
            rules: Vec::new(),
        }
    }

    /// Expands every per-packet decision before the simulator starts.
    pub fn from_seed(
        seed: u64,
        links: impl IntoIterator<Item = DirectedLink>,
        messages_per_link: u64,
    ) -> Self {
        let mut state = seed;
        let mut links: Vec<_> = links.into_iter().collect();
        links.sort_unstable();
        links.dedup();
        let mut builder = FaultScheduleBuilder::new(seed);
        for link in links {
            for occurrence in 0..messages_per_link {
                let draw = splitmix64(&mut state);
                let effects = match draw % 6 {
                    0 => Vec::new(),
                    1 => vec![FaultEffect::Delay {
                        ticks: 1 + ((draw >> 8) % 3),
                    }],
                    2 => vec![FaultEffect::Drop],
                    3 => vec![FaultEffect::Duplicate { copies: 2 }],
                    4 => vec![FaultEffect::Reorder {
                        extra_ticks: 1 + ((draw >> 8) % 4),
                    }],
                    _ => vec![
                        FaultEffect::Delay { ticks: 1 },
                        FaultEffect::Duplicate { copies: 2 },
                    ],
                };
                if !effects.is_empty() {
                    builder = builder.on_nth(link, occurrence, effects);
                }
            }
        }
        builder.build()
    }

    pub fn rules(&self) -> &[FaultRule] {
        &self.rules
    }

    pub fn without_rule(&self, index: usize) -> Self {
        let mut candidate = self.clone();
        candidate.rules.remove(index);
        candidate
    }

    pub fn without_effect(&self, rule: usize, effect: usize) -> Self {
        let mut candidate = self.clone();
        candidate.rules[rule].effects.remove(effect);
        candidate
    }

    fn effects_for(&self, link: DirectedLink, occurrence: u64) -> &[FaultEffect] {
        self.rules
            .binary_search_by_key(&(link, occurrence), |rule| (rule.link, rule.occurrence))
            .ok()
            .map_or(&[], |index| self.rules[index].effects.as_slice())
    }
}

#[derive(Debug, Clone)]
pub struct FaultScheduleBuilder {
    schedule: FaultSchedule,
}

impl FaultScheduleBuilder {
    pub fn new(seed: u64) -> Self {
        Self {
            schedule: FaultSchedule::empty(seed),
        }
    }

    pub fn on_nth(
        mut self,
        link: DirectedLink,
        occurrence: u64,
        effects: impl IntoIterator<Item = FaultEffect>,
    ) -> Self {
        self.schedule.rules.push(FaultRule {
            link,
            occurrence,
            effects: effects.into_iter().collect(),
        });
        self
    }

    pub fn build(mut self) -> FaultSchedule {
        self.schedule
            .rules
            .sort_by_key(|rule| (rule.link, rule.occurrence));
        for pair in self.schedule.rules.windows(2) {
            assert_ne!(
                (pair[0].link, pair[0].occurrence),
                (pair[1].link, pair[1].occurrence),
                "fault schedule contains a duplicate link occurrence"
            );
        }
        self.schedule
    }
}

/// Delta-debug a schedule. Every predicate invocation receives a complete,
/// independent candidate; callers must construct fresh system state from it.
pub fn minimize_schedule(
    schedule: &FaultSchedule,
    still_fails: impl Fn(&FaultSchedule) -> bool,
) -> FaultSchedule {
    if !still_fails(schedule) {
        return schedule.clone();
    }

    let mut current = schedule.clone();
    let mut granularity = 2usize;
    while current.rules.len() >= 2 {
        let chunk = current.rules.len().div_ceil(granularity);
        let mut reduced = false;
        let mut start = 0usize;
        while start < current.rules.len() {
            let end = (start + chunk).min(current.rules.len());
            let mut candidate = current.clone();
            candidate.rules.drain(start..end);
            if still_fails(&candidate) {
                current = candidate;
                granularity = granularity.saturating_sub(1).max(2);
                reduced = true;
                break;
            }
            start = end;
        }
        if !reduced {
            if granularity >= current.rules.len() {
                break;
            }
            granularity = (granularity * 2).min(current.rules.len());
        }
    }

    loop {
        let mut reduced = false;
        for index in 0..current.rules.len() {
            let mut candidate = current.clone();
            candidate.rules.remove(index);
            if still_fails(&candidate) {
                current = candidate;
                reduced = true;
                break;
            }
        }
        if !reduced {
            break;
        }
    }

    loop {
        let mut reduced = false;
        'rules: for rule in 0..current.rules.len() {
            for effect in 0..current.rules[rule].effects.len() {
                let mut candidate = current.clone();
                candidate.rules[rule].effects.remove(effect);
                if still_fails(&candidate) {
                    current = candidate;
                    reduced = true;
                    break 'rules;
                }
            }
        }
        if !reduced {
            break;
        }
    }
    current
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Scheduled,
    Delivered,
    Dropped,
    Partitioned,
    Healed,
    Reconnected,
    StaleGeneration,
}

impl EventKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Delivered => "delivered",
            Self::Dropped => "dropped",
            Self::Partitioned => "partitioned",
            Self::Healed => "healed",
            Self::Reconnected => "reconnected",
            Self::StaleGeneration => "stale_generation",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRecord {
    pub sequence: u64,
    pub time: u64,
    pub kind: EventKind,
    pub packet_id: Option<u64>,
    pub parent_packet_id: Option<u64>,
    pub copy: Option<u16>,
    pub link: Option<DirectedLink>,
    pub node: Option<NodeId>,
    pub generation: Option<u64>,
    pub frame_fingerprint: Option<String>,
}

#[derive(Default)]
struct EventDetails {
    packet_id: Option<u64>,
    parent_packet_id: Option<u64>,
    copy: Option<u16>,
    link: Option<DirectedLink>,
    node: Option<NodeId>,
    generation: Option<u64>,
    frame_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct QueueKey {
    due: u64,
    ordinal: u64,
    packet_id: u64,
    copy: u16,
}

struct QueuedPacket {
    link: DirectedLink,
    source_generation: u64,
    target_generation: u64,
    frame: Frame,
    frame_fingerprint: String,
}

struct Peer {
    generation: u64,
    addr: SocketAddr,
    sender: mpsc::Sender<(NodeId, Frame)>,
}

struct NetworkState {
    now: u64,
    peers: BTreeMap<NodeId, Peer>,
    generations: BTreeMap<NodeId, u64>,
    occurrences: BTreeMap<DirectedLink, u64>,
    partitions: BTreeSet<DirectedLink>,
    queue: BTreeMap<QueueKey, QueuedPacket>,
    events: Vec<EventRecord>,
    next_packet_id: u64,
    next_event_sequence: u64,
}

impl NetworkState {
    fn record(&mut self, kind: EventKind, details: EventDetails) {
        self.events.push(EventRecord {
            sequence: self.next_event_sequence,
            time: self.now,
            kind,
            packet_id: details.packet_id,
            parent_packet_id: details.parent_packet_id,
            copy: details.copy,
            link: details.link,
            node: details.node,
            generation: details.generation,
            frame_fingerprint: details.frame_fingerprint,
        });
        self.next_event_sequence += 1;
    }
}

#[derive(Clone)]
pub struct DeterministicNetwork {
    schedule: Arc<FaultSchedule>,
    state: Arc<Mutex<NetworkState>>,
}

impl DeterministicNetwork {
    pub fn new(schedule: FaultSchedule) -> Self {
        Self {
            schedule: Arc::new(schedule),
            state: Arc::new(Mutex::new(NetworkState {
                now: 0,
                peers: BTreeMap::new(),
                generations: BTreeMap::new(),
                occurrences: BTreeMap::new(),
                partitions: BTreeSet::new(),
                queue: BTreeMap::new(),
                events: Vec::new(),
                next_packet_id: 0,
                next_event_sequence: 0,
            })),
        }
    }

    pub async fn add_node(&self, node_id: NodeId) -> DeterministicBackend {
        let (sender, receiver) = mpsc::channel(1024);
        let mut state = self.state.lock().await;
        let generation = state
            .generations
            .get(&node_id)
            .copied()
            .map_or(0, |generation| generation + 1);
        let reconnect = state.generations.insert(node_id, generation).is_some();
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, generation as u16));
        state.peers.insert(
            node_id,
            Peer {
                generation,
                addr,
                sender,
            },
        );
        if reconnect {
            state.record(
                EventKind::Reconnected,
                EventDetails {
                    node: Some(node_id),
                    generation: Some(generation),
                    ..EventDetails::default()
                },
            );
        }
        DeterministicBackend {
            node_id,
            generation,
            network: self.clone(),
            incoming: Mutex::new(Some(receiver)),
            closed: AtomicBool::new(false),
        }
    }

    pub async fn partition(&self, link: DirectedLink) {
        let mut state = self.state.lock().await;
        if state.partitions.insert(link) {
            state.record(
                EventKind::Partitioned,
                EventDetails {
                    link: Some(link),
                    ..EventDetails::default()
                },
            );
        }
    }

    pub async fn heal(&self, link: DirectedLink) {
        let mut state = self.state.lock().await;
        if state.partitions.remove(&link) {
            state.record(
                EventKind::Healed,
                EventDetails {
                    link: Some(link),
                    ..EventDetails::default()
                },
            );
        }
    }

    pub async fn is_partitioned(&self, link: DirectedLink) -> bool {
        self.state.lock().await.partitions.contains(&link)
    }

    pub async fn now(&self) -> u64 {
        self.state.lock().await.now
    }

    pub async fn advance_by(&self, ticks: u64) -> Result<(), TransportError> {
        let mut state = self.state.lock().await;
        state.now = state.now.saturating_add(ticks);
        drain_due(&mut state)
    }

    pub async fn run_until_idle(&self) -> Result<(), TransportError> {
        while self.deliver_next().await? {}
        Ok(())
    }

    /// Advances to and resolves exactly one deliverable queued packet/copy.
    /// Returns `false` when only partition-blocked packets (or no packets)
    /// remain. This is the coordinator primitive for per-network-event oracles.
    pub async fn deliver_next(&self) -> Result<bool, TransportError> {
        let mut state = self.state.lock().await;
        let Some(key) = state
            .queue
            .iter()
            .find(|(_, packet)| !state.partitions.contains(&packet.link))
            .map(|(key, _)| *key)
        else {
            return Ok(false);
        };
        state.now = state.now.max(key.due);
        deliver_key(&mut state, key)?;
        Ok(true)
    }

    pub async fn pending_count(&self) -> usize {
        self.state.lock().await.queue.len()
    }

    pub async fn events_after(&self, sequence: u64) -> Vec<EventRecord> {
        self.state
            .lock()
            .await
            .events
            .iter()
            .filter(|event| event.sequence >= sequence)
            .cloned()
            .collect()
    }

    pub async fn events(&self) -> Vec<EventRecord> {
        self.state.lock().await.events.clone()
    }

    pub async fn stable_trace(&self) -> Vec<String> {
        self.state
            .lock()
            .await
            .events
            .iter()
            .map(stable_event)
            .collect()
    }

    async fn enqueue(
        &self,
        source: NodeId,
        source_generation: u64,
        target: NodeId,
        frame: Frame,
    ) -> Result<(), TransportError> {
        let mut state = self.state.lock().await;
        match state.peers.get(&source) {
            Some(peer) if peer.generation == source_generation => {}
            _ => {
                return Err(TransportError::Connection(
                    "deterministic backend registration is stale".into(),
                ));
            }
        }
        let target_generation = state
            .peers
            .get(&target)
            .map(|peer| peer.generation)
            .ok_or_else(|| TransportError::Connection("target is not connected".into()))?;
        let link = DirectedLink::new(source, target);
        let occurrence = state.occurrences.entry(link).or_default();
        let effects = self.schedule.effects_for(link, *occurrence).to_vec();
        *occurrence += 1;
        let packet_id = state.next_packet_id;
        state.next_packet_id += 1;
        let frame_fingerprint = frame_fingerprint(&frame)?;

        let mut due = state.now;
        let mut copies = 1u16;
        let mut drop = state.partitions.contains(&link);
        for effect in effects {
            match effect {
                FaultEffect::Delay { ticks } => due = due.saturating_add(ticks),
                FaultEffect::Drop => drop = true,
                FaultEffect::Duplicate { copies: requested } => copies = requested.max(1),
                FaultEffect::Reorder { extra_ticks } => due = due.saturating_add(extra_ticks),
            }
        }
        if drop {
            state.record(
                EventKind::Dropped,
                EventDetails {
                    packet_id: Some(packet_id),
                    link: Some(link),
                    frame_fingerprint: Some(frame_fingerprint),
                    ..EventDetails::default()
                },
            );
            return Ok(());
        }

        for copy in 0..copies {
            let ordinal = packet_id.saturating_mul(u16::MAX as u64) + u64::from(copy);
            state.queue.insert(
                QueueKey {
                    due,
                    ordinal,
                    packet_id,
                    copy,
                },
                QueuedPacket {
                    link,
                    source_generation,
                    target_generation,
                    frame: frame.clone(),
                    frame_fingerprint: frame_fingerprint.clone(),
                },
            );
            state.record(
                EventKind::Scheduled,
                EventDetails {
                    packet_id: Some(packet_id),
                    parent_packet_id: (copy > 0).then_some(packet_id),
                    copy: Some(copy),
                    link: Some(link),
                    frame_fingerprint: Some(frame_fingerprint.clone()),
                    ..EventDetails::default()
                },
            );
        }
        Ok(())
    }
}

fn drain_due(state: &mut NetworkState) -> Result<(), TransportError> {
    let due_keys: Vec<_> = state
        .queue
        .keys()
        .copied()
        .take_while(|key| key.due <= state.now)
        .collect();
    for key in due_keys {
        deliver_key(state, key)?;
    }
    Ok(())
}

fn deliver_key(state: &mut NetworkState, key: QueueKey) -> Result<(), TransportError> {
    let packet = state.queue.remove(&key).expect("queue key was observed");
    if state.partitions.contains(&packet.link) {
        state.queue.insert(key, packet);
        return Ok(());
    }
    let source_is_current = state
        .peers
        .get(&packet.link.source)
        .is_some_and(|peer| peer.generation == packet.source_generation);
    let Some(peer) = state.peers.get(&packet.link.target) else {
        state.record(
            EventKind::Dropped,
            EventDetails {
                packet_id: Some(key.packet_id),
                parent_packet_id: (key.copy > 0).then_some(key.packet_id),
                copy: Some(key.copy),
                link: Some(packet.link),
                frame_fingerprint: Some(packet.frame_fingerprint.clone()),
                ..EventDetails::default()
            },
        );
        return Ok(());
    };
    if !source_is_current || peer.generation != packet.target_generation {
        state.record(
            EventKind::StaleGeneration,
            EventDetails {
                packet_id: Some(key.packet_id),
                parent_packet_id: (key.copy > 0).then_some(key.packet_id),
                copy: Some(key.copy),
                link: Some(packet.link),
                frame_fingerprint: Some(packet.frame_fingerprint.clone()),
                ..EventDetails::default()
            },
        );
        return Ok(());
    }
    peer.sender
        .try_send((packet.link.source, packet.frame))
        .map_err(|error| TransportError::Send(error.to_string()))?;
    state.record(
        EventKind::Delivered,
        EventDetails {
            packet_id: Some(key.packet_id),
            parent_packet_id: (key.copy > 0).then_some(key.packet_id),
            copy: Some(key.copy),
            link: Some(packet.link),
            frame_fingerprint: Some(packet.frame_fingerprint),
            ..EventDetails::default()
        },
    );
    Ok(())
}

fn stable_event(event: &EventRecord) -> String {
    let link = event.link.map_or_else(
        || "-".to_owned(),
        |link| format!("{}>{}", encode_node(link.source), encode_node(link.target)),
    );
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        event.sequence,
        event.time,
        event.kind.as_str(),
        event
            .packet_id
            .map_or_else(|| "-".to_owned(), |id| id.to_string()),
        event
            .copy
            .map_or_else(|| "-".to_owned(), |copy| copy.to_string()),
        link,
        event
            .parent_packet_id
            .map_or_else(|| "-".to_owned(), |id| id.to_string()),
        event.node.map_or_else(|| "-".to_owned(), encode_node),
        event
            .generation
            .map_or_else(|| "-".to_owned(), |generation| generation.to_string()),
        event.frame_fingerprint.as_deref().unwrap_or("-")
    )
}

fn frame_fingerprint(frame: &Frame) -> Result<String, TransportError> {
    let encoded = serde_json::to_vec(frame)
        .map_err(|error| TransportError::Send(format!("frame fingerprint: {error}")))?;
    let hash = encoded.iter().fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    Ok(format!("fnv1a64:{hash:016x}"))
}

fn encode_node(node: NodeId) -> String {
    node.as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub struct DeterministicBackend {
    node_id: NodeId,
    generation: u64,
    network: DeterministicNetwork,
    incoming: Mutex<Option<mpsc::Receiver<(NodeId, Frame)>>>,
    closed: AtomicBool,
}

#[async_trait]
impl MessageBackend for DeterministicBackend {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TransportError::Connection(
                "deterministic backend is closed".into(),
            ));
        }
        self.network
            .enqueue(self.node_id, self.generation, target, frame)
            .await
    }

    async fn broadcast(&self, frame: Frame) -> Result<usize, TransportError> {
        let targets: Vec<_> = {
            let state = self.network.state.lock().await;
            state
                .peers
                .keys()
                .copied()
                .filter(|target| *target != self.node_id)
                .collect()
        };
        for target in &targets {
            self.send(*target, frame.clone()).await?;
        }
        Ok(targets.len())
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        self.incoming
            .lock()
            .await
            .take()
            .ok_or_else(|| TransportError::Subscribe("already subscribed".into()))
    }

    async fn close(&self) -> Result<(), TransportError> {
        self.closed.store(true, Ordering::SeqCst);
        let mut state = self.network.state.lock().await;
        if state
            .peers
            .get(&self.node_id)
            .is_some_and(|peer| peer.generation == self.generation)
        {
            state.peers.remove(&self.node_id);
        }
        Ok(())
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        let Ok(state) = self.network.state.try_lock() else {
            return Vec::new();
        };
        state
            .peers
            .iter()
            .filter(|(node, _)| **node != self.node_id)
            .map(|(node, peer)| (*node, peer.addr))
            .collect()
    }
}
