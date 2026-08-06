use crate::types::{Peer, Status};
use alopex_chirps_wire::node_id::NodeId;
use std::cmp::min;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

/// Describes a membership status transition detected by timeout checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusChange {
    pub node_id: NodeId,
    pub from: Status,
    pub to: Status,
    pub incarnation: u64,
}

impl StatusChange {
    pub fn new(node_id: NodeId, from: Status, to: Status, incarnation: u64) -> Self {
        Self {
            node_id,
            from,
            to,
            incarnation,
        }
    }
}

/// Calculates gossip fan-out using `max(3, ceil(sqrt(N)))` capped by `N-1`.
///
/// If an explicit `configured` value is provided, it is clamped to `N-1`.
pub fn calculate_fanout(total_nodes: usize, configured: Option<usize>) -> usize {
    if total_nodes <= 1 {
        return 0;
    }

    let cap = total_nodes - 1;
    if let Some(fanout) = configured {
        return min(fanout, cap);
    }

    let sqrt_n = (total_nodes as f64).sqrt().ceil() as usize;
    let default_fanout = std::cmp::max(3, sqrt_n);
    min(default_fanout, cap)
}

/// Applies timeout-based status transitions for all peers.
///
/// Transitions:
/// - `Alive` -> `Suspect` when `last_seen` exceeds `suspect_after`
/// - `Suspect` -> `Dead` when `suspect_since` exceeds `dead_after`
/// - `Suspect` -> `Alive` when a recent `last_seen` is within `suspect_after`
///
/// The function mutates the provided peer map and returns the list of transitions.
pub fn check_timeouts(
    peers: &mut HashMap<NodeId, Peer>,
    now: Instant,
    suspect_after: Duration,
    dead_after: Duration,
) -> Vec<StatusChange> {
    let mut changes = Vec::new();

    for (node_id, peer) in peers.iter_mut() {
        let state = &mut peer.state;
        match state.status {
            Status::Alive => {
                let elapsed = now.saturating_duration_since(state.last_seen);
                if elapsed >= suspect_after {
                    state.status = Status::Suspect;
                    state.suspect_since = Some(now);
                    changes.push(StatusChange::new(
                        *node_id,
                        Status::Alive,
                        Status::Suspect,
                        state.incarnation,
                    ));
                }
            }
            Status::Suspect => {
                let suspect_started = state.suspect_since.unwrap_or(state.last_seen);
                let since_suspect = now.saturating_duration_since(suspect_started);
                let since_last_seen = now.saturating_duration_since(state.last_seen);

                if since_last_seen < suspect_after {
                    state.status = Status::Alive;
                    state.suspect_since = None;
                    changes.push(StatusChange::new(
                        *node_id,
                        Status::Suspect,
                        Status::Alive,
                        state.incarnation,
                    ));
                } else if since_suspect >= dead_after {
                    state.status = Status::Dead;
                    changes.push(StatusChange::new(
                        *node_id,
                        Status::Suspect,
                        Status::Dead,
                        state.incarnation,
                    ));
                }
            }
            Status::Dead => { /* terminal state; wait for explicit recovery */ }
        }
    }

    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PeerState;
    use std::net::SocketAddr;

    fn peer(node_id: NodeId, addr: SocketAddr, state: PeerState) -> Peer {
        Peer {
            node_id,
            addr,
            state,
        }
    }

    #[test]
    fn fanout_defaults_and_caps() {
        assert_eq!(calculate_fanout(0, None), 0);
        assert_eq!(calculate_fanout(1, None), 0);
        assert_eq!(calculate_fanout(2, None), 1); // only one other node
        assert_eq!(calculate_fanout(10, None), 4); // ceil(sqrt(10)) = 4
        assert_eq!(calculate_fanout(4, Some(10)), 3); // capped at N-1
        assert_eq!(calculate_fanout(5, Some(2)), 2); // respects config
    }

    #[test]
    fn alive_transitions_to_suspect() {
        let now = Instant::now();
        let mut peers = HashMap::new();
        let node_id = NodeId::new();
        let mut state = PeerState::new(0, Status::Alive);
        state.last_seen = now - Duration::from_secs(4);
        peers.insert(
            node_id,
            peer(node_id, "127.0.0.1:9000".parse().unwrap(), state),
        );

        let changes = check_timeouts(
            &mut peers,
            now,
            Duration::from_secs(3),
            Duration::from_secs(6),
        );

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].from, Status::Alive);
        assert_eq!(changes[0].to, Status::Suspect);
        let peer_state = &peers.get(&node_id).unwrap().state;
        assert_eq!(peer_state.status, Status::Suspect);
        assert!(peer_state.suspect_since.is_some());
    }

    #[test]
    fn suspect_transitions_to_dead_after_timeout() {
        let now = Instant::now();
        let mut peers = HashMap::new();
        let node_id = NodeId::new();
        let mut state = PeerState::new(0, Status::Suspect);
        state.last_seen = now - Duration::from_secs(7);
        state.suspect_since = Some(now - Duration::from_secs(7));
        peers.insert(
            node_id,
            peer(node_id, "127.0.0.1:9001".parse().unwrap(), state),
        );

        let changes = check_timeouts(
            &mut peers,
            now,
            Duration::from_secs(3),
            Duration::from_secs(6),
        );

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].from, Status::Suspect);
        assert_eq!(changes[0].to, Status::Dead);
        assert_eq!(peers.get(&node_id).unwrap().state.status, Status::Dead);
    }

    #[test]
    fn suspect_recovers_to_alive_on_recent_activity() {
        let now = Instant::now();
        let mut peers = HashMap::new();
        let node_id = NodeId::new();
        let mut state = PeerState::new(0, Status::Suspect);
        state.last_seen = now - Duration::from_millis(100);
        state.suspect_since = Some(now - Duration::from_secs(2));
        peers.insert(
            node_id,
            peer(node_id, "127.0.0.1:9002".parse().unwrap(), state),
        );

        let changes = check_timeouts(
            &mut peers,
            now,
            Duration::from_secs(3),
            Duration::from_secs(6),
        );

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].from, Status::Suspect);
        assert_eq!(changes[0].to, Status::Alive);
        let peer_state = &peers.get(&node_id).unwrap().state;
        assert_eq!(peer_state.status, Status::Alive);
        assert!(peer_state.suspect_since.is_none());
    }
}
