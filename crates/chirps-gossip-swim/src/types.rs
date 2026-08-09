use alopex_chirps_wire::frame::MemberStatus;
use alopex_chirps_wire::node_id::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::time::Instant;

/// The status of a peer.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Alive,
    Suspect,
    Dead,
}

impl From<MemberStatus> for Status {
    fn from(status: MemberStatus) -> Self {
        match status {
            MemberStatus::Alive => Status::Alive,
            MemberStatus::Suspect => Status::Suspect,
            MemberStatus::Dead => Status::Dead,
        }
    }
}

/// Represents a peer in the cluster.
#[derive(Debug, Clone)]
pub struct Peer {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub state: PeerState,
}

/// The state of a peer.
#[derive(Debug, Clone)]
pub struct PeerState {
    pub incarnation: u64,
    pub status: Status,
    pub last_seen: Instant,
    pub suspect_since: Option<Instant>,
    /// Identity of the last membership event applied for this peer.
    #[cfg(feature = "hlc")]
    pub event_id: Option<alopex_chirps_wire::frame::HlcEventId>,
    /// Causal timestamp of the last membership event applied for this peer.
    #[cfg(feature = "hlc")]
    pub timestamp: Option<alopex_chirps_wire::hlc::HybridTimestamp>,
}

impl PeerState {
    pub fn new(incarnation: u64, status: Status) -> Self {
        Self {
            incarnation,
            status,
            last_seen: Instant::now(),
            suspect_since: None,
            #[cfg(feature = "hlc")]
            event_id: None,
            #[cfg(feature = "hlc")]
            timestamp: None,
        }
    }
}

/// A view of the membership of the cluster.
#[derive(Debug, Clone)]
pub struct MembershipView {
    pub peers: HashMap<NodeId, Peer>,
}

impl MembershipView {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }
}

impl Default for MembershipView {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_state_creation() {
        let peer_state = PeerState::new(1, Status::Alive);
        assert_eq!(peer_state.incarnation, 1);
        assert_eq!(peer_state.status, Status::Alive);
        assert!(peer_state.last_seen.elapsed().as_secs() < 1);
        assert!(peer_state.suspect_since.is_none());
    }
}
