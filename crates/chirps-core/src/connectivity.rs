//! Identity-independent network endpoint candidates.

use alopex_chirps_wire::node_id::NodeId;
use std::net::SocketAddr;
use std::time::SystemTime;

/// Where a network endpoint candidate came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EndpointSource {
    /// A backwards-compatible static `NodeConfig::seeds` entry.
    StaticSeed,
    /// A future authenticated discovery record.
    Discovery,
    /// A future validated NAT traversal result.
    NatTraversal,
    /// A future authenticated relay path.
    Relay,
    /// The active endpoint observed after an authenticated connection.
    Observed,
}

/// A physical location candidate; it is not a node identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointCandidate {
    pub address: SocketAddr,
    pub source: EndpointSource,
    /// `None` is indefinitely valid. Dynamic sources must set an expiry.
    pub expires_at: Option<SystemTime>,
}

impl EndpointCandidate {
    pub fn static_seed(address: SocketAddr) -> Self {
        Self {
            address,
            source: EndpointSource::StaticSeed,
            expires_at: None,
        }
    }

    pub fn observed(address: SocketAddr) -> Self {
        Self {
            address,
            source: EndpointSource::Observed,
            expires_at: None,
        }
    }

    pub fn is_current_at(&self, now: SystemTime) -> bool {
        self.expires_at.is_none_or(|expires_at| expires_at > now)
    }
}

/// Candidate locations for one peer. A bootstrap seed has no known identity
/// until its authenticated QUIC handshake completes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEndpoints {
    pub node_id: Option<NodeId>,
    pub candidates: Vec<EndpointCandidate>,
}

impl PeerEndpoints {
    pub fn new(node_id: Option<NodeId>, candidates: Vec<EndpointCandidate>) -> Self {
        Self {
            node_id,
            candidates,
        }
    }
}

/// Supplies the currently usable locations without making a location an identity.
pub trait EndpointResolver: Send + Sync {
    fn resolve(&self) -> Vec<PeerEndpoints>;
}

/// Resolver for the existing static seed configuration.
#[derive(Debug, Clone, Default)]
pub struct StaticEndpointResolver {
    peers: Vec<PeerEndpoints>,
}

impl StaticEndpointResolver {
    pub fn from_seeds(seeds: impl IntoIterator<Item = SocketAddr>) -> Self {
        Self {
            peers: seeds
                .into_iter()
                .map(|address| {
                    PeerEndpoints::new(None, vec![EndpointCandidate::static_seed(address)])
                })
                .collect(),
        }
    }
}

impl EndpointResolver for StaticEndpointResolver {
    fn resolve(&self) -> Vec<PeerEndpoints> {
        self.peers.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn static_seeds_remain_identity_independent_candidates() {
        let address = "127.0.0.1:7000".parse().unwrap();
        let peers = StaticEndpointResolver::from_seeds([address]).resolve();

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, None);
        assert_eq!(
            peers[0].candidates,
            vec![EndpointCandidate::static_seed(address)]
        );
    }

    #[test]
    fn peer_candidate_freshness_is_preserved() {
        let address = "127.0.0.1:7001".parse().unwrap();
        let alternate = "127.0.0.1:7002".parse().unwrap();
        let now = SystemTime::now();
        let expired = EndpointCandidate {
            address,
            source: EndpointSource::Discovery,
            expires_at: Some(now.checked_sub(Duration::from_secs(1)).unwrap()),
        };
        let peer = PeerEndpoints::new(
            Some(NodeId::from([9; 16])),
            vec![
                expired.clone(),
                EndpointCandidate::static_seed(address),
                EndpointCandidate::static_seed(alternate),
            ],
        );

        assert_eq!(peer.node_id, Some(NodeId::from([9; 16])));
        assert_eq!(peer.candidates.len(), 3);
        assert_eq!(
            peer.candidates[2],
            EndpointCandidate::static_seed(alternate)
        );
        assert!(!expired.is_current_at(now));
    }
}
