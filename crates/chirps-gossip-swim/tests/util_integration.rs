use alopex_chirps_gossip_swim::types::{Peer, PeerState, Status};
use alopex_chirps_gossip_swim::util::{StatusChange, calculate_fanout, check_timeouts};
use alopex_chirps_wire::node_id::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::Instant;

#[test]
fn fanout_respects_defaults_and_caps() {
    assert_eq!(calculate_fanout(1, None), 0);
    assert_eq!(calculate_fanout(5, None), 3); // max(3, ceil(sqrt(5)))
    assert_eq!(calculate_fanout(5, Some(10)), 4); // capped at N-1
    assert_eq!(calculate_fanout(5, Some(2)), 2);
}

#[test]
fn timeouts_produce_status_changes() {
    let now = Instant::now();
    let mut peers = HashMap::new();
    let node = NodeId::new();
    let mut state = PeerState::new(0, Status::Alive);
    state.last_seen = now - Duration::from_millis(400);
    peers.insert(
        node,
        Peer {
            node_id: node,
            addr: "127.0.0.1:9000".parse::<SocketAddr>().unwrap(),
            state,
        },
    );

    let changes = check_timeouts(
        &mut peers,
        now,
        Duration::from_millis(200),
        Duration::from_millis(600),
    );
    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0],
        StatusChange::new(node, Status::Alive, Status::Suspect, 0)
    );
}
