use alopex_chirps_core::connectivity::EndpointResolver;
use alopex_chirps_wire::node_id::NodeId;
use quinn::{ClientConfig, Connection, Endpoint};
use rand::{Rng, thread_rng};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tokio::time::{interval, sleep};
use tracing::{info, warn};

use super::{
    DEFAULT_SERVER_NAME, ExtendedTransportMetrics, HandshakeConfig, NegotiatedCapabilities,
    ReceiveHandler, RetransmissionBuffer, TransportCounters, handle_connection,
};

#[derive(Debug)]
pub enum ReconnectCommand {
    Trigger,
}

pub fn start_seed_reconnector(
    endpoint_resolver: Arc<dyn EndpointResolver>,
    endpoint: Endpoint,
    client_config: ClientConfig,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    receive_handler: Arc<ReceiveHandler>,
    peer_capabilities: Arc<RwLock<HashMap<NodeId, NegotiatedCapabilities>>>,
    retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
    metrics_ext: Arc<ExtendedTransportMetrics>,
    shutdown: broadcast::Sender<()>,
    local_id: NodeId,
    metrics: Arc<TransportCounters>,
    handshake_config: HandshakeConfig,
) -> mpsc::Sender<ReconnectCommand> {
    let inflight = Arc::new(Mutex::new(HashSet::new()));
    let (tx, mut rx) = mpsc::channel(8);
    let mut ticker = interval(Duration::from_secs(60));

    tokio::spawn({
        let endpoint_resolver = Arc::clone(&endpoint_resolver);
        let inflight = Arc::clone(&inflight);
        let endpoint = endpoint.clone();
        let client_config = client_config.clone();
        let connections = Arc::clone(&connections);
        let handler = Arc::clone(&receive_handler);
        let peer_capabilities = Arc::clone(&peer_capabilities);
        let retransmit_buffer = Arc::clone(&retransmit_buffer);
        let metrics_ext = Arc::clone(&metrics_ext);
        let shutdown = shutdown.clone();
        let metrics = Arc::clone(&metrics);
        async move {
            let mut shutdown_rx = shutdown.subscribe();
            loop {
                select! {
                    _ = shutdown_rx.recv() => break,
                    _ = ticker.tick() => {
                        launch_attempts(
                            Arc::clone(&endpoint_resolver),
                            endpoint.clone(),
                            client_config.clone(),
                            Arc::clone(&connections),
                            Arc::clone(&handler),
                            Arc::clone(&peer_capabilities),
                            Arc::clone(&retransmit_buffer),
                            Arc::clone(&metrics_ext),
                            shutdown.clone(),
                            local_id,
                            Arc::clone(&metrics),
                            handshake_config.clone(),
                            Arc::clone(&inflight),
                        ).await;
                    }
                    Some(ReconnectCommand::Trigger) = rx.recv() => {
                        launch_attempts(
                            Arc::clone(&endpoint_resolver),
                            endpoint.clone(),
                            client_config.clone(),
                            Arc::clone(&connections),
                            Arc::clone(&handler),
                            Arc::clone(&peer_capabilities),
                            Arc::clone(&retransmit_buffer),
                            Arc::clone(&metrics_ext),
                            shutdown.clone(),
                            local_id,
                            Arc::clone(&metrics),
                            handshake_config.clone(),
                            Arc::clone(&inflight),
                        ).await;
                    }
                    else => break,
                }
            }
        }
    });

    tx
}

async fn launch_attempts(
    endpoint_resolver: Arc<dyn EndpointResolver>,
    endpoint: Endpoint,
    client_config: ClientConfig,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    receive_handler: Arc<ReceiveHandler>,
    peer_capabilities: Arc<RwLock<HashMap<NodeId, NegotiatedCapabilities>>>,
    retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
    metrics_ext: Arc<ExtendedTransportMetrics>,
    shutdown: broadcast::Sender<()>,
    local_id: NodeId,
    metrics: Arc<TransportCounters>,
    handshake_config: HandshakeConfig,
    inflight: Arc<Mutex<HashSet<SocketAddr>>>,
) {
    let now = std::time::SystemTime::now();
    let candidates = endpoint_resolver
        .resolve()
        .into_iter()
        .flat_map(|peer| {
            peer.candidates
                .into_iter()
                .filter(|candidate| candidate.is_current_at(now))
                .map(move |candidate| (peer.node_id, candidate.address))
        })
        .collect::<HashSet<_>>();
    for (expected_remote_id, seed) in candidates {
        if is_connected(&connections, expected_remote_id, &seed).await {
            continue;
        }
        let mut guard = inflight.lock().await;
        if guard.contains(&seed) {
            continue;
        }
        guard.insert(seed);
        drop(guard);

        tokio::spawn(reconnect_seed(
            seed,
            expected_remote_id,
            Arc::clone(&endpoint_resolver),
            endpoint.clone(),
            client_config.clone(),
            Arc::clone(&connections),
            Arc::clone(&receive_handler),
            Arc::clone(&peer_capabilities),
            Arc::clone(&retransmit_buffer),
            Arc::clone(&metrics_ext),
            shutdown.clone(),
            local_id,
            Arc::clone(&metrics),
            handshake_config.clone(),
            Arc::clone(&inflight),
        ));
    }
}

async fn reconnect_seed(
    seed: SocketAddr,
    expected_remote_id: Option<NodeId>,
    endpoint_resolver: Arc<dyn EndpointResolver>,
    endpoint: Endpoint,
    client_config: ClientConfig,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    receive_handler: Arc<ReceiveHandler>,
    peer_capabilities: Arc<RwLock<HashMap<NodeId, NegotiatedCapabilities>>>,
    retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
    metrics_ext: Arc<ExtendedTransportMetrics>,
    shutdown: broadcast::Sender<()>,
    local_id: NodeId,
    metrics: Arc<TransportCounters>,
    handshake_config: HandshakeConfig,
    inflight: Arc<Mutex<HashSet<SocketAddr>>>,
) {
    let mut shutdown_rx = shutdown.subscribe();
    let mut backoff = Duration::from_millis(200);
    let max_backoff = Duration::from_secs(5);

    loop {
        if shutdown_rx.try_recv().is_ok() {
            break;
        }
        if !candidate_is_current(&endpoint_resolver, expected_remote_id, seed) {
            break;
        }
        if is_connected(&connections, expected_remote_id, &seed).await {
            backoff = Duration::from_millis(200);
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        match endpoint.connect_with(client_config.clone(), seed, DEFAULT_SERVER_NAME) {
            Ok(connecting) => match connecting.await {
                Ok(connection) => {
                    info!("connected to seed {seed}");
                    let connections = Arc::clone(&connections);
                    let handler = Arc::clone(&receive_handler);
                    let peer_capabilities = Arc::clone(&peer_capabilities);
                    let retransmit_buffer = Arc::clone(&retransmit_buffer);
                    let metrics_ext = Arc::clone(&metrics_ext);
                    let mut handler_shutdown = shutdown.subscribe();
                    let metrics = Arc::clone(&metrics);
                    let hs_cfg = handshake_config.clone();
                    if let Err(err) = handle_connection(
                        connection,
                        local_id,
                        expected_remote_id,
                        connections,
                        peer_capabilities,
                        handler,
                        retransmit_buffer,
                        metrics_ext,
                        metrics,
                        &mut handler_shutdown,
                        hs_cfg,
                    )
                    .await
                    {
                        warn!("seed connection handler failed: {err}");
                    }
                    backoff = Duration::from_millis(200);
                }
                Err(err) => warn!("connect to seed {seed} failed: {err}"),
            },
            Err(err) => warn!("connect setup to seed {seed} failed: {err}"),
        }

        let jitter = thread_rng().gen_range(0..100);
        sleep(backoff + Duration::from_millis(jitter)).await;
        backoff = (backoff * 2).min(max_backoff);
    }

    let mut guard = inflight.lock().await;
    guard.remove(&seed);
}

async fn is_connected(
    connections: &Arc<RwLock<HashMap<NodeId, Connection>>>,
    expected_remote_id: Option<NodeId>,
    seed: &SocketAddr,
) -> bool {
    let guard = connections.read().await;
    expected_remote_id.is_some_and(|node_id| guard.contains_key(&node_id))
        || guard.values().any(|conn| conn.remote_address() == *seed)
}

fn candidate_is_current(
    endpoint_resolver: &Arc<dyn EndpointResolver>,
    expected_remote_id: Option<NodeId>,
    seed: SocketAddr,
) -> bool {
    let now = std::time::SystemTime::now();
    endpoint_resolver.resolve().into_iter().any(|peer| {
        peer.node_id == expected_remote_id
            && peer
                .candidates
                .into_iter()
                .any(|candidate| candidate.address == seed && candidate.is_current_at(now))
    })
}

#[cfg(test)]
mod tests {
    use super::candidate_is_current;
    use alopex_chirps_core::connectivity::{EndpointCandidate, EndpointResolver, PeerEndpoints};
    use alopex_chirps_wire::node_id::NodeId;
    use std::net::SocketAddr;
    use std::sync::Arc;

    struct Resolver(Vec<PeerEndpoints>);

    impl EndpointResolver for Resolver {
        fn resolve(&self) -> Vec<PeerEndpoints> {
            self.0.clone()
        }
    }

    #[test]
    fn removed_or_reassigned_candidates_are_not_retried() {
        let node_a = NodeId::from([1; 16]);
        let node_b = NodeId::from([2; 16]);
        let address: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let resolver: Arc<dyn EndpointResolver> = Arc::new(Resolver(vec![PeerEndpoints::new(
            Some(node_a),
            vec![EndpointCandidate::static_seed(address)],
        )]));

        assert!(candidate_is_current(&resolver, Some(node_a), address));
        assert!(!candidate_is_current(&resolver, Some(node_b), address));
        assert!(!candidate_is_current(&resolver, None, address));
        assert!(!candidate_is_current(
            &resolver,
            Some(node_a),
            "127.0.0.1:7001".parse().unwrap()
        ));
    }
}
