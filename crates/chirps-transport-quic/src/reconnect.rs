use alopex_chirps_wire::node_id::NodeId;
use quinn::{ClientConfig, Connection, Endpoint};
use rand::{Rng, thread_rng};
use rustls::ClientConfig as RustlsClientConfig;
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
    seeds: Vec<SocketAddr>,
    endpoint: Endpoint,
    client_config: Arc<RustlsClientConfig>,
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
    let seeds = Arc::new(seeds);
    let inflight = Arc::new(Mutex::new(HashSet::new()));
    let (tx, mut rx) = mpsc::channel(8);
    let mut ticker = interval(Duration::from_secs(60));

    tokio::spawn({
        let seeds = Arc::clone(&seeds);
        let inflight = Arc::clone(&inflight);
        let endpoint = endpoint.clone();
        let client_config = Arc::clone(&client_config);
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
                            Arc::clone(&seeds),
                            endpoint.clone(),
                            Arc::clone(&client_config),
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
                            Arc::clone(&seeds),
                            endpoint.clone(),
                            Arc::clone(&client_config),
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
    seeds: Arc<Vec<SocketAddr>>,
    endpoint: Endpoint,
    client_config: Arc<RustlsClientConfig>,
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
    for seed in seeds.iter().copied() {
        if is_connected(&connections, &seed).await {
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
            endpoint.clone(),
            Arc::clone(&client_config),
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
    endpoint: Endpoint,
    client_config: Arc<RustlsClientConfig>,
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
        if is_connected(&connections, &seed).await {
            backoff = Duration::from_millis(200);
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        match endpoint.connect_with(
            ClientConfig::new(client_config.clone()),
            seed,
            DEFAULT_SERVER_NAME,
        ) {
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
    seed: &SocketAddr,
) -> bool {
    let guard = connections.read().await;
    guard.values().any(|conn| conn.remote_address() == *seed)
}
