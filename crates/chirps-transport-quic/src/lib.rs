use chirps_core::backend::MessageBackend;
use chirps_core::config::NodeConfig;
use chirps_core::error::TransportError;
use chirps_wire::frame::Frame;
use chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use bincode::{deserialize, serialize};
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, ServerConfig};
use rand::{thread_rng, Rng};
use rcgen::generate_simple_self_signed;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tokio::time::sleep;
use tracing::{info, warn};
use rustls::{Certificate, ClientConfig as RustlsClientConfig, PrivateKey, RootCertStore};

const DEFAULT_SERVER_NAME: &str = "alopex.local";
const MAX_FRAME_SIZE: usize = 64 * 1024;

#[derive(Serialize, Deserialize)]
enum WireMessage {
    Handshake(NodeId),
    Frame(FrameEnvelope),
}

#[derive(Serialize, Deserialize)]
struct FrameEnvelope {
    from: NodeId,
    frame: Frame,
}

pub struct QuicBackend {
    node_id: NodeId,
    endpoint: Endpoint,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    incoming_tx: mpsc::Sender<(NodeId, Frame)>,
    incoming_rx: Arc<Mutex<Option<mpsc::Receiver<(NodeId, Frame)>>>>,
    shutdown: broadcast::Sender<()>,
}

impl QuicBackend {
    pub async fn new(node_id: NodeId, config: Arc<NodeConfig>) -> anyhow::Result<Self> {
        let (server_config, client_config) = build_tls_configs(&config)?;
        let mut endpoint = Endpoint::server(server_config, config.bind_addr)?;
        endpoint.set_default_client_config(ClientConfig::new(client_config.clone()));

        let (incoming_tx, incoming_rx) = mpsc::channel(1024);
        let (shutdown, _) = broadcast::channel(4);
        let backend = QuicBackend {
            node_id,
            endpoint: endpoint.clone(),
            connections: Arc::new(RwLock::new(HashMap::new())),
            incoming_tx,
            incoming_rx: Arc::new(Mutex::new(Some(incoming_rx))),
            shutdown,
        };

        backend.spawn_accept_loop();

        for seed in config.seeds.iter().cloned() {
            backend.spawn_seed_connector(seed, client_config.clone());
        }

        Ok(backend)
    }

    fn spawn_accept_loop(&self) {
        let endpoint = self.endpoint.clone();
        let connections = Arc::clone(&self.connections);
        let incoming_tx = self.incoming_tx.clone();
        let mut shutdown_rx = self.shutdown.subscribe();
        let local_id = self.node_id;

        tokio::spawn(async move {
            loop {
                select! {
                    _ = shutdown_rx.recv() => break,
                    incoming = endpoint.accept() => {
                        match incoming {
                            Some(connecting) => {
                                match connecting.await {
                                    Ok(connection) => {
                                        let connections = Arc::clone(&connections);
                                        let incoming_tx = incoming_tx.clone();
                                        let mut shutdown_rx = shutdown_rx.resubscribe();
                                        tokio::spawn(async move {
                                            if let Err(err) = handle_connection(connection, local_id, connections, incoming_tx, &mut shutdown_rx).await {
                                                warn!("connection handler failed: {err}");
                                            }
                                        });
                                    }
                                    Err(err) => warn!("failed to accept connection: {err}"),
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });
    }

    fn spawn_seed_connector(&self, addr: SocketAddr, client_config: Arc<RustlsClientConfig>) {
        let endpoint = self.endpoint.clone();
        let connections = Arc::clone(&self.connections);
        let incoming_tx = self.incoming_tx.clone();
        let mut shutdown_rx = self.shutdown.subscribe();
        let local_id = self.node_id;

        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(200);
            let max_backoff = Duration::from_secs(5);

            loop {
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }
                match endpoint.connect_with(ClientConfig::new(client_config.clone()), addr, DEFAULT_SERVER_NAME) {
                    Ok(connecting) => match connecting.await {
                        Ok(connection) => {
                            info!("connected to seed {addr}");
                            let connections = Arc::clone(&connections);
                            let incoming_tx = incoming_tx.clone();
                            let mut shutdown_rx = shutdown_rx.resubscribe();
                            tokio::spawn(async move {
                                if let Err(err) = handle_connection(connection, local_id, connections, incoming_tx, &mut shutdown_rx).await {
                                    warn!("seed connection handler failed: {err}");
                                }
                            });
                            break;
                        }
                        Err(err) => warn!("connect to seed {addr} failed: {err}"),
                    },
                    Err(err) => warn!("connect setup to seed {addr} failed: {err}"),
                }
                let jitter = thread_rng().gen_range(0..100);
                sleep(backoff + Duration::from_millis(jitter)).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        });
    }
}

#[async_trait]
impl MessageBackend for QuicBackend {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError> {
        let conn = {
            let map = self.connections.read().await;
            map.get(&target)
                .cloned()
                .ok_or_else(|| TransportError::Connection(format!("peer {target:?} not connected")))?            
        };
        send_frame(&conn, &self.node_id, frame).await
    }

    async fn broadcast(&self, frame: Frame) -> Result<usize, TransportError> {
        let peers: Vec<Connection> = {
            let map = self.connections.read().await;
            map.values().cloned().collect()
        };
        let mut sent = 0;
        for conn in peers {
            if let Err(err) = send_frame(&conn, &self.node_id, frame.clone()).await {
                warn!("broadcast send failed: {err}");
            } else {
                sent += 1;
            }
        }
        Ok(sent)
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        let mut guard = self.incoming_rx.lock().await;
        guard
            .take()
            .ok_or_else(|| TransportError::Subscribe("already subscribed".into()))
    }

    async fn close(&self) -> Result<(), TransportError> {
        let _ = self.shutdown.send(());
        self.endpoint.close(0u32.into(), b"shutdown");
        Ok(())
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        if let Ok(map) = self.connections.try_read() {
            map.iter()
                .map(|(id, conn)| (*id, conn.remote_address()))
                .collect()
        } else {
            Vec::new()
        }
    }
}

async fn handle_connection(
    connection: Connection,
    local_id: NodeId,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    incoming_tx: mpsc::Sender<(NodeId, Frame)>,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<(), TransportError> {
    send_handshake(&connection, local_id).await?;
    let remote_id = recv_handshake(&connection).await?;

    connections.write().await.insert(remote_id, connection.clone());

    loop {
        select! {
            _ = shutdown_rx.recv() => {
                connections.write().await.remove(&remote_id);
                break;
            }
            next = connection.accept_uni() => match next {
                Ok(mut recv) => {
                    match read_wire_message(&mut recv).await {
                        Ok(WireMessage::Frame(env)) => {
                            let _ = incoming_tx.send((env.from, env.frame)).await;
                        }
                        Ok(WireMessage::Handshake(id)) => {
                            connections.write().await.insert(id, connection.clone());
                        }
                        Err(err) => warn!("failed to read stream: {err}"),
                    }
                }
                Err(err) => {
                    connections.write().await.remove(&remote_id);
                    return Err(TransportError::Connection(err.to_string()));
                }
            },
        }
    }

    Ok(())
}

async fn send_handshake(connection: &Connection, node_id: NodeId) -> Result<(), TransportError> {
    send_wire_message(connection, WireMessage::Handshake(node_id)).await
}

async fn recv_handshake(connection: &Connection) -> Result<NodeId, TransportError> {
    match connection.accept_uni().await {
        Ok(mut recv) => match read_wire_message(&mut recv).await? {
            WireMessage::Handshake(node_id) => Ok(node_id),
            _ => Err(TransportError::Connection("unexpected message during handshake".into())),
        },
        Err(err) => Err(TransportError::Connection(err.to_string())),
    }
}

async fn send_frame(
    connection: &Connection,
    from: &NodeId,
    frame: Frame,
) -> Result<(), TransportError> {
    let env = WireMessage::Frame(FrameEnvelope {
        from: *from,
        frame,
    });
    send_wire_message(connection, env).await
}

async fn send_wire_message(connection: &Connection, msg: WireMessage) -> Result<(), TransportError> {
    let bytes = serialize(&msg).map_err(|e| TransportError::Send(e.to_string()))?;
    let mut stream = connection
        .open_uni()
        .await
        .map_err(|e| TransportError::Send(e.to_string()))?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| TransportError::Send(e.to_string()))?;
    stream
        .finish()
        .await
        .map_err(|e| TransportError::Send(e.to_string()))
}

async fn read_wire_message(recv: &mut RecvStream) -> Result<WireMessage, TransportError> {
    let bytes = recv
        .read_to_end(MAX_FRAME_SIZE)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    deserialize(&bytes).map_err(|e| TransportError::Io(e.to_string()))
}

fn build_tls_configs(
    config: &NodeConfig,
) -> anyhow::Result<(ServerConfig, Arc<RustlsClientConfig>)> {
    let (cert_der, key_der) = if let (Some(cert_path), Some(key_path)) =
        (config.cert_path.as_ref(), config.key_path.as_ref())
    {
        (fs::read(cert_path)?, fs::read(key_path)?)
    } else {
        let cert = generate_simple_self_signed([DEFAULT_SERVER_NAME.to_string()])?;
        (cert.serialize_der()?, cert.serialize_private_key_der())
    };

    let cert_chain = vec![Certificate(cert_der.clone())];
    let priv_key = PrivateKey(key_der);
    let server_config = ServerConfig::with_single_cert(cert_chain.clone(), priv_key)?;

    let mut roots = RootCertStore::empty();
    roots.add(&Certificate(cert_der)).map_err(|_| anyhow::anyhow!("failed to add root cert"))?;

    let mut client_crypto = RustlsClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"alopex".to_vec()];

    Ok((server_config, Arc::new(client_crypto)))
}
