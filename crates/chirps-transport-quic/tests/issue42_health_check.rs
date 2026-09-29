use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_core::config::NodeConfig;
use alopex_chirps_transport_quic::QuicBackend;
use alopex_chirps_wire::node_id::NodeId;
use rcgen::generate_simple_self_signed;
use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("free address")
        .local_addr()
        .expect("local address")
}

fn node_config(
    dir: &TempDir,
    index: usize,
    bind_addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    certs: &[std::path::PathBuf],
) -> Arc<NodeConfig> {
    let cert = generate_simple_self_signed(["alopex.local".to_owned()]).expect("certificate");
    let cert_path = dir.path().join(format!("{index}.crt"));
    let key_path = dir.path().join(format!("{index}.key"));
    fs::write(&cert_path, cert.serialize_der().expect("certificate der")).expect("write cert");
    fs::write(&key_path, cert.serialize_private_key_der()).expect("write key");
    Arc::new(NodeConfig {
        bind_addr,
        seeds,
        cert_path: Some(cert_path),
        key_path: Some(key_path),
        trusted_cert_paths: certs.to_vec(),
        ..NodeConfig::default()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_check_removes_a_stale_peer_before_the_next_send() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let addr_a = free_addr();
    let addr_b = free_addr();
    let addr_c = free_addr();
    let node_a = NodeId::new();
    let node_b = NodeId::new();
    let node_c = NodeId::new();
    let cert_a = dir.path().join("0.crt");
    let cert_b = dir.path().join("1.crt");
    let cert_c = dir.path().join("2.crt");
    let trusted = [cert_a.clone(), cert_b.clone(), cert_c.clone()];
    let config_b = node_config(&dir, 1, addr_b, vec![addr_a], &trusted);
    let config_c = node_config(&dir, 2, addr_c, vec![addr_a], &trusted);
    let config_a = node_config(&dir, 0, addr_a, vec![], &trusted);
    let backend_a = QuicBackend::new(node_a, config_a).await?;
    let backend_b = QuicBackend::new(node_b, Arc::clone(&config_b)).await?;
    let backend_c = QuicBackend::new(node_c, config_c).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while backend_a.connected_peers().len() < 2 {
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for two connected peers");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    backend_b.close().await?;
    assert!(!backend_a.health_check(node_b).await?);
    assert!(
        backend_a
            .connected_peers()
            .iter()
            .all(|(id, _)| *id != node_b)
    );
    assert!(backend_a.health_check(node_c).await?);

    let mut reconnect_config = (*config_b).clone();
    reconnect_config.bind_addr = free_addr();
    let backend_b2 = QuicBackend::new(node_b, Arc::new(reconnect_config)).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while backend_a
        .connected_peers()
        .iter()
        .all(|(id, _)| *id != node_b)
    {
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for the peer to reconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    backend_b2.close().await?;
    backend_c.close().await?;
    backend_a.close().await?;
    Ok(())
}
