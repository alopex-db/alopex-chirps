use chirps_core::backend::MessageBackend;
use chirps_core::config::NodeConfig;
use chirps_transport_quic::QuicBackend;
use chirps_wire::frame::{Frame, UserMessage};
use chirps_wire::node_id::NodeId;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

fn free_addr() -> io::Result<SocketAddr> {
    TcpListener::bind("127.0.0.1:0").map(|l| l.local_addr().unwrap())
}

fn config(bind_addr: SocketAddr, seeds: Vec<SocketAddr>) -> Arc<NodeConfig> {
    let mut cfg = NodeConfig::default();
    cfg.bind_addr = bind_addr;
    cfg.seeds = seeds;
    Arc::new(cfg)
}

async fn wait_for_connected(backend: &QuicBackend, expected: usize) {
    wait_for_connected_with_timeout(backend, expected, Duration::from_secs(3)).await;
}

async fn wait_for_connected_with_timeout(
    backend: &QuicBackend,
    expected: usize,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if backend.connected_peers().len() >= expected {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timed out waiting for peers");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ping_ack_roundtrip() -> anyhow::Result<()> {
    let node_a = NodeId::new();
    let node_b = NodeId::new();

    let addr_a = match free_addr() {
        Ok(a) => a,
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skip ping_ack_roundtrip: cannot bind socket ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    let addr_b = match free_addr() {
        Ok(a) => a,
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skip ping_ack_roundtrip: cannot bind socket ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let backend_a = match QuicBackend::new(node_a, config(addr_a, vec![])).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                eprintln!("skip ping_ack_roundtrip: network denied ({msg})");
                return Ok(());
            }
            return Err(e);
        }
    };
    let backend_b = match QuicBackend::new(node_b, config(addr_b, vec![addr_a])).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                eprintln!("skip ping_ack_roundtrip: network denied ({msg})");
                return Ok(());
            }
            return Err(e);
        }
    };

    let mut rx_a = backend_a.subscribe().await?;
    let mut rx_b = backend_b.subscribe().await?;

    wait_for_connected(&backend_a, 1).await;
    wait_for_connected(&backend_b, 1).await;

    backend_a
        .send(
            node_b,
            Frame::Ping {
                seq: 1,
                from: node_a,
            },
        )
        .await?;

    let (from_a, frame_a) = tokio::time::timeout(Duration::from_secs(1), rx_b.recv())
        .await?
        .expect("peer B should receive ping");
    assert_eq!(from_a, node_a);
    let seq = match frame_a {
        Frame::Ping { seq, from } => {
            assert_eq!(from, node_a);
            seq
        }
        other => panic!("expected Ping, got {other:?}"),
    };

    backend_b
        .send(node_a, Frame::Ack { seq, from: node_b })
        .await?;

    let (from_b, frame_b) = tokio::time::timeout(Duration::from_secs(1), rx_a.recv())
        .await?
        .expect("peer A should receive ack");
    assert_eq!(from_b, node_b);
    match frame_b {
        Frame::Ack { seq: ack_seq, from } => {
            assert_eq!(from, node_b);
            assert_eq!(ack_seq, seq);
        }
        other => panic!("expected Ack, got {other:?}"),
    };

    backend_a.close().await?;
    backend_b.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_delivers_to_connected_peers() -> anyhow::Result<()> {
    let node_a = NodeId::new();
    let node_b = NodeId::new();

    let addr_a = match free_addr() {
        Ok(a) => a,
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skip broadcast_delivers_to_connected_peers: cannot bind socket ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    let addr_b = match free_addr() {
        Ok(a) => a,
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skip broadcast_delivers_to_connected_peers: cannot bind socket ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let backend_a = match QuicBackend::new(node_a, config(addr_a, vec![])).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                eprintln!("skip broadcast_delivers_to_connected_peers: network denied ({msg})");
                return Ok(());
            }
            return Err(e);
        }
    };
    let backend_b = match QuicBackend::new(node_b, config(addr_b, vec![addr_a])).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                eprintln!("skip broadcast_delivers_to_connected_peers: network denied ({msg})");
                return Ok(());
            }
            return Err(e);
        }
    };

    let mut rx_a = backend_a.subscribe().await?;
    let _rx_b = backend_b.subscribe().await?;

    wait_for_connected(&backend_a, 1).await;
    wait_for_connected(&backend_b, 1).await;

    let sent = backend_b
        .broadcast(Frame::User(UserMessage {
            payload: b"hello".to_vec(),
        }))
        .await?;
    assert_eq!(sent, 1, "should broadcast to one connected peer");

    let (from, frame) = tokio::time::timeout(Duration::from_secs(1), rx_a.recv())
        .await?
        .expect("receiver should get broadcast frame");
    assert_eq!(from, node_b);
    match frame {
        Frame::User(user) => assert_eq!(user.payload, b"hello"),
        other => panic!("expected User frame, got {other:?}"),
    }

    backend_a.close().await?;
    backend_b.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnects_when_seed_becomes_available() -> anyhow::Result<()> {
    let node_a = NodeId::new();
    let node_b = NodeId::new();

    let addr_a = match free_addr() {
        Ok(a) => a,
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skip reconnects_when_seed_becomes_available: cannot bind socket ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    let addr_b = match free_addr() {
        Ok(a) => a,
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skip reconnects_when_seed_becomes_available: cannot bind socket ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let backend_b = match QuicBackend::new(node_b, config(addr_b, vec![addr_a])).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                eprintln!("skip reconnects_when_seed_becomes_available: network denied ({msg})");
                return Ok(());
            }
            return Err(e);
        }
    };

    tokio::time::sleep(Duration::from_millis(300)).await;

    let backend_a = match QuicBackend::new(node_a, config(addr_a, vec![])).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                eprintln!("skip reconnects_when_seed_becomes_available: network denied ({msg})");
                return Ok(());
            }
            return Err(e);
        }
    };

    let mut rx_a = backend_a.subscribe().await?;
    let mut _rx_b = backend_b.subscribe().await?;

    backend_b.reconnect_to_seeds().await?;

    wait_for_connected_with_timeout(&backend_a, 1, Duration::from_secs(10)).await;
    wait_for_connected_with_timeout(&backend_b, 1, Duration::from_secs(10)).await;

    backend_b
        .send(
            node_a,
            Frame::Ping {
                seq: 42,
                from: node_b,
            },
        )
        .await?;

    let (from, frame) = tokio::time::timeout(Duration::from_secs(2), rx_a.recv())
        .await?
        .expect("peer A should receive ping after reconnect");
    assert_eq!(from, node_b);
    match frame {
        Frame::Ping { seq, from } => {
            assert_eq!(seq, 42);
            assert_eq!(from, node_b);
        }
        other => panic!("expected Ping, got {other:?}"),
    }

    backend_a.close().await?;
    backend_b.close().await?;
    Ok(())
}
