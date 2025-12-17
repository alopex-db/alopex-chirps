#![allow(clippy::field_reassign_with_default)]
use chirps_core::backend::MessageBackend;
use chirps_core::config::NodeConfig;
use chirps_core::error::TransportError;
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
#[ignore = "QUIC integration test - requires network, run manually with --ignored"]
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
#[ignore = "QUIC integration test - requires network, run manually with --ignored"]
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
#[ignore = "QUIC integration test - requires network, run manually with --ignored"]
async fn gossip_not_blocked_by_large_user_stream() -> anyhow::Result<()> {
    let node_a = NodeId::new();
    let node_b = NodeId::new();

    let addr_a = match free_addr() {
        Ok(a) => a,
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skip gossip_not_blocked_by_large_user_stream: cannot bind socket ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    let addr_b = match free_addr() {
        Ok(a) => a,
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skip gossip_not_blocked_by_large_user_stream: cannot bind socket ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let backend_a = match QuicBackend::new(node_a, config(addr_a, vec![])).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                eprintln!("skip gossip_not_blocked_by_large_user_stream: network denied ({msg})");
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
                eprintln!("skip gossip_not_blocked_by_large_user_stream: network denied ({msg})");
                return Ok(());
            }
            return Err(e);
        }
    };

    let mut rx_a = backend_a.subscribe().await?;
    let mut _rx_b = backend_b.subscribe().await?;

    wait_for_connected(&backend_a, 1).await;
    wait_for_connected(&backend_b, 1).await;

    let payload = vec![1u8; 60_000];
    let send_user = backend_b.send(
        node_a,
        Frame::User(UserMessage {
            payload: payload.clone(),
        }),
    );
    let send_ping = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        backend_b
            .send(
                node_a,
                Frame::Ping {
                    seq: 99,
                    from: node_b,
                },
            )
            .await
    };
    let (user_res, ping_res) = tokio::join!(send_user, send_ping);
    user_res?;
    ping_res?;

    let mut order = Vec::new();
    for _ in 0..2 {
        let (from, frame) = tokio::time::timeout(Duration::from_secs(2), rx_a.recv())
            .await?
            .expect("expected frame");
        assert_eq!(from, node_b);
        match frame {
            Frame::Ping { seq, from } => {
                assert_eq!(seq, 99);
                assert_eq!(from, node_b);
                order.push("ping");
            }
            Frame::User(user) => {
                assert_eq!(user.payload, payload);
                order.push("user");
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }

    assert_eq!(
        order[0], "ping",
        "gossip/control frames should not be blocked behind large user streams"
    );
    assert_eq!(order[1], "user");

    backend_a.close().await?;
    backend_b.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "QUIC integration test - requires network, run manually with --ignored"]
async fn send_queue_overflow_returns_error() -> anyhow::Result<()> {
    let node_a = NodeId::new();
    let node_b = NodeId::new();

    let addr_a = match free_addr() {
        Ok(a) => a,
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skip send_queue_overflow_returns_error: cannot bind socket ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    let addr_b = match free_addr() {
        Ok(a) => a,
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skip send_queue_overflow_returns_error: cannot bind socket ({e})");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let mut cfg_a = NodeConfig::default();
    cfg_a.bind_addr = addr_a;
    cfg_a.send_queue_capacity = 1;
    let backend_a = match QuicBackend::new(node_a, Arc::new(cfg_a)).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                eprintln!("skip send_queue_overflow_returns_error: network denied ({msg})");
                return Ok(());
            }
            return Err(e);
        }
    };

    let mut cfg_b = NodeConfig::default();
    cfg_b.bind_addr = addr_b;
    cfg_b.seeds = vec![addr_a];
    cfg_b.send_queue_capacity = 1;
    let backend_b = match QuicBackend::new(node_b, Arc::new(cfg_b)).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                eprintln!("skip send_queue_overflow_returns_error: network denied ({msg})");
                return Ok(());
            }
            return Err(e);
        }
    };

    let backend_a = Arc::new(backend_a);
    let backend_b = Arc::new(backend_b);

    let mut _rx_a = backend_a.subscribe().await?;
    let mut _rx_b = backend_b.subscribe().await?;

    wait_for_connected(&backend_a, 1).await;
    wait_for_connected(&backend_b, 1).await;

    let t1 = {
        let backend = Arc::clone(&backend_b);
        tokio::spawn(async move {
            backend
                .send(
                    node_a,
                    Frame::User(UserMessage {
                        payload: b"msg1".to_vec(),
                    }),
                )
                .await
        })
    };
    let t2 = {
        let backend = Arc::clone(&backend_b);
        tokio::spawn(async move {
            backend
                .send(
                    node_a,
                    Frame::User(UserMessage {
                        payload: b"msg2".to_vec(),
                    }),
                )
                .await
        })
    };
    let t3 = {
        let backend = Arc::clone(&backend_b);
        tokio::spawn(async move {
            backend
                .send(
                    node_a,
                    Frame::User(UserMessage {
                        payload: b"msg3".to_vec(),
                    }),
                )
                .await
        })
    };

    let results = vec![t1.await?, t2.await?, t3.await?];
    let overflow_errors = results
        .into_iter()
        .filter_map(|res| match res {
            Ok(_) => None,
            Err(TransportError::Timeout(msg)) if msg.contains("queue") => Some(()),
            Err(e) => panic!("unexpected send error: {e:?}"),
        })
        .count();
    assert!(
        overflow_errors >= 1,
        "at least one send should fail when the queue is full"
    );
    let metrics = backend_b.metrics();
    assert!(
        metrics.dropped >= 1,
        "transport metrics should record dropped sends"
    );

    backend_a.close().await?;
    backend_b.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "QUIC integration test - requires network, run manually with --ignored"]
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
