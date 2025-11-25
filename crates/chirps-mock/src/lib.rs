use async_trait::async_trait;
use chirps_core::backend::MessageBackend;
use chirps_core::error::TransportError;
use chirps_wire::frame::Frame;
use chirps_wire::node_id::NodeId;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, RwLock, mpsc};

type SharedPeers = Arc<RwLock<HashMap<NodeId, (SocketAddr, mpsc::Sender<(NodeId, Frame)>)>>>;

/// 単一プロセス内でのメモリ内トランスポートを提供するネットワーク。
pub struct MockNetwork {
    peers: SharedPeers,
}

impl MockNetwork {
    /// 空のネットワークを生成する。
    pub fn new() -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// ノードをネットワークに登録し、対応するトランスポートを返す。
    pub async fn add_node(&self, node_id: NodeId, addr: SocketAddr) -> MockBackend {
        let (tx, rx) = mpsc::channel(1024);
        {
            let mut guard = self.peers.write().await;
            guard.insert(node_id, (addr, tx));
        }
        MockBackend {
            node_id,
            peers: Arc::clone(&self.peers),
            incoming_rx: Mutex::new(Some(rx)),
            closed: AtomicBool::new(false),
        }
    }
}

impl Default for MockNetwork {
    fn default() -> Self {
        Self::new()
    }
}

/// インメモリで `MessageBackend` を提供する実装。
pub struct MockBackend {
    node_id: NodeId,
    peers: SharedPeers,
    incoming_rx: Mutex<Option<mpsc::Receiver<(NodeId, Frame)>>>,
    closed: AtomicBool,
}

#[async_trait]
impl MessageBackend for MockBackend {
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TransportError::Connection(
                "モックバックエンドはクローズ済みです".into(),
            ));
        }
        let sender = {
            let guard = self.peers.read().await;
            guard
                .get(&target)
                .cloned()
                .ok_or_else(|| TransportError::Connection("ターゲットに接続されていません".into()))?
                .1
        };
        sender
            .send((self.node_id, frame))
            .await
            .map_err(|_| TransportError::Send("送信先が閉じています".into()))
    }

    async fn broadcast(&self, frame: Frame) -> Result<usize, TransportError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TransportError::Connection(
                "モックバックエンドはクローズ済みです".into(),
            ));
        }
        let targets: Vec<mpsc::Sender<(NodeId, Frame)>> = {
            let guard = self.peers.read().await;
            guard
                .iter()
                .filter(|(id, _)| **id != self.node_id)
                .map(|(_, (_, tx))| tx.clone())
                .collect()
        };

        let mut sent = 0;
        for tx in targets {
            if tx.send((self.node_id, frame.clone())).await.is_ok() {
                sent += 1;
            }
        }
        Ok(sent)
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        let mut guard = self.incoming_rx.lock().await;
        guard
            .take()
            .ok_or_else(|| TransportError::Subscribe("すでに購読済みです".into()))
    }

    async fn close(&self) -> Result<(), TransportError> {
        self.closed.store(true, Ordering::SeqCst);
        let mut guard = self.peers.write().await;
        guard.remove(&self.node_id);
        Ok(())
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        if let Ok(guard) = self.peers.try_read() {
            guard
                .iter()
                .filter(|(id, _)| **id != self.node_id)
                .map(|(id, (addr, _))| (*id, *addr))
                .collect()
        } else {
            Vec::new()
        }
    }
}

impl MockBackend {
    /// 任意の `SocketAddr` を簡単に得るためのヘルパ。
    pub fn ephemeral_addr() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chirps_wire::frame::UserMessage;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_delivers_to_target() -> anyhow::Result<()> {
        let network = MockNetwork::new();
        let a = NodeId::new();
        let b = NodeId::new();
        let backend_a = network.add_node(a, MockBackend::ephemeral_addr()).await;
        let backend_b = network.add_node(b, MockBackend::ephemeral_addr()).await;

        let mut rx_b = backend_b.subscribe().await?;
        backend_a
            .send(
                b,
                Frame::User(UserMessage {
                    payload: b"hello".to_vec(),
                }),
            )
            .await?;

        let (from, frame) = rx_b.recv().await.expect("メッセージを受信できる");
        assert_eq!(from, a);
        match frame {
            Frame::User(msg) => assert_eq!(msg.payload, b"hello"),
            other => panic!("Userフレームを期待しましたが {:?} を受信", other),
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broadcast_reaches_all_peers_except_self() -> anyhow::Result<()> {
        let network = MockNetwork::new();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();
        let backend_a = network.add_node(a, MockBackend::ephemeral_addr()).await;
        let backend_b = network.add_node(b, MockBackend::ephemeral_addr()).await;
        let backend_c = network.add_node(c, MockBackend::ephemeral_addr()).await;

        let mut rx_b = backend_b.subscribe().await?;
        let mut rx_c = backend_c.subscribe().await?;

        let sent = backend_a.broadcast(Frame::Ping { seq: 1, from: a }).await?;
        assert_eq!(sent, 2);

        let (from_b, frame_b) = rx_b.recv().await.expect("Bが受信できる");
        let (from_c, frame_c) = rx_c.recv().await.expect("Cが受信できる");
        assert_eq!(from_b, a);
        assert_eq!(from_c, a);
        matches!(frame_b, Frame::Ping { .. });
        matches!(frame_c, Frame::Ping { .. });
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_removes_peer_and_blocks_send() -> anyhow::Result<()> {
        let network = MockNetwork::new();
        let a = NodeId::new();
        let b = NodeId::new();
        let backend_a = network.add_node(a, MockBackend::ephemeral_addr()).await;
        let backend_b = network.add_node(b, MockBackend::ephemeral_addr()).await;

        let mut rx_b = backend_b.subscribe().await?;
        backend_b.close().await?;

        let result = backend_a
            .send(
                b,
                Frame::User(UserMessage {
                    payload: b"ping".to_vec(),
                }),
            )
            .await;
        assert!(result.is_err());
        assert!(rx_b.recv().await.is_none());
        Ok(())
    }
}
