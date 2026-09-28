#![cfg(feature = "tso")]

use alopex_chirps::multi_raft::{MultiRaftManager, WalRaftStorageFactory};
use alopex_chirps::tso::{Clock, TSO_GROUP_ID, TimestampOracle, TsoConfig, TsoStateMachine};
use alopex_chirps::{ChirpsRaftTransport, RaftConfig};
use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_mock::{MockBackend, MockNetwork};
use alopex_chirps_raft_storage::wal_storage::WalStorageConfig;
use alopex_chirps_wire::node_id::NodeId;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::{sleep, timeout};

pub type TsoManager = MultiRaftManager<WalRaftStorageFactory<TsoStateMachine>>;

#[derive(Default)]
pub struct ManualClock {
    physical: AtomicU64,
    lease: AtomicU64,
}

impl ManualClock {
    pub fn new(millis: u64) -> Self {
        Self {
            physical: AtomicU64::new(millis),
            lease: AtomicU64::new(millis),
        }
    }

    pub fn set(&self, millis: u64) {
        self.physical.store(millis, Ordering::Release);
    }
}

impl Clock for ManualClock {
    fn physical_millis(&self) -> u64 {
        self.physical.load(Ordering::Acquire)
    }

    fn lease_millis(&self) -> u64 {
        self.lease.load(Ordering::Acquire)
    }
}

pub fn wire_node_id(node_id: u64) -> NodeId {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&node_id.to_be_bytes());
    NodeId::from(bytes)
}

pub async fn manager(root: &std::path::Path, node_id: u64) -> Arc<TsoManager> {
    let factory = Arc::new(WalRaftStorageFactory::new(
        WalStorageConfig {
            wal_dir: root.join("wal"),
            snapshot_dir: root.join("snapshot"),
            ..WalStorageConfig::default()
        },
        node_id,
    ));
    let network = MockNetwork::new();
    let backend = network
        .add_node(wire_node_id(node_id), MockBackend::ephemeral_addr())
        .await;
    let backend: Arc<dyn MessageBackend> = Arc::new(backend);
    let transport = Arc::new(ChirpsRaftTransport::new(backend, TSO_GROUP_ID, node_id));
    Arc::new(MultiRaftManager::new(
        transport,
        factory,
        RaftConfig {
            node_id,
            election_timeout_ms: 80,
            heartbeat_interval_ms: 20,
            ..RaftConfig::default()
        },
    ))
}

pub async fn leader_oracle(
    manager: &Arc<TsoManager>,
    state_machine: TsoStateMachine,
    clock: Arc<ManualClock>,
) -> TimestampOracle {
    manager
        .create_group(TSO_GROUP_ID, BTreeSet::from([1]), state_machine)
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            let _ = manager.tick_all().await;
            if manager
                .get_group(TSO_GROUP_ID)
                .unwrap()
                .metrics()
                .current_leader
                == Some(1)
            {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("single-voter TSO group must elect its leader");

    let injected_clock: Arc<dyn Clock> = clock;
    TimestampOracle::new(
        1,
        manager.get_group(TSO_GROUP_ID).unwrap(),
        injected_clock,
        TsoConfig {
            timestamp_ttl: Duration::from_millis(1_000),
        },
    )
    .unwrap()
}
