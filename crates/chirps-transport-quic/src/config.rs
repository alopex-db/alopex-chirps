use std::time::Duration;

/// Chirps v0.4 向けのトランスポート設定集。
#[derive(Clone, Debug)]
pub struct TransportConfigV04 {
    /// 送信処理のタイムアウト。
    pub send_timeout: Duration,
    /// 送信キューのバッファサイズ。
    pub send_queue_capacity: usize,
    /// 優先度スケジューラ設定。
    pub priority: PriorityConfig,
    /// 再送バッファ設定。
    pub retransmit: RetransmitConfig,
    /// QoS/バックプレッシャー設定。
    pub qos: QosConfig,
    /// ハンドシェイク動作設定。
    pub handshake: HandshakeConfig,
}

impl Default for TransportConfigV04 {
    fn default() -> Self {
        Self {
            send_timeout: Duration::from_millis(200),
            send_queue_capacity: 1024,
            priority: PriorityConfig::default(),
            retransmit: RetransmitConfig::default(),
            qos: QosConfig::default(),
            handshake: HandshakeConfig::default(),
        }
    }
}

/// 優先度制御の基本パラメータ。
#[derive(Clone, Debug)]
pub struct PriorityConfig {
    pub enabled: bool,
    pub weights: [u32; 3],
}

impl Default for PriorityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            weights: [4, 2, 1],
        }
    }
}

/// 再送バッファの容量制御設定。
#[derive(Debug, Clone)]
pub struct RetransmitConfig {
    pub max_buffer_bytes: usize,
    pub max_messages_per_peer: usize,
    pub message_ttl: Duration,
}

impl Default for RetransmitConfig {
    fn default() -> Self {
        Self {
            max_buffer_bytes: 32 * 1024 * 1024,
            max_messages_per_peer: 10_000,
            message_ttl: Duration::from_secs(60),
        }
    }
}

/// QoS とバックプレッシャーの設定。
#[derive(Clone, Debug)]
pub struct QosConfig {
    pub enabled: bool,
    pub bandwidth: BandwidthConfig,
    pub queue_limits: QueueLimits,
}

impl Default for QosConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bandwidth: BandwidthConfig::default(),
            queue_limits: QueueLimits::default(),
        }
    }
}

/// ストリーム種別ごとのキュー上限。
#[derive(Clone, Debug)]
pub struct QueueLimits {
    pub raft_max_bytes: usize,
    pub raft_max_items: usize,
    pub user_max_bytes: usize,
    pub user_max_items: usize,
    pub gossip_max_bytes: usize,
    pub gossip_max_items: usize,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            raft_max_bytes: 16 * 1024 * 1024,
            raft_max_items: 10_000,
            user_max_bytes: 64 * 1024 * 1024,
            user_max_items: 50_000,
            gossip_max_bytes: 8 * 1024 * 1024,
            gossip_max_items: 5_000,
        }
    }
}

/// 帯域・スロットルの設定。
#[derive(Clone, Debug)]
pub struct BandwidthConfig {
    pub raft_ratio: f32,
    pub user_ratio: f32,
    pub gossip_ratio: f32,
    pub total_bandwidth: Option<u64>,
    pub snapshot_bandwidth_limit: u64,
    pub throttle_timeout: Duration,
}

impl Default for BandwidthConfig {
    fn default() -> Self {
        Self {
            raft_ratio: 0.40,
            user_ratio: 0.50,
            gossip_ratio: 0.10,
            total_bandwidth: None,
            snapshot_bandwidth_limit: 50 * 1024 * 1024,
            throttle_timeout: Duration::from_secs(5),
        }
    }
}

/// ハンドシェイクのタイムアウト・互換性チェック設定。
#[derive(Clone, Debug)]
pub struct HandshakeConfig {
    pub timeout: Duration,
}

impl Default for HandshakeConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
        }
    }
}
