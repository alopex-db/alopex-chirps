use std::sync::Arc;
use std::time::Duration;

/// Flow-control values used by the v0.5.2 File Transfer performance profile.
///
/// Quinn's defaults target a 100 Mbps / 100 ms path.  The release profile was
/// measured with these larger windows and 256 concurrent unidirectional
/// streams, so they are also the safe production defaults for Chirps.
pub const FILE_TRANSFER_STREAM_RECEIVE_WINDOW_BYTES: u64 = 16 * 1024 * 1024;
pub const FILE_TRANSFER_CONNECTION_RECEIVE_WINDOW_BYTES: u64 = 64 * 1024 * 1024;
pub const FILE_TRANSFER_SEND_WINDOW_BYTES: u64 = 64 * 1024 * 1024;
pub const FILE_TRANSFER_MAX_CONCURRENT_UNI_STREAMS: u32 = 256;
pub const DEFAULT_MAX_CONNECTIONS: usize = 64;

/// Chirps v0.4 向けのトランスポート設定集。
#[derive(Clone, Debug)]
pub struct TransportConfigV04 {
    /// Maximum bytes the peer may send without acknowledgement on one stream.
    pub stream_receive_window: u64,
    /// Maximum bytes the peer may send without acknowledgement on one connection.
    pub receive_window: u64,
    /// Maximum bytes Chirps may send without acknowledgement.
    pub send_window: u64,
    /// Maximum number of concurrently open peer-initiated unidirectional streams.
    pub max_concurrent_uni_streams: u32,
    /// QUIC idle timeout.  This is also the threshold used to evict idle peers.
    pub max_idle_timeout: Duration,
    /// Optional QUIC keep-alive interval.  It must be shorter than the idle timeout.
    pub keep_alive_interval: Option<Duration>,
    /// Maximum number of established peer connections retained by this backend.
    pub max_connections: usize,
    /// 送信処理のタイムアウト。
    pub send_timeout: Duration,
    /// Whether data sends wait for peer-side stream stop notifications.
    /// Disable only for high-fanout Raft workloads with envelope retransmit.
    pub await_peer_stop: bool,
    /// Enables per-stream histograms, metrics recorder updates, and detailed
    /// transport diagnostics. Disable for the normal hot path; controlled
    /// evidence runs enable it explicitly.
    pub diagnostics_enabled: bool,
    /// 送信キューのバッファサイズ。
    pub send_queue_capacity: usize,
    /// 優先度スケジューラ設定。
    pub priority: PriorityConfig,
    /// Maximum number of ordinary Raft envelopes per temporary QUIC stream.
    /// A value of one restores the legacy one-envelope-per-stream behavior.
    pub raft_stream_batch_size: usize,
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
            stream_receive_window: FILE_TRANSFER_STREAM_RECEIVE_WINDOW_BYTES,
            receive_window: FILE_TRANSFER_CONNECTION_RECEIVE_WINDOW_BYTES,
            send_window: FILE_TRANSFER_SEND_WINDOW_BYTES,
            max_concurrent_uni_streams: FILE_TRANSFER_MAX_CONCURRENT_UNI_STREAMS,
            max_idle_timeout: Duration::from_secs(30),
            keep_alive_interval: None,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            send_timeout: Duration::from_millis(200),
            await_peer_stop: true,
            diagnostics_enabled: true,
            send_queue_capacity: 1024,
            priority: PriorityConfig::default(),
            raft_stream_batch_size: 32,
            retransmit: RetransmitConfig::default(),
            qos: QosConfig::default(),
            handshake: HandshakeConfig::default(),
        }
    }
}

impl TransportConfigV04 {
    /// Returns the production profile used for the v0.5.2 File Transfer SLO.
    pub fn file_transfer_performance() -> Self {
        Self {
            stream_receive_window: FILE_TRANSFER_STREAM_RECEIVE_WINDOW_BYTES,
            receive_window: FILE_TRANSFER_CONNECTION_RECEIVE_WINDOW_BYTES,
            send_window: FILE_TRANSFER_SEND_WINDOW_BYTES,
            max_concurrent_uni_streams: FILE_TRANSFER_MAX_CONCURRENT_UNI_STREAMS,
            ..Self::default()
        }
    }

    /// Builds the Quinn transport configuration represented by this Chirps config.
    pub fn to_quinn_transport_config(&self) -> anyhow::Result<Arc<quinn::TransportConfig>> {
        if self.stream_receive_window == 0
            || self.receive_window == 0
            || self.send_window == 0
            || self.max_concurrent_uni_streams == 0
            || self.max_connections == 0
            || self.max_idle_timeout.is_zero()
        {
            anyhow::bail!(
                "QUIC windows, stream limit, and connection limit must be greater than zero"
            );
        }
        if self.receive_window < self.stream_receive_window {
            anyhow::bail!("receive_window must be at least stream_receive_window");
        }
        if let Some(keep_alive) = self.keep_alive_interval
            && keep_alive >= self.max_idle_timeout
        {
            anyhow::bail!("keep_alive_interval must be shorter than max_idle_timeout");
        }

        let mut config = quinn::TransportConfig::default();
        config
            .stream_receive_window(
                quinn::VarInt::try_from(self.stream_receive_window)
                    .map_err(|_| anyhow::anyhow!("stream_receive_window exceeds QUIC VarInt"))?,
            )
            .receive_window(
                quinn::VarInt::try_from(self.receive_window)
                    .map_err(|_| anyhow::anyhow!("receive_window exceeds QUIC VarInt"))?,
            )
            .send_window(self.send_window)
            .max_concurrent_uni_streams(self.max_concurrent_uni_streams.into())
            .max_idle_timeout(Some(self.max_idle_timeout.try_into().map_err(|_| {
                anyhow::anyhow!("max_idle_timeout is outside QUIC limits")
            })?))
            .keep_alive_interval(self.keep_alive_interval);
        Ok(Arc::new(config))
    }
}

#[cfg(test)]
mod tests {
    use super::TransportConfigV04;
    use std::time::Duration;

    #[test]
    fn peer_stop_wait_is_safe_by_default() {
        assert!(TransportConfigV04::default().await_peer_stop);
    }

    #[test]
    fn peer_stop_wait_can_be_disabled_for_fanout_lane() {
        let config = TransportConfigV04 {
            await_peer_stop: false,
            ..TransportConfigV04::default()
        };
        assert!(!config.await_peer_stop);
    }

    #[test]
    fn diagnostics_are_enabled_by_default_and_detachable() {
        assert!(TransportConfigV04::default().diagnostics_enabled);
        let config = TransportConfigV04 {
            diagnostics_enabled: false,
            ..TransportConfigV04::default()
        };
        assert!(!config.diagnostics_enabled);
    }

    #[test]
    fn raft_stream_batching_defaults_to_thirty_two_and_is_disableable() {
        assert_eq!(TransportConfigV04::default().raft_stream_batch_size, 32);
        let config = TransportConfigV04 {
            raft_stream_batch_size: 1,
            ..TransportConfigV04::default()
        };
        assert_eq!(config.raft_stream_batch_size, 1);
    }

    #[test]
    fn production_defaults_match_file_transfer_profile() {
        let default = TransportConfigV04::default();
        let profile = TransportConfigV04::file_transfer_performance();
        assert_eq!(default.stream_receive_window, 16 * 1024 * 1024);
        assert_eq!(default.receive_window, 64 * 1024 * 1024);
        assert_eq!(default.send_window, 64 * 1024 * 1024);
        assert_eq!(default.max_concurrent_uni_streams, 256);
        assert_eq!(
            (
                default.stream_receive_window,
                default.receive_window,
                default.send_window,
                default.max_concurrent_uni_streams
            ),
            (
                profile.stream_receive_window,
                profile.receive_window,
                profile.send_window,
                profile.max_concurrent_uni_streams
            )
        );
    }

    #[test]
    fn quinn_transport_config_rejects_unsafe_values() {
        let config = TransportConfigV04 {
            keep_alive_interval: Some(Duration::from_secs(30)),
            max_idle_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        assert!(config.to_quinn_transport_config().is_err());
    }
}

/// 優先度制御の基本パラメータ。weights は [High, Normal, Low] の順でデフォルト [4,2,1]。
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

/// 再送バッファの容量制御設定。デフォルトは 32MB / 10,000 件 / TTL 60 秒。
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

/// QoS とバックプレッシャーの設定。bandwidth と queue_limits を内包する。
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

/// ストリーム種別ごとのキュー上限。Raft/Control/Snapshot は raft_* を共有。
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

/// 帯域・スロットルの設定。snapshot_bandwidth_limit は RaftSnapshot に適用する帯域上限 (デフォルト 50MB/s)。
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
