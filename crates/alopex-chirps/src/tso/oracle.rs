use super::{Clock, TimestampRange, TsoCommand, TsoError, TsoResponse};
use crate::multi_raft::{GroupHandle, GroupId, MultiRaftError};
use crate::raft::{RaftMetricsCollector, TsoMetricsUpdate};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Reserved group namespace for the single cluster-wide Timestamp Oracle.
pub const TSO_GROUP_ID: GroupId = GroupId(u64::MAX);

#[derive(Clone, Copy, Debug)]
pub struct TsoConfig {
    pub timestamp_ttl: Duration,
}

impl Default for TsoConfig {
    fn default() -> Self {
        Self {
            timestamp_ttl: Duration::from_secs(3),
        }
    }
}

/// Leader-side facade over the dedicated, durable OpenRaft TSO group.
pub struct TimestampOracle {
    node_id: u64,
    group: Arc<GroupHandle>,
    clock: Arc<dyn Clock>,
    lease_expiry: Mutex<Option<u64>>,
    lease_duration_ms: u64,
    metrics_collector: RwLock<Option<Arc<RaftMetricsCollector>>>,
}

impl TimestampOracle {
    pub fn new(
        node_id: u64,
        group: Arc<GroupHandle>,
        clock: Arc<dyn Clock>,
        config: TsoConfig,
    ) -> Result<Self, TsoError> {
        if group.group_id() != TSO_GROUP_ID {
            return Err(TsoError::InvalidTsoGroup {
                expected: TSO_GROUP_ID,
                actual: group.group_id(),
            });
        }
        let lease_duration_ms: u64 = config.timestamp_ttl.as_millis().try_into().map_err(|_| {
            TsoError::InvalidConfig("timestamp_ttl exceeds u64 milliseconds".into())
        })?;
        if lease_duration_ms == 0 {
            return Err(TsoError::InvalidConfig(
                "timestamp_ttl must be greater than zero".into(),
            ));
        }
        Ok(Self {
            node_id,
            group,
            clock,
            lease_expiry: Mutex::new(None),
            lease_duration_ms,
            metrics_collector: RwLock::new(None),
        })
    }

    /// Registers the Prometheus collector used for subsequent TSO requests.
    ///
    /// Replacing the collector is atomic with respect to request observations;
    /// an in-flight request records to either the old or new collector, never
    /// to both.
    pub fn set_metrics_collector(&self, collector: Arc<RaftMetricsCollector>) {
        *self
            .metrics_collector
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(collector);
    }

    pub fn is_leader(&self) -> bool {
        let metrics = self.group.metrics();
        metrics.id == self.node_id && metrics.current_leader == Some(self.node_id)
    }

    pub fn leader_id(&self) -> Option<u64> {
        self.group.metrics().current_leader
    }

    pub async fn get_timestamp(&self) -> Result<super::HybridTimestamp, TsoError> {
        Ok(self.get_timestamps(1).await?.start)
    }

    pub async fn get_timestamps(&self, count: u32) -> Result<TimestampRange, TsoError> {
        self.get_timestamps_started(count, Instant::now()).await
    }

    pub(crate) async fn get_timestamps_started(
        &self,
        count: u32,
        started: Instant,
    ) -> Result<TimestampRange, TsoError> {
        let result = self.get_timestamps_inner(count).await;
        self.record_request(started, &result);
        result
    }

    async fn get_timestamps_inner(&self, count: u32) -> Result<TimestampRange, TsoError> {
        if count == 0 {
            return Err(TsoError::InvalidCount);
        }
        self.require_leader()?;
        let physical_ms = self.clock.physical_millis();
        let lease_now_ms = self.clock.lease_millis();
        self.ensure_lease(lease_now_ms).await?;
        match self
            .propose(TsoCommand::Allocate {
                leader_id: self.node_id,
                lease_now_ms,
                physical_ms,
                count,
            })
            .await?
        {
            TsoResponse::Allocated(range) => Ok(range),
            TsoResponse::LeasePending { not_before_ms } => {
                *self.lease_expiry.lock().await = None;
                Err(TsoError::LeaseNotReady { not_before_ms })
            }
            TsoResponse::InvalidCount => Err(TsoError::InvalidCount),
            TsoResponse::TimestampOverflow => Err(TsoError::TimestampOverflow),
            TsoResponse::LeaseAcquired { .. } => Err(TsoError::Codec(
                "allocation returned a lease response".into(),
            )),
        }
    }

    pub(crate) fn record_rejected_request(&self, started: Instant, error: &TsoError) {
        self.record_request(started, &Err(error.clone()));
    }

    fn record_request(&self, started: Instant, result: &Result<TimestampRange, TsoError>) {
        let collector = self
            .metrics_collector
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(collector) = collector else {
            return;
        };

        let mut update = TsoMetricsUpdate {
            result: Some(match result {
                Ok(_) => "success",
                Err(error) => metric_result(error),
            }),
            request_count: 1,
            request_latency_seconds: Some(started.elapsed().as_secs_f64()),
            ..Default::default()
        };
        if let Ok(range) = result {
            update.allocated = u64::from(range.count);
            update.physical_time = Some(range.end.physical);
            update.logical_counter = Some(u64::from(range.end.logical));
            update.batch_size = Some(u64::from(range.count));
        }
        collector.update_tso(&update);
    }

    fn require_leader(&self) -> Result<(), TsoError> {
        if self.is_leader() {
            Ok(())
        } else {
            Err(TsoError::NotLeader(self.leader_id()))
        }
    }

    async fn ensure_lease(&self, now_ms: u64) -> Result<(), TsoError> {
        let mut cached = self.lease_expiry.lock().await;
        if cached.is_some_and(|expires| now_ms < expires) {
            return Ok(());
        }
        match self
            .propose(TsoCommand::AcquireLease {
                leader_id: self.node_id,
                now_ms,
                lease_duration_ms: self.lease_duration_ms,
            })
            .await?
        {
            TsoResponse::LeaseAcquired { expires_at_ms } => {
                *cached = Some(expires_at_ms);
                Ok(())
            }
            TsoResponse::LeasePending { not_before_ms } => {
                Err(TsoError::LeaseNotReady { not_before_ms })
            }
            TsoResponse::TimestampOverflow => Err(TsoError::TimestampOverflow),
            _ => Err(TsoError::Codec(
                "lease acquisition returned an allocation response".into(),
            )),
        }
    }

    async fn propose(&self, command: TsoCommand) -> Result<TsoResponse, TsoError> {
        self.require_leader()?;
        let bytes =
            bincode::serialize(&command).map_err(|error| TsoError::Codec(error.to_string()))?;
        let response = self.group.propose(bytes).await.map_err(|error| {
            if !self.is_leader() {
                TsoError::NotLeader(self.leader_id())
            } else {
                match error {
                    MultiRaftError::Routing { reason, .. } => TsoError::Raft(reason),
                    other => TsoError::Raft(other.to_string()),
                }
            }
        })?;
        bincode::deserialize(&response).map_err(|error| TsoError::Codec(error.to_string()))
    }
}

fn metric_result(error: &TsoError) -> &'static str {
    match error {
        TsoError::InvalidCount => "invalid_count",
        TsoError::InvalidTsoGroup { .. } | TsoError::InvalidConfig(_) => "invalid_config",
        TsoError::NotLeader(_) => "not_leader",
        TsoError::LeaseNotReady { .. } => "lease_not_ready",
        TsoError::Unauthenticated { .. } => "unauthorized",
        TsoError::Transport(_) => "transport_error",
        TsoError::Raft(_) => "raft_error",
        TsoError::Codec(_) => "codec_error",
        TsoError::TimestampOverflow => "overflow",
        TsoError::NonMonotonicResponse => "non_monotonic",
    }
}
