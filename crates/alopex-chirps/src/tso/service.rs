use super::{TimestampOracle, TimestampRange, TsoError, client::TsoFrame};
use crate::raft::RaftMetricsCollector;
use alopex_chirps_wire::frame::{Frame, UserMessage};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TsoRequest {
    pub requester: u64,
    pub credential: Vec<u8>,
    pub count: u32,
}

impl fmt::Debug for TsoRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TsoRequest")
            .field("requester", &self.requester)
            .field("credential", &"[REDACTED]")
            .field("count", &self.count)
            .finish()
    }
}

/// Authentication seam implemented by the node identity established at the
/// transport boundary. The oracle is never called before this check succeeds.
#[async_trait]
pub trait NodeAuthenticator: Send + Sync + 'static {
    async fn authenticate(&self, node_id: u64, credential: &[u8]) -> bool;
}

pub struct TsoService {
    oracle: Arc<TimestampOracle>,
    authenticator: Arc<dyn NodeAuthenticator>,
}

impl TsoService {
    pub fn new(oracle: Arc<TimestampOracle>, authenticator: Arc<dyn NodeAuthenticator>) -> Self {
        Self {
            oracle,
            authenticator,
        }
    }

    /// Registers one collector for authenticated and rejected TSO requests.
    pub fn set_metrics_collector(&self, collector: Arc<RaftMetricsCollector>) {
        self.oracle.set_metrics_collector(collector);
    }

    pub async fn get_timestamps(&self, request: TsoRequest) -> Result<TimestampRange, TsoError> {
        let started = Instant::now();
        if !self
            .authenticator
            .authenticate(request.requester, &request.credential)
            .await
        {
            let error = TsoError::Unauthenticated {
                node_id: request.requester,
            };
            self.oracle.record_rejected_request(started, &error);
            return Err(error);
        }
        self.oracle
            .get_timestamps_started(request.count, started)
            .await
    }

    pub(crate) async fn handle_frame(&self, from: NodeId, frame: Frame) -> Option<Frame> {
        let Frame::User(UserMessage { payload }) = frame else {
            return None;
        };
        let message = bincode::deserialize::<TsoFrame>(&payload).ok()?;
        let response = match message {
            TsoFrame::Request {
                correlation_id,
                request,
            } => TsoFrame::TimestampResponse {
                correlation_id,
                response: self.get_timestamps(request).await,
            },
            TsoFrame::DiscoverLeader {
                correlation_id,
                requester,
                credential,
            } => {
                let authenticated = self
                    .authenticator
                    .authenticate(requester, &credential)
                    .await;
                let response = if !authenticated {
                    Err(TsoError::Unauthenticated { node_id: requester })
                } else {
                    Ok(self.oracle.leader_id().unwrap_or(self.oracle.node_id()))
                };
                TsoFrame::LeaderResponse {
                    correlation_id,
                    response,
                }
            }
            TsoFrame::TimestampResponse { .. } | TsoFrame::LeaderResponse { .. } => return None,
        };
        let payload = bincode::serialize(&response).ok()?;
        let _ = from;
        Some(Frame::User(UserMessage { payload }))
    }
}
