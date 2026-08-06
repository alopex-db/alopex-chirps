use super::{TimestampOracle, TimestampRange, TsoError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

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

    pub async fn get_timestamps(&self, request: TsoRequest) -> Result<TimestampRange, TsoError> {
        if !self
            .authenticator
            .authenticate(request.requester, &request.credential)
            .await
        {
            return Err(TsoError::Unauthenticated {
                node_id: request.requester,
            });
        }
        self.oracle.get_timestamps(request.count).await
    }
}
