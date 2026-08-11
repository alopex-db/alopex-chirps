use super::{HybridTimestamp, TimestampRange, TsoError, TsoRequest};
use crate::mesh::MeshHandle;
use alopex_chirps_wire::frame::{Frame, UserMessage};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time;

#[async_trait]
pub trait TsoTransport: Send + Sync + 'static {
    async fn get_timestamps(
        &self,
        target: u64,
        request: TsoRequest,
    ) -> Result<TimestampRange, TsoError>;

    async fn discover_leader(&self) -> Result<u64, TsoError>;
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum TsoFrame {
    Request {
        correlation_id: u64,
        request: TsoRequest,
    },
    DiscoverLeader {
        correlation_id: u64,
        requester: u64,
        credential: Vec<u8>,
    },
    TimestampResponse {
        correlation_id: u64,
        response: Result<TimestampRange, TsoError>,
    },
    LeaderResponse {
        correlation_id: u64,
        response: Result<u64, TsoError>,
    },
}

struct PendingResponse(oneshot::Sender<Result<TsoFrame, TsoError>>);

/// Production TSO client transport backed by the QUIC/Frame mesh.
pub struct ChirpsTsoTransport {
    mesh: MeshHandle,
    requester: u64,
    credential: Vec<u8>,
    next_correlation: AtomicU64,
    pending: Arc<Mutex<std::collections::HashMap<u64, PendingResponse>>>,
    peers: RwLock<std::collections::HashMap<u64, NodeId>>,
    discovery_target: Mutex<Option<u64>>,
}

impl ChirpsTsoTransport {
    pub async fn new(
        mesh: MeshHandle,
        requester: u64,
        credential: Vec<u8>,
    ) -> Result<Arc<Self>, TsoError> {
        let mut receiver = mesh
            .subscribe()
            .await
            .map_err(|error| TsoError::Transport(error.to_string()))?;
        let pending = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let transport = Arc::new(Self {
            mesh,
            requester,
            credential,
            next_correlation: AtomicU64::new(1),
            pending: Arc::clone(&pending),
            peers: RwLock::new(std::collections::HashMap::new()),
            discovery_target: Mutex::new(None),
        });
        tokio::spawn(async move {
            while let Some((_from, Frame::User(UserMessage { payload }))) = receiver.recv().await {
                let Ok(message) = bincode::deserialize::<TsoFrame>(&payload) else {
                    continue;
                };
                let correlation_id = match &message {
                    TsoFrame::TimestampResponse { correlation_id, .. }
                    | TsoFrame::LeaderResponse { correlation_id, .. } => *correlation_id,
                    _ => continue,
                };
                let pending = pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&correlation_id);
                match (pending, message) {
                    (
                        Some(PendingResponse(sender)),
                        response @ TsoFrame::TimestampResponse { .. },
                    )
                    | (Some(PendingResponse(sender)), response @ TsoFrame::LeaderResponse { .. }) =>
                    {
                        let _ = sender.send(Ok(response));
                    }
                    _ => {}
                }
            }
        });
        Ok(transport)
    }

    pub fn register_node_id(&self, tso_node_id: u64, wire_node_id: NodeId) {
        self.peers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(tso_node_id, wire_node_id);
    }

    pub fn set_discovery_target(&self, tso_node_id: u64) {
        *self
            .discovery_target
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(tso_node_id);
    }

    async fn request(&self, target: u64, message: TsoFrame) -> Result<TsoFrame, TsoError> {
        let correlation_id = match &message {
            TsoFrame::Request { correlation_id, .. }
            | TsoFrame::DiscoverLeader { correlation_id, .. } => *correlation_id,
            _ => return Err(TsoError::Codec("invalid outbound TSO frame".into())),
        };
        let (pending, response) = match message {
            TsoFrame::Request { .. } => {
                let (sender, receiver) = oneshot::channel();
                (PendingResponse(sender), receiver)
            }
            TsoFrame::DiscoverLeader { .. } => {
                let (sender, receiver) = oneshot::channel();
                (PendingResponse(sender), receiver)
            }
            _ => unreachable!(),
        };
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(correlation_id, pending);
        let payload =
            bincode::serialize(&message).map_err(|error| TsoError::Codec(error.to_string()))?;
        let wire_target = self
            .peers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&target)
            .copied()
            .unwrap_or_else(|| canonical_node_id(target));
        if let Err(error) = self
            .mesh
            .send_to(wire_target, Frame::User(UserMessage { payload }))
            .await
        {
            self.pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&correlation_id);
            return Err(TsoError::Transport(error.to_string()));
        }
        match time::timeout(Duration::from_secs(10), response).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => Err(TsoError::Transport("TSO response channel closed".into())),
            Err(_) => {
                self.pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&correlation_id);
                Err(TsoError::Transport("TSO request timed out".into()))
            }
        }
    }
}

#[async_trait]
impl TsoTransport for ChirpsTsoTransport {
    async fn get_timestamps(
        &self,
        target: u64,
        request: TsoRequest,
    ) -> Result<TimestampRange, TsoError> {
        let correlation_id = self.next_correlation.fetch_add(1, Ordering::Relaxed);
        let response = self
            .request(
                target,
                TsoFrame::Request {
                    correlation_id,
                    request,
                },
            )
            .await?;
        match response {
            TsoFrame::TimestampResponse { response, .. } => response,
            _ => Err(TsoError::Codec("unexpected TSO timestamp response".into())),
        }
    }

    async fn discover_leader(&self) -> Result<u64, TsoError> {
        let correlation_id = self.next_correlation.fetch_add(1, Ordering::Relaxed);
        let target = self
            .discovery_target
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .or_else(|| {
                self.peers
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .keys()
                    .next()
                    .copied()
            })
            .ok_or_else(|| TsoError::Transport("no TSO discovery target configured".into()))?;
        let response = self
            .request(
                target,
                TsoFrame::DiscoverLeader {
                    correlation_id,
                    requester: self.requester,
                    credential: self.credential.clone(),
                },
            )
            .await?;
        match response {
            TsoFrame::LeaderResponse { response, .. } => response,
            TsoFrame::TimestampResponse { .. } => {
                Err(TsoError::Codec("unexpected TSO leader response".into()))
            }
            _ => Err(TsoError::Codec("unexpected TSO leader response".into())),
        }
    }
}

fn canonical_node_id(id: u64) -> NodeId {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&id.to_be_bytes());
    NodeId::from(bytes)
}

#[async_trait]
pub trait BackoffSleeper: Send + Sync + 'static {
    async fn sleep(&self, duration: Duration);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TokioBackoffSleeper;

#[async_trait]
impl BackoffSleeper for TokioBackoffSleeper {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TsoClientConfig {
    pub batch_size: u32,
    pub prefetch_threshold: u32,
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for TsoClientConfig {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            prefetch_threshold: 1_000,
            max_retries: 10,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
        }
    }
}

#[derive(Clone, Copy)]
struct CachedRange {
    range: TimestampRange,
    next_offset: u32,
}

impl CachedRange {
    fn pop(&mut self) -> Option<HybridTimestamp> {
        let value = self.range.at(self.next_offset)?;
        self.next_offset += 1;
        Some(value)
    }

    fn exhausted(self) -> bool {
        self.next_offset >= self.range.count
    }
}

pub struct TsoClient {
    transport: Arc<dyn TsoTransport>,
    sleeper: Arc<dyn BackoffSleeper>,
    requester: u64,
    credential: Vec<u8>,
    leader: Option<u64>,
    config: TsoClientConfig,
    cache: VecDeque<CachedRange>,
    last_returned: Option<HybridTimestamp>,
}

impl TsoClient {
    pub fn new(
        transport: Arc<dyn TsoTransport>,
        requester: u64,
        credential: Vec<u8>,
        leader: Option<u64>,
        config: TsoClientConfig,
    ) -> Result<Self, TsoError> {
        Self::with_sleeper(
            transport,
            Arc::new(TokioBackoffSleeper),
            requester,
            credential,
            leader,
            config,
        )
    }

    pub fn with_sleeper(
        transport: Arc<dyn TsoTransport>,
        sleeper: Arc<dyn BackoffSleeper>,
        requester: u64,
        credential: Vec<u8>,
        leader: Option<u64>,
        config: TsoClientConfig,
    ) -> Result<Self, TsoError> {
        if config.batch_size == 0 {
            return Err(TsoError::InvalidConfig(
                "client batch_size must be greater than zero".into(),
            ));
        }
        if config.initial_backoff.is_zero() || config.initial_backoff > config.max_backoff {
            return Err(TsoError::InvalidConfig(
                "initial_backoff must be non-zero and no greater than max_backoff".into(),
            ));
        }
        Ok(Self {
            transport,
            sleeper,
            requester,
            credential,
            leader,
            config,
            cache: VecDeque::new(),
            last_returned: None,
        })
    }

    pub fn leader_hint(&self) -> Option<u64> {
        self.leader
    }

    pub async fn refresh_leader(&mut self) -> Result<u64, TsoError> {
        let leader = self.transport.discover_leader().await?;
        self.leader = Some(leader);
        Ok(leader)
    }

    pub async fn get_timestamp(&mut self) -> Result<HybridTimestamp, TsoError> {
        Ok(self.get_timestamps(1).await?.remove(0))
    }

    pub async fn get_timestamps(&mut self, count: u32) -> Result<Vec<HybridTimestamp>, TsoError> {
        if count == 0 {
            return Err(TsoError::InvalidCount);
        }
        let mut values = Vec::with_capacity(count as usize);
        while values.len() < count as usize {
            if self.cache_remaining() > 0
                && self.cache_remaining() <= u64::from(self.config.prefetch_threshold)
            {
                self.refill(self.config.batch_size).await?;
            }
            if let Some(value) = self.pop_cache()? {
                values.push(value);
                continue;
            }
            let remaining = count - values.len() as u32;
            self.refill(self.config.batch_size.max(remaining)).await?;
        }
        Ok(values)
    }

    fn pop_cache(&mut self) -> Result<Option<HybridTimestamp>, TsoError> {
        while self.cache.front().is_some_and(|cache| cache.exhausted()) {
            self.cache.pop_front();
        }
        let Some(cache) = self.cache.front_mut() else {
            return Ok(None);
        };
        let Some(value) = cache.pop() else {
            return Ok(None);
        };
        if self.last_returned.is_some_and(|last| value <= last) {
            self.cache.clear();
            return Err(TsoError::NonMonotonicResponse);
        }
        self.last_returned = Some(value);
        Ok(Some(value))
    }

    fn cache_remaining(&self) -> u64 {
        self.cache
            .iter()
            .map(|range| u64::from(range.range.count.saturating_sub(range.next_offset)))
            .sum()
    }

    async fn refill(&mut self, count: u32) -> Result<(), TsoError> {
        let mut failures = 0;
        loop {
            let target = match self.leader {
                Some(leader) => leader,
                None => match self.refresh_leader().await {
                    Ok(leader) => leader,
                    Err(error @ TsoError::Transport(_)) => {
                        if failures >= self.config.max_retries {
                            return Err(error);
                        }
                        let delay = self.backoff(failures);
                        failures += 1;
                        self.sleeper.sleep(delay).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                },
            };
            let request = TsoRequest {
                requester: self.requester,
                credential: self.credential.clone(),
                count,
            };
            match self.transport.get_timestamps(target, request).await {
                Ok(range) => {
                    if range.count != count || range.at(count - 1) != Some(range.end) {
                        return Err(TsoError::Codec(
                            "server returned a non-contiguous or wrong-sized range".into(),
                        ));
                    }
                    self.cache.push_back(CachedRange {
                        range,
                        next_offset: 0,
                    });
                    return Ok(());
                }
                Err(TsoError::NotLeader(hint)) => {
                    self.leader = hint;
                    failures += 1;
                    if failures > self.config.max_retries {
                        return Err(TsoError::NotLeader(hint));
                    }
                }
                Err(error @ TsoError::Transport(_))
                | Err(error @ TsoError::LeaseNotReady { .. }) => {
                    if failures >= self.config.max_retries {
                        return Err(error);
                    }
                    let delay = self.backoff(failures);
                    failures += 1;
                    self.sleeper.sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn backoff(&self, failures: u32) -> Duration {
        let factor = 1u32.checked_shl(failures.min(31)).unwrap_or(u32::MAX);
        self.config
            .initial_backoff
            .saturating_mul(factor)
            .min(self.config.max_backoff)
    }
}
