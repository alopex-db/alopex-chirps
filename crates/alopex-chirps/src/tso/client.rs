use super::{HybridTimestamp, TimestampRange, TsoError, TsoRequest};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

#[async_trait]
pub trait TsoTransport: Send + Sync + 'static {
    async fn get_timestamps(
        &self,
        target: u64,
        request: TsoRequest,
    ) -> Result<TimestampRange, TsoError>;

    async fn discover_leader(&self) -> Result<u64, TsoError>;
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
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for TsoClientConfig {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
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
    cache: Option<CachedRange>,
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
            cache: None,
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
        let Some(cache) = self.cache.as_mut() else {
            return Ok(None);
        };
        let Some(value) = cache.pop() else {
            self.cache = None;
            return Ok(None);
        };
        if self.last_returned.is_some_and(|last| value <= last) {
            self.cache = None;
            return Err(TsoError::NonMonotonicResponse);
        }
        self.last_returned = Some(value);
        if cache.exhausted() {
            self.cache = None;
        }
        Ok(Some(value))
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
                    self.cache = Some(CachedRange {
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
