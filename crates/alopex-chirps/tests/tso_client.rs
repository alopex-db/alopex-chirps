#![cfg(feature = "tso")]

use alopex_chirps::tso::{
    BackoffSleeper, HybridTimestamp, TimestampRange, TsoClient, TsoClientConfig, TsoError,
    TsoRequest, TsoTransport,
};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct ScriptedTransport {
    responses: Mutex<VecDeque<Result<TimestampRange, TsoError>>>,
    targets: Mutex<Vec<u64>>,
    requests: Mutex<Vec<TsoRequest>>,
    discovered: Mutex<VecDeque<Result<u64, TsoError>>>,
}

impl ScriptedTransport {
    fn with_responses(responses: Vec<Result<TimestampRange, TsoError>>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
            ..Self::default()
        }
    }
}

#[async_trait]
impl TsoTransport for ScriptedTransport {
    async fn get_timestamps(
        &self,
        target: u64,
        request: TsoRequest,
    ) -> Result<TimestampRange, TsoError> {
        self.targets.lock().unwrap().push(target);
        self.requests.lock().unwrap().push(request);
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted response")
    }

    async fn discover_leader(&self) -> Result<u64, TsoError> {
        self.discovered.lock().unwrap().pop_front().unwrap_or(Ok(1))
    }
}

#[derive(Default)]
struct RecordingSleeper(Mutex<Vec<Duration>>);

#[async_trait]
impl BackoffSleeper for RecordingSleeper {
    async fn sleep(&self, duration: Duration) {
        self.0.lock().unwrap().push(duration);
    }
}

fn range(physical: u64, logical: u32, count: u32) -> TimestampRange {
    TimestampRange::from_start(HybridTimestamp::new(physical, logical), count).unwrap()
}

fn config(batch_size: u32) -> TsoClientConfig {
    TsoClientConfig {
        batch_size,
        prefetch_threshold: 0,
        max_retries: 3,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(100),
    }
}

#[tokio::test]
async fn empty_cache_fetches_and_consumes_a_committed_batch() {
    let transport = Arc::new(ScriptedTransport::with_responses(vec![Ok(range(10, 0, 3))]));
    let sleeper = Arc::new(RecordingSleeper::default());
    let mut client = TsoClient::with_sleeper(
        transport.clone(),
        sleeper,
        7,
        b"credential".to_vec(),
        Some(1),
        config(3),
    )
    .unwrap();

    let values = client.get_timestamps(3).await.unwrap();

    assert_eq!(
        values,
        vec![
            HybridTimestamp::new(10, 0),
            HybridTimestamp::new(10, 1),
            HybridTimestamp::new(10, 2),
        ]
    );
    assert_eq!(transport.targets.lock().unwrap().as_slice(), &[1]);
    assert_eq!(transport.requests.lock().unwrap()[0].count, 3);
}

#[tokio::test]
async fn not_leader_refreshes_hint_and_retries() {
    let transport = Arc::new(ScriptedTransport::with_responses(vec![
        Err(TsoError::NotLeader(Some(2))),
        Ok(range(20, 0, 2)),
    ]));
    let sleeper = Arc::new(RecordingSleeper::default());
    let mut client = TsoClient::with_sleeper(
        transport.clone(),
        sleeper,
        7,
        b"credential".to_vec(),
        Some(1),
        config(2),
    )
    .unwrap();

    assert_eq!(
        client.get_timestamp().await.unwrap(),
        HybridTimestamp::new(20, 0)
    );
    assert_eq!(transport.targets.lock().unwrap().as_slice(), &[1, 2]);
    assert_eq!(client.leader_hint(), Some(2));
}

#[tokio::test]
async fn leader_rediscovery_transport_failure_uses_backoff() {
    let transport = Arc::new(ScriptedTransport::with_responses(vec![
        Err(TsoError::NotLeader(None)),
        Ok(range(25, 0, 1)),
    ]));
    transport.discovered.lock().unwrap().extend([
        Err(TsoError::Transport("discovery unavailable".into())),
        Ok(2),
    ]);
    let sleeper = Arc::new(RecordingSleeper::default());
    let mut client = TsoClient::with_sleeper(
        transport.clone(),
        sleeper.clone(),
        7,
        b"credential".to_vec(),
        Some(1),
        config(1),
    )
    .unwrap();

    assert_eq!(
        client.get_timestamp().await.unwrap(),
        HybridTimestamp::new(25, 0)
    );
    assert_eq!(transport.targets.lock().unwrap().as_slice(), &[1, 2]);
    assert_eq!(client.leader_hint(), Some(2));
    assert_eq!(
        sleeper.0.lock().unwrap().as_slice(),
        &[Duration::from_millis(20)]
    );
}

#[tokio::test]
async fn retryable_failure_preserves_cache_and_monotonicity() {
    let transport = Arc::new(ScriptedTransport::with_responses(vec![
        Ok(range(30, 0, 2)),
        Err(TsoError::Transport("temporary-1".into())),
        Err(TsoError::Transport("temporary-2".into())),
        Ok(range(30, 2, 2)),
    ]));
    let sleeper = Arc::new(RecordingSleeper::default());
    let mut client = TsoClient::with_sleeper(
        transport,
        sleeper.clone(),
        7,
        b"credential".to_vec(),
        Some(1),
        config(2),
    )
    .unwrap();

    let first = client.get_timestamp().await.unwrap();
    let cached = client.get_timestamp().await.unwrap();
    let after_retry = client.get_timestamp().await.unwrap();

    assert_eq!(first, HybridTimestamp::new(30, 0));
    assert_eq!(cached, HybridTimestamp::new(30, 1));
    assert_eq!(after_retry, HybridTimestamp::new(30, 2));
    assert!(first < cached && cached < after_retry);
    assert_eq!(
        sleeper.0.lock().unwrap().as_slice(),
        &[Duration::from_millis(10), Duration::from_millis(20)]
    );
}
