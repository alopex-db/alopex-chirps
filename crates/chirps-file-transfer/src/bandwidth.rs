use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Token-bucket bandwidth limiter for file transfer streams.
#[derive(Debug)]
pub struct BandwidthThrottle {
    limit: u64,
    tokens: AtomicU64,
    last_refill: Mutex<Instant>,
}

impl BandwidthThrottle {
    /// Creates a throttler with a bytes-per-second limit.
    /// Use `0` for unlimited throughput.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn new(limit_bytes_per_sec: u64) -> Self {
        let tokens = if limit_bytes_per_sec == 0 {
            0
        } else {
            limit_bytes_per_sec
        };
        BandwidthThrottle {
            limit: limit_bytes_per_sec,
            tokens: AtomicU64::new(tokens),
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Creates an unlimited throttler (no waiting).
    ///
    /// # Panics
    /// This method does not panic.
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// Waits until `bytes` can be consumed from the token bucket.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn acquire(&self, bytes: u64) {
        if bytes == 0 || self.limit == 0 {
            return;
        }

        let mut remaining = bytes;
        while remaining > 0 {
            let wait_duration = {
                let mut last_refill = self.last_refill.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(*last_refill);
                let add = tokens_for_elapsed(self.limit, elapsed);
                if add > 0 {
                    let current = self.tokens.load(Ordering::Relaxed);
                    let new_tokens = current.saturating_add(add).min(self.limit);
                    self.tokens.store(new_tokens, Ordering::Relaxed);
                    *last_refill = now;
                }

                let available = self.tokens.load(Ordering::Relaxed);
                if available >= remaining {
                    self.tokens.fetch_sub(remaining, Ordering::Relaxed);
                    return;
                }

                if available > 0 {
                    self.tokens.store(0, Ordering::Relaxed);
                    remaining = remaining.saturating_sub(available);
                }

                missing_wait(self.limit, remaining)
            };

            if let Some(duration) = wait_duration {
                sleep(duration).await;
            } else {
                return;
            }
        }
    }
}

fn tokens_for_elapsed(limit: u64, elapsed: Duration) -> u64 {
    if limit == 0 {
        return 0;
    }
    let nanos = elapsed.as_nanos();
    let add = nanos
        .saturating_mul(limit as u128)
        .saturating_div(1_000_000_000u128);
    add.min(u64::MAX as u128) as u64
}

fn missing_wait(limit: u64, missing: u64) -> Option<Duration> {
    if limit == 0 {
        return None;
    }
    let nanos = (missing as u128)
        .saturating_mul(1_000_000_000u128)
        .saturating_div(limit as u128)
        .max(1);
    let nanos = nanos.min(u64::MAX as u128) as u64;
    Some(Duration::from_nanos(nanos))
}
