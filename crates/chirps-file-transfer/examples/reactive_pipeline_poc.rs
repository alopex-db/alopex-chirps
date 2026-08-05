//! Feasibility PoC for a bounded, chunk-streaming transfer pipeline.
//!
//! This is deliberately independent from the production transfer code. It proves
//! that read/compress/transport/decompress/hash/write can overlap without
//! collecting the whole file, while preserving chunk order at the sink.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio::sync::watch;

const CHUNK_BYTES: usize = 1024 * 1024;
const CHUNKS: usize = 32;
const QUEUE_CAPACITY: usize = 2;

#[derive(Debug)]
struct RawChunk {
    index: usize,
    data: Vec<u8>,
}

#[derive(Debug)]
struct WireChunk {
    index: usize,
    original_len: usize,
    data: Vec<u8>,
}

#[derive(Debug)]
struct DecodedChunk {
    index: usize,
    data: Vec<u8>,
}

#[derive(Default, Debug)]
struct StageStats {
    items: usize,
    bytes: usize,
    elapsed: Duration,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let started = Instant::now();
    let sink_delay_ms = std::env::var("POC_SINK_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0u64);
    let cancel_after_ms = std::env::var("POC_CANCEL_AFTER_MS")
        .ok()
        .and_then(|v| v.parse().ok());
    let (cancel_tx, cancel_rx) = watch::channel(false);
    if let Some(delay) = cancel_after_ms {
        let tx = cancel_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            let _ = tx.send(true);
        });
    }
    let (raw_tx, mut raw_rx) = mpsc::channel::<RawChunk>(QUEUE_CAPACITY);
    let (wire_tx, mut wire_rx) = mpsc::channel::<WireChunk>(QUEUE_CAPACITY);
    let (decoded_tx, mut decoded_rx) = mpsc::channel::<DecodedChunk>(QUEUE_CAPACITY);

    let producer_cancel = cancel_rx.clone();
    let producer = tokio::spawn(async move {
        let mut stats = StageStats::default();
        for index in 0..CHUNKS {
            if *producer_cancel.borrow() {
                return Err("cancelled".into());
            }
            let stage = Instant::now();
            let mut data = vec![0u8; CHUNK_BYTES];
            for (offset, byte) in data.iter_mut().enumerate() {
                *byte = ((index * 31 + offset) % 251) as u8;
            }
            raw_tx
                .send(RawChunk { index, data })
                .await
                .map_err(|_| "raw stage closed")?;
            stats.items += 1;
            stats.bytes += CHUNK_BYTES;
            stats.elapsed += stage.elapsed();
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(stats)
    });

    let compressor_cancel = cancel_rx.clone();
    let compressor = tokio::spawn(async move {
        let mut stats = StageStats::default();
        while let Some(chunk) = raw_rx.recv().await {
            if *compressor_cancel.borrow() {
                return Err("cancelled".into());
            }
            let stage = Instant::now();
            let original_len = chunk.data.len();
            let data = zstd::stream::encode_all(chunk.data.as_slice(), 1)?;
            wire_tx
                .send(WireChunk {
                    index: chunk.index,
                    original_len,
                    data,
                })
                .await
                .map_err(|_| "wire stage closed")?;
            stats.items += 1;
            stats.bytes += original_len;
            stats.elapsed += stage.elapsed();
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(stats)
    });

    let transport_cancel = cancel_rx.clone();
    let transport = tokio::spawn(async move {
        let mut stats = StageStats::default();
        while let Some(chunk) = wire_rx.recv().await {
            if *transport_cancel.borrow() {
                return Err("cancelled".into());
            }
            let stage = Instant::now();
            tokio::time::sleep(Duration::from_micros(150)).await;
            decoded_tx
                .send(DecodedChunk {
                    index: chunk.index,
                    data: zstd::stream::decode_all(chunk.data.as_slice())?,
                })
                .await
                .map_err(|_| "decode stage closed")?;
            stats.items += 1;
            stats.bytes += chunk.original_len;
            stats.elapsed += stage.elapsed();
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(stats)
    });

    let sink_cancel = cancel_rx;
    let sink = tokio::spawn(async move {
        let mut stats = StageStats::default();
        let mut next = 0usize;
        let mut pending = BTreeMap::new();
        let mut hasher = Sha256::new();
        let mut written = 0usize;
        while let Some(chunk) = decoded_rx.recv().await {
            if *sink_cancel.borrow() {
                return Err("cancelled".into());
            }
            pending.insert(chunk.index, chunk.data);
            while let Some(data) = pending.remove(&next) {
                let stage = Instant::now();
                if sink_delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(sink_delay_ms)).await;
                }
                hasher.update(&data);
                written += data.len();
                stats.items += 1;
                stats.bytes += data.len();
                stats.elapsed += stage.elapsed();
                next += 1;
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((stats, written, hasher.finalize()))
    });

    let producer_stats = producer.await??;
    let compressor_stats = compressor.await??;
    let transport_stats = transport.await??;
    let (sink_stats, written, digest) = sink.await??;
    assert_eq!(written, CHUNKS * CHUNK_BYTES);
    assert_eq!(sink_stats.items, CHUNKS);

    println!("pipeline=bounded-chunk-poc");
    println!("chunks={CHUNKS} chunk_bytes={CHUNK_BYTES} queue_capacity={QUEUE_CAPACITY}");
    println!("written_bytes={written} sha256={:x}", digest);
    println!("elapsed_ms={:.2}", started.elapsed().as_secs_f64() * 1000.0);
    for (name, stats) in [
        ("read", producer_stats),
        ("compress", compressor_stats),
        ("transport+decode", transport_stats),
        ("hash+write", sink_stats),
    ] {
        println!(
            "stage={name} items={} bytes={} cumulative_ms={:.2}",
            stats.items,
            stats.bytes,
            stats.elapsed.as_secs_f64() * 1000.0
        );
    }
    Ok(())
}
