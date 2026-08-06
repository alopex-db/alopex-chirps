#![cfg(feature = "snapshot")]

use alopex_chirps::snapshot::{
    NoopSnapshotProgressObserver, SnapshotChunk, SnapshotChunkSink, SnapshotManifest,
    SnapshotReceiver, SnapshotSender, SnapshotTransferConfig, SnapshotTransferError,
    SnapshotTransferReceipt,
};
use async_trait::async_trait;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::sync::Arc;

struct DurableSink {
    receiver: tokio::sync::Mutex<Option<SnapshotReceiver>>,
    checkpoint: tempfile::TempDir,
}

impl DurableSink {
    fn new() -> Self {
        Self {
            receiver: tokio::sync::Mutex::new(None),
            checkpoint: tempfile::tempdir().expect("benchmark checkpoint directory"),
        }
    }
}

#[async_trait]
impl SnapshotChunkSink for DurableSink {
    async fn begin(&self, manifest: SnapshotManifest) -> Result<(), SnapshotTransferError> {
        *self.receiver.lock().await = Some(SnapshotReceiver::new(
            manifest,
            Arc::new(NoopSnapshotProgressObserver),
        )?);
        Ok(())
    }

    async fn send_chunk(&self, chunk: SnapshotChunk) -> Result<(), SnapshotTransferError> {
        self.receiver
            .lock()
            .await
            .as_mut()
            .expect("begin precedes chunks")
            .accept(chunk)
    }

    async fn finish(
        &self,
        snapshot_id: &str,
    ) -> Result<SnapshotTransferReceipt, SnapshotTransferError> {
        let receiver = self
            .receiver
            .lock()
            .await
            .take()
            .expect("begin precedes finish");
        let verified = receiver.into_verified(snapshot_id)?;
        let path = self.checkpoint.path().join("snapshot.checkpoint");
        let file = std::fs::File::create(&path)
            .map_err(|error| SnapshotTransferError::terminal(error.to_string()))?;
        file.set_len(verified.bytes.len() as u64)
            .map_err(|error| SnapshotTransferError::terminal(error.to_string()))?;
        std::fs::write(&path, &verified.bytes)
            .map_err(|error| SnapshotTransferError::terminal(error.to_string()))?;
        std::fs::File::open(&path)
            .and_then(|file| file.sync_all())
            .map_err(|error| SnapshotTransferError::terminal(error.to_string()))?;
        Ok(SnapshotTransferReceipt::installed(
            verified.bytes.len() as u64
        ))
    }

    async fn abort(&self, _snapshot_id: &str) {
        *self.receiver.lock().await = None;
    }
}

fn snapshot_component_throughput(criterion: &mut Criterion) {
    let bytes = std::env::var("CHIRPS_SNAPSHOT_BENCH_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64 * 1024 * 1024);
    let payload = Arc::new(vec![0x5a; bytes]);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let mut group = criterion.benchmark_group("snapshot_component");
    group.throughput(Throughput::Bytes(bytes as u64));
    group.sample_size(10);
    group.bench_function("verified_parallel_durable", |bencher| {
        bencher.to_async(&runtime).iter(|| {
            let payload = Arc::clone(&payload);
            async move {
                let sender = SnapshotSender::new(
                    SnapshotTransferConfig::default(),
                    Arc::new(NoopSnapshotProgressObserver),
                )
                .unwrap();
                sender
                    .transfer(
                        "criterion-snapshot",
                        payload.as_ref().clone(),
                        Arc::new(DurableSink::new()),
                    )
                    .await
                    .unwrap()
            }
        });
    });
    group.finish();
}

criterion_group!(benches, snapshot_component_throughput);
criterion_main!(benches);
