use bincode::deserialize;
use chirps_wire::frame::Frame;
use chirps_wire::node_id::NodeId;
use quinn::RecvStream;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::warn;

use crate::retransmit::{DeduplicationTable, RetransmissionBuffer};
use crate::{ExtendedTransportMetrics, StreamKind, TransportError};

const MAX_ENVELOPE_SIZE: usize = 128 * 1024; // defensive cap

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FrameEnvelopeV2 {
    pub kind: u8,
    pub seq: u64,
    pub ack_seq: u64,
    pub frame: Frame,
}

pub struct ReceiveHandler {
    dedup_table: tokio::sync::Mutex<DeduplicationTable>,
    retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
    incoming_tx: mpsc::Sender<(NodeId, Frame)>,
    metrics: Arc<ExtendedTransportMetrics>,
}

impl ReceiveHandler {
    pub fn new(
        retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
        incoming_tx: mpsc::Sender<(NodeId, Frame)>,
        metrics: Arc<ExtendedTransportMetrics>,
    ) -> Self {
        ReceiveHandler {
            dedup_table: tokio::sync::Mutex::new(DeduplicationTable::new()),
            retransmit_buffer,
            incoming_tx,
            metrics,
        }
    }

    pub async fn handle_stream(
        &self,
        peer: NodeId,
        mut recv: RecvStream,
    ) -> Result<(), TransportError> {
        let bytes = recv
            .read_to_end(MAX_ENVELOPE_SIZE)
            .await
            .map_err(|e| TransportError::Io(e.to_string()))?;

        let env: FrameEnvelopeV2 =
            deserialize(&bytes).map_err(|e| TransportError::Io(e.to_string()))?;

        let kind = StreamKind::try_from(env.kind)?;

        {
            let mut dedup = self.dedup_table.lock().await;
            if !dedup.check_and_update(peer, env.seq) {
                self.metrics.record_duplicate();
                return Ok(());
            }
        }

        {
            let mut buf = self.retransmit_buffer.write().await;
            buf.process_ack(peer, env.ack_seq);
        }

        self.metrics.record_receive(kind, None);
        let _ = self.incoming_tx.send((peer, env.frame)).await;

        Ok(())
    }

    pub async fn get_ack_seq_for_peer(&self, peer: NodeId) -> u64 {
        let dedup = self.dedup_table.lock().await;
        dedup.last_seen_seq(peer)
    }
}
