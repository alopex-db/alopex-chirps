use alopex_chirps_wire::envelope::{FRAME_ENVELOPE_V2_HEADER_SIZE, FrameEnvelopeV2};
use alopex_chirps_wire::frame::Frame;
use alopex_chirps_wire::node_id::NodeId;
use quinn::RecvStream;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::warn;

use crate::retransmit::{DeduplicationTable, RetransmissionBuffer};
use crate::{ExtendedTransportMetrics, StreamKind, TransportError};

const MAX_ENVELOPE_SIZE: usize = 256 * 1024; // defensive cap
const CHUNK_STREAM_MAGIC: u8 = 0x46;

pub struct ReceiveHandler {
    dedup_table: tokio::sync::Mutex<DeduplicationTable>,
    retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
    incoming_tx: mpsc::Sender<(NodeId, Frame)>,
    file_transfer_tx: Option<mpsc::Sender<(NodeId, RecvStream)>>,
    metrics: Arc<ExtendedTransportMetrics>,
}

impl ReceiveHandler {
    pub fn new(
        retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
        incoming_tx: mpsc::Sender<(NodeId, Frame)>,
        metrics: Arc<ExtendedTransportMetrics>,
    ) -> Self {
        ReceiveHandler::new_with_file_transfer(retransmit_buffer, incoming_tx, None, metrics)
    }

    pub fn new_with_file_transfer(
        retransmit_buffer: Arc<RwLock<RetransmissionBuffer>>,
        incoming_tx: mpsc::Sender<(NodeId, Frame)>,
        file_transfer_tx: Option<mpsc::Sender<(NodeId, RecvStream)>>,
        metrics: Arc<ExtendedTransportMetrics>,
    ) -> Self {
        ReceiveHandler {
            dedup_table: tokio::sync::Mutex::new(DeduplicationTable::new()),
            retransmit_buffer,
            incoming_tx,
            file_transfer_tx,
            metrics,
        }
    }

    pub async fn handle_stream(
        &self,
        peer: NodeId,
        mut recv: RecvStream,
    ) -> Result<(), TransportError> {
        let mut first_byte = [0u8; 1];
        recv.read_exact(&mut first_byte)
            .await
            .map_err(|e| TransportError::Io(e.to_string()))?;

        if first_byte[0] == CHUNK_STREAM_MAGIC {
            self.metrics.record_receive(StreamKind::FileTransfer, None);
            if let Some(tx) = &self.file_transfer_tx {
                tx.send((peer, recv))
                    .await
                    .map_err(|_| TransportError::Io("file transfer handler closed".into()))?;
            } else {
                warn!("file transfer stream received without handler from peer {peer:?}");
            }
            return Ok(());
        }

        let remaining = recv
            .read_to_end(MAX_ENVELOPE_SIZE.saturating_sub(1))
            .await
            .map_err(|e| TransportError::Io(e.to_string()))?;
        let mut bytes = Vec::with_capacity(1 + remaining.len());
        bytes.push(first_byte[0]);
        bytes.extend_from_slice(&remaining);

        if bytes.len() < FRAME_ENVELOPE_V2_HEADER_SIZE {
            return Err(TransportError::Io("empty stream".into()));
        }

        let env = FrameEnvelopeV2::decode(&bytes).map_err(TransportError::Io)?;

        let kind = match StreamKind::try_from(env.kind) {
            Ok(k) => k,
            Err(err) => {
                warn!("invalid stream kind {} from peer {peer:?}: {err}", env.kind);
                return Err(err);
            }
        };

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
