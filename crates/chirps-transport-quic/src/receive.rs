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
pub(crate) const RAFT_BATCH_STREAM_MAGIC: u8 = 0xB7;
const MAX_BATCH_ENVELOPES_PER_STREAM: usize = 1024;

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

        if first_byte[0] == RAFT_BATCH_STREAM_MAGIC {
            let mut envelope_count = 0;
            loop {
                let mut length_buf = [0u8; 4];
                if read_optional_exact(&mut recv, &mut length_buf).await? {
                    break;
                }
                let length = u32::from_be_bytes(length_buf) as usize;
                if !(FRAME_ENVELOPE_V2_HEADER_SIZE..=MAX_ENVELOPE_SIZE).contains(&length) {
                    return Err(TransportError::Io(
                        "invalid raft batch envelope length".into(),
                    ));
                }
                let mut encoded = vec![0u8; length];
                read_required_exact(&mut recv, &mut encoded).await?;
                let envelope = FrameEnvelopeV2::decode(&encoded).map_err(TransportError::Io)?;
                self.process_envelope(peer, envelope).await?;
                envelope_count += 1;
                if envelope_count > MAX_BATCH_ENVELOPES_PER_STREAM {
                    return Err(TransportError::Io(
                        "raft batch stream envelope limit exceeded".into(),
                    ));
                }
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

        self.process_envelope(peer, env).await
    }

    async fn process_envelope(
        &self,
        peer: NodeId,
        env: FrameEnvelopeV2,
    ) -> Result<(), TransportError> {
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
            self.metrics
                .update_buffer_bytes(buf.total_buffered_bytes() as u64);
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

async fn read_optional_exact(
    recv: &mut RecvStream,
    buf: &mut [u8],
) -> Result<bool, TransportError> {
    let mut offset = 0;
    while offset < buf.len() {
        let read = recv
            .read(&mut buf[offset..])
            .await
            .map_err(|e| TransportError::Io(e.to_string()))?;
        match read {
            None | Some(0) if offset == 0 => return Ok(true),
            None | Some(0) => {
                return Err(TransportError::Io("truncated raft batch length".into()));
            }
            Some(read) => offset += read,
        }
    }
    Ok(false)
}

async fn read_required_exact(recv: &mut RecvStream, buf: &mut [u8]) -> Result<(), TransportError> {
    let mut offset = 0;
    while offset < buf.len() {
        let read = recv
            .read(&mut buf[offset..])
            .await
            .map_err(|e| TransportError::Io(e.to_string()))?;
        match read {
            None | Some(0) => {
                return Err(TransportError::Io("truncated raft batch envelope".into()));
            }
            Some(read) => offset += read,
        }
    }
    Ok(())
}

#[cfg(test)]
fn decode_batch_envelopes(bytes: &[u8]) -> Result<Vec<FrameEnvelopeV2>, TransportError> {
    let mut offset = 0usize;
    let mut envelopes = Vec::new();
    while offset < bytes.len() {
        if bytes.len() - offset < 4 {
            return Err(TransportError::Io("truncated raft batch length".into()));
        }
        let length = u32::from_be_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .map_err(|_| TransportError::Io("invalid raft batch length".into()))?,
        ) as usize;
        offset += 4;
        if !(FRAME_ENVELOPE_V2_HEADER_SIZE..=MAX_ENVELOPE_SIZE).contains(&length)
            || bytes.len() - offset < length
        {
            return Err(TransportError::Io(
                "invalid raft batch envelope length".into(),
            ));
        }
        let envelope =
            FrameEnvelopeV2::decode(&bytes[offset..offset + length]).map_err(TransportError::Io)?;
        envelopes.push(envelope);
        offset += length;
    }
    Ok(envelopes)
}

#[cfg(test)]
mod batch_tests {
    use super::decode_batch_envelopes;
    use alopex_chirps_wire::envelope::FrameEnvelopeV2;
    use alopex_chirps_wire::frame::{Frame, RaftFrame};

    #[test]
    fn batch_decoder_preserves_envelope_order() {
        let first = FrameEnvelopeV2::new(
            3,
            7,
            0,
            0,
            Frame::Raft(RaftFrame {
                group_id: 1,
                payload: vec![],
            }),
        );
        let second = FrameEnvelopeV2::new(
            3,
            8,
            7,
            0,
            Frame::Raft(RaftFrame {
                group_id: 1,
                payload: vec![],
            }),
        );
        let mut bytes = Vec::new();
        for envelope in [&first, &second] {
            let encoded = envelope.encode();
            bytes.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&encoded);
        }
        let decoded = decode_batch_envelopes(&bytes).expect("valid batch");
        assert_eq!(
            decoded.iter().map(|item| item.seq).collect::<Vec<_>>(),
            [7, 8]
        );
    }

    #[test]
    fn batch_decoder_rejects_truncated_lengths() {
        assert!(decode_batch_envelopes(&[0, 0, 0]).is_err());
        let mut bytes = 10u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0; 10]);
        assert!(decode_batch_envelopes(&bytes).is_err());
    }
}
