use crate::frame::Frame;
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read};
use std::time::{SystemTime, UNIX_EPOCH};

pub const FRAME_ENVELOPE_V2_HEADER_SIZE: usize = 29;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameEnvelopeV2 {
    pub kind: u8,
    pub seq: u64,
    pub ack_seq: u64,
    pub timestamp: u64,
    pub payload_len: u32,
    pub frame: Frame,
}

impl FrameEnvelopeV2 {
    pub fn new(kind: u8, seq: u64, ack_seq: u64, payload_len: u32, frame: Frame) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        FrameEnvelopeV2 {
            kind,
            seq,
            ack_seq,
            timestamp,
            payload_len,
            frame,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let body = bincode::serialize(&self.frame).expect("failed to serialize frame");
        self.encode_with_payload(&body)
    }

    /// Encode the envelope header using an already serialized frame body.
    /// This avoids a second bincode traversal in transports that already need
    /// the body for queue sizing or retransmission bookkeeping.
    pub fn encode_with_payload(&self, body: &[u8]) -> Vec<u8> {
        let payload_len = body.len() as u32;
        let mut buf = Vec::with_capacity(FRAME_ENVELOPE_V2_HEADER_SIZE + payload_len as usize);
        buf.push(self.kind);
        buf.extend_from_slice(&self.seq.to_be_bytes());
        buf.extend_from_slice(&self.ack_seq.to_be_bytes());
        buf.extend_from_slice(&self.timestamp.to_be_bytes());
        buf.extend_from_slice(&payload_len.to_be_bytes());
        buf.extend_from_slice(body);
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < FRAME_ENVELOPE_V2_HEADER_SIZE {
            return Err("not enough bytes for FrameEnvelopeV2 header".into());
        }
        let mut cursor = Cursor::new(bytes);
        let mut kind_buf = [0u8; 1];
        cursor
            .read_exact(&mut kind_buf)
            .map_err(|e| e.to_string())?;

        let mut seq_buf = [0u8; 8];
        cursor.read_exact(&mut seq_buf).map_err(|e| e.to_string())?;

        let mut ack_buf = [0u8; 8];
        cursor.read_exact(&mut ack_buf).map_err(|e| e.to_string())?;

        let mut ts_buf = [0u8; 8];
        cursor.read_exact(&mut ts_buf).map_err(|e| e.to_string())?;

        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf).map_err(|e| e.to_string())?;

        let payload_len = u32::from_be_bytes(len_buf);
        let body_offset = FRAME_ENVELOPE_V2_HEADER_SIZE;
        if bytes.len() < body_offset + payload_len as usize {
            return Err("payload length mismatch".into());
        }
        let frame: Frame =
            bincode::deserialize(&bytes[body_offset..body_offset + payload_len as usize])
                .map_err(|e| e.to_string())?;

        Ok(FrameEnvelopeV2 {
            kind: kind_buf[0],
            seq: u64::from_be_bytes(seq_buf),
            ack_seq: u64::from_be_bytes(ack_buf),
            timestamp: u64::from_be_bytes(ts_buf),
            payload_len,
            frame,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::FrameEnvelopeV2;
    use crate::frame::{Frame, RaftFrame};

    #[test]
    fn pre_serialized_payload_encoding_matches_normal_encoding() {
        let envelope = FrameEnvelopeV2::new(
            3,
            7,
            6,
            0,
            Frame::Raft(RaftFrame {
                group_id: 1,
                payload: vec![1, 2, 3],
            }),
        );
        let body = bincode::serialize(&envelope.frame).expect("frame body");
        assert_eq!(envelope.encode(), envelope.encode_with_payload(&body));
    }
}
