use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bincode::serialized_size;
use chirps_wire::{frame::Frame, node_id::NodeId};

#[derive(Debug, Clone, Default)]
pub struct BufferStats {
    pub buffered_count: usize,
    pub buffered_bytes: usize,
    pub next_seq: u64,
    pub acked_seq: u64,
    pub unacked_count: usize,
}

/// Track last-seen sequence numbers per peer to perform receive-side deduplication.
#[derive(Default)]
pub struct DeduplicationTable {
    last_seen: std::collections::HashMap<NodeId, u64>,
    window_size: usize,
}

impl DeduplicationTable {
    pub fn new() -> Self {
        DeduplicationTable {
            last_seen: std::collections::HashMap::new(),
            window_size: 1000,
        }
    }

    pub fn with_window(window_size: usize) -> Self {
        DeduplicationTable {
            last_seen: std::collections::HashMap::new(),
            window_size,
        }
    }

    /// Returns true if this seq is new and updates the last-seen value; false if duplicate/old.
    pub fn check_and_update(&mut self, peer: NodeId, seq: u64) -> bool {
        let entry = self.last_seen.entry(peer).or_insert(0);
        if seq <= *entry {
            false
        } else {
            *entry = seq;
            true
        }
    }

    pub fn last_seen_seq(&self, peer: NodeId) -> u64 {
        self.last_seen.get(&peer).copied().unwrap_or(0)
    }

    pub fn remove_peer(&mut self, peer: NodeId) {
        self.last_seen.remove(&peer);
    }
}

#[derive(Debug)]
pub enum BufferError {
    Serialize(String),
    SequenceExhausted,
}

#[derive(Debug, Clone)]
pub struct RetransmitConfig {
    pub max_buffer_bytes: usize,
    pub max_messages_per_peer: usize,
    pub message_ttl: Duration,
}

impl Default for RetransmitConfig {
    fn default() -> Self {
        Self {
            max_buffer_bytes: 32 * 1024 * 1024,
            max_messages_per_peer: 10_000,
            message_ttl: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BufferedMessage {
    pub seq: u64,
    pub frame: Frame,
    pub size_bytes: usize,
    pub timestamp: Instant,
}

#[derive(Default)]
struct PeerBuffer {
    messages: VecDeque<BufferedMessage>,
    total_bytes: usize,
    next_seq: u64,
    acked_seq: u64,
}

impl PeerBuffer {
    fn new() -> Self {
        PeerBuffer {
            messages: VecDeque::new(),
            total_bytes: 0,
            next_seq: 1,
            acked_seq: 0,
        }
    }
}

pub struct RetransmissionBuffer {
    buffers: std::collections::HashMap<NodeId, PeerBuffer>,
    config: RetransmitConfig,
}

impl RetransmissionBuffer {
    pub fn new(config: RetransmitConfig) -> Self {
        RetransmissionBuffer {
            buffers: std::collections::HashMap::new(),
            config,
        }
    }

    /// Buffer a frame for a peer, assigning a monotonically increasing sequence number.
    /// Returns the assigned sequence number. Drops oldest messages on overflow.
    pub fn buffer(&mut self, peer: NodeId, frame: Frame) -> Result<u64, BufferError> {
        let peer_buf = self.buffers.entry(peer).or_insert_with(PeerBuffer::new);
        let seq = peer_buf.next_seq;
        peer_buf.next_seq = peer_buf
            .next_seq
            .checked_add(1)
            .ok_or(BufferError::SequenceExhausted)?;

        let size_bytes = serialized_size(&frame)
            .map(|s| s as usize)
            .map_err(|e| BufferError::Serialize(e.to_string()))?;
        let msg = BufferedMessage {
            seq,
            frame,
            size_bytes,
            timestamp: Instant::now(),
        };
        peer_buf.total_bytes = peer_buf.total_bytes.saturating_add(msg.size_bytes);
        peer_buf.messages.push_back(msg);

        let _dropped = self.handle_overflow(peer);

        Ok(seq)
    }

    /// Process an ACK, removing all messages with seq <= ack_seq.
    /// Returns the count of removed messages.
    pub fn process_ack(&mut self, peer: NodeId, ack_seq: u64) -> usize {
        let Some(buf) = self.buffers.get_mut(&peer) else {
            return 0;
        };

        buf.acked_seq = buf.acked_seq.max(ack_seq);

        let mut removed = 0;
        while let Some(front) = buf.messages.front() {
            if front.seq <= ack_seq {
                buf.total_bytes = buf.total_bytes.saturating_sub(front.size_bytes);
                buf.messages.pop_front();
                removed += 1;
            } else {
                break;
            }
        }

        removed
    }

    /// Return unacked messages in sequence order for retransmission. Messages remain buffered.
    pub fn drain_for_retransmit(&mut self, peer: NodeId) -> Vec<BufferedMessage> {
        // Ensure TTL/limit trimming happens even if no new buffer() calls occurred recently.
        let _ = self.handle_overflow(peer);

        self.buffers
            .get(&peer)
            .map(|buf| {
                buf.messages
                    .iter()
                    .filter(|m| m.seq > buf.acked_seq)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drops expired or overflowing messages; returns count dropped.
    pub fn handle_overflow(&mut self, peer: NodeId) -> usize {
        let Some(buf) = self.buffers.get_mut(&peer) else {
            return 0;
        };

        let mut dropped = 0;
        let now = Instant::now();

        // Expire old messages based on TTL.
        while let Some(front) = buf.messages.front() {
            if now.duration_since(front.timestamp) > self.config.message_ttl {
                buf.total_bytes = buf.total_bytes.saturating_sub(front.size_bytes);
                buf.messages.pop_front();
                dropped += 1;
            } else {
                break;
            }
        }

        // Enforce size and count limits by dropping oldest first.
        while buf.total_bytes > self.config.max_buffer_bytes
            || buf.messages.len() > self.config.max_messages_per_peer
        {
            if let Some(front) = buf.messages.pop_front() {
                buf.total_bytes = buf.total_bytes.saturating_sub(front.size_bytes);
                dropped += 1;
            } else {
                break;
            }
        }

        dropped
    }

    pub fn stats(&self, peer: NodeId) -> BufferStats {
        if let Some(buf) = self.buffers.get(&peer) {
            let unacked_count = buf
                .messages
                .iter()
                .filter(|m| m.seq > buf.acked_seq)
                .count();
            BufferStats {
                buffered_count: buf.messages.len(),
                buffered_bytes: buf.total_bytes,
                next_seq: buf.next_seq,
                acked_seq: buf.acked_seq,
                unacked_count,
            }
        } else {
            BufferStats::default()
        }
    }

    /// Get the last acknowledged sequence number for a peer (used as ack_seq when sending).
    pub fn get_ack_seq(&self, peer: NodeId) -> u64 {
        self.buffers.get(&peer).map(|b| b.acked_seq).unwrap_or(0)
    }
}
