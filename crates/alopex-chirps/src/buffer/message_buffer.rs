use super::{BackpressureController, BackpressureLevel, PriorityQueue};
use crate::MessageProfile;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BufferError {
    #[error("message buffer backpressure at {level:?}: requested {requested_bytes} bytes")]
    Backpressure {
        level: BackpressureLevel,
        requested_bytes: usize,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub struct BufferedMessage {
    pub profile: MessageProfile,
    pub payload: Vec<u8>,
}

/// Node-wide bounded receive buffer shared by all delivery profiles.
#[derive(Debug)]
pub struct MessageBuffer {
    max_buffer_bytes: usize,
    used_bytes: usize,
    controller: BackpressureController,
    queue: PriorityQueue<BufferedMessage>,
    profile_bytes: [usize; 3],
}

impl MessageBuffer {
    pub fn new(max_buffer_bytes: usize, warning_threshold: f32, limited_threshold: f32) -> Self {
        Self {
            max_buffer_bytes,
            used_bytes: 0,
            controller: BackpressureController::new(warning_threshold, limited_threshold),
            queue: PriorityQueue::default(),
            profile_bytes: [0; 3],
        }
    }

    pub fn push(&mut self, profile: MessageProfile, payload: Vec<u8>) -> Result<(), BufferError> {
        let requested_bytes = payload.len();
        let projected = self.used_bytes.saturating_add(requested_bytes);
        let level = if projected == self.max_buffer_bytes {
            BackpressureLevel::Limited
        } else {
            self.controller.level(projected, self.max_buffer_bytes)
        };
        if !self.controller.allows(profile, level) {
            return Err(BufferError::Backpressure {
                level,
                requested_bytes,
            });
        }

        self.used_bytes = projected;
        self.profile_bytes[profile_index(profile)] += requested_bytes;
        self.queue
            .push(profile, BufferedMessage { profile, payload });
        Ok(())
    }

    pub fn pop(&mut self) -> Option<BufferedMessage> {
        let (profile, message) = self.queue.pop()?;
        let bytes = message.payload.len();
        self.used_bytes = self.used_bytes.saturating_sub(bytes);
        let profile_bytes = &mut self.profile_bytes[profile_index(profile)];
        *profile_bytes = profile_bytes.saturating_sub(bytes);
        Some(message)
    }

    pub fn max_buffer_bytes(&self) -> usize {
        self.max_buffer_bytes
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    pub fn backpressure_level(&self) -> BackpressureLevel {
        self.controller
            .level(self.used_bytes, self.max_buffer_bytes)
    }

    pub fn bytes_for(&self, profile: MessageProfile) -> usize {
        self.profile_bytes[profile_index(profile)]
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

const fn profile_index(profile: MessageProfile) -> usize {
    match profile {
        MessageProfile::Control => 0,
        MessageProfile::Durable => 1,
        MessageProfile::Ephemeral => 2,
    }
}
