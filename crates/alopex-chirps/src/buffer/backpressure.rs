use crate::MessageProfile;

/// The pressure stage of a node-wide receive buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureLevel {
    Normal,
    Warning,
    Limited,
    Reject,
}

/// Decides which profile traffic can enter a bounded buffer.
#[derive(Debug, Clone, Copy)]
pub struct BackpressureController {
    warning_threshold: f32,
    limited_threshold: f32,
}

impl BackpressureController {
    pub fn new(warning_threshold: f32, limited_threshold: f32) -> Self {
        assert!(warning_threshold > 0.0 && warning_threshold < 1.0);
        assert!(limited_threshold > warning_threshold && limited_threshold < 1.0);
        Self {
            warning_threshold,
            limited_threshold,
        }
    }

    pub fn level(&self, used_bytes: usize, max_bytes: usize) -> BackpressureLevel {
        if max_bytes == 0 || used_bytes >= max_bytes {
            return BackpressureLevel::Reject;
        }
        let ratio = used_bytes as f32 / max_bytes as f32;
        if ratio >= self.limited_threshold {
            BackpressureLevel::Limited
        } else if ratio >= self.warning_threshold {
            BackpressureLevel::Warning
        } else {
            BackpressureLevel::Normal
        }
    }

    pub(crate) fn allows(&self, profile: MessageProfile, level: BackpressureLevel) -> bool {
        match level {
            BackpressureLevel::Normal | BackpressureLevel::Warning => true,
            BackpressureLevel::Limited => profile != MessageProfile::Ephemeral,
            BackpressureLevel::Reject => false,
        }
    }
}
