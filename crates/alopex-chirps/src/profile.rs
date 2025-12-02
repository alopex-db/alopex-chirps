use chirps_wire::frame::Frame;
use tracing::warn;

/// Messaging profile to control reliability/priority semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageProfile {
    /// Control/reliable path (Raft/handshake/snapshot).
    Control,
    /// Best-effort path without retransmission.
    Ephemeral,
    /// Durable messaging (stub; not implemented in v0.4).
    Durable,
}

/// Decide effective profile for a frame. Raft/snapshot traffic must use Control.
pub fn enforce_profile(frame: &Frame, requested: MessageProfile) -> Result<MessageProfile, &'static str> {
    if matches!(requested, MessageProfile::Durable) {
        warn!("Durable profile requested but not implemented in v0.4");
        return Err("Durable profile is not implemented in v0.4");
    }

    if is_raft_frame(frame) && matches!(requested, MessageProfile::Ephemeral) {
        warn!("Ephemeral requested for Raft traffic; overriding to Control");
        return Ok(MessageProfile::Control);
    }

    Ok(requested)
}

fn is_raft_frame(_frame: &Frame) -> bool {
    // Placeholder: chirps-wire frames currently don't encode Raft messages explicitly.
    // When Raft wire types land, update this matcher to detect AppendEntries/Vote/Snapshot frames.
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use chirps_wire::frame::{Frame, UserMessage};
    use tracing_test::traced_test;

    fn user_frame() -> Frame {
        Frame::User(UserMessage {
            payload: b"hello".to_vec(),
        })
    }

    #[test]
    fn control_pass_through() {
        let frame = user_frame();
        let eff = enforce_profile(&frame, MessageProfile::Control).unwrap();
        assert_eq!(eff, MessageProfile::Control);
    }

    #[test]
    fn ephemeral_pass_through_when_not_raft() {
        let frame = user_frame();
        let eff = enforce_profile(&frame, MessageProfile::Ephemeral).unwrap();
        assert_eq!(eff, MessageProfile::Ephemeral);
    }

    #[traced_test]
    #[test]
    fn durable_returns_error_and_logs_warn() {
        let frame = user_frame();
        let res = enforce_profile(&frame, MessageProfile::Durable);
        assert!(res.is_err());
        assert!(logs_contain("Durable profile requested"));
    }
}
