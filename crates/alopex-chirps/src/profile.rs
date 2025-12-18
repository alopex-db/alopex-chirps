use alopex_chirps_wire::frame::Frame;
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
pub fn enforce_profile(
    frame: &Frame,
    requested: MessageProfile,
) -> Result<MessageProfile, &'static str> {
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
    matches!(_frame, Frame::Raft(_) | Frame::RaftSnapshot(_))
}
