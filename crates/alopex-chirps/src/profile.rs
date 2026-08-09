use alopex_chirps_wire::frame::Frame;
use thiserror::Error;
use tracing::warn;

pub use alopex_chirps_core::backend::{BackendCapabilities, BackendProfile, EnvelopeMetadata};

/// Backwards-compatible name for the delivery profile.
pub type MessageProfile = BackendProfile;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProfileError {
    #[error("message profile {profile:?} is not supported: {reason}")]
    Unsupported {
        profile: MessageProfile,
        reason: &'static str,
    },
}

/// Resolves the effective profile against explicit backend capabilities.
pub fn resolve_profile(
    frame: &Frame,
    requested: MessageProfile,
    capabilities: BackendCapabilities,
) -> Result<MessageProfile, ProfileError> {
    let effective = if is_raft_frame(frame) && requested == MessageProfile::Ephemeral {
        warn!("Ephemeral requested for Raft traffic; overriding to Control");
        MessageProfile::Control
    } else {
        requested
    };

    if !capabilities.supports(effective) {
        if effective == MessageProfile::Durable {
            warn!("Durable profile requested but the backend has no durable capability");
        }
        return Err(ProfileError::Unsupported {
            profile: effective,
            reason: "backend capability is unavailable",
        });
    }
    Ok(effective)
}

/// Backwards-compatible profile enforcement using the v0.6 default backend capabilities.
pub fn enforce_profile(
    frame: &Frame,
    requested: MessageProfile,
) -> Result<MessageProfile, &'static str> {
    resolve_profile(frame, requested, BackendCapabilities::default()).map_err(|error| match error {
        ProfileError::Unsupported {
            profile: MessageProfile::Durable,
            ..
        } => "Durable profile is not implemented in v0.6",
        ProfileError::Unsupported { .. } => "requested profile is not supported by backend",
    })
}

fn is_raft_frame(frame: &Frame) -> bool {
    matches!(frame, Frame::Raft(_) | Frame::RaftSnapshot(_))
}
