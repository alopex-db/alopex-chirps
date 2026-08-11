//! Provider-neutral contracts for the independent Durable message plane.

pub mod message;

pub use message::{
    AttemptBinding, AttemptFailureKind, AttemptPhase, CanonicalEnvelope, ConfirmationBoundary,
    DurableAttemptId, DurableMessageId, DurableMessageRoute, DurableReceipt, DurableSendOutcome,
    DurableSendResult, EnvelopeDigest, MessageIdError, PayloadDigest, PreflightFailure,
    PreflightFailureKind, PreflightResult, PrepareFailure, PrepareResult, PreparedDurableSend,
    ReceiptError, ResourceEpoch, ResourceId, ResultShapeError, SessionFingerprint,
};
