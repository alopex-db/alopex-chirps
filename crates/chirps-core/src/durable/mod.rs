//! Provider-neutral contracts for the independent Durable message plane.

pub mod message;
pub mod subscription;

pub use message::{
    AttemptBinding, AttemptFailureKind, AttemptPhase, CanonicalEnvelope, ConfirmationBoundary,
    DurableAttemptId, DurableMessageId, DurableMessageRoute, DurableReceipt, DurableSendOutcome,
    DurableSendResult, EnvelopeDigest, MessageIdError, PayloadDigest, PreflightFailure,
    PreflightFailureKind, PreflightResult, PrepareFailure, PrepareResult, PreparedDurableSend,
    ReceiptError, ResourceEpoch, ResourceId, ResultShapeError, SessionFingerprint,
};
pub use subscription::{
    CheckedPollRecord, CheckpointDirectoryId, CheckpointInstallBinding, CheckpointOutcome,
    CreationFailureKind, CreationRecoveryBinding, Delivery, DeliveryContext, DeliveryHandle,
    DeliveryHandleState, HandleTransitionError, InitialPosition, PollObservation,
    PollObservationError, PollRecordError, PollResolution, ReplayError, SubscriptionBinding,
    SubscriptionCreationOutcome, SubscriptionId, expected_offset,
};
