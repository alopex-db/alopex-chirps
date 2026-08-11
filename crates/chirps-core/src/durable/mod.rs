//! Provider-neutral contracts for the independent Durable message plane.

pub mod lifecycle;
pub mod message;
pub mod subscription;

pub use lifecycle::{
    AdmissionTicket, CheckpointOperationPhase, CheckpointOperationReport, ChirpsPlaneHealth,
    ControlPlaneHealth, ControlUnavailableReason, DurableEvent, DurableEventKind, DurableHealth,
    DurableMetricLabels, DurableTraceContext, LifecycleGate, LifecycleGeneration, LifecyclePhase,
    LifecycleTransitionError, MetricBoundary, MetricFailureStage, MetricOperation, MetricOutcome,
    OperationReportError, PartitionFaultReason, PartitionHealth, PartitionState, Readiness,
    RecoveryReason, RegisteredOperation, RegisteredOperationKind, SendOperationPhase,
    SendOperationReport, ShutdownFreeze, ShutdownFreezeError, ShutdownGeneration, ShutdownReport,
    ShutdownReportCache, ShutdownReportError, UnavailableReason,
};
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
