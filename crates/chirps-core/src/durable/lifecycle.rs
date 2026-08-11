use super::message::{AttemptFailureKind, DurableAttemptId, DurableSendOutcome, ResourceEpoch};
use super::subscription::CheckpointOutcome;
use thiserror::Error;

/// Durable lifecycle axis. It is deliberately independent of readiness and
/// partition health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecyclePhase {
    /// Local state and provider capability are still being established.
    Starting,
    /// New work may be admitted for the current lifecycle generation.
    Ready,
    /// Admission is closed and the started-operation set is being drained.
    Draining,
    /// Owned transport is closed and registered workers have joined.
    Closed,
}

/// Bounded reasons why Durable cannot currently accept work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum UnavailableReason {
    /// Durable was not configured.
    #[error("durable plane is unconfigured")]
    Unconfigured,
    /// The provider session is not connected.
    #[error("durable connectivity is unavailable")]
    Connectivity,
    /// Required provider capability does not match.
    #[error("durable capability does not match")]
    CapabilityMismatch,
    /// Authentication failed before data-plane use.
    #[error("durable authentication failed")]
    Authentication,
    /// The runtime principal lacks a required permission.
    #[error("durable permission is unavailable")]
    Permission,
    /// TLS peer identity validation failed.
    #[error("durable TLS identity does not match")]
    TlsIdentityMismatch,
    /// A hard local capacity gate is closed.
    #[error("durable capacity is unavailable")]
    Capacity,
    /// Shutdown has closed new Durable admission.
    #[error("durable plane is shutting down")]
    Shutdown,
}

/// Bounded reasons requiring explicit recovery before readiness can return.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum RecoveryReason {
    /// Checkpoint install reachability is unknown.
    #[error("checkpoint installation is indeterminate")]
    CheckpointIndeterminate,
    /// The selected replay frontier has been evicted.
    #[error("retention gap requires recovery")]
    RetentionGap,
    /// The observed broker resource incarnation changed.
    #[error("resource epoch does not match")]
    ResourceEpochMismatch,
    /// Canonical local state cannot be proven valid.
    #[error("durable local state is corrupt")]
    CorruptState,
    /// The durable local owner epoch does not match.
    #[error("durable owner epoch does not match")]
    OwnerEpochMismatch,
}

/// Readiness axis, orthogonal to [`LifecyclePhase`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Readiness {
    /// Capability, session, and canonical local state are valid.
    Available,
    /// A bounded transient/configuration reason prevents new work.
    Unavailable(UnavailableReason),
    /// Explicit recovery is required; no automatic skip is allowed.
    RecoveryRequired(RecoveryReason),
}

/// Bounded fail-stop reasons for one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum PartitionFaultReason {
    /// A canonical envelope failed validation.
    #[error("partition observed an invalid envelope")]
    InvalidEnvelope,
    /// Atomic poll framing or the replay truth table was invalid.
    #[error("partition observed an invalid poll observation")]
    InvalidPollObservation,
    /// A record did not match the canonical inclusive expected offset.
    #[error("partition observed an offset conflict")]
    OffsetConflict,
    /// Hard capacity prevents preserving additional live state.
    #[error("partition capacity is unavailable")]
    Capacity,
    /// A delivery handle did not match current owner/attempt identity.
    #[error("partition observed a stale delivery handle")]
    StaleHandle,
}

/// Per-partition state, independent of global Durable readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PartitionState {
    /// The partition may poll/deliver for its current owner.
    Active,
    /// A bounded local protocol/data fault stopped this partition.
    Faulted(PartitionFaultReason),
    /// Durable recovery is required before reactivation.
    RecoveryRequired(RecoveryReason),
}

/// One partition's provider-neutral health state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionHealth {
    partition: u32,
    state: PartitionState,
}

impl PartitionHealth {
    /// Creates one explicit partition health entry.
    #[must_use]
    pub const fn new(partition: u32, state: PartitionState) -> Self {
        Self { partition, state }
    }

    /// Returns the explicit partition number.
    #[must_use]
    pub const fn partition(self) -> u32 {
        self.partition
    }

    /// Returns this partition's state axis.
    #[must_use]
    pub const fn state(self) -> PartitionState {
        self.state
    }
}

/// Snapshot of the Durable plane's independent health axes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableHealth {
    lifecycle: LifecyclePhase,
    readiness: Readiness,
    partitions: Vec<PartitionHealth>,
}

impl DurableHealth {
    /// Creates an orthogonal health snapshot. Combinations such as
    /// `Ready + Unavailable` and `Draining + Available` remain representable.
    #[must_use]
    pub fn new(
        lifecycle: LifecyclePhase,
        readiness: Readiness,
        partitions: Vec<PartitionHealth>,
    ) -> Self {
        Self {
            lifecycle,
            readiness,
            partitions,
        }
    }

    /// Returns the lifecycle axis.
    #[must_use]
    pub const fn lifecycle(&self) -> LifecyclePhase {
        self.lifecycle
    }

    /// Returns the readiness axis.
    #[must_use]
    pub const fn readiness(&self) -> Readiness {
        self.readiness
    }

    /// Returns provider-neutral per-partition health entries.
    #[must_use]
    pub fn partitions(&self) -> &[PartitionHealth] {
        &self.partitions
    }
}

/// Bounded Control-plane health, kept separate from Durable shutdown/failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlPlaneHealth {
    /// Control workers remain available.
    Available,
    /// Control workers are unavailable for a bounded operational reason.
    Unavailable(ControlUnavailableReason),
}

/// Bounded Control-plane health reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum ControlUnavailableReason {
    /// The Control transport is unavailable.
    #[error("control transport is unavailable")]
    Transport,
    /// Control shutdown was requested explicitly.
    #[error("control plane is shutting down")]
    Shutdown,
}

/// Separate Control and Durable health snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChirpsPlaneHealth {
    control: ControlPlaneHealth,
    durable: DurableHealth,
}

impl ChirpsPlaneHealth {
    /// Creates a snapshot without coupling either plane's lifecycle.
    #[must_use]
    pub const fn new(control: ControlPlaneHealth, durable: DurableHealth) -> Self {
        Self { control, durable }
    }

    /// Returns Control-plane health.
    #[must_use]
    pub const fn control(&self) -> ControlPlaneHealth {
        self.control
    }

    /// Returns Durable-plane health.
    #[must_use]
    pub const fn durable(&self) -> &DurableHealth {
        &self.durable
    }
}

/// Bounded operation label allowed on Durable metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricOperation {
    /// Send attempt.
    Send,
    /// Checked poll.
    Poll,
    /// Canonical checkpoint install.
    Checkpoint,
    /// Explicit recovery.
    Recovery,
    /// Capacity admission.
    Capacity,
}

/// Bounded durability/operation boundary label allowed on metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricBoundary {
    /// No durable mutation boundary has begun.
    BeforeMutation,
    /// Append invocation has begun.
    AppendInvoked,
    /// Broker acknowledgement only.
    BrokerAccepted,
    /// Exact compatible OS-sync acknowledgement.
    OsSyncedAccepted,
    /// Canonical checkpoint installation.
    CheckpointInstall,
}

/// Bounded failure-stage label allowed on metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricFailureStage {
    /// No failure.
    None,
    /// State-free preparation.
    Prepare,
    /// Session/capability preflight.
    Preflight,
    /// Owned transport invocation.
    Transport,
    /// Provider response verification.
    Response,
    /// Checked poll decode/truth table.
    PollDecode,
    /// Checkpoint candidate write.
    CheckpointWrite,
    /// File sync.
    FileSync,
    /// Atomic rename.
    Rename,
    /// Parent directory sync.
    DirectorySync,
    /// Explicit recovery.
    Recovery,
    /// Hard capacity admission.
    Capacity,
}

/// Bounded result label allowed on metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricOutcome {
    /// Operation succeeded at its requested boundary.
    Success,
    /// Send was known not submitted.
    NotSubmitted,
    /// Send result is ambiguous.
    Indeterminate,
    /// Checkpoint/creation is known not committed.
    NotCommitted,
    /// Durable mutation reachability is unknown.
    Unknown,
    /// A duplicate was detected.
    Duplicate,
    /// A retention gap was detected.
    Gap,
    /// Explicit recovery is required.
    RecoveryRequired,
    /// Operation is unavailable before mutation.
    Unavailable,
}

/// Low-cardinality metric labels only.
///
/// Message IDs, targets, payloads, credentials, partition numbers, owner
/// epochs, and resource epochs are intentionally absent. Correlation identity
/// belongs in structured logs through [`DurableTraceContext`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DurableMetricLabels {
    operation: MetricOperation,
    boundary: MetricBoundary,
    failure_stage: MetricFailureStage,
    outcome: MetricOutcome,
}

impl DurableMetricLabels {
    /// Creates a label set from bounded enums only.
    #[must_use]
    pub const fn new(
        operation: MetricOperation,
        boundary: MetricBoundary,
        failure_stage: MetricFailureStage,
        outcome: MetricOutcome,
    ) -> Self {
        Self {
            operation,
            boundary,
            failure_stage,
            outcome,
        }
    }

    /// Returns the bounded operation label.
    #[must_use]
    pub const fn operation(self) -> MetricOperation {
        self.operation
    }

    /// Returns the bounded boundary label.
    #[must_use]
    pub const fn boundary(self) -> MetricBoundary {
        self.boundary
    }

    /// Returns the bounded failure-stage label.
    #[must_use]
    pub const fn failure_stage(self) -> MetricFailureStage {
        self.failure_stage
    }

    /// Returns the bounded outcome label.
    #[must_use]
    pub const fn outcome(self) -> MetricOutcome {
        self.outcome
    }
}

/// Bounded kind for traceable structured Durable events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurableEventKind {
    /// Partition or operation fault.
    Fault,
    /// Explicit recovery attempt/result.
    Recovery,
    /// Redelivery after no checkpoint advance.
    Redelivery,
    /// Checkpoint transition.
    Checkpoint,
    /// Broker resource resynchronization.
    ResourceResync,
    /// Provider session rebind.
    SessionRebind,
    /// Duplicate logical identity observation.
    Duplicate,
    /// Retention gap observation.
    Gap,
}

/// High-cardinality correlation context for structured logs, never metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurableTraceContext {
    /// Correlates a send event to one explicit attempt.
    Attempt(DurableAttemptId),
    /// Correlates partition state to owner and resource incarnation.
    Partition {
        /// Explicit partition.
        partition: u32,
        /// Durable local owner epoch.
        owner_epoch: u64,
        /// Broker resource incarnation.
        resource_epoch: ResourceEpoch,
    },
}

impl DurableTraceContext {
    /// Creates an attempt trace context.
    #[must_use]
    pub const fn attempt(attempt_id: DurableAttemptId) -> Self {
        Self::Attempt(attempt_id)
    }

    /// Creates a partition/owner/resource trace context.
    #[must_use]
    pub const fn partition(
        partition: u32,
        owner_epoch: u64,
        resource_epoch: ResourceEpoch,
    ) -> Self {
        Self::Partition {
            partition,
            owner_epoch,
            resource_epoch,
        }
    }
}

/// One structured event with bounded kind and non-metric trace context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DurableEvent {
    kind: DurableEventKind,
    trace: DurableTraceContext,
}

impl DurableEvent {
    /// Creates a traceable event.
    #[must_use]
    pub const fn new(kind: DurableEventKind, trace: DurableTraceContext) -> Self {
        Self { kind, trace }
    }

    /// Returns the bounded event kind.
    #[must_use]
    pub const fn kind(self) -> DurableEventKind {
        self.kind
    }

    /// Returns structured-log correlation context.
    #[must_use]
    pub const fn trace(self) -> DurableTraceContext {
        self.trace
    }
}

/// Monotonic lifecycle generation shared by admission and registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LifecycleGeneration(u64);

impl LifecycleGeneration {
    /// Returns the numeric generation for persistence/diagnostics.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Generation that owns one idempotent shutdown sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShutdownGeneration(u64);

impl ShutdownGeneration {
    /// Reconstructs a generation from verified lifecycle state.
    #[must_use]
    pub const fn from_value(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric generation.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Admission proof captured before registration races shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AdmissionTicket {
    generation: LifecycleGeneration,
}

impl AdmissionTicket {
    /// Returns the generation captured by admission.
    #[must_use]
    pub const fn generation(self) -> LifecycleGeneration {
        self.generation
    }
}

/// Bounded kind of a lifecycle-registered operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegisteredOperationKind {
    /// Durable send attempt.
    Send,
    /// Canonical checkpoint installation.
    Checkpoint,
    /// Owned worker task.
    Worker,
    /// State compaction operation.
    Compaction,
}

/// Successfully registered operation bound to the admission generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RegisteredOperation {
    generation: LifecycleGeneration,
    kind: RegisteredOperationKind,
}

impl RegisteredOperation {
    /// Returns the registration generation.
    #[must_use]
    pub const fn generation(self) -> LifecycleGeneration {
        self.generation
    }

    /// Returns the bounded operation kind.
    #[must_use]
    pub const fn kind(self) -> RegisteredOperationKind {
        self.kind
    }
}

/// Immutable snapshot of operations that had registered before shutdown.
///
/// Construction also proves that delivery handles were fenced before the
/// snapshot became observable. This keeps post-freeze work out of the started
/// set and prevents pre-freeze handles from advancing canonical state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownFreeze {
    generation: ShutdownGeneration,
    started_operations: Vec<RegisteredOperation>,
}

impl ShutdownFreeze {
    /// Freezes operations from the lifecycle generation immediately preceding
    /// `generation`, after delivery handles have been fenced.
    pub fn try_new(
        generation: ShutdownGeneration,
        started_operations: Vec<RegisteredOperation>,
        handles_fenced: bool,
    ) -> Result<Self, ShutdownFreezeError> {
        if !handles_fenced {
            return Err(ShutdownFreezeError::HandlesNotFenced);
        }
        for operation in &started_operations {
            if operation.generation.value().checked_add(1) != Some(generation.value()) {
                return Err(ShutdownFreezeError::OperationGenerationMismatch {
                    operation: operation.generation.value(),
                    shutdown: generation.value(),
                });
            }
        }
        Ok(Self {
            generation,
            started_operations,
        })
    }

    /// Returns the generation that owns this frozen set.
    #[must_use]
    pub const fn generation(&self) -> ShutdownGeneration {
        self.generation
    }

    /// Returns the complete immutable set of operations started before drain.
    #[must_use]
    pub fn started_operations(&self) -> &[RegisteredOperation] {
        &self.started_operations
    }

    /// A constructed freeze always proves delivery-handle fencing.
    #[must_use]
    pub const fn handles_fenced(&self) -> bool {
        true
    }
}

/// A rejected shutdown freeze boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ShutdownFreezeError {
    /// Delivery handles were not fenced before freezing the started set.
    #[error("delivery handles must be fenced before freezing started operations")]
    HandlesNotFenced,
    /// A registered operation does not belong to the prior lifecycle
    /// generation.
    #[error(
        "operation generation {operation} is not immediately before shutdown generation {shutdown}"
    )]
    OperationGenerationMismatch {
        /// Registration generation carried by the operation.
        operation: u64,
        /// Shutdown generation being constructed.
        shutdown: u64,
    },
}

/// Send phase frozen by shutdown before classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOperationPhase {
    /// No started operation exists; it must not enter the frozen set.
    Idle,
    /// Prepared/pre-invocation request.
    Prepared,
    /// Append invocation began, so zero-or-one append is possible.
    AppendInvoked,
    /// The operation already had this terminal outcome before shutdown.
    Terminal(DurableSendOutcome),
}

/// Phase-preserving send entry in a shutdown report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendOperationReport {
    phase: SendOperationPhase,
    outcome: DurableSendOutcome,
}

impl SendOperationReport {
    /// Classifies a frozen send without overwriting any existing terminal
    /// outcome.
    pub const fn at_shutdown(phase: SendOperationPhase) -> Result<Self, OperationReportError> {
        let outcome = match phase {
            SendOperationPhase::Idle => return Err(OperationReportError::SendNotStarted),
            SendOperationPhase::Prepared => {
                DurableSendOutcome::NotSubmitted(AttemptFailureKind::Shutdown)
            }
            SendOperationPhase::AppendInvoked => {
                DurableSendOutcome::Indeterminate(AttemptFailureKind::Shutdown)
            }
            SendOperationPhase::Terminal(outcome) => outcome,
        };
        Ok(Self { phase, outcome })
    }

    /// Returns the original frozen phase.
    #[must_use]
    pub const fn phase(self) -> SendOperationPhase {
        self.phase
    }

    /// Returns the phase-derived or pre-existing terminal outcome.
    #[must_use]
    pub const fn outcome(self) -> DurableSendOutcome {
        self.outcome
    }
}

/// Checkpoint phase frozen by shutdown before classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointOperationPhase {
    /// No started checkpoint operation exists.
    Idle,
    /// Candidate write has not reached ambiguity.
    Prewrite,
    /// A known-old failure has already been established.
    KnownOld,
    /// Candidate install reachability is unknown.
    InstallUnknown,
    /// Canonical checkpoint sync is confirmed.
    Confirmed,
    /// The operation already had this terminal outcome before shutdown.
    Terminal(CheckpointOutcome),
}

/// Phase-preserving checkpoint entry in a shutdown report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointOperationReport {
    phase: CheckpointOperationPhase,
    outcome: CheckpointOutcome,
}

impl CheckpointOperationReport {
    /// Classifies a frozen checkpoint without changing a pre-existing terminal
    /// outcome or claiming a frontier advance.
    pub const fn at_shutdown(
        phase: CheckpointOperationPhase,
    ) -> Result<Self, OperationReportError> {
        let outcome = match phase {
            CheckpointOperationPhase::Idle => {
                return Err(OperationReportError::CheckpointNotStarted);
            }
            CheckpointOperationPhase::Prewrite | CheckpointOperationPhase::KnownOld => {
                CheckpointOutcome::CheckpointNotCommitted
            }
            CheckpointOperationPhase::InstallUnknown => CheckpointOutcome::CheckpointUnknown,
            CheckpointOperationPhase::Confirmed => CheckpointOutcome::CheckpointCommitted,
            CheckpointOperationPhase::Terminal(outcome) => outcome,
        };
        Ok(Self { phase, outcome })
    }

    /// Returns the original frozen phase.
    #[must_use]
    pub const fn phase(self) -> CheckpointOperationPhase {
        self.phase
    }

    /// Returns the phase-derived or pre-existing terminal outcome.
    #[must_use]
    pub const fn outcome(self) -> CheckpointOutcome {
        self.outcome
    }
}

/// A non-started operation incorrectly included in shutdown classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum OperationReportError {
    /// An idle send is not part of the frozen started-operation set.
    #[error("idle send was not started")]
    SendNotStarted,
    /// An idle checkpoint is not part of the frozen started-operation set.
    #[error("idle checkpoint was not started")]
    CheckpointNotStarted,
}

/// Immutable terminal report returned by every completed shutdown call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    generation: ShutdownGeneration,
    send_operations: Vec<SendOperationReport>,
    checkpoint_operations: Vec<CheckpointOperationReport>,
}

impl ShutdownReport {
    /// Creates a terminal report only after owned transport is closed and all
    /// registered workers have joined.
    pub fn try_new(
        generation: ShutdownGeneration,
        transport_closed: bool,
        workers_joined: bool,
        send_operations: Vec<SendOperationReport>,
        checkpoint_operations: Vec<CheckpointOperationReport>,
    ) -> Result<Self, ShutdownReportError> {
        if !transport_closed {
            return Err(ShutdownReportError::TransportStillOpen);
        }
        if !workers_joined {
            return Err(ShutdownReportError::WorkersNotJoined);
        }
        Ok(Self {
            generation,
            send_operations,
            checkpoint_operations,
        })
    }

    /// Returns the shutdown generation that froze these operations.
    #[must_use]
    pub const fn generation(&self) -> ShutdownGeneration {
        self.generation
    }

    /// Returns phase-preserving send reports.
    #[must_use]
    pub fn send_operations(&self) -> &[SendOperationReport] {
        &self.send_operations
    }

    /// Returns phase-preserving checkpoint reports.
    #[must_use]
    pub fn checkpoint_operations(&self) -> &[CheckpointOperationReport] {
        &self.checkpoint_operations
    }

    /// A valid terminal report always implies closed owned transport.
    #[must_use]
    pub const fn transport_closed(&self) -> bool {
        true
    }

    /// A valid terminal report always implies joined registered workers.
    #[must_use]
    pub const fn workers_joined(&self) -> bool {
        true
    }
}

/// A report attempted before shutdown reached its terminal boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ShutdownReportError {
    /// Owned transport is still open and may retain blocked I/O.
    #[error("owned transport is still open")]
    TransportStillOpen,
    /// One or more registered workers have not joined.
    #[error("registered workers have not joined")]
    WorkersNotJoined,
}

/// Memoizes the first terminal shutdown report for process lifetime.
#[derive(Debug, Default)]
pub struct ShutdownReportCache {
    report: Option<ShutdownReport>,
    install_count: u8,
}

impl ShutdownReportCache {
    /// Creates an empty report cache.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            report: None,
            install_count: 0,
        }
    }

    /// Installs the first report. Every repeat returns the exact cached value
    /// and performs no replacement or additional installation.
    pub fn install_or_get(&mut self, report: ShutdownReport) -> &ShutdownReport {
        if self.report.is_none() {
            self.report = Some(report);
            self.install_count = 1;
        }
        self.report.as_ref().expect("report was installed")
    }

    /// Returns the cached terminal report.
    #[must_use]
    pub const fn get(&self) -> Option<&ShutdownReport> {
        self.report.as_ref()
    }

    /// Returns zero before installation and one afterwards.
    #[must_use]
    pub const fn install_count(&self) -> u8 {
        self.install_count
    }
}

/// Minimal generation/admission contract refined by the adapter coordinator.
#[derive(Debug)]
pub struct LifecycleGate {
    phase: LifecyclePhase,
    generation: LifecycleGeneration,
    admission_open: bool,
    shutdown_generation: Option<ShutdownGeneration>,
    report_cache: ShutdownReportCache,
}

impl LifecycleGate {
    /// Creates a starting gate with admission closed at generation zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            phase: LifecyclePhase::Starting,
            generation: LifecycleGeneration(0),
            admission_open: false,
            shutdown_generation: None,
            report_cache: ShutdownReportCache::new(),
        }
    }

    /// Returns the lifecycle phase.
    #[must_use]
    pub const fn phase(&self) -> LifecyclePhase {
        self.phase
    }

    /// Returns the current lifecycle generation.
    #[must_use]
    pub const fn generation(&self) -> LifecycleGeneration {
        self.generation
    }

    /// Returns whether new admission tickets may be issued.
    #[must_use]
    pub const fn admission_open(&self) -> bool {
        self.admission_open
    }

    /// Opens admission once startup validation has succeeded.
    pub fn mark_ready(&mut self) -> Result<(), LifecycleTransitionError> {
        if self.phase != LifecyclePhase::Starting {
            return Err(LifecycleTransitionError::InvalidPhase(self.phase));
        }
        self.phase = LifecyclePhase::Ready;
        self.admission_open = true;
        Ok(())
    }

    /// Captures the generation used later at registration.
    pub fn admission_ticket(&self) -> Result<AdmissionTicket, LifecycleTransitionError> {
        if self.phase != LifecyclePhase::Ready || !self.admission_open {
            return Err(LifecycleTransitionError::AdmissionClosed);
        }
        Ok(AdmissionTicket {
            generation: self.generation,
        })
    }

    /// Registers a ticket only if it still matches the same open generation.
    pub fn register(
        &self,
        ticket: AdmissionTicket,
        kind: RegisteredOperationKind,
    ) -> Result<RegisteredOperation, LifecycleTransitionError> {
        if ticket.generation != self.generation {
            return Err(LifecycleTransitionError::GenerationFenced {
                ticket: ticket.generation.value(),
                current: self.generation.value(),
            });
        }
        if self.phase != LifecyclePhase::Ready || !self.admission_open {
            return Err(LifecycleTransitionError::AdmissionClosed);
        }
        Ok(RegisteredOperation {
            generation: self.generation,
            kind,
        })
    }

    /// Closes admission and advances exactly one shutdown generation. Repeated
    /// calls during draining or after close reuse the same generation.
    pub fn begin_shutdown(&mut self) -> Result<ShutdownGeneration, LifecycleTransitionError> {
        if let Some(generation) = self.shutdown_generation {
            return Ok(generation);
        }
        let next = self
            .generation
            .0
            .checked_add(1)
            .ok_or(LifecycleTransitionError::GenerationExhausted)?;
        self.generation = LifecycleGeneration(next);
        self.admission_open = false;
        self.phase = LifecyclePhase::Draining;
        let generation = ShutdownGeneration(next);
        self.shutdown_generation = Some(generation);
        Ok(generation)
    }

    /// Closes the lifecycle with its exact generation. Repeated completion
    /// returns the first cached report without replacing it.
    pub fn complete_shutdown(
        &mut self,
        report: ShutdownReport,
    ) -> Result<&ShutdownReport, LifecycleTransitionError> {
        let Some(expected) = self.shutdown_generation else {
            return Err(LifecycleTransitionError::ShutdownNotStarted);
        };
        if report.generation != expected {
            return Err(LifecycleTransitionError::ShutdownGenerationMismatch {
                expected: expected.value(),
                actual: report.generation.value(),
            });
        }
        if self.phase != LifecyclePhase::Draining && self.phase != LifecyclePhase::Closed {
            return Err(LifecycleTransitionError::InvalidPhase(self.phase));
        }
        self.phase = LifecyclePhase::Closed;
        Ok(self.report_cache.install_or_get(report))
    }

    /// Returns the process-lifetime cached terminal report.
    #[must_use]
    pub const fn cached_shutdown_report(&self) -> Option<&ShutdownReport> {
        self.report_cache.get()
    }

    /// Returns the number of terminal report installations (zero or one).
    #[must_use]
    pub const fn shutdown_install_count(&self) -> u8 {
        self.report_cache.install_count()
    }
}

impl Default for LifecycleGate {
    fn default() -> Self {
        Self::new()
    }
}

/// A rejected lifecycle generation or phase transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LifecycleTransitionError {
    /// New admission is closed.
    #[error("durable admission is closed")]
    AdmissionClosed,
    /// A pre-drain ticket no longer matches the lifecycle generation.
    #[error("admission ticket generation {ticket} was fenced by {current}")]
    GenerationFenced {
        /// Ticket generation.
        ticket: u64,
        /// Current lifecycle generation.
        current: u64,
    },
    /// The monotonic lifecycle generation cannot advance.
    #[error("lifecycle generation is exhausted")]
    GenerationExhausted,
    /// The operation is not legal in this lifecycle phase.
    #[error("invalid lifecycle phase {0:?}")]
    InvalidPhase(LifecyclePhase),
    /// A terminal report arrived before shutdown began.
    #[error("shutdown has not started")]
    ShutdownNotStarted,
    /// The report belongs to a different shutdown generation.
    #[error("shutdown generation mismatch: expected {expected}, got {actual}")]
    ShutdownGenerationMismatch {
        /// Expected shutdown generation.
        expected: u64,
        /// Report generation.
        actual: u64,
    },
}

#[cfg(test)]
mod v07_task_2_3 {
    use super::*;
    use crate::durable::{
        AttemptFailureKind, CheckpointOutcome, DurableSendOutcome, ResourceEpoch, ResourceId,
    };

    fn shutdown_report(
        generation: ShutdownGeneration,
        send_phase: SendOperationPhase,
        checkpoint_phase: CheckpointOperationPhase,
    ) -> ShutdownReport {
        ShutdownReport::try_new(
            generation,
            true,
            true,
            vec![SendOperationReport::at_shutdown(send_phase).expect("send report")],
            vec![
                CheckpointOperationReport::at_shutdown(checkpoint_phase)
                    .expect("checkpoint report"),
            ],
        )
        .expect("complete terminal report")
    }

    #[test]
    fn lifecycle_readiness_and_partition_axes_are_orthogonal() {
        let partition = PartitionHealth::new(
            3,
            PartitionState::RecoveryRequired(RecoveryReason::CheckpointIndeterminate),
        );
        let health = DurableHealth::new(
            LifecyclePhase::Ready,
            Readiness::Unavailable(UnavailableReason::Connectivity),
            vec![partition],
        );

        assert_eq!(health.lifecycle(), LifecyclePhase::Ready);
        assert_eq!(
            health.readiness(),
            Readiness::Unavailable(UnavailableReason::Connectivity)
        );
        assert_eq!(
            health.partitions()[0].state(),
            PartitionState::RecoveryRequired(RecoveryReason::CheckpointIndeterminate)
        );
    }

    #[test]
    fn durable_shutdown_does_not_rewrite_control_plane_health() {
        let health = ChirpsPlaneHealth::new(
            ControlPlaneHealth::Available,
            DurableHealth::new(
                LifecyclePhase::Closed,
                Readiness::Unavailable(UnavailableReason::Shutdown),
                Vec::new(),
            ),
        );

        assert_eq!(health.control(), ControlPlaneHealth::Available);
        assert_eq!(health.durable().lifecycle(), LifecyclePhase::Closed);
    }

    #[test]
    fn partition_health_keeps_bounded_fault_reason_and_trace_identity_separate() {
        let epoch = ResourceEpoch::new(ResourceId::from_bytes([0x11; 16]), 7);
        let partition = PartitionHealth::new(
            4,
            PartitionState::Faulted(PartitionFaultReason::InvalidPollObservation),
        );
        let event = DurableEvent::new(
            DurableEventKind::Fault,
            DurableTraceContext::partition(4, 9, epoch),
        );

        assert_eq!(partition.partition(), 4);
        assert_eq!(event.kind(), DurableEventKind::Fault);
        assert_eq!(event.trace(), DurableTraceContext::partition(4, 9, epoch));
    }

    #[test]
    fn metric_labels_are_bounded_and_do_not_carry_trace_identity() {
        let labels = DurableMetricLabels::new(
            MetricOperation::Checkpoint,
            MetricBoundary::CheckpointInstall,
            MetricFailureStage::DirectorySync,
            MetricOutcome::Unknown,
        );

        assert_eq!(labels.operation(), MetricOperation::Checkpoint);
        assert_eq!(labels.boundary(), MetricBoundary::CheckpointInstall);
        assert_eq!(labels.failure_stage(), MetricFailureStage::DirectorySync);
        assert_eq!(labels.outcome(), MetricOutcome::Unknown);
    }

    #[test]
    fn readiness_reasons_distinguish_connectivity_capability_and_recovery() {
        assert_ne!(
            Readiness::Unavailable(UnavailableReason::Connectivity),
            Readiness::Unavailable(UnavailableReason::CapabilityMismatch)
        );
        assert_ne!(
            Readiness::RecoveryRequired(RecoveryReason::RetentionGap),
            Readiness::RecoveryRequired(RecoveryReason::ResourceEpochMismatch)
        );
    }

    #[test]
    fn admission_and_registration_use_the_same_lifecycle_generation() {
        let mut gate = LifecycleGate::new();
        assert_eq!(gate.phase(), LifecyclePhase::Starting);
        gate.mark_ready().expect("starting becomes ready");
        let ticket = gate.admission_ticket().expect("ready admits");
        let operation = gate
            .register(ticket, RegisteredOperationKind::Send)
            .expect("same generation registers");

        assert_eq!(operation.generation(), gate.generation());
        assert_eq!(operation.kind(), RegisteredOperationKind::Send);
    }

    #[test]
    fn shutdown_generation_closes_admission_and_fences_old_tickets() {
        let mut gate = LifecycleGate::new();
        gate.mark_ready().expect("ready");
        let stale = gate.admission_ticket().expect("ticket");
        let shutdown = gate.begin_shutdown().expect("begin shutdown");

        assert_eq!(gate.phase(), LifecyclePhase::Draining);
        assert!(!gate.admission_open());
        assert_eq!(shutdown.value(), gate.generation().value());
        assert_eq!(
            gate.register(stale, RegisteredOperationKind::Checkpoint),
            Err(LifecycleTransitionError::GenerationFenced {
                ticket: stale.generation().value(),
                current: gate.generation().value(),
            })
        );
        assert_eq!(
            gate.admission_ticket(),
            Err(LifecycleTransitionError::AdmissionClosed)
        );
    }

    #[test]
    fn repeated_begin_shutdown_reuses_one_generation() {
        let mut gate = LifecycleGate::new();
        gate.mark_ready().expect("ready");
        let first = gate.begin_shutdown().expect("first shutdown");
        let second = gate.begin_shutdown().expect("repeated begin");

        assert_eq!(first, second);
        assert_eq!(gate.generation().value(), first.value());
    }

    #[test]
    fn shutdown_freeze_requires_fenced_handles_and_prior_generation_operations() {
        let mut gate = LifecycleGate::new();
        gate.mark_ready().expect("ready");
        let ticket = gate.admission_ticket().expect("ticket");
        let operation = gate
            .register(ticket, RegisteredOperationKind::Checkpoint)
            .expect("registered operation");
        let shutdown = gate.begin_shutdown().expect("shutdown generation");

        assert_eq!(
            ShutdownFreeze::try_new(shutdown, vec![operation], false),
            Err(ShutdownFreezeError::HandlesNotFenced)
        );
        let freeze =
            ShutdownFreeze::try_new(shutdown, vec![operation], true).expect("frozen started set");
        assert_eq!(freeze.generation(), shutdown);
        assert_eq!(freeze.started_operations(), &[operation]);
        assert!(freeze.handles_fenced());
    }

    #[test]
    fn send_shutdown_classification_preserves_original_phase() {
        let prepared = SendOperationReport::at_shutdown(SendOperationPhase::Prepared)
            .expect("prepared report");
        let invoked = SendOperationReport::at_shutdown(SendOperationPhase::AppendInvoked)
            .expect("invoked report");

        assert_eq!(prepared.phase(), SendOperationPhase::Prepared);
        assert_eq!(
            prepared.outcome(),
            DurableSendOutcome::NotSubmitted(AttemptFailureKind::Shutdown)
        );
        assert_eq!(
            invoked.outcome(),
            DurableSendOutcome::Indeterminate(AttemptFailureKind::Shutdown)
        );
    }

    #[test]
    fn terminal_send_outcome_is_never_reclassified_by_shutdown() {
        let existing = DurableSendOutcome::OsSyncedAccepted;
        let report = SendOperationReport::at_shutdown(SendOperationPhase::Terminal(existing))
            .expect("terminal report");

        assert_eq!(report.outcome(), existing);
        assert_eq!(report.phase(), SendOperationPhase::Terminal(existing));
    }

    #[test]
    fn checkpoint_shutdown_classification_preserves_install_phase() {
        for (phase, expected) in [
            (
                CheckpointOperationPhase::Prewrite,
                CheckpointOutcome::CheckpointNotCommitted,
            ),
            (
                CheckpointOperationPhase::KnownOld,
                CheckpointOutcome::CheckpointNotCommitted,
            ),
            (
                CheckpointOperationPhase::InstallUnknown,
                CheckpointOutcome::CheckpointUnknown,
            ),
            (
                CheckpointOperationPhase::Confirmed,
                CheckpointOutcome::CheckpointCommitted,
            ),
        ] {
            let report = CheckpointOperationReport::at_shutdown(phase).expect("phase report");
            assert_eq!(report.phase(), phase);
            assert_eq!(report.outcome(), expected);
        }
    }

    #[test]
    fn terminal_checkpoint_outcome_is_never_reclassified_by_shutdown() {
        let report = CheckpointOperationReport::at_shutdown(CheckpointOperationPhase::Terminal(
            CheckpointOutcome::CheckpointCommitted,
        ))
        .expect("terminal checkpoint report");

        assert_eq!(report.outcome(), CheckpointOutcome::CheckpointCommitted);
    }

    #[test]
    fn shutdown_report_requires_closed_transport_and_joined_workers() {
        let generation = ShutdownGeneration::from_value(3);
        assert_eq!(
            ShutdownReport::try_new(generation, false, true, Vec::new(), Vec::new()),
            Err(ShutdownReportError::TransportStillOpen)
        );
        assert_eq!(
            ShutdownReport::try_new(generation, true, false, Vec::new(), Vec::new()),
            Err(ShutdownReportError::WorkersNotJoined)
        );
    }

    #[test]
    fn terminal_report_cache_returns_the_same_value_without_reinstalling() {
        let generation = ShutdownGeneration::from_value(3);
        let first = shutdown_report(
            generation,
            SendOperationPhase::Prepared,
            CheckpointOperationPhase::Prewrite,
        );
        let alternate = shutdown_report(
            generation,
            SendOperationPhase::AppendInvoked,
            CheckpointOperationPhase::InstallUnknown,
        );
        let mut cache = ShutdownReportCache::new();

        assert_eq!(cache.install_or_get(first.clone()), &first);
        assert_eq!(cache.install_or_get(alternate), &first);
        assert_eq!(cache.install_count(), 1);
        assert_eq!(cache.get(), Some(&first));
    }

    #[test]
    fn lifecycle_closes_only_with_its_generation_and_returns_cached_report() {
        let mut gate = LifecycleGate::new();
        gate.mark_ready().expect("ready");
        let generation = gate.begin_shutdown().expect("draining");
        let report = shutdown_report(
            generation,
            SendOperationPhase::AppendInvoked,
            CheckpointOperationPhase::InstallUnknown,
        );
        let cached = gate.complete_shutdown(report.clone()).expect("closed");

        assert_eq!(cached, &report);
        assert_eq!(gate.phase(), LifecyclePhase::Closed);
        assert_eq!(gate.cached_shutdown_report(), Some(&report));

        let alternate = shutdown_report(
            generation,
            SendOperationPhase::Prepared,
            CheckpointOperationPhase::Prewrite,
        );
        assert_eq!(gate.complete_shutdown(alternate).expect("repeat"), &report);
        assert_eq!(gate.shutdown_install_count(), 1);
    }
}
