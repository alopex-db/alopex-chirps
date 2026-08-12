//! One-attempt Durable producer coordination and exact result mapping.
//!
//! Preparation is state-free and fixes the message identity, route, canonical
//! bytes, and digests before backend contact. Preflight produces no attempt ID.
//! Starting an attempt creates exactly one ID, and execution calls the injected
//! append port exactly once without reconnect, replay, batching, or retry.
//!
//! Chirps does not own a producer outbox. Callers that need an application
//! transaction and publish to be atomic must persist their source record or the
//! immutable prepared request in an application-owned outbox. An explicit retry
//! reuses that prepared value and creates only a new attempt ID. In particular,
//! response loss remains duplicate-capable and does not imply broker exactly-once.

use crate::codec::{self, EnvelopeFields};
use crate::protocol::{ResourceLocation, VerifiedAppendOneSyncedResponse};
use crate::routing::{PartitionRouter, RoutingError};
use crate::session::{BoundSession, SessionError, SessionInvocationError};
use crate::transport::{
    DataPlaneRequestFrame, DevelopmentProfileReadbackFrame, InvocationError, LoginRequestFrame,
    OwnedTransport, TransportCloseHandle, TransportLimits, TransportShutdownReport,
};
use alopex_chirps_core::durable::{
    AttemptBinding, AttemptFailureKind, ConfirmationBoundary, DurableAttemptId, DurableMessageId,
    DurableReceipt, DurableSendResult, EnvelopeDigest, MessageIdError, PreflightFailure,
    PreflightFailureKind, PrepareFailure, PreparedDurableSend, ReceiptError, ResultShapeError,
    SessionFingerprint,
};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use iggy_binary_protocol::responses::topics::GetTopicResponse;
use iggy_binary_protocol::{ResponseFrame, STATUS_OK, WireDecode};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

/// Combines the immutable partition router and canonical codec before any
/// backend contact.
pub fn prepare(
    router: &PartitionRouter,
    target: NodeId,
    ordering_key: Vec<u8>,
    payload: &[u8],
) -> Result<PreparedDurableSend, ProducerPrepareError> {
    let partition = router.partition_for(target, &ordering_key)?;
    let route = router.route(target, ordering_key.clone())?;
    PreparedDurableSend::prepare(route, |message_id| {
        codec::encode(
            message_id,
            EnvelopeFields::new(
                router.source(),
                target,
                router.generation(),
                partition,
                &ordering_key,
                payload,
            ),
        )
        .map_err(|_| PrepareFailure::CanonicalEncoding)
    })
    .map_err(ProducerPrepareError::Prepare)
}

/// State-free preparation failed before a preflight or attempt existed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProducerPrepareError {
    /// Deterministic route validation failed.
    #[error("durable producer routing failed: {0}")]
    Routing(#[from] RoutingError),
    /// Message-ID generation or canonical encoding failed.
    #[error("durable producer preparation failed: {0}")]
    Prepare(PrepareFailure),
}

/// Session/resource values proven by append-port preflight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendContext {
    session_fingerprint: SessionFingerprint,
    location: ResourceLocation,
}

impl AppendContext {
    /// Creates the immutable context returned by a verified adapter preflight.
    #[must_use]
    pub const fn new(session_fingerprint: SessionFingerprint, location: ResourceLocation) -> Self {
        Self {
            session_fingerprint,
            location,
        }
    }

    /// Returns the authenticated session fingerprint.
    #[must_use]
    pub const fn session_fingerprint(self) -> SessionFingerprint {
        self.session_fingerprint
    }

    /// Returns the exact resource incarnation and explicit provider location.
    #[must_use]
    pub const fn location(self) -> ResourceLocation {
        self.location
    }
}

/// Immutable one-envelope request passed to the append port exactly once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendAttempt {
    attempt_id: DurableAttemptId,
    message_id: DurableMessageId,
    envelope_digest: EnvelopeDigest,
    session_fingerprint: SessionFingerprint,
    requested_boundary: ConfirmationBoundary,
    location: ResourceLocation,
    canonical_envelope: Vec<u8>,
}

/// One-shot signal owned by an [`AppendPort`] until the exact public append
/// ambiguity boundary is crossed.
///
/// A port must consume this value with [`Self::admit`] only after all local
/// validation has passed and the append operation has been admitted. Dropping
/// it proves that no append was admitted and no future automatic send remains.
#[derive(Debug)]
pub struct AppendAdmission {
    signal: Option<oneshot::Sender<()>>,
}

impl AppendAdmission {
    fn new(signal: oneshot::Sender<()>) -> Self {
        Self {
            signal: Some(signal),
        }
    }

    /// Marks the exact append invocation boundary once. Repeated admission is
    /// impossible because this method consumes the token.
    pub fn admit(mut self) {
        if let Some(signal) = self.signal.take() {
            let _ = signal.send(());
        }
    }
}

impl AppendAttempt {
    fn new(
        prepared: &PreparedDurableSend,
        binding: &AttemptBinding,
        context: AppendContext,
    ) -> Result<Self, ProducerCoordinatorError> {
        Ok(Self {
            attempt_id: binding
                .attempt_id()
                .ok_or(ProducerCoordinatorError::MissingAttemptBinding)?,
            message_id: prepared.message_id(),
            envelope_digest: prepared.envelope_digest(),
            session_fingerprint: context.session_fingerprint,
            requested_boundary: binding
                .requested_boundary()
                .ok_or(ProducerCoordinatorError::MissingAttemptBinding)?,
            location: context.location,
            canonical_envelope: prepared.canonical_bytes().to_vec(),
        })
    }

    /// Returns this attempt's fresh UUIDv4 identity.
    #[must_use]
    pub const fn attempt_id(&self) -> DurableAttemptId {
        self.attempt_id
    }

    /// Returns the logical message identity fixed during preparation.
    #[must_use]
    pub const fn message_id(&self) -> DurableMessageId {
        self.message_id
    }

    /// Returns the canonical envelope digest fixed during preparation.
    #[must_use]
    pub const fn envelope_digest(&self) -> EnvelopeDigest {
        self.envelope_digest
    }

    /// Returns the preflight-verified authenticated connection fingerprint.
    #[must_use]
    pub const fn session_fingerprint(&self) -> SessionFingerprint {
        self.session_fingerprint
    }

    /// Returns the caller-requested weak or strong confirmation boundary.
    #[must_use]
    pub const fn requested_boundary(&self) -> ConfirmationBoundary {
        self.requested_boundary
    }

    /// Returns the exact preflight-verified location.
    #[must_use]
    pub const fn location(&self) -> ResourceLocation {
        self.location
    }

    /// Returns the one immutable canonical envelope for this attempt.
    #[must_use]
    pub fn canonical_envelope(&self) -> &[u8] {
        &self.canonical_envelope
    }
}

/// Sealed proof that the trusted strong adapter correlated every echoed field
/// with the exact current request before releasing assigned location values.
///
/// There is deliberately no public constructor. A development or third-party
/// [`AppendPort`] can produce weak acceptance, but cannot fabricate or recycle
/// a strong confirmation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrongAppendConfirmation {
    attempt_id: DurableAttemptId,
    message_id: DurableMessageId,
    envelope_digest: EnvelopeDigest,
    session_fingerprint: SessionFingerprint,
    requested_boundary: ConfirmationBoundary,
    location: ResourceLocation,
    assigned_offset: u64,
    assigned_index: u64,
}

impl StrongAppendConfirmation {
    fn from_verified(attempt: &AppendAttempt, response: VerifiedAppendOneSyncedResponse) -> Self {
        Self {
            attempt_id: attempt.attempt_id,
            message_id: attempt.message_id,
            envelope_digest: attempt.envelope_digest,
            session_fingerprint: attempt.session_fingerprint,
            requested_boundary: attempt.requested_boundary,
            location: response.location(),
            assigned_offset: response.assigned_offset(),
            assigned_index: response.assigned_index(),
        }
    }

    fn matches(&self, attempt: &AppendAttempt) -> bool {
        self.attempt_id == attempt.attempt_id
            && self.message_id == attempt.message_id
            && self.envelope_digest == attempt.envelope_digest
            && self.session_fingerprint == attempt.session_fingerprint
            && self.requested_boundary == attempt.requested_boundary
            && self.location == attempt.location
    }
}

/// Complete result of the append port's sole call for one attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum AppendPortOutcome {
    /// The port proved that append invocation did not begin.
    NotInvoked(AttemptFailureKind),
    /// The standard development broker returned its ordinary weak success.
    BrokerAccepted,
    /// A sealed current-request correlation proof from the strong adapter.
    OsSyncedAccepted(StrongAppendConfirmation),
    /// Append invocation began but no valid success was established.
    Indeterminate(AttemptFailureKind),
}

/// Narrow dependency-injection boundary for one append attempt.
///
/// Implementations must return `NotInvoked` only when no append command was
/// admitted. Every timeout, disconnect, cancellation, or invalid response after
/// admission is `Indeterminate`. The coordinator calls `append_once` at most
/// once and never retries an outcome.
#[async_trait]
pub trait AppendPort: Send + Sync {
    /// Verifies availability, requested boundary, session, and resource before
    /// an attempt ID is created.
    fn preflight(
        &self,
        prepared: &PreparedDurableSend,
        requested_boundary: ConfirmationBoundary,
    ) -> Result<AppendContext, PreflightFailureKind>;

    /// Performs at most one data-plane append invocation for this attempt.
    async fn append_once(
        &self,
        attempt: &AppendAttempt,
        admission: AppendAdmission,
    ) -> AppendPortOutcome;
}

/// A successful preflight that still has no attempt ID.
#[derive(Debug)]
pub struct PreflightedSend {
    prepared: PreparedDurableSend,
    context: AppendContext,
    requested_boundary: ConfirmationBoundary,
}

impl PreflightedSend {
    /// Explicitly starts one attempt and creates its sole fresh attempt ID.
    pub fn start(self) -> Result<StartedSend, ProducerCoordinatorError> {
        let binding = AttemptBinding::start(
            &self.prepared,
            self.context.session_fingerprint,
            self.requested_boundary,
        )
        .map_err(ProducerCoordinatorError::AttemptIdentity)?;
        Ok(StartedSend {
            prepared: self.prepared,
            context: self.context,
            binding,
        })
    }
}

/// A linear started attempt that has not yet called the append port.
#[derive(Debug)]
pub struct StartedSend {
    prepared: PreparedDurableSend,
    context: AppendContext,
    binding: AttemptBinding,
}

impl StartedSend {
    /// Returns the attempt identity created by [`PreflightedSend::start`].
    #[must_use]
    pub fn attempt_id(&self) -> DurableAttemptId {
        self.binding
            .attempt_id()
            .expect("StartedSend always owns a started binding")
    }

    /// Cancels before an operation owner or append invocation exists.
    pub fn cancel_before_execute(self) -> Result<DurableSendResult, ProducerCoordinatorError> {
        DurableSendResult::not_submitted(
            &self.prepared,
            self.binding,
            AttemptFailureKind::Cancelled,
        )
        .map_err(ProducerCoordinatorError::ResultShape)
    }
}

/// A completed operation-owner result retained independently of any one wait
/// future. Cancelling and dropping a wait does not discard the attempt binding.
#[derive(Debug)]
pub struct AttemptTerminal {
    result: Result<DurableSendResult, ProducerCoordinatorError>,
}

impl AttemptTerminal {
    /// Returns the bound terminal send result or an unreachable shape failure.
    pub const fn result(&self) -> Result<&DurableSendResult, &ProducerCoordinatorError> {
        self.result.as_ref()
    }
}

/// Cancellation-safe handle to one owned send-attempt operation.
///
/// The operation owner, rather than the caller's wait future, retains the
/// prepared request and linear attempt binding. Dropping a wait is harmless;
/// [`Self::cancel`] terminalizes a pre-invocation attempt as `NotSubmitted` and
/// a possibly invoked attempt as `Indeterminate`. Dropping the handle requests
/// the same cancellation before releasing it.
#[derive(Debug)]
pub struct SendAttemptHandle {
    attempt_id: DurableAttemptId,
    cancel_tx: watch::Sender<bool>,
    terminal_rx: watch::Receiver<Option<Arc<AttemptTerminal>>>,
    owner: Option<JoinHandle<()>>,
}

impl SendAttemptHandle {
    /// Returns the immutable attempt ID owned by this handle.
    #[must_use]
    pub const fn attempt_id(&self) -> DurableAttemptId {
        self.attempt_id
    }

    /// Requests idempotent caller cancellation without dropping terminal state.
    pub fn cancel(&self) {
        self.cancel_tx.send_replace(true);
    }

    /// Returns whether the owner has installed a terminal result.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.terminal_rx.borrow().is_some()
    }

    /// Waits for the cached terminal result. This method may itself be dropped
    /// and awaited again on the same handle without losing the result.
    pub async fn terminal(&mut self) -> Arc<AttemptTerminal> {
        loop {
            if let Some(terminal) = self.terminal_rx.borrow().clone() {
                if let Some(owner) = self.owner.take() {
                    let _ = owner.await;
                }
                return terminal;
            }
            if self.terminal_rx.changed().await.is_err() {
                continue;
            }
        }
    }
}

impl Drop for SendAttemptHandle {
    fn drop(&mut self) {
        self.cancel_tx.send_replace(true);
    }
}

/// One-attempt coordinator over an owned shared provider append port.
#[derive(Debug)]
pub struct ProducerCoordinator<P: ?Sized> {
    port: Arc<P>,
}

impl<P: ?Sized> Clone for ProducerCoordinator<P> {
    fn clone(&self) -> Self {
        Self {
            port: Arc::clone(&self.port),
        }
    }
}

impl<P> ProducerCoordinator<P>
where
    P: AppendPort + ?Sized,
{
    /// Shares a port with operation owners; no reconnect, retry, or outbox loop
    /// is created.
    #[must_use]
    pub fn new(port: Arc<P>) -> Self {
        Self { port }
    }

    /// Verifies the current port/session/resource before generating an attempt.
    pub fn preflight(
        &self,
        prepared: &PreparedDurableSend,
        requested_boundary: ConfirmationBoundary,
    ) -> Result<PreflightedSend, PreflightFailure> {
        let context = self
            .port
            .preflight(prepared, requested_boundary)
            .map_err(|kind| PreflightFailure::new(prepared, kind))?;
        if context.location.partition_id() != prepared.partition()
            || context
                .session_fingerprint
                .as_bytes()
                .iter()
                .all(|byte| *byte == 0)
        {
            return Err(PreflightFailure::new(
                prepared,
                PreflightFailureKind::BindingRejected,
            ));
        }
        Ok(PreflightedSend {
            prepared: prepared.clone(),
            context,
            requested_boundary,
        })
    }

    /// Starts an operation owner and returns immediately with a cancellation-
    /// safe handle. The owner calls the append port at most once.
    pub fn execute(&self, started: StartedSend) -> SendAttemptHandle
    where
        P: 'static,
    {
        let attempt_id = started.attempt_id();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let port = Arc::clone(&self.port);
        let owner = tokio::spawn(async move {
            let result = run_attempt(port, started, cancel_rx).await;
            terminal_tx.send_replace(Some(Arc::new(AttemptTerminal { result })));
        });
        SendAttemptHandle {
            attempt_id,
            cancel_tx,
            terminal_rx,
            owner: Some(owner),
        }
    }
}

async fn run_attempt<P>(
    port: Arc<P>,
    started: StartedSend,
    mut cancel_rx: watch::Receiver<bool>,
) -> Result<DurableSendResult, ProducerCoordinatorError>
where
    P: AppendPort + ?Sized,
{
    let StartedSend {
        prepared,
        context,
        binding,
    } = started;
    let attempt = AppendAttempt::new(&prepared, &binding, context)?;
    if *cancel_rx.borrow() {
        return DurableSendResult::not_submitted(&prepared, binding, AttemptFailureKind::Cancelled)
            .map_err(ProducerCoordinatorError::ResultShape);
    }

    let (admission_tx, mut admission_rx) = oneshot::channel();
    let append = port.append_once(&attempt, AppendAdmission::new(admission_tx));
    tokio::pin!(append);
    enum InitialAppendState {
        Admitted,
        Cancelled,
        Completed(AppendPortOutcome),
    }
    let mut admission_open = true;
    let initial = loop {
        tokio::select! {
            biased;
            admitted = &mut admission_rx, if admission_open => match admitted {
                Ok(()) => break InitialAppendState::Admitted,
                Err(_) => admission_open = false,
            },
            () = wait_for_cancel(&mut cancel_rx) => {
                break InitialAppendState::Cancelled;
            }
            outcome = &mut append => {
                break InitialAppendState::Completed(outcome);
            }
        }
    };

    let outcome = match initial {
        InitialAppendState::Cancelled => {
            return DurableSendResult::not_submitted(
                &prepared,
                binding,
                AttemptFailureKind::Cancelled,
            )
            .map_err(ProducerCoordinatorError::ResultShape);
        }
        InitialAppendState::Admitted => {
            let invoked = binding
                .append_invoked()
                .map_err(ProducerCoordinatorError::ResultShape)?;
            return await_admitted_outcome(
                &prepared,
                &attempt,
                invoked,
                &mut append,
                &mut cancel_rx,
            )
            .await;
        }
        InitialAppendState::Completed(outcome) => outcome,
    };

    if admission_rx.try_recv().is_ok() {
        let invoked = binding
            .append_invoked()
            .map_err(ProducerCoordinatorError::ResultShape)?;
        return map_admitted_outcome(&prepared, &attempt, invoked, outcome);
    }

    match outcome {
        AppendPortOutcome::NotInvoked(failure) => {
            DurableSendResult::not_submitted(&prepared, binding, failure)
                .map_err(ProducerCoordinatorError::ResultShape)
        }
        _ => {
            // A port that claims an invoked outcome without consuming the
            // admission token violated the boundary contract. Do not promote
            // the result; conservatively retain a bound ambiguity.
            let invoked = binding
                .append_invoked()
                .map_err(ProducerCoordinatorError::ResultShape)?;
            DurableSendResult::indeterminate(&prepared, invoked, AttemptFailureKind::Protocol)
                .map_err(ProducerCoordinatorError::ResultShape)
        }
    }
}

async fn await_admitted_outcome(
    prepared: &PreparedDurableSend,
    attempt: &AppendAttempt,
    invoked: AttemptBinding,
    append: &mut (impl std::future::Future<Output = AppendPortOutcome> + Unpin),
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<DurableSendResult, ProducerCoordinatorError> {
    let outcome = tokio::select! {
        biased;
        outcome = append => outcome,
        () = wait_for_cancel(cancel_rx) => {
            return DurableSendResult::indeterminate(
                prepared,
                invoked,
                AttemptFailureKind::Cancelled,
            )
            .map_err(ProducerCoordinatorError::ResultShape);
        }
    };
    map_admitted_outcome(prepared, attempt, invoked, outcome)
}

fn map_admitted_outcome(
    prepared: &PreparedDurableSend,
    attempt: &AppendAttempt,
    invoked: AttemptBinding,
    outcome: AppendPortOutcome,
) -> Result<DurableSendResult, ProducerCoordinatorError> {
    match outcome {
        AppendPortOutcome::NotInvoked(_) => {
            DurableSendResult::indeterminate(prepared, invoked, AttemptFailureKind::Protocol)
                .map_err(ProducerCoordinatorError::ResultShape)
        }
        AppendPortOutcome::Indeterminate(failure) => {
            DurableSendResult::indeterminate(prepared, invoked, failure)
                .map_err(ProducerCoordinatorError::ResultShape)
        }
        AppendPortOutcome::BrokerAccepted => {
            if attempt.requested_boundary != ConfirmationBoundary::BrokerAccepted {
                return DurableSendResult::indeterminate(
                    prepared,
                    invoked,
                    AttemptFailureKind::Protocol,
                )
                .map_err(ProducerCoordinatorError::ResultShape);
            }
            DurableSendResult::broker_accepted(prepared, invoked)
                .map_err(ProducerCoordinatorError::ResultShape)
        }
        AppendPortOutcome::OsSyncedAccepted(confirmation) => {
            if attempt.requested_boundary != ConfirmationBoundary::OsSyncedAccepted
                || !confirmation.matches(attempt)
            {
                return DurableSendResult::indeterminate(
                    prepared,
                    invoked,
                    AttemptFailureKind::Protocol,
                )
                .map_err(ProducerCoordinatorError::ResultShape);
            }
            let receipt = DurableReceipt::try_new(
                prepared,
                invoked,
                confirmation.location.resource_epoch(),
                confirmation.location.partition_id(),
                confirmation.assigned_offset,
                confirmation.assigned_index,
            )
            .map_err(ProducerCoordinatorError::ReceiptShape)?;
            Ok(DurableSendResult::os_synced_accepted(receipt))
        }
    }
}

async fn wait_for_cancel(cancel_rx: &mut watch::Receiver<bool>) {
    if *cancel_rx.borrow() {
        return;
    }
    loop {
        if cancel_rx.changed().await.is_err() || *cancel_rx.borrow() {
            return;
        }
    }
}

/// An internal linear-token or result-shape failure that should be unreachable
/// when callers use preflight, start, and execute in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ProducerCoordinatorError {
    /// The CSPRNG could not create an attempt UUID after successful preflight.
    #[error("durable attempt identity generation failed: {0}")]
    AttemptIdentity(MessageIdError),
    /// A supposedly started token did not contain an attempt binding.
    #[error("durable producer is missing its started attempt binding")]
    MissingAttemptBinding,
    /// A provider-neutral terminal result rejected the phase or identity shape.
    #[error("durable producer result shape is invalid: {0}")]
    ResultShape(ResultShapeError),
    /// An exact strong response could not construct its provider-neutral receipt.
    #[error("durable producer receipt shape is invalid: {0}")]
    ReceiptShape(ReceiptError),
}

/// Verified bytes read back from the development broker's actual startup
/// configuration.
///
/// Iggy 0.8.0 has no authenticated protocol field for the global message
/// deduplication switch. The deployment must therefore read back the exact
/// startup configuration artifact and pass those bytes here. Implicit defaults,
/// missing sections, duplicate keys, and any value other than explicit `false`
/// are rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DevelopmentBrokerConfigReadback {
    digest: [u8; 32],
}

impl DevelopmentBrokerConfigReadback {
    /// Verifies an actual broker startup-config readback and binds its SHA-256.
    pub fn verify_actual_startup_config(bytes: &[u8]) -> Result<Self, DevelopmentProfileError> {
        if !has_one_explicit_disabled_dedup_setting(bytes)? {
            return Err(DevelopmentProfileError::DeduplicationNotDisabled);
        }
        Ok(Self {
            digest: Sha256::digest(bytes).into(),
        })
    }

    /// Returns the digest that release evidence can compare with its manifest.
    #[must_use]
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }
}

/// Fail-closed development-profile verification error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DevelopmentProfileError {
    /// The broker startup-config readback was not valid UTF-8/TOML-like text.
    #[error("development broker startup configuration readback is invalid")]
    InvalidConfigReadback,
    /// The exact deduplication section did not contain one explicit false value.
    #[error("development broker message deduplication is not explicitly disabled")]
    DeduplicationNotDisabled,
    /// The authenticated official resource response was malformed or rejected.
    #[error("development broker resource readback is invalid")]
    InvalidResourceReadback,
    /// Numeric topic, explicit partition, or replication factor did not match.
    #[error("development broker resource readback does not match the requested profile")]
    ResourceReadbackMismatch,
}

/// Connection/authentication or profile verification failed before the
/// development append port became constructible.
#[derive(Debug, Error)]
pub enum DevelopmentConnectError {
    /// TCP/TLS/login/readback transport or authentication failed.
    #[error(transparent)]
    Session(#[from] SessionError),
    /// The fail-closed broker profile checks failed.
    #[error(transparent)]
    Profile(#[from] DevelopmentProfileError),
}

fn has_one_explicit_disabled_dedup_setting(bytes: &[u8]) -> Result<bool, DevelopmentProfileError> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| DevelopmentProfileError::InvalidConfigReadback)?;
    let mut in_dedup_section = false;
    let mut enabled = None;
    for raw_line in text.lines() {
        let line = raw_line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') {
            in_dedup_section = line == "[system.message_deduplication]";
            continue;
        }
        if !in_dedup_section {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(DevelopmentProfileError::InvalidConfigReadback);
        };
        if key.trim() != "enabled" {
            continue;
        }
        if enabled.is_some() {
            return Err(DevelopmentProfileError::InvalidConfigReadback);
        }
        enabled = Some(value.trim() == "false");
    }
    Ok(enabled == Some(true))
}

/// Concrete official-Iggy development connection for weak `BrokerAccepted`
/// sends. It owns one TLS/login transport and never creates a high-level Iggy
/// producer, reconnect loop, replay loop, batch, or automatic login.
pub struct DevelopmentAppendConnection {
    transport: OwnedTransport,
    location: ResourceLocation,
    fingerprint: SessionFingerprint,
    broker_config: DevelopmentBrokerConfigReadback,
}

impl std::fmt::Debug for DevelopmentAppendConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DevelopmentAppendConnection")
            .field("location", &self.location)
            .field("closed", &self.transport.close_handle().is_closed())
            .finish_non_exhaustive()
    }
}

impl DevelopmentAppendConnection {
    /// Establishes one caller-configured TLS connection, performs one explicit
    /// official login, and binds the weak profile to an explicit numeric
    /// stream/topic/partition. Login failure closes and joins the transport.
    #[allow(clippy::too_many_arguments)]
    pub async fn connect_tls_and_authenticate(
        address: SocketAddr,
        server_name: ServerName<'static>,
        client_config: Arc<ClientConfig>,
        limits: TransportLimits,
        login: LoginRequestFrame,
        location: ResourceLocation,
        broker_config: DevelopmentBrokerConfigReadback,
        deadline: Instant,
    ) -> Result<Self, DevelopmentConnectError> {
        let readback = DevelopmentProfileReadbackFrame::for_location(location)
            .map_err(|_| DevelopmentProfileError::InvalidResourceReadback)?;
        let transport = timeout_at(
            deadline,
            OwnedTransport::connect_tls(address, server_name, client_config, limits),
        )
        .await
        .map_err(|_| SessionError::Deadline)?
        .map_err(SessionError::Transport)?;

        let login_result = timeout_at(deadline, transport.login(login)).await;
        let login_result = match login_result {
            Ok(Ok(response)) => validate_official_login_response(&response),
            Ok(Err(error)) => Err(SessionError::Transport(error)),
            Err(_) => Err(SessionError::Deadline),
        };
        if let Err(error) = login_result {
            transport.close_handle().close();
            let _ = transport.shutdown(deadline).await;
            return Err(error.into());
        }

        let response = timeout_at(deadline, transport.development_profile_readback(readback)).await;
        let profile_result = match response {
            Ok(Ok(response)) => validate_development_resource_readback(&response, location),
            Ok(Err(error)) => Err(DevelopmentConnectError::Session(SessionError::Transport(
                error,
            ))),
            Err(_) => Err(DevelopmentConnectError::Session(SessionError::Deadline)),
        };
        if let Err(error) = profile_result {
            transport.close_handle().close();
            let _ = transport.shutdown(deadline).await;
            return Err(error);
        }

        Ok(Self {
            transport,
            location,
            fingerprint: fresh_connection_fingerprint(),
            broker_config,
        })
    }

    /// Returns the per-authenticated-connection correlation fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> SessionFingerprint {
        self.fingerprint
    }

    /// Returns the verified startup-config digest bound to this connection.
    #[must_use]
    pub const fn broker_config_digest(&self) -> [u8; 32] {
        self.broker_config.digest()
    }

    /// Returns an idempotent close handle for lifecycle coordination.
    #[must_use]
    pub fn close_handle(&self) -> TransportCloseHandle {
        self.transport.close_handle()
    }

    /// Closes and joins the exact development connection.
    pub async fn shutdown(self, deadline: Instant) -> TransportShutdownReport {
        self.transport.shutdown(deadline).await
    }
}

#[async_trait]
impl AppendPort for DevelopmentAppendConnection {
    fn preflight(
        &self,
        prepared: &PreparedDurableSend,
        requested_boundary: ConfirmationBoundary,
    ) -> Result<AppendContext, PreflightFailureKind> {
        if requested_boundary != ConfirmationBoundary::BrokerAccepted {
            return Err(PreflightFailureKind::Unsupported);
        }
        if self.transport.close_handle().is_closed() {
            return Err(PreflightFailureKind::Unavailable);
        }
        if self.location.partition_id() != prepared.partition() {
            return Err(PreflightFailureKind::BindingRejected);
        }
        Ok(AppendContext::new(self.fingerprint, self.location))
    }

    async fn append_once(
        &self,
        attempt: &AppendAttempt,
        admission: AppendAdmission,
    ) -> AppendPortOutcome {
        if attempt.requested_boundary != ConfirmationBoundary::BrokerAccepted
            || attempt.location != self.location
        {
            return AppendPortOutcome::NotInvoked(AttemptFailureKind::Protocol);
        }
        let request = match DataPlaneRequestFrame::from_standard_append(
            self.location,
            attempt.message_id,
            &attempt.canonical_envelope,
        ) {
            Ok(request) => request,
            Err(_) => return AppendPortOutcome::NotInvoked(AttemptFailureKind::Protocol),
        };
        match self
            .transport
            .invoke_with_admission(request, move || admission.admit())
            .await
        {
            Ok(response) if validate_standard_append_response(&response) => {
                AppendPortOutcome::BrokerAccepted
            }
            Ok(_) => AppendPortOutcome::Indeterminate(AttemptFailureKind::Protocol),
            Err(InvocationError::NotInvoked(_)) => {
                AppendPortOutcome::NotInvoked(AttemptFailureKind::Transport)
            }
            Err(InvocationError::Indeterminate(_)) => {
                AppendPortOutcome::Indeterminate(AttemptFailureKind::Transport)
            }
        }
    }
}

fn validate_official_login_response(bytes: &[u8]) -> Result<(), SessionError> {
    let (frame, consumed) =
        ResponseFrame::decode(bytes).map_err(|_| SessionError::InvalidAuthenticationResponse)?;
    if consumed != bytes.len() {
        return Err(SessionError::InvalidAuthenticationResponse);
    }
    if frame.status != STATUS_OK {
        return Err(SessionError::AuthenticationRejected(frame.status));
    }
    Ok(())
}

fn validate_development_resource_readback(
    bytes: &[u8],
    location: ResourceLocation,
) -> Result<(), DevelopmentConnectError> {
    let (frame, consumed) = ResponseFrame::decode(bytes)
        .map_err(|_| DevelopmentProfileError::InvalidResourceReadback)?;
    if consumed != bytes.len() || frame.status != STATUS_OK {
        return Err(DevelopmentProfileError::InvalidResourceReadback.into());
    }
    let (topic, payload_consumed) = GetTopicResponse::decode(frame.payload)
        .map_err(|_| DevelopmentProfileError::InvalidResourceReadback)?;
    let matching_partitions = topic
        .partitions
        .iter()
        .filter(|partition| partition.id == location.partition_id())
        .count();
    if payload_consumed != frame.payload.len()
        || topic.topic.id != location.topic_id()
        || topic.topic.replication_factor != 1
        || matching_partitions != 1
    {
        return Err(DevelopmentProfileError::ResourceReadbackMismatch.into());
    }
    Ok(())
}

fn validate_standard_append_response(bytes: &[u8]) -> bool {
    ResponseFrame::decode(bytes).is_ok_and(|(frame, consumed)| {
        consumed == bytes.len() && frame.status == STATUS_OK && frame.payload.is_empty()
    })
}

fn fresh_connection_fingerprint() -> SessionFingerprint {
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(first.as_bytes());
    bytes[16..].copy_from_slice(second.as_bytes());
    SessionFingerprint::from_bytes(bytes)
}

#[async_trait]
impl AppendPort for BoundSession {
    fn preflight(
        &self,
        prepared: &PreparedDurableSend,
        requested_boundary: ConfirmationBoundary,
    ) -> Result<AppendContext, PreflightFailureKind> {
        if requested_boundary != ConfirmationBoundary::OsSyncedAccepted {
            return Err(PreflightFailureKind::Unsupported);
        }
        if !matches!(
            self.readiness(),
            alopex_chirps_core::durable::Readiness::Available
        ) {
            return Err(PreflightFailureKind::Unavailable);
        }
        let location = self.report().location();
        if location.partition_id() != prepared.partition() {
            return Err(PreflightFailureKind::BindingRejected);
        }
        Ok(AppendContext::new(self.binding().fingerprint(), location))
    }

    async fn append_once(
        &self,
        attempt: &AppendAttempt,
        admission: AppendAdmission,
    ) -> AppendPortOutcome {
        if attempt.requested_boundary != ConfirmationBoundary::OsSyncedAccepted
            || attempt.location != self.report().location()
        {
            return AppendPortOutcome::NotInvoked(AttemptFailureKind::Protocol);
        }
        if !matches!(
            self.readiness(),
            alopex_chirps_core::durable::Readiness::Available
        ) {
            return AppendPortOutcome::NotInvoked(AttemptFailureKind::Unavailable);
        }
        match self
            .append_one_synced_with_admission(
                *attempt.attempt_id.as_bytes(),
                *attempt.message_id.as_bytes(),
                attempt.envelope_digest,
                attempt.canonical_envelope.clone(),
                move || admission.admit(),
            )
            .await
        {
            Ok(response) => AppendPortOutcome::OsSyncedAccepted(
                StrongAppendConfirmation::from_verified(attempt, response),
            ),
            Err(SessionInvocationError::Session(_)) => {
                AppendPortOutcome::NotInvoked(AttemptFailureKind::Unavailable)
            }
            Err(SessionInvocationError::InvalidRequest) => {
                AppendPortOutcome::NotInvoked(AttemptFailureKind::Protocol)
            }
            Err(SessionInvocationError::Invocation(InvocationError::NotInvoked(_))) => {
                AppendPortOutcome::NotInvoked(AttemptFailureKind::Transport)
            }
            Err(SessionInvocationError::Invocation(InvocationError::Indeterminate(_))) => {
                AppendPortOutcome::Indeterminate(AttemptFailureKind::Transport)
            }
            Err(SessionInvocationError::InvalidResponse) => {
                AppendPortOutcome::Indeterminate(AttemptFailureKind::Protocol)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppendAttempt, AppendContext, AppendPort, AppendPortOutcome, AttemptTerminal,
        DevelopmentBrokerConfigReadback, DevelopmentProfileError, ProducerCoordinator,
        StrongAppendConfirmation, prepare, validate_development_resource_readback,
        validate_standard_append_response,
    };
    use crate::protocol::{
        AppendOneSyncedRequest, AppendOneSyncedResponse, PrivateRequest, PrivateResponse,
        ResourceLocation, SessionBinding, VerifiedPrivateResponse,
    };
    use crate::routing::{PartitionRouter, ROUTING_MAP_VERSION, ValidatedRoutingConfiguration};
    use alopex_chirps_core::durable::{
        AttemptFailureKind, AttemptPhase, ConfirmationBoundary, DurableSendOutcome,
        PreflightFailureKind, PreparedDurableSend, ResourceId, SessionFingerprint,
    };
    use alopex_chirps_wire::node_id::NodeId;
    use async_trait::async_trait;
    use bytes::BytesMut;
    use iggy_binary_protocol::responses::streams::TopicHeader;
    use iggy_binary_protocol::responses::topics::{GetTopicResponse, PartitionResponse};
    use iggy_binary_protocol::{ResponseFrame, WireEncode, WireName};
    use sha2::{Digest, Sha256};
    use std::collections::VecDeque;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, timeout};

    #[derive(Debug, Clone, Copy)]
    enum FakeOutcome {
        NotInvoked(AttemptFailureKind),
        Indeterminate(AttemptFailureKind),
        BrokerAccepted,
        OsSyncedAccepted(ResourceLocation),
        CacheStrong(ResourceLocation),
        ReplayCachedStrong,
        BlockBeforeAdmission,
        BlockAfterInvocation,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ObservedAttempt {
        attempt_id: [u8; 16],
        message_id: [u8; 16],
        envelope_digest: [u8; 32],
        boundary: ConfirmationBoundary,
        location: ResourceLocation,
        canonical_envelope: Vec<u8>,
    }

    #[derive(Debug)]
    struct FakePort {
        context: AppendContext,
        preflight_error: Option<PreflightFailureKind>,
        outcomes: Mutex<VecDeque<FakeOutcome>>,
        append_calls: AtomicUsize,
        invocation_count: AtomicUsize,
        observed: Mutex<Vec<ObservedAttempt>>,
        cached_strong: Mutex<Option<StrongAppendConfirmation>>,
    }

    impl FakePort {
        fn new(
            location: ResourceLocation,
            outcomes: impl IntoIterator<Item = FakeOutcome>,
        ) -> Self {
            Self {
                context: AppendContext::new(SessionFingerprint::from_bytes([0x31; 32]), location),
                preflight_error: None,
                outcomes: Mutex::new(outcomes.into_iter().collect()),
                append_calls: AtomicUsize::new(0),
                invocation_count: AtomicUsize::new(0),
                observed: Mutex::new(Vec::new()),
                cached_strong: Mutex::new(None),
            }
        }

        fn rejecting(location: ResourceLocation, error: PreflightFailureKind) -> Self {
            let mut port = Self::new(location, []);
            port.preflight_error = Some(error);
            port
        }
    }

    #[async_trait]
    impl AppendPort for FakePort {
        fn preflight(
            &self,
            _prepared: &PreparedDurableSend,
            _requested_boundary: ConfirmationBoundary,
        ) -> Result<AppendContext, PreflightFailureKind> {
            self.preflight_error.map_or(Ok(self.context), Err)
        }

        async fn append_once(
            &self,
            attempt: &AppendAttempt,
            admission: super::AppendAdmission,
        ) -> AppendPortOutcome {
            self.append_calls.fetch_add(1, Ordering::SeqCst);
            self.observed.lock().unwrap().push(ObservedAttempt {
                attempt_id: *attempt.attempt_id().as_bytes(),
                message_id: *attempt.message_id().as_bytes(),
                envelope_digest: *attempt.envelope_digest().as_bytes(),
                boundary: attempt.requested_boundary(),
                location: attempt.location(),
                canonical_envelope: attempt.canonical_envelope().to_vec(),
            });
            let outcome = self.outcomes.lock().unwrap().pop_front().unwrap();
            match outcome {
                FakeOutcome::NotInvoked(failure) => AppendPortOutcome::NotInvoked(failure),
                FakeOutcome::Indeterminate(failure) => {
                    admission.admit();
                    self.invocation_count.fetch_add(1, Ordering::SeqCst);
                    AppendPortOutcome::Indeterminate(failure)
                }
                FakeOutcome::BrokerAccepted => {
                    admission.admit();
                    self.invocation_count.fetch_add(1, Ordering::SeqCst);
                    AppendPortOutcome::BrokerAccepted
                }
                FakeOutcome::OsSyncedAccepted(location) => {
                    admission.admit();
                    self.invocation_count.fetch_add(1, Ordering::SeqCst);
                    AppendPortOutcome::OsSyncedAccepted(strong_confirmation(attempt, location))
                }
                FakeOutcome::CacheStrong(location) => {
                    admission.admit();
                    self.invocation_count.fetch_add(1, Ordering::SeqCst);
                    let confirmation = strong_confirmation(attempt, location);
                    *self.cached_strong.lock().unwrap() = Some(confirmation.clone());
                    AppendPortOutcome::OsSyncedAccepted(confirmation)
                }
                FakeOutcome::ReplayCachedStrong => {
                    admission.admit();
                    self.invocation_count.fetch_add(1, Ordering::SeqCst);
                    AppendPortOutcome::OsSyncedAccepted(
                        self.cached_strong.lock().unwrap().clone().unwrap(),
                    )
                }
                FakeOutcome::BlockBeforeAdmission => pending::<AppendPortOutcome>().await,
                FakeOutcome::BlockAfterInvocation => {
                    admission.admit();
                    self.invocation_count.fetch_add(1, Ordering::SeqCst);
                    pending::<AppendPortOutcome>().await
                }
            }
        }
    }

    fn uuid_v4(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x40 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn location(seed: u8, partition: u32) -> ResourceLocation {
        ResourceLocation::new(
            ResourceId::from_bytes(uuid_v4(seed)),
            u64::from(seed),
            11,
            22,
            partition,
        )
        .unwrap()
    }

    fn prepared() -> PreparedDurableSend {
        let router = PartitionRouter::from_validated_configuration(ValidatedRoutingConfiguration {
            source: NodeId::new(),
            generation: 7,
            partition_count: 1,
            mapping_version: ROUTING_MAP_VERSION,
        })
        .unwrap();
        prepare(
            &router,
            NodeId::new(),
            b"account-42".to_vec(),
            b"one immutable payload",
        )
        .unwrap()
    }

    fn development_resource_response(topic_id: u32, partition_id: u32, replication: u8) -> Vec<u8> {
        let response = GetTopicResponse {
            topic: TopicHeader {
                id: topic_id,
                created_at: 1,
                partitions_count: 1,
                message_expiry: 0,
                compression_algorithm: 1,
                max_topic_size: 1024,
                replication_factor: replication,
                size_bytes: 0,
                messages_count: 0,
                name: WireName::new("chirps-dev").unwrap(),
            },
            partitions: vec![PartitionResponse {
                id: partition_id,
                created_at: 1,
                segments_count: 0,
                current_offset: 0,
                size_bytes: 0,
                messages_count: 0,
            }],
        }
        .to_bytes();
        let mut frame = BytesMut::new();
        ResponseFrame::encode_ok(&response, &mut frame).unwrap();
        frame.to_vec()
    }

    fn strong_confirmation(
        attempt: &AppendAttempt,
        response_location: ResourceLocation,
    ) -> StrongAppendConfirmation {
        let binding = SessionBinding::new(
            uuid_v4(0x41),
            uuid_v4(0x42),
            SessionFingerprint::from_bytes([0x31; 32]),
        );
        let request = PrivateRequest::AppendOneSynced(
            AppendOneSyncedRequest::new(
                binding,
                *attempt.attempt_id().as_bytes(),
                response_location,
                *attempt.message_id().as_bytes(),
                attempt.envelope_digest(),
                attempt.canonical_envelope().to_vec(),
            )
            .unwrap(),
        );
        let response = PrivateResponse::AppendOneSynced(
            AppendOneSyncedResponse::new(
                binding,
                *attempt.attempt_id().as_bytes(),
                response_location,
                90,
                91,
                *attempt.message_id().as_bytes(),
                attempt.envelope_digest(),
            )
            .unwrap(),
        )
        .encode()
        .unwrap();
        let verified = match PrivateResponse::decode_for_request(&response, &request).unwrap() {
            VerifiedPrivateResponse::AppendOneSynced(response) => response,
            _ => unreachable!(),
        };
        StrongAppendConfirmation::from_verified(attempt, verified)
    }

    async fn terminal(handle: &mut super::SendAttemptHandle) -> Arc<AttemptTerminal> {
        handle.terminal().await
    }

    #[test]
    fn v07_task_3_7_prepare_freezes_router_codec_and_payload_without_backend_contact() {
        let prepared = prepared();
        assert_eq!(prepared.partition(), 0);
        assert_eq!(prepared.ordering_key(), b"account-42");
        assert_eq!(prepared.codec_version(), crate::codec::CODEC_VERSION);
        assert!(!prepared.canonical_bytes().is_empty());
    }

    #[test]
    fn v07_task_3_7_preflight_failure_has_not_attempted_binding_and_no_append() {
        let prepared = prepared();
        let port = Arc::new(FakePort::rejecting(
            location(1, 0),
            PreflightFailureKind::Unavailable,
        ));
        let coordinator = ProducerCoordinator::new(Arc::clone(&port));

        let error = match coordinator.preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted) {
            Ok(_) => panic!("preflight unexpectedly succeeded"),
            Err(error) => error,
        };

        assert_eq!(error.message_id(), prepared.message_id());
        assert_eq!(error.kind(), PreflightFailureKind::Unavailable);
        assert_eq!(error.binding().phase(), AttemptPhase::NotAttempted);
        assert_eq!(port.append_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn v07_task_3_7_preflight_rejects_location_partition_mismatch() {
        let prepared = prepared();
        let port = Arc::new(FakePort::new(location(1, 1), []));
        let coordinator = ProducerCoordinator::new(Arc::clone(&port));

        let error = match coordinator.preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted) {
            Ok(_) => panic!("mismatched partition unexpectedly passed"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), PreflightFailureKind::BindingRejected);
        assert_eq!(port.append_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn v07_task_3_7_cancel_before_execute_is_bound_not_submitted() {
        let prepared = prepared();
        let port = Arc::new(FakePort::new(location(1, 0), []));
        let coordinator = ProducerCoordinator::new(Arc::clone(&port));
        let started = coordinator
            .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .unwrap()
            .start()
            .unwrap();

        let result = started.cancel_before_execute().unwrap();

        assert_eq!(
            result.outcome(),
            DurableSendOutcome::NotSubmitted(AttemptFailureKind::Cancelled)
        );
        assert_eq!(result.attempt_binding().phase(), AttemptPhase::Started);
        assert_eq!(port.append_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn v07_task_3_7_preinvocation_failure_is_not_submitted() {
        let prepared = prepared();
        let port = Arc::new(FakePort::new(
            location(1, 0),
            [FakeOutcome::NotInvoked(AttemptFailureKind::Unavailable)],
        ));
        let coordinator = ProducerCoordinator::new(Arc::clone(&port));
        let started = coordinator
            .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .unwrap()
            .start()
            .unwrap();

        let mut handle = coordinator.execute(started);
        let completed = terminal(&mut handle).await;
        let result = completed.result().unwrap();

        assert_eq!(
            result.outcome(),
            DurableSendOutcome::NotSubmitted(AttemptFailureKind::Unavailable)
        );
        assert_eq!(result.attempt_binding().phase(), AttemptPhase::Started);
        assert!(result.receipt().is_none());
        assert_eq!(port.append_calls.load(Ordering::SeqCst), 1);
        assert_eq!(port.invocation_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn v07_task_3_7_every_postinvocation_failure_is_indeterminate_once() {
        for failure in [
            AttemptFailureKind::Transport,
            AttemptFailureKind::Protocol,
            AttemptFailureKind::Cancelled,
            AttemptFailureKind::Shutdown,
        ] {
            let prepared = prepared();
            let port = Arc::new(FakePort::new(
                location(1, 0),
                [FakeOutcome::Indeterminate(failure)],
            ));
            let coordinator = ProducerCoordinator::new(Arc::clone(&port));
            let started = coordinator
                .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
                .unwrap()
                .start()
                .unwrap();

            let mut handle = coordinator.execute(started);
            let completed = terminal(&mut handle).await;
            let result = completed.result().unwrap();

            assert_eq!(result.outcome(), DurableSendOutcome::Indeterminate(failure));
            assert_eq!(
                result.attempt_binding().phase(),
                AttemptPhase::AppendInvoked
            );
            assert!(result.receipt().is_none());
            assert_eq!(port.append_calls.load(Ordering::SeqCst), 1);
            assert_eq!(port.invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn v07_task_3_7_development_success_is_weak_and_location_free() {
        let prepared = prepared();
        let port = Arc::new(FakePort::new(location(1, 0), [FakeOutcome::BrokerAccepted]));
        let coordinator = ProducerCoordinator::new(Arc::clone(&port));
        let started = coordinator
            .preflight(&prepared, ConfirmationBoundary::BrokerAccepted)
            .unwrap()
            .start()
            .unwrap();

        let mut handle = coordinator.execute(started);
        let completed = terminal(&mut handle).await;
        let result = completed.result().unwrap();

        assert_eq!(result.outcome(), DurableSendOutcome::BrokerAccepted);
        assert!(result.receipt().is_none());
        assert_eq!(port.invocation_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn v07_task_3_7_verified_strong_response_builds_exact_receipt() {
        let prepared = prepared();
        let exact_location = location(1, 0);
        let port = Arc::new(FakePort::new(
            exact_location,
            [FakeOutcome::OsSyncedAccepted(exact_location)],
        ));
        let coordinator = ProducerCoordinator::new(Arc::clone(&port));
        let started = coordinator
            .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .unwrap()
            .start()
            .unwrap();
        let attempt_id = started.attempt_id();

        let mut handle = coordinator.execute(started);
        let completed = terminal(&mut handle).await;
        let result = completed.result().unwrap();
        let receipt = result.receipt().unwrap();

        assert_eq!(result.outcome(), DurableSendOutcome::OsSyncedAccepted);
        assert_eq!(receipt.resource_epoch(), exact_location.resource_epoch());
        assert_eq!(receipt.partition(), prepared.partition());
        assert_eq!(receipt.assigned_offset(), 90);
        assert_eq!(receipt.assigned_index(), 91);
        assert_eq!(receipt.message_id(), prepared.message_id());
        assert_eq!(receipt.envelope_digest(), prepared.envelope_digest());
        assert_eq!(receipt.attempt_binding().attempt_id(), Some(attempt_id));
        assert_eq!(
            receipt.attempt_binding().session_fingerprint(),
            Some(SessionFingerprint::from_bytes([0x31; 32]))
        );
    }

    #[tokio::test]
    async fn v07_task_3_7_success_boundary_or_location_substitution_is_indeterminate() {
        let prepared = prepared();
        let exact_location = location(1, 0);

        let weak_port = Arc::new(FakePort::new(exact_location, [FakeOutcome::BrokerAccepted]));
        let weak_coordinator = ProducerCoordinator::new(Arc::clone(&weak_port));
        let mut weak_handle = weak_coordinator.execute(
            weak_coordinator
                .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
                .unwrap()
                .start()
                .unwrap(),
        );
        let weak_terminal = terminal(&mut weak_handle).await;
        let weak = weak_terminal.result().unwrap();
        assert_eq!(
            weak.outcome(),
            DurableSendOutcome::Indeterminate(AttemptFailureKind::Protocol)
        );

        let substituted_location = location(2, 0);
        let strong_port = Arc::new(FakePort::new(
            exact_location,
            [FakeOutcome::OsSyncedAccepted(substituted_location)],
        ));
        let strong_coordinator = ProducerCoordinator::new(Arc::clone(&strong_port));
        let mut strong_handle = strong_coordinator.execute(
            strong_coordinator
                .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
                .unwrap()
                .start()
                .unwrap(),
        );
        let strong_terminal = terminal(&mut strong_handle).await;
        let strong = strong_terminal.result().unwrap();
        assert_eq!(
            strong.outcome(),
            DurableSendOutcome::Indeterminate(AttemptFailureKind::Protocol)
        );
        assert!(strong.receipt().is_none());
    }

    #[tokio::test]
    async fn v07_task_3_7_cached_strong_proof_cannot_confirm_another_attempt() {
        let prepared = prepared();
        let exact_location = location(1, 0);
        let port = Arc::new(FakePort::new(
            exact_location,
            [
                FakeOutcome::CacheStrong(exact_location),
                FakeOutcome::ReplayCachedStrong,
            ],
        ));
        let coordinator = ProducerCoordinator::new(Arc::clone(&port));

        let first = coordinator
            .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .unwrap()
            .start()
            .unwrap();
        let mut first_handle = coordinator.execute(first);
        let first_terminal = terminal(&mut first_handle).await;
        assert_eq!(
            first_terminal.result().unwrap().outcome(),
            DurableSendOutcome::OsSyncedAccepted
        );

        let second = coordinator
            .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .unwrap()
            .start()
            .unwrap();
        let mut second_handle = coordinator.execute(second);
        let second_terminal = terminal(&mut second_handle).await;
        let second_result = second_terminal.result().unwrap();
        assert_eq!(
            second_result.outcome(),
            DurableSendOutcome::Indeterminate(AttemptFailureKind::Protocol)
        );
        assert!(second_result.receipt().is_none());
    }

    #[tokio::test]
    async fn v07_task_3_7_dropped_wait_then_postinvocation_cancel_retains_terminal_result() {
        let prepared = prepared();
        let port = Arc::new(FakePort::new(
            location(1, 0),
            [FakeOutcome::BlockAfterInvocation],
        ));
        let coordinator = ProducerCoordinator::new(Arc::clone(&port));
        let started = coordinator
            .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .unwrap()
            .start()
            .unwrap();
        let mut handle = coordinator.execute(started);

        timeout(Duration::from_secs(1), async {
            while port.invocation_count.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            timeout(Duration::from_millis(10), handle.terminal())
                .await
                .is_err()
        );

        handle.cancel();
        let completed = timeout(Duration::from_secs(1), handle.terminal())
            .await
            .unwrap();
        let result = completed.result().unwrap();
        assert_eq!(
            result.outcome(),
            DurableSendOutcome::Indeterminate(AttemptFailureKind::Cancelled)
        );
        assert_eq!(
            result.attempt_binding().phase(),
            AttemptPhase::AppendInvoked
        );
        assert_eq!(port.append_calls.load(Ordering::SeqCst), 1);
        assert_eq!(port.invocation_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn v07_task_3_7_polled_but_preadmission_cancel_is_bound_not_submitted() {
        let prepared = prepared();
        let port = Arc::new(FakePort::new(
            location(1, 0),
            [FakeOutcome::BlockBeforeAdmission],
        ));
        let coordinator = ProducerCoordinator::new(Arc::clone(&port));
        let started = coordinator
            .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .unwrap()
            .start()
            .unwrap();
        let mut handle = coordinator.execute(started);

        timeout(Duration::from_secs(1), async {
            while port.append_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(port.invocation_count.load(Ordering::SeqCst), 0);

        handle.cancel();
        let completed = timeout(Duration::from_secs(1), handle.terminal())
            .await
            .unwrap();
        let result = completed.result().unwrap();
        assert_eq!(
            result.outcome(),
            DurableSendOutcome::NotSubmitted(AttemptFailureKind::Cancelled)
        );
        assert_eq!(result.attempt_binding().phase(), AttemptPhase::Started);
        assert_eq!(port.append_calls.load(Ordering::SeqCst), 1);
        assert_eq!(port.invocation_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn v07_task_3_7_standard_success_requires_exact_empty_ok_frame() {
        let mut ok = BytesMut::new();
        ResponseFrame::encode_ok(&[], &mut ok).unwrap();
        assert!(validate_standard_append_response(&ok));

        let mut payload = BytesMut::new();
        ResponseFrame::encode_ok(b"unapproved location claim", &mut payload).unwrap();
        assert!(!validate_standard_append_response(&payload));

        ok.extend_from_slice(&[0]);
        assert!(!validate_standard_append_response(&ok));
    }

    #[test]
    fn v07_task_3_7_development_profile_requires_explicit_dedup_off_readback() {
        let exact = b"[system.message_deduplication]\nenabled = false\n";
        let verified =
            DevelopmentBrokerConfigReadback::verify_actual_startup_config(exact).unwrap();
        assert_eq!(verified.digest(), Sha256::digest(exact).as_slice());

        for rejected in [
            b"[system.message_deduplication]\nenabled = true\n".as_slice(),
            b"[system.message_deduplication]\nmax_entries = 10\n".as_slice(),
            b"[other]\nenabled = false\n".as_slice(),
            b"[system.message_deduplication]\nenabled = false\nenabled = false\n".as_slice(),
        ] {
            assert!(matches!(
                DevelopmentBrokerConfigReadback::verify_actual_startup_config(rejected),
                Err(DevelopmentProfileError::DeduplicationNotDisabled)
                    | Err(DevelopmentProfileError::InvalidConfigReadback)
            ));
        }
    }

    #[test]
    fn v07_task_3_7_development_resource_readback_matches_topic_partition_and_rf_one() {
        let exact_location = location(1, 0);
        let exact = development_resource_response(exact_location.topic_id(), 0, 1);
        validate_development_resource_readback(&exact, exact_location).unwrap();

        for substituted in [
            development_resource_response(exact_location.topic_id() + 1, 0, 1),
            development_resource_response(exact_location.topic_id(), 1, 1),
            development_resource_response(exact_location.topic_id(), 0, 2),
        ] {
            assert!(validate_development_resource_readback(&substituted, exact_location).is_err());
        }
    }

    #[tokio::test]
    async fn v07_task_3_7_explicit_response_loss_retry_reuses_message_not_attempt() {
        let prepared = prepared();
        let exact_location = location(1, 0);
        let port = Arc::new(FakePort::new(
            exact_location,
            [
                FakeOutcome::Indeterminate(AttemptFailureKind::Transport),
                FakeOutcome::OsSyncedAccepted(exact_location),
            ],
        ));
        let coordinator = ProducerCoordinator::new(Arc::clone(&port));

        let first = coordinator
            .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .unwrap()
            .start()
            .unwrap();
        let first_attempt = first.attempt_id();
        let mut first_handle = coordinator.execute(first);
        let first_terminal = terminal(&mut first_handle).await;
        let first_result = first_terminal.result().unwrap();
        assert_eq!(
            first_result.outcome(),
            DurableSendOutcome::Indeterminate(AttemptFailureKind::Transport)
        );
        assert_eq!(port.append_calls.load(Ordering::SeqCst), 1);

        let second = coordinator
            .preflight(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .unwrap()
            .start()
            .unwrap();
        let second_attempt = second.attempt_id();
        let mut second_handle = coordinator.execute(second);
        let second_terminal = terminal(&mut second_handle).await;
        let second_result = second_terminal.result().unwrap();
        assert_eq!(
            second_result.outcome(),
            DurableSendOutcome::OsSyncedAccepted
        );

        assert_ne!(first_attempt, second_attempt);
        let observed = port.observed.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[0].message_id, observed[1].message_id);
        assert_eq!(observed[0].envelope_digest, observed[1].envelope_digest);
        assert_eq!(
            observed[0].canonical_envelope,
            observed[1].canonical_envelope
        );
        assert_ne!(observed[0].attempt_id, observed[1].attempt_id);
        assert_eq!(port.append_calls.load(Ordering::SeqCst), 2);
        assert_eq!(port.invocation_count.load(Ordering::SeqCst), 2);
    }
}
