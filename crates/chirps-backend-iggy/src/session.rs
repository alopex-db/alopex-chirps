//! Authenticated capability binding and same-connection lease renewal.
//!
//! A production session can only be created by a TLS connection followed by
//! one explicit successful official login and one exactly correlated private
//! `CapabilityBind`. The resulting session owns that connection, validates
//! every configured capability projection field before data-plane use, and
//! fail-closes renewal failures without reconnecting or replaying.

use crate::protocol::{
    AppendOneSyncedRequest, CapabilityBindRequest, CapabilityReport, CheckedPollRequest,
    ChecksumMode, LeaseRenewRequest, PrivateProtocolError, PrivateRequest, PrivateResponse,
    ResourceLocation, SessionBinding, VerifiedAppendOneSyncedResponse, VerifiedCheckedPollResponse,
    VerifiedPrivateResponse,
};
use crate::transport::{
    DataPlaneRequestFrame, InvocationError, LoginRequestFrame, OwnedTransport,
    SessionControlRequestFrame, TransportCloseHandle, TransportError, TransportLimits,
    TransportShutdownReport, TransportStage,
};
use alopex_chirps_core::durable::{EnvelopeDigest, Readiness, UnavailableReason};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::{ResponseFrame, STATUS_OK};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use thiserror::Error;
use tokio::time::{Duration, Instant, timeout_at};

/// Exact configured field that did not match an authenticated capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CapabilityMismatchField {
    /// Pinned compatible-server build identity.
    BuildSha,
    /// Server boot identity was absent or unknown.
    BootIdentity,
    /// Resource UUID/epoch or explicit stream/topic/partition location.
    ResourceLocation,
    /// Retention byte projection.
    RetentionBytes,
    /// Retention message-count projection.
    RetentionMessages,
    /// Partition checksum projection.
    ChecksumMode,
    /// Configuration projection digest.
    ConfigurationProjection,
    /// Security projection digest.
    SecurityProjection,
    /// Runtime capability/permission projection digest.
    CapabilityProjection,
    /// Server session identity was absent or unknown.
    SessionIdentity,
    /// Session projection fingerprint was absent or unknown.
    SessionFingerprint,
    /// Requested active lease duration was invalid.
    LeaseDuration,
}

/// Bounded reason why a previously bound session was permanently fenced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SessionFenceReason {
    /// The conservative client-side lease deadline elapsed.
    LeaseExpired = 1,
    /// The server explicitly rejected renewal.
    RenewalRejected = 2,
    /// Renewal reached the owned connection but no valid response was proven.
    RenewalIndeterminate = 3,
    /// A renewal response was malformed, stale, or bound to another session.
    ResponseMismatch = 4,
    /// The owned socket was closed or disconnected.
    Disconnected = 5,
}

/// Authentication, capability, and lease failure without credential data.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SessionError {
    /// A direct owned-transport operation failed.
    #[error("session transport failed: {0}")]
    Transport(TransportError),
    /// The configured connect/authenticate deadline elapsed.
    #[error("TLS authentication deadline elapsed")]
    Deadline,
    /// Official login returned a non-success status.
    #[error("authentication was rejected with status {0}")]
    AuthenticationRejected(u32),
    /// Official login response framing was invalid or concatenated.
    #[error("authentication response framing is invalid")]
    InvalidAuthenticationResponse,
    /// The private bind/renew response failed bounded decoding or correlation.
    #[error("session private protocol response is invalid")]
    Protocol,
    /// Runtime principal lacked the capability-bind permission.
    #[error("capability binding was denied with status {0}")]
    PermissionDenied(u32),
    /// An authenticated report did not equal the required projection.
    #[error("authenticated capability mismatch at {0:?}")]
    CapabilityMismatch(CapabilityMismatchField),
    /// The conservative client-side active-lease deadline elapsed.
    #[error("authenticated session lease expired")]
    LeaseExpired,
    /// The server explicitly rejected same-connection renewal.
    #[error("lease renewal was rejected with status {0}")]
    LeaseRejected(u32),
    /// A renewal response did not move the server-monotonic expiry forward.
    #[error("lease renewal did not advance server expiry")]
    LeaseDidNotAdvance,
    /// The session was previously fenced and cannot become active again.
    #[error("authenticated session is fenced: {0:?}")]
    Fenced(SessionFenceReason),
}

impl SessionError {
    /// Maps detailed session failures to the finite public readiness labels.
    #[must_use]
    pub const fn readiness_reason(&self) -> UnavailableReason {
        match self {
            Self::AuthenticationRejected(_) | Self::InvalidAuthenticationResponse => {
                UnavailableReason::Authentication
            }
            Self::PermissionDenied(_) => UnavailableReason::Permission,
            Self::CapabilityMismatch(_) | Self::Protocol | Self::LeaseDidNotAdvance => {
                UnavailableReason::CapabilityMismatch
            }
            Self::Transport(TransportError::Io {
                stage: TransportStage::TlsHandshake,
                ..
            }) => UnavailableReason::TlsIdentityMismatch,
            Self::Transport(_)
            | Self::Deadline
            | Self::LeaseExpired
            | Self::LeaseRejected(_)
            | Self::Fenced(_) => UnavailableReason::Connectivity,
        }
    }
}

/// Data-plane invocation rejected by session preflight or the owned transport.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SessionInvocationError {
    /// No data-plane invocation occurred because the session was not active.
    #[error("session preflight failed: {0}")]
    Session(SessionError),
    /// Canonical request validation failed before transport invocation.
    #[error("session data-plane request is invalid")]
    InvalidRequest,
    /// The data-plane request crossed the transport invocation boundary.
    #[error("session invocation failed: {0}")]
    Invocation(InvocationError),
    /// A response arrived after invocation but failed exact correlation.
    #[error("session data-plane response is invalid")]
    InvalidResponse,
}

/// Complete expected capability projection for one pre-provisioned resource.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ExpectedCapability {
    build_sha: [u8; 20],
    location: ResourceLocation,
    retention_bytes: u64,
    retention_messages: u64,
    checksum_mode: ChecksumMode,
    configuration_digest: [u8; 32],
    security_digest: [u8; 32],
    capability_digest: [u8; 32],
    lease_millis: u32,
}

impl std::fmt::Debug for ExpectedCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExpectedCapability")
            .field("location", &self.location)
            .field("retention_bytes", &self.retention_bytes)
            .field("retention_messages", &self.retention_messages)
            .field("checksum_mode", &self.checksum_mode)
            .field("lease_millis", &self.lease_millis)
            .finish_non_exhaustive()
    }
}

impl ExpectedCapability {
    /// Creates one exact pre-provisioned projection and rejects unknown
    /// identity/digest sentinels before any broker contact.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        build_sha: [u8; 20],
        location: ResourceLocation,
        retention_bytes: u64,
        retention_messages: u64,
        checksum_mode: ChecksumMode,
        configuration_digest: [u8; 32],
        security_digest: [u8; 32],
        capability_digest: [u8; 32],
        lease_millis: u32,
    ) -> Result<Self, SessionError> {
        for (unknown, field) in [
            (is_zero(&build_sha), CapabilityMismatchField::BuildSha),
            (
                is_zero(&configuration_digest),
                CapabilityMismatchField::ConfigurationProjection,
            ),
            (
                is_zero(&security_digest),
                CapabilityMismatchField::SecurityProjection,
            ),
            (
                is_zero(&capability_digest),
                CapabilityMismatchField::CapabilityProjection,
            ),
            (lease_millis == 0, CapabilityMismatchField::LeaseDuration),
        ] {
            if unknown {
                return Err(SessionError::CapabilityMismatch(field));
            }
        }
        Ok(Self {
            build_sha,
            location,
            retention_bytes,
            retention_messages,
            checksum_mode,
            configuration_digest,
            security_digest,
            capability_digest,
            lease_millis,
        })
    }

    /// Returns the exact explicit resource location.
    #[must_use]
    pub const fn location(self) -> ResourceLocation {
        self.location
    }

    /// Returns the requested active lease duration.
    #[must_use]
    pub const fn lease_millis(self) -> u32 {
        self.lease_millis
    }
}

trait SessionClock: Send + Sync {
    fn now(&self) -> Instant;
}

#[derive(Debug, Default)]
struct SystemSessionClock;

impl SessionClock for SystemSessionClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[async_trait]
trait SessionIo: Send + Sync {
    async fn session_control(
        &self,
        request: SessionControlRequestFrame,
    ) -> Result<Bytes, TransportError>;

    async fn invoke_with_admission(
        &self,
        request: DataPlaneRequestFrame,
        on_admitted: Box<dyn FnOnce() + Send>,
    ) -> Result<Bytes, InvocationError>;

    fn close(&self);

    fn is_closed(&self) -> bool;

    fn close_handle(&self) -> TransportCloseHandle;

    async fn shutdown(self: Box<Self>, deadline: Instant) -> TransportShutdownReport;
}

struct OwnedSessionIo(OwnedTransport);

#[async_trait]
impl SessionIo for OwnedSessionIo {
    async fn session_control(
        &self,
        request: SessionControlRequestFrame,
    ) -> Result<Bytes, TransportError> {
        self.0.session_control(request).await
    }

    async fn invoke_with_admission(
        &self,
        request: DataPlaneRequestFrame,
        on_admitted: Box<dyn FnOnce() + Send>,
    ) -> Result<Bytes, InvocationError> {
        self.0.invoke_with_admission(request, on_admitted).await
    }

    fn close(&self) {
        self.0.close_handle().close();
    }

    fn is_closed(&self) -> bool {
        self.0.close_handle().is_closed()
    }

    fn close_handle(&self) -> TransportCloseHandle {
        self.0.close_handle()
    }

    async fn shutdown(self: Box<Self>, deadline: Instant) -> TransportShutdownReport {
        self.0.shutdown(deadline).await
    }
}

/// TLS-authenticated owned connection that cannot invoke data-plane commands
/// until an exact capability projection has been bound.
pub struct AuthenticatedConnection {
    io: Box<dyn SessionIo>,
    clock: Arc<dyn SessionClock>,
}

impl std::fmt::Debug for AuthenticatedConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticatedConnection")
            .finish_non_exhaustive()
    }
}

impl AuthenticatedConnection {
    /// Establishes a caller-trusted TLS connection and performs exactly one
    /// explicit official login before the supplied deadline. Login failure
    /// closes and joins the owned transport before returning.
    #[allow(clippy::too_many_arguments)]
    pub async fn connect_tls_and_authenticate(
        address: SocketAddr,
        server_name: ServerName<'static>,
        client_config: Arc<ClientConfig>,
        limits: TransportLimits,
        login: LoginRequestFrame,
        deadline: Instant,
    ) -> Result<Self, SessionError> {
        let transport = timeout_at(
            deadline,
            OwnedTransport::connect_tls(address, server_name, client_config, limits),
        )
        .await
        .map_err(|_| SessionError::Deadline)?
        .map_err(SessionError::Transport)?;

        let login_result = timeout_at(deadline, transport.login(login)).await;
        let result = match login_result {
            Ok(Ok(response)) => validate_login_response(&response),
            Ok(Err(error)) => Err(SessionError::Transport(error)),
            Err(_) => Err(SessionError::Deadline),
        };
        if let Err(error) = result {
            transport.close_handle().close();
            let _ = transport.shutdown(deadline).await;
            return Err(error);
        }

        Ok(Self {
            io: Box::new(OwnedSessionIo(transport)),
            clock: Arc::new(SystemSessionClock),
        })
    }

    /// Sends one explicit capability bind. On failure the authenticated
    /// connection is closed and returned inside [`BindFailure`] for joined
    /// shutdown; data-plane access is never exposed by this unbound state.
    pub async fn bind(self, expected: ExpectedCapability) -> Result<BoundSession, BindFailure> {
        let started_at = self.clock.now();
        let local_expiry = match local_expiry(started_at, expected.lease_millis) {
            Ok(value) => value,
            Err(error) => return Err(BindFailure::new(error, self)),
        };
        let request = match CapabilityBindRequest::new(
            expected.location.stream_id(),
            expected.location.topic_id(),
            expected.location.partition_id(),
            expected.lease_millis,
        ) {
            Ok(request) => PrivateRequest::CapabilityBind(request),
            Err(_) => return Err(BindFailure::new(SessionError::Protocol, self)),
        };
        let frame = match SessionControlRequestFrame::from_private(&request) {
            Ok(frame) => frame,
            Err(_) => return Err(BindFailure::new(SessionError::Protocol, self)),
        };
        let response = match self.io.session_control(frame).await {
            Ok(response) => response,
            Err(error) => {
                return Err(BindFailure::new(SessionError::Transport(error), self));
            }
        };
        let verified = match PrivateResponse::decode_for_request(&response, &request) {
            Ok(VerifiedPrivateResponse::CapabilityBind(response)) => response,
            Err(PrivateProtocolError::RemoteStatus(status)) => {
                return Err(BindFailure::new(
                    SessionError::PermissionDenied(status),
                    self,
                ));
            }
            Err(PrivateProtocolError::ResponseBindingMismatch("location")) => {
                return Err(BindFailure::new(
                    SessionError::CapabilityMismatch(CapabilityMismatchField::ResourceLocation),
                    self,
                ));
            }
            _ => return Err(BindFailure::new(SessionError::Protocol, self)),
        };
        let report = verified.report();
        let binding = verified.binding();
        if let Err(error) = validate_capability(report, binding, expected) {
            return Err(BindFailure::new(error, self));
        }
        if self.clock.now() >= local_expiry {
            return Err(BindFailure::new(SessionError::LeaseExpired, self));
        }

        Ok(BoundSession {
            io: self.io,
            clock: self.clock,
            expected,
            report,
            binding,
            server_expiry_millis: verified.expires_at_monotonic_millis(),
            local_expiry,
            fenced: AtomicU8::new(0),
        })
    }

    /// Returns an idempotent close handle without exposing data-plane access.
    #[must_use]
    pub fn close_handle(&self) -> TransportCloseHandle {
        self.io.close_handle()
    }

    /// Closes and joins the authenticated but unbound owned connection.
    pub async fn shutdown(self, deadline: Instant) -> TransportShutdownReport {
        self.io.shutdown(deadline).await
    }

    #[cfg(test)]
    fn from_test_io(io: Box<dyn SessionIo>, clock: Arc<dyn SessionClock>) -> Self {
        Self { io, clock }
    }
}

/// Capability-bind failure that preserves ownership of the already-closed,
/// data-plane-inaccessible connection for joined shutdown.
pub struct BindFailure {
    error: SessionError,
    connection: AuthenticatedConnection,
}

impl BindFailure {
    fn new(error: SessionError, connection: AuthenticatedConnection) -> Self {
        connection.io.close();
        Self { error, connection }
    }

    /// Returns the bounded bind failure.
    #[must_use]
    pub const fn error(&self) -> &SessionError {
        &self.error
    }

    /// Returns a cloneable handle for the already-requested connection close.
    #[must_use]
    pub fn close_handle(&self) -> TransportCloseHandle {
        self.connection.close_handle()
    }

    /// Joins every owned worker after the failed bind closed the connection.
    pub async fn shutdown(self, deadline: Instant) -> TransportShutdownReport {
        self.connection.shutdown(deadline).await
    }
}

impl std::fmt::Debug for BindFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BindFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for BindFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for BindFailure {}

/// One exact capability binding and lease on its original authenticated
/// connection. A fenced instance can only be shut down, never rebound.
pub struct BoundSession {
    io: Box<dyn SessionIo>,
    clock: Arc<dyn SessionClock>,
    expected: ExpectedCapability,
    report: CapabilityReport,
    binding: SessionBinding,
    server_expiry_millis: u64,
    local_expiry: Instant,
    fenced: AtomicU8,
}

impl std::fmt::Debug for BoundSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundSession")
            .field("report", &self.report)
            .field("server_expiry_millis", &self.server_expiry_millis)
            .field("fenced", &self.stored_fence_reason())
            .finish_non_exhaustive()
    }
}

impl BoundSession {
    /// Returns the correlation-verified capability projection.
    #[must_use]
    pub const fn report(&self) -> CapabilityReport {
        self.report
    }

    /// Returns the exact session/boot/fingerprint binding.
    #[must_use]
    pub const fn binding(&self) -> SessionBinding {
        self.binding
    }

    /// Returns the latest server-monotonic expiry received on this connection.
    #[must_use]
    pub const fn server_expiry_millis(&self) -> u64 {
        self.server_expiry_millis
    }

    /// Returns the permanent fence reason, if renewal fail-closed the session.
    #[must_use]
    pub fn fence_reason(&self) -> Option<SessionFenceReason> {
        if self.stored_fence_reason().is_none() && self.io.is_closed() {
            self.fence(SessionFenceReason::Disconnected);
        }
        self.stored_fence_reason()
    }

    /// Returns the current bounded readiness projection.
    #[must_use]
    pub fn readiness(&self) -> Readiness {
        match self.ensure_active() {
            Ok(()) => Readiness::Available,
            Err(error) => Readiness::Unavailable(error.readiness_reason()),
        }
    }

    /// Returns the current finite unavailable reason. Callers should use this
    /// only when [`Self::readiness`] is not available.
    #[must_use]
    pub fn unavailable_reason(&self) -> UnavailableReason {
        self.ensure_active()
            .err()
            .map_or(UnavailableReason::Connectivity, |error| {
                error.readiness_reason()
            })
    }

    /// Renews only on the same authenticated owned connection and requires
    /// the exact binding plus a strictly increasing server expiry.
    pub async fn renew(&mut self) -> Result<(), SessionError> {
        if let Some(reason) = self.fence_reason() {
            return Err(SessionError::Fenced(reason));
        }
        let started_at = self.clock.now();
        if started_at >= self.local_expiry {
            self.fence(SessionFenceReason::LeaseExpired);
            return Err(SessionError::LeaseExpired);
        }
        let candidate_local_expiry = local_expiry(started_at, self.expected.lease_millis)?;
        let request = PrivateRequest::LeaseRenew(
            LeaseRenewRequest::new(self.binding, self.expected.lease_millis)
                .map_err(|_| SessionError::Protocol)?,
        );
        let frame = SessionControlRequestFrame::from_private(&request)
            .map_err(|_| SessionError::Protocol)?;
        let response = match self.io.session_control(frame).await {
            Ok(response) => response,
            Err(error) => {
                self.fence(SessionFenceReason::RenewalIndeterminate);
                return Err(SessionError::Transport(error));
            }
        };
        let verified = match PrivateResponse::decode_for_request(&response, &request) {
            Ok(VerifiedPrivateResponse::LeaseRenew(response)) => response,
            Err(PrivateProtocolError::RemoteStatus(status)) => {
                self.fence(SessionFenceReason::RenewalRejected);
                return Err(SessionError::LeaseRejected(status));
            }
            _ => {
                self.fence(SessionFenceReason::ResponseMismatch);
                return Err(SessionError::Protocol);
            }
        };
        if verified.binding() != self.binding {
            self.fence(SessionFenceReason::ResponseMismatch);
            return Err(SessionError::Protocol);
        }
        if verified.expires_at_monotonic_millis() <= self.server_expiry_millis {
            self.fence(SessionFenceReason::ResponseMismatch);
            return Err(SessionError::LeaseDidNotAdvance);
        }
        if self.clock.now() >= candidate_local_expiry {
            self.fence(SessionFenceReason::LeaseExpired);
            return Err(SessionError::LeaseExpired);
        }
        self.server_expiry_millis = verified.expires_at_monotonic_millis();
        self.local_expiry = candidate_local_expiry;
        Ok(())
    }

    /// Constructs and invokes one strong append from this session's exact
    /// verified binding and resource location.
    pub async fn append_one_synced(
        &self,
        attempt_id: [u8; 16],
        message_id: [u8; 16],
        envelope_digest: EnvelopeDigest,
        canonical_envelope: Vec<u8>,
    ) -> Result<VerifiedAppendOneSyncedResponse, SessionInvocationError> {
        self.append_one_synced_with_admission(
            attempt_id,
            message_id,
            envelope_digest,
            canonical_envelope,
            || {},
        )
        .await
    }

    /// Constructs one strong append and fires `on_admitted` only after the
    /// owned transport has accepted the exact data-plane job into its queue.
    pub async fn append_one_synced_with_admission<F>(
        &self,
        attempt_id: [u8; 16],
        message_id: [u8; 16],
        envelope_digest: EnvelopeDigest,
        canonical_envelope: Vec<u8>,
        on_admitted: F,
    ) -> Result<VerifiedAppendOneSyncedResponse, SessionInvocationError>
    where
        F: FnOnce() + Send + 'static,
    {
        let request = PrivateRequest::AppendOneSynced(
            AppendOneSyncedRequest::new(
                self.binding,
                attempt_id,
                self.expected.location,
                message_id,
                envelope_digest,
                canonical_envelope,
            )
            .map_err(|_| SessionInvocationError::InvalidRequest)?,
        );
        match self
            .invoke_private_with_admission(request, Box::new(on_admitted))
            .await?
        {
            VerifiedPrivateResponse::AppendOneSynced(response) => Ok(response),
            _ => {
                self.fence(SessionFenceReason::ResponseMismatch);
                Err(SessionInvocationError::InvalidResponse)
            }
        }
    }

    /// Constructs and invokes one checked poll from this session's exact
    /// verified binding and resource location.
    pub async fn checked_poll(
        &self,
        expected_offset: u64,
    ) -> Result<VerifiedCheckedPollResponse, SessionInvocationError> {
        let request = PrivateRequest::CheckedPoll(CheckedPollRequest::new(
            self.binding,
            self.expected.location,
            expected_offset,
        ));
        match self.invoke_private(request).await? {
            VerifiedPrivateResponse::CheckedPoll(response) => Ok(response),
            _ => {
                self.fence(SessionFenceReason::ResponseMismatch);
                Err(SessionInvocationError::InvalidResponse)
            }
        }
    }

    /// Returns an idempotent close handle for lifecycle deadline coordination.
    #[must_use]
    pub fn close_handle(&self) -> TransportCloseHandle {
        self.io.close_handle()
    }

    /// Closes and joins the exact connection that owns this binding.
    pub async fn shutdown(self, deadline: Instant) -> TransportShutdownReport {
        self.io.shutdown(deadline).await
    }

    fn ensure_active(&self) -> Result<(), SessionError> {
        if let Some(reason) = self.stored_fence_reason() {
            return Err(SessionError::Fenced(reason));
        }
        if self.io.is_closed() {
            self.fence(SessionFenceReason::Disconnected);
            return Err(SessionError::Fenced(SessionFenceReason::Disconnected));
        }
        if self.clock.now() >= self.local_expiry {
            self.fence(SessionFenceReason::LeaseExpired);
            return Err(SessionError::LeaseExpired);
        }
        Ok(())
    }

    async fn invoke_private(
        &self,
        request: PrivateRequest,
    ) -> Result<VerifiedPrivateResponse, SessionInvocationError> {
        self.invoke_private_with_admission(request, Box::new(|| {}))
            .await
    }

    async fn invoke_private_with_admission(
        &self,
        request: PrivateRequest,
        on_admitted: Box<dyn FnOnce() + Send>,
    ) -> Result<VerifiedPrivateResponse, SessionInvocationError> {
        self.ensure_active()
            .map_err(SessionInvocationError::Session)?;
        let frame = DataPlaneRequestFrame::from_private(&request)
            .map_err(|_| SessionInvocationError::InvalidRequest)?;
        let response = match self.io.invoke_with_admission(frame, on_admitted).await {
            Ok(response) => response,
            Err(error) => {
                if self.io.is_closed() {
                    self.fence(SessionFenceReason::Disconnected);
                }
                return Err(SessionInvocationError::Invocation(error));
            }
        };
        match PrivateResponse::decode_for_request(&response, &request) {
            Ok(response) => Ok(response),
            Err(_) => {
                self.fence(SessionFenceReason::ResponseMismatch);
                Err(SessionInvocationError::InvalidResponse)
            }
        }
    }

    fn fence(&self, reason: SessionFenceReason) {
        let _ = self
            .fenced
            .compare_exchange(0, reason as u8, Ordering::AcqRel, Ordering::Acquire);
        self.io.close();
    }

    fn stored_fence_reason(&self) -> Option<SessionFenceReason> {
        match self.fenced.load(Ordering::Acquire) {
            0 => None,
            1 => Some(SessionFenceReason::LeaseExpired),
            2 => Some(SessionFenceReason::RenewalRejected),
            3 => Some(SessionFenceReason::RenewalIndeterminate),
            4 => Some(SessionFenceReason::ResponseMismatch),
            5 => Some(SessionFenceReason::Disconnected),
            _ => Some(SessionFenceReason::ResponseMismatch),
        }
    }
}

fn validate_login_response(bytes: &[u8]) -> Result<(), SessionError> {
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

fn validate_capability(
    report: CapabilityReport,
    binding: SessionBinding,
    expected: ExpectedCapability,
) -> Result<(), SessionError> {
    let mismatch = if is_zero(report.boot_id()) {
        Some(CapabilityMismatchField::BootIdentity)
    } else if is_zero(binding.session_id()) {
        Some(CapabilityMismatchField::SessionIdentity)
    } else if is_zero(binding.fingerprint().as_bytes()) {
        Some(CapabilityMismatchField::SessionFingerprint)
    } else if report.build_sha() != &expected.build_sha {
        Some(CapabilityMismatchField::BuildSha)
    } else if report.location() != expected.location {
        Some(CapabilityMismatchField::ResourceLocation)
    } else if report.retention_bytes() != expected.retention_bytes {
        Some(CapabilityMismatchField::RetentionBytes)
    } else if report.retention_messages() != expected.retention_messages {
        Some(CapabilityMismatchField::RetentionMessages)
    } else if report.checksum_mode() != expected.checksum_mode {
        Some(CapabilityMismatchField::ChecksumMode)
    } else if report.configuration_digest() != &expected.configuration_digest {
        Some(CapabilityMismatchField::ConfigurationProjection)
    } else if report.security_digest() != &expected.security_digest {
        Some(CapabilityMismatchField::SecurityProjection)
    } else if report.capability_digest() != &expected.capability_digest {
        Some(CapabilityMismatchField::CapabilityProjection)
    } else if binding.boot_id() != report.boot_id() {
        Some(CapabilityMismatchField::BootIdentity)
    } else {
        None
    };
    mismatch.map_or(Ok(()), |field| Err(SessionError::CapabilityMismatch(field)))
}

fn local_expiry(started_at: Instant, lease_millis: u32) -> Result<Instant, SessionError> {
    started_at
        .checked_add(Duration::from_millis(u64::from(lease_millis)))
        .ok_or(SessionError::LeaseExpired)
}

fn is_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| *byte == 0)
}

#[cfg(test)]
mod tests {
    use super::{
        AuthenticatedConnection, CapabilityMismatchField, ExpectedCapability, SessionClock,
        SessionError, SessionFenceReason, SessionInvocationError, SessionIo,
        validate_login_response,
    };
    use crate::protocol::{
        CapabilityBindResponse, CapabilityReport, CheckedPollResponse, ChecksumMode,
        LeaseRenewResponse, PrivateResponse, ResourceLocation, SessionBinding,
    };
    use crate::routing::{PartitionRouter, ROUTING_MAP_VERSION, ValidatedRoutingConfiguration};
    use crate::transport::{
        DataPlaneRequestFrame, InvocationError, SessionControlRequestFrame, TransportCloseHandle,
        TransportError, TransportShutdownReport, TransportStage,
    };
    use alopex_chirps_core::durable::{
        Readiness, ResourceId, SessionFingerprint, UnavailableReason,
    };
    use alopex_chirps_wire::node_id::NodeId;
    use async_trait::async_trait;
    use bytes::{Bytes, BytesMut};
    use iggy_binary_protocol::ResponseFrame;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::time::{Duration, Instant};

    const BUILD_SHA: [u8; 20] = [0x11; 20];
    const BOOT_ID: [u8; 16] = [0x22; 16];
    const SESSION_ID: [u8; 16] = [0x33; 16];
    const CONFIGURATION_DIGEST: [u8; 32] = [0x44; 32];
    const SECURITY_DIGEST: [u8; 32] = [0x55; 32];
    const CAPABILITY_DIGEST: [u8; 32] = [0x66; 32];
    const FINGERPRINT: [u8; 32] = [0x77; 32];

    #[derive(Debug)]
    struct ManualClock {
        base: Instant,
        elapsed_millis: AtomicU64,
    }

    impl ManualClock {
        fn new() -> Self {
            Self {
                base: Instant::now(),
                elapsed_millis: AtomicU64::new(0),
            }
        }

        fn advance(&self, millis: u64) {
            self.elapsed_millis.fetch_add(millis, Ordering::SeqCst);
        }
    }

    impl SessionClock for ManualClock {
        fn now(&self) -> Instant {
            self.base + Duration::from_millis(self.elapsed_millis.load(Ordering::SeqCst))
        }
    }

    #[derive(Debug)]
    struct FakeIo {
        control_responses: Mutex<VecDeque<Result<Bytes, TransportError>>>,
        control_calls: AtomicUsize,
        invocation_calls: AtomicUsize,
        fail_before_admission: AtomicBool,
        closed: AtomicBool,
    }

    impl FakeIo {
        fn new(responses: Vec<Result<Bytes, TransportError>>) -> Self {
            Self {
                control_responses: Mutex::new(responses.into()),
                control_calls: AtomicUsize::new(0),
                invocation_calls: AtomicUsize::new(0),
                fail_before_admission: AtomicBool::new(false),
                closed: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl SessionIo for Arc<FakeIo> {
        async fn session_control(
            &self,
            _request: SessionControlRequestFrame,
        ) -> Result<Bytes, TransportError> {
            self.control_calls.fetch_add(1, Ordering::SeqCst);
            self.control_responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("one configured control response")
        }

        async fn invoke_with_admission(
            &self,
            _request: DataPlaneRequestFrame,
            on_admitted: Box<dyn FnOnce() + Send>,
        ) -> Result<Bytes, InvocationError> {
            if self.fail_before_admission.load(Ordering::SeqCst) {
                return Err(InvocationError::NotInvoked(TransportError::Closed));
            }
            on_admitted();
            self.invocation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(poll_response(
                binding(SESSION_ID, BOOT_ID, FINGERPRINT),
                location(12),
                1,
            ))
        }

        fn close(&self) {
            self.closed.store(true, Ordering::SeqCst);
        }

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::SeqCst)
        }

        fn close_handle(&self) -> TransportCloseHandle {
            panic!("not used by focused session tests")
        }

        async fn shutdown(self: Box<Self>, _deadline: Instant) -> TransportShutdownReport {
            panic!("not used by focused session tests")
        }
    }

    fn uuid_v4(fill: u8) -> [u8; 16] {
        let mut value = [fill; 16];
        value[6] = (value[6] & 0x0f) | 0x40;
        value[8] = (value[8] & 0x3f) | 0x80;
        value
    }

    fn location(partition_id: u32) -> ResourceLocation {
        ResourceLocation::new(
            ResourceId::from_bytes(uuid_v4(0x88)),
            9,
            10,
            11,
            partition_id,
        )
        .unwrap()
    }

    fn expected(lease_millis: u32) -> ExpectedCapability {
        ExpectedCapability::new(
            BUILD_SHA,
            location(12),
            1_000_000,
            10_000,
            ChecksumMode::Enabled,
            CONFIGURATION_DIGEST,
            SECURITY_DIGEST,
            CAPABILITY_DIGEST,
            lease_millis,
        )
        .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn report(
        build_sha: [u8; 20],
        boot_id: [u8; 16],
        location: ResourceLocation,
        retention_bytes: u64,
        retention_messages: u64,
        checksum_mode: ChecksumMode,
        configuration_digest: [u8; 32],
        security_digest: [u8; 32],
        capability_digest: [u8; 32],
    ) -> CapabilityReport {
        CapabilityReport::new(
            build_sha,
            boot_id,
            location,
            retention_bytes,
            retention_messages,
            checksum_mode,
            configuration_digest,
            security_digest,
            capability_digest,
        )
    }

    fn matching_report() -> CapabilityReport {
        report(
            BUILD_SHA,
            BOOT_ID,
            location(12),
            1_000_000,
            10_000,
            ChecksumMode::Enabled,
            CONFIGURATION_DIGEST,
            SECURITY_DIGEST,
            CAPABILITY_DIGEST,
        )
    }

    fn binding(session_id: [u8; 16], boot_id: [u8; 16], fingerprint: [u8; 32]) -> SessionBinding {
        SessionBinding::new(
            session_id,
            boot_id,
            SessionFingerprint::from_bytes(fingerprint),
        )
    }

    fn bind_response(
        report: CapabilityReport,
        session_id: [u8; 16],
        fingerprint: [u8; 32],
        server_expiry: u64,
    ) -> Bytes {
        PrivateResponse::CapabilityBind(
            CapabilityBindResponse::new(
                report,
                session_id,
                SessionFingerprint::from_bytes(fingerprint),
                server_expiry,
            )
            .unwrap(),
        )
        .encode()
        .unwrap()
    }

    fn renew_response(binding: SessionBinding, server_expiry: u64) -> Bytes {
        PrivateResponse::LeaseRenew(LeaseRenewResponse::new(binding, server_expiry).unwrap())
            .encode()
            .unwrap()
    }

    fn remote_error(mut valid_response: Bytes, status: u32) -> Bytes {
        let mut bytes = valid_response.to_vec();
        bytes[..4].copy_from_slice(&status.to_le_bytes());
        valid_response = Bytes::from(bytes);
        valid_response
    }

    fn fake_connection(
        responses: Vec<Result<Bytes, TransportError>>,
        clock: Arc<ManualClock>,
    ) -> (AuthenticatedConnection, Arc<FakeIo>) {
        let io = Arc::new(FakeIo::new(responses));
        (
            AuthenticatedConnection::from_test_io(Box::new(Arc::clone(&io)), clock),
            io,
        )
    }

    fn poll_response(binding: SessionBinding, location: ResourceLocation, expected: u64) -> Bytes {
        PrivateResponse::CheckedPoll(
            CheckedPollResponse::new(binding, location, expected, expected, expected, None)
                .unwrap(),
        )
        .encode()
        .unwrap()
    }

    #[tokio::test]
    async fn v07_task_3_6_exact_authenticated_projection_binds_and_allows_preflight() {
        let clock = Arc::new(ManualClock::new());
        let (connection, io) = fake_connection(
            vec![Ok(bind_response(
                matching_report(),
                SESSION_ID,
                FINGERPRINT,
                1_000,
            ))],
            Arc::clone(&clock),
        );
        let session = connection.bind(expected(100)).await.unwrap();

        assert_eq!(session.readiness(), Readiness::Available);
        assert_eq!(session.report(), matching_report());
        assert_eq!(session.server_expiry_millis(), 1_000);
        let observation = session.checked_poll(1).await.unwrap().into_observation();
        assert_eq!(observation.end_exclusive(), 1);
        assert_eq!(io.control_calls.load(Ordering::SeqCst), 1);
        assert_eq!(io.invocation_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn v07_task_3_7_strong_append_does_not_admit_before_transport_queue() {
        let router = PartitionRouter::from_validated_configuration(ValidatedRoutingConfiguration {
            source: NodeId::new(),
            generation: 7,
            partition_count: 1,
            mapping_version: ROUTING_MAP_VERSION,
        })
        .unwrap();
        let prepared = crate::producer::prepare(
            &router,
            NodeId::new(),
            b"admission".to_vec(),
            b"canonical envelope",
        )
        .unwrap();
        let exact_report = report(
            BUILD_SHA,
            BOOT_ID,
            location(prepared.partition()),
            1_000_000,
            10_000,
            ChecksumMode::Enabled,
            CONFIGURATION_DIGEST,
            SECURITY_DIGEST,
            CAPABILITY_DIGEST,
        );
        let exact_expected = ExpectedCapability::new(
            BUILD_SHA,
            location(prepared.partition()),
            1_000_000,
            10_000,
            ChecksumMode::Enabled,
            CONFIGURATION_DIGEST,
            SECURITY_DIGEST,
            CAPABILITY_DIGEST,
            100,
        )
        .unwrap();
        let clock = Arc::new(ManualClock::new());
        let (connection, io) = fake_connection(
            vec![Ok(bind_response(
                exact_report,
                SESSION_ID,
                FINGERPRINT,
                1_000,
            ))],
            Arc::clone(&clock),
        );
        let session = connection.bind(exact_expected).await.unwrap();
        io.fail_before_admission.store(true, Ordering::SeqCst);
        let admissions = Arc::new(AtomicUsize::new(0));
        let observed_admissions = Arc::clone(&admissions);

        let result = session
            .append_one_synced_with_admission(
                uuid_v4(0x91),
                *prepared.message_id().as_bytes(),
                prepared.envelope_digest(),
                prepared.canonical_bytes().to_vec(),
                move || {
                    observed_admissions.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await;

        assert!(matches!(
            result,
            Err(SessionInvocationError::Invocation(
                InvocationError::NotInvoked(TransportError::Closed)
            ))
        ));
        assert_eq!(admissions.load(Ordering::SeqCst), 0);
        assert_eq!(io.invocation_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn v07_task_3_6_every_projection_mismatch_fails_before_data_plane() {
        let cases = vec![
            (
                CapabilityMismatchField::BuildSha,
                report(
                    [0x12; 20],
                    BOOT_ID,
                    location(12),
                    1_000_000,
                    10_000,
                    ChecksumMode::Enabled,
                    CONFIGURATION_DIGEST,
                    SECURITY_DIGEST,
                    CAPABILITY_DIGEST,
                ),
            ),
            (
                CapabilityMismatchField::ResourceLocation,
                report(
                    BUILD_SHA,
                    BOOT_ID,
                    location(13),
                    1_000_000,
                    10_000,
                    ChecksumMode::Enabled,
                    CONFIGURATION_DIGEST,
                    SECURITY_DIGEST,
                    CAPABILITY_DIGEST,
                ),
            ),
            (
                CapabilityMismatchField::RetentionBytes,
                report(
                    BUILD_SHA,
                    BOOT_ID,
                    location(12),
                    999_999,
                    10_000,
                    ChecksumMode::Enabled,
                    CONFIGURATION_DIGEST,
                    SECURITY_DIGEST,
                    CAPABILITY_DIGEST,
                ),
            ),
            (
                CapabilityMismatchField::RetentionMessages,
                report(
                    BUILD_SHA,
                    BOOT_ID,
                    location(12),
                    1_000_000,
                    9_999,
                    ChecksumMode::Enabled,
                    CONFIGURATION_DIGEST,
                    SECURITY_DIGEST,
                    CAPABILITY_DIGEST,
                ),
            ),
            (
                CapabilityMismatchField::ChecksumMode,
                report(
                    BUILD_SHA,
                    BOOT_ID,
                    location(12),
                    1_000_000,
                    10_000,
                    ChecksumMode::Disabled,
                    CONFIGURATION_DIGEST,
                    SECURITY_DIGEST,
                    CAPABILITY_DIGEST,
                ),
            ),
            (
                CapabilityMismatchField::ConfigurationProjection,
                report(
                    BUILD_SHA,
                    BOOT_ID,
                    location(12),
                    1_000_000,
                    10_000,
                    ChecksumMode::Enabled,
                    [0x45; 32],
                    SECURITY_DIGEST,
                    CAPABILITY_DIGEST,
                ),
            ),
            (
                CapabilityMismatchField::SecurityProjection,
                report(
                    BUILD_SHA,
                    BOOT_ID,
                    location(12),
                    1_000_000,
                    10_000,
                    ChecksumMode::Enabled,
                    CONFIGURATION_DIGEST,
                    [0x56; 32],
                    CAPABILITY_DIGEST,
                ),
            ),
            (
                CapabilityMismatchField::CapabilityProjection,
                report(
                    BUILD_SHA,
                    BOOT_ID,
                    location(12),
                    1_000_000,
                    10_000,
                    ChecksumMode::Enabled,
                    CONFIGURATION_DIGEST,
                    SECURITY_DIGEST,
                    [0x67; 32],
                ),
            ),
        ];

        for (field, mismatched_report) in cases {
            let clock = Arc::new(ManualClock::new());
            let (connection, io) = fake_connection(
                vec![Ok(bind_response(
                    mismatched_report,
                    SESSION_ID,
                    FINGERPRINT,
                    1_000,
                ))],
                clock,
            );
            let failure = connection.bind(expected(100)).await.unwrap_err();
            assert_eq!(failure.error(), &SessionError::CapabilityMismatch(field));
            assert_eq!(io.control_calls.load(Ordering::SeqCst), 1);
            assert_eq!(io.invocation_calls.load(Ordering::SeqCst), 0);
            assert!(io.closed.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn v07_task_3_6_unknown_boot_session_and_fingerprint_fail_closed() {
        let cases = [
            (
                CapabilityMismatchField::BootIdentity,
                bind_response(
                    report(
                        BUILD_SHA,
                        [0; 16],
                        location(12),
                        1_000_000,
                        10_000,
                        ChecksumMode::Enabled,
                        CONFIGURATION_DIGEST,
                        SECURITY_DIGEST,
                        CAPABILITY_DIGEST,
                    ),
                    SESSION_ID,
                    FINGERPRINT,
                    1_000,
                ),
            ),
            (
                CapabilityMismatchField::SessionIdentity,
                bind_response(matching_report(), [0; 16], FINGERPRINT, 1_000),
            ),
            (
                CapabilityMismatchField::SessionFingerprint,
                bind_response(matching_report(), SESSION_ID, [0; 32], 1_000),
            ),
        ];

        for (field, response) in cases {
            let clock = Arc::new(ManualClock::new());
            let (connection, io) = fake_connection(vec![Ok(response)], clock);
            let failure = connection.bind(expected(100)).await.unwrap_err();
            assert_eq!(failure.error(), &SessionError::CapabilityMismatch(field));
            assert_eq!(io.invocation_calls.load(Ordering::SeqCst), 0);
            assert!(io.closed.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn v07_task_3_6_expiry_fences_preflight_without_invocation() {
        let clock = Arc::new(ManualClock::new());
        let (connection, io) = fake_connection(
            vec![Ok(bind_response(
                matching_report(),
                SESSION_ID,
                FINGERPRINT,
                1_000,
            ))],
            Arc::clone(&clock),
        );
        let session = connection.bind(expected(100)).await.unwrap();
        clock.advance(100);

        assert_eq!(
            session.readiness(),
            Readiness::Unavailable(session.unavailable_reason())
        );
        assert_eq!(
            session.checked_poll(1).await,
            Err(SessionInvocationError::Session(SessionError::Fenced(
                SessionFenceReason::LeaseExpired
            )))
        );
        assert_eq!(io.invocation_calls.load(Ordering::SeqCst), 0);
        assert!(io.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn v07_task_3_6_same_connection_renewal_advances_both_expiries() {
        let current_binding = binding(SESSION_ID, BOOT_ID, FINGERPRINT);
        let clock = Arc::new(ManualClock::new());
        let (connection, io) = fake_connection(
            vec![
                Ok(bind_response(
                    matching_report(),
                    SESSION_ID,
                    FINGERPRINT,
                    1_000,
                )),
                Ok(renew_response(current_binding, 2_000)),
            ],
            Arc::clone(&clock),
        );
        let mut session = connection.bind(expected(100)).await.unwrap();
        clock.advance(10);
        session.renew().await.unwrap();

        assert_eq!(session.binding(), current_binding);
        assert_eq!(session.server_expiry_millis(), 2_000);
        assert_eq!(session.readiness(), Readiness::Available);
        assert_eq!(io.control_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn v07_task_3_6_restart_or_revoke_renewal_fences_session() {
        let current_binding = binding(SESSION_ID, BOOT_ID, FINGERPRINT);
        let valid_renew = renew_response(current_binding, 2_000);
        let clock = Arc::new(ManualClock::new());
        let (connection, io) = fake_connection(
            vec![
                Ok(bind_response(
                    matching_report(),
                    SESSION_ID,
                    FINGERPRINT,
                    1_000,
                )),
                Ok(remote_error(valid_renew, 17)),
            ],
            Arc::clone(&clock),
        );
        let mut session = connection.bind(expected(100)).await.unwrap();
        assert_eq!(session.renew().await, Err(SessionError::LeaseRejected(17)));
        assert_eq!(
            session.fence_reason(),
            Some(SessionFenceReason::RenewalRejected)
        );
        assert!(io.closed.load(Ordering::SeqCst));
        assert_eq!(
            session.checked_poll(1).await,
            Err(SessionInvocationError::Session(SessionError::Fenced(
                SessionFenceReason::RenewalRejected
            )))
        );
        assert_eq!(io.invocation_calls.load(Ordering::SeqCst), 0);

        let other_binding = binding(SESSION_ID, [0x99; 16], FINGERPRINT);
        let clock = Arc::new(ManualClock::new());
        let (connection, _) = fake_connection(
            vec![
                Ok(bind_response(
                    matching_report(),
                    SESSION_ID,
                    FINGERPRINT,
                    1_000,
                )),
                Ok(renew_response(other_binding, 2_000)),
            ],
            clock,
        );
        let mut session = connection.bind(expected(100)).await.unwrap();
        assert!(matches!(session.renew().await, Err(SessionError::Protocol)));
        assert_eq!(
            session.fence_reason(),
            Some(SessionFenceReason::ResponseMismatch)
        );
    }

    #[tokio::test]
    async fn v07_task_3_6_permission_and_nonadvancing_or_expired_renewal_fail_closed() {
        let denied = remote_error(
            bind_response(matching_report(), SESSION_ID, FINGERPRINT, 1_000),
            31,
        );
        let clock = Arc::new(ManualClock::new());
        let (connection, io) = fake_connection(vec![Ok(denied)], clock);
        let failure = connection.bind(expected(100)).await.unwrap_err();
        assert_eq!(failure.error(), &SessionError::PermissionDenied(31));
        assert!(io.closed.load(Ordering::SeqCst));
        assert_eq!(io.invocation_calls.load(Ordering::SeqCst), 0);

        let current_binding = binding(SESSION_ID, BOOT_ID, FINGERPRINT);
        let clock = Arc::new(ManualClock::new());
        let (connection, io) = fake_connection(
            vec![
                Ok(bind_response(
                    matching_report(),
                    SESSION_ID,
                    FINGERPRINT,
                    1_000,
                )),
                Ok(renew_response(current_binding, 1_000)),
            ],
            clock,
        );
        let mut session = connection.bind(expected(100)).await.unwrap();
        assert_eq!(session.renew().await, Err(SessionError::LeaseDidNotAdvance));
        assert_eq!(
            session.fence_reason(),
            Some(SessionFenceReason::ResponseMismatch)
        );
        assert_eq!(io.control_calls.load(Ordering::SeqCst), 2);
        assert!(io.closed.load(Ordering::SeqCst));

        let clock = Arc::new(ManualClock::new());
        let (connection, io) = fake_connection(
            vec![Ok(bind_response(
                matching_report(),
                SESSION_ID,
                FINGERPRINT,
                1_000,
            ))],
            Arc::clone(&clock),
        );
        let mut session = connection.bind(expected(100)).await.unwrap();
        clock.advance(100);
        assert_eq!(session.renew().await, Err(SessionError::LeaseExpired));
        assert_eq!(
            session.fence_reason(),
            Some(SessionFenceReason::LeaseExpired)
        );
        assert_eq!(io.control_calls.load(Ordering::SeqCst), 1);
        assert_eq!(io.invocation_calls.load(Ordering::SeqCst), 0);
        assert!(io.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn v07_task_3_6_disconnect_is_persistently_fenced_before_data_plane() {
        let clock = Arc::new(ManualClock::new());
        let (connection, io) = fake_connection(
            vec![Ok(bind_response(
                matching_report(),
                SESSION_ID,
                FINGERPRINT,
                1_000,
            ))],
            clock,
        );
        let session = connection.bind(expected(100)).await.unwrap();
        io.close();

        assert_eq!(
            session.readiness(),
            Readiness::Unavailable(UnavailableReason::Connectivity)
        );
        assert_eq!(
            session.fence_reason(),
            Some(SessionFenceReason::Disconnected)
        );
        assert_eq!(
            session.checked_poll(1).await,
            Err(SessionInvocationError::Session(SessionError::Fenced(
                SessionFenceReason::Disconnected
            )))
        );
        assert_eq!(io.invocation_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn v07_task_3_6_authentication_status_and_framing_are_typed_without_secrets() {
        let mut ok = BytesMut::new();
        ResponseFrame::encode_ok(&[], &mut ok).unwrap();
        assert_eq!(validate_login_response(&ok), Ok(()));

        let mut rejected = ok.to_vec();
        rejected[..4].copy_from_slice(&23_u32.to_le_bytes());
        assert_eq!(
            validate_login_response(&rejected),
            Err(SessionError::AuthenticationRejected(23))
        );

        let mut trailing = ok.to_vec();
        trailing.push(0);
        assert_eq!(
            validate_login_response(&trailing),
            Err(SessionError::InvalidAuthenticationResponse)
        );
        assert_eq!(
            SessionError::Transport(TransportError::Io {
                stage: TransportStage::TlsHandshake,
                kind: std::io::ErrorKind::InvalidData,
            })
            .readiness_reason(),
            UnavailableReason::TlsIdentityMismatch
        );
        assert!(!format!("{:?}", expected(100)).contains("credential"));
    }
}
