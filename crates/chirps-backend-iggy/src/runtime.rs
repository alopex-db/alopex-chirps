//! Backend-owned composition root used by the optional umbrella facade.

use crate::lifecycle::{
    DeliveryFence, LifecycleCoordinator, LifecycleCoordinatorError, LifecycleShutdownTrigger,
    ShutdownTransport, ShutdownTransportReport,
};
use crate::observability::{BoundedObservability, ObservabilityLimits, ObservabilityReport};
use crate::producer::{
    AppendPort, DevelopmentAppendConnection, DevelopmentBrokerConfigReadback,
    DevelopmentConnectError, DevelopmentProfileError, ProducerCoordinator,
    ProducerCoordinatorError,
};
#[cfg(feature = "durable-verification")]
use crate::producer::{AppendVerificationObserver, ObservedAppendPort};
use crate::protocol::{ChecksumMode, ResourceLocation};
use crate::routing::{PartitionRouter, ROUTING_MAP_VERSION, RoutingError};
use crate::session::{AuthenticatedConnection, BoundSession, ExpectedCapability, SessionError};
use crate::state::capacity::{
    CapacityController, CapacityError, CapacityFootprint, CapacityLimit, CapacityLimits,
    CapacityToken, Retention, StartupReserves, StateCategory,
};
use crate::state::compaction::{
    ClockObservation, CompactionBase, CompactionError, CompactionFault, CompactionIdentity,
    CompactionMutation, CompactionOutcome, CompactionStore, GenerationBarrier,
};
use crate::state::creation::{
    ActiveSubscription, CreationNamespace, CreationRequest, CreationStore, CreationStoreResult,
};
use crate::state::identity::IDENTITY_BODY_LEN;
use crate::state::journal::{JournalInitialization, JournalNamespace, JournalStore};
use crate::subscriber::{
    DeliveryClock, NextDelivery, PartitionStatus, SubscriberCoordinator, SubscriberError,
};
use crate::transport::{LoginRequestFrame, TransportCloseHandle, TransportLimits};
use alopex_chirps_core::durable::{
    CheckpointOperationPhase, CheckpointOutcome, ConfirmationBoundary, ControlPlaneHealth,
    CreationRecoveryBinding, DeliveryHandle, DurableEvent, DurableEventKind, DurableHealth,
    DurableMetricLabels, DurableSendOutcome, DurableSendResult, DurableTraceContext,
    InitialPosition, LifecyclePhase, MetricBoundary, MetricFailureStage, MetricOperation,
    MetricOutcome, PartitionFaultReason, PartitionState, PreparedDurableSend, Readiness,
    RecoveryReason, ResourceId, SendOperationPhase, ShutdownReport, SubscriptionCreationOutcome,
    SubscriptionId, UnavailableReason,
};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use bytes::BytesMut;
use fs2::FileExt;
use iggy_binary_protocol::codes::{LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE};
use iggy_binary_protocol::requests::personal_access_tokens::LoginWithPersonalAccessTokenRequest;
use iggy_binary_protocol::requests::users::LoginUserRequest;
use iggy_binary_protocol::{RequestFrame, WireEncode, WireName};
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;
use tokio::time::{Duration, Instant};

/// Fixed hard bound enforced by the local checkpoint journal parser.
pub const CHECKPOINT_JOURNAL_LIMIT_BYTES: u64 = crate::state::journal::MAX_JOURNAL_LEN;

const LOCAL_COMPACTION_DIRECTORY: &str = ".chirps-compaction";
const DEFAULT_CAPACITY_COUNT: u64 = 1_048_576;
const DEFAULT_CAPACITY_BYTES: u64 = 1 << 40;
const DEFAULT_RESERVE_COUNT: u64 = 1;
const DEFAULT_RESERVE_BYTES: u64 = 64 * 1024;

/// Provider-neutral names for independently bounded local-state categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RuntimeStateCategory {
    Payload,
    InFlight,
    CheckpointJournal,
    ProcessedIdentity,
    Queue,
    Concurrency,
}

impl RuntimeStateCategory {
    pub const ALL: [Self; 6] = [
        Self::Payload,
        Self::InFlight,
        Self::CheckpointJournal,
        Self::ProcessedIdentity,
        Self::Queue,
        Self::Concurrency,
    ];

    const fn backend(self) -> StateCategory {
        match self {
            Self::Payload => StateCategory::Payload,
            Self::InFlight => StateCategory::InFlight,
            Self::CheckpointJournal => StateCategory::CheckpointJournal,
            Self::ProcessedIdentity => StateCategory::ProcessedIdentity,
            Self::Queue => StateCategory::Queue,
            Self::Concurrency => StateCategory::Concurrency,
        }
    }
}

/// One hard count/byte bound at the runtime composition boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeCapacityLimit {
    pub count: u64,
    pub bytes: u64,
}

impl RuntimeCapacityLimit {
    pub const fn new(count: u64, bytes: u64) -> Self {
        Self { count, bytes }
    }
}

/// Complete local-state bounds and startup reserve configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeCapacityConfig {
    limits: [RuntimeCapacityLimit; 6],
    checkpoint_reserve: RuntimeCapacityLimit,
    compaction_reserve: RuntimeCapacityLimit,
}

impl RuntimeCapacityConfig {
    pub const fn uniform(
        limit: RuntimeCapacityLimit,
        checkpoint_reserve: RuntimeCapacityLimit,
        compaction_reserve: RuntimeCapacityLimit,
    ) -> Self {
        Self {
            limits: [limit; 6],
            checkpoint_reserve,
            compaction_reserve,
        }
    }

    pub fn with_limit(
        mut self,
        category: RuntimeStateCategory,
        limit: RuntimeCapacityLimit,
    ) -> Self {
        self.limits[category.backend() as usize] = limit;
        self
    }

    /// Validates all hard limits and startup reserves without opening local state.
    pub fn validate(self) -> Result<(), RuntimeBuildError> {
        self.controller()?;
        Ok(())
    }

    fn controller(self) -> Result<CapacityController, CapacityError> {
        let mut limits = CapacityLimits::uniform(CapacityLimit::new(
            self.limits[0].count,
            self.limits[0].bytes,
        )?);
        for category in RuntimeStateCategory::ALL {
            let limit = self.limits[category.backend() as usize];
            limits = limits.with_limit(
                category.backend(),
                CapacityLimit::new(limit.count, limit.bytes)?,
            );
        }
        let checkpoint =
            CapacityFootprint::new(self.checkpoint_reserve.count, self.checkpoint_reserve.bytes)?;
        let compaction =
            CapacityFootprint::new(self.compaction_reserve.count, self.compaction_reserve.bytes)?;
        CapacityController::start(limits, StartupReserves::new(checkpoint, compaction))
    }
}

impl Default for RuntimeCapacityConfig {
    fn default() -> Self {
        Self::uniform(
            RuntimeCapacityLimit::new(DEFAULT_CAPACITY_COUNT, DEFAULT_CAPACITY_BYTES),
            RuntimeCapacityLimit::new(DEFAULT_RESERVE_COUNT, DEFAULT_RESERVE_BYTES),
            RuntimeCapacityLimit::new(DEFAULT_RESERVE_COUNT, DEFAULT_RESERVE_BYTES),
        )
    }
}

/// Trust attached to one configured durable-clock observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeClockTrust {
    Trusted,
    RollbackDetected,
    Unknown,
}

/// One clock reading used only for identity-horizon decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeClockReading {
    pub unix_millis: u64,
    pub trust: RuntimeClockTrust,
}

/// Runtime-owned clock seam used by normal local-state compaction.
pub trait RuntimeClockSource: Send + Sync {
    fn read(&self) -> RuntimeClockReading;
}

/// Exact per-category capacity observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeCapacityUsage {
    pub category: RuntimeStateCategory,
    pub count: u64,
    pub bytes: u64,
}

/// Public-safe projection of recovered local compaction and capacity state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLocalStateStatus {
    pub generation: u64,
    pub applied_through: u64,
    pub suffix_sequences: Vec<u64>,
    pub identity_count: usize,
    pub checkpoint_present: bool,
    pub recovery_required: bool,
    pub admission_open: bool,
    pub poll_open: bool,
    pub capacity: Vec<RuntimeCapacityUsage>,
}

/// Terminal result of one safe local compaction request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCompactionResult {
    Committed { generation: u64, collected: usize },
    KeptOld,
    Unknown,
}

/// Provider-neutral credential material resolved only for one connection.
pub enum SessionCredentialInput {
    UsernamePassword { username: String, password: String },
    PersonalAccessToken(String),
}

/// Provider-neutral expected projection for one partition resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionProjectionInput {
    pub build_sha: [u8; 20],
    pub resource_id: [u8; 16],
    pub resource_epoch: u64,
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub retention_bytes: u64,
    pub retention_messages: u64,
    pub checksum_enabled: bool,
    pub configuration_digest: [u8; 32],
    pub security_digest: [u8; 32],
    pub capability_digest: [u8; 32],
    pub lease_millis: u32,
}

/// Provider-neutral inputs converted into concrete TLS and Iggy frames only
/// inside the backend composition root.
pub struct SessionConnectionInput {
    pub address: SocketAddr,
    pub tls_server_name: String,
    pub trusted_roots_der: Vec<Vec<u8>>,
    pub max_frame_len: usize,
    pub credential: Option<SessionCredentialInput>,
    pub profile: SessionProfileInput,
    pub projection: SessionProjectionInput,
    pub renew_interval: Duration,
}

/// Provider-neutral durability profile selected for every configured session.
pub enum SessionProfileInput {
    /// Requires the compatible server extension's OS-synced acceptance proof.
    OsSyncedAccepted,
    /// Uses official Iggy append acceptance after verifying the actual broker
    /// startup configuration supplied by deployment readback.
    BrokerAccepted { broker_startup_config: Vec<u8> },
}

#[derive(Clone, Copy)]
enum SessionProfile {
    OsSyncedAccepted,
    BrokerAccepted(DevelopmentBrokerConfigReadback),
}

/// Complete connection and capability projection for one explicit partition.
#[derive(Clone)]
pub struct SessionConnectConfig {
    address: SocketAddr,
    server_name: ServerName<'static>,
    client_config: Arc<ClientConfig>,
    limits: TransportLimits,
    login: Option<LoginRequestFrame>,
    expected: ExpectedCapability,
    renew_interval: Duration,
    profile: SessionProfile,
}

impl std::fmt::Debug for SessionConnectConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionConnectConfig")
            .field("address", &self.address)
            .field("server_name", &self.server_name)
            .field("limits", &self.limits)
            .field("expected", &self.expected)
            .field("renew_interval", &self.renew_interval)
            .finish_non_exhaustive()
    }
}

impl SessionConnectConfig {
    /// Converts provider-neutral connection inputs into backend-owned values.
    pub fn from_neutral(input: SessionConnectionInput) -> Result<Self, RuntimeBuildError> {
        let mut roots = RootCertStore::empty();
        if input.trusted_roots_der.is_empty() {
            return Err(RuntimeBuildError::InvalidTlsIdentity);
        }
        for certificate in input.trusted_roots_der {
            roots
                .add(CertificateDer::from(certificate))
                .map_err(|_| RuntimeBuildError::InvalidTlsIdentity)?;
        }
        let client_config = Arc::new(
            ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| RuntimeBuildError::InvalidTlsIdentity)?
            .with_root_certificates(roots)
            .with_no_client_auth(),
        );
        let server_name = ServerName::try_from(input.tls_server_name)
            .map_err(|_| RuntimeBuildError::InvalidTlsIdentity)?;
        let limits = TransportLimits::new(input.max_frame_len)
            .map_err(|_| RuntimeBuildError::InvalidTransportLimits)?;
        let login = input.credential.map(encode_login).transpose()?;
        let profile = match input.profile {
            SessionProfileInput::OsSyncedAccepted => SessionProfile::OsSyncedAccepted,
            SessionProfileInput::BrokerAccepted {
                broker_startup_config,
            } => SessionProfile::BrokerAccepted(
                DevelopmentBrokerConfigReadback::verify_actual_startup_config(
                    &broker_startup_config,
                )?,
            ),
        };
        let projection = input.projection;
        let location = ResourceLocation::new(
            ResourceId::from_bytes(projection.resource_id),
            projection.resource_epoch,
            projection.stream_id,
            projection.topic_id,
            projection.partition_id,
        )
        .map_err(|_| RuntimeBuildError::InvalidResourceProjection)?;
        let expected = ExpectedCapability::new(
            projection.build_sha,
            location,
            projection.retention_bytes,
            projection.retention_messages,
            if projection.checksum_enabled {
                ChecksumMode::Enabled
            } else {
                ChecksumMode::Disabled
            },
            projection.configuration_digest,
            projection.security_digest,
            projection.capability_digest,
            projection.lease_millis,
        )?;
        Self::new(
            input.address,
            server_name,
            client_config,
            limits,
            login,
            expected,
            input.renew_interval,
            profile,
        )
    }

    /// Installs validated credential material after every non-secret
    /// connection and capability input has passed validation.
    pub fn bind_credential(
        &mut self,
        credential: SessionCredentialInput,
    ) -> Result<(), RuntimeBuildError> {
        if self.login.is_some() {
            return Err(RuntimeBuildError::InvalidCredential);
        }
        self.login = Some(encode_login(credential)?);
        Ok(())
    }

    /// Binds one production connection input to one exact capability projection.
    #[allow(clippy::too_many_arguments)]
    fn new(
        address: SocketAddr,
        server_name: ServerName<'static>,
        client_config: Arc<ClientConfig>,
        limits: TransportLimits,
        login: Option<LoginRequestFrame>,
        expected: ExpectedCapability,
        renew_interval: Duration,
        profile: SessionProfile,
    ) -> Result<Self, RuntimeBuildError> {
        if renew_interval.is_zero()
            || renew_interval >= Duration::from_millis(u64::from(expected.lease_millis()))
        {
            return Err(RuntimeBuildError::InvalidRenewInterval);
        }
        Ok(Self {
            address,
            server_name,
            client_config,
            limits,
            login,
            expected,
            renew_interval,
            profile,
        })
    }

    fn partition(&self) -> u32 {
        self.expected.location().partition_id()
    }

    async fn connect(&self, deadline: Instant) -> Result<RuntimeSession, RuntimeBuildError> {
        let login = self
            .login
            .clone()
            .ok_or(RuntimeBuildError::InvalidCredential)?;
        match self.profile {
            SessionProfile::OsSyncedAccepted => {
                let authenticated = AuthenticatedConnection::connect_tls_and_authenticate(
                    self.address,
                    self.server_name.clone(),
                    Arc::clone(&self.client_config),
                    self.limits,
                    login,
                    deadline,
                )
                .await?;
                match authenticated.bind(self.expected).await {
                    Ok(session) => Ok(RuntimeSession::Strong(Arc::new(session))),
                    Err(failure) => {
                        let error = failure.error().clone();
                        let _ = failure.shutdown(deadline).await;
                        Err(error.into())
                    }
                }
            }
            SessionProfile::BrokerAccepted(broker_config) => {
                DevelopmentAppendConnection::connect_tls_and_authenticate(
                    self.address,
                    self.server_name.clone(),
                    Arc::clone(&self.client_config),
                    self.limits,
                    login,
                    self.expected.location(),
                    broker_config,
                    deadline,
                )
                .await
                .map(|connection| RuntimeSession::Development(Arc::new(connection)))
                .map_err(Into::into)
            }
        }
    }
}

/// The one owner of routing, sessions, local subscription state, and lifecycle.
pub struct DurableRuntime {
    router: PartitionRouter,
    sessions: BTreeMap<u32, RuntimeSession>,
    append_ports: BTreeMap<u32, Arc<dyn AppendPort>>,
    checkpoint_root: PathBuf,
    persistence_lifecycle_generation: u64,
    subscriptions: BTreeMap<SubscriptionId, RuntimeSubscription>,
    local_state: LocalStateRuntime,
    lifecycle: LifecycleCoordinator,
}

struct LocalStateRuntime {
    _lock: File,
    store: CompactionStore,
    barrier: GenerationBarrier,
    capacity: SharedCapacity,
    clock: Arc<dyn RuntimeClockSource>,
    next_sequence: u64,
    checkpoint_tokens: Vec<CapacityToken>,
    faulted: bool,
}

#[derive(Clone)]
struct SharedCapacity(Arc<Mutex<CapacityController>>);

impl SharedCapacity {
    fn new(capacity: CapacityController) -> Self {
        Self(Arc::new(Mutex::new(capacity)))
    }

    fn lock(&self) -> Result<MutexGuard<'_, CapacityController>, CapacityError> {
        self.0.lock().map_err(|_| CapacityError::StatePoisoned)
    }
}

struct SendCapacityRelease {
    capacity: SharedCapacity,
    tokens: Option<[CapacityToken; 2]>,
}

impl SendCapacityRelease {
    fn new(capacity: SharedCapacity, payload: CapacityToken, concurrency: CapacityToken) -> Self {
        Self {
            capacity,
            tokens: Some([payload, concurrency]),
        }
    }

    fn complete(self) {}
}

impl Drop for SendCapacityRelease {
    fn drop(&mut self) {
        if let Some(tokens) = self.tokens.take() {
            if let Ok(mut capacity) = self.capacity.lock() {
                for token in tokens {
                    let _ = capacity.release_terminal(token);
                }
            }
        }
    }
}

impl LocalStateRuntime {
    fn open(
        checkpoint_root: &Path,
        immutable_runtime_binding: Vec<u8>,
        capacity_config: RuntimeCapacityConfig,
        clock: Arc<dyn RuntimeClockSource>,
    ) -> Result<Self, RuntimeBuildError> {
        fs::create_dir_all(checkpoint_root).map_err(|_| RuntimeBuildError::Compaction)?;
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(checkpoint_root.join(".chirps-compaction.lock"))
            .map_err(|_| RuntimeBuildError::Compaction)?;
        lock.try_lock_exclusive()
            .map_err(|_| RuntimeBuildError::LocalStateOwned)?;
        let mut capacity = capacity_config.controller()?;
        let root = checkpoint_root.join(LOCAL_COMPACTION_DIRECTORY);
        let store = if root.join("compaction.root").exists() {
            CompactionStore::recover(&root)?
        } else {
            CompactionStore::initialize(
                &root,
                CompactionBase::new(
                    1,
                    0,
                    immutable_runtime_binding,
                    None,
                    BTreeMap::new(),
                    &BTreeMap::new(),
                )?,
            )?
        };
        let (_, identities) = store.active().materialized_state()?;
        for (message_id, encoded) in &identities {
            capacity.try_admit_identity(
                *message_id,
                CapacityFootprint::new(1, encoded.len() as u64)?,
            )?;
        }
        let barrier = GenerationBarrier::from_recovered(store.active())?;
        let next_sequence = store
            .active()
            .applied_through()
            .checked_add(store.active().suffix().len() as u64)
            .and_then(|sequence| sequence.checked_add(1))
            .ok_or(CompactionError::SequenceConflict)?;
        Ok(Self {
            _lock: lock,
            store,
            barrier,
            capacity: SharedCapacity::new(capacity),
            clock,
            next_sequence,
            checkpoint_tokens: Vec::new(),
            faulted: false,
        })
    }

    fn admit(
        &mut self,
        category: RuntimeStateCategory,
        count: u64,
        bytes: u64,
        retention: Retention,
    ) -> Result<CapacityToken, CapacityError> {
        self.capacity.lock()?.try_admit(
            category.backend(),
            CapacityFootprint::new(count, bytes)?,
            retention,
        )
    }

    fn append_mutation(&mut self, mutation: CompactionMutation) -> Result<(), CompactionError> {
        let sequence = self.next_sequence;
        self.barrier.register_mutation(sequence, mutation)?;
        self.store.commit_registered(&mut self.barrier, sequence)?;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or(CompactionError::SequenceConflict)?;
        Ok(())
    }

    fn status(&self) -> Result<RuntimeLocalStateStatus, RuntimeLocalStateError> {
        let (checkpoint, identities) = self.store.active().materialized_state()?;
        let capacity_controller = self.capacity.lock()?;
        let capacity = RuntimeStateCategory::ALL
            .into_iter()
            .map(|category| {
                let usage = capacity_controller.usage(category.backend());
                RuntimeCapacityUsage {
                    category,
                    count: usage.count(),
                    bytes: usage.bytes(),
                }
            })
            .collect();
        let state = capacity_controller.status();
        Ok(RuntimeLocalStateStatus {
            generation: self.store.active().generation(),
            applied_through: self.store.active().applied_through(),
            suffix_sequences: self.store.active().suffix().keys().copied().collect(),
            identity_count: identities.len(),
            checkpoint_present: checkpoint.is_some(),
            recovery_required: self.faulted || self.store.recovery_required(),
            admission_open: state.admission_open(),
            poll_open: state.poll_open(),
            capacity,
        })
    }

    fn compact(&mut self, broker_oldest: u64) -> Result<RuntimeCompactionResult, CompactionError> {
        if self.faulted {
            return Err(CompactionError::RecoveryRequired);
        }
        let mut capacity = self.capacity.lock()?;
        self.barrier.acquire(&mut capacity)?;
        let reading = self.clock.read();
        let clock = match reading.trust {
            RuntimeClockTrust::Trusted => {
                capacity.restore_trusted_clock();
                ClockObservation::Trusted
            }
            RuntimeClockTrust::RollbackDetected => ClockObservation::RollbackDetected,
            RuntimeClockTrust::Unknown => ClockObservation::Unknown,
        };
        let (_, active_identities) = self.store.active().materialized_state()?;
        let identities = active_identities
            .iter()
            .map(|(message_id, encoded)| CompactionIdentity::new(*message_id, encoded.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let plan = self.store.plan_compaction(
            &self.barrier,
            identities,
            broker_oldest,
            reading.unix_millis,
            clock,
            &mut capacity,
        )?;
        match self.store.compact(
            plan,
            &mut self.barrier,
            &mut capacity,
            CompactionFault::None,
        )? {
            CompactionOutcome::Committed(proof) => {
                let mut collected = 0;
                for message_id in active_identities.keys().copied() {
                    if proof.contains(message_id) {
                        let token = capacity.identity_gc_token(message_id)?;
                        capacity.release_identity_after_gc(token, message_id, &proof)?;
                        collected += 1;
                    }
                }
                for token in self.checkpoint_tokens.drain(..) {
                    capacity.release_terminal(token)?;
                }
                Ok(RuntimeCompactionResult::Committed {
                    generation: self.store.active().generation(),
                    collected,
                })
            }
            CompactionOutcome::KeptOld => Ok(RuntimeCompactionResult::KeptOld),
            CompactionOutcome::Unknown => Ok(RuntimeCompactionResult::Unknown),
        }
    }
}

#[derive(Clone)]
enum RuntimeSession {
    Strong(Arc<BoundSession>),
    Development(Arc<DevelopmentAppendConnection>),
}

impl RuntimeSession {
    fn append_port(&self) -> Arc<dyn AppendPort> {
        match self {
            Self::Strong(session) => Arc::clone(session) as Arc<dyn AppendPort>,
            Self::Development(session) => Arc::clone(session) as Arc<dyn AppendPort>,
        }
    }

    fn request_close(&self) {
        match self {
            Self::Strong(session) => session.close_handle().close(),
            Self::Development(session) => session.close_handle().close(),
        }
    }

    fn close_handle(&self) -> TransportCloseHandle {
        match self {
            Self::Strong(session) => session.close_handle(),
            Self::Development(session) => session.close_handle(),
        }
    }

    fn readiness(&self) -> Readiness {
        match self {
            Self::Strong(session) => session.readiness(),
            Self::Development(session) if session.close_handle().is_closed() => {
                Readiness::Unavailable(alopex_chirps_core::durable::UnavailableReason::Connectivity)
            }
            Self::Development(_) => Readiness::Available,
        }
    }
}

/// Cloneable close signal used to interrupt blocked Durable I/O before the
/// owning runtime performs its bounded join.
#[derive(Debug, Clone)]
pub struct RuntimeShutdownTrigger {
    handles: Vec<TransportCloseHandle>,
    lifecycle: LifecycleShutdownTrigger,
}

impl RuntimeShutdownTrigger {
    /// Starts lifecycle drain once, then closes every transport to wake blocked I/O.
    pub fn request_shutdown(&self) -> Result<(), RuntimeShutdownError> {
        self.lifecycle.request_shutdown()?;
        for handle in &self.handles {
            handle.close();
        }
        Ok(())
    }

    /// Returns whether every transport received the close request.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.lifecycle.is_shutdown_requested()
            && self.handles.iter().all(TransportCloseHandle::is_closed)
    }
}

struct RuntimeSubscription {
    _active: ActiveSubscription,
    coordinator: SubscriberCoordinator<BoundSession>,
    fence: Arc<AtomicBool>,
    _queue_token: CapacityToken,
    in_flight_token: Option<CapacityToken>,
    rebind_pending: bool,
}

impl RuntimeSubscription {
    fn trace(&self) -> DurableTraceContext {
        let binding = self._active.binding();
        DurableTraceContext::partition(
            binding.partition(),
            binding.owner_epoch(),
            self._active.creation().captured_resource_epoch(),
        )
    }
}

struct SubscriptionFence(Arc<AtomicBool>);

impl DeliveryFence for SubscriptionFence {
    fn fence(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

struct SessionSetTransport {
    sessions: Vec<RuntimeSession>,
}

#[async_trait]
impl ShutdownTransport for SessionSetTransport {
    fn request_close(&self) {
        for session in &self.sessions {
            session.request_close();
        }
    }

    async fn shutdown(self: Box<Self>, deadline: Instant) -> ShutdownTransportReport {
        let mut closed = true;
        let mut joined = true;
        for session in self.sessions {
            let report = shutdown_session(session, deadline).await;
            closed &= report.socket_close_requested();
            joined &= report.all_workers_joined();
        }
        ShutdownTransportReport::new(closed, joined)
    }
}

async fn shutdown_session(session: RuntimeSession, deadline: Instant) -> ShutdownTransportReport {
    match session {
        RuntimeSession::Strong(session) => {
            while Arc::strong_count(&session) != 1 && Instant::now() < deadline {
                tokio::task::yield_now().await;
            }
            match Arc::try_unwrap(session) {
                Ok(session) => {
                    let report = session.shutdown(deadline).await;
                    ShutdownTransportReport::new(
                        report.socket_close_requested(),
                        report.all_workers_joined(),
                    )
                }
                Err(session) => {
                    session.close_handle().close();
                    ShutdownTransportReport::new(session.close_handle().is_closed(), false)
                }
            }
        }
        RuntimeSession::Development(session) => {
            while Arc::strong_count(&session) != 1 && Instant::now() < deadline {
                tokio::task::yield_now().await;
            }
            match Arc::try_unwrap(session) {
                Ok(session) => {
                    let report = session.shutdown(deadline).await;
                    ShutdownTransportReport::new(
                        report.socket_close_requested(),
                        report.all_workers_joined(),
                    )
                }
                Err(session) => {
                    session.close_handle().close();
                    ShutdownTransportReport::new(session.close_handle().is_closed(), false)
                }
            }
        }
    }
}

impl DurableRuntime {
    /// Returns a cloneable close trigger that does not own worker joins.
    #[must_use]
    pub fn shutdown_trigger(&self) -> RuntimeShutdownTrigger {
        RuntimeShutdownTrigger {
            handles: self
                .sessions
                .values()
                .map(RuntimeSession::close_handle)
                .collect(),
            lifecycle: self.lifecycle.shutdown_trigger(),
        }
    }

    /// Prepares without connecting while keeping router construction private to
    /// the backend composition boundary.
    pub fn prepare_configured(
        source: NodeId,
        inbox_generation: u64,
        partition_count: u32,
        target: NodeId,
        ordering_key: Vec<u8>,
        payload: &[u8],
    ) -> Result<PreparedDurableSend, RuntimePrepareError> {
        let router = PartitionRouter::new(source, inbox_generation, partition_count)?;
        Ok(crate::producer::prepare(
            &router,
            target,
            ordering_key,
            payload,
        )?)
    }

    /// Connects, authenticates, capability-binds, and schedules renewal for one
    /// exact session per configured partition.
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        source: NodeId,
        inbox_generation: u64,
        partition_count: u32,
        checkpoint_root: PathBuf,
        persistence_lifecycle_generation: u64,
        capacity_config: RuntimeCapacityConfig,
        clock: Arc<dyn RuntimeClockSource>,
        connections: Vec<SessionConnectConfig>,
        deadline: Instant,
    ) -> Result<Self, RuntimeBuildError> {
        if persistence_lifecycle_generation == 0 {
            return Err(RuntimeBuildError::InvalidLifecycleGeneration);
        }
        let router = PartitionRouter::new(source, inbox_generation, partition_count)?;
        validate_connection_partitions(partition_count, &connections)?;
        let mut binding = Vec::with_capacity(28);
        binding.extend_from_slice(source.as_bytes());
        binding.extend_from_slice(&inbox_generation.to_be_bytes());
        binding.extend_from_slice(&partition_count.to_be_bytes());
        let local_state =
            LocalStateRuntime::open(&checkpoint_root, binding, capacity_config, clock)?;

        let mut sessions = BTreeMap::new();
        for connection in &connections {
            let session = match connection.connect(deadline).await {
                Ok(session) => session,
                Err(error) => {
                    shutdown_partial_sessions(sessions.into_values(), deadline).await;
                    return Err(error);
                }
            };
            sessions.insert(connection.partition(), session);
        }
        let append_ports = sessions
            .iter()
            .map(|(&partition, session)| (partition, session.append_port()))
            .collect();

        let transport = SessionSetTransport {
            sessions: sessions.values().cloned().collect(),
        };
        let limits = ObservabilityLimits::new(64, 64)
            .map_err(|_| RuntimeBuildError::LifecycleConfiguration)?;
        let mut lifecycle =
            LifecycleCoordinator::new(Box::new(transport), BoundedObservability::new(limits));
        lifecycle.mark_ready(Readiness::Available)?;
        for partition in sessions.keys().copied() {
            lifecycle.set_partition_state(partition, PartitionState::Active);
        }
        for connection in &connections {
            let session = sessions
                .get(&connection.partition())
                .ok_or(RuntimeBuildError::MissingPartition(connection.partition()))?
                .clone();
            let RuntimeSession::Strong(session) = session else {
                continue;
            };
            let interval = connection.renew_interval;
            let ticket = lifecycle.admission_ticket()?;
            lifecycle.register_worker(ticket, move |mut cancelled| async move {
                loop {
                    tokio::select! {
                        changed = cancelled.changed() => {
                            if changed.is_err() || *cancelled.borrow() {
                                return;
                            }
                        }
                        _ = tokio::time::sleep(interval) => {
                            if session.renew().await.is_err() {
                                return;
                            }
                        }
                    }
                }
            })?;
        }

        Ok(Self {
            router,
            sessions,
            append_ports,
            checkpoint_root,
            persistence_lifecycle_generation,
            subscriptions: BTreeMap::new(),
            local_state,
            lifecycle,
        })
    }

    /// Installs one verification observer around every real append port.
    /// Existing production construction never calls this method.
    #[cfg(feature = "durable-verification")]
    pub fn install_append_observer(&mut self, observer: Arc<dyn AppendVerificationObserver>) {
        self.append_ports = std::mem::take(&mut self.append_ports)
            .into_iter()
            .map(|(partition, port)| {
                let observed: Arc<dyn AppendPort> =
                    Arc::new(ObservedAppendPort::new(port, Arc::clone(&observer)));
                (partition, observed)
            })
            .collect();
    }

    /// Prepares one immutable canonical request with this runtime's routing.
    pub fn prepare(
        &self,
        target: NodeId,
        ordering_key: Vec<u8>,
        payload: &[u8],
    ) -> Result<PreparedDurableSend, crate::producer::ProducerPrepareError> {
        crate::producer::prepare(&self.router, target, ordering_key, payload)
    }

    /// Executes one lifecycle-registered attempt and returns only its terminal
    /// provider-neutral result.
    pub async fn send(
        &mut self,
        prepared: &PreparedDurableSend,
        boundary: ConfirmationBoundary,
    ) -> Result<DurableSendResult, RuntimeSendError> {
        let result = async {
            self.validate_prepared(prepared)?;
            let port = self
                .append_ports
                .get(&prepared.partition())
                .ok_or(RuntimeSendError::PartitionUnavailable(prepared.partition()))?
                .clone();
            let payload = match self.local_state.admit(
                RuntimeStateCategory::Payload,
                1,
                prepared.canonical_bytes().len() as u64,
                Retention::Terminal,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.record_capacity_rejection();
                    return Err(error.into());
                }
            };
            let concurrency = {
                let mut capacity = self.local_state.capacity.lock()?;
                capacity.try_admit_continuation(
                    StateCategory::Concurrency,
                    CapacityFootprint::new(1, 1)?,
                    Retention::Terminal,
                )
            };
            let concurrency = match concurrency {
                Ok(token) => token,
                Err(error) => {
                    self.record_capacity_rejection();
                    self.local_state
                        .capacity
                        .lock()?
                        .release_terminal(payload)?;
                    return Err(error.into());
                }
            };
            let capacity_release =
                SendCapacityRelease::new(self.local_state.capacity.clone(), payload, concurrency);
            let result = execute_send(
                &mut self.lifecycle,
                port,
                prepared,
                boundary,
                capacity_release,
            )
            .await;
            result
        }
        .await;
        let labels = send_metric_labels(boundary, &result);
        self.record_metric(labels);
        result
    }

    /// Atomically creates a new subscription and initializes its journal.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        target: NodeId,
        partition: u32,
        namespace_digest: [u8; 32],
        initial_position: InitialPosition,
    ) -> Result<SubscriptionCreationOutcome, RuntimeSubscriptionError> {
        self.open_subscription(
            subscription_id,
            target,
            partition,
            namespace_digest,
            SubscriptionOpenMode::Create(initial_position),
        )
        .await
    }

    /// Reopens an existing committed subscription and journal.
    pub async fn reopen_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        target: NodeId,
        partition: u32,
        namespace_digest: [u8; 32],
    ) -> Result<SubscriptionCreationOutcome, RuntimeSubscriptionError> {
        self.open_subscription(
            subscription_id,
            target,
            partition,
            namespace_digest,
            SubscriptionOpenMode::Reopen,
        )
        .await
    }

    /// Recovers an indeterminate subscription creation with its exact binding.
    pub async fn recover_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        target: NodeId,
        partition: u32,
        namespace_digest: [u8; 32],
        recovery: CreationRecoveryBinding,
    ) -> Result<SubscriptionCreationOutcome, RuntimeSubscriptionError> {
        self.open_subscription(
            subscription_id,
            target,
            partition,
            namespace_digest,
            SubscriptionOpenMode::Recover(recovery),
        )
        .await
    }

    /// Polls one open subscription without releasing more than one delivery.
    pub async fn next_delivery(
        &mut self,
        subscription_id: SubscriptionId,
        retry_not_before_unix_ms: u64,
        clock: DeliveryClock,
    ) -> Result<NextDelivery, RuntimeSubscriptionError> {
        {
            let subscription = self.subscription_mut(subscription_id)?;
            ensure_not_fenced(subscription)?;
        }
        if !self.local_state.capacity.lock()?.status().poll_open() {
            self.record_capacity_rejection();
            return Err(RuntimeSubscriptionError::Capacity);
        }
        let identity_preview = match self.local_state.admit(
            RuntimeStateCategory::ProcessedIdentity,
            1,
            IDENTITY_BODY_LEN as u64,
            Retention::Terminal,
        ) {
            Ok(token) => token,
            Err(error) => {
                self.record_capacity_rejection();
                return Err(error.into());
            }
        };
        let in_flight = {
            let mut capacity = self.local_state.capacity.lock()?;
            capacity.try_admit_continuation(
                StateCategory::InFlight,
                CapacityFootprint::new(1, 1)?,
                Retention::Terminal,
            )
        };
        let in_flight = match in_flight {
            Ok(token) => token,
            Err(error) => {
                self.record_capacity_rejection();
                self.local_state
                    .capacity
                    .lock()?
                    .release_terminal(identity_preview)?;
                return Err(error.into());
            }
        };
        let result = {
            let subscription = self.subscription_mut(subscription_id)?;
            subscription
                .coordinator
                .next_delivery(retry_not_before_unix_ms, clock)
                .await
        };
        let delivery = match result {
            Ok(NextDelivery::Delivery(delivery)) => delivery,
            Ok(other) => {
                self.local_state
                    .capacity
                    .lock()?
                    .release_terminal(identity_preview)?;
                self.local_state
                    .capacity
                    .lock()?
                    .release_terminal(in_flight)?;
                let trace = self.subscription_mut(subscription_id)?.trace();
                let tail = matches!(&other, NextDelivery::Tail);
                let rebind = {
                    let subscription = self.subscription_mut(subscription_id)?;
                    let rebind = tail && subscription.rebind_pending;
                    if tail {
                        subscription.rebind_pending = false;
                    }
                    rebind
                };
                self.record_metric(DurableMetricLabels::new(
                    MetricOperation::Poll,
                    MetricBoundary::BeforeMutation,
                    MetricFailureStage::None,
                    if tail {
                        MetricOutcome::Success
                    } else {
                        MetricOutcome::NotCommitted
                    },
                ));
                if rebind {
                    self.record_event(DurableEvent::new(DurableEventKind::SessionRebind, trace));
                }
                return Ok(other);
            }
            Err(error) => {
                self.local_state
                    .capacity
                    .lock()?
                    .release_terminal(identity_preview)?;
                self.local_state
                    .capacity
                    .lock()?
                    .release_terminal(in_flight)?;
                let (trace, status) = {
                    let subscription = self.subscription_mut(subscription_id)?;
                    (subscription.trace(), subscription.coordinator.status())
                };
                let outcome = match status {
                    PartitionStatus::RetentionGap => MetricOutcome::Gap,
                    PartitionStatus::RecoveryRequired => MetricOutcome::RecoveryRequired,
                    _ => MetricOutcome::Unavailable,
                };
                self.record_metric(DurableMetricLabels::new(
                    MetricOperation::Poll,
                    MetricBoundary::BeforeMutation,
                    MetricFailureStage::PollDecode,
                    outcome,
                ));
                if status == PartitionStatus::RetentionGap {
                    self.record_event(DurableEvent::new(DurableEventKind::Gap, trace));
                } else if matches!(
                    status,
                    PartitionStatus::RecoveryRequired | PartitionStatus::Faulted
                ) {
                    self.record_event(DurableEvent::new(DurableEventKind::Fault, trace));
                }
                return Err(error.into());
            }
        };
        let redelivery = delivery.handle().delivery_attempt() > 1;
        let message_id = *delivery.handle().message_id().as_bytes();
        let encoded = self
            .subscription_mut(subscription_id)?
            .coordinator
            .identity_bytes(message_id)
            .ok_or(RuntimeSubscriptionError::InvalidLocalState)?;
        if self
            .local_state
            .capacity
            .lock()?
            .contains_identity(message_id)
        {
            self.local_state
                .capacity
                .lock()?
                .release_terminal(identity_preview)?;
        } else {
            if encoded.len() != IDENTITY_BODY_LEN {
                return Err(RuntimeSubscriptionError::InvalidLocalState);
            }
            self.local_state
                .capacity
                .lock()?
                .bind_identity(identity_preview, message_id)?;
        }
        self.local_state
            .append_mutation(CompactionMutation::UpsertIdentity {
                message_id,
                encoded,
            })?;
        self.subscription_mut(subscription_id)?.in_flight_token = Some(in_flight);
        let trace = {
            let subscription = self.subscription_mut(subscription_id)?;
            if redelivery {
                subscription.rebind_pending = false;
            }
            subscription.trace()
        };
        self.record_metric(DurableMetricLabels::new(
            MetricOperation::Poll,
            MetricBoundary::BeforeMutation,
            MetricFailureStage::None,
            if redelivery {
                MetricOutcome::Duplicate
            } else {
                MetricOutcome::Success
            },
        ));
        if redelivery {
            self.record_event(DurableEvent::new(DurableEventKind::Redelivery, trace));
        }
        Ok(NextDelivery::Delivery(delivery))
    }

    /// Commits the exact active core delivery handle.
    pub fn ack(
        &mut self,
        subscription_id: SubscriptionId,
        handle: &mut DeliveryHandle,
    ) -> Result<CheckpointOutcome, RuntimeSubscriptionError> {
        let trace = self.subscription_mut(subscription_id)?.trace();
        let journal_token = match self.local_state.admit(
            RuntimeStateCategory::CheckpointJournal,
            1,
            256,
            Retention::Terminal,
        ) {
            Ok(token) => token,
            Err(error) => {
                self.record_capacity_rejection();
                return Err(error.into());
            }
        };
        let message_id = *handle.message_id().as_bytes();
        let outcome = match self.ack_coordinated(subscription_id, handle) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.local_state
                    .capacity
                    .lock()?
                    .release_terminal(journal_token)?;
                return Err(error);
            }
        };
        if outcome == CheckpointOutcome::CheckpointCommitted {
            let (checkpoint, identity) = {
                let subscription = self.subscription_mut(subscription_id)?;
                (
                    subscription.coordinator.checkpoint_bytes(),
                    subscription.coordinator.identity_bytes(message_id),
                )
            };
            let mut compaction_result = Ok(());
            if let Some(checkpoint) = checkpoint {
                compaction_result = self
                    .local_state
                    .append_mutation(CompactionMutation::InstallCheckpoint(checkpoint));
            }
            if compaction_result.is_ok()
                && let Some(encoded) = identity
            {
                compaction_result =
                    self.local_state
                        .append_mutation(CompactionMutation::UpsertIdentity {
                            message_id,
                            encoded,
                        });
            }
            if compaction_result.is_err() {
                // The canonical checkpoint is already durable. Preserve that outcome and
                // require recovery instead of reporting a false checkpoint failure.
                self.local_state.faulted = true;
            }
            self.local_state.checkpoint_tokens.push(journal_token);
            if let Some(token) = self
                .subscriptions
                .get_mut(&subscription_id)
                .and_then(|subscription| subscription.in_flight_token.take())
            {
                if self
                    .local_state
                    .capacity
                    .lock()
                    .and_then(|mut capacity| capacity.release_terminal(token))
                    .is_err()
                {
                    self.local_state.faulted = true;
                }
            }
        } else {
            self.local_state
                .capacity
                .lock()?
                .release_terminal(journal_token)?;
        }
        self.record_metric(DurableMetricLabels::new(
            MetricOperation::Checkpoint,
            MetricBoundary::CheckpointInstall,
            MetricFailureStage::None,
            match outcome {
                CheckpointOutcome::CheckpointCommitted => MetricOutcome::Success,
                CheckpointOutcome::CheckpointNotCommitted => MetricOutcome::NotCommitted,
                CheckpointOutcome::CheckpointUnknown => MetricOutcome::Unknown,
            },
        ));
        self.record_event(DurableEvent::new(DurableEventKind::Checkpoint, trace));
        Ok(outcome)
    }

    fn ack_coordinated(
        &mut self,
        subscription_id: SubscriptionId,
        handle: &mut DeliveryHandle,
    ) -> Result<CheckpointOutcome, RuntimeSubscriptionError> {
        // Reject missing or already-fenced owners before lifecycle registration.
        // The second check below remains necessary because shutdown can fence
        // the subscription after this preflight and freeze the registered phase.
        ensure_not_fenced(self.subscription_mut(subscription_id)?)?;
        let ticket = self.lifecycle.admission_ticket()?;
        let operation = self
            .lifecycle
            .register_checkpoint(ticket, CheckpointOperationPhase::Prewrite)?;
        let subscription = self.subscription_mut(subscription_id)?;
        ensure_not_fenced(subscription)?;
        let phase_operation = operation.clone();
        match subscription
            .coordinator
            .ack_with_phase(handle, move |phase| {
                phase_operation.transition(phase).is_ok()
            }) {
            Ok(outcome) => {
                match operation.transition(CheckpointOperationPhase::Terminal(outcome)) {
                    Ok(()) => Ok(outcome),
                    Err(LifecycleCoordinatorError::OperationFrozen) => {
                        let (phase, frozen) = operation.snapshot()?;
                        debug_assert!(frozen);
                        checkpoint_outcome_at_freeze(phase)
                    }
                    Err(error) => Err(error.into()),
                }
            }
            Err(error) => {
                let (phase, frozen) = operation.snapshot()?;
                if !frozen && phase == CheckpointOperationPhase::Prewrite {
                    operation.transition(CheckpointOperationPhase::KnownOld)?;
                }
                let (phase, frozen) = operation.snapshot()?;
                if !frozen {
                    let outcome = checkpoint_outcome_at_freeze(phase)?;
                    operation.transition(CheckpointOperationPhase::Terminal(outcome))?;
                }
                Err(error.into())
            }
        }
    }

    /// Releases the exact active core delivery handle without checkpointing.
    pub fn release(
        &mut self,
        subscription_id: SubscriptionId,
        handle: &mut DeliveryHandle,
    ) -> Result<(), RuntimeSubscriptionError> {
        {
            let subscription = self.subscription_mut(subscription_id)?;
            ensure_not_fenced(subscription)?;
            subscription.coordinator.release(handle)?;
        }
        if let Some(token) = self
            .subscription_mut(subscription_id)?
            .in_flight_token
            .take()
        {
            self.local_state.capacity.lock()?.release_terminal(token)?;
        }
        Ok(())
    }

    /// Returns the recovered compaction generation and exact capacity usage.
    pub fn local_state_status(&self) -> Result<RuntimeLocalStateStatus, RuntimeLocalStateError> {
        self.local_state.status().map_err(Into::into)
    }

    /// Runs one fault-free generation cutover using the configured durable clock.
    pub fn compact_local_state(
        &mut self,
        broker_oldest: u64,
    ) -> Result<RuntimeCompactionResult, RuntimeLocalStateError> {
        self.local_state.compact(broker_oldest).map_err(Into::into)
    }

    /// Returns health from the lifecycle coordinator after refreshing its
    /// readiness and partition projections from owned components.
    #[must_use]
    pub fn health(&mut self) -> DurableHealth {
        let mut readiness = resolve_runtime_readiness(
            self.lifecycle.phase(),
            self.sessions.values().map(RuntimeSession::readiness),
        );
        let capacity_open = self
            .local_state
            .capacity
            .lock()
            .is_ok_and(|capacity| capacity.status().admission_open());
        if readiness == Readiness::Available && !capacity_open {
            readiness = Readiness::Unavailable(UnavailableReason::Capacity);
        }
        self.lifecycle.set_readiness(readiness);
        for partition in self.sessions.keys().copied().collect::<Vec<_>>() {
            self.lifecycle
                .set_partition_state(partition, self.partition_state(partition));
        }
        self.lifecycle
            .health_snapshot(ControlPlaneHealth::Available)
            .durable()
            .clone()
    }

    /// Returns one owned, finite observability report without changing runtime state.
    #[must_use]
    pub fn observability(&self) -> ObservabilityReport {
        self.lifecycle.observability().report()
    }

    /// Fences subscription state, closes every socket, cancels renewal, and
    /// returns the lifecycle coordinator's exact terminal report.
    pub async fn shutdown(
        &mut self,
        deadline: Instant,
    ) -> Result<ShutdownReport, RuntimeShutdownError> {
        self.lifecycle.request_shutdown()?;
        let faulted = self
            .subscriptions
            .values()
            .filter(|subscription| subscription.coordinator.has_in_flight())
            .map(RuntimeSubscription::trace)
            .collect::<Vec<_>>();
        for trace in faulted {
            self.record_event(DurableEvent::new(DurableEventKind::Fault, trace));
        }
        for session in self.sessions.values() {
            session.request_close();
        }
        for subscription in self.subscriptions.values_mut() {
            subscription.coordinator.close()?;
        }
        self.subscriptions.clear();
        self.append_ports.clear();
        self.sessions.clear();
        self.lifecycle.shutdown(deadline).await.map_err(Into::into)
    }

    async fn open_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        target: NodeId,
        partition: u32,
        namespace_digest: [u8; 32],
        mode: SubscriptionOpenMode,
    ) -> Result<SubscriptionCreationOutcome, RuntimeSubscriptionError> {
        if self.subscriptions.contains_key(&subscription_id) {
            return Err(RuntimeSubscriptionError::AlreadyOpen);
        }
        let ticket = self.lifecycle.admission_ticket()?;
        let queue_token =
            match self
                .local_state
                .admit(RuntimeStateCategory::Queue, 1, 128, Retention::Terminal)
            {
                Ok(token) => token,
                Err(error) => {
                    self.record_capacity_rejection();
                    return Err(error.into());
                }
            };
        let Some(runtime_session) = self.sessions.get(&partition) else {
            self.local_state
                .capacity
                .lock()?
                .release_terminal(queue_token)?;
            return Err(RuntimeSubscriptionError::PartitionUnavailable(partition));
        };
        let session = match runtime_session {
            RuntimeSession::Strong(session) => Arc::clone(session),
            RuntimeSession::Development(_) => {
                self.local_state
                    .capacity
                    .lock()?
                    .release_terminal(queue_token)?;
                return Err(RuntimeSubscriptionError::UnsupportedProfile);
            }
        };
        let observation = match session.checked_poll(0).await {
            Ok(observation) => observation.into_observation(),
            Err(_) => {
                self.local_state
                    .capacity
                    .lock()?
                    .release_terminal(queue_token)?;
                return Err(RuntimeSubscriptionError::PollUnavailable);
            }
        };
        let namespace = CreationNamespace::new(
            subscription_id,
            target,
            self.router.generation(),
            partition,
            self.persistence_lifecycle_generation,
            namespace_digest,
        );
        let directory = self
            .checkpoint_root
            .join(hex_subscription_id(subscription_id));
        let fence = Arc::new(AtomicBool::new(false));
        if let Err(error) = self
            .lifecycle
            .add_delivery_fence(ticket, Box::new(SubscriptionFence(Arc::clone(&fence))))
        {
            self.local_state
                .capacity
                .lock()?
                .release_terminal(queue_token)?;
            return Err(error.into());
        }
        let created = match match mode {
            SubscriptionOpenMode::Create(initial) => CreationStore.create(
                &directory,
                CreationRequest::new(namespace, initial),
                &observation,
            ),
            SubscriptionOpenMode::Reopen => {
                CreationStore.open(&directory, namespace, observation.resource_epoch())
            }
            SubscriptionOpenMode::Recover(binding) => {
                CreationStore.recover(&directory, binding, observation.resource_epoch())
            }
        } {
            Ok(created) => created,
            Err(_) => {
                self.abandon_subscription_open(queue_token)?;
                return Err(RuntimeSubscriptionError::InvalidLocalState);
            }
        };
        let outcome = created.public_outcome();
        let CreationStoreResult::Created(active) = created else {
            self.abandon_subscription_open(queue_token)?;
            return Ok(outcome);
        };
        let binding = active.binding();
        let creation = active.creation();
        let journal_namespace = JournalNamespace::new(
            binding.subscription_id(),
            binding.target(),
            binding.generation(),
            binding.partition(),
            binding.lifecycle_generation(),
            creation.resolved_initial(),
            creation.captured_resource_epoch(),
        );
        let journal = match match mode {
            SubscriptionOpenMode::Create(_) => {
                open_new_journal(active.directory(), journal_namespace, binding.owner_epoch())
            }
            SubscriptionOpenMode::Reopen | SubscriptionOpenMode::Recover(_) => {
                JournalStore::open(active.directory(), journal_namespace, binding.owner_epoch())
                    .map_err(|_| RuntimeSubscriptionError::InvalidLocalState)
            }
        } {
            Ok(journal) => journal,
            Err(error) => {
                self.abandon_subscription_open(queue_token)?;
                return Err(error);
            }
        };
        let rebind_pending = !matches!(mode, SubscriptionOpenMode::Create(_));
        let trace = DurableTraceContext::partition(
            binding.partition(),
            binding.owner_epoch(),
            creation.captured_resource_epoch(),
        );
        let coordinator = SubscriberCoordinator::new(
            binding,
            creation.captured_resource_epoch(),
            session,
            journal,
        );
        let recovery_pending = rebind_pending && coordinator.checkpoint_bytes().is_none();
        self.subscriptions.insert(
            subscription_id,
            RuntimeSubscription {
                coordinator,
                _active: *active,
                fence,
                _queue_token: queue_token,
                in_flight_token: None,
                rebind_pending,
            },
        );
        if recovery_pending {
            self.record_metric(DurableMetricLabels::new(
                MetricOperation::Recovery,
                MetricBoundary::BeforeMutation,
                MetricFailureStage::None,
                MetricOutcome::Success,
            ));
            self.record_event(DurableEvent::new(DurableEventKind::Recovery, trace));
        }
        Ok(outcome)
    }

    fn record_metric(&mut self, labels: DurableMetricLabels) {
        let _ = self.lifecycle.observability_mut().record_metric(labels);
    }

    fn record_event(&mut self, event: DurableEvent) {
        self.lifecycle.observability_mut().record_event(event);
    }

    fn record_capacity_rejection(&mut self) {
        self.record_metric(DurableMetricLabels::new(
            MetricOperation::Capacity,
            MetricBoundary::BeforeMutation,
            MetricFailureStage::Capacity,
            MetricOutcome::Unavailable,
        ));
    }

    fn abandon_subscription_open(
        &mut self,
        queue_token: CapacityToken,
    ) -> Result<(), RuntimeSubscriptionError> {
        let lifecycle = self.lifecycle.cancel_last_delivery_fence();
        let capacity = self
            .local_state
            .capacity
            .lock()
            .and_then(|mut capacity| capacity.release_terminal(queue_token));
        lifecycle?;
        capacity?;
        Ok(())
    }

    fn validate_prepared(&self, prepared: &PreparedDurableSend) -> Result<(), RuntimeSendError> {
        if prepared.source() != self.router.source()
            || prepared.generation() != self.router.generation()
            || prepared.routing_map_version() != ROUTING_MAP_VERSION
        {
            return Err(RuntimeSendError::RouteBindingMismatch);
        }
        self.router.validate_partition(
            prepared.target(),
            prepared.ordering_key(),
            prepared.partition(),
        )?;
        Ok(())
    }

    fn subscription_mut(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> Result<&mut RuntimeSubscription, RuntimeSubscriptionError> {
        self.subscriptions
            .get_mut(&subscription_id)
            .ok_or(RuntimeSubscriptionError::NotOpen)
    }

    fn partition_state(&self, partition: u32) -> PartitionState {
        self.subscriptions
            .values()
            .find(|subscription| {
                subscription.coordinator.status() != PartitionStatus::Active
                    && subscription._active.binding().partition() == partition
            })
            .map_or(PartitionState::Active, |subscription| {
                partition_state(subscription.coordinator.status())
            })
    }
}

fn resolve_runtime_readiness(
    phase: LifecyclePhase,
    session_readiness: impl IntoIterator<Item = Readiness>,
) -> Readiness {
    if matches!(phase, LifecyclePhase::Draining | LifecyclePhase::Closed) {
        return Readiness::Unavailable(UnavailableReason::Shutdown);
    }
    session_readiness
        .into_iter()
        .find(|readiness| *readiness != Readiness::Available)
        .unwrap_or(Readiness::Available)
}

async fn execute_send<P>(
    lifecycle: &mut LifecycleCoordinator,
    port: Arc<P>,
    prepared: &PreparedDurableSend,
    boundary: ConfirmationBoundary,
    capacity_release: SendCapacityRelease,
) -> Result<DurableSendResult, RuntimeSendError>
where
    P: AppendPort + Send + Sync + 'static + ?Sized,
{
    let producer = ProducerCoordinator::new(port);
    let started = producer.preflight(prepared, boundary)?.start()?;
    let ticket = lifecycle.admission_ticket()?;
    let operation = lifecycle.register_send(ticket, SendOperationPhase::Prepared)?;
    let admitted_operation = operation.clone();
    let completed_operation = operation.clone();
    let mut attempt = producer.execute_with_callbacks(
        started,
        move || {
            admitted_operation
                .transition(SendOperationPhase::AppendInvoked)
                .is_ok()
        },
        move |terminal| {
            let outcome = terminal.as_ref().map_or(
                DurableSendOutcome::Indeterminate(
                    alopex_chirps_core::durable::AttemptFailureKind::Protocol,
                ),
                DurableSendResult::outcome,
            );
            let _ = completed_operation.transition(SendOperationPhase::Terminal(outcome));
            capacity_release.complete();
        },
    );
    let (cancel, owner) = attempt.take_owner().ok_or_else(|| {
        RuntimeSendError::Coordinator("send attempt owner was unavailable".to_owned())
    })?;
    lifecycle.own_operation_worker(cancel, owner)?;
    let terminal = attempt.terminal().await;
    let (phase, frozen) = operation.snapshot()?;
    match terminal.result() {
        Ok(result) if frozen => {
            let binding = result.attempt_binding().clone();
            match phase {
                SendOperationPhase::Prepared => DurableSendResult::not_submitted(
                    prepared,
                    binding,
                    alopex_chirps_core::durable::AttemptFailureKind::Shutdown,
                ),
                SendOperationPhase::AppendInvoked => DurableSendResult::indeterminate(
                    prepared,
                    binding,
                    alopex_chirps_core::durable::AttemptFailureKind::Shutdown,
                ),
                SendOperationPhase::Terminal(_) | SendOperationPhase::Idle => {
                    return Err(RuntimeSendError::Coordinator(
                        "shutdown froze an invalid send phase".to_owned(),
                    ));
                }
            }
            .map_err(|error| RuntimeSendError::Coordinator(error.to_string()))
        }
        Ok(result) => {
            if phase != SendOperationPhase::Terminal(result.outcome()) {
                return Err(RuntimeSendError::Coordinator(
                    "send owner did not install its terminal lifecycle phase".to_owned(),
                ));
            }
            Ok(result.clone())
        }
        Err(_) if frozen => Err(LifecycleCoordinatorError::OperationFrozen.into()),
        Err(error) if matches!(phase, SendOperationPhase::Terminal(_)) => {
            Err(RuntimeSendError::Coordinator(error.to_string()))
        }
        Err(_) => Err(RuntimeSendError::Coordinator(
            "send owner did not install its terminal failure phase".to_owned(),
        )),
    }
}

#[derive(Clone, Copy)]
enum SubscriptionOpenMode {
    Create(InitialPosition),
    Reopen,
    Recover(CreationRecoveryBinding),
}

fn encode_login(
    credential: SessionCredentialInput,
) -> Result<LoginRequestFrame, RuntimeBuildError> {
    let (code, payload) = match credential {
        SessionCredentialInput::UsernamePassword { username, password } => {
            let username =
                WireName::new(&username).map_err(|_| RuntimeBuildError::InvalidCredential)?;
            (
                LOGIN_USER_CODE,
                LoginUserRequest {
                    username,
                    password,
                    version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                    context: Some(String::new()),
                }
                .to_bytes(),
            )
        }
        SessionCredentialInput::PersonalAccessToken(token) => {
            let token = WireName::new(&token).map_err(|_| RuntimeBuildError::InvalidCredential)?;
            (
                LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
                LoginWithPersonalAccessTokenRequest { token }.to_bytes(),
            )
        }
    };
    let mut bytes = BytesMut::new();
    RequestFrame::encode(code, &payload, &mut bytes)
        .map_err(|_| RuntimeBuildError::InvalidCredential)?;
    LoginRequestFrame::try_from(bytes.freeze()).map_err(|_| RuntimeBuildError::InvalidCredential)
}

fn validate_connection_partitions(
    partition_count: u32,
    connections: &[SessionConnectConfig],
) -> Result<(), RuntimeBuildError> {
    let mut seen = BTreeMap::new();
    for connection in connections {
        let partition = connection.partition();
        if partition >= partition_count {
            return Err(RuntimeBuildError::UnexpectedPartition(partition));
        }
        if seen.insert(partition, ()).is_some() {
            return Err(RuntimeBuildError::DuplicatePartition(partition));
        }
    }
    for partition in 0..partition_count {
        if !seen.contains_key(&partition) {
            return Err(RuntimeBuildError::MissingPartition(partition));
        }
    }
    Ok(())
}

async fn shutdown_partial_sessions(
    sessions: impl Iterator<Item = RuntimeSession>,
    deadline: Instant,
) {
    let sessions: Vec<_> = sessions.collect();
    for session in &sessions {
        session.request_close();
    }
    for session in sessions {
        let _ = shutdown_session(session, deadline).await;
    }
}

fn open_new_journal(
    directory: &Path,
    namespace: JournalNamespace,
    owner_epoch: u64,
) -> Result<JournalStore, RuntimeSubscriptionError> {
    match JournalStore::initialize(directory, namespace, owner_epoch)
        .map_err(|_| RuntimeSubscriptionError::InvalidLocalState)?
    {
        JournalInitialization::Ready(journal) => Ok(*journal),
        JournalInitialization::NotCommitted => Err(RuntimeSubscriptionError::StorageUnavailable),
        JournalInitialization::Unknown => Err(RuntimeSubscriptionError::RecoveryRequired),
    }
}

fn ensure_not_fenced(subscription: &RuntimeSubscription) -> Result<(), RuntimeSubscriptionError> {
    if subscription.fence.load(Ordering::Acquire) {
        Err(RuntimeSubscriptionError::LifecycleUnavailable)
    } else {
        Ok(())
    }
}

fn hex_subscription_id(id: SubscriptionId) -> String {
    id.as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn partition_state(status: PartitionStatus) -> PartitionState {
    match status {
        PartitionStatus::Active | PartitionStatus::InFlight => PartitionState::Active,
        PartitionStatus::RetentionGap => {
            PartitionState::RecoveryRequired(RecoveryReason::RetentionGap)
        }
        PartitionStatus::RecoveryRequired => {
            PartitionState::RecoveryRequired(RecoveryReason::CheckpointIndeterminate)
        }
        PartitionStatus::Faulted | PartitionStatus::Closed => {
            PartitionState::Faulted(PartitionFaultReason::InvalidPollObservation)
        }
    }
}

fn send_metric_labels(
    boundary: ConfirmationBoundary,
    result: &Result<DurableSendResult, RuntimeSendError>,
) -> DurableMetricLabels {
    let boundary = match boundary {
        ConfirmationBoundary::BrokerAccepted => MetricBoundary::BrokerAccepted,
        ConfirmationBoundary::OsSyncedAccepted => MetricBoundary::OsSyncedAccepted,
    };
    let (failure_stage, outcome) = match result {
        Ok(result) => match result.outcome() {
            DurableSendOutcome::BrokerAccepted | DurableSendOutcome::OsSyncedAccepted => {
                (MetricFailureStage::None, MetricOutcome::Success)
            }
            DurableSendOutcome::NotSubmitted(_) => {
                (MetricFailureStage::Preflight, MetricOutcome::NotSubmitted)
            }
            DurableSendOutcome::Indeterminate(_) => {
                (MetricFailureStage::Response, MetricOutcome::Indeterminate)
            }
        },
        Err(RuntimeSendError::Capacity) => {
            (MetricFailureStage::Capacity, MetricOutcome::Unavailable)
        }
        Err(RuntimeSendError::Preflight(_))
        | Err(RuntimeSendError::RouteBindingMismatch)
        | Err(RuntimeSendError::PartitionUnavailable(_))
        | Err(RuntimeSendError::Routing(_)) => {
            (MetricFailureStage::Preflight, MetricOutcome::Unavailable)
        }
        Err(_) => (MetricFailureStage::Transport, MetricOutcome::Unavailable),
    };
    DurableMetricLabels::new(MetricOperation::Send, boundary, failure_stage, outcome)
}

fn checkpoint_outcome_at_freeze(
    phase: CheckpointOperationPhase,
) -> Result<CheckpointOutcome, RuntimeSubscriptionError> {
    match phase {
        CheckpointOperationPhase::Prewrite | CheckpointOperationPhase::KnownOld => {
            Ok(CheckpointOutcome::CheckpointNotCommitted)
        }
        CheckpointOperationPhase::InstallUnknown => Ok(CheckpointOutcome::CheckpointUnknown),
        CheckpointOperationPhase::Confirmed => Ok(CheckpointOutcome::CheckpointCommitted),
        CheckpointOperationPhase::Terminal(outcome) => Ok(outcome),
        CheckpointOperationPhase::Idle => Err(RuntimeSubscriptionError::LifecycleUnavailable),
    }
}

/// Runtime construction rejected before public work can be admitted.
#[derive(Debug, Error)]
pub enum RuntimeBuildError {
    #[error("durable lifecycle generation must be non-zero")]
    InvalidLifecycleGeneration,
    #[error("durable renewal interval is invalid")]
    InvalidRenewInterval,
    #[error("durable TLS server identity or trust is invalid")]
    InvalidTlsIdentity,
    #[error("durable credential material is invalid")]
    InvalidCredential,
    #[error("durable transport limits are invalid")]
    InvalidTransportLimits,
    #[error("durable resource projection is invalid")]
    InvalidResourceProjection,
    #[error("durable session for partition {0} is missing")]
    MissingPartition(u32),
    #[error("durable session for partition {0} is duplicated")]
    DuplicatePartition(u32),
    #[error("durable session reported unexpected partition {0}")]
    UnexpectedPartition(u32),
    #[error("durable routing configuration is invalid: {0}")]
    Routing(#[from] RoutingError),
    #[error("durable session setup failed: {0}")]
    Session(#[from] SessionError),
    #[error("durable development session setup failed: {0}")]
    DevelopmentSession(#[from] DevelopmentConnectError),
    #[error("durable development profile is invalid: {0}")]
    DevelopmentProfile(#[from] DevelopmentProfileError),
    #[error("durable lifecycle setup failed: {0}")]
    Lifecycle(#[from] LifecycleCoordinatorError),
    #[error("durable lifecycle observability configuration is invalid")]
    LifecycleConfiguration,
    #[error("durable local-state capacity configuration failed")]
    Capacity,
    #[error("durable local compaction recovery failed")]
    Compaction,
    #[error("durable local state is already owned by another process")]
    LocalStateOwned,
}

/// Offline preparation rejected by routing or canonical encoding.
#[derive(Debug, Error)]
pub enum RuntimePrepareError {
    #[error(transparent)]
    Routing(#[from] RoutingError),
    #[error(transparent)]
    Producer(#[from] crate::producer::ProducerPrepareError),
}

/// Send rejected at the runtime composition boundary.
#[derive(Debug, Error)]
pub enum RuntimeSendError {
    #[error("durable prepared route does not belong to this handle")]
    RouteBindingMismatch,
    #[error("durable partition {0} is unavailable")]
    PartitionUnavailable(u32),
    #[error("durable route is invalid: {0}")]
    Routing(#[from] RoutingError),
    #[error(transparent)]
    Preflight(#[from] alopex_chirps_core::durable::PreflightFailure),
    #[error(transparent)]
    CoordinatorStart(#[from] ProducerCoordinatorError),
    #[error("durable send coordination failed: {0}")]
    Coordinator(String),
    #[error("durable lifecycle rejected send: {0}")]
    Lifecycle(#[from] LifecycleCoordinatorError),
    #[error("durable local-state capacity rejected send")]
    Capacity,
}

/// Subscription creation or operation failed without exposing state internals.
#[derive(Debug, Error)]
pub enum RuntimeSubscriptionError {
    #[error("durable partition {0} is unavailable")]
    PartitionUnavailable(u32),
    #[error("durable subscription is unsupported by the selected profile")]
    UnsupportedProfile,
    #[error("subscription is already open")]
    AlreadyOpen,
    #[error("subscription is not open")]
    NotOpen,
    #[error("checked poll is unavailable")]
    PollUnavailable,
    #[error("local subscription storage is unavailable")]
    StorageUnavailable,
    #[error("local subscription state requires recovery")]
    RecoveryRequired,
    #[error("local subscription state is invalid")]
    InvalidLocalState,
    #[error("durable lifecycle is unavailable")]
    LifecycleUnavailable,
    #[error(transparent)]
    Subscriber(#[from] SubscriberError),
    #[error("durable lifecycle rejected subscription work: {0}")]
    Lifecycle(#[from] LifecycleCoordinatorError),
    #[error("durable local-state capacity rejected subscription work")]
    Capacity,
    #[error("durable local compaction state rejected subscription work")]
    Compaction,
}

/// Safe local-state observation or compaction failure.
#[derive(Debug, Error)]
pub enum RuntimeLocalStateError {
    #[error("durable local-state capacity failed")]
    Capacity,
    #[error("durable local compaction failed")]
    Compaction,
}

impl From<CapacityError> for RuntimeBuildError {
    fn from(_: CapacityError) -> Self {
        Self::Capacity
    }
}

impl From<CompactionError> for RuntimeBuildError {
    fn from(_: CompactionError) -> Self {
        Self::Compaction
    }
}

impl From<CapacityError> for RuntimeSendError {
    fn from(_: CapacityError) -> Self {
        Self::Capacity
    }
}

impl From<CapacityError> for RuntimeSubscriptionError {
    fn from(_: CapacityError) -> Self {
        Self::Capacity
    }
}

impl From<CompactionError> for RuntimeSubscriptionError {
    fn from(_: CompactionError) -> Self {
        Self::Compaction
    }
}

impl From<CapacityError> for RuntimeLocalStateError {
    fn from(_: CapacityError) -> Self {
        Self::Capacity
    }
}

impl From<CompactionError> for RuntimeLocalStateError {
    fn from(_: CompactionError) -> Self {
        Self::Compaction
    }
}

/// Shutdown failed to prove transport close and worker join.
#[derive(Debug, Error)]
pub enum RuntimeShutdownError {
    #[error(transparent)]
    Subscriber(#[from] SubscriberError),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleCoordinatorError),
}

#[cfg(test)]
mod tests {
    use super::{
        RuntimeBuildError, RuntimeCapacityConfig, RuntimeCapacityLimit, RuntimeStateCategory,
        SendCapacityRelease, SharedCapacity, SubscriptionFence, execute_send,
        resolve_runtime_readiness, validate_connection_partitions,
    };
    use crate::lifecycle::{LifecycleCoordinator, ShutdownTransport, ShutdownTransportReport};
    use crate::observability::{BoundedObservability, ObservabilityLimits};
    use crate::producer::{
        AppendAdmission, AppendAttempt, AppendContext, AppendPort, AppendPortOutcome, prepare,
    };
    use crate::protocol::ResourceLocation;
    use crate::routing::PartitionRouter;
    use crate::state::capacity::{CapacityFootprint, Retention, StateCategory};
    use alopex_chirps_core::durable::{
        ConfirmationBoundary, LifecyclePhase, PreflightFailureKind, Readiness,
        RegisteredOperationKind, ResourceId, SessionFingerprint, UnavailableReason,
    };
    use alopex_chirps_wire::node_id::NodeId;
    use async_trait::async_trait;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;
    use tokio::time::{Duration, Instant, timeout};

    struct NoopTransport;

    struct BlockingAppendPort {
        location: ResourceLocation,
        admitted: Arc<Notify>,
    }

    #[async_trait]
    impl AppendPort for BlockingAppendPort {
        fn preflight(
            &self,
            _prepared: &alopex_chirps_core::durable::PreparedDurableSend,
            _requested_boundary: ConfirmationBoundary,
        ) -> Result<AppendContext, PreflightFailureKind> {
            Ok(AppendContext::new(
                SessionFingerprint::from_bytes([0x41; 32]),
                self.location,
            ))
        }

        async fn append_once(
            &self,
            _attempt: &AppendAttempt,
            admission: AppendAdmission,
        ) -> AppendPortOutcome {
            assert!(admission.admit());
            self.admitted.notify_one();
            pending().await
        }
    }

    #[async_trait]
    impl ShutdownTransport for NoopTransport {
        fn request_close(&self) {}

        async fn shutdown(self: Box<Self>, _deadline: Instant) -> ShutdownTransportReport {
            ShutdownTransportReport::new(true, true)
        }
    }

    fn lifecycle() -> LifecycleCoordinator {
        let mut lifecycle = LifecycleCoordinator::new(
            Box::new(NoopTransport),
            BoundedObservability::new(ObservabilityLimits::new(8, 8).unwrap()),
        );
        lifecycle.mark_ready(Readiness::Available).unwrap();
        lifecycle
    }

    #[test]
    fn v07_task_6_5_closed_runtime_keeps_shutdown_readiness() {
        assert_eq!(
            resolve_runtime_readiness(LifecyclePhase::Draining, std::iter::empty()),
            Readiness::Unavailable(UnavailableReason::Shutdown)
        );
        assert_eq!(
            resolve_runtime_readiness(LifecyclePhase::Closed, std::iter::empty()),
            Readiness::Unavailable(UnavailableReason::Shutdown)
        );
    }

    #[test]
    fn v07_task_4_6_runtime_requires_every_explicit_partition() {
        assert!(matches!(
            validate_connection_partitions(2, &[]),
            Err(RuntimeBuildError::MissingPartition(0))
        ));
    }

    #[test]
    fn v07_task_6_5_creation_registers_before_persistence_or_stays_fenced() {
        let mut rejected = lifecycle();
        let stale = rejected.admission_ticket().unwrap();
        rejected.request_shutdown().unwrap();
        let rejected_fence = Arc::new(AtomicBool::new(false));
        let mut persistence_started = false;
        let admission = rejected.add_delivery_fence(
            stale,
            Box::new(SubscriptionFence(Arc::clone(&rejected_fence))),
        );
        if admission.is_ok() {
            persistence_started = true;
        }
        assert!(admission.is_err());
        assert!(!persistence_started);
        assert!(!rejected_fence.load(Ordering::Acquire));

        let mut known_old = lifecycle();
        let ticket = known_old.admission_ticket().unwrap();
        let cancelled_fence = Arc::new(AtomicBool::new(false));
        known_old
            .add_delivery_fence(
                ticket,
                Box::new(SubscriptionFence(Arc::clone(&cancelled_fence))),
            )
            .unwrap();
        known_old.cancel_last_delivery_fence().unwrap();
        known_old.request_shutdown().unwrap();
        assert!(!cancelled_fence.load(Ordering::Acquire));
        assert!(
            known_old
                .shutdown_freeze()
                .unwrap()
                .started_operations()
                .is_empty()
        );

        let mut accepted = lifecycle();
        let ticket = accepted.admission_ticket().unwrap();
        let accepted_fence = Arc::new(AtomicBool::new(false));
        accepted
            .add_delivery_fence(
                ticket,
                Box::new(SubscriptionFence(Arc::clone(&accepted_fence))),
            )
            .unwrap();
        accepted.request_shutdown().unwrap();
        persistence_started = true;
        assert!(persistence_started);
        assert!(accepted_fence.load(Ordering::Acquire));
        let freeze = accepted.shutdown_freeze().unwrap();
        assert_eq!(freeze.started_operations().len(), 1);
        assert_eq!(
            freeze.started_operations()[0].kind(),
            RegisteredOperationKind::Worker
        );
    }

    #[tokio::test]
    async fn v07_task_6_5_cancelled_send_holds_capacity_until_owner_terminal() {
        let capacity = SharedCapacity::new(
            RuntimeCapacityConfig::uniform(
                RuntimeCapacityLimit::new(3, 1024),
                RuntimeCapacityLimit::new(1, 1),
                RuntimeCapacityLimit::new(1, 1),
            )
            .with_limit(
                RuntimeStateCategory::Payload,
                RuntimeCapacityLimit::new(1, 8),
            )
            .with_limit(
                RuntimeStateCategory::Concurrency,
                RuntimeCapacityLimit::new(1, 1),
            )
            .controller()
            .unwrap(),
        );
        let payload = capacity
            .lock()
            .unwrap()
            .try_admit(
                StateCategory::Payload,
                CapacityFootprint::new(1, 8).unwrap(),
                Retention::Terminal,
            )
            .unwrap();
        let concurrency = capacity
            .lock()
            .unwrap()
            .try_admit_continuation(
                StateCategory::Concurrency,
                CapacityFootprint::new(1, 1).unwrap(),
                Retention::Terminal,
            )
            .unwrap();
        let release = SendCapacityRelease::new(capacity.clone(), payload, concurrency);
        let router = PartitionRouter::new(NodeId::new(), 1, 1).unwrap();
        let prepared = prepare(&router, NodeId::new(), b"key".to_vec(), b"payload").unwrap();
        let mut resource_id = [0x42; 16];
        resource_id[6] = 0x42;
        resource_id[8] = 0x82;
        let location =
            ResourceLocation::new(ResourceId::from_bytes(resource_id), 1, 1, 1, 0).unwrap();
        let admitted = Arc::new(Notify::new());
        let port = Arc::new(BlockingAppendPort {
            location,
            admitted: Arc::clone(&admitted),
        });
        let mut lifecycle = lifecycle();
        let task = tokio::spawn(async move {
            execute_send(
                &mut lifecycle,
                port,
                &prepared,
                ConfirmationBoundary::BrokerAccepted,
                release,
            )
            .await
        });
        timeout(Duration::from_secs(1), admitted.notified())
            .await
            .unwrap();
        assert!(
            capacity
                .lock()
                .unwrap()
                .try_admit(
                    StateCategory::Payload,
                    CapacityFootprint::new(1, 1).unwrap(),
                    Retention::Terminal,
                )
                .is_err()
        );
        task.abort();
        let _ = task.await;
        timeout(Duration::from_secs(1), async {
            loop {
                let released = {
                    let capacity = capacity.lock().unwrap();
                    capacity.usage(StateCategory::Payload) == CapacityFootprint::default()
                        && capacity.usage(StateCategory::Concurrency)
                            == CapacityFootprint::default()
                };
                if released {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            capacity
                .lock()
                .unwrap()
                .try_admit(
                    StateCategory::Payload,
                    CapacityFootprint::new(1, 1).unwrap(),
                    Retention::Terminal,
                )
                .is_ok()
        );
    }
}
