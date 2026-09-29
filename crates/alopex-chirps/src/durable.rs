//! Additive Durable construction for the optional Iggy adapter.

use crate::{MeshHandle, NodeId};
pub use alopex_chirps_backend_iggy::observability::ObservabilityReport as DurableObservabilityReport;
#[cfg(feature = "durable-verification")]
pub use alopex_chirps_backend_iggy::producer::{
    AppendVerificationError, AppendVerificationObserver,
};
use alopex_chirps_backend_iggy::producer::{
    DevelopmentBrokerConfigReadback, DevelopmentConnectError,
};
use alopex_chirps_backend_iggy::runtime::{
    CHECKPOINT_JOURNAL_LIMIT_BYTES, DevelopmentSessionConnectionInput, DurableRuntime,
    RuntimeBuildError, RuntimeCapacityConfig, RuntimeCapacityLimit, RuntimeClockReading,
    RuntimeClockSource, RuntimeClockTrust, RuntimeCompactionResult, RuntimeLocalStateError,
    RuntimeLocalStateStatus, RuntimePrepareError, RuntimeSendError, RuntimeShutdownError,
    RuntimeShutdownTrigger, RuntimeStateCategory, RuntimeSubscriptionError, SessionConnectConfig,
    SessionConnectionInput, SessionCredentialInput, SessionProfileInput, SessionProjectionInput,
};
use alopex_chirps_backend_iggy::session::SessionError;
use alopex_chirps_backend_iggy::subscriber::{DeliveryClock, NextDelivery};
use alopex_chirps_core::durable::{
    CheckpointOutcome, ConfirmationBoundary, CreationRecoveryBinding, Delivery, DeliveryHandle,
    DurableHealth, DurableSendResult, InitialPosition, LifecyclePhase, PreparedDurableSend,
    Readiness, ShutdownGeneration, ShutdownReport, SubscriptionCreationOutcome, SubscriptionId,
    UnavailableReason,
};
use async_trait::async_trait;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::time::{Duration, Instant};

/// Fixed local checkpoint journal bound supported by this release.
pub const DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES: u64 = CHECKPOINT_JOURNAL_LIMIT_BYTES;

/// Independently bounded local-state categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DurableStateCategory {
    Payload,
    InFlight,
    CheckpointJournal,
    ProcessedIdentity,
    Queue,
    Concurrency,
}

impl DurableStateCategory {
    pub const ALL: [Self; 6] = [
        Self::Payload,
        Self::InFlight,
        Self::CheckpointJournal,
        Self::ProcessedIdentity,
        Self::Queue,
        Self::Concurrency,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    const fn backend(self) -> RuntimeStateCategory {
        match self {
            Self::Payload => RuntimeStateCategory::Payload,
            Self::InFlight => RuntimeStateCategory::InFlight,
            Self::CheckpointJournal => RuntimeStateCategory::CheckpointJournal,
            Self::ProcessedIdentity => RuntimeStateCategory::ProcessedIdentity,
            Self::Queue => RuntimeStateCategory::Queue,
            Self::Concurrency => RuntimeStateCategory::Concurrency,
        }
    }

    const fn from_backend(value: RuntimeStateCategory) -> Self {
        match value {
            RuntimeStateCategory::Payload => Self::Payload,
            RuntimeStateCategory::InFlight => Self::InFlight,
            RuntimeStateCategory::CheckpointJournal => Self::CheckpointJournal,
            RuntimeStateCategory::ProcessedIdentity => Self::ProcessedIdentity,
            RuntimeStateCategory::Queue => Self::Queue,
            RuntimeStateCategory::Concurrency => Self::Concurrency,
        }
    }
}

/// One hard count/byte limit or startup reserve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableCapacityLimit {
    count: u64,
    bytes: u64,
}

impl DurableCapacityLimit {
    #[must_use]
    pub const fn new(count: u64, bytes: u64) -> Self {
        Self { count, bytes }
    }
}

/// Complete hard-limit and startup-reserve policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableCapacityConfig {
    limits: [DurableCapacityLimit; 6],
    checkpoint_reserve: DurableCapacityLimit,
    compaction_reserve: DurableCapacityLimit,
}

impl DurableCapacityConfig {
    #[must_use]
    pub const fn uniform(
        limit: DurableCapacityLimit,
        checkpoint_reserve: DurableCapacityLimit,
        compaction_reserve: DurableCapacityLimit,
    ) -> Self {
        Self {
            limits: [limit; 6],
            checkpoint_reserve,
            compaction_reserve,
        }
    }

    #[must_use]
    pub const fn with_limit(
        mut self,
        category: DurableStateCategory,
        limit: DurableCapacityLimit,
    ) -> Self {
        self.limits[category.index()] = limit;
        self
    }

    fn backend(self) -> RuntimeCapacityConfig {
        let mut config = RuntimeCapacityConfig::uniform(
            RuntimeCapacityLimit::new(self.limits[0].count, self.limits[0].bytes),
            RuntimeCapacityLimit::new(self.checkpoint_reserve.count, self.checkpoint_reserve.bytes),
            RuntimeCapacityLimit::new(self.compaction_reserve.count, self.compaction_reserve.bytes),
        );
        for category in DurableStateCategory::ALL {
            let limit = self.limits[category.index()];
            config = config.with_limit(
                category.backend(),
                RuntimeCapacityLimit::new(limit.count, limit.bytes),
            );
        }
        config
    }
}

impl Default for DurableCapacityConfig {
    fn default() -> Self {
        Self::uniform(
            DurableCapacityLimit::new(1_048_576, 1 << 40),
            DurableCapacityLimit::new(1, 64 * 1024),
            DurableCapacityLimit::new(1, 64 * 1024),
        )
    }
}

/// Trust attached to a durable-clock observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableClockTrust {
    Trusted,
    RollbackDetected,
    Unknown,
}

/// One wall-clock reading used for the retry-age horizon only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableClockReading {
    unix_millis: u64,
    trust: DurableClockTrust,
}

impl DurableClockReading {
    #[must_use]
    pub const fn new(unix_millis: u64, trust: DurableClockTrust) -> Self {
        Self { unix_millis, trust }
    }
}

/// Application-provided clock source for identity-horizon compaction.
pub trait DurableClockSource: Send + Sync {
    fn read(&self) -> DurableClockReading;
}

#[derive(Debug)]
struct SystemDurableClock;

impl DurableClockSource for SystemDurableClock {
    fn read(&self) -> DurableClockReading {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => DurableClockReading::new(
                duration.as_millis().min(u128::from(u64::MAX)) as u64,
                DurableClockTrust::Trusted,
            ),
            Err(_) => DurableClockReading::new(0, DurableClockTrust::Unknown),
        }
    }
}

struct ClockSourceAdapter(Arc<dyn DurableClockSource>);

impl RuntimeClockSource for ClockSourceAdapter {
    fn read(&self) -> RuntimeClockReading {
        let reading = self.0.read();
        RuntimeClockReading {
            unix_millis: reading.unix_millis,
            trust: match reading.trust {
                DurableClockTrust::Trusted => RuntimeClockTrust::Trusted,
                DurableClockTrust::RollbackDetected => RuntimeClockTrust::RollbackDetected,
                DurableClockTrust::Unknown => RuntimeClockTrust::Unknown,
            },
        }
    }
}

/// Provider-neutral production configuration for the Durable plane.
#[derive(Debug, Clone)]
pub struct DurableConfig {
    endpoint: SocketAddr,
    tls: DurableTlsConfig,
    credential_reference: String,
    profile: DurableProfile,
    routing: DurableRoutingConfig,
    resource: DurableResourceBinding,
    checkpoint: DurableCheckpointConfig,
    lease: Option<DurableLeaseConfig>,
    extension: DurableExtensionConfig,
}

impl DurableConfig {
    /// Creates one explicit configuration without retaining credential values.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        endpoint: SocketAddr,
        tls: DurableTlsConfig,
        credential_reference: String,
        profile: DurableProfile,
        routing: DurableRoutingConfig,
        resource: DurableResourceConfig,
        checkpoint: DurableCheckpointConfig,
        lease: DurableLeaseConfig,
        extension: DurableExtensionConfig,
    ) -> Self {
        Self {
            endpoint,
            tls,
            credential_reference,
            profile,
            routing,
            resource: DurableResourceBinding::Compatible(resource),
            checkpoint,
            lease: Some(lease),
            extension,
        }
    }
    /// Configures official broker acceptance from actual startup bytes and
    /// numeric resource coordinates. No compatible capability or lease is claimed.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn broker_accepted(
        endpoint: SocketAddr,
        tls: DurableTlsConfig,
        credential_reference: String,
        broker_startup_config: Vec<u8>,
        routing: DurableRoutingConfig,
        resource: DurableDevelopmentResourceConfig,
        checkpoint: DurableCheckpointConfig,
        max_frame_len: usize,
    ) -> Self {
        Self {
            endpoint,
            tls,
            credential_reference,
            profile: DurableProfile::broker_accepted(broker_startup_config),
            routing,
            resource: DurableResourceBinding::Development(resource),
            checkpoint,
            lease: None,
            extension: DurableExtensionConfig {
                required: false,
                max_frame_len,
            },
        }
    }
}

/// TLS server identity and explicit trust anchors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableTlsConfig {
    server_name: String,
    trusted_roots_der: Vec<Vec<u8>>,
}

impl DurableTlsConfig {
    #[must_use]
    pub fn new(server_name: String, trusted_roots_der: Vec<Vec<u8>>) -> Self {
        Self {
            server_name,
            trusted_roots_der,
        }
    }
}

/// Supported provider-neutral delivery profile.
#[derive(Clone, PartialEq, Eq)]
pub enum DurableProfile {
    OsSyncedAccepted,
    BrokerAccepted { broker_startup_config: Vec<u8> },
}

impl DurableProfile {
    /// Selects official Iggy broker acceptance after binding the exact broker
    /// startup configuration read back by deployment automation.
    #[must_use]
    pub fn broker_accepted(broker_startup_config: Vec<u8>) -> Self {
        Self::BrokerAccepted {
            broker_startup_config,
        }
    }
}

impl std::fmt::Debug for DurableProfile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OsSyncedAccepted => formatter.write_str("OsSyncedAccepted"),
            Self::BrokerAccepted {
                broker_startup_config,
            } => formatter
                .debug_struct("BrokerAccepted")
                .field("startup_config_bytes", &broker_startup_config.len())
                .finish(),
        }
    }
}

/// Immutable generation and explicit partition map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableRoutingConfig {
    inbox_generation: u64,
    partition_count: u32,
}

impl DurableRoutingConfig {
    #[must_use]
    pub const fn new(inbox_generation: u64, partition_count: u32) -> Self {
        Self {
            inbox_generation,
            partition_count,
        }
    }
}

/// One pre-provisioned resource and every exact partition projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableResourceConfig {
    stream_id: u32,
    topic_id: u32,
    partitions: Vec<DurablePartitionProjection>,
}

impl DurableResourceConfig {
    #[must_use]
    pub fn new(stream_id: u32, topic_id: u32, partitions: Vec<DurablePartitionProjection>) -> Self {
        Self {
            stream_id,
            topic_id,
            partitions,
        }
    }
}

/// Official development resource coordinates, verified through standard
/// authenticated topic readback. These are not a strong capability projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableDevelopmentResourceConfig {
    stream_id: u32,
    topic_id: u32,
    partitions: Vec<u32>,
}

impl DurableDevelopmentResourceConfig {
    #[must_use]
    pub fn new(stream_id: u32, topic_id: u32, partitions: Vec<u32>) -> Self {
        Self {
            stream_id,
            topic_id,
            partitions,
        }
    }
}

#[derive(Debug, Clone)]
enum DurableResourceBinding {
    Compatible(DurableResourceConfig),
    Development(DurableDevelopmentResourceConfig),
}

impl DurableResourceBinding {
    fn partition_ids(&self) -> Vec<u32> {
        match self {
            Self::Compatible(resource) => resource
                .partitions
                .iter()
                .map(|partition| partition.partition_id)
                .collect(),
            Self::Development(resource) => resource.partitions.clone(),
        }
    }
}

/// Exact capability projection for one explicit partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurablePartitionProjection {
    partition_id: u32,
    resource_id: [u8; 16],
    resource_epoch: u64,
    build_sha: [u8; 20],
    retention_bytes: u64,
    retention_messages: u64,
    checksum_enabled: bool,
    configuration_digest: [u8; 32],
    security_digest: [u8; 32],
    capability_digest: [u8; 32],
}

impl DurablePartitionProjection {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        partition_id: u32,
        resource_id: [u8; 16],
        resource_epoch: u64,
        build_sha: [u8; 20],
        retention_bytes: u64,
        retention_messages: u64,
        checksum_enabled: bool,
        configuration_digest: [u8; 32],
        security_digest: [u8; 32],
        capability_digest: [u8; 32],
    ) -> Self {
        Self {
            partition_id,
            resource_id,
            resource_epoch,
            build_sha,
            retention_bytes,
            retention_messages,
            checksum_enabled,
            configuration_digest,
            security_digest,
            capability_digest,
        }
    }
}

/// Local checkpoint ownership configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCheckpointConfig {
    root: PathBuf,
    lifecycle_generation: u64,
    max_journal_bytes: u64,
}

impl DurableCheckpointConfig {
    #[must_use]
    pub fn new(root: PathBuf, lifecycle_generation: u64, max_journal_bytes: u64) -> Self {
        Self {
            root,
            lifecycle_generation,
            max_journal_bytes,
        }
    }
}

/// Lease duration and same-connection renewal cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableLeaseConfig {
    lease_millis: u32,
    renew_interval: Duration,
}

impl DurableLeaseConfig {
    #[must_use]
    pub const fn new(lease_millis: u32, renew_interval: Duration) -> Self {
        Self {
            lease_millis,
            renew_interval,
        }
    }
}

/// Required compatible-server extension and bounded frame limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableExtensionConfig {
    required: bool,
    max_frame_len: usize,
}

impl DurableExtensionConfig {
    #[must_use]
    pub const fn required(max_frame_len: usize) -> Self {
        Self {
            required: true,
            max_frame_len,
        }
    }
}

/// Credential material returned transiently by a configured provider.
#[derive(Clone)]
pub enum DurableCredential {
    UsernamePassword {
        username: Arc<str>,
        password: Arc<str>,
    },
    PersonalAccessToken(Arc<str>),
}

impl DurableCredential {
    #[must_use]
    pub fn username_password(username: String, password: String) -> Self {
        Self::UsernamePassword {
            username: username.into(),
            password: password.into(),
        }
    }

    #[must_use]
    pub fn personal_access_token(token: String) -> Self {
        Self::PersonalAccessToken(token.into())
    }
}

/// Resolves a credential reference only while a production connection starts.
#[async_trait]
pub trait DurableCredentialProvider: Send + Sync {
    async fn resolve(
        &self,
        reference: &str,
    ) -> Result<DurableCredential, DurableCredentialProviderError>;
}

/// Bounded credential-provider failure without secret material.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DurableCredentialProviderError {
    #[error("durable credential is unavailable")]
    Unavailable,
    #[error("durable credential reference was rejected")]
    Rejected,
}

/// Builds one explicit Durable handle without changing a Mesh.
#[derive(Clone)]
pub struct DurableBuilder {
    source: NodeId,
    generation: Option<u64>,
    partitions: Option<u32>,
    capacity: DurableCapacityConfig,
    clock: Arc<dyn DurableClockSource>,
}

impl std::fmt::Debug for DurableBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableBuilder")
            .field("source", &self.source)
            .field("generation", &self.generation)
            .field("partitions", &self.partitions)
            .field("capacity", &self.capacity)
            .field("clock", &"configured")
            .finish()
    }
}

impl DurableBuilder {
    /// Starts additive Durable configuration for an explicit local node.
    #[must_use]
    pub fn new(source: NodeId) -> Self {
        Self {
            source,
            generation: None,
            partitions: None,
            capacity: DurableCapacityConfig::default(),
            clock: Arc::new(SystemDurableClock),
        }
    }

    /// Starts additive Durable configuration from an existing Mesh identity.
    #[must_use]
    pub fn from_mesh(mesh: &MeshHandle) -> Self {
        Self::new(mesh.node_id())
    }

    /// Selects the immutable inbox generation.
    #[must_use]
    pub fn inbox_generation(mut self, generation: u64) -> Self {
        self.generation = Some(generation);
        self
    }

    /// Selects the non-zero explicit partition count.
    #[must_use]
    pub fn explicit_partitions(mut self, partitions: u32) -> Self {
        self.partitions = Some(partitions);
        self
    }

    /// Configures every local hard bound and both startup reserves.
    #[must_use]
    pub fn local_capacity(mut self, capacity: DurableCapacityConfig) -> Self {
        self.capacity = capacity;
        self
    }

    /// Configures the clock used only for identity retry-age decisions.
    #[must_use]
    pub fn durable_clock(mut self, clock: Arc<dyn DurableClockSource>) -> Self {
        self.clock = clock;
        self
    }

    /// Validates and constructs an explicitly unconfigured local handle.
    pub fn build(self) -> Result<DurableHandle, DurableBuildError> {
        let (generation, partitions) = self.validated_routing()?;
        Ok(DurableHandle {
            source: self.source,
            generation,
            partitions,
            runtime: None,
        })
    }

    /// Connects and authenticates one session per explicit partition. Strong
    /// profiles additionally bind capabilities and start lease renewal.
    pub async fn connect<P>(
        self,
        config: DurableConfig,
        credentials: &P,
        deadline: Instant,
    ) -> Result<DurableHandle, DurableBuildError>
    where
        P: DurableCredentialProvider + ?Sized,
    {
        let generation = config.routing.inbox_generation;
        let partitions = config.routing.partition_count;
        if let DurableProfile::BrokerAccepted {
            broker_startup_config,
        } = &config.profile
        {
            DevelopmentBrokerConfigReadback::verify_actual_startup_config(broker_startup_config)
                .map_err(|_| DurableBuildError::BackendConfiguration)?;
        }
        let partition_ids = config.resource.partition_ids();
        if matches!(config.profile, DurableProfile::OsSyncedAccepted) {
            let lease = config
                .lease
                .ok_or(DurableBuildError::BackendConfiguration)?;
            if !config.extension.required
                || lease.lease_millis == 0
                || lease.renew_interval.is_zero()
                || lease.renew_interval >= Duration::from_millis(u64::from(lease.lease_millis))
            {
                return Err(DurableBuildError::BackendConfiguration);
            }
        }
        if partitions == 0
            || partition_ids.len() != partitions as usize
            || config.checkpoint.lifecycle_generation == 0
            || config.checkpoint.max_journal_bytes != CHECKPOINT_JOURNAL_LIMIT_BYTES
            || config.checkpoint.root.as_os_str().is_empty()
            || config.credential_reference.is_empty()
            || config.tls.server_name.is_empty()
            || config.tls.trusted_roots_der.is_empty()
            || config.extension.max_frame_len < 8
        {
            return Err(DurableBuildError::BackendConfiguration);
        }
        if self.generation.is_some_and(|value| value != generation)
            || self.partitions.is_some_and(|value| value != partitions)
        {
            return Err(DurableBuildError::BackendConfiguration);
        }
        let mut seen = vec![false; partitions as usize];
        for &partition in &partition_ids {
            let Some(slot) = seen.get_mut(partition as usize) else {
                return Err(DurableBuildError::BackendConfiguration);
            };
            if *slot {
                return Err(DurableBuildError::BackendConfiguration);
            }
            *slot = true;
        }
        let capacity = self.capacity.backend();
        capacity
            .validate()
            .map_err(DurableBuildError::from_runtime)?;
        let mut connections = Vec::with_capacity(partition_ids.len());
        match &config.resource {
            DurableResourceBinding::Compatible(resource) => {
                let lease = config
                    .lease
                    .ok_or(DurableBuildError::BackendConfiguration)?;
                for projection in &resource.partitions {
                    connections.push(
                        SessionConnectConfig::from_neutral(SessionConnectionInput {
                            address: config.endpoint,
                            tls_server_name: config.tls.server_name.clone(),
                            trusted_roots_der: config.tls.trusted_roots_der.clone(),
                            max_frame_len: config.extension.max_frame_len,
                            credential: None,
                            profile: match &config.profile {
                                DurableProfile::OsSyncedAccepted => {
                                    SessionProfileInput::OsSyncedAccepted
                                }
                                DurableProfile::BrokerAccepted {
                                    broker_startup_config,
                                } => SessionProfileInput::BrokerAccepted {
                                    broker_startup_config: broker_startup_config.clone(),
                                },
                            },
                            projection: SessionProjectionInput {
                                build_sha: projection.build_sha,
                                resource_id: projection.resource_id,
                                resource_epoch: projection.resource_epoch,
                                stream_id: resource.stream_id,
                                topic_id: resource.topic_id,
                                partition_id: projection.partition_id,
                                retention_bytes: projection.retention_bytes,
                                retention_messages: projection.retention_messages,
                                checksum_enabled: projection.checksum_enabled,
                                configuration_digest: projection.configuration_digest,
                                security_digest: projection.security_digest,
                                capability_digest: projection.capability_digest,
                                lease_millis: lease.lease_millis,
                            },
                            renew_interval: lease.renew_interval,
                        })
                        .map_err(DurableBuildError::from_runtime)?,
                    );
                }
            }
            DurableResourceBinding::Development(resource) => {
                let DurableProfile::BrokerAccepted {
                    broker_startup_config,
                } = &config.profile
                else {
                    return Err(DurableBuildError::BackendConfiguration);
                };
                for &partition_id in &resource.partitions {
                    connections.push(
                        SessionConnectConfig::from_development(DevelopmentSessionConnectionInput {
                            address: config.endpoint,
                            tls_server_name: config.tls.server_name.clone(),
                            trusted_roots_der: config.tls.trusted_roots_der.clone(),
                            max_frame_len: config.extension.max_frame_len,
                            credential: None,
                            broker_startup_config: broker_startup_config.clone(),
                            stream_id: resource.stream_id,
                            topic_id: resource.topic_id,
                            partition_id,
                        })
                        .map_err(DurableBuildError::from_runtime)?,
                    );
                }
            }
        }
        let credential = credentials
            .resolve(&config.credential_reference)
            .await
            .map_err(|error| match error {
                DurableCredentialProviderError::Unavailable => {
                    DurableBuildError::CredentialUnavailable
                }
                DurableCredentialProviderError::Rejected => DurableBuildError::CredentialRejected,
            })?;
        for connection in &mut connections {
            let credential = match credential.clone() {
                DurableCredential::UsernamePassword { username, password } => {
                    SessionCredentialInput::UsernamePassword {
                        username: username.to_string(),
                        password: password.to_string(),
                    }
                }
                DurableCredential::PersonalAccessToken(token) => {
                    SessionCredentialInput::PersonalAccessToken(token.to_string())
                }
            };
            connection
                .bind_credential(credential)
                .map_err(DurableBuildError::from_runtime)?;
        }
        let runtime = DurableRuntime::connect(
            self.source,
            generation,
            partitions,
            config.checkpoint.root,
            config.checkpoint.lifecycle_generation,
            capacity,
            Arc::new(ClockSourceAdapter(Arc::clone(&self.clock))),
            connections,
            deadline,
        )
        .await
        .map_err(DurableBuildError::from_runtime)?;
        Ok(DurableHandle {
            source: self.source,
            generation,
            partitions,
            runtime: Some(runtime),
        })
    }

    /// Connects normally, then installs a verification-only observer around
    /// every real append port before the handle can send.
    #[cfg(feature = "durable-verification")]
    pub async fn connect_with_append_observer<P>(
        self,
        config: DurableConfig,
        credentials: &P,
        deadline: Instant,
        observer: Arc<dyn AppendVerificationObserver>,
    ) -> Result<DurableHandle, DurableBuildError>
    where
        P: DurableCredentialProvider + ?Sized,
    {
        let mut handle = self.connect(config, credentials, deadline).await?;
        handle
            .runtime
            .as_mut()
            .expect("connected Durable handle always owns a runtime")
            .install_append_observer(observer);
        Ok(handle)
    }

    fn validated_routing(&self) -> Result<(u64, u32), DurableBuildError> {
        let generation = self
            .generation
            .ok_or(DurableBuildError::MissingGeneration)?;
        let partitions = self
            .partitions
            .ok_or(DurableBuildError::MissingPartitions)?;
        if partitions == 0 {
            return Err(DurableBuildError::InvalidPartitions);
        }
        Ok((generation, partitions))
    }
}

/// A Durable handle with immutable routing and optional concrete backend.
pub struct DurableHandle {
    source: NodeId,
    generation: u64,
    partitions: u32,
    runtime: Option<DurableRuntime>,
}

/// Cloneable signal that interrupts blocked Durable I/O without taking
/// ownership of the final shutdown report.
#[derive(Debug, Clone)]
pub struct DurableShutdownTrigger {
    inner: RuntimeShutdownTrigger,
}

impl DurableShutdownTrigger {
    /// Starts lifecycle drain once and closes every partition session.
    pub fn request_shutdown(&self) -> Result<(), DurableShutdownError> {
        self.inner
            .request_shutdown()
            .map_err(DurableShutdownError::from_runtime)
    }

    /// Returns whether every partition session received the close request.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.inner.is_shutdown_requested()
    }
}

impl DurableHandle {
    /// Returns a control that can interrupt blocked I/O from another task.
    #[must_use]
    pub fn shutdown_trigger(&self) -> Option<DurableShutdownTrigger> {
        self.runtime.as_ref().map(|runtime| DurableShutdownTrigger {
            inner: runtime.shutdown_trigger(),
        })
    }

    /// Prepares one immutable envelope without contacting a backend.
    pub fn prepare(
        &self,
        target: NodeId,
        ordering_key: Vec<u8>,
        payload: &[u8],
    ) -> Result<PreparedDurableSend, DurablePrepareError> {
        match &self.runtime {
            Some(runtime) => runtime
                .prepare(target, ordering_key, payload)
                .map_err(|_| DurablePrepareError::Rejected),
            None => DurableRuntime::prepare_configured(
                self.source,
                self.generation,
                self.partitions,
                target,
                ordering_key,
                payload,
            )
            .map_err(DurablePrepareError::from_runtime),
        }
    }

    /// Executes one backend attempt and returns its terminal core result.
    pub async fn send(
        &mut self,
        prepared: &PreparedDurableSend,
        boundary: ConfirmationBoundary,
    ) -> Result<DurableSendResult, DurableSendError> {
        self.runtime
            .as_mut()
            .ok_or(DurableSendError::Unconfigured)?
            .send(prepared, boundary)
            .await
            .map_err(DurableSendError::from_runtime)
    }

    /// Creates one explicit subscription with an explicit initial position.
    pub async fn create_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        target: NodeId,
        partition: u32,
        namespace_digest: [u8; 32],
        initial_position: InitialPosition,
    ) -> Result<SubscriptionCreationOutcome, DurableSubscriptionError> {
        self.runtime_mut()?
            .create_subscription(
                subscription_id,
                target,
                partition,
                namespace_digest,
                initial_position,
            )
            .await
            .map_err(DurableSubscriptionError::from_runtime)
    }

    /// Reopens one committed subscription and its existing journal.
    pub async fn reopen_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        target: NodeId,
        partition: u32,
        namespace_digest: [u8; 32],
    ) -> Result<SubscriptionCreationOutcome, DurableSubscriptionError> {
        self.runtime_mut()?
            .reopen_subscription(subscription_id, target, partition, namespace_digest)
            .await
            .map_err(DurableSubscriptionError::from_runtime)
    }

    /// Recovers one indeterminate creation using its exact core binding.
    pub async fn recover_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        target: NodeId,
        partition: u32,
        namespace_digest: [u8; 32],
        recovery: CreationRecoveryBinding,
    ) -> Result<SubscriptionCreationOutcome, DurableSubscriptionError> {
        self.runtime_mut()?
            .recover_subscription(
                subscription_id,
                target,
                partition,
                namespace_digest,
                recovery,
            )
            .await
            .map_err(DurableSubscriptionError::from_runtime)
    }

    /// Polls one open subscription at its canonical local frontier.
    pub async fn next_delivery(
        &mut self,
        subscription_id: SubscriptionId,
        retry_not_before_unix_ms: u64,
        clock: DurableDeliveryClock,
    ) -> Result<DurablePoll, DurableSubscriptionError> {
        self.runtime_mut()?
            .next_delivery(subscription_id, retry_not_before_unix_ms, clock.into())
            .await
            .map(DurablePoll::from_backend)
            .map_err(DurableSubscriptionError::from_runtime)
    }

    /// Commits the exact active core delivery handle.
    pub fn ack(
        &mut self,
        subscription_id: SubscriptionId,
        handle: &mut DeliveryHandle,
    ) -> Result<CheckpointOutcome, DurableSubscriptionError> {
        self.runtime_mut()?
            .ack(subscription_id, handle)
            .map_err(DurableSubscriptionError::from_runtime)
    }

    /// Releases the exact active core delivery handle without checkpointing.
    pub fn release(
        &mut self,
        subscription_id: SubscriptionId,
        handle: &mut DeliveryHandle,
    ) -> Result<(), DurableSubscriptionError> {
        self.runtime_mut()?
            .release(subscription_id, handle)
            .map_err(DurableSubscriptionError::from_runtime)
    }

    /// Returns Durable health from the lifecycle coordinator.
    #[must_use]
    pub fn health(&mut self) -> DurableHealth {
        self.runtime.as_mut().map_or_else(
            || {
                DurableHealth::new(
                    LifecyclePhase::Starting,
                    Readiness::Unavailable(UnavailableReason::Unconfigured),
                    Vec::new(),
                )
            },
            DurableRuntime::health,
        )
    }

    /// Returns one owned, bounded report from the production backend.
    #[must_use]
    pub fn observability(&self) -> Option<DurableObservabilityReport> {
        self.runtime.as_ref().map(DurableRuntime::observability)
    }

    /// Returns the recovered local-state generation and exact bounded usage.
    pub fn local_state_status(&self) -> Result<DurableLocalStateStatus, DurableLocalStateError> {
        self.runtime
            .as_ref()
            .ok_or(DurableLocalStateError::Unconfigured)?
            .local_state_status()
            .map(DurableLocalStateStatus::from_backend)
            .map_err(DurableLocalStateError::from_runtime)
    }

    /// Requests one safe generation-barrier cutover at the supplied broker oldest offset.
    pub fn compact_local_state(
        &mut self,
        broker_oldest: u64,
    ) -> Result<DurableCompactionOutcome, DurableLocalStateError> {
        self.runtime
            .as_mut()
            .ok_or(DurableLocalStateError::Unconfigured)?
            .compact_local_state(broker_oldest)
            .map(DurableCompactionOutcome::from_backend)
            .map_err(DurableLocalStateError::from_runtime)
    }

    /// Closes and joins only the independently owned Durable backend.
    pub async fn shutdown(
        &mut self,
        deadline: Instant,
    ) -> Result<ShutdownReport, DurableShutdownError> {
        match self.runtime.as_mut() {
            Some(runtime) => runtime
                .shutdown(deadline)
                .await
                .map_err(DurableShutdownError::from_runtime),
            None => ShutdownReport::try_new(
                ShutdownGeneration::from_value(1),
                true,
                true,
                Vec::new(),
                Vec::new(),
            )
            .map_err(|_| DurableShutdownError::Invariant),
        }
    }

    fn runtime_mut(&mut self) -> Result<&mut DurableRuntime, DurableSubscriptionError> {
        self.runtime
            .as_mut()
            .ok_or(DurableSubscriptionError::Unconfigured)
    }
}

/// Provider-neutral clock provenance for retry-age persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableDeliveryClock {
    Trusted,
    RollbackDetected,
    Unknown,
}

impl From<DurableDeliveryClock> for DeliveryClock {
    fn from(value: DurableDeliveryClock) -> Self {
        match value {
            DurableDeliveryClock::Trusted => Self::Trusted,
            DurableDeliveryClock::RollbackDetected => Self::RollbackDetected,
            DurableDeliveryClock::Unknown => Self::Unknown,
        }
    }
}

/// Public bounded result of one checked poll.
#[derive(Debug, PartialEq, Eq)]
pub enum DurablePoll {
    Tail,
    IdentityNotCommitted,
    Delivery(Delivery),
}

/// One exact local-state category usage observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableCapacityUsage {
    category: DurableStateCategory,
    count: u64,
    bytes: u64,
}

impl DurableCapacityUsage {
    #[must_use]
    pub const fn category(self) -> DurableStateCategory {
        self.category
    }

    #[must_use]
    pub const fn count(self) -> u64 {
        self.count
    }

    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }
}

/// Safe public projection of local compaction and capacity state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableLocalStateStatus {
    generation: u64,
    applied_through: u64,
    suffix_sequences: Vec<u64>,
    identity_count: usize,
    checkpoint_present: bool,
    recovery_required: bool,
    admission_open: bool,
    poll_open: bool,
    capacity: Vec<DurableCapacityUsage>,
}

impl DurableLocalStateStatus {
    fn from_backend(value: RuntimeLocalStateStatus) -> Self {
        Self {
            generation: value.generation,
            applied_through: value.applied_through,
            suffix_sequences: value.suffix_sequences,
            identity_count: value.identity_count,
            checkpoint_present: value.checkpoint_present,
            recovery_required: value.recovery_required,
            admission_open: value.admission_open,
            poll_open: value.poll_open,
            capacity: value
                .capacity
                .into_iter()
                .map(|usage| DurableCapacityUsage {
                    category: DurableStateCategory::from_backend(usage.category),
                    count: usage.count,
                    bytes: usage.bytes,
                })
                .collect(),
        }
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn applied_through(&self) -> u64 {
        self.applied_through
    }

    #[must_use]
    pub fn suffix_sequences(&self) -> &[u64] {
        &self.suffix_sequences
    }

    #[must_use]
    pub const fn identity_count(&self) -> usize {
        self.identity_count
    }

    #[must_use]
    pub const fn checkpoint_present(&self) -> bool {
        self.checkpoint_present
    }

    #[must_use]
    pub const fn recovery_required(&self) -> bool {
        self.recovery_required
    }

    #[must_use]
    pub const fn admission_open(&self) -> bool {
        self.admission_open
    }

    #[must_use]
    pub const fn poll_open(&self) -> bool {
        self.poll_open
    }

    #[must_use]
    pub fn capacity(&self) -> &[DurableCapacityUsage] {
        &self.capacity
    }
}

/// Terminal outcome of one local generation-barrier cutover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableCompactionOutcome {
    Committed { generation: u64, collected: usize },
    KeptOld,
    Unknown,
}

impl DurableCompactionOutcome {
    fn from_backend(value: RuntimeCompactionResult) -> Self {
        match value {
            RuntimeCompactionResult::Committed {
                generation,
                collected,
            } => Self::Committed {
                generation,
                collected,
            },
            RuntimeCompactionResult::KeptOld => Self::KeptOld,
            RuntimeCompactionResult::Unknown => Self::Unknown,
        }
    }
}

impl DurablePoll {
    fn from_backend(value: NextDelivery) -> Self {
        match value {
            NextDelivery::Tail => Self::Tail,
            NextDelivery::IdentityNotCommitted => Self::IdentityNotCommitted,
            NextDelivery::Delivery(delivery) => Self::Delivery(delivery),
        }
    }
}

/// Durable construction rejected before work can be admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DurableBuildError {
    #[error("durable inbox generation must be selected explicitly")]
    MissingGeneration,
    #[error("durable explicit partition count must be selected")]
    MissingPartitions,
    #[error("durable explicit partition count must be non-zero")]
    InvalidPartitions,
    #[error("durable backend configuration is invalid")]
    BackendConfiguration,
    #[error("durable backend is unavailable")]
    BackendUnavailable,
    #[error("durable credential provider is unavailable")]
    CredentialUnavailable,
    #[error("durable credential reference was rejected")]
    CredentialRejected,
}

impl DurableBuildError {
    fn from_runtime(error: RuntimeBuildError) -> Self {
        match error {
            RuntimeBuildError::InvalidCredential
            | RuntimeBuildError::Session(SessionError::AuthenticationRejected(_))
            | RuntimeBuildError::DevelopmentSession(DevelopmentConnectError::Session(
                SessionError::AuthenticationRejected(_),
            )) => Self::CredentialRejected,
            RuntimeBuildError::Session(_)
            | RuntimeBuildError::DevelopmentSession(_)
            | RuntimeBuildError::Lifecycle(_)
            | RuntimeBuildError::Compaction
            | RuntimeBuildError::LocalStateOwned => Self::BackendUnavailable,
            _ => Self::BackendConfiguration,
        }
    }
}

/// Offline preparation failed without exposing the backend adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DurablePrepareError {
    #[error("durable request preparation was rejected")]
    Rejected,
}

impl DurablePrepareError {
    fn from_runtime(_error: RuntimePrepareError) -> Self {
        Self::Rejected
    }
}

/// A send rejected without exposing adapter-specific handles or errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DurableSendError {
    #[error("durable plane is unconfigured")]
    Unconfigured,
    #[error("prepared route does not belong to this durable handle")]
    RouteRejected,
    #[error("durable send is unavailable")]
    Unavailable,
    #[error("durable lifecycle rejected send")]
    Lifecycle,
    #[error("durable send failed")]
    Failed,
}

impl DurableSendError {
    fn from_runtime(error: RuntimeSendError) -> Self {
        match error {
            RuntimeSendError::RouteBindingMismatch | RuntimeSendError::Routing(_) => {
                Self::RouteRejected
            }
            RuntimeSendError::PartitionUnavailable(_) | RuntimeSendError::Preflight(_) => {
                Self::Unavailable
            }
            RuntimeSendError::Capacity => Self::Unavailable,
            RuntimeSendError::Lifecycle(_) => Self::Lifecycle,
            RuntimeSendError::CoordinatorStart(_) | RuntimeSendError::Coordinator(_) => {
                Self::Failed
            }
        }
    }
}

/// Safe local-state observation or compaction failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DurableLocalStateError {
    #[error("durable plane is unconfigured")]
    Unconfigured,
    #[error("durable local state is unavailable")]
    Unavailable,
}

impl DurableLocalStateError {
    fn from_runtime(_error: RuntimeLocalStateError) -> Self {
        Self::Unavailable
    }
}

/// A subscription operation rejected by the explicit Durable plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DurableSubscriptionError {
    #[error("durable plane is unconfigured")]
    Unconfigured,
    #[error("durable subscription is unavailable")]
    Unavailable,
    #[error("durable subscription requires recovery")]
    RecoveryRequired,
    #[error("durable subscription state is invalid")]
    InvalidState,
    #[error("durable subscription is already open")]
    AlreadyOpen,
    #[error("durable subscription is not open")]
    NotOpen,
}

impl DurableSubscriptionError {
    fn from_runtime(error: RuntimeSubscriptionError) -> Self {
        match error {
            RuntimeSubscriptionError::AlreadyOpen => Self::AlreadyOpen,
            RuntimeSubscriptionError::NotOpen => Self::NotOpen,
            RuntimeSubscriptionError::RecoveryRequired => Self::RecoveryRequired,
            RuntimeSubscriptionError::InvalidLocalState => Self::InvalidState,
            RuntimeSubscriptionError::UnsupportedProfile => Self::Unavailable,
            _ => Self::Unavailable,
        }
    }
}

/// Shutdown failed to prove transport close and worker join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DurableShutdownError {
    #[error("durable shutdown could not close and join every owned resource")]
    Incomplete,
    #[error("durable shutdown report invariant failed")]
    Invariant,
}

impl DurableShutdownError {
    fn from_runtime(_error: RuntimeShutdownError) -> Self {
        Self::Incomplete
    }
}

#[cfg(test)]
mod tests {
    use super::{DurableBuildError, RuntimeBuildError, SessionError};

    #[test]
    fn runtime_build_failures_keep_public_configuration_and_availability_categories() {
        assert_eq!(
            DurableBuildError::from_runtime(RuntimeBuildError::LocalStateOwned),
            DurableBuildError::BackendUnavailable
        );
        assert_eq!(
            DurableBuildError::from_runtime(RuntimeBuildError::InvalidCredential),
            DurableBuildError::CredentialRejected
        );
        assert_eq!(
            DurableBuildError::from_runtime(RuntimeBuildError::Session(
                SessionError::AuthenticationRejected(401),
            )),
            DurableBuildError::CredentialRejected
        );
    }
}
