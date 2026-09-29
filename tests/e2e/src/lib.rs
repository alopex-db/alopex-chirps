//! Shared support for the explicit v0.7 end-to-end targets.

pub mod v07 {
    use alopex_chirps::{
        AppendVerificationError, AppendVerificationObserver,
        DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES, DurableBuildError, DurableBuilder,
        DurableCheckpointConfig, DurableConfig, DurableCredentialProvider, DurableExtensionConfig,
        DurableHandle, DurableLeaseConfig, DurablePartitionProjection, DurableProfile,
        DurableResourceConfig, DurableRoutingConfig, DurableTlsConfig, NodeId,
    };
    use alopex_chirps_backend_iggy::protocol::{
        CapabilityBindRequest, CapabilityReport, CheckedPollRequest, PrivateRequest,
        PrivateResponse, SessionBinding, VerifiedPrivateResponse,
    };
    use alopex_chirps_backend_iggy::transport::{
        DataPlaneRequestFrame, LoginRequestFrame, OwnedTransport, SessionControlRequestFrame,
        TransportLimits, TransportShutdownReport,
    };
    use alopex_chirps_core::durable::{
        AttemptBinding, AttemptFailureKind, ConfirmationBoundary, DurableSendOutcome,
        PollObservation, PreparedDurableSend,
    };
    use anyhow::{Context, Result, anyhow, ensure};
    use bytes::BytesMut;
    use chirps_fault_oracle::{
        APPEND_ONE_SYNCED_CODE, FaultProxy, ObservationKind, OracleAttemptId, OracleIntent,
        OracleObservation, OracleRecord, OracleStore, ResponseObservation, WireObservation,
        WireStage, verify_attempt,
    };
    use iggy::prelude::{
        CompressionAlgorithm, Identifier, IggyClient, IggyExpiry, MaxTopicSize, StreamClient,
        TopicClient, UserClient,
    };
    use iggy_binary_protocol::codes::LOGIN_USER_CODE;
    use iggy_binary_protocol::requests::users::LoginUserRequest;
    use iggy_binary_protocol::{RequestFrame, ResponseFrame, STATUS_OK, WireEncode, WireName};
    use iggy_common::{
        GlobalPermissions, Permissions, UserInfoDetails, UserStatus, calculate_checksum,
    };
    use rustls::ClientConfig;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, ServerName};
    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;
    use std::env;
    use std::fs::{self, OpenOptions};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::time::{Instant, sleep, timeout_at};

    pub const ROOT_USERNAME: &str = "root-v07";
    pub const ROOT_PASSWORD: &str = "root-secret-v07";
    pub const ADMIN_USERNAME: &str = "admin-v07";
    pub const ADMIN_PASSWORD: &str = "admin-secret-v07";
    pub const RUNTIME_USERNAME: &str = "runtime-v07";
    pub const RUNTIME_PASSWORD: &str = "runtime-secret-v07";
    pub const FIXTURE_RETENTION_BYTES: u64 = 2_u64 << 30;
    pub const FIXTURE_LEASE_MILLIS: u32 = 800;

    const CONFIGURATION_DOMAIN: &[u8] = b"ALOPEX-CHIRPS-CONFIGURATION-PROJECTION\0";
    const SECURITY_DOMAIN: &[u8] = b"ALOPEX-CHIRPS-SECURITY-PROJECTION\0";
    const CAPABILITY_DOMAIN: &[u8] = b"ALOPEX-CHIRPS-CAPABILITY-PROJECTION\0";
    const RESOURCE_WAL_MAGIC: &[u8; 8] = b"CHRWAL01";
    const RESOURCE_WAL_COMMIT_MARKER: u64 = 0x4348_5257_414c_434d;
    const RESOURCE_WAL_FRAME_SIZE: usize = 78;
    const SECURITY_WAL_MAGIC: &[u8; 8] = b"CHSECP01";
    const SECURITY_WAL_HEADER_SIZE: usize = 42;

    /// Bounded compatible-server termination evidence.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ServerStopReport {
        graceful: bool,
        forced: bool,
    }

    impl ServerStopReport {
        #[must_use]
        pub const fn graceful(self) -> bool {
            self.graceful
        }

        #[must_use]
        pub const fn forced(self) -> bool {
            self.forced
        }
    }

    /// Runner-verified compatible-server artifact identity.
    #[derive(Debug, Clone)]
    pub struct VerifiedArtifact {
        pub binary: PathBuf,
        pub kind: String,
        pub sha256: String,
        pub source_commit: String,
        pub source_tree: String,
        pub lane: String,
    }

    impl VerifiedArtifact {
        pub fn from_runner_environment() -> Result<Self> {
            let value = Self {
                binary: required_path("CHIRPS_SERVER_BINARY")?,
                kind: required("CHIRPS_ARTIFACT_KIND")?,
                sha256: required("CHIRPS_SERVER_SHA256")?,
                source_commit: required("CHIRPS_SERVER_SOURCE_COMMIT")?,
                source_tree: required("CHIRPS_SERVER_SOURCE_TREE")?,
                lane: required("CHIRPS_E2E_LANE")?,
            };
            ensure!(
                value.binary.is_file(),
                "server artifact is not a regular file"
            );
            ensure!(
                !fs::symlink_metadata(&value.binary)?
                    .file_type()
                    .is_symlink(),
                "server artifact must not be a symlink"
            );
            let actual = hex(&Sha256::digest(fs::read(&value.binary)?));
            ensure!(
                actual == value.sha256,
                "server artifact digest changed after runner verification"
            );
            ensure!(
                matches!(
                    (value.lane.as_str(), value.kind.as_str()),
                    ("production", "production") | ("fault", "publish-disabled-test")
                ),
                "runner lane and artifact kind disagree"
            );
            Ok(value)
        }

        pub fn build_sha(&self) -> Result<[u8; 20]> {
            decode_hex::<20>(&self.source_commit)
        }
    }

    /// Official-SDK responses retained as the independent resource fixture.
    pub struct ProvisionedFixture {
        pub stream_id: u32,
        pub topic_id: u32,
        pub partition_id: u32,
        pub retention_bytes: u64,
    }

    /// Creates the durable resource through the same public SDK available to users.
    pub async fn provision_durable_fixture(
        client: &IggyClient,
        stream_name: &str,
        topic_name: &str,
    ) -> Result<ProvisionedFixture> {
        let stream = client.create_stream(stream_name).await?;
        let topic = client
            .create_topic(
                &Identifier::numeric(stream.id)?,
                topic_name,
                1,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::from(FIXTURE_RETENTION_BYTES),
            )
            .await?;
        ensure!(
            topic.partitions_count == 1 && topic.partitions.len() == 1,
            "official SDK returned a different partition shape"
        );
        ensure!(
            topic.max_topic_size.as_bytes_u64() == FIXTURE_RETENTION_BYTES,
            "official SDK returned a different retention limit"
        );
        Ok(ProvisionedFixture {
            stream_id: stream.id,
            topic_id: topic.id,
            partition_id: topic.partitions[0].id,
            retention_bytes: topic.max_topic_size.as_bytes_u64(),
        })
    }

    /// Creates the management principal required by the production profile.
    pub async fn provision_production_admin(client: &IggyClient) -> Result<()> {
        client
            .create_user(
                ADMIN_USERNAME,
                ADMIN_PASSWORD,
                UserStatus::Active,
                Some(Permissions {
                    global: GlobalPermissions {
                        manage_servers: true,
                        manage_users: true,
                        manage_streams: true,
                        manage_topics: true,
                        ..GlobalPermissions::default()
                    },
                    streams: None,
                }),
            )
            .await?;
        Ok(())
    }

    /// Fixture-owned expected capability projection, independent of bind responses.
    #[derive(Debug, Clone, Copy)]
    pub struct FixtureIdentity {
        pub stream_id: u32,
        pub topic_id: u32,
        pub partition_id: u32,
        pub build_sha: [u8; 20],
        pub resource_id: [u8; 16],
        pub resource_epoch: u64,
        pub retention_bytes: u64,
        pub retention_messages: u64,
        pub checksum_enabled: bool,
        pub configuration_digest: [u8; 32],
        pub security_digest: [u8; 32],
        pub capability_digest: [u8; 32],
    }

    impl FixtureIdentity {
        pub fn assert_report(self, report: CapabilityReport) -> Result<()> {
            let location = report.location();
            let resource = location.resource_epoch();
            ensure!(report.build_sha() == &self.build_sha, "build SHA drifted");
            ensure!(
                location.stream_id() == self.stream_id
                    && location.topic_id() == self.topic_id
                    && location.partition_id() == self.partition_id,
                "resource location drifted"
            );
            ensure!(
                resource.resource_id().as_bytes() == &self.resource_id
                    && resource.epoch() == self.resource_epoch,
                "resource identity drifted"
            );
            ensure!(
                report.retention_bytes() == self.retention_bytes
                    && report.retention_messages() == self.retention_messages
                    && (report.checksum_mode()
                        == alopex_chirps_backend_iggy::protocol::ChecksumMode::Enabled)
                        == self.checksum_enabled,
                "retention/checksum projection drifted"
            );
            ensure!(
                report.configuration_digest() == &self.configuration_digest
                    && report.security_digest() == &self.security_digest
                    && report.capability_digest() == &self.capability_digest,
                "configuration/security/capability projection drifted"
            );
            Ok(())
        }

        #[must_use]
        pub fn with_mismatched_capability(mut self) -> Self {
            self.capability_digest[0] ^= 1;
            self
        }
    }

    /// Connects the public facade with the verification observer installed
    /// before any send can cross the real append boundary.
    pub async fn connect_fixture_with_observer<P>(
        server: &ServerProcess,
        fixture: FixtureIdentity,
        checkpoint_root: &Path,
        source: NodeId,
        credential_reference: &str,
        credentials: &P,
        observer: Arc<dyn AppendVerificationObserver>,
    ) -> Result<DurableHandle, DurableBuildError>
    where
        P: DurableCredentialProvider + ?Sized,
    {
        let projection = DurablePartitionProjection::new(
            fixture.partition_id,
            fixture.resource_id,
            fixture.resource_epoch,
            fixture.build_sha,
            fixture.retention_bytes,
            fixture.retention_messages,
            fixture.checksum_enabled,
            fixture.configuration_digest,
            fixture.security_digest,
            fixture.capability_digest,
        );
        let config = DurableConfig::new(
            server.address(),
            DurableTlsConfig::new("localhost".to_owned(), vec![server.certificate_der()]),
            credential_reference.to_owned(),
            DurableProfile::OsSyncedAccepted,
            DurableRoutingConfig::new(1, 1),
            DurableResourceConfig::new(fixture.stream_id, fixture.topic_id, vec![projection]),
            DurableCheckpointConfig::new(
                checkpoint_root.to_path_buf(),
                1,
                DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
            ),
            DurableLeaseConfig::new(
                FIXTURE_LEASE_MILLIS,
                Duration::from_millis(u64::from(FIXTURE_LEASE_MILLIS) / 4),
            ),
            DurableExtensionConfig::required(1024 * 1024),
        );
        DurableBuilder::new(source)
            .inbox_generation(1)
            .explicit_partitions(1)
            .connect_with_append_observer(
                config,
                credentials,
                Instant::now() + Duration::from_secs(10),
                observer,
            )
            .await
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct PersistedIdentity {
        resource_id: [u8; 16],
        resource_epoch: u64,
        security_epoch: u64,
    }

    /// Append-only, run-local evidence carrying the immutable artifact identity.
    pub struct EvidenceSink {
        file: Option<std::fs::File>,
        artifact: VerifiedArtifact,
        target: &'static str,
    }

    impl EvidenceSink {
        pub fn new(artifact: VerifiedArtifact) -> Result<Self> {
            Self::for_target(artifact, "durable_session")
        }

        pub fn for_target(artifact: VerifiedArtifact, target: &'static str) -> Result<Self> {
            ensure!(!target.is_empty(), "E2E evidence target is empty");
            let file = env::var_os("CHIRPS_E2E_EVIDENCE")
                .map(|path| {
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                        .context("open E2E evidence")
                })
                .transpose()?;
            Ok(Self {
                file,
                artifact,
                target,
            })
        }

        pub fn record(&mut self, scenario: &str, verdict: &str) -> Result<()> {
            let row = format!(
                "{{\"target\":\"{}\",\"lane\":\"{}\",\"artifact_kind\":\"{}\",\"artifact_sha256\":\"{}\",\"source_commit\":\"{}\",\"source_tree\":\"{}\",\"scenario\":\"{}\",\"verdict\":\"{}\"}}\n",
                self.target,
                self.artifact.lane,
                self.artifact.kind,
                self.artifact.sha256,
                self.artifact.source_commit,
                self.artifact.source_tree,
                scenario,
                verdict,
            );
            print!("{row}");
            if let Some(file) = self.file.as_mut() {
                file.write_all(row.as_bytes())?;
                file.sync_data()?;
            }
            Ok(())
        }

        pub fn record_binding(&mut self, report: CapabilityReport) -> Result<()> {
            let location = report.location();
            let resource = location.resource_epoch();
            let row = format!(
                "{{\"target\":\"{}\",\"lane\":\"{}\",\"artifact_sha256\":\"{}\",\"scenario\":\"capability-bind\",\"build_sha\":\"{}\",\"boot_id\":\"{}\",\"resource_id\":\"{}\",\"resource_epoch\":{},\"stream_id\":{},\"topic_id\":{},\"partition_id\":{},\"retention_bytes\":{},\"retention_messages\":{},\"configuration_digest\":\"{}\",\"security_digest\":\"{}\",\"capability_digest\":\"{}\"}}\n",
                self.target,
                self.artifact.lane,
                self.artifact.sha256,
                hex(report.build_sha()),
                hex(report.boot_id()),
                hex(resource.resource_id().as_bytes()),
                resource.epoch(),
                location.stream_id(),
                location.topic_id(),
                location.partition_id(),
                report.retention_bytes(),
                report.retention_messages(),
                hex(report.configuration_digest()),
                hex(report.security_digest()),
                hex(report.capability_digest()),
            );
            print!("{row}");
            if let Some(file) = self.file.as_mut() {
                file.write_all(row.as_bytes())?;
                file.sync_data()?;
            }
            Ok(())
        }
    }

    /// Verification-only bridge from the public Durable append boundary to
    /// the independent fsynced oracle.
    pub struct OracleAppendObserver {
        proxy: Mutex<FaultProxy>,
        pending: Mutex<HashMap<[u8; 16], OracleAttemptId>>,
        failed: AtomicBool,
    }

    impl OracleAppendObserver {
        #[must_use]
        pub fn new(store: OracleStore) -> Self {
            Self {
                proxy: Mutex::new(FaultProxy::new(store)),
                pending: Mutex::new(HashMap::new()),
                failed: AtomicBool::new(false),
            }
        }

        #[must_use]
        pub fn failed(&self) -> bool {
            self.failed.load(Ordering::Acquire)
        }
    }

    /// Exact identities proven by one fsynced oracle intent and its real
    /// append-admission callback.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct OracleAppendEvidence {
        pub attempt_id: String,
        pub message_id: String,
        pub envelope_digest: String,
        pub session_fingerprint: String,
        pub requested_boundary: ConfirmationBoundary,
    }

    impl OracleAppendEvidence {
        #[must_use]
        pub fn matches_prepared(&self, prepared: &PreparedDurableSend) -> bool {
            self.message_id == hex(prepared.message_id().as_bytes())
                && self.envelope_digest == hex(prepared.envelope_digest().as_bytes())
        }
    }

    pub fn read_oracle_append_evidence(store: &OracleStore) -> Result<Vec<OracleAppendEvidence>> {
        let records = store.load()?;
        let mut intents = Vec::new();
        let mut admitted = HashMap::<[u8; 16], OracleObservation>::new();
        for record in &records {
            match record {
                OracleRecord::Intent(intent) => intents.push(intent),
                OracleRecord::Observation(observation) => {
                    ensure!(
                        matches!(observation.kind(), ObservationKind::Wire(_)),
                        "append observer persisted a non-wire observation"
                    );
                    ensure!(
                        admitted
                            .insert(observation.attempt_id().as_bytes(), observation.clone())
                            .is_none(),
                        "oracle contains duplicate append invocation evidence"
                    );
                }
            }
        }
        ensure!(
            records.len() == intents.len() * 2 && admitted.len() == intents.len(),
            "oracle must contain exactly one intent and invocation per attempt"
        );
        let mut evidence = Vec::with_capacity(intents.len());
        for intent in intents {
            let attempt = intent.attempt_id().as_bytes();
            let observation = admitted
                .remove(&attempt)
                .context("oracle intent/invocation correlation is incomplete")?;
            // This reader proves only the admission prefix. The E2E case checks
            // the real terminal result separately, so close the verifier with
            // its weakest legal post-invocation outcome without claiming receipt.
            let validation_response = OracleObservation::response(
                intent.attempt_id(),
                ResponseObservation::new(
                    DurableSendOutcome::Indeterminate(AttemptFailureKind::Transport),
                    None,
                ),
            );
            let verdict = verify_attempt(intent, &[observation, validation_response])
                .map_err(|error| anyhow!("oracle rejected append evidence: {error:?}"))?;
            ensure!(
                verdict.append_count() == 1 && !verdict.exact_record_proven(),
                "oracle append-admission verdict has an invalid cardinality"
            );
            evidence.push(OracleAppendEvidence {
                attempt_id: hex(&attempt),
                message_id: hex(&intent.message_id().as_bytes()),
                envelope_digest: hex(intent.envelope_digest().as_bytes()),
                session_fingerprint: hex(intent.session_fingerprint().as_bytes()),
                requested_boundary: intent.requested_boundary(),
            });
        }
        ensure!(admitted.is_empty(), "oracle contains an orphan invocation");
        Ok(evidence)
    }

    impl AppendVerificationObserver for OracleAppendObserver {
        fn before_append(
            &self,
            prepared: &PreparedDurableSend,
            binding: &AttemptBinding,
        ) -> Result<(), AppendVerificationError> {
            let intent = OracleIntent::from_started_attempt(prepared, binding)
                .map_err(|_| AppendVerificationError)?;
            let attempt = binding.attempt_id().ok_or(AppendVerificationError)?;
            let mut pending = self.pending.lock().map_err(|_| AppendVerificationError)?;
            if pending.contains_key(attempt.as_bytes()) {
                return Err(AppendVerificationError);
            }
            self.proxy
                .lock()
                .map_err(|_| AppendVerificationError)?
                .append_after_intent(&intent, || ())
                .map_err(|_| AppendVerificationError)?;
            pending.insert(*attempt.as_bytes(), intent.attempt_id());
            Ok(())
        }

        fn append_admitted(&self, _prepared: &PreparedDurableSend, binding: &AttemptBinding) {
            let observed = (|| {
                let attempt = binding.attempt_id()?;
                let mut pending = self.pending.lock().ok()?;
                let oracle_attempt = pending.remove(attempt.as_bytes())?;
                let observation = OracleObservation::wire(
                    oracle_attempt,
                    WireObservation::new(
                        APPEND_ONE_SYNCED_CODE,
                        WireStage::AppendInvocation,
                        1,
                        1,
                        1,
                        1,
                    ),
                );
                self.proxy.lock().ok()?.observe(&observation).ok()
            })();
            if observed.is_none() {
                self.failed.store(true, Ordering::Release);
            }
        }
    }

    const OBSERVATION_DOMAIN: &[u8] = b"iggy-chirps-observation-v1\0";
    const OBSERVATION_HEAD_MAGIC: &[u8; 8] = b"CHOBSH01";
    const OBSERVATION_HEAD_PREFIX_LEN: usize = 8 + 2 + 8 + 32;
    const OBSERVATION_HEAD_LEN: usize = OBSERVATION_HEAD_PREFIX_LEN + 32;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub enum AppendObservationStage {
        WireWrite,
        JournalFlush,
        MessageSync,
        IndexSync,
        Response,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct AppendObservationRecord {
        pub version: u16,
        pub sequence: u64,
        pub command_code: u32,
        pub stage: AppendObservationStage,
        pub attempt_id: String,
        pub message_id: String,
        pub envelope_digest: String,
        pub resource_id: String,
        pub resource_epoch: u64,
        pub partition_id: u32,
        pub assigned_offset: Option<u64>,
        pub assigned_index: Option<u64>,
        pub wire_count: u32,
        pub command_count: u32,
        pub record_count: u32,
        pub payload_count: u32,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct PersistedAppendObservation {
        previous_hash: String,
        record_hash: String,
        value: AppendObservationRecord,
    }

    pub fn read_append_observations(path: &Path) -> Result<Vec<AppendObservationRecord>> {
        let metadata =
            fs::symlink_metadata(path).context("inspect compatible-server observation log")?;
        ensure!(
            metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
            "observation log must be a non-symlink regular file"
        );
        let bytes = fs::read(path).context("read compatible-server observation log")?;
        ensure!(
            bytes.is_empty() || bytes.ends_with(b"\n"),
            "observation log has a partial final record"
        );
        let mut head = [0_u8; 32];
        let mut records = Vec::new();
        if !bytes.is_empty() {
            for (index, line) in bytes[..bytes.len() - 1]
                .split(|byte| *byte == b'\n')
                .enumerate()
            {
                ensure!(!line.is_empty(), "observation log contains an empty record");
                let persisted: PersistedAppendObservation = serde_json::from_slice(line)?;
                let expected_sequence = u64::try_from(index)? + 1;
                let value = &persisted.value;
                ensure!(
                    value.version == 1
                        && value.sequence == expected_sequence
                        && value.command_code == APPEND_ONE_SYNCED_CODE
                        && value.attempt_id.len() == 32
                        && value.message_id.len() == 32
                        && value.envelope_digest.len() == 64
                        && value.resource_id.len() == 32
                        && value.wire_count == 1
                        && value.command_count == 1
                        && value.record_count == 1
                        && value.payload_count == 1,
                    "observation record violates the exact append schema"
                );
                decode_hex::<16>(&value.attempt_id)?;
                decode_hex::<16>(&value.message_id)?;
                decode_hex::<32>(&value.envelope_digest)?;
                decode_hex::<16>(&value.resource_id)?;
                ensure!(
                    matches!(value.stage, AppendObservationStage::WireWrite)
                        == value.assigned_offset.is_none()
                        && value.assigned_offset.is_some() == value.assigned_index.is_some(),
                    "observation location disagrees with its stage"
                );
                ensure!(
                    decode_hex::<32>(&persisted.previous_hash)? == head,
                    "observation hash chain is discontinuous"
                );
                let mut hash_input = Vec::new();
                hash_input.extend_from_slice(OBSERVATION_DOMAIN);
                hash_input.extend_from_slice(&head);
                hash_input.extend_from_slice(&serde_json::to_vec(value)?);
                head = Sha256::digest(hash_input).into();
                ensure!(
                    decode_hex::<32>(&persisted.record_hash)? == head,
                    "observation record hash is invalid"
                );
                records.push(persisted.value);
            }
        }
        let mut head_path = path.as_os_str().to_os_string();
        head_path.push(".head");
        let head_path = PathBuf::from(head_path);
        let mut head_temp_path = path.as_os_str().to_os_string();
        head_temp_path.push(".head.tmp");
        ensure!(
            !PathBuf::from(head_temp_path).exists(),
            "observation head installation is incomplete"
        );
        let metadata = fs::symlink_metadata(&head_path)
            .context("inspect compatible-server observation head")?;
        ensure!(
            metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
            "observation head must be a non-symlink regular file"
        );
        let persisted_head = fs::read(&head_path)?;
        ensure!(
            persisted_head.len() == OBSERVATION_HEAD_LEN
                && &persisted_head[..8] == OBSERVATION_HEAD_MAGIC,
            "observation head has an invalid envelope"
        );
        ensure!(
            u16::from_le_bytes(persisted_head[8..10].try_into()?) == 1
                && u64::from_le_bytes(persisted_head[10..18].try_into()?)
                    == u64::try_from(records.len())?
                && persisted_head[18..50] == head
                && persisted_head[OBSERVATION_HEAD_PREFIX_LEN..]
                    == Sha256::digest(&persisted_head[..OBSERVATION_HEAD_PREFIX_LEN])[..],
            "observation log/head frontier is incomplete or corrupt"
        );
        Ok(records)
    }

    /// One real compatible-server process with a persistent state directory.
    pub struct ServerProcess {
        root: TempDir,
        address: SocketAddr,
        certificate_der: Vec<u8>,
        certificate_path: PathBuf,
        configuration_path: PathBuf,
        private_key_path: PathBuf,
        stderr_path: PathBuf,
        binary: PathBuf,
        child: Option<Child>,
    }

    impl ServerProcess {
        pub fn new(binary: PathBuf) -> Result<Self> {
            let root = tempfile::tempdir().context("create server fixture")?;
            let certificate = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
            let certificate_der = certificate.serialize_der()?;
            let certificate_path = root.path().join("server-cert.pem");
            let private_key_path = root.path().join("server-key.pem");
            let stderr_path = root.path().join("server.stderr");
            let configuration_path = root.path().join("server-config.toml");
            fs::write(
                &configuration_path,
                b"[system.message_deduplication]\nenabled = false\n",
            )?;
            fs::write(&certificate_path, certificate.serialize_pem()?)?;
            fs::write(&private_key_path, certificate.serialize_private_key_pem())?;
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let address = listener.local_addr()?;
            drop(listener);
            Ok(Self {
                root,
                address,
                certificate_der,
                certificate_path,
                configuration_path,
                private_key_path,
                stderr_path,
                binary,
                child: None,
            })
        }

        #[must_use]
        pub const fn address(&self) -> SocketAddr {
            self.address
        }

        #[must_use]
        pub fn certificate_der(&self) -> Vec<u8> {
            self.certificate_der.clone()
        }

        #[must_use]
        pub fn certificate_path(&self) -> &Path {
            &self.certificate_path
        }

        /// Exact file passed to the compatible server as its startup configuration.
        #[must_use]
        pub fn configuration_path(&self) -> &Path {
            &self.configuration_path
        }

        /// Returns the owned child ID only while that child is still running.
        pub fn running_process_id(&mut self) -> Result<u32> {
            let child = self.child.as_mut().context("server is not running")?;
            ensure!(child.try_wait()?.is_none(), "server process exited");
            Ok(child.id())
        }

        #[must_use]
        pub fn state_path(&self) -> PathBuf {
            self.root.path().join("state")
        }

        /// Reads the stopped initial fixture and independently projects the bind report.
        pub fn read_fixture_projection(
            &self,
            artifact: &VerifiedArtifact,
            provisioned: &ProvisionedFixture,
            runtime_user: &UserInfoDetails,
            expected_runtime_username: &str,
            expected_runtime_status: UserStatus,
            expected_runtime_permissions: &Permissions,
        ) -> Result<FixtureIdentity> {
            ensure!(
                self.child.is_none(),
                "fixture identity must be read from a stopped server"
            );
            ensure!(
                runtime_user.username == expected_runtime_username,
                "runtime principal username differs from the provisioned fixture"
            );
            ensure!(
                runtime_user.status == expected_runtime_status,
                "runtime principal status differs from the provisioned fixture"
            );
            ensure!(
                runtime_user.permissions.as_ref() == Some(expected_runtime_permissions),
                "runtime principal permissions differ from the provisioned fixture"
            );
            let persisted = read_initial_persisted_identity(&self.state_path())?;
            let digests = fixture_projection_digests(
                provisioned,
                runtime_user.id,
                expected_runtime_status,
                Some(expected_runtime_permissions),
                persisted.security_epoch,
            )?;
            Ok(FixtureIdentity {
                stream_id: provisioned.stream_id,
                topic_id: provisioned.topic_id,
                partition_id: provisioned.partition_id,
                build_sha: artifact.build_sha()?,
                resource_id: persisted.resource_id,
                resource_epoch: persisted.resource_epoch,
                retention_bytes: provisioned.retention_bytes,
                retention_messages: 0,
                checksum_enabled: true,
                configuration_digest: digests.configuration,
                security_digest: digests.security,
                capability_digest: digests.capability,
            })
        }

        pub async fn start(&mut self, production: bool, failpoint: Option<&str>) -> Result<()> {
            self.start_with_observation(production, failpoint, None)
                .await
        }

        pub async fn start_with_observation(
            &mut self,
            production: bool,
            failpoint: Option<&str>,
            observation_log: Option<&Path>,
        ) -> Result<()> {
            self.start_with_environment(production, failpoint, observation_log, &[])
                .await
        }

        /// Starts the fixture with the small production-safe configuration
        /// surface needed to exercise real retention cleanup.
        pub async fn start_with_environment(
            &mut self,
            production: bool,
            failpoint: Option<&str>,
            observation_log: Option<&Path>,
            environment: &[(&str, &str)],
        ) -> Result<()> {
            ensure!(self.child.is_none(), "server process is already running");
            for (name, _) in environment {
                ensure!(
                    matches!(
                        *name,
                        "IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED"
                            | "IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL"
                            | "IGGY_SYSTEM_SEGMENT_SIZE"
                    ),
                    "server fixture environment key is not allowlisted: {name}"
                );
            }
            fs::create_dir_all(self.state_path())?;
            let stderr = fs::File::create(&self.stderr_path)?;
            let mut command = Command::new(&self.binary);
            command
                .env_clear()
                .current_dir(self.root.path())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::from(stderr))
                .env("IGGY_CONFIG_PATH", &self.configuration_path)
                .env("IGGY_SYSTEM_PATH", self.state_path())
                .env("IGGY_TCP_ENABLED", "true")
                .env("IGGY_TCP_ADDRESS", self.address.to_string())
                .env("IGGY_TCP_TLS_ENABLED", "true")
                .env("IGGY_TCP_TLS_SELF_SIGNED", "false")
                .env("IGGY_TCP_TLS_CERT_FILE", &self.certificate_path)
                .env("IGGY_TCP_TLS_KEY_FILE", &self.private_key_path)
                .env("IGGY_HTTP_ENABLED", "false")
                .env("IGGY_QUIC_ENABLED", "false")
                .env("IGGY_WEBSOCKET_ENABLED", "false")
                .env("IGGY_TELEMETRY_ENABLED", "false")
                .env("IGGY_SYSTEM_LOGGING_FILE_ENABLED", "false")
                .env("IGGY_SYSTEM_MEMORY_POOL_ENABLED", "false")
                .env("IGGY_SYSTEM_STATE_ENFORCE_FSYNC", "true")
                .env("IGGY_SYSTEM_PARTITION_ENFORCE_FSYNC", "true")
                .env("IGGY_SYSTEM_PARTITION_VALIDATE_CHECKSUM", "true")
                .env("IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE", "1")
                .env(
                    "IGGY_SYSTEM_PARTITION_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE",
                    "1 B",
                )
                .env("IGGY_SYSTEM_MESSAGE_DEDUPLICATION_ENABLED", "false")
                .env("IGGY_ROOT_USERNAME", ROOT_USERNAME)
                .env("IGGY_ROOT_PASSWORD", ROOT_PASSWORD)
                .env("IGGY_ADMIN_USERNAME", ADMIN_USERNAME)
                .env("IGGY_ADMIN_PASSWORD", ADMIN_PASSWORD)
                .env("IGGY_RUNTIME_USERNAME", RUNTIME_USERNAME)
                .env("IGGY_RUNTIME_PASSWORD", RUNTIME_PASSWORD)
                .env("RUST_BACKTRACE", "1");
            if let Some(token) = env::var_os("CHIRPS_E2E_RUN_TOKEN") {
                command.env("CHIRPS_E2E_RUN_TOKEN", token);
            }
            if production {
                command.env("IGGY_CHIRPS_PRODUCTION_PROFILE", "true");
            } else {
                command.env_remove("IGGY_CHIRPS_PRODUCTION_PROFILE");
            }
            match failpoint {
                Some(value) => {
                    command.env("IGGY_CHIRPS_FAILPOINT", value);
                }
                None => {
                    command.env_remove("IGGY_CHIRPS_FAILPOINT");
                }
            }
            if let Some(path) = observation_log {
                command.env("IGGY_CHIRPS_OBSERVATION_LOG", path);
            }
            command.envs(environment.iter().copied());
            self.child = Some(command.spawn().context("spawn compatible server")?);
            self.wait_ready().await
        }

        async fn wait_ready(&mut self) -> Result<()> {
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                if let Some(status) = self.child.as_mut().expect("child exists").try_wait()? {
                    let stderr = fs::read_to_string(&self.stderr_path).unwrap_or_default();
                    return Err(anyhow!(
                        "compatible server exited before readiness: {status}: {stderr}"
                    ));
                }
                if let Ok(Ok(transport)) = timeout_at(
                    deadline,
                    OwnedTransport::connect_tls(
                        self.address,
                        ServerName::try_from("localhost".to_owned())?,
                        tls_client_config(self.certificate_der())?,
                        TransportLimits::new(1024 * 1024)?,
                    ),
                )
                .await
                {
                    let ready = match timeout_at(
                        deadline,
                        transport.login(login_frame(ROOT_USERNAME, ROOT_PASSWORD)?),
                    )
                    .await
                    {
                        Ok(Ok(bytes)) => validate_login_response(&bytes).is_ok(),
                        Err(_) => false,
                        Ok(Err(_)) => false,
                    };
                    let report = transport
                        .shutdown(Instant::now() + Duration::from_secs(1))
                        .await;
                    ensure!(
                        report.socket_close_requested()
                            && report.all_workers_joined()
                            && report.panicked_worker_count() == 0,
                        "readiness probe leaked transport workers"
                    );
                    if ready {
                        return Ok(());
                    }
                }
                ensure!(
                    Instant::now() < deadline,
                    "compatible server readiness timed out"
                );
                sleep(Duration::from_millis(25)).await;
            }
        }

        pub fn signal(&self, signal: &str) -> Result<()> {
            let pid = self.child.as_ref().context("server is not running")?.id();
            let status = Command::new("kill")
                .args([signal, &pid.to_string()])
                .status()?;
            ensure!(status.success(), "kill {signal} failed");
            Ok(())
        }

        /// Waits until Linux reports that the running fixture is stopped.
        pub async fn wait_stopped(&self, deadline: Instant) -> Result<()> {
            let pid = self.child.as_ref().context("server is not running")?.id();
            let status_path = PathBuf::from(format!("/proc/{pid}/status"));
            loop {
                let status = fs::read_to_string(&status_path)
                    .context("read compatible server process status")?;
                if status.lines().any(|line| {
                    line.strip_prefix("State:")
                        .and_then(|state| state.split_whitespace().next())
                        == Some("T")
                }) {
                    return Ok(());
                }
                ensure!(
                    Instant::now() < deadline,
                    "compatible server did not enter the stopped state"
                );
                sleep(Duration::from_millis(10)).await;
            }
        }

        pub async fn stop(&mut self, deadline: Instant) -> Result<ServerStopReport> {
            let Some(mut child) = self.child.take() else {
                return Ok(ServerStopReport {
                    graceful: true,
                    forced: false,
                });
            };
            if child.try_wait()?.is_some() {
                return Ok(ServerStopReport {
                    graceful: true,
                    forced: false,
                });
            }
            let pid = child.id();
            let status = Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()?;
            ensure!(
                status.success(),
                "graceful server termination request failed"
            );
            while Instant::now() < deadline {
                if child.try_wait()?.is_some() {
                    return Ok(ServerStopReport {
                        graceful: true,
                        forced: false,
                    });
                }
                sleep(Duration::from_millis(25)).await;
            }
            child.kill()?;
            let kill_deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < kill_deadline {
                if child.try_wait()?.is_some() {
                    return Ok(ServerStopReport {
                        graceful: false,
                        forced: true,
                    });
                }
                sleep(Duration::from_millis(10)).await;
            }
            self.child = Some(child);
            Err(anyhow!(
                "compatible server did not stop after kill fallback"
            ))
        }

        /// Forces one running fixture process down and reaps it by the deadline.
        pub async fn force_stop(&mut self, deadline: Instant) -> Result<ServerStopReport> {
            let Some(mut child) = self.child.take() else {
                return Err(anyhow!("server is not running"));
            };
            ensure!(
                child.try_wait()?.is_none(),
                "server exited before forced stop"
            );
            child.kill()?;
            while Instant::now() < deadline {
                if child.try_wait()?.is_some() {
                    return Ok(ServerStopReport {
                        graceful: false,
                        forced: true,
                    });
                }
                sleep(Duration::from_millis(10)).await;
            }
            self.child = Some(child);
            Err(anyhow!("compatible server did not exit after forced stop"))
        }
    }

    impl Drop for ServerProcess {
        fn drop(&mut self) {
            if let Some(child) = self.child.as_mut() {
                let _ = child.kill();
                let deadline = std::time::Instant::now() + Duration::from_secs(2);
                while std::time::Instant::now() < deadline {
                    if child.try_wait().ok().flatten().is_some() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct ProjectionDigests {
        configuration: [u8; 32],
        security: [u8; 32],
        capability: [u8; 32],
    }

    fn fixture_projection_digests(
        provisioned: &ProvisionedFixture,
        principal: u32,
        status: UserStatus,
        permissions: Option<&Permissions>,
        security_epoch: u64,
    ) -> Result<ProjectionDigests> {
        let mut configuration = Vec::with_capacity(96);
        configuration.extend_from_slice(CONFIGURATION_DOMAIN);
        configuration.extend_from_slice(&provisioned.stream_id.to_le_bytes());
        configuration.extend_from_slice(&provisioned.topic_id.to_le_bytes());
        configuration.extend_from_slice(&provisioned.partition_id.to_le_bytes());
        configuration.extend_from_slice(&provisioned.retention_bytes.to_le_bytes());
        configuration.extend_from_slice(&0_u64.to_le_bytes());
        configuration.push(1);
        configuration.extend_from_slice(&1_u32.to_le_bytes());
        configuration.extend_from_slice(&1_u64.to_le_bytes());
        configuration.push(1);

        let mut security = Vec::with_capacity(SECURITY_DOMAIN.len() + 13);
        security.extend_from_slice(SECURITY_DOMAIN);
        security.extend_from_slice(&principal.to_le_bytes());
        security.push(status.as_code());
        security.extend_from_slice(&security_epoch.to_le_bytes());

        let permission_bytes = rmp_serde::to_vec(&permissions)?;
        let mut capability = Vec::with_capacity(CAPABILITY_DOMAIN.len() + permission_bytes.len());
        capability.extend_from_slice(CAPABILITY_DOMAIN);
        capability.extend_from_slice(&permission_bytes);

        Ok(ProjectionDigests {
            configuration: Sha256::digest(configuration).into(),
            security: Sha256::digest(security).into(),
            capability: Sha256::digest(capability).into(),
        })
    }

    fn read_initial_persisted_identity(system_path: &Path) -> Result<PersistedIdentity> {
        let resource_directory = system_path.join("state/chirps-resource-identity");
        ensure_initial_files(
            &resource_directory,
            &["resource_identity.wal.1"],
            &["resource_identity.wal.", "resource_identity.snapshot."],
        )?;
        let resource = parse_initial_resource_wal(&read_exact_regular_file(
            &resource_directory.join("resource_identity.wal.1"),
            RESOURCE_WAL_FRAME_SIZE,
        )?)?;

        let security_directory = system_path.join("state/chirps-security-epoch");
        ensure_initial_files(
            &security_directory,
            &["security_epoch.wal.1"],
            &["security_epoch.wal."],
        )?;
        let security_epoch = parse_initial_security_wal(&read_exact_regular_file(
            &security_directory.join("security_epoch.wal.1"),
            SECURITY_WAL_HEADER_SIZE,
        )?)?;
        Ok(PersistedIdentity {
            resource_id: resource.0,
            resource_epoch: resource.1,
            security_epoch,
        })
    }

    fn ensure_initial_files(
        directory: &Path,
        expected: &[&str],
        owned_prefixes: &[&str],
    ) -> Result<()> {
        let mut actual = Vec::new();
        for entry in fs::read_dir(directory)
            .with_context(|| format!("read fixture directory {}", directory.display()))?
        {
            let entry = entry.with_context(|| {
                format!("read fixture directory entry in {}", directory.display())
            })?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow!("fixture directory contains a non-UTF-8 entry"))?;
            if owned_prefixes.iter().any(|prefix| name.starts_with(prefix)) {
                actual.push(name);
            }
        }
        actual.sort();
        ensure!(
            actual == expected,
            "initial fixture files differ: expected {expected:?}, got {actual:?}"
        );
        Ok(())
    }

    fn read_exact_regular_file(path: &Path, expected_len: usize) -> Result<Vec<u8>> {
        let before = fs::symlink_metadata(path)
            .with_context(|| format!("inspect fixture file {}", path.display()))?;
        ensure!(
            before.file_type().is_file() && !before.file_type().is_symlink(),
            "fixture path is not a non-symlink regular file: {}",
            path.display()
        );
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| format!("open fixture file {}", path.display()))?;
        let opened = file.metadata()?;
        ensure!(opened.is_file(), "opened fixture is not a regular file");
        ensure!(
            opened.len() == expected_len as u64,
            "fixture file has an unexpected size"
        );
        let after = fs::symlink_metadata(path)?;
        ensure!(
            after.file_type().is_file() && !after.file_type().is_symlink(),
            "fixture path changed while opening"
        );
        #[cfg(unix)]
        ensure!(
            before.dev() == opened.dev()
                && before.ino() == opened.ino()
                && after.dev() == opened.dev()
                && after.ino() == opened.ino(),
            "fixture path changed while opening"
        );

        let mut bytes = Vec::with_capacity(expected_len + 1);
        file.take(expected_len as u64 + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() == expected_len,
            "fixture file does not contain exactly {expected_len} bytes"
        );
        Ok(bytes)
    }

    fn parse_initial_resource_wal(bytes: &[u8]) -> Result<([u8; 16], u64)> {
        ensure!(
            bytes.len() == RESOURCE_WAL_FRAME_SIZE,
            "initial resource WAL must contain exactly one complete frame"
        );
        ensure!(&bytes[..8] == RESOURCE_WAL_MAGIC, "resource WAL magic");
        ensure!(read_u16(bytes, 8)? == 1, "resource WAL version");
        ensure!(read_u64(bytes, 10)? == 1, "resource WAL generation");
        ensure!(read_u64(bytes, 18)? == 1, "resource WAL sequence");
        ensure!(read_u32(bytes, 26)? == 32, "resource WAL identity size");
        ensure!(
            read_u64(bytes, 70)? == RESOURCE_WAL_COMMIT_MARKER,
            "resource WAL commit marker"
        );
        ensure!(
            read_u64(bytes, 62)? == calculate_checksum(&bytes[8..62]),
            "resource WAL checksum"
        );
        let resource_id: [u8; 16] = bytes[30..46].try_into()?;
        ensure!(
            resource_id.iter().any(|byte| *byte != 0)
                && resource_id[6] & 0xf0 == 0x40
                && resource_id[8] & 0xc0 == 0x80,
            "resource WAL UUID is not a non-nil RFC4122 v4 UUID"
        );
        let resource_epoch = read_u64(bytes, 46)?;
        ensure!(resource_epoch == 1, "initial resource epoch");
        ensure!(read_u64(bytes, 54)? == 1, "initial lifecycle generation");
        Ok((resource_id, resource_epoch))
    }

    fn parse_initial_security_wal(bytes: &[u8]) -> Result<u64> {
        ensure!(
            bytes.len() == SECURITY_WAL_HEADER_SIZE,
            "initial security WAL must contain only its authenticated header"
        );
        ensure!(&bytes[..8] == SECURITY_WAL_MAGIC, "security WAL magic");
        ensure!(read_u16(bytes, 8)? == 1, "security WAL version");
        let expected_digest: [u8; 32] = Sha256::digest(&bytes[..10]).into();
        ensure!(
            bytes[10..42] == expected_digest,
            "security WAL header digest"
        );
        Ok(1)
    }

    fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
        Ok(u16::from_le_bytes(bytes[offset..offset + 2].try_into()?))
    }

    fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
        Ok(u32::from_le_bytes(bytes[offset..offset + 4].try_into()?))
    }

    fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
        Ok(u64::from_le_bytes(bytes[offset..offset + 8].try_into()?))
    }

    /// A raw authenticated session used only to obtain the server-owned projection.
    pub struct RealSession {
        transport: Option<OwnedTransport>,
        report: CapabilityReport,
        binding: SessionBinding,
    }

    impl RealSession {
        #[allow(clippy::too_many_arguments)]
        pub async fn bind(
            address: SocketAddr,
            certificate_der: Vec<u8>,
            username: &str,
            password: &str,
            stream_id: u32,
            topic_id: u32,
            partition_id: u32,
            lease_millis: u32,
        ) -> Result<Self> {
            let transport = OwnedTransport::connect_tls(
                address,
                ServerName::try_from("localhost".to_owned())?,
                tls_client_config(certificate_der)?,
                TransportLimits::new(1024 * 1024)?,
            )
            .await?;
            let login = transport.login(login_frame(username, password)?).await?;
            validate_login_response(&login)?;
            let request = PrivateRequest::CapabilityBind(CapabilityBindRequest::new(
                stream_id,
                topic_id,
                partition_id,
                lease_millis,
            )?);
            let response = transport
                .session_control(SessionControlRequestFrame::from_private(&request)?)
                .await?;
            let VerifiedPrivateResponse::CapabilityBind(response) =
                PrivateResponse::decode_for_request(&response, &request)?
            else {
                return Err(anyhow!("server returned a different private response"));
            };
            Ok(Self {
                transport: Some(transport),
                report: response.report(),
                binding: response.binding(),
            })
        }

        #[must_use]
        pub const fn report(&self) -> CapabilityReport {
            self.report
        }

        pub async fn checked_poll(&self, expected_offset: u64) -> Result<PollObservation> {
            let transport = self
                .transport
                .as_ref()
                .context("raw session transport is already shut down")?;
            let request = PrivateRequest::CheckedPoll(CheckedPollRequest::new(
                self.binding,
                self.report.location(),
                expected_offset,
            ));
            let response = transport
                .invoke(DataPlaneRequestFrame::from_private(&request)?)
                .await?;
            let VerifiedPrivateResponse::CheckedPoll(response) =
                PrivateResponse::decode_for_request(&response, &request)?
            else {
                return Err(anyhow!("server returned a different private response"));
            };
            Ok(response.into_observation())
        }

        pub async fn shutdown(&mut self, deadline: Instant) -> Result<TransportShutdownReport> {
            let transport = self
                .transport
                .take()
                .context("raw session transport was shut down twice")?;
            let report = transport.shutdown(deadline).await;
            ensure!(
                report.socket_close_requested() && report.all_workers_joined(),
                "raw transport shutdown did not close and join every worker"
            );
            ensure!(
                report.panicked_worker_count() == 0,
                "raw transport worker panicked during shutdown"
            );
            Ok(report)
        }
    }

    fn tls_client_config(certificate_der: Vec<u8>) -> Result<Arc<ClientConfig>> {
        let mut roots = RootCertStore::empty();
        roots.add(CertificateDer::from(certificate_der))?;
        Ok(Arc::new(
            ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth(),
        ))
    }

    fn login_frame(username: &str, password: &str) -> Result<LoginRequestFrame> {
        let payload = LoginUserRequest {
            username: WireName::new(username)?,
            password: password.to_owned(),
            version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            context: Some(String::new()),
        }
        .to_bytes();
        let mut bytes = BytesMut::new();
        RequestFrame::encode(LOGIN_USER_CODE, &payload, &mut bytes)?;
        Ok(LoginRequestFrame::try_from(bytes.freeze())?)
    }

    fn validate_login_response(bytes: &[u8]) -> Result<()> {
        let (frame, consumed) = ResponseFrame::decode(bytes).context("decode login response")?;
        ensure!(
            consumed == bytes.len(),
            "login response contains trailing bytes"
        );
        ensure!(
            frame.status == STATUS_OK,
            "login response rejected credentials"
        );
        Ok(())
    }

    fn required(name: &'static str) -> Result<String> {
        env::var(name).with_context(|| format!("runner did not set {name}"))
    }

    fn required_path(name: &'static str) -> Result<PathBuf> {
        env::var_os(name)
            .map(PathBuf::from)
            .with_context(|| format!("runner did not set {name}"))
    }

    fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N]> {
        ensure!(value.len() == N * 2, "invalid fixed-width hex value");
        let mut output = [0; N];
        for (index, byte) in output.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
        }
        Ok(output)
    }

    fn hex(bytes: &[u8]) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(DIGITS[(byte >> 4) as usize] as char);
            output.push(DIGITS[(byte & 0x0f) as usize] as char);
        }
        output
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn fixture_reports_only_its_live_owned_child() -> Result<()> {
            if env::var_os("CHIRPS_FIXTURE_CHILD_WAIT").is_some() {
                std::thread::sleep(Duration::from_secs(2));
                return Ok(());
            }
            let executable = env::current_exe()?;
            let mut server = ServerProcess::new(executable.clone())?;
            ensure!(
                server.running_process_id().is_err(),
                "unstarted fixture has a PID"
            );
            ensure!(
                server.configuration_path().is_file(),
                "startup file is absent"
            );
            let child = Command::new(executable)
                .args([
                    "--exact",
                    "v07::tests::fixture_reports_only_its_live_owned_child",
                ])
                .env("CHIRPS_FIXTURE_CHILD_WAIT", "1")
                .stdout(Stdio::null())
                .spawn()?;
            let expected = child.id();
            server.child = Some(child);
            ensure!(
                server.running_process_id()? == expected,
                "fixture returned another PID"
            );
            server.child.as_mut().expect("child assigned").wait()?;
            ensure!(
                server.running_process_id().is_err(),
                "exited child remains measurable"
            );
            Ok(())
        }

        #[test]
        fn initial_wal_parsers_authenticate_the_single_fixture_state() -> Result<()> {
            let resource_id = [
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x46, 0x17, 0x98, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
                0x1e, 0x1f,
            ];
            let mut resource = vec![0_u8; RESOURCE_WAL_FRAME_SIZE];
            resource[..8].copy_from_slice(RESOURCE_WAL_MAGIC);
            resource[8..10].copy_from_slice(&1_u16.to_le_bytes());
            resource[10..18].copy_from_slice(&1_u64.to_le_bytes());
            resource[18..26].copy_from_slice(&1_u64.to_le_bytes());
            resource[26..30].copy_from_slice(&32_u32.to_le_bytes());
            resource[30..46].copy_from_slice(&resource_id);
            resource[46..54].copy_from_slice(&1_u64.to_le_bytes());
            resource[54..62].copy_from_slice(&1_u64.to_le_bytes());
            let checksum = calculate_checksum(&resource[8..62]);
            resource[62..70].copy_from_slice(&checksum.to_le_bytes());
            resource[70..78].copy_from_slice(&RESOURCE_WAL_COMMIT_MARKER.to_le_bytes());
            assert_eq!(parse_initial_resource_wal(&resource)?, (resource_id, 1));
            resource[62] ^= 1;
            assert!(parse_initial_resource_wal(&resource).is_err());

            let mut security = vec![0_u8; SECURITY_WAL_HEADER_SIZE];
            security[..8].copy_from_slice(SECURITY_WAL_MAGIC);
            security[8..10].copy_from_slice(&1_u16.to_le_bytes());
            let digest = Sha256::digest(&security[..10]);
            security[10..42].copy_from_slice(&digest);
            assert_eq!(parse_initial_security_wal(&security)?, 1);
            security.push(0);
            assert!(parse_initial_security_wal(&security).is_err());
            Ok(())
        }

        #[test]
        fn fixture_reader_is_exact_and_does_not_follow_symlinks() -> Result<()> {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("fixture.wal");
            fs::write(&path, [1_u8, 2, 3])?;
            assert_eq!(read_exact_regular_file(&path, 3)?, [1, 2, 3]);
            assert!(read_exact_regular_file(&path, 2).is_err());

            #[cfg(unix)]
            {
                use std::os::unix::fs::symlink;

                let link = directory.path().join("fixture-link.wal");
                symlink(&path, &link)?;
                assert!(read_exact_regular_file(&link, 3).is_err());
            }
            Ok(())
        }

        #[test]
        fn fixture_digests_bind_resource_principal_and_capability() -> Result<()> {
            let provisioned = ProvisionedFixture {
                stream_id: 1,
                topic_id: 1,
                partition_id: 0,
                retention_bytes: FIXTURE_RETENTION_BYTES,
            };
            let permissions = Permissions::default();
            let baseline = fixture_projection_digests(
                &provisioned,
                3,
                UserStatus::Active,
                Some(&permissions),
                1,
            )?;
            assert_eq!(
                baseline,
                fixture_projection_digests(
                    &provisioned,
                    3,
                    UserStatus::Active,
                    Some(&permissions),
                    1,
                )?
            );
            let changed_principal = fixture_projection_digests(
                &provisioned,
                4,
                UserStatus::Active,
                Some(&permissions),
                1,
            )?;
            assert_ne!(baseline.security, changed_principal.security);
            let changed_capability =
                fixture_projection_digests(&provisioned, 3, UserStatus::Active, None, 1)?;
            assert_ne!(baseline.capability, changed_capability.capability);
            Ok(())
        }
    }
}
