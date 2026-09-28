//! Publish-disabled performance evidence tool.

mod evidence;

use alopex_chirps::NodeId;
use alopex_chirps::durable::{
    DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES, DurableBuilder, DurableCheckpointConfig, DurableConfig,
    DurableCredential, DurableCredentialProvider, DurableCredentialProviderError,
    DurableExtensionConfig, DurableHandle, DurableLeaseConfig, DurablePartitionProjection,
    DurableProfile, DurableResourceConfig, DurableRoutingConfig, DurableTlsConfig,
};
use alopex_chirps_core::durable::{ConfirmationBoundary, DurableSendOutcome};
use anyhow::{Context, Result};
use evidence::{
    AA_SCHEMA, AaArtifact, Arm, BoundsArtifact, ComparableAxes, FREEZE_SCHEMA, FreezeRecord,
    FullConfirmationProfile, LatencyVector, MetricVector, RawObservation, SAFETY_CONTROLS,
    SafetyArtifact, SafetyControl, SafetyObservation, build_bounds, build_paired, build_safety,
    validate_bounds, validate_freeze,
};
use iggy::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::task::{JoinSet, LocalSet};
use tokio::time::Instant;

#[derive(Clone, Copy)]
enum Mode {
    Aa,
    SafetyAblation,
    Paired,
}

struct Cli {
    mode: Mode,
    candidate_manifest: PathBuf,
    bounds: Option<PathBuf>,
    output: PathBuf,
}

#[derive(Deserialize)]
struct CandidateManifest {
    performance: PerformancePlan,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PerformancePlan {
    axes: ComparableAxes,
    samples: u64,
    workload: Workload,
    probe: AuditProbe,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuditProbe {
    program: PathBuf,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Workload {
    endpoint: SocketAddr,
    tls_server_name: String,
    tls_ca_pem_path: PathBuf,
    tls_ca_pem_sha256: String,
    tls_root_der_path: PathBuf,
    tls_root_der_sha256: String,
    credential_reference: String,
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
    source_node_id_hex: String,
    target_node_id_hex: String,
    inbox_generation: u64,
    lifecycle_generation: u64,
    checkpoint_root: PathBuf,
    lease_millis: u32,
    renew_interval_millis: u64,
    max_frame_len: usize,
    resource_id_hex: String,
    resource_epoch: u64,
    build_sha_hex: String,
    retention_bytes: u64,
    retention_messages: u64,
    checksum_enabled: bool,
    configuration_digest_hex: String,
    security_digest_hex: String,
    capability_digest_hex: String,
    ordering_key_hex: String,
    payload_path: PathBuf,
    payload_sha256: String,
    operation_timeout_millis: u64,
    connect_timeout_millis: u64,
    #[serde(default)]
    broker_startup_config: Option<DigestFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DigestFile {
    path: PathBuf,
    sha256: String,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SecretCredential {
    UsernamePassword { username: String, password: String },
    PersonalAccessToken { token: String },
}

struct EnvironmentCredentialProvider {
    reference: String,
    credential: SecretCredential,
}

impl DurableCredentialProvider for EnvironmentCredentialProvider {
    fn resolve<'life0, 'life1, 'async_trait>(
        &'life0 self,
        reference: &'life1 str,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = std::result::Result<DurableCredential, DurableCredentialProviderError>,
                > + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            if reference != self.reference {
                return Err(DurableCredentialProviderError::Rejected);
            }
            Ok(match &self.credential {
                SecretCredential::UsernamePassword { username, password } => {
                    DurableCredential::username_password(username.clone(), password.clone())
                }
                SecretCredential::PersonalAccessToken { token } => {
                    DurableCredential::personal_access_token(token.clone())
                }
            })
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuditVector {
    observed_errors: u64,
    observed_timeouts: u64,
    unexpected_duplicates: u64,
    wrong_identities: u64,
    wrong_digests: u64,
    peak_rss_bytes: u64,
    queue_depth_after_drain: u64,
    lag_after_drain: u64,
    disk_growth_bytes: u64,
    hard_resource_limit_exceeded: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuditResponse {
    schema: String,
    action: ProbeAction,
    observation_id: String,
    phase: String,
    arm: Arm,
    sample_index: u64,
    safety_control: Option<SafetyControl>,
    observed_axes: ComparableAxes,
    safety_control_active: bool,
    metrics: Option<AuditVector>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProbeAction {
    Begin,
    Finish,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct ProbeRequest<'a> {
    schema: &'static str,
    action: ProbeAction,
    observation_id: &'a str,
    phase: &'a str,
    arm: Arm,
    sample_index: u64,
    safety_control: Option<SafetyControl>,
    completed_operations: u64,
    payload_sha256: &'a str,
}

enum Adapter {
    Direct {
        client: IggyClient,
        producer: Option<IggyProducer>,
    },
    Full {
        handle: Box<DurableHandle>,
        target: NodeId,
        ordering_key: Vec<u8>,
        boundary: ConfirmationBoundary,
    },
    #[cfg(test)]
    Test { delay: Duration },
    #[cfg(test)]
    ControlledTest {
        reject: Arc<std::sync::atomic::AtomicBool>,
    },
}

struct Measurement {
    elapsed: Duration,
    completed_operations: u64,
    errors: u64,
    timeouts: u64,
    raw_latency_micros: Vec<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = parse_cli()?;
    let candidate_bytes = fs::read(&cli.candidate_manifest).with_context(|| {
        format!(
            "read candidate manifest {}",
            cli.candidate_manifest.display()
        )
    })?;
    let candidate: CandidateManifest = serde_json::from_slice(&candidate_bytes)?;
    anyhow::ensure!(candidate.performance.samples > 0, "sample count is zero");
    validate_plan(&candidate.performance)?;
    let candidate_sha256 = sha256(&candidate_bytes);
    fs::create_dir_all(&cli.output)?;

    match cli.mode {
        Mode::Aa => run_aa(&cli.output, &candidate_sha256, &candidate.performance).await,
        Mode::SafetyAblation => {
            run_safety(
                &cli.output,
                &candidate_sha256,
                &candidate.performance,
                cli.bounds.as_deref().context("--bounds is required")?,
            )
            .await
        }
        Mode::Paired => {
            run_paired(
                &cli.output,
                &candidate_sha256,
                &candidate.performance,
                cli.bounds.as_deref().context("--bounds is required")?,
            )
            .await
        }
    }
}

async fn run_aa(output: &Path, candidate_sha256: &str, plan: &PerformancePlan) -> Result<()> {
    let mut left = Vec::with_capacity(plan.samples as usize);
    let mut right = Vec::with_capacity(plan.samples as usize);
    for sample in 0..plan.samples {
        left.push(collect(plan, "aa_left", sample, Arm::Direct, None).await?);
        right.push(collect(plan, "aa_right", sample, Arm::Direct, None).await?);
    }
    let aa = AaArtifact {
        schema: AA_SCHEMA.into(),
        candidate_sha256: candidate_sha256.into(),
        axes: plan.axes.clone(),
        left,
        right,
    };
    let aa_bytes = serde_json::to_vec_pretty(&aa)?;
    let bounds = build_bounds(
        candidate_sha256,
        aa.left.clone(),
        aa.right.clone(),
        &sha256(&aa_bytes),
        unix_millis()?,
    )?;
    write_new(&output.join("aa.json"), &aa_bytes)?;
    write_new(
        &output.join("bounds.json"),
        &serde_json::to_vec_pretty(&bounds)?,
    )
}

async fn run_safety(
    output: &Path,
    candidate_sha256: &str,
    plan: &PerformancePlan,
    bounds_path: &Path,
) -> Result<()> {
    let bounds_bytes = fs::read(bounds_path)?;
    let bounds: BoundsArtifact = serde_json::from_slice(&bounds_bytes)?;
    let bounds_sha256 = sha256(&bounds_bytes);
    validate_bounds(candidate_sha256, &bounds, &bounds_sha256)?;
    verify_bounds_source(candidate_sha256, &bounds, bounds_path)?;
    let mut controls = Vec::with_capacity(SAFETY_CONTROLS.len());
    for (index, control) in SAFETY_CONTROLS.into_iter().enumerate() {
        controls.push(SafetyObservation {
            control,
            observation: collect(
                plan,
                &format!("safety_{}", control_name(control)),
                index as u64,
                Arm::Full,
                Some(control),
            )
            .await?,
        });
    }
    let safety = build_safety(candidate_sha256, &bounds, &bounds_sha256, controls)?;
    let safety_bytes = serde_json::to_vec_pretty(&safety)?;
    let freeze = FreezeRecord {
        schema: FREEZE_SCHEMA.into(),
        candidate_sha256: candidate_sha256.into(),
        axes: plan.axes.clone(),
        bounds_sha256,
        safety_sha256: sha256(&safety_bytes),
        frozen_at_unix_millis: unix_millis()?,
    };
    write_new(&output.join("safety.json"), &safety_bytes)?;
    write_new(
        &output.join("freeze.json"),
        &serde_json::to_vec_pretty(&freeze)?,
    )
}

async fn run_paired(
    output: &Path,
    candidate_sha256: &str,
    plan: &PerformancePlan,
    bounds_path: &Path,
) -> Result<()> {
    let bounds_bytes = fs::read(bounds_path)?;
    let bounds: BoundsArtifact = serde_json::from_slice(&bounds_bytes)?;
    let bounds_sha256 = sha256(&bounds_bytes);
    verify_bounds_source(candidate_sha256, &bounds, bounds_path)?;
    let evidence_root = bounds_path
        .parent()
        .and_then(Path::parent)
        .context("bounds must be stored under the A/A evidence directory")?;
    let safety_bytes = fs::read(evidence_root.join("safety/safety.json"))?;
    let safety: SafetyArtifact = serde_json::from_slice(&safety_bytes)?;
    let verified_safety = build_safety(
        candidate_sha256,
        &bounds,
        &bounds_sha256,
        safety.controls.clone(),
    )?;
    anyhow::ensure!(safety == verified_safety, "safety artifact differs");
    let freeze_bytes = fs::read(evidence_root.join("safety/freeze.json"))?;
    let freeze: FreezeRecord = serde_json::from_slice(&freeze_bytes)?;
    anyhow::ensure!(
        freeze.safety_sha256 == sha256(&safety_bytes),
        "safety digest differs"
    );
    validate_freeze(
        candidate_sha256,
        &bounds,
        &bounds_sha256,
        &freeze,
        &sha256(&freeze_bytes),
    )?;

    let mut direct = Vec::with_capacity(plan.samples as usize);
    let mut full = Vec::with_capacity(plan.samples as usize);
    for sample in 0..plan.samples {
        direct.push(collect(plan, "paired_direct", sample, Arm::Direct, None).await?);
        full.push(collect(plan, "paired_full", sample, Arm::Full, None).await?);
    }
    let artifact = build_paired(
        candidate_sha256,
        &bounds,
        &bounds_sha256,
        &freeze,
        &sha256(&freeze_bytes),
        direct,
        full,
    )?;
    write_new(
        &output.join("paired.json"),
        &serde_json::to_vec_pretty(&artifact)?,
    )?;
    anyhow::ensure!(
        artifact.verdict == evidence::OverallVerdict::Pass,
        "paired evidence did not pass: {:?}",
        artifact.verdict
    );
    Ok(())
}

async fn collect(
    plan: &PerformancePlan,
    phase: &str,
    sample: u64,
    expected_arm: Arm,
    safety_control: Option<SafetyControl>,
) -> Result<RawObservation> {
    let credential = load_credential(&plan.workload.credential_reference)?;
    let payload = Arc::new(read_digest_file(
        &plan.workload.payload_path,
        &plan.workload.payload_sha256,
        "payload",
    )?);
    let adapter = Arc::new(Mutex::new(
        build_adapter(plan, expected_arm, credential, phase, sample).await?,
    ));

    let observation_id = observation_id(plan, phase, sample, expected_arm, safety_control)?;
    let measurement = measure_with_probe(
        &adapter,
        &payload,
        &plan.axes,
        plan.workload.operation_timeout_millis,
        || {
            run_probe(
                &plan.probe,
                &plan.workload.credential_reference,
                ProbeRequest {
                    schema: "chirps.durable-perf-audit-request/v1",
                    action: ProbeAction::Begin,
                    observation_id: &observation_id,
                    phase,
                    arm: expected_arm,
                    sample_index: sample,
                    safety_control,
                    completed_operations: 0,
                    payload_sha256: &plan.workload.payload_sha256,
                },
            )
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(plan.axes.drain_millis)).await;
    let completed_operations = measurement
        .as_ref()
        .map(|(_, measurement)| measurement.completed_operations)
        .unwrap_or(0);
    let finish = run_probe(
        &plan.probe,
        &plan.workload.credential_reference,
        ProbeRequest {
            schema: "chirps.durable-perf-audit-request/v1",
            action: ProbeAction::Finish,
            observation_id: &observation_id,
            phase,
            arm: expected_arm,
            sample_index: sample,
            safety_control,
            completed_operations,
            payload_sha256: &plan.workload.payload_sha256,
        },
    );
    let mut adapter = Arc::try_unwrap(adapter)
        .map_err(|_| anyhow::anyhow!("scheduled adapter operations remain after join"))?
        .into_inner();
    let shutdown = adapter
        .shutdown(plan.workload.operation_timeout_millis)
        .await;

    let (begin, measurement) = measurement?;
    let finish = finish?;
    shutdown?;
    let audit = finish
        .metrics
        .context("Finish probe response omitted audit metrics")?;

    let errors = measurement
        .errors
        .checked_add(audit.observed_errors)
        .context("audited error count overflow")?;
    let timeouts = measurement
        .timeouts
        .checked_add(audit.observed_timeouts)
        .context("audited timeout count overflow")?;
    let elapsed_nanos = u64::try_from(measurement.elapsed.as_nanos())?;
    let throughput_per_second =
        measurement.completed_operations as f64 / measurement.elapsed.as_secs_f64();
    let latency_micros = percentiles(&measurement.raw_latency_micros);
    Ok(RawObservation {
        axes: plan.axes.clone(),
        observed_axes_begin: begin.observed_axes,
        observed_axes_finish: finish.observed_axes,
        observation_id,
        arm: expected_arm,
        sample_index: sample,
        metrics: MetricVector {
            elapsed_nanos,
            completed_operations: measurement.completed_operations,
            throughput_per_second,
            latency_micros,
            errors,
            timeouts,
            unexpected_duplicates: audit.unexpected_duplicates,
            wrong_identities: audit.wrong_identities,
            wrong_digests: audit.wrong_digests,
            peak_rss_bytes: audit.peak_rss_bytes,
            queue_depth_after_drain: audit.queue_depth_after_drain,
            lag_after_drain: audit.lag_after_drain,
            disk_growth_bytes: audit.disk_growth_bytes,
            hard_resource_limit_exceeded: audit.hard_resource_limit_exceeded,
            raw_latency_micros: measurement.raw_latency_micros,
        },
    })
}

async fn measure_with_probe<T>(
    adapter: &Arc<Mutex<Adapter>>,
    payload: &Arc<Vec<u8>>,
    axes: &ComparableAxes,
    operation_timeout_millis: u64,
    begin: impl FnOnce() -> Result<T>,
) -> Result<(T, Measurement)> {
    let first_sequence = run_warmup(adapter, payload, axes, operation_timeout_millis).await?;
    // Warmup is a clean prerequisite, not a safety-ablation observation. Start
    // auditing/injection only once its complete arrival window has finished.
    let observation = begin()?;
    let measurement = run_measurement(
        Arc::clone(adapter),
        Arc::clone(payload),
        axes,
        operation_timeout_millis,
        first_sequence,
    )
    .await?;
    Ok((observation, measurement))
}

async fn run_warmup(
    adapter: &Arc<Mutex<Adapter>>,
    payload: &Arc<Vec<u8>>,
    axes: &ComparableAxes,
    operation_timeout_millis: u64,
) -> Result<u64> {
    let operation_count =
        exact_operation_count(axes.offered_load_per_second, axes.warmup_millis, "warmup")?;
    let measurement = run_scheduled_operations(
        Arc::clone(adapter),
        Arc::clone(payload),
        axes.offered_load_per_second,
        axes.warmup_millis,
        "warmup",
        operation_timeout_millis,
        0,
    )
    .await?;
    anyhow::ensure!(
        measurement.completed_operations == operation_count
            && measurement.errors == 0
            && measurement.timeouts == 0,
        "warmup did not complete without errors or timeouts"
    );
    Ok(operation_count)
}

async fn run_measurement(
    adapter: Arc<Mutex<Adapter>>,
    payload: Arc<Vec<u8>>,
    axes: &ComparableAxes,
    operation_timeout_millis: u64,
    first_sequence: u64,
) -> Result<Measurement> {
    run_scheduled_operations(
        adapter,
        payload,
        axes.offered_load_per_second,
        axes.measure_millis,
        "measurement",
        operation_timeout_millis,
        first_sequence,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_scheduled_operations(
    adapter: Arc<Mutex<Adapter>>,
    payload: Arc<Vec<u8>>,
    offered_load_per_second: u64,
    duration_millis: u64,
    phase: &str,
    operation_timeout_millis: u64,
    first_sequence: u64,
) -> Result<Measurement> {
    let operation_count = exact_operation_count(offered_load_per_second, duration_millis, phase)?;
    LocalSet::new()
        .run_until(async move {
            let measurement_start = Instant::now();
            let timeout = Duration::from_millis(operation_timeout_millis);
            let mut operations = JoinSet::new();
            let mut outcomes = Vec::with_capacity(operation_count as usize);
            for operation in 0..operation_count {
                let scheduled =
                    scheduled_at(measurement_start, operation, offered_load_per_second)?;
                tokio::time::sleep_until(scheduled).await;
                while let Some(outcome) = operations.try_join_next() {
                    outcomes.push(outcome.context("measurement operation task failed")?);
                }
                let adapter = Arc::clone(&adapter);
                let payload = Arc::clone(&payload);
                let sequence = first_sequence
                    .checked_add(operation)
                    .context("send sequence overflow")?;
                let deadline = scheduled
                    .checked_add(timeout)
                    .context("operation deadline overflow")?;
                // DurableHandle owns mutable lifecycle state. This mutex is its
                // public API admission queue; queue wait remains inside latency
                // and timeout while LocalSet supports its !Send future.
                operations.spawn_local(async move {
                    let outcome = tokio::time::timeout_at(deadline, async {
                        adapter
                            .lock()
                            .await
                            .send(payload.as_slice(), sequence)
                            .await
                    })
                    .await;
                    (operation, outcome, scheduled.elapsed())
                });
            }

            while let Some(outcome) = operations.join_next().await {
                outcomes.push(outcome.context("measurement operation task failed")?);
            }
            // Fast operations can finish before the last arrival interval ends.
            // Retain the declared window in the throughput denominator; slow
            // completions still extend it and their full latency remains visible.
            let window_end = measurement_start
                .checked_add(Duration::from_millis(duration_millis))
                .context("measurement window overflow")?;
            tokio::time::sleep_until(window_end).await;
            let elapsed = measurement_start.elapsed();
            outcomes.sort_unstable_by_key(|(operation, _, _)| *operation);

            let mut completed_operations = 0_u64;
            let mut errors = 0_u64;
            let mut timeouts = 0_u64;
            let mut raw_latency_micros = Vec::with_capacity(operation_count as usize);
            for (_, outcome, elapsed) in outcomes {
                match outcome {
                    Ok(Ok(())) => {
                        completed_operations = completed_operations
                            .checked_add(1)
                            .context("completed-operation count overflow")?
                    }
                    Ok(Err(_)) => errors = errors.checked_add(1).context("error count overflow")?,
                    Err(_) => {
                        timeouts = timeouts.checked_add(1).context("timeout count overflow")?
                    }
                }
                raw_latency_micros.push(duration_micros(elapsed)?);
            }
            Ok(Measurement {
                elapsed,
                completed_operations,
                errors,
                timeouts,
                raw_latency_micros,
            })
        })
        .await
}

impl Adapter {
    async fn send(&mut self, payload: &[u8], sequence: u64) -> Result<()> {
        match self {
            Self::Direct { producer, .. } => producer
                .as_ref()
                .context("Direct producer was already shut down")?
                .send(vec![IggyMessage::from(payload.to_vec())])
                .await
                .context("Direct send failed"),
            Self::Full {
                handle,
                target,
                ordering_key,
                boundary,
            } => {
                let mut exact_ordering_key = ordering_key.clone();
                exact_ordering_key.extend_from_slice(&sequence.to_be_bytes());
                let prepared = handle.prepare(*target, exact_ordering_key, payload)?;
                let result = handle.send(&prepared, *boundary).await?;
                let accepted = matches!(
                    (*boundary, result.outcome()),
                    (
                        ConfirmationBoundary::BrokerAccepted,
                        DurableSendOutcome::BrokerAccepted
                    ) | (
                        ConfirmationBoundary::OsSyncedAccepted,
                        DurableSendOutcome::OsSyncedAccepted
                    )
                );
                anyhow::ensure!(accepted, "Full send did not reach its declared boundary");
                Ok(())
            }
            #[cfg(test)]
            Self::Test { delay } => {
                tokio::time::sleep(*delay).await;
                Ok(())
            }
            #[cfg(test)]
            Self::ControlledTest { reject } => {
                anyhow::ensure!(
                    !reject.load(std::sync::atomic::Ordering::Acquire),
                    "injected operation error"
                );
                Ok(())
            }
        }
    }

    async fn shutdown(&mut self, timeout_millis: u64) -> Result<()> {
        match self {
            Self::Direct { client, producer } => {
                if let Some(producer) = producer.take() {
                    producer.shutdown().await;
                }
                tokio::time::timeout(Duration::from_millis(timeout_millis), client.shutdown())
                    .await
                    .context("Direct shutdown timed out")??;
            }
            Self::Full { handle, .. } => {
                handle
                    .shutdown(Instant::now() + Duration::from_millis(timeout_millis))
                    .await?;
            }
            #[cfg(test)]
            Self::Test { .. } | Self::ControlledTest { .. } => {}
        }
        Ok(())
    }
}

async fn build_adapter(
    plan: &PerformancePlan,
    arm: Arm,
    credential: SecretCredential,
    phase: &str,
    sample: u64,
) -> Result<Adapter> {
    match arm {
        Arm::Direct => build_direct(&plan.workload, credential).await,
        Arm::Full => build_full(plan, credential, phase, sample).await,
    }
}

async fn build_direct(workload: &Workload, credential: SecretCredential) -> Result<Adapter> {
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(workload.endpoint.to_string())
        .with_tls_enabled(true)
        .with_tls_domain(workload.tls_server_name.clone())
        .with_tls_ca_file(path_text(&workload.tls_ca_pem_path, "TLS CA PEM")?)
        .with_tls_validate_certificate(true)
        .with_no_delay()
        .build()?;
    let connect_timeout = Duration::from_millis(workload.connect_timeout_millis);
    tokio::time::timeout(connect_timeout, client.connect())
        .await
        .context("Direct connect timed out")??;
    match &credential {
        SecretCredential::UsernamePassword { username, password } => {
            let result =
                tokio::time::timeout(connect_timeout, client.login_user(username, password)).await;
            anyhow::ensure!(matches!(result, Ok(Ok(_))), "Direct authentication failed");
        }
        SecretCredential::PersonalAccessToken { token } => {
            let result = tokio::time::timeout(
                connect_timeout,
                client.login_with_personal_access_token(token),
            )
            .await;
            anyhow::ensure!(matches!(result, Ok(Ok(_))), "Direct authentication failed");
        }
    }
    let stream = workload.stream_id.to_string();
    let topic = workload.topic_id.to_string();
    let producer = client
        .producer(&stream, &topic)?
        .direct(DirectConfig::builder().batch_length(1).build())
        .partitioning(Partitioning::partition_id(workload.partition_id))
        .send_retries(Some(0), None)
        .do_not_create_stream_if_not_exists()
        .do_not_create_topic_if_not_exists()
        .build();
    tokio::time::timeout(connect_timeout, producer.init())
        .await
        .context("Direct producer initialization timed out")??;
    Ok(Adapter::Direct {
        client,
        producer: Some(producer),
    })
}

async fn build_full(
    plan: &PerformancePlan,
    credential: SecretCredential,
    phase: &str,
    sample: u64,
) -> Result<Adapter> {
    let workload = &plan.workload;
    let trusted_root = read_digest_file(
        &workload.tls_root_der_path,
        &workload.tls_root_der_sha256,
        "TLS root DER",
    )?;
    let (profile, boundary) = match plan.axes.full_confirmation_profile {
        FullConfirmationProfile::BrokerAccepted => {
            let startup = workload
                .broker_startup_config
                .as_ref()
                .context("broker-accepted profile requires broker startup config")?;
            (
                DurableProfile::broker_accepted(read_digest_file(
                    &startup.path,
                    &startup.sha256,
                    "broker startup config",
                )?),
                ConfirmationBoundary::BrokerAccepted,
            )
        }
        FullConfirmationProfile::OsSyncedAccepted => {
            anyhow::ensure!(
                workload.broker_startup_config.is_none(),
                "OS-synced profile must not carry broker startup config"
            );
            (
                DurableProfile::OsSyncedAccepted,
                ConfirmationBoundary::OsSyncedAccepted,
            )
        }
    };
    let checkpoint = workload
        .checkpoint_root
        .join(format!("{phase}-{sample}-full"));
    let config = DurableConfig::new(
        workload.endpoint,
        DurableTlsConfig::new(workload.tls_server_name.clone(), vec![trusted_root]),
        workload.credential_reference.clone(),
        profile,
        DurableRoutingConfig::new(workload.inbox_generation, 1),
        DurableResourceConfig::new(
            workload.stream_id,
            workload.topic_id,
            vec![DurablePartitionProjection::new(
                workload.partition_id,
                decode_hex(&workload.resource_id_hex, "resource ID")?,
                workload.resource_epoch,
                decode_hex(&workload.build_sha_hex, "build SHA")?,
                workload.retention_bytes,
                workload.retention_messages,
                workload.checksum_enabled,
                decode_hex(&workload.configuration_digest_hex, "configuration digest")?,
                decode_hex(&workload.security_digest_hex, "security digest")?,
                decode_hex(&workload.capability_digest_hex, "capability digest")?,
            )],
        ),
        DurableCheckpointConfig::new(
            checkpoint,
            workload.lifecycle_generation,
            DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
        ),
        DurableLeaseConfig::new(
            workload.lease_millis,
            Duration::from_millis(workload.renew_interval_millis),
        ),
        DurableExtensionConfig::required(workload.max_frame_len),
    );
    let provider = EnvironmentCredentialProvider {
        reference: workload.credential_reference.clone(),
        credential,
    };
    let deadline = Instant::now() + Duration::from_millis(workload.connect_timeout_millis);
    let handle = DurableBuilder::new(NodeId::from(decode_hex(
        &workload.source_node_id_hex,
        "source node ID",
    )?))
    .inbox_generation(workload.inbox_generation)
    .explicit_partitions(1)
    .connect(config, &provider, deadline)
    .await?;
    Ok(Adapter::Full {
        handle: Box::new(handle),
        target: NodeId::from(decode_hex(&workload.target_node_id_hex, "target node ID")?),
        ordering_key: decode_hex_bytes(&workload.ordering_key_hex, "ordering key")?,
        boundary,
    })
}

fn validate_plan(plan: &PerformancePlan) -> Result<()> {
    validate_direct_boundary(plan.axes.full_confirmation_profile)?;
    anyhow::ensure!(plan.samples > 0, "sample count is zero");
    anyhow::ensure!(
        plan.workload.partition_id == 0,
        "only exact partition 0 is supported"
    );
    anyhow::ensure!(
        plan.workload.operation_timeout_millis > 0,
        "operation timeout is zero"
    );
    anyhow::ensure!(
        plan.workload.connect_timeout_millis > 0,
        "connect timeout is zero"
    );
    anyhow::ensure!(
        plan.workload.renew_interval_millis > 0
            && plan.workload.renew_interval_millis < u64::from(plan.workload.lease_millis),
        "lease cadence is invalid"
    );
    anyhow::ensure!(
        !plan.workload.credential_reference.is_empty(),
        "credential reference is empty"
    );
    anyhow::ensure!(
        plan.workload
            .credential_reference
            .bytes()
            .all(|byte| { byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_' }),
        "credential reference must be an uppercase environment-variable name"
    );
    for (path, label) in [
        (&plan.workload.tls_ca_pem_path, "TLS CA PEM"),
        (&plan.workload.tls_root_der_path, "TLS root DER"),
        (&plan.workload.payload_path, "payload"),
        (&plan.workload.checkpoint_root, "checkpoint root"),
    ] {
        anyhow::ensure!(path.is_absolute(), "{label} path must be absolute");
    }
    let payload = read_digest_file(
        &plan.workload.payload_path,
        &plan.workload.payload_sha256,
        "payload",
    )?;
    anyhow::ensure!(
        plan.axes.payload_digest == plan.workload.payload_sha256,
        "payload axis digest differs"
    );
    anyhow::ensure!(
        plan.axes.payload_bytes == u64::try_from(payload.len())?,
        "payload axis size differs"
    );
    read_digest_file(
        &plan.workload.tls_ca_pem_path,
        &plan.workload.tls_ca_pem_sha256,
        "TLS CA PEM",
    )?;
    read_digest_file(
        &plan.workload.tls_root_der_path,
        &plan.workload.tls_root_der_sha256,
        "TLS root DER",
    )?;
    if let Some(startup) = &plan.workload.broker_startup_config {
        anyhow::ensure!(
            startup.path.is_absolute(),
            "broker startup config path must be absolute"
        );
        read_digest_file(&startup.path, &startup.sha256, "broker startup config")?;
    }
    match plan.axes.full_confirmation_profile {
        FullConfirmationProfile::BrokerAccepted => anyhow::ensure!(
            plan.workload.broker_startup_config.is_some(),
            "broker-accepted profile requires broker startup config"
        ),
        FullConfirmationProfile::OsSyncedAccepted => anyhow::ensure!(
            plan.workload.broker_startup_config.is_none(),
            "OS-synced profile must not carry broker startup config"
        ),
    }
    let projection = ProjectionFingerprint {
        stream_id: plan.workload.stream_id,
        topic_id: plan.workload.topic_id,
        partition_id: plan.workload.partition_id,
        inbox_generation: plan.workload.inbox_generation,
        resource_id_hex: &plan.workload.resource_id_hex,
        resource_epoch: plan.workload.resource_epoch,
        build_sha_hex: &plan.workload.build_sha_hex,
        retention_bytes: plan.workload.retention_bytes,
        retention_messages: plan.workload.retention_messages,
        checksum_enabled: plan.workload.checksum_enabled,
        configuration_digest_hex: &plan.workload.configuration_digest_hex,
        security_digest_hex: &plan.workload.security_digest_hex,
        capability_digest_hex: &plan.workload.capability_digest_hex,
    };
    anyhow::ensure!(
        plan.axes.partition_set_digest == sha256(&serde_json::to_vec(&projection)?),
        "partition-set axis digest differs from the exact projection"
    );
    decode_hex::<16>(&plan.workload.source_node_id_hex, "source node ID")?;
    decode_hex::<16>(&plan.workload.target_node_id_hex, "target node ID")?;
    decode_hex::<16>(&plan.workload.resource_id_hex, "resource ID")?;
    decode_hex::<20>(&plan.workload.build_sha_hex, "build SHA")?;
    decode_hex::<32>(
        &plan.workload.configuration_digest_hex,
        "configuration digest",
    )?;
    decode_hex::<32>(&plan.workload.security_digest_hex, "security digest")?;
    decode_hex::<32>(&plan.workload.capability_digest_hex, "capability digest")?;
    anyhow::ensure!(
        !decode_hex_bytes(&plan.workload.ordering_key_hex, "ordering key")?.is_empty(),
        "ordering key is empty"
    );
    verify_probe(&plan.probe)
}

fn validate_direct_boundary(profile: FullConfirmationProfile) -> Result<()> {
    // Official SDK send confirms ordinary broker acceptance. It does not return
    // the compatible extension's exact OS-synced receipt used by the Full arm.
    anyhow::ensure!(
        profile == FullConfirmationProfile::BrokerAccepted,
        "Direct SDK control cannot attest OsSyncedAccepted; a matching strong control is required"
    );
    Ok(())
}

#[derive(Serialize)]
struct ProjectionFingerprint<'a> {
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
    inbox_generation: u64,
    resource_id_hex: &'a str,
    resource_epoch: u64,
    build_sha_hex: &'a str,
    retention_bytes: u64,
    retention_messages: u64,
    checksum_enabled: bool,
    configuration_digest_hex: &'a str,
    security_digest_hex: &'a str,
    capability_digest_hex: &'a str,
}

fn run_probe(
    probe: &AuditProbe,
    credential_reference: &str,
    request: ProbeRequest<'_>,
) -> Result<AuditResponse> {
    verify_probe(probe)?;
    let output = probe_command(probe, credential_reference)
        .arg("--chirps-audit-request-json")
        .arg(serde_json::to_string(&request)?)
        .output()
        .with_context(|| {
            format!(
                "run auxiliary audit probe for {} sample {}",
                request.phase, request.sample_index
            )
        })?;
    anyhow::ensure!(
        output.status.success(),
        "auxiliary audit probe failed for {} sample {}: {}",
        request.phase,
        request.sample_index,
        output.status
    );
    let response: AuditResponse = serde_json::from_slice(&output.stdout).with_context(|| {
        format!(
            "decode auxiliary audit for {} sample {}",
            request.phase, request.sample_index
        )
    })?;
    validate_audit_response(&request, response)
}

fn validate_audit_response(
    request: &ProbeRequest<'_>,
    response: AuditResponse,
) -> Result<AuditResponse> {
    anyhow::ensure!(
        response.schema == "chirps.durable-perf-audit-response/v1",
        "unknown auxiliary audit schema"
    );
    anyhow::ensure!(
        response.action == request.action
            && response.observation_id == request.observation_id
            && response.phase == request.phase
            && response.arm == request.arm
            && response.sample_index == request.sample_index,
        "auxiliary audit response differs from its request"
    );
    anyhow::ensure!(
        response.safety_control == request.safety_control,
        "auxiliary audit safety control differs from its request"
    );
    match request.action {
        ProbeAction::Begin => {
            anyhow::ensure!(
                response.safety_control_active == request.safety_control.is_some(),
                "Begin probe did not establish the requested safety control"
            );
            anyhow::ensure!(response.metrics.is_none(), "Begin probe returned metrics");
        }
        ProbeAction::Finish => {
            anyhow::ensure!(
                !response.safety_control_active,
                "Finish probe left the safety control active"
            );
            anyhow::ensure!(response.metrics.is_some(), "Finish probe omitted metrics");
        }
    }
    Ok(response)
}

fn probe_command(probe: &AuditProbe, credential_reference: &str) -> Command {
    let mut command = Command::new(&probe.program);
    command.env_remove(credential_reference);
    command
}

fn verify_probe(probe: &AuditProbe) -> Result<()> {
    anyhow::ensure!(
        probe.program.is_absolute(),
        "audit probe path must be absolute"
    );
    let bytes = fs::read(&probe.program)?;
    anyhow::ensure!(sha256(&bytes) == probe.sha256, "audit probe digest differs");
    Ok(())
}

fn load_credential(reference: &str) -> Result<SecretCredential> {
    let encoded = std::env::var(reference)
        .map_err(|_| anyhow::anyhow!("credential reference is unavailable"))?;
    parse_credential(&encoded)
}

fn parse_credential(encoded: &str) -> Result<SecretCredential> {
    serde_json::from_str(encoded).map_err(|_| anyhow::anyhow!("credential reference is invalid"))
}

fn read_digest_file(path: &Path, expected_sha256: &str, label: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(path).with_context(|| format!("read {label}"))?;
    anyhow::ensure!(sha256(&bytes) == expected_sha256, "{label} digest differs");
    Ok(bytes)
}

fn path_text(path: &Path, label: &str) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .with_context(|| format!("{label} path is not UTF-8"))
}

fn exact_operation_count(rate: u64, millis: u64, phase: &str) -> Result<u64> {
    if millis == 0 {
        return Ok(0);
    }
    let numerator = rate
        .checked_mul(millis)
        .with_context(|| format!("{phase} operation count overflow"))?;
    anyhow::ensure!(
        numerator % 1_000 == 0,
        "{phase} duration does not produce an exact operation count"
    );
    let count = numerator / 1_000;
    anyhow::ensure!(count > 0, "{phase} operation count is zero");
    Ok(count)
}

fn scheduled_at(start: Instant, operation: u64, rate: u64) -> Result<Instant> {
    let offset_nanos = u128::from(operation)
        .checked_mul(1_000_000_000)
        .context("schedule offset overflow")?
        / u128::from(rate);
    let offset_nanos = u64::try_from(offset_nanos)?;
    start
        .checked_add(Duration::from_nanos(offset_nanos))
        .context("schedule instant overflow")
}

fn duration_micros(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_micros()).context("latency does not fit in u64")
}

fn percentiles(raw: &[u64]) -> LatencyVector {
    let mut sorted = raw.to_vec();
    sorted.sort_unstable();
    LatencyVector {
        p50: percentile(&sorted, 50),
        p95: percentile(&sorted, 95),
        p99: percentile(&sorted, 99),
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn decode_hex<const N: usize>(value: &str, label: &str) -> Result<[u8; N]> {
    let bytes = decode_hex_bytes(value, label)?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{label} must contain exactly {N} bytes"))
}

fn decode_hex_bytes(value: &str, label: &str) -> Result<Vec<u8>> {
    anyhow::ensure!(value.len().is_multiple_of(2), "{label} has odd-length hex");
    anyhow::ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{label} is not lowercase hex"
    );
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("hex chunks are UTF-8");
            u8::from_str_radix(pair, 16).with_context(|| format!("{label} is not lowercase hex"))
        })
        .collect::<Result<Vec<_>>>()
}

fn verify_bounds_source(
    candidate_sha256: &str,
    bounds: &BoundsArtifact,
    bounds_path: &Path,
) -> Result<()> {
    let aa_path = bounds_path
        .parent()
        .context("bounds path has no A/A directory")?
        .join("aa.json");
    let aa_bytes = fs::read(aa_path)?;
    let source_sha256 = sha256(&aa_bytes);
    anyhow::ensure!(
        source_sha256 == bounds.aa_source_sha256,
        "A/A source digest differs"
    );
    let aa: AaArtifact = serde_json::from_slice(&aa_bytes)?;
    anyhow::ensure!(aa.schema == AA_SCHEMA, "unknown A/A schema");
    anyhow::ensure!(
        aa.candidate_sha256 == candidate_sha256,
        "A/A candidate differs"
    );
    anyhow::ensure!(aa.axes == bounds.axes, "A/A axes differ");
    let rebuilt = build_bounds(
        candidate_sha256,
        aa.left,
        aa.right,
        &source_sha256,
        bounds.frozen_at_unix_millis,
    )?;
    anyhow::ensure!(&rebuilt == bounds, "bounds differ from A/A source");
    Ok(())
}

fn parse_cli() -> Result<Cli> {
    parse_args(&std::env::args().skip(1).collect::<Vec<_>>())
}

fn parse_args(args: &[String]) -> Result<Cli> {
    anyhow::ensure!(
        args.len().is_multiple_of(2),
        "every option requires one value"
    );
    let mut options = BTreeMap::new();
    for pair in args.as_chunks::<2>().0.iter() {
        anyhow::ensure!(
            matches!(
                pair[0].as_str(),
                "--mode" | "--candidate-manifest" | "--bounds" | "--output"
            ),
            "unknown option {}",
            pair[0]
        );
        anyhow::ensure!(
            options.insert(pair[0].as_str(), pair[1].as_str()).is_none(),
            "duplicate option {}",
            pair[0]
        );
    }
    let mode = match required(&options, "--mode")? {
        "aa" => Mode::Aa,
        "safety-ablation" => Mode::SafetyAblation,
        "paired" => Mode::Paired,
        value => anyhow::bail!("invalid --mode {value}"),
    };
    let bounds = options.get("--bounds").copied().map(PathBuf::from);
    if matches!(mode, Mode::Aa) {
        anyhow::ensure!(bounds.is_none(), "A/A does not accept external bounds");
    }
    Ok(Cli {
        mode,
        candidate_manifest: required(&options, "--candidate-manifest")?.into(),
        bounds,
        output: required(&options, "--output")?.into(),
    })
}

fn required<'a>(options: &'a BTreeMap<&str, &str>, name: &str) -> Result<&'a str> {
    options
        .get(name)
        .copied()
        .with_context(|| format!("missing {name}"))
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let name = path.file_name().context("evidence path has no file name")?;
    let temporary = path.with_file_name(format!(
        ".{}.{}.tmp",
        name.to_string_lossy(),
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    if let Err(error) = fs::hard_link(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    fs::remove_file(&temporary)?;
    fs::File::open(path.parent().context("evidence path has no parent")?)?.sync_all()?;
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn observation_id(
    plan: &PerformancePlan,
    phase: &str,
    sample: u64,
    arm: Arm,
    safety_control: Option<SafetyControl>,
) -> Result<String> {
    Ok(sha256(&serde_json::to_vec(&(
        &plan.axes,
        phase,
        sample,
        arm,
        safety_control,
    ))?))
}

fn unix_millis() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

fn control_name(control: SafetyControl) -> &'static str {
    match control {
        SafetyControl::ForbiddenError => "forbidden_error",
        SafetyControl::Timeout => "timeout",
        SafetyControl::UnexpectedDuplicate => "unexpected_duplicate",
        SafetyControl::WrongIdentity => "wrong_identity",
        SafetyControl::WrongDigest => "wrong_digest",
        SafetyControl::UndrainedQueue => "undrained_queue",
        SafetyControl::UndrainedLag => "undrained_lag",
        SafetyControl::HardResourceLimit => "hard_resource_limit",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(extra: &[&str]) -> Vec<String> {
        [
            "--mode",
            "aa",
            "--candidate-manifest",
            "/candidate.json",
            "--output",
            "/evidence",
        ]
        .into_iter()
        .chain(extra.iter().copied())
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn v07_task_6_12_cli_rejects_external_thresholds_and_aa_bounds() {
        assert!(parse_args(&args(&["--threshold", "123"])).is_err());
        assert!(parse_args(&args(&["--bounds", "/external.json"])).is_err());
    }

    #[test]
    fn v07_task_6_12_exact_rate_and_raw_percentiles_are_owned_by_the_tool() {
        assert_eq!(exact_operation_count(100, 1_000, "test").unwrap(), 100);
        assert!(exact_operation_count(3, 500, "test").is_err());
        assert_eq!(
            percentiles(&[40, 10, 30, 20]),
            LatencyVector {
                p50: 20,
                p95: 40,
                p99: 40,
            }
        );
    }

    #[test]
    fn v07_task_6_12_auxiliary_probe_cannot_replace_or_relabel_the_workload() {
        let request = ProbeRequest {
            schema: "chirps.durable-perf-audit-request/v1",
            action: ProbeAction::Finish,
            observation_id: "observation",
            phase: "paired_full",
            arm: Arm::Full,
            sample_index: 7,
            safety_control: None,
            completed_operations: 11,
            payload_sha256: "payload",
        };
        let encoded = serde_json::to_string(&request).unwrap();
        assert!(!encoded.contains("credential"));
        assert!(!encoded.contains("latency"));

        let mut response = audit_response();
        response.arm = Arm::Direct;
        assert!(validate_audit_response(&request, response).is_err());
        let mut response = audit_response();
        response.sample_index = 8;
        assert!(validate_audit_response(&request, response).is_err());
        let mut response = audit_response();
        response.observation_id = "other".into();
        assert!(validate_audit_response(&request, response).is_err());
        let mut response = audit_response();
        response.action = ProbeAction::Begin;
        assert!(validate_audit_response(&request, response).is_err());
    }

    #[test]
    fn v07_task_6_12_credential_parse_error_never_echoes_secret_material() {
        let secret = "not-json-super-secret";
        let error = parse_credential(secret).err().unwrap().to_string();
        assert!(!error.contains(secret));
    }

    #[test]
    fn v07_task_6_12_probe_command_removes_the_credential_environment() {
        let probe = AuditProbe {
            program: "/probe".into(),
            sha256: "digest".into(),
        };
        let command = probe_command(&probe, "CHIRPS_PERF_CREDENTIAL");
        assert!(command.get_envs().any(|(name, value)| {
            name == std::ffi::OsStr::new("CHIRPS_PERF_CREDENTIAL") && value.is_none()
        }));
    }

    #[test]
    fn v07_task_6_12_candidate_manifest_allows_future_identity_fields() {
        let json = r#"{
            "source_sha256":"future",
            "performance":{
                "axes":{},
                "samples":1,
                "workload":{},
                "probe":{}
            }
        }"#;
        let error = serde_json::from_str::<CandidateManifest>(json)
            .err()
            .expect("nested strict fields remain incomplete");
        assert!(!error.to_string().contains("source_sha256"));
    }

    #[test]
    fn v07_task_6_12_probe_begin_must_activate_the_requested_control() {
        let request = ProbeRequest {
            schema: "chirps.durable-perf-audit-request/v1",
            action: ProbeAction::Begin,
            observation_id: "observation",
            phase: "safety_timeout",
            arm: Arm::Full,
            sample_index: 1,
            safety_control: Some(SafetyControl::Timeout),
            completed_operations: 0,
            payload_sha256: "payload",
        };
        let mut response = audit_response();
        response.action = ProbeAction::Begin;
        response.phase = request.phase.into();
        response.sample_index = request.sample_index;
        response.safety_control = request.safety_control;
        response.safety_control_active = true;
        response.metrics = None;
        assert!(validate_audit_response(&request, response).is_ok());

        let mut response = audit_response();
        response.action = ProbeAction::Begin;
        response.phase = request.phase.into();
        response.sample_index = request.sample_index;
        response.safety_control = request.safety_control;
        response.metrics = None;
        assert!(validate_audit_response(&request, response).is_err());
    }

    #[tokio::test]
    async fn fast_workload_still_measures_the_declared_arrival_window() {
        let mut axes = audit_response().observed_axes;
        axes.offered_load_per_second = 100;
        axes.measure_millis = 20;
        let adapter = Arc::new(Mutex::new(Adapter::Test {
            delay: Duration::ZERO,
        }));
        let measurement = run_measurement(adapter, Arc::new(vec![1]), &axes, 1_000, 0)
            .await
            .unwrap();
        assert_eq!(measurement.completed_operations, 2);
        assert!(measurement.elapsed >= Duration::from_millis(20));
        assert!(
            measurement.completed_operations as f64 / measurement.elapsed.as_secs_f64() <= 100.0
        );
    }

    #[test]
    fn ordinary_sdk_control_cannot_be_compared_to_os_synced_full_sends() {
        assert!(validate_direct_boundary(FullConfirmationProfile::BrokerAccepted).is_ok());
        assert!(validate_direct_boundary(FullConfirmationProfile::OsSyncedAccepted).is_err());
    }

    #[tokio::test]
    async fn safety_control_errors_are_measured_after_clean_warmup() {
        let mut axes = audit_response().observed_axes;
        axes.offered_load_per_second = 100;
        axes.warmup_millis = 10;
        axes.measure_millis = 10;
        let reject = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let adapter = Arc::new(Mutex::new(Adapter::ControlledTest {
            reject: Arc::clone(&reject),
        }));
        let (_, measurement) =
            measure_with_probe(&adapter, &Arc::new(vec![1]), &axes, 1_000, || {
                reject.store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            })
            .await
            .expect("a safety control must produce an observed failure, not abort warmup");
        assert_eq!(measurement.completed_operations, 0);
        assert_eq!(measurement.errors, 1);
        assert_eq!(measurement.timeouts, 0);
        assert_eq!(measurement.raw_latency_micros.len(), 1);
    }

    #[tokio::test]
    async fn failed_warmup_never_activates_a_safety_control() {
        let mut axes = audit_response().observed_axes;
        axes.offered_load_per_second = 100;
        axes.warmup_millis = 10;
        let adapter = Arc::new(Mutex::new(Adapter::ControlledTest {
            reject: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }));
        let mut activated = false;
        let result = measure_with_probe(&adapter, &Arc::new(vec![1]), &axes, 1_000, || {
            activated = true;
            Ok(())
        })
        .await;
        assert!(result.is_err());
        assert!(!activated);
    }

    #[tokio::test]
    async fn v07_task_6_12_arrival_schedule_charges_public_api_queue_wait_to_latency() {
        let mut axes = audit_response().observed_axes;
        axes.offered_load_per_second = 100;
        axes.measure_millis = 100;
        let adapter = Arc::new(Mutex::new(Adapter::Test {
            delay: Duration::from_millis(30),
        }));
        let measurement = run_measurement(adapter, Arc::new(vec![1]), &axes, 1_000, 0)
            .await
            .unwrap();
        assert_eq!(measurement.completed_operations, 10);
        assert_eq!(measurement.raw_latency_micros.len(), 10);
        assert!(measurement.raw_latency_micros.iter().max().unwrap() >= &200_000);
    }

    #[tokio::test]
    async fn v07_task_6_12_warmup_reuses_schedule_and_rejects_timeout() {
        let mut axes = audit_response().observed_axes;
        axes.offered_load_per_second = 100;
        axes.warmup_millis = 100;
        let adapter = Arc::new(Mutex::new(Adapter::Test {
            delay: Duration::from_millis(30),
        }));
        assert!(
            run_warmup(&adapter, &Arc::new(vec![1]), &axes, 15)
                .await
                .is_err()
        );
    }

    fn audit_response() -> AuditResponse {
        AuditResponse {
            schema: "chirps.durable-perf-audit-response/v1".into(),
            action: ProbeAction::Finish,
            observation_id: "observation".into(),
            phase: "paired_full".into(),
            arm: Arm::Full,
            sample_index: 7,
            safety_control: None,
            observed_axes: ComparableAxes {
                host_fingerprint: "host".into(),
                server_image_digest: "image".into(),
                server_source_digest: "source".into(),
                server_config_digest: "config".into(),
                payload_digest: "payload".into(),
                payload_bytes: 1,
                partition_set_digest: "partition".into(),
                full_confirmation_profile: FullConfirmationProfile::BrokerAccepted,
                offered_load_per_second: 1,
                warmup_millis: 0,
                measure_millis: 1_000,
                drain_millis: 1,
                client_placement: "same-host".into(),
                execution_class: evidence::ExecutionClass::Loopback,
                host_count: 1,
                broker_count: 1,
                replication_factor: 1,
            },
            safety_control_active: false,
            metrics: Some(AuditVector {
                observed_errors: 0,
                observed_timeouts: 0,
                unexpected_duplicates: 0,
                wrong_identities: 0,
                wrong_digests: 0,
                peak_rss_bytes: 1,
                queue_depth_after_drain: 0,
                lag_after_drain: 0,
                disk_growth_bytes: 1,
                hard_resource_limit_exceeded: false,
            }),
        }
    }
}
