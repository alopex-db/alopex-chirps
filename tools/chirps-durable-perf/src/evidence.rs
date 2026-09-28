use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const AA_SCHEMA: &str = "chirps.durable-perf-aa/v1";
pub const BOUNDS_SCHEMA: &str = "chirps.durable-perf-bounds/v1";
pub const SAFETY_SCHEMA: &str = "chirps.durable-perf-safety/v1";
pub const FREEZE_SCHEMA: &str = "chirps.durable-perf-freeze/v1";
pub const PAIRED_SCHEMA: &str = "chirps.durable-perf-paired/v1";
pub const BOUND_ESTIMATOR: &str = "max_symmetric_fractional_delta/v1";
pub const SAFETY_POLICY: &str = "zero_forbidden_events/v1";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComparableAxes {
    pub host_fingerprint: String,
    pub server_image_digest: String,
    pub server_source_digest: String,
    pub server_config_digest: String,
    pub payload_digest: String,
    pub payload_bytes: u64,
    pub partition_set_digest: String,
    pub full_confirmation_profile: FullConfirmationProfile,
    pub offered_load_per_second: u64,
    pub warmup_millis: u64,
    pub measure_millis: u64,
    pub drain_millis: u64,
    pub client_placement: String,
    pub execution_class: ExecutionClass,
    pub host_count: u64,
    pub broker_count: u64,
    pub replication_factor: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FullConfirmationProfile {
    BrokerAccepted,
    OsSyncedAccepted,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionClass {
    Loopback,
    Container,
    Physical,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Arm {
    Direct,
    Full,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawObservation {
    pub axes: ComparableAxes,
    pub observed_axes_begin: ComparableAxes,
    pub observed_axes_finish: ComparableAxes,
    pub observation_id: String,
    pub arm: Arm,
    pub sample_index: u64,
    pub metrics: MetricVector,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricVector {
    pub elapsed_nanos: u64,
    pub completed_operations: u64,
    pub throughput_per_second: f64,
    pub latency_micros: LatencyVector,
    pub errors: u64,
    pub timeouts: u64,
    pub unexpected_duplicates: u64,
    pub wrong_identities: u64,
    pub wrong_digests: u64,
    pub peak_rss_bytes: u64,
    pub queue_depth_after_drain: u64,
    pub lag_after_drain: u64,
    pub disk_growth_bytes: u64,
    pub hard_resource_limit_exceeded: bool,
    pub raw_latency_micros: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyVector {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BoundsArtifact {
    pub schema: String,
    pub candidate_sha256: String,
    pub axes: ComparableAxes,
    pub aa_source_sha256: String,
    pub frozen_at_unix_millis: u64,
    pub estimator: String,
    pub safety_policy: String,
    pub bounds: MetricBounds,
    pub safety_bounds: SafetyBounds,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AaArtifact {
    pub schema: String,
    pub candidate_sha256: String,
    pub axes: ComparableAxes,
    pub left: Vec<RawObservation>,
    pub right: Vec<RawObservation>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricBounds {
    pub throughput_fraction: f64,
    pub p50_fraction: f64,
    pub p95_fraction: f64,
    pub p99_fraction: f64,
    pub peak_rss_fraction: f64,
    pub disk_growth_fraction: f64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SafetyBounds {
    pub errors: u64,
    pub timeouts: u64,
    pub unexpected_duplicates: u64,
    pub wrong_identities: u64,
    pub wrong_digests: u64,
    pub queue_depth_after_drain: u64,
    pub lag_after_drain: u64,
    pub hard_resource_limit_exceeded: bool,
}

const ZERO_SAFETY_BOUNDS: SafetyBounds = SafetyBounds {
    errors: 0,
    timeouts: 0,
    unexpected_duplicates: 0,
    wrong_identities: 0,
    wrong_digests: 0,
    queue_depth_after_drain: 0,
    lag_after_drain: 0,
    hard_resource_limit_exceeded: false,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SafetyControl {
    ForbiddenError,
    Timeout,
    UnexpectedDuplicate,
    WrongIdentity,
    WrongDigest,
    UndrainedQueue,
    UndrainedLag,
    HardResourceLimit,
}

pub const SAFETY_CONTROLS: [SafetyControl; 8] = [
    SafetyControl::ForbiddenError,
    SafetyControl::Timeout,
    SafetyControl::UnexpectedDuplicate,
    SafetyControl::WrongIdentity,
    SafetyControl::WrongDigest,
    SafetyControl::UndrainedQueue,
    SafetyControl::UndrainedLag,
    SafetyControl::HardResourceLimit,
];

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SafetyObservation {
    pub control: SafetyControl,
    pub observation: RawObservation,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SafetyArtifact {
    pub schema: String,
    pub candidate_sha256: String,
    pub axes: ComparableAxes,
    pub bounds_sha256: String,
    pub controls: Vec<SafetyObservation>,
    pub passed: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FreezeRecord {
    pub schema: String,
    pub candidate_sha256: String,
    pub axes: ComparableAxes,
    pub bounds_sha256: String,
    pub safety_sha256: String,
    pub frozen_at_unix_millis: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OverallVerdict {
    Pass,
    PerformanceRegression,
    SafetyFailure,
    Incomparable,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceFailure {
    pub sample_index: u64,
    pub metric: String,
    pub observed_fraction: f64,
    pub bound_fraction: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PairedArtifact {
    pub schema: String,
    pub candidate_sha256: String,
    pub axes: ComparableAxes,
    pub bounds_sha256: String,
    pub freeze_sha256: String,
    pub claim_scope: ClaimScope,
    pub direct: Vec<RawObservation>,
    pub full: Vec<RawObservation>,
    pub safety_failures: Vec<SafetyFailure>,
    pub failures: Vec<PerformanceFailure>,
    pub incomparability_reasons: Vec<String>,
    pub verdict: OverallVerdict,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SafetyFailure {
    pub arm: Arm,
    pub sample_index: u64,
    pub controls: Vec<SafetyControl>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimScope {
    Loopback,
    Container,
    PhysicalLimited,
    PhysicalMultiHostReplicated,
}

pub fn build_bounds(
    candidate_sha256: &str,
    left: Vec<RawObservation>,
    right: Vec<RawObservation>,
    aa_source_sha256: &str,
    frozen_at_unix_millis: u64,
) -> Result<BoundsArtifact> {
    anyhow::ensure!(!candidate_sha256.is_empty(), "candidate digest is empty");
    anyhow::ensure!(!aa_source_sha256.is_empty(), "A/A source digest is empty");
    let axes = validate_pairs(&left, &right, Some(Arm::Direct), Some(Arm::Direct))?;

    let mut bounds = MetricBounds {
        throughput_fraction: 0.0,
        p50_fraction: 0.0,
        p95_fraction: 0.0,
        p99_fraction: 0.0,
        peak_rss_fraction: 0.0,
        disk_growth_fraction: 0.0,
    };
    for (a, b) in left.iter().zip(&right) {
        anyhow::ensure!(
            is_safe(&a.metrics)
                && is_safe(&b.metrics)
                && has_completed_measurement(&a.metrics)
                && has_completed_measurement(&b.metrics),
            "unsafe A/A sample"
        );
        bounds.throughput_fraction = bounds.throughput_fraction.max(relative_delta(
            a.metrics.throughput_per_second,
            b.metrics.throughput_per_second,
        ));
        bounds.p50_fraction = bounds.p50_fraction.max(relative_delta(
            a.metrics.latency_micros.p50 as f64,
            b.metrics.latency_micros.p50 as f64,
        ));
        bounds.p95_fraction = bounds.p95_fraction.max(relative_delta(
            a.metrics.latency_micros.p95 as f64,
            b.metrics.latency_micros.p95 as f64,
        ));
        bounds.p99_fraction = bounds.p99_fraction.max(relative_delta(
            a.metrics.latency_micros.p99 as f64,
            b.metrics.latency_micros.p99 as f64,
        ));
        bounds.peak_rss_fraction = bounds.peak_rss_fraction.max(relative_delta(
            a.metrics.peak_rss_bytes as f64,
            b.metrics.peak_rss_bytes as f64,
        ));
        bounds.disk_growth_fraction = bounds.disk_growth_fraction.max(relative_delta(
            a.metrics.disk_growth_bytes as f64,
            b.metrics.disk_growth_bytes as f64,
        ));
    }
    Ok(BoundsArtifact {
        schema: BOUNDS_SCHEMA.into(),
        candidate_sha256: candidate_sha256.into(),
        axes,
        aa_source_sha256: aa_source_sha256.into(),
        frozen_at_unix_millis,
        estimator: BOUND_ESTIMATOR.into(),
        safety_policy: SAFETY_POLICY.into(),
        bounds,
        safety_bounds: ZERO_SAFETY_BOUNDS,
    })
}

pub fn build_safety(
    candidate_sha256: &str,
    bounds: &BoundsArtifact,
    bounds_sha256: &str,
    controls: Vec<SafetyObservation>,
) -> Result<SafetyArtifact> {
    validate_bounds(candidate_sha256, bounds, bounds_sha256)?;
    anyhow::ensure!(
        controls.len() == SAFETY_CONTROLS.len(),
        "incomplete safety controls"
    );
    for expected in SAFETY_CONTROLS {
        let matching = controls
            .iter()
            .filter(|entry| entry.control == expected)
            .collect::<Vec<_>>();
        anyhow::ensure!(matching.len() == 1, "missing or duplicate safety control");
        let observation = &matching[0].observation;
        validate_observation(observation)?;
        anyhow::ensure!(observation.axes == bounds.axes, "safety axes differ");
        anyhow::ensure!(
            observation.observed_axes_begin == bounds.axes
                && observation.observed_axes_finish == bounds.axes,
            "observed safety axes differ"
        );
        anyhow::ensure!(observation.arm == Arm::Full, "safety arm is not Full");
        anyhow::ensure!(
            violated_controls(&observation.metrics) == vec![expected],
            "safety control was not detected in isolation"
        );
    }
    Ok(SafetyArtifact {
        schema: SAFETY_SCHEMA.into(),
        candidate_sha256: candidate_sha256.into(),
        axes: bounds.axes.clone(),
        bounds_sha256: bounds_sha256.into(),
        controls,
        passed: true,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn build_paired(
    candidate_sha256: &str,
    bounds: &BoundsArtifact,
    bounds_sha256: &str,
    freeze: &FreezeRecord,
    freeze_sha256: &str,
    direct: Vec<RawObservation>,
    full: Vec<RawObservation>,
) -> Result<PairedArtifact> {
    validate_freeze(
        candidate_sha256,
        bounds,
        bounds_sha256,
        freeze,
        freeze_sha256,
    )?;
    let incomparability_reasons = paired_incomparability_reasons(&direct, &full, &bounds.axes)?;

    let safety_failures = direct
        .iter()
        .chain(&full)
        .filter_map(|observation| {
            let controls = violated_controls(&observation.metrics);
            (!controls.is_empty()).then_some(SafetyFailure {
                arm: observation.arm,
                sample_index: observation.sample_index,
                controls,
            })
        })
        .collect::<Vec<_>>();
    let mut failures = Vec::new();
    if incomparability_reasons.is_empty() {
        for (direct, full) in direct.iter().zip(&full) {
            push_lower_failure(
                &mut failures,
                direct.sample_index,
                "throughput_per_second",
                direct.metrics.throughput_per_second,
                full.metrics.throughput_per_second,
                bounds.bounds.throughput_fraction,
            );
            push_upper_failure(
                &mut failures,
                direct.sample_index,
                "latency_p50_micros",
                direct.metrics.latency_micros.p50 as f64,
                full.metrics.latency_micros.p50 as f64,
                bounds.bounds.p50_fraction,
            );
            push_upper_failure(
                &mut failures,
                direct.sample_index,
                "latency_p95_micros",
                direct.metrics.latency_micros.p95 as f64,
                full.metrics.latency_micros.p95 as f64,
                bounds.bounds.p95_fraction,
            );
            push_upper_failure(
                &mut failures,
                direct.sample_index,
                "latency_p99_micros",
                direct.metrics.latency_micros.p99 as f64,
                full.metrics.latency_micros.p99 as f64,
                bounds.bounds.p99_fraction,
            );
            push_upper_failure(
                &mut failures,
                direct.sample_index,
                "peak_rss_bytes",
                direct.metrics.peak_rss_bytes as f64,
                full.metrics.peak_rss_bytes as f64,
                bounds.bounds.peak_rss_fraction,
            );
            push_upper_failure(
                &mut failures,
                direct.sample_index,
                "disk_growth_bytes",
                direct.metrics.disk_growth_bytes as f64,
                full.metrics.disk_growth_bytes as f64,
                bounds.bounds.disk_growth_fraction,
            );
        }
    }
    let verdict = if !incomparability_reasons.is_empty() {
        OverallVerdict::Incomparable
    } else if !safety_failures.is_empty() {
        OverallVerdict::SafetyFailure
    } else if failures.is_empty() {
        OverallVerdict::Pass
    } else {
        OverallVerdict::PerformanceRegression
    };
    Ok(PairedArtifact {
        schema: PAIRED_SCHEMA.into(),
        candidate_sha256: candidate_sha256.into(),
        axes: bounds.axes.clone(),
        bounds_sha256: bounds_sha256.into(),
        freeze_sha256: freeze_sha256.into(),
        claim_scope: claim_scope(&bounds.axes),
        direct,
        full,
        safety_failures,
        failures,
        incomparability_reasons,
        verdict,
    })
}

fn validate_pairs(
    left: &[RawObservation],
    right: &[RawObservation],
    left_arm: Option<Arm>,
    right_arm: Option<Arm>,
) -> Result<ComparableAxes> {
    anyhow::ensure!(!left.is_empty(), "observation set is empty");
    anyhow::ensure!(left.len() == right.len(), "paired sample counts differ");
    let axes = left[0].axes.clone();
    validate_axes(&axes)?;
    let mut indexes = BTreeSet::new();
    for (a, b) in left.iter().zip(right) {
        validate_observation(a)?;
        validate_observation(b)?;
        anyhow::ensure!(a.axes == axes && b.axes == axes, "comparison axes differ");
        anyhow::ensure!(
            a.observed_axes_begin == axes
                && a.observed_axes_finish == axes
                && b.observed_axes_begin == axes
                && b.observed_axes_finish == axes,
            "observed comparison axes differ"
        );
        anyhow::ensure!(
            a.sample_index == b.sample_index,
            "paired sample indexes differ"
        );
        anyhow::ensure!(indexes.insert(a.sample_index), "duplicate sample index");
        if let Some(expected) = left_arm {
            anyhow::ensure!(a.arm == expected, "left arm differs");
        }
        if let Some(expected) = right_arm {
            anyhow::ensure!(b.arm == expected, "right arm differs");
        }
    }
    Ok(axes)
}

fn paired_incomparability_reasons(
    direct: &[RawObservation],
    full: &[RawObservation],
    expected_axes: &ComparableAxes,
) -> Result<Vec<String>> {
    let mut reasons = BTreeSet::new();
    if direct.is_empty() || full.is_empty() {
        reasons.insert("paired observation set is empty".to_owned());
    }
    if direct.len() != full.len() {
        reasons.insert("paired sample counts differ".to_owned());
    }

    let mut direct_indexes = BTreeSet::new();
    let mut full_indexes = BTreeSet::new();
    let mut observation_ids = BTreeSet::new();
    for (label, expected_arm, observations, indexes) in [
        ("direct", Arm::Direct, direct, &mut direct_indexes),
        ("full", Arm::Full, full, &mut full_indexes),
    ] {
        for observation in observations {
            validate_observation(observation)?;
            if observation.arm != expected_arm {
                reasons.insert(format!("{label} arm differs"));
            }
            if observation.axes != *expected_axes {
                reasons.insert(format!("{label} declared axes differ"));
            }
            if observation.observed_axes_begin != observation.axes {
                reasons.insert(format!("{label} begin axes differ"));
            }
            if observation.observed_axes_finish != observation.axes {
                reasons.insert(format!("{label} finish axes differ"));
            }
            if !indexes.insert(observation.sample_index) {
                reasons.insert(format!("{label} sample index is duplicated"));
            }
            if !observation_ids.insert(&observation.observation_id) {
                reasons.insert("observation identity is duplicated".to_owned());
            }
            if !has_completed_measurement(&observation.metrics) {
                reasons.insert(format!("{label} sample has no completed measurement"));
            }
        }
    }
    for (direct, full) in direct.iter().zip(full) {
        if direct.sample_index != full.sample_index {
            reasons.insert("paired sample indexes differ".to_owned());
        }
    }
    Ok(reasons.into_iter().collect())
}

pub fn validate_bounds(
    candidate_sha256: &str,
    bounds: &BoundsArtifact,
    bounds_sha256: &str,
) -> Result<()> {
    anyhow::ensure!(!candidate_sha256.is_empty(), "candidate digest is empty");
    anyhow::ensure!(!bounds_sha256.is_empty(), "bounds digest is empty");
    anyhow::ensure!(bounds.schema == BOUNDS_SCHEMA, "unknown bounds schema");
    anyhow::ensure!(
        bounds.estimator == BOUND_ESTIMATOR,
        "unknown bound estimator"
    );
    anyhow::ensure!(
        bounds.safety_policy == SAFETY_POLICY,
        "unknown safety policy"
    );
    anyhow::ensure!(
        bounds.safety_bounds == ZERO_SAFETY_BOUNDS,
        "safety bounds are not the frozen zero-failure policy"
    );
    anyhow::ensure!(
        bounds.candidate_sha256 == candidate_sha256,
        "bounds candidate differs"
    );
    for bound in [
        bounds.bounds.throughput_fraction,
        bounds.bounds.p50_fraction,
        bounds.bounds.p95_fraction,
        bounds.bounds.p99_fraction,
        bounds.bounds.peak_rss_fraction,
        bounds.bounds.disk_growth_fraction,
    ] {
        anyhow::ensure!(
            bound.is_finite() && (0.0..=1.0).contains(&bound),
            "invalid bound"
        );
    }
    validate_axes(&bounds.axes)
}

pub fn validate_freeze(
    candidate_sha256: &str,
    bounds: &BoundsArtifact,
    bounds_sha256: &str,
    freeze: &FreezeRecord,
    freeze_sha256: &str,
) -> Result<()> {
    validate_bounds(candidate_sha256, bounds, bounds_sha256)?;
    anyhow::ensure!(!freeze_sha256.is_empty(), "freeze digest is empty");
    anyhow::ensure!(freeze.schema == FREEZE_SCHEMA, "unknown freeze schema");
    anyhow::ensure!(
        freeze.candidate_sha256 == candidate_sha256,
        "freeze candidate differs"
    );
    anyhow::ensure!(freeze.axes == bounds.axes, "freeze axes differ");
    anyhow::ensure!(
        freeze.bounds_sha256 == bounds_sha256,
        "freeze bounds differ"
    );
    anyhow::ensure!(
        !freeze.safety_sha256.is_empty(),
        "freeze lacks safety evidence"
    );
    anyhow::ensure!(
        freeze.frozen_at_unix_millis >= bounds.frozen_at_unix_millis,
        "freeze predates A/A bounds"
    );
    Ok(())
}

fn validate_axes(axes: &ComparableAxes) -> Result<()> {
    for value in [
        &axes.host_fingerprint,
        &axes.server_image_digest,
        &axes.server_source_digest,
        &axes.server_config_digest,
        &axes.payload_digest,
        &axes.partition_set_digest,
        &axes.client_placement,
    ] {
        anyhow::ensure!(!value.is_empty(), "comparison axis is empty");
    }
    anyhow::ensure!(axes.payload_bytes > 0, "payload size is zero");
    anyhow::ensure!(axes.offered_load_per_second > 0, "offered load is zero");
    anyhow::ensure!(axes.measure_millis > 0, "measurement duration is zero");
    anyhow::ensure!(axes.drain_millis > 0, "drain duration is zero");
    anyhow::ensure!(axes.host_count > 0, "host count is zero");
    anyhow::ensure!(axes.broker_count > 0, "broker count is zero");
    anyhow::ensure!(axes.replication_factor > 0, "replication factor is zero");
    Ok(())
}

fn validate_observation(observation: &RawObservation) -> Result<()> {
    validate_axes(&observation.axes)?;
    anyhow::ensure!(
        !observation.observation_id.is_empty(),
        "observation identity is empty"
    );
    let metrics = &observation.metrics;
    anyhow::ensure!(metrics.elapsed_nanos > 0, "elapsed duration is zero");
    anyhow::ensure!(
        metrics.throughput_per_second.is_finite(),
        "throughput is not finite"
    );
    anyhow::ensure!(
        metrics.throughput_per_second >= 0.0,
        "throughput is negative"
    );
    anyhow::ensure!(
        metrics.latency_micros.p50 <= metrics.latency_micros.p95
            && metrics.latency_micros.p95 <= metrics.latency_micros.p99,
        "latency percentiles are unordered"
    );
    anyhow::ensure!(
        metrics.completed_operations == 0 || !metrics.raw_latency_micros.is_empty(),
        "raw latency vector is empty"
    );
    Ok(())
}

fn has_completed_measurement(metrics: &MetricVector) -> bool {
    metrics.completed_operations > 0 && !metrics.raw_latency_micros.is_empty()
}

fn is_safe(metrics: &MetricVector) -> bool {
    violated_controls(metrics).is_empty()
}

fn violated_controls(metrics: &MetricVector) -> Vec<SafetyControl> {
    SAFETY_CONTROLS
        .into_iter()
        .filter(|control| control_detected(*control, metrics))
        .collect()
}

fn control_detected(control: SafetyControl, metrics: &MetricVector) -> bool {
    match control {
        SafetyControl::ForbiddenError => metrics.errors > 0,
        SafetyControl::Timeout => metrics.timeouts > 0,
        SafetyControl::UnexpectedDuplicate => metrics.unexpected_duplicates > 0,
        SafetyControl::WrongIdentity => metrics.wrong_identities > 0,
        SafetyControl::WrongDigest => metrics.wrong_digests > 0,
        SafetyControl::UndrainedQueue => metrics.queue_depth_after_drain > 0,
        SafetyControl::UndrainedLag => metrics.lag_after_drain > 0,
        SafetyControl::HardResourceLimit => metrics.hard_resource_limit_exceeded,
    }
}

fn relative_delta(a: f64, b: f64) -> f64 {
    (a - b).abs() / a.abs().max(b.abs()).max(1.0)
}

fn push_lower_failure(
    failures: &mut Vec<PerformanceFailure>,
    sample_index: u64,
    metric: &str,
    direct: f64,
    full: f64,
    bound: f64,
) {
    let observed = ((direct - full) / direct.abs().max(full.abs()).max(1.0)).max(0.0);
    if observed > bound + 1e-12 {
        failures.push(PerformanceFailure {
            sample_index,
            metric: metric.into(),
            observed_fraction: observed,
            bound_fraction: bound,
        });
    }
}

fn push_upper_failure(
    failures: &mut Vec<PerformanceFailure>,
    sample_index: u64,
    metric: &str,
    direct: f64,
    full: f64,
    bound: f64,
) {
    let observed = ((full - direct) / direct.abs().max(full.abs()).max(1.0)).max(0.0);
    if observed > bound + 1e-12 {
        failures.push(PerformanceFailure {
            sample_index,
            metric: metric.into(),
            observed_fraction: observed,
            bound_fraction: bound,
        });
    }
}

fn claim_scope(axes: &ComparableAxes) -> ClaimScope {
    match axes.execution_class {
        ExecutionClass::Loopback => ClaimScope::Loopback,
        ExecutionClass::Container => ClaimScope::Container,
        ExecutionClass::Physical
            if axes.host_count > 1 && axes.broker_count > 1 && axes.replication_factor > 1 =>
        {
            ClaimScope::PhysicalMultiHostReplicated
        }
        ExecutionClass::Physical => ClaimScope::PhysicalLimited,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn axes() -> ComparableAxes {
        ComparableAxes {
            host_fingerprint: "host".into(),
            server_image_digest: "image".into(),
            server_source_digest: "source".into(),
            server_config_digest: "config".into(),
            payload_digest: "payload".into(),
            payload_bytes: 128,
            partition_set_digest: "partitions".into(),
            full_confirmation_profile: FullConfirmationProfile::BrokerAccepted,
            offered_load_per_second: 100,
            warmup_millis: 1_000,
            measure_millis: 5_000,
            drain_millis: 500,
            client_placement: "same-host".into(),
            execution_class: ExecutionClass::Loopback,
            host_count: 1,
            broker_count: 1,
            replication_factor: 1,
        }
    }

    fn metrics(throughput: f64) -> MetricVector {
        MetricVector {
            elapsed_nanos: 1_000_000_000,
            completed_operations: 4,
            throughput_per_second: throughput,
            latency_micros: LatencyVector {
                p50: 20,
                p95: 30,
                p99: 30,
            },
            errors: 0,
            timeouts: 0,
            unexpected_duplicates: 0,
            wrong_identities: 0,
            wrong_digests: 0,
            peak_rss_bytes: 1_000,
            queue_depth_after_drain: 0,
            lag_after_drain: 0,
            disk_growth_bytes: 100,
            hard_resource_limit_exceeded: false,
            raw_latency_micros: vec![10, 20, 30, 40],
        }
    }

    fn observation(arm: Arm, index: u64, throughput: f64) -> RawObservation {
        let axes = axes();
        RawObservation {
            observed_axes_begin: axes.clone(),
            observed_axes_finish: axes.clone(),
            axes,
            observation_id: format!("observation-{arm:?}-{index}"),
            arm,
            sample_index: index,
            metrics: metrics(throughput),
        }
    }

    fn bounds() -> BoundsArtifact {
        build_bounds(
            "candidate",
            vec![observation(Arm::Direct, 0, 4.0)],
            vec![observation(Arm::Direct, 0, 3.8)],
            "aa-source",
            10,
        )
        .unwrap()
    }

    fn detected(control: SafetyControl) -> SafetyObservation {
        let mut observation = observation(Arm::Full, control as u64, 4.0);
        match control {
            SafetyControl::ForbiddenError => observation.metrics.errors = 1,
            SafetyControl::Timeout => observation.metrics.timeouts = 1,
            SafetyControl::UnexpectedDuplicate => observation.metrics.unexpected_duplicates = 1,
            SafetyControl::WrongIdentity => observation.metrics.wrong_identities = 1,
            SafetyControl::WrongDigest => observation.metrics.wrong_digests = 1,
            SafetyControl::UndrainedQueue => observation.metrics.queue_depth_after_drain = 1,
            SafetyControl::UndrainedLag => observation.metrics.lag_after_drain = 1,
            SafetyControl::HardResourceLimit => {
                observation.metrics.hard_resource_limit_exceeded = true
            }
        }
        SafetyObservation {
            control,
            observation,
        }
    }

    fn freeze(bounds: &BoundsArtifact) -> FreezeRecord {
        FreezeRecord {
            schema: FREEZE_SCHEMA.into(),
            candidate_sha256: "candidate".into(),
            axes: bounds.axes.clone(),
            bounds_sha256: "bounds".into(),
            safety_sha256: "safety".into(),
            frozen_at_unix_millis: 20,
        }
    }

    #[test]
    fn v07_task_6_12_aa_freezes_exact_axis_bounds_and_preserves_source() {
        let artifact = bounds();
        assert_eq!(artifact.aa_source_sha256, "aa-source");
        assert!((artifact.bounds.throughput_fraction - 0.05).abs() < f64::EPSILON);
        assert_eq!(artifact.safety_bounds, ZERO_SAFETY_BOUNDS);
        assert_eq!(artifact.frozen_at_unix_millis, 10);

        let mut weakened = artifact.clone();
        weakened.safety_bounds.errors = 1;
        assert!(validate_bounds("candidate", &weakened, "bounds").is_err());
    }

    #[test]
    fn v07_task_6_12_aa_rejects_every_incomparable_axis_and_arm() {
        let baseline = observation(Arm::Direct, 0, 4.0);
        let mut mutations = Vec::new();
        macro_rules! mutation {
            ($field:ident, $value:expr) => {{
                let mut changed = baseline.clone();
                changed.axes.$field = $value;
                mutations.push(changed);
            }};
        }
        mutation!(host_fingerprint, "other".into());
        mutation!(server_image_digest, "other".into());
        mutation!(server_source_digest, "other".into());
        mutation!(server_config_digest, "other".into());
        mutation!(payload_digest, "other".into());
        mutation!(payload_bytes, 129);
        mutation!(partition_set_digest, "other".into());
        mutation!(
            full_confirmation_profile,
            FullConfirmationProfile::OsSyncedAccepted
        );
        mutation!(offered_load_per_second, 101);
        mutation!(warmup_millis, 1_001);
        mutation!(measure_millis, 5_001);
        mutation!(drain_millis, 501);
        mutation!(client_placement, "other".into());
        mutation!(execution_class, ExecutionClass::Container);
        mutation!(host_count, 2);
        mutation!(broker_count, 2);
        mutation!(replication_factor, 2);
        let mut wrong_arm = baseline.clone();
        wrong_arm.arm = Arm::Full;
        mutations.push(wrong_arm);

        for changed in mutations {
            assert!(
                build_bounds("candidate", vec![baseline.clone()], vec![changed], "raw", 1).is_err()
            );
        }

        assert!(
            build_bounds(
                "candidate",
                vec![observation(Arm::Full, 0, 4.0)],
                vec![observation(Arm::Full, 0, 4.0)],
                "raw",
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn v07_task_6_12_safety_ablation_requires_and_detects_every_red_control() {
        let bounds = bounds();
        let controls = SAFETY_CONTROLS.map(detected).to_vec();
        assert!(
            build_safety("candidate", &bounds, "bounds", controls.clone())
                .unwrap()
                .passed
        );
        for omitted in SAFETY_CONTROLS {
            let incomplete = controls
                .iter()
                .filter(|entry| entry.control != omitted)
                .cloned()
                .collect();
            assert!(build_safety("candidate", &bounds, "bounds", incomplete).is_err());

            let mut undetected = controls.clone();
            undetected
                .iter_mut()
                .find(|entry| entry.control == omitted)
                .unwrap()
                .observation
                .metrics = metrics(4.0);
            assert!(build_safety("candidate", &bounds, "bounds", undetected).is_err());
        }

        let mut non_isolated = controls;
        non_isolated[0].observation.metrics.timeouts = 1;
        assert!(build_safety("candidate", &bounds, "bounds", non_isolated).is_err());
    }

    #[test]
    fn v07_task_6_12_paired_keeps_raw_vectors_and_safety_overrides_fast_results() {
        let bounds = bounds();
        let freeze = freeze(&bounds);
        for control in SAFETY_CONTROLS {
            let direct = observation(Arm::Direct, 0, 4.0);
            let mut full = detected(control).observation;
            full.sample_index = 0;
            full.metrics.throughput_per_second = 8.0;
            let artifact = build_paired(
                "candidate",
                &bounds,
                "bounds",
                &freeze,
                "freeze",
                vec![direct],
                vec![full],
            )
            .unwrap();
            assert_eq!(artifact.verdict, OverallVerdict::SafetyFailure);
            assert!(!artifact.safety_failures.is_empty());
            assert_eq!(
                artifact.full[0].metrics.raw_latency_micros,
                vec![10, 20, 30, 40]
            );
        }
    }

    #[test]
    fn v07_task_6_12_paired_records_incomparable_axes_and_arms() {
        let bounds = bounds();
        let freeze = freeze(&bounds);
        let direct = observation(Arm::Direct, 0, 4.0);
        let mut full = observation(Arm::Full, 0, 4.0);
        full.axes.payload_bytes += 1;
        let artifact = build_paired(
            "candidate",
            &bounds,
            "bounds",
            &freeze,
            "freeze",
            vec![direct],
            vec![full],
        )
        .unwrap();
        assert_eq!(artifact.verdict, OverallVerdict::Incomparable);
        assert!(!artifact.incomparability_reasons.is_empty());
        assert_eq!(artifact.direct.len(), 1);
        assert_eq!(artifact.full.len(), 1);
        let mut wrong_arm = observation(Arm::Direct, 0, 4.0);
        wrong_arm.arm = Arm::Full;
        let artifact = build_paired(
            "candidate",
            &bounds,
            "bounds",
            &freeze,
            "freeze",
            vec![wrong_arm],
            vec![observation(Arm::Full, 0, 4.0)],
        )
        .unwrap();
        assert_eq!(artifact.verdict, OverallVerdict::Incomparable);
        let artifact = build_paired(
            "candidate",
            &bounds,
            "bounds",
            &freeze,
            "freeze",
            vec![
                observation(Arm::Direct, 0, 4.0),
                observation(Arm::Direct, 1, 4.0),
            ],
            vec![observation(Arm::Full, 0, 4.0)],
        )
        .unwrap();
        assert_eq!(artifact.verdict, OverallVerdict::Incomparable);
        assert_eq!(artifact.direct.len(), 2);
        assert!(
            artifact
                .incomparability_reasons
                .iter()
                .any(|reason| reason.contains("counts"))
        );
        assert!(
            build_paired(
                "other-candidate",
                &bounds,
                "bounds",
                &freeze,
                "freeze",
                vec![observation(Arm::Direct, 0, 4.0)],
                vec![observation(Arm::Full, 0, 4.0)]
            )
            .is_err()
        );
    }

    #[test]
    fn v07_task_6_12_paired_uses_only_frozen_aa_bounds() {
        let bounds = bounds();
        let freeze = freeze(&bounds);
        let artifact = build_paired(
            "candidate",
            &bounds,
            "bounds",
            &freeze,
            "freeze",
            vec![observation(Arm::Direct, 0, 4.0)],
            vec![observation(Arm::Full, 0, 3.0)],
        )
        .unwrap();
        assert_eq!(artifact.verdict, OverallVerdict::PerformanceRegression);
        assert_eq!(artifact.claim_scope, ClaimScope::Loopback);
        assert!(!artifact.failures.is_empty());
    }

    #[test]
    fn v07_task_6_12_limited_environments_are_not_promoted_to_physical_replication() {
        let bounds = bounds();
        let freeze = freeze(&bounds);
        let mut direct = observation(Arm::Direct, 0, 4.0);
        direct.axes.execution_class = ExecutionClass::Container;
        direct.axes.host_count = 3;
        direct.axes.broker_count = 3;
        direct.axes.replication_factor = 3;
        direct.observed_axes_begin = direct.axes.clone();
        direct.observed_axes_finish = direct.axes.clone();
        let mut full = direct.clone();
        full.arm = Arm::Full;
        let mut matching_bounds = bounds;
        matching_bounds.axes = direct.axes.clone();
        let mut matching_freeze = freeze;
        matching_freeze.axes = direct.axes.clone();
        let artifact = build_paired(
            "candidate",
            &matching_bounds,
            "bounds",
            &matching_freeze,
            "freeze",
            vec![direct],
            vec![full],
        )
        .unwrap();
        assert_eq!(artifact.claim_scope, ClaimScope::Container);
    }

    #[test]
    fn v07_task_6_12_paired_uses_the_frozen_symmetric_estimator() {
        let mut bounds = bounds();
        bounds.bounds.p50_fraction = relative_delta(100.0, 200.0);
        let freeze = freeze(&bounds);
        let mut direct = observation(Arm::Direct, 0, 4.0);
        direct.metrics.latency_micros = LatencyVector {
            p50: 100,
            p95: 200,
            p99: 200,
        };
        let mut full = observation(Arm::Full, 0, 4.0);
        full.metrics.latency_micros = LatencyVector {
            p50: 200,
            p95: 200,
            p99: 200,
        };
        let artifact = build_paired(
            "candidate",
            &bounds,
            "bounds",
            &freeze,
            "freeze",
            vec![direct],
            vec![full],
        )
        .unwrap();
        assert!(
            artifact
                .failures
                .iter()
                .all(|failure| failure.metric != "latency_p50_micros")
        );
    }

    #[test]
    fn v07_task_6_12_paired_preserves_observed_axis_drift_as_incomparable() {
        let bounds = bounds();
        let freeze = freeze(&bounds);
        let direct = observation(Arm::Direct, 0, 4.0);
        let mut full = observation(Arm::Full, 0, 4.0);
        full.observed_axes_finish.host_fingerprint = "drifted-host".into();
        let artifact = build_paired(
            "candidate",
            &bounds,
            "bounds",
            &freeze,
            "freeze",
            vec![direct],
            vec![full],
        )
        .unwrap();
        assert_eq!(artifact.verdict, OverallVerdict::Incomparable);
        assert!(
            artifact
                .incomparability_reasons
                .iter()
                .any(|reason| reason.contains("finish axes"))
        );
    }
}
