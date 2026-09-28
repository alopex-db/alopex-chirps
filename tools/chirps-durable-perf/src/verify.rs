//! Read-only replay of performance evidence. Never invokes the audit program.
use crate::evidence::*;
use crate::{control_name, exact_operation_count, observation_id_for_axes, percentiles, sha256};
use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use std::{fs, path::Path};

pub fn verify(root: &Path, candidate: &str, axes: &ComparableAxes, samples: u64) -> Result<()> {
    anyhow::ensure!(samples > 0, "sample count is zero");
    crate::validate_direct_boundary(axes.full_confirmation_profile)?;
    let (aa, aa_hash): (AaArtifact, _) = read(root, "aa/aa.json")?;
    let (bounds, bounds_hash): (BoundsArtifact, _) = read(root, "aa/bounds.json")?;
    let (safety, safety_hash): (SafetyArtifact, _) = read(root, "safety/safety.json")?;
    let (freeze, freeze_hash): (FreezeRecord, _) = read(root, "safety/freeze.json")?;
    let (paired, _): (PairedArtifact, _) = read(root, "paired/paired.json")?;
    anyhow::ensure!(
        aa.schema == AA_SCHEMA && aa.candidate_sha256 == candidate && &aa.axes == axes,
        "A/A identity differs from candidate"
    );
    observations(&aa.left, axes, samples, "aa_left", Arm::Direct)?;
    observations(&aa.right, axes, samples, "aa_right", Arm::Direct)?;
    let rebuilt = build_bounds(
        candidate,
        aa.left,
        aa.right,
        &aa_hash,
        bounds.frozen_at_unix_millis,
    )?;
    anyhow::ensure!(bounds == rebuilt, "bounds differ from A/A recomputation");
    for (index, control) in SAFETY_CONTROLS.into_iter().enumerate() {
        let observed = safety
            .controls
            .iter()
            .find(|value| value.control == control)
            .context("missing safety control")?;
        observation(
            &observed.observation,
            axes,
            index as u64,
            &format!("safety_{}", control_name(control)),
            Arm::Full,
            Some(control),
        )?;
    }
    let rebuilt = build_safety(candidate, &bounds, &bounds_hash, safety.controls.clone())?;
    anyhow::ensure!(safety == rebuilt, "safety differs from recomputation");
    anyhow::ensure!(
        freeze.safety_sha256 == safety_hash,
        "freeze safety digest differs"
    );
    validate_freeze(candidate, &bounds, &bounds_hash, &freeze, &freeze_hash)?;
    observations(&paired.direct, axes, samples, "paired_direct", Arm::Direct)?;
    observations(&paired.full, axes, samples, "paired_full", Arm::Full)?;
    let rebuilt = build_paired(
        candidate,
        &bounds,
        &bounds_hash,
        &freeze,
        &freeze_hash,
        paired.direct.clone(),
        paired.full.clone(),
    )?;
    anyhow::ensure!(
        paired == rebuilt,
        "paired result differs from recomputation"
    );
    anyhow::ensure!(
        rebuilt.verdict == OverallVerdict::Pass,
        "recomputed performance verdict is not Pass"
    );
    Ok(())
}

fn read<T: DeserializeOwned>(root: &Path, relative: &str) -> Result<(T, String)> {
    let bytes = fs::read(root.join(relative)).with_context(|| format!("read {relative}"))?;
    Ok((
        serde_json::from_slice(&bytes).with_context(|| format!("decode {relative}"))?,
        sha256(&bytes),
    ))
}

fn observations(
    values: &[RawObservation],
    axes: &ComparableAxes,
    samples: u64,
    phase: &str,
    arm: Arm,
) -> Result<()> {
    anyhow::ensure!(
        u64::try_from(values.len())? == samples,
        "sample count differs from candidate"
    );
    for (index, value) in values.iter().enumerate() {
        observation(value, axes, index as u64, phase, arm, None)?;
    }
    Ok(())
}

fn observation(
    value: &RawObservation,
    axes: &ComparableAxes,
    sample: u64,
    phase: &str,
    arm: Arm,
    control: Option<SafetyControl>,
) -> Result<()> {
    anyhow::ensure!(
        &value.axes == axes
            && &value.observed_axes_begin == axes
            && &value.observed_axes_finish == axes,
        "observation axes differ from candidate"
    );
    anyhow::ensure!(
        value.arm == arm
            && value.sample_index == sample
            && value.observation_id == observation_id_for_axes(axes, phase, sample, arm, control)?,
        "observation identity differs"
    );
    let metrics = &value.metrics;
    let operations = exact_operation_count(
        axes.offered_load_per_second,
        axes.measure_millis,
        "measurement",
    )?;
    anyhow::ensure!(
        u64::try_from(metrics.raw_latency_micros.len())? == operations,
        "raw latency count differs from offered operations"
    );
    anyhow::ensure!(
        metrics.completed_operations <= operations,
        "completed operations exceed arrivals"
    );
    anyhow::ensure!(
        metrics
            .completed_operations
            .saturating_add(metrics.errors)
            .saturating_add(metrics.timeouts)
            >= operations,
        "operation outcomes are missing"
    );
    let minimum = axes
        .measure_millis
        .checked_mul(1_000_000)
        .context("measurement window overflow")?;
    anyhow::ensure!(
        metrics.elapsed_nanos >= minimum,
        "elapsed duration shortens measurement window"
    );
    let throughput = metrics.completed_operations as f64
        / std::time::Duration::from_nanos(metrics.elapsed_nanos).as_secs_f64();
    anyhow::ensure!(
        metrics.throughput_per_second.is_finite()
            && (metrics.throughput_per_second - throughput).abs()
                <= throughput.abs().max(1.0) * 1e-12,
        "throughput differs from raw elapsed/count"
    );
    anyhow::ensure!(
        metrics.latency_micros == percentiles(&metrics.raw_latency_micros),
        "latency percentiles differ from raw samples"
    );
    anyhow::ensure!(metrics.peak_rss_bytes > 0, "RSS observation is missing");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde_json::{Value, json};
    use std::path::PathBuf;

    struct Fixture {
        root: PathBuf,
        axes: ComparableAxes,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }
    fn write(root: &Path, name: &str, value: &impl Serialize) -> String {
        let bytes = serde_json::to_vec_pretty(value).unwrap();
        fs::create_dir_all(root.join(name).parent().unwrap()).unwrap();
        fs::write(root.join(name), &bytes).unwrap();
        sha256(&bytes)
    }
    fn raw(
        axes: &ComparableAxes,
        phase: &str,
        arm: Arm,
        sample: u64,
        control: Option<SafetyControl>,
    ) -> RawObservation {
        RawObservation {
            axes: axes.clone(),
            observed_axes_begin: axes.clone(),
            observed_axes_finish: axes.clone(),
            observation_id: observation_id_for_axes(axes, phase, sample, arm, control).unwrap(),
            arm,
            sample_index: sample,
            metrics: MetricVector {
                elapsed_nanos: 1_000_000_000,
                completed_operations: 4,
                throughput_per_second: 4.0,
                latency_micros: percentiles(&[10, 20, 30, 40]),
                raw_latency_micros: vec![10, 20, 30, 40],
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
            },
        }
    }
    fn fixture() -> Fixture {
        let root = std::env::temp_dir().join(format!(
            "chirps-perf-verify-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let axes = ComparableAxes {
            host_fingerprint: "synthetic-host".into(),
            server_image_digest: "image".into(),
            server_source_digest: "source".into(),
            server_config_digest: "config".into(),
            payload_digest: "payload".into(),
            payload_bytes: 1,
            partition_set_digest: "partition".into(),
            full_confirmation_profile: FullConfirmationProfile::BrokerAccepted,
            offered_load_per_second: 4,
            warmup_millis: 0,
            measure_millis: 1_000,
            drain_millis: 1,
            client_placement: "same-host".into(),
            execution_class: ExecutionClass::Loopback,
            host_count: 1,
            broker_count: 1,
            replication_factor: 1,
        };
        let aa = AaArtifact {
            schema: AA_SCHEMA.into(),
            candidate_sha256: "candidate".into(),
            axes: axes.clone(),
            left: vec![raw(&axes, "aa_left", Arm::Direct, 0, None)],
            right: vec![raw(&axes, "aa_right", Arm::Direct, 0, None)],
        };
        let aa_hash = write(&root, "aa/aa.json", &aa);
        let bounds = build_bounds("candidate", aa.left, aa.right, &aa_hash, 10).unwrap();
        let bounds_hash = write(&root, "aa/bounds.json", &bounds);
        let controls = SAFETY_CONTROLS
            .into_iter()
            .enumerate()
            .map(|(index, control)| {
                let mut observation = raw(
                    &axes,
                    &format!("safety_{}", control_name(control)),
                    Arm::Full,
                    index as u64,
                    Some(control),
                );
                let metrics = &mut observation.metrics;
                match control {
                    SafetyControl::ForbiddenError => metrics.errors = 1,
                    SafetyControl::Timeout => metrics.timeouts = 1,
                    SafetyControl::UnexpectedDuplicate => metrics.unexpected_duplicates = 1,
                    SafetyControl::WrongIdentity => metrics.wrong_identities = 1,
                    SafetyControl::WrongDigest => metrics.wrong_digests = 1,
                    SafetyControl::UndrainedQueue => metrics.queue_depth_after_drain = 1,
                    SafetyControl::UndrainedLag => metrics.lag_after_drain = 1,
                    SafetyControl::HardResourceLimit => metrics.hard_resource_limit_exceeded = true,
                }
                SafetyObservation {
                    control,
                    observation,
                }
            })
            .collect();
        let safety = build_safety("candidate", &bounds, &bounds_hash, controls).unwrap();
        let safety_hash = write(&root, "safety/safety.json", &safety);
        let freeze = FreezeRecord {
            schema: FREEZE_SCHEMA.into(),
            candidate_sha256: "candidate".into(),
            axes: axes.clone(),
            bounds_sha256: bounds_hash.clone(),
            safety_sha256: safety_hash,
            frozen_at_unix_millis: 20,
        };
        let freeze_hash = write(&root, "safety/freeze.json", &freeze);
        let paired = build_paired(
            "candidate",
            &bounds,
            &bounds_hash,
            &freeze,
            &freeze_hash,
            vec![raw(&axes, "paired_direct", Arm::Direct, 0, None)],
            vec![raw(&axes, "paired_full", Arm::Full, 0, None)],
        )
        .unwrap();
        write(&root, "paired/paired.json", &paired);
        Fixture { root, axes }
    }

    #[test]
    fn replays_complete_chain_without_writing() {
        let f = fixture();
        let paths = [
            "aa/aa.json",
            "aa/bounds.json",
            "safety/safety.json",
            "safety/freeze.json",
            "paired/paired.json",
        ];
        let before: Vec<_> = paths
            .iter()
            .map(|path| fs::read(f.root.join(path)).unwrap())
            .collect();
        verify(&f.root, "candidate", &f.axes, 1).unwrap();
        let after: Vec<_> = paths
            .iter()
            .map(|path| fs::read(f.root.join(path)).unwrap())
            .collect();
        assert_eq!(before, after);
        assert!(verify(&f.root, "other", &f.axes, 1).is_err());
        assert!(verify(&f.root, "candidate", &f.axes, 2).is_err());
        assert!(verify(&f.root, "candidate", &f.axes, 0).is_err());
        let absent = f.root.join("absent");
        assert!(verify(&absent, "candidate", &f.axes, 1).is_err());
        assert!(!absent.exists());
    }

    #[test]
    fn rejects_tampered_chain_and_forged_pass_statistics() {
        let f = fixture();
        let mutations = [
            ("aa/aa.json", "/left/0/observation_id", json!("other")),
            ("aa/aa.json", "/schema", json!("other")),
            ("aa/bounds.json", "/bounds/p99_fraction", json!(1.0)),
            ("safety/safety.json", "/passed", json!(false)),
            (
                "safety/safety.json",
                "/controls/0/observation/metrics/errors",
                json!(0),
            ),
            ("safety/freeze.json", "/safety_sha256", json!("other")),
            ("safety/freeze.json", "/frozen_at_unix_millis", json!(9)),
            (
                "paired/paired.json",
                "/full/0/metrics/throughput_per_second",
                json!(100.0),
            ),
            (
                "paired/paired.json",
                "/full/0/metrics/latency_micros/p99",
                json!(39),
            ),
            (
                "paired/paired.json",
                "/full/0/metrics/raw_latency_micros",
                json!([10, 20, 30]),
            ),
            (
                "paired/paired.json",
                "/full/0/metrics/completed_operations",
                json!(5),
            ),
            (
                "paired/paired.json",
                "/full/0/metrics/elapsed_nanos",
                json!(999999999),
            ),
            (
                "paired/paired.json",
                "/full/0/metrics/peak_rss_bytes",
                json!(0),
            ),
            (
                "paired/paired.json",
                "/full/0/metrics/wrong_digests",
                json!(1),
            ),
            ("paired/paired.json", "/full/0/sample_index", json!(1)),
            (
                "paired/paired.json",
                "/full/0/observed_axes_finish/host_fingerprint",
                json!("other"),
            ),
            ("paired/paired.json", "/freeze_sha256", json!("other")),
            (
                "paired/paired.json",
                "/verdict",
                json!("performance_regression"),
            ),
        ];
        for (file, pointer, replacement) in mutations {
            let bytes = fs::read(f.root.join(file)).unwrap();
            let mut value: Value = serde_json::from_slice(&bytes).unwrap();
            *value.pointer_mut(pointer).unwrap() = replacement;
            write(&f.root, file, &value);
            assert!(
                verify(&f.root, "candidate", &f.axes, 1).is_err(),
                "accepted {file}{pointer}"
            );
            fs::write(f.root.join(file), bytes).unwrap();
        }
    }

    #[test]
    fn rejects_consistent_recomputed_regression_verdict() {
        let f = fixture();
        let (bounds, bounds_hash): (BoundsArtifact, _) = read(&f.root, "aa/bounds.json").unwrap();
        let (freeze, freeze_hash): (FreezeRecord, _) = read(&f.root, "safety/freeze.json").unwrap();
        let mut full = raw(&f.axes, "paired_full", Arm::Full, 0, None);
        full.metrics.elapsed_nanos *= 2;
        full.metrics.throughput_per_second /= 2.0;
        let paired = build_paired(
            "candidate",
            &bounds,
            &bounds_hash,
            &freeze,
            &freeze_hash,
            vec![raw(&f.axes, "paired_direct", Arm::Direct, 0, None)],
            vec![full],
        )
        .unwrap();
        assert_eq!(paired.verdict, OverallVerdict::PerformanceRegression);
        write(&f.root, "paired/paired.json", &paired);
        assert!(verify(&f.root, "candidate", &f.axes, 1).is_err());
    }
}
