use crate::schema::*;
use crate::statistics::{bootstrap_ci95_lower, median};
use anyhow::{Context, anyhow, ensure};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

const EPSILON: f64 = 1e-6;

#[derive(Clone, Debug)]
pub struct Verification {
    pub artifact: Artifact,
    pub computed_statistics: Statistics,
    pub computed_verdict: Verdict,
}

pub fn verify_artifact(path: &Path) -> anyhow::Result<Verification> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let artifact: Artifact = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse strict artifact {}", path.display()))?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    verify_identity(&artifact)?;
    verify_environment(&artifact.execution_environment)?;
    verify_config(&artifact.resolved_config)?;
    verify_raw_artifacts(base, &artifact)?;
    verify_samples(&artifact)?;
    let computed_statistics = compute_statistics(&artifact)?;
    compare_statistics(&artifact.statistics, &computed_statistics)?;
    let computed_verdict = compute_verdict(&artifact, &computed_statistics)?;
    compare_verdict(&artifact.verdict, &computed_verdict)?;
    Ok(Verification {
        artifact,
        computed_statistics,
        computed_verdict,
    })
}

pub fn assemble_artifact(input: ArtifactInput, base: &Path) -> anyhow::Result<Artifact> {
    let mut artifact = Artifact {
        schema: input.schema,
        commit_sha: input.commit_sha,
        binary_sha256: input.binary_sha256,
        runner_command: input.runner_command,
        execution_environment: input.execution_environment,
        resolved_config: input.resolved_config,
        samples: input.samples,
        per_group: input.per_group,
        raw_metrics_artifacts: input.raw_metrics_artifacts,
        raw_artifact_set_sha256: input.raw_artifact_set_sha256,
        statistics: Statistics {
            seed: STATISTICS_SEED.to_owned(),
            resamples: STATISTICS_RESAMPLES,
            multi_raft_median: 0.0,
            multi_raft_ci95_lower: 0.0,
            single_group_median: 0.0,
            overhead_ratio: 0.0,
        },
        verdict: Verdict {
            throughput: Gate::Fail,
            overhead: Gate::Fail,
            integrity: Gate::Fail,
            overall: Gate::Fail,
        },
    };
    verify_identity(&artifact)?;
    verify_environment(&artifact.execution_environment)?;
    verify_config(&artifact.resolved_config)?;
    verify_raw_artifacts(base, &artifact)?;
    verify_samples(&artifact)?;
    artifact.statistics = compute_statistics(&artifact)?;
    artifact.verdict = compute_verdict(&artifact, &artifact.statistics)?;
    Ok(artifact)
}

fn verify_identity(artifact: &Artifact) -> anyhow::Result<()> {
    ensure!(
        artifact.schema == SCHEMA,
        "unsupported schema {}",
        artifact.schema
    );
    ensure!(
        is_lower_hex(&artifact.commit_sha, 40),
        "commit_sha must be 40 lowercase hex"
    );
    ensure!(
        is_lower_hex(&artifact.binary_sha256, 64),
        "binary_sha256 must be 64 lowercase hex"
    );
    ensure!(
        artifact.runner_command
            == [
                "scripts/perf/run-controlled-container-multi-raft.sh",
                "--output",
                "<OUTPUT>"
            ],
        "runner_command is not the controlled entry point"
    );
    ensure!(
        artifact
            .runner_command
            .iter()
            .all(|part| !part.contains('\0')),
        "runner command contains NUL"
    );
    Ok(())
}

fn verify_environment(environment: &ExecutionEnvironment) -> anyhow::Result<()> {
    ensure!(
        environment.host_count == 1,
        "controlled profile requires one host"
    );
    ensure!(
        environment.logical_nodes == 3,
        "controlled profile requires three nodes"
    );
    ensure!(
        !environment.physical_deployment,
        "controlled-local artifact must not be physical deployment evidence"
    );
    ensure!(
        environment.cores > 0 && environment.ram_bytes > 0,
        "host resources are missing"
    );
    ensure!(
        environment.swap_bytes_after <= environment.swap_bytes_before,
        "swap grew during run"
    );
    ensure!(
        environment.process_or_container_ids.len() == 30
            && environment
                .process_or_container_ids
                .iter()
                .collect::<HashSet<_>>()
                .len()
                == 30,
        "expected three unique node container identities for each sample"
    );
    for (name, value) in [
        ("cpu", &environment.cpu),
        ("kernel", &environment.kernel),
        ("rust_version", &environment.rust_version),
        ("storage", &environment.storage),
        ("filesystem", &environment.filesystem),
        ("network_shaper", &environment.network_shaper),
        ("governor", &environment.governor),
    ] {
        ensure!(!value.trim().is_empty(), "environment {name} is empty");
    }
    ensure!(
        environment
            .node_cpu_sets
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
            == BTreeSet::from([1, 2, 3]),
        "node CPU sets must cover nodes 1,2,3"
    );
    ensure!(
        environment
            .loadgen_cpu_sets
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
            == BTreeSet::from([1, 2, 3]),
        "loadgen CPU sets must cover nodes 1,2,3"
    );
    let mut used = BTreeSet::new();
    for (label, sets) in [
        ("node", &environment.node_cpu_sets),
        ("loadgen", &environment.loadgen_cpu_sets),
    ] {
        for (node, value) in sets {
            for cpu in parse_cpu_set(value).with_context(|| format!("{label} {node} CPU set"))? {
                ensure!(used.insert(cpu), "CPU {cpu} is assigned more than once");
            }
        }
    }
    Ok(())
}

fn verify_config(config: &ResolvedConfig) -> anyhow::Result<()> {
    ensure!(config.nodes == 3, "nodes must be 3");
    ensure!(config.groups == 100, "groups must be 100");
    ensure!(config.payload_bytes == 1024, "payload must be 1024 bytes");
    ensure!(approx(config.rtt_ms, 1.0), "configured RTT must be 1.0 ms");
    ensure!(
        config.clients == 300 && config.clients_per_node == 100,
        "client allocation must be 3x100"
    );
    ensure!(config.warmup_seconds == 15, "warm-up must be 15 seconds");
    ensure!(
        config.measure_seconds == 60,
        "measurement must be 60 seconds"
    );
    ensure!(config.drain_seconds == 5, "drain must be 5 seconds");
    ensure!(config.samples == 5, "sample count must be 5 per mode");
    ensure!(config.fsync_interval == 0, "fsync_interval must be zero");
    ensure!(
        config.snapshot_threshold == 10_000,
        "snapshot threshold mismatch"
    );
    ensure!(
        config.send_queue_capacity == 4_096,
        "send queue capacity mismatch"
    );
    Ok(())
}

fn verify_raw_artifacts(base: &Path, artifact: &Artifact) -> anyhow::Result<()> {
    ensure!(
        !artifact.raw_metrics_artifacts.is_empty(),
        "raw artifact set is empty"
    );
    ensure!(
        artifact
            .raw_metrics_artifacts
            .windows(2)
            .all(|items| items[0].path < items[1].path),
        "raw artifacts must be in canonical path order"
    );
    let mut seen = HashSet::new();
    let mut set_hasher = Sha256::new();
    let mut node_files = BTreeSet::new();
    let mut raw_node_metrics = BTreeMap::new();
    let mut loadgen_reports: BTreeMap<(Mode, u64), Vec<LoadgenReport>> = BTreeMap::new();
    let mut paths_by_kind: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut raw_memberships = BTreeMap::new();
    let mut raw_rtts = BTreeMap::new();
    for raw in &artifact.raw_metrics_artifacts {
        ensure!(
            is_safe_relative_path(&raw.path),
            "unsafe raw path {}",
            raw.path
        );
        ensure!(
            seen.insert(raw.path.clone()),
            "duplicate raw path {}",
            raw.path
        );
        ensure!(
            is_lower_hex(&raw.sha256, 64),
            "invalid raw digest for {}",
            raw.path
        );
        let absolute = base.join(&raw.path);
        let metadata = fs::symlink_metadata(&absolute)
            .with_context(|| format!("stat raw artifact {}", raw.path))?;
        ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "raw artifact must be a regular non-symlink file: {}",
            raw.path
        );
        let bytes =
            fs::read(&absolute).with_context(|| format!("read raw artifact {}", raw.path))?;
        let actual = hex_digest(&bytes);
        ensure!(actual == raw.sha256, "raw digest mismatch for {}", raw.path);
        set_hasher.update(raw.path.as_bytes());
        set_hasher.update([0]);
        set_hasher.update(raw.sha256.as_bytes());
        set_hasher.update(b"\n");
        paths_by_kind
            .entry(format!("{:?}", raw.kind))
            .or_default()
            .insert(raw.path.clone());
        match raw.kind {
            RawArtifactKind::NodeMetricsJsonl => {
                let (mode, sample_index, node_id) =
                    parse_sample_raw_path(&raw.path, "node", "-metrics.jsonl")?;
                ensure!(
                    node_files.insert((mode, sample_index, node_id)),
                    "duplicate node metric identity"
                );
                let mut previous = None;
                let mut lines = 0u64;
                let mut previous_metric: Option<RawMetricsLine> = None;
                let mut metrics = Vec::new();
                for (index, line) in String::from_utf8(bytes)?.lines().enumerate() {
                    let metric: RawMetricsLine = serde_json::from_str(line)
                        .with_context(|| format!("{} line {}", raw.path, index + 1))?;
                    ensure!(
                        metric.node_id == node_id,
                        "raw node identity mismatch in {}",
                        raw.path
                    );
                    if let Some(last) = previous {
                        ensure!(
                            metric.monotonic_ns > last,
                            "non-monotonic raw metrics {}",
                            raw.path
                        );
                    }
                    if let Some(last) = previous_metric.as_ref() {
                        ensure!(
                            metric.cpu_seconds >= last.cpu_seconds,
                            "CPU counter regressed in {}",
                            raw.path
                        );
                        ensure!(
                            metric.disk_read_bytes >= last.disk_read_bytes
                                && metric.disk_write_bytes >= last.disk_write_bytes,
                            "disk counter regressed in {}",
                            raw.path
                        );
                        ensure!(
                            metric.fsync_calls >= last.fsync_calls,
                            "WAL sync counter regressed in {}",
                            raw.path
                        );
                        ensure!(
                            metric.network_rx_bytes >= last.network_rx_bytes
                                && metric.network_tx_bytes >= last.network_tx_bytes,
                            "network counter regressed in {}",
                            raw.path
                        );
                        ensure!(
                            metric.transport_sent >= last.transport_sent
                                && metric.transport_received >= last.transport_received
                                && metric.transport_dropped >= last.transport_dropped
                                && metric.transport_retried >= last.transport_retried,
                            "transport counter regressed in {}",
                            raw.path
                        );
                    }
                    previous = Some(metric.monotonic_ns);
                    metrics.push(metric.clone());
                    previous_metric = Some(metric);
                    lines += 1;
                }
                ensure!(
                    lines >= 60,
                    "{} has fewer than 60 metric observations",
                    raw.path
                );
                raw_node_metrics.insert((mode, sample_index, node_id), metrics);
            }
            RawArtifactKind::LoadgenReport => {
                let (path_mode, path_index, origin) =
                    parse_sample_raw_path(&raw.path, "loadgen", ".json")?;
                let report: LoadgenReport = serde_json::from_slice(&bytes)?;
                ensure!(
                    (report.mode, report.sample_index, report.origin_node)
                        == (path_mode, path_index, origin),
                    "loadgen report identity/path mismatch"
                );
                ensure!(report.clients == 100, "each loadgen must own 100 clients");
                ensure!(
                    report.payload_bytes == 1024,
                    "loadgen payload size mismatch"
                );
                ensure!(
                    report
                        .monotonic_end_ns
                        .checked_sub(report.monotonic_start_ns)
                        == Some(60_000_000_000),
                    "loadgen interval must be exactly 60 seconds"
                );
                ensure!(
                    report.errors == 0 && report.timeouts == 0,
                    "loadgen report contains errors or timeouts"
                );
                ensure!(
                    report.per_group_committed.values().sum::<u64>() == report.committed,
                    "loadgen group counts do not sum"
                );
                ensure!(
                    report.latency_us.values().sum::<u64>() == report.committed,
                    "latency histogram does not cover commits"
                );
                ensure!(
                    report
                        .per_group_committed
                        .keys()
                        .copied()
                        .collect::<BTreeSet<_>>()
                        == expected_groups(report.mode),
                    "loadgen group coverage mismatch"
                );
                let reports = loadgen_reports
                    .entry((report.mode, report.sample_index))
                    .or_default();
                ensure!(
                    !reports
                        .iter()
                        .any(|value| value.origin_node == report.origin_node),
                    "duplicate loadgen origin"
                );
                reports.push(report);
            }
            RawArtifactKind::ContainerInspect => {
                let value: serde_json::Value = serde_json::from_slice(&bytes)?;
                let container = value
                    .as_array()
                    .and_then(|items| items.first())
                    .ok_or_else(|| anyhow!("invalid container inspect {}", raw.path))?;
                ensure!(
                    container
                        .pointer("/State/OOMKilled")
                        .and_then(serde_json::Value::as_bool)
                        == Some(false),
                    "container was OOM-killed in {}",
                    raw.path
                );
                ensure!(
                    container
                        .get("RestartCount")
                        .and_then(serde_json::Value::as_u64)
                        == Some(0),
                    "container restarted in {}",
                    raw.path
                );
            }
            RawArtifactKind::NetworkInspect => {
                let _: serde_json::Value = serde_json::from_slice(&bytes)?;
            }
            RawArtifactKind::ShaperConfig => {
                let text = String::from_utf8(bytes)?;
                ensure!(
                    shaper_evidence_matches(&text),
                    "shaper evidence mismatch in {}",
                    raw.path
                );
            }
            RawArtifactKind::ControlObservation => {
                let (mode, index, filename) = parse_sample_directory(&raw.path)?;
                match filename {
                    "membership.json" => {
                        let memberships: Vec<GroupMembership> = serde_json::from_slice(&bytes)?;
                        ensure!(
                            raw_memberships.insert((mode, index), memberships).is_none(),
                            "duplicate membership evidence"
                        );
                    }
                    "rtt-unloaded.json" | "rtt-shaped.json" => {
                        let observations: Vec<RawRttObservation> = serde_json::from_slice(&bytes)?;
                        verify_raw_rtt(&observations)?;
                        ensure!(
                            raw_rtts
                                .insert((mode, index, filename == "rtt-shaped.json"), observations)
                                .is_none(),
                            "duplicate RTT phase evidence"
                        );
                    }
                    _ => return Err(anyhow!("unexpected control observation {}", raw.path)),
                }
            }
            RawArtifactKind::HostFacts => ensure!(!bytes.is_empty(), "host facts are empty"),
        }
    }
    let expected_keys = artifact
        .samples
        .iter()
        .map(|sample| (sample.mode, sample.index))
        .collect::<BTreeSet<_>>();
    ensure!(
        node_files
            .iter()
            .map(|(mode, index, _)| (*mode, *index))
            .collect::<BTreeSet<_>>()
            == expected_keys,
        "node metrics sample coverage mismatch"
    );
    ensure!(
        node_files.len() == 30,
        "expected three node metric files for each sample"
    );
    ensure!(
        loadgen_reports.keys().copied().collect::<BTreeSet<_>>() == expected_keys,
        "loadgen report sample coverage mismatch"
    );
    ensure!(
        loadgen_reports.values().all(|reports| reports
            .iter()
            .map(|report| report.origin_node)
            .collect::<BTreeSet<_>>()
            == BTreeSet::from([1, 2, 3])),
        "loadgen origins must be 1,2,3"
    );
    let mut container_paths = BTreeSet::new();
    let mut network_paths = BTreeSet::new();
    let mut shaper_paths = BTreeSet::new();
    let mut control_paths = BTreeSet::new();
    for (mode, index) in &expected_keys {
        let mode_name = match mode {
            Mode::MultiRaft => "multi_raft",
            Mode::SingleGroup => "single_group",
        };
        let directory = format!("samples/{mode_name}-{index}");
        network_paths.insert(format!("{directory}/network-inspect.json"));
        control_paths.insert(format!("{directory}/membership.json"));
        control_paths.insert(format!("{directory}/rtt-shaped.json"));
        control_paths.insert(format!("{directory}/rtt-unloaded.json"));
        for node in 1..=3 {
            container_paths.insert(format!("{directory}/node{node}-inspect.json"));
            shaper_paths.insert(format!("{directory}/node{node}-qdisc.txt"));
        }
    }
    for (kind, expected_paths) in [
        ("ContainerInspect", container_paths),
        ("NetworkInspect", network_paths),
        ("ShaperConfig", shaper_paths),
        ("ControlObservation", control_paths),
        ("HostFacts", BTreeSet::from(["host-facts.txt".to_owned()])),
    ] {
        ensure!(
            paths_by_kind.get(kind) == Some(&expected_paths),
            "raw {kind} artifact paths mismatch"
        );
    }
    for sample in &artifact.samples {
        let key = (sample.mode, sample.index);
        let reports = loadgen_reports
            .get(&key)
            .ok_or_else(|| anyhow!("missing loadgen evidence"))?;
        ensure!(
            reports.iter().map(|report| report.committed).sum::<u64>() == sample.committed,
            "raw loadgen commits differ from sample summary"
        );
        ensure!(
            reports.iter().map(|report| report.errors).sum::<u64>() == sample.errors
                && reports.iter().map(|report| report.timeouts).sum::<u64>() == sample.timeouts,
            "raw loadgen failures differ from sample summary"
        );
        let start = reports
            .iter()
            .map(|report| report.monotonic_start_ns)
            .max()
            .unwrap();
        let end = reports
            .iter()
            .map(|report| report.monotonic_end_ns)
            .min()
            .unwrap();
        ensure!(
            start == sample.monotonic_start_ns
                && end == sample.monotonic_end_ns
                && (end - start) / 1_000_000 == sample.actual_measure_duration_ms,
            "raw loadgen interval differs from sample summary"
        );
        let mut histogram = BTreeMap::new();
        let mut group_counts = BTreeMap::new();
        for report in reports {
            for (latency, count) in &report.latency_us {
                *histogram.entry(*latency).or_insert(0u64) += count;
            }
            for (group, count) in &report.per_group_committed {
                *group_counts.entry(*group).or_insert(0u64) += count;
            }
        }
        ensure!(
            approx(
                sample.latency_ms.p50,
                histogram_percentile(&histogram, 0.50) as f64 / 1000.0
            ) && approx(
                sample.latency_ms.p95,
                histogram_percentile(&histogram, 0.95) as f64 / 1000.0
            ) && approx(
                sample.latency_ms.p99,
                histogram_percentile(&histogram, 0.99) as f64 / 1000.0
            ),
            "raw latency histogram differs from sample summary"
        );
        let rows = artifact
            .per_group
            .iter()
            .filter(|row| (row.mode, row.sample_index) == key)
            .map(|row| (row.group_id, row.committed))
            .collect::<BTreeMap<_, _>>();
        ensure!(
            rows == group_counts,
            "raw group counts differ from per-group summary"
        );
        let mut cpu_seconds = 0.0;
        let mut peak_rss_bytes = 0;
        let mut disk_bytes = 0;
        let mut fsync_calls = 0;
        let mut network_bytes = 0;
        for node in 1..=3 {
            let values = raw_node_metrics
                .get(&(sample.mode, sample.index, node))
                .ok_or_else(|| anyhow!("missing raw node metrics"))?
                .iter()
                .filter(|value| value.monotonic_ns >= start && value.monotonic_ns <= end)
                .collect::<Vec<_>>();
            ensure!(values.len() >= 60, "node metric interval coverage mismatch");
            ensure!(
                values.iter().all(|value| value
                    .per_group_queue_depth
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    == expected_groups(sample.mode)),
                "per-group queue-depth coverage mismatch"
            );
            let first = values[0];
            let last = values[values.len() - 1];
            cpu_seconds += (last.cpu_seconds - first.cpu_seconds).max(0.0);
            peak_rss_bytes = peak_rss_bytes.max(
                values
                    .iter()
                    .map(|value| value.rss_bytes)
                    .max()
                    .unwrap_or(0),
            );
            disk_bytes += last
                .disk_read_bytes
                .saturating_add(last.disk_write_bytes)
                .saturating_sub(first.disk_read_bytes.saturating_add(first.disk_write_bytes));
            fsync_calls += last.fsync_calls.saturating_sub(first.fsync_calls);
            network_bytes += last
                .network_rx_bytes
                .saturating_add(last.network_tx_bytes)
                .saturating_sub(
                    first
                        .network_rx_bytes
                        .saturating_add(first.network_tx_bytes),
                );
        }
        ensure!(
            close(cpu_seconds, sample.cpu_seconds, 1e-9)
                && peak_rss_bytes == sample.peak_rss_bytes
                && disk_bytes == sample.disk_bytes
                && fsync_calls == sample.fsync_calls
                && network_bytes == sample.network_bytes,
            "raw resource metrics differ from sample summary"
        );
        ensure!(
            raw_memberships.get(&key) == Some(&sample.group_membership_after_drain),
            "raw membership differs from sample summary"
        );
        let unloaded = raw_rtts
            .get(&(sample.mode, sample.index, false))
            .ok_or_else(|| anyhow!("missing unloaded RTT evidence"))?;
        let shaped = raw_rtts
            .get(&(sample.mode, sample.index, true))
            .ok_or_else(|| anyhow!("missing shaped RTT evidence"))?;
        for summary in &sample.network_rtt_ms {
            let unloaded = unloaded
                .iter()
                .find(|value| {
                    (value.source, value.destination) == (summary.source, summary.destination)
                })
                .ok_or_else(|| anyhow!("missing unloaded RTT pair"))?;
            let shaped = shaped
                .iter()
                .find(|value| {
                    (value.source, value.destination) == (summary.source, summary.destination)
                })
                .ok_or_else(|| anyhow!("missing shaped RTT pair"))?;
            ensure!(
                approx(unloaded.p50, summary.unloaded.p50)
                    && approx(unloaded.p95, summary.unloaded.p95),
                "unloaded RTT summary mismatch"
            );
            ensure!(
                approx(shaped.p50, summary.shaped.p50) && approx(shaped.p95, summary.shaped.p95),
                "shaped RTT summary mismatch"
            );
        }
    }
    ensure!(
        format!("{:x}", set_hasher.finalize()) == artifact.raw_artifact_set_sha256,
        "raw artifact-set digest mismatch"
    );
    Ok(())
}

/// Linux `tc` may render a requested 250us netem delay as 249us after
/// converting the value through its kernel clock representation. Both strings
/// describe the same configured evidence; larger deviations remain invalid.
fn shaper_evidence_matches(text: &str) -> bool {
    text.contains("netem") && (text.contains("delay 250us") || text.contains("delay 249us"))
}

fn parse_sample_raw_path(
    path: &str,
    prefix: &str,
    suffix: &str,
) -> anyhow::Result<(Mode, u64, u64)> {
    let (mode, index, filename) = parse_sample_directory(path)?;
    let identity = filename
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(suffix))
        .ok_or_else(|| anyhow!("invalid raw filename {filename}"))?
        .parse()?;
    ensure!((1..=3).contains(&identity), "raw identity must be 1,2,3");
    Ok((mode, index, identity))
}

fn parse_sample_directory(path: &str) -> anyhow::Result<(Mode, u64, &str)> {
    let parts = path.split('/').collect::<Vec<_>>();
    ensure!(
        parts.len() == 3 && parts[0] == "samples",
        "unexpected controlled raw path {path}"
    );
    let (mode_text, index_text) = parts[1]
        .rsplit_once('-')
        .ok_or_else(|| anyhow!("invalid sample directory {}", parts[1]))?;
    let mode = match mode_text {
        "multi_raft" => Mode::MultiRaft,
        "single_group" => Mode::SingleGroup,
        _ => return Err(anyhow!("invalid sample mode {mode_text}")),
    };
    let index = index_text.parse()?;
    Ok((mode, index, parts[2]))
}

fn verify_raw_rtt(observations: &[RawRttObservation]) -> anyhow::Result<()> {
    let expected = (1..=3)
        .flat_map(|source| {
            (1..=3)
                .filter(move |destination| *destination != source)
                .map(move |destination| (source, destination))
        })
        .collect::<BTreeSet<_>>();
    ensure!(
        observations.len() == 6
            && observations
                .iter()
                .map(|value| (value.source, value.destination))
                .collect::<BTreeSet<_>>()
                == expected,
        "raw RTT pair coverage mismatch"
    );
    for observation in observations {
        ensure!(
            observation.raw_samples_ms.len() == 200,
            "raw RTT pair must contain 200 samples"
        );
        ensure!(
            observation
                .raw_samples_ms
                .iter()
                .all(|value| value.is_finite() && *value >= 0.0),
            "invalid raw RTT sample"
        );
        let mut ordered = observation.raw_samples_ms.clone();
        ordered.sort_by(f64::total_cmp);
        ensure!(
            approx(observation.p50, ordered[99]) && approx(observation.p95, ordered[189]),
            "raw RTT percentile mismatch"
        );
    }
    Ok(())
}

fn histogram_percentile(histogram: &BTreeMap<u64, u64>, percentile: f64) -> u64 {
    let target = ((histogram.values().sum::<u64>() as f64 * percentile).ceil() as u64).max(1);
    let mut seen = 0;
    for (value, count) in histogram {
        seen += count;
        if seen >= target {
            return *value;
        }
    }
    0
}

fn verify_samples(artifact: &Artifact) -> anyhow::Result<()> {
    let expected = [
        (Mode::MultiRaft, 0),
        (Mode::SingleGroup, 0),
        (Mode::SingleGroup, 1),
        (Mode::MultiRaft, 1),
        (Mode::MultiRaft, 2),
        (Mode::SingleGroup, 2),
        (Mode::SingleGroup, 3),
        (Mode::MultiRaft, 3),
        (Mode::MultiRaft, 4),
        (Mode::SingleGroup, 4),
    ];
    ensure!(
        artifact.samples.len() == expected.len(),
        "expected ten ordered samples"
    );
    let sample_process_ids = artifact
        .samples
        .iter()
        .flat_map(|sample| sample.process_or_container_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    ensure!(
        sample_process_ids.len() == 30
            && sample_process_ids
                == artifact
                    .execution_environment
                    .process_or_container_ids
                    .iter()
                    .cloned()
                    .collect(),
        "sample/environment process identities differ"
    );
    for (sample, expected_key) in artifact.samples.iter().zip(expected) {
        ensure!(
            (sample.mode, sample.index) == expected_key,
            "sample order/index does not alternate"
        );
        verify_sample(sample)?;
    }

    let sample_keys = artifact
        .samples
        .iter()
        .map(|sample| (sample.mode, sample.index))
        .collect::<BTreeSet<_>>();
    let mut rows: BTreeMap<(Mode, u64), Vec<&PerGroup>> = BTreeMap::new();
    for row in &artifact.per_group {
        ensure!(
            sample_keys.contains(&(row.mode, row.sample_index)),
            "orphan per_group row"
        );
        ensure!(
            row.committed > 0 && row.throughput_per_sec > 0.0,
            "group made no progress"
        );
        rows.entry((row.mode, row.sample_index))
            .or_default()
            .push(row);
    }
    for sample in &artifact.samples {
        let group_rows = rows
            .get(&(sample.mode, sample.index))
            .ok_or_else(|| anyhow!("sample lacks per_group rows"))?;
        let expected_groups = expected_groups(sample.mode);
        ensure!(
            group_rows
                .iter()
                .map(|row| row.group_id)
                .collect::<BTreeSet<_>>()
                == expected_groups,
            "per_group coverage mismatch"
        );
        ensure!(
            group_rows.iter().map(|row| row.committed).sum::<u64>() == sample.committed,
            "per_group commits do not sum to sample"
        );
        let throughputs = group_rows
            .iter()
            .map(|row| row.throughput_per_sec)
            .collect::<Vec<_>>();
        for row in group_rows {
            ensure!(
                close(
                    row.throughput_per_sec,
                    row.committed as f64 / (sample.actual_measure_duration_ms as f64 / 1000.0),
                    1e-9
                ),
                "per-group throughput recomputation mismatch"
            );
        }
        let slowest = throughputs.iter().copied().min_by(f64::total_cmp).unwrap();
        ensure!(
            slowest + EPSILON >= 0.5 * median(&throughputs)?,
            "slowest group is below 50% of median"
        );
    }
    Ok(())
}

fn verify_sample(sample: &Sample) -> anyhow::Result<()> {
    let expected_groups = expected_groups(sample.mode);
    ensure!(
        sample.group_count == expected_groups.len() as u64,
        "sample group_count mismatch"
    );
    ensure!(sample.clients == 300, "sample clients must be 300");
    ensure!(
        sample.process_or_container_ids.len() == 3,
        "sample must identify three node processes/containers"
    );
    ensure!(
        sample
            .process_or_container_ids
            .iter()
            .collect::<HashSet<_>>()
            .len()
            == 3,
        "sample process identities are not unique"
    );
    ensure!(
        sample.monotonic_end_ns > sample.monotonic_start_ns,
        "sample monotonic interval is reversed"
    );
    ensure!(
        sample.actual_measure_duration_ms >= 60_000,
        "sample measured less than 60 seconds"
    );
    ensure!(
        sample.monotonic_end_ns - sample.monotonic_start_ns >= 60_000_000_000,
        "monotonic interval is shorter than 60 seconds"
    );
    ensure!(sample.committed > 0, "sample has no commits");
    let throughput = sample.committed as f64 / (sample.actual_measure_duration_ms as f64 / 1000.0);
    ensure!(
        close(throughput, sample.throughput_per_sec, 1e-7),
        "sample throughput was not computed from actual duration"
    );
    ensure!(
        sample.latency_ms.p50.is_finite()
            && sample.latency_ms.p95.is_finite()
            && sample.latency_ms.p99.is_finite(),
        "latency is non-finite"
    );
    ensure!(
        sample.latency_ms.p50 <= sample.latency_ms.p95
            && sample.latency_ms.p95 <= sample.latency_ms.p99,
        "latency percentiles are unordered"
    );
    ensure!(
        sample.errors == 0 && sample.timeouts == 0,
        "sample contains errors or timeouts"
    );
    ensure!(sample.cpu_seconds > 0.0, "sample has no CPU evidence");
    ensure!(sample.peak_rss_bytes > 0, "sample has no RSS evidence");
    ensure!(sample.fsync_calls > 0, "sample has no WAL sync evidence");
    ensure!(sample.network_bytes > 0, "sample has no network evidence");
    ensure!(
        !sample.oom_killed && !sample.process_restarted && !sample.shaper_mismatch,
        "sample invalidation flag set"
    );
    verify_rtt(&sample.network_rtt_ms)?;
    ensure!(
        sample
            .group_membership_after_drain
            .iter()
            .map(|group| group.group_id)
            .collect::<BTreeSet<_>>()
            == expected_groups,
        "post-drain group coverage mismatch"
    );
    for group in &sample.group_membership_after_drain {
        verify_membership(group)?;
    }
    Ok(())
}

fn verify_rtt(values: &[DirectedRtt]) -> anyhow::Result<()> {
    let expected = (1..=3)
        .flat_map(|source| {
            (1..=3)
                .filter(move |destination| *destination != source)
                .map(move |destination| (source, destination))
        })
        .collect::<BTreeSet<_>>();
    ensure!(
        values
            .iter()
            .map(|value| (value.source, value.destination))
            .collect::<BTreeSet<_>>()
            == expected,
        "RTT must cover all six directed pairs exactly"
    );
    ensure!(values.len() == 6, "duplicate RTT observations");
    for value in values {
        ensure!(
            value.unloaded.p50 >= 0.0 && value.unloaded.p95 >= value.unloaded.p50,
            "invalid unloaded RTT"
        );
        ensure!(
            value.shaped.p50 >= 0.0 && value.shaped.p95 >= value.shaped.p50,
            "invalid shaped RTT"
        );
        ensure!(
            (0.8..=1.2).contains(&value.shaped.p95),
            "shaped RTT p95 outside 1.0±0.2ms"
        );
    }
    Ok(())
}

fn verify_membership(group: &GroupMembership) -> anyhow::Result<()> {
    ensure!(
        group.replicas.len() == 3,
        "group {} must have three replicas",
        group.group_id
    );
    ensure!(
        group
            .replicas
            .iter()
            .map(|replica| replica.node_id)
            .collect::<BTreeSet<_>>()
            == BTreeSet::from([1, 2, 3]),
        "group {} replica IDs mismatch",
        group.group_id
    );
    let first = &group.replicas[0];
    ensure!(
        first.last_applied > 0,
        "group {} did not apply a log",
        group.group_id
    );
    ensure!(
        is_lower_hex(&first.committed_digest, 64),
        "group {} digest invalid",
        group.group_id
    );
    ensure!(
        (1..=3).contains(&first.leader_id),
        "group {} leader invalid",
        group.group_id
    );
    for replica in &group.replicas {
        ensure!(
            replica.voters == vec![1, 2, 3],
            "group {} voters mismatch",
            group.group_id
        );
        ensure!(
            replica.leader_id == first.leader_id,
            "group {} replicas disagree on leader",
            group.group_id
        );
        ensure!(
            replica.last_applied == first.last_applied,
            "group {} replicas disagree on commit",
            group.group_id
        );
        ensure!(
            replica.committed_digest == first.committed_digest,
            "group {} replicas diverged",
            group.group_id
        );
    }
    Ok(())
}

fn compute_statistics(artifact: &Artifact) -> anyhow::Result<Statistics> {
    let multi = artifact
        .samples
        .iter()
        .filter(|sample| sample.mode == Mode::MultiRaft)
        .map(|sample| sample.throughput_per_sec)
        .collect::<Vec<_>>();
    let baseline = artifact
        .samples
        .iter()
        .filter(|sample| sample.mode == Mode::SingleGroup)
        .map(|sample| sample.throughput_per_sec)
        .collect::<Vec<_>>();
    ensure!(
        multi.len() == 5 && baseline.len() == 5,
        "statistics require five samples per mode"
    );
    let multi_median = median(&multi)?;
    let baseline_median = median(&baseline)?;
    ensure!(baseline_median > 0.0, "baseline median is zero");
    Ok(Statistics {
        seed: STATISTICS_SEED.to_owned(),
        resamples: STATISTICS_RESAMPLES,
        multi_raft_median: multi_median,
        multi_raft_ci95_lower: bootstrap_ci95_lower(&multi, 0x600, STATISTICS_RESAMPLES as usize)?,
        single_group_median: baseline_median,
        overhead_ratio: 1.0 - multi_median / baseline_median,
    })
}

fn compare_statistics(stored: &Statistics, computed: &Statistics) -> anyhow::Result<()> {
    ensure!(
        stored.seed == computed.seed && stored.resamples == computed.resamples,
        "statistics algorithm identity mismatch"
    );
    for (name, left, right) in [
        (
            "multi median",
            stored.multi_raft_median,
            computed.multi_raft_median,
        ),
        (
            "multi CI",
            stored.multi_raft_ci95_lower,
            computed.multi_raft_ci95_lower,
        ),
        (
            "baseline median",
            stored.single_group_median,
            computed.single_group_median,
        ),
        ("overhead", stored.overhead_ratio, computed.overhead_ratio),
    ] {
        ensure!(
            close(left, right, 1e-9),
            "stored {name} does not match recomputation"
        );
    }
    Ok(())
}

fn compute_verdict(artifact: &Artifact, statistics: &Statistics) -> anyhow::Result<Verdict> {
    let throughput = if statistics.multi_raft_median >= 100_000.0
        && statistics.multi_raft_ci95_lower >= 100_000.0
    {
        Gate::Pass
    } else {
        Gate::Fail
    };
    let overhead = if statistics.overhead_ratio < 0.10 {
        Gate::Pass
    } else {
        Gate::Fail
    };
    let integrity = if artifact.samples.iter().all(|sample| {
        sample.errors == 0
            && sample.timeouts == 0
            && !sample.oom_killed
            && !sample.process_restarted
            && !sample.shaper_mismatch
    }) {
        Gate::Pass
    } else {
        Gate::Fail
    };
    let overall = if throughput == Gate::Pass && overhead == Gate::Pass && integrity == Gate::Pass {
        Gate::Pass
    } else {
        Gate::Fail
    };
    Ok(Verdict {
        throughput,
        overhead,
        integrity,
        overall,
    })
}

fn compare_verdict(stored: &Verdict, computed: &Verdict) -> anyhow::Result<()> {
    ensure!(
        stored.throughput == computed.throughput
            && stored.overhead == computed.overhead
            && stored.integrity == computed.integrity
            && stored.overall == computed.overall,
        "stored verdict differs from recomputation"
    );
    Ok(())
}

fn expected_groups(mode: Mode) -> BTreeSet<u64> {
    match mode {
        Mode::MultiRaft => (1..=100).collect(),
        Mode::SingleGroup => BTreeSet::from([1]),
    }
}

fn parse_cpu_set(value: &str) -> anyhow::Result<BTreeSet<u64>> {
    let mut result = BTreeSet::new();
    for part in value.split(',') {
        if let Some((start, end)) = part.split_once('-') {
            let start: u64 = start.parse()?;
            let end: u64 = end.parse()?;
            ensure!(start <= end, "reversed CPU range");
            result.extend(start..=end);
        } else {
            result.insert(part.parse()?);
        }
    }
    ensure!(!result.is_empty(), "empty CPU set");
    Ok(result)
}

fn is_safe_relative_path(value: &str) -> bool {
    let path = PathBuf::from(value);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn hex_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn approx(left: f64, right: f64) -> bool {
    (left - right).abs() <= EPSILON
}

fn close(left: f64, right: f64, relative: f64) -> bool {
    left.is_finite()
        && right.is_finite()
        && (left - right).abs() <= relative * left.abs().max(right.abs()).max(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    struct Fixture {
        _directory: TempDir,
        artifact_path: PathBuf,
        first_raw_path: PathBuf,
    }

    #[test]
    fn strict_fixture_verifies_and_rejects_tampering() {
        let fixture = fixture();
        let verified = verify_artifact(&fixture.artifact_path).unwrap();
        assert_eq!(verified.computed_verdict.overall, Gate::Pass);

        let mut unknown: serde_json::Value =
            serde_json::from_slice(&fs::read(&fixture.artifact_path).unwrap()).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("unreviewed".into(), true.into());
        let unknown_path = fixture.artifact_path.with_file_name("unknown.json");
        fs::write(&unknown_path, serde_json::to_vec(&unknown).unwrap()).unwrap();
        assert!(
            verify_artifact(&unknown_path)
                .unwrap_err()
                .to_string()
                .contains("parse strict artifact")
        );

        fs::write(&fixture.first_raw_path, b"tampered\n").unwrap();
        assert!(
            verify_artifact(&fixture.artifact_path)
                .unwrap_err()
                .to_string()
                .contains("raw digest mismatch")
        );
    }

    #[test]
    fn shaper_verifier_accepts_kernel_rounded_delay_only() {
        assert!(shaper_evidence_matches("qdisc netem delay 250us"));
        assert!(shaper_evidence_matches("qdisc netem delay 249us"));
        assert!(!shaper_evidence_matches("qdisc netem delay 248us"));
        assert!(!shaper_evidence_matches("qdisc fq_codel delay 250us"));
    }

    fn fixture() -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let order = [
            (Mode::MultiRaft, 0),
            (Mode::SingleGroup, 0),
            (Mode::SingleGroup, 1),
            (Mode::MultiRaft, 1),
            (Mode::MultiRaft, 2),
            (Mode::SingleGroup, 2),
            (Mode::SingleGroup, 3),
            (Mode::MultiRaft, 3),
            (Mode::MultiRaft, 4),
            (Mode::SingleGroup, 4),
        ];
        let mut samples = Vec::new();
        let mut per_group = Vec::new();
        let mut raw = Vec::new();
        for (mode, index) in order {
            let mode_name = match mode {
                Mode::MultiRaft => "multi_raft",
                Mode::SingleGroup => "single_group",
            };
            let directory_name = format!("samples/{mode_name}-{index}");
            let groups = expected_groups(mode);
            let committed = if mode == Mode::MultiRaft {
                7_200_000
            } else {
                7_500_000
            };
            let each_group = committed / groups.len() as u64;
            let membership: Vec<GroupMembership> = groups
                .iter()
                .map(|group_id| GroupMembership {
                    group_id: *group_id,
                    replicas: (1..=3)
                        .map(|node_id| ReplicaState {
                            node_id,
                            voters: vec![1, 2, 3],
                            leader_id: 1,
                            last_applied: each_group,
                            committed_digest: "a".repeat(64),
                        })
                        .collect(),
                })
                .collect();
            samples.push(Sample {
                mode,
                index,
                group_count: groups.len() as u64,
                clients: 300,
                process_or_container_ids: (1..=3)
                    .map(|node| format!("{mode_name}-{index}-node{node}"))
                    .collect(),
                actual_measure_duration_ms: 60_000,
                monotonic_start_ns: 1_000_000_000_000 + index * 100_000_000_000,
                monotonic_end_ns: 1_060_000_000_000 + index * 100_000_000_000,
                network_rtt_ms: (1..=3)
                    .flat_map(|source| {
                        (1..=3)
                            .filter(move |destination| *destination != source)
                            .map(move |destination| DirectedRtt {
                                source,
                                destination,
                                unloaded: Percentiles {
                                    p50: 0.10,
                                    p95: 0.10,
                                },
                                shaped: Percentiles { p50: 1.0, p95: 1.0 },
                            })
                    })
                    .collect(),
                group_membership_after_drain: membership.clone(),
                committed,
                throughput_per_sec: committed as f64 / 60.0,
                latency_ms: Latency {
                    p50: 1.0,
                    p95: 1.0,
                    p99: 1.0,
                },
                errors: 0,
                timeouts: 0,
                server_errors: 0,
                transport_errors: 0,
                server_error_reasons: BTreeMap::new(),
                cpu_seconds: 177.0,
                peak_rss_bytes: 1024,
                disk_bytes: 1947,
                fsync_calls: 177,
                network_bytes: 35_400,
                oom_killed: false,
                process_restarted: false,
                shaper_mismatch: false,
            });
            for group_id in &groups {
                per_group.push(PerGroup {
                    mode,
                    sample_index: index,
                    group_id: *group_id,
                    committed: each_group,
                    throughput_per_sec: each_group as f64 / 60.0,
                });
            }
            for node in 1..=3 {
                let mut lines = String::new();
                for tick in 0..60u64 {
                    let metric = RawMetricsLine {
                        monotonic_ns: 1_000_000_000_000
                            + index * 100_000_000_000
                            + tick * 1_000_000_000,
                        node_id: node,
                        cpu_seconds: tick as f64 + 1.0,
                        rss_bytes: 1024,
                        disk_read_bytes: tick,
                        disk_write_bytes: tick * 10,
                        fsync_calls: tick,
                        network_rx_bytes: tick * 100,
                        network_tx_bytes: tick * 100,
                        transport_sent: tick,
                        transport_received: tick,
                        transport_dropped: 0,
                        transport_retried: 0,
                        per_group_queue_depth: groups.iter().map(|group| (*group, 0)).collect(),
                        leader_by_group: groups.iter().map(|group| (*group, 1)).collect(),
                        proposal_inflight: BTreeMap::new(),
                        dispatch_queue_depth: 0,
                        transport_queue_utilization: BTreeMap::new(),
                        retransmission_total: 0,
                        retransmission_buffer_bytes: 0,
                        queue_overflow_total: 0,
                        backpressure_triggered_total: 0,
                        response_send_inflight: 0,
                        response_send_max_inflight: 0,
                        response_send_dropped: 0,
                        response_send_failed: 0,
                        dispatch_budget_in_use_bytes: 0,
                        dispatch_budget_waits: 0,
                    };
                    lines.push_str(&serde_json::to_string(&metric).unwrap());
                    lines.push('\n');
                }
                add_raw(
                    root,
                    &mut raw,
                    RawArtifactKind::NodeMetricsJsonl,
                    format!("{directory_name}/node{node}-metrics.jsonl"),
                    lines.as_bytes(),
                );
                add_raw(
                    root,
                    &mut raw,
                    RawArtifactKind::ContainerInspect,
                    format!("{directory_name}/node{node}-inspect.json"),
                    br#"[{"State":{"OOMKilled":false},"RestartCount":0}]"#,
                );
                add_raw(
                    root,
                    &mut raw,
                    RawArtifactKind::ShaperConfig,
                    format!("{directory_name}/node{node}-qdisc.txt"),
                    b"netem delay 250us\n",
                );

                let origin_committed = committed / 3;
                let per_group_committed = groups
                    .iter()
                    .map(|group| (*group, origin_committed / groups.len() as u64))
                    .collect();
                let report = LoadgenReport {
                    mode,
                    sample_index: index,
                    origin_node: node,
                    clients: 100,
                    payload_bytes: 1024,
                    monotonic_start_ns: 1_000_000_000_000 + index * 100_000_000_000,
                    monotonic_end_ns: 1_060_000_000_000 + index * 100_000_000_000,
                    committed: origin_committed,
                    errors: 0,
                    timeouts: 0,
                    server_errors: 0,
                    transport_errors: 0,
                    server_error_reasons: BTreeMap::new(),
                    per_group_committed,
                    latency_us: BTreeMap::from([(1_000, origin_committed)]),
                    peak_rss_bytes: 1024,
                    rss_start_bytes: 1024,
                    rss_warmup_peak_bytes: 1024,
                    rss_measure_peak_bytes: 1024,
                    rss_drain_peak_bytes: 1024,
                };
                add_raw(
                    root,
                    &mut raw,
                    RawArtifactKind::LoadgenReport,
                    format!("{directory_name}/loadgen{node}.json"),
                    &serde_json::to_vec(&report).unwrap(),
                );
            }
            add_raw(
                root,
                &mut raw,
                RawArtifactKind::NetworkInspect,
                format!("{directory_name}/network-inspect.json"),
                b"{}\n",
            );
            add_raw(
                root,
                &mut raw,
                RawArtifactKind::ControlObservation,
                format!("{directory_name}/membership.json"),
                &serde_json::to_vec(&membership).unwrap(),
            );
            add_raw(
                root,
                &mut raw,
                RawArtifactKind::ControlObservation,
                format!("{directory_name}/rtt-shaped.json"),
                &serde_json::to_vec(&fixture_rtt(1.0)).unwrap(),
            );
            add_raw(
                root,
                &mut raw,
                RawArtifactKind::ControlObservation,
                format!("{directory_name}/rtt-unloaded.json"),
                &serde_json::to_vec(&fixture_rtt(0.1)).unwrap(),
            );
        }
        add_raw(
            root,
            &mut raw,
            RawArtifactKind::HostFacts,
            "host-facts.txt".into(),
            b"fixture host\n",
        );
        raw.sort_by(|left, right| left.path.cmp(&right.path));
        let first_raw_path = root.join(&raw[0].path);
        let mut set_hasher = Sha256::new();
        for item in &raw {
            set_hasher.update(item.path.as_bytes());
            set_hasher.update([0]);
            set_hasher.update(item.sha256.as_bytes());
            set_hasher.update(b"\n");
        }
        let process_ids = samples
            .iter()
            .flat_map(|sample| sample.process_or_container_ids.iter().cloned())
            .collect();
        let input = ArtifactInput {
            schema: SCHEMA.into(),
            commit_sha: "a".repeat(40),
            binary_sha256: "b".repeat(64),
            runner_command: vec![
                "scripts/perf/run-controlled-container-multi-raft.sh".into(),
                "--output".into(),
                "<OUTPUT>".into(),
            ],
            execution_environment: ExecutionEnvironment {
                class: ExecutionClass::Container,
                host_count: 1,
                logical_nodes: 3,
                process_or_container_ids: process_ids,
                node_cpu_sets: BTreeMap::from([(1, "0".into()), (2, "2".into()), (3, "4".into())]),
                loadgen_cpu_sets: BTreeMap::from([
                    (1, "1".into()),
                    (2, "3".into()),
                    (3, "5".into()),
                ]),
                cpu: "fixture".into(),
                cores: 8,
                ram_bytes: 1 << 30,
                kernel: "fixture".into(),
                rust_version: "fixture".into(),
                storage: "tmpfs".into(),
                filesystem: "tmpfs".into(),
                network_shaper: "netem".into(),
                governor: "performance".into(),
                physical_deployment: false,
                swap_bytes_before: 0,
                swap_bytes_after: 0,
            },
            resolved_config: ResolvedConfig {
                nodes: 3,
                groups: 100,
                payload_bytes: 1024,
                rtt_ms: 1.0,
                clients: 300,
                clients_per_node: 100,
                warmup_seconds: 15,
                measure_seconds: 60,
                drain_seconds: 5,
                samples: 5,
                fsync_interval: 0,
                snapshot_threshold: 10_000,
                send_queue_capacity: 4_096,
            },
            samples,
            per_group,
            raw_metrics_artifacts: raw,
            raw_artifact_set_sha256: format!("{:x}", set_hasher.finalize()),
        };
        let artifact = assemble_artifact(input, root).unwrap();
        let artifact_path = root.join("artifact.json");
        fs::write(
            &artifact_path,
            serde_json::to_vec_pretty(&artifact).unwrap(),
        )
        .unwrap();
        Fixture {
            _directory: directory,
            artifact_path,
            first_raw_path,
        }
    }

    fn add_raw(
        root: &Path,
        raw: &mut Vec<RawArtifact>,
        kind: RawArtifactKind,
        path: String,
        bytes: &[u8],
    ) {
        let absolute = root.join(&path);
        fs::create_dir_all(absolute.parent().unwrap()).unwrap();
        fs::write(&absolute, bytes).unwrap();
        raw.push(RawArtifact {
            kind,
            path,
            sha256: hex_digest(bytes),
        });
    }

    fn fixture_rtt(value: f64) -> Vec<RawRttObservation> {
        (1..=3)
            .flat_map(|source| {
                (1..=3)
                    .filter(move |destination| *destination != source)
                    .map(move |destination| RawRttObservation {
                        source,
                        destination,
                        p50: value,
                        p95: value,
                        raw_samples_ms: vec![value; 200],
                    })
            })
            .collect()
    }
}
