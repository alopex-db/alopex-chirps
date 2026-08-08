use crate::schema::*;
use anyhow::{Context, ensure};
use std::collections::BTreeMap;
use std::path::Path;

pub fn summarize(observation: SampleObservation, base: &Path) -> anyhow::Result<SampleSummary> {
    ensure!(
        observation.loadgen_report_paths.len() == 3,
        "three loadgen reports required"
    );
    if observation.resource_audit {
        ensure!(
            observation.node_metrics_paths.len() == 3,
            "three node metric files required when resource audit is enabled"
        );
    } else {
        ensure!(
            observation.node_metrics_paths.is_empty(),
            "node metric files must be omitted when resource audit is disabled"
        );
    }
    let reports = observation
        .loadgen_report_paths
        .iter()
        .map(|path| {
            serde_json::from_slice::<LoadgenReport>(&std::fs::read(base.join(path))?)
                .with_context(|| format!("loadgen report {path}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (expected_node, report) in (1..=3).zip(&reports) {
        ensure!(
            report.mode == observation.mode && report.sample_index == observation.index,
            "loadgen sample identity mismatch"
        );
        ensure!(
            report.origin_node == expected_node,
            "loadgen origin order mismatch"
        );
        ensure!(
            report.clients == 100 && report.payload_bytes == 1024,
            "loadgen contract mismatch"
        );
    }
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
    ensure!(end > start, "loadgen measured intervals do not overlap");
    let duration_ms = (end - start) / 1_000_000;
    let committed = reports.iter().map(|report| report.committed).sum::<u64>();
    let errors = reports.iter().map(|report| report.errors).sum();
    let timeouts = reports.iter().map(|report| report.timeouts).sum();
    let server_errors = reports.iter().map(|report| report.server_errors).sum();
    let transport_errors = reports.iter().map(|report| report.transport_errors).sum();
    let mut server_error_reasons = BTreeMap::new();
    let mut per_group_counts = BTreeMap::new();
    let mut histogram = BTreeMap::new();
    for report in &reports {
        for (reason, count) in &report.server_error_reasons {
            *server_error_reasons.entry(reason.clone()).or_insert(0) += count;
        }
        for (group, count) in &report.per_group_committed {
            *per_group_counts.entry(*group).or_insert(0) += count;
        }
        for (micros, count) in &report.latency_us {
            *histogram.entry(*micros).or_insert(0) += count;
        }
    }
    ensure!(
        histogram.values().sum::<u64>() == committed,
        "latency histogram coverage mismatch"
    );
    let metrics = if observation.resource_audit {
        observation
            .node_metrics_paths
            .iter()
            .map(|path| read_metrics(&base.join(path), start, end))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    let cpu_seconds = metrics.iter().map(|values| values.cpu).sum();
    let peak_rss_bytes = metrics
        .iter()
        .map(|values| values.peak_rss)
        .max()
        .unwrap_or(0);
    let disk_bytes = metrics.iter().map(|values| values.disk).sum();
    let fsync_calls = metrics.iter().map(|values| values.fsync).sum();
    let durability_barriers = metrics
        .iter()
        .map(|values| values.durability_barriers)
        .sum();
    let durability_participant_syncs = metrics
        .iter()
        .map(|values| values.durability_participant_syncs)
        .sum();
    let network_bytes = metrics.iter().map(|values| values.network).sum();
    let duration_seconds = duration_ms as f64 / 1000.0;
    let per_group = per_group_counts
        .into_iter()
        .map(|(group_id, committed)| PerGroup {
            mode: observation.mode,
            sample_index: observation.index,
            group_id,
            committed,
            throughput_per_sec: committed as f64 / duration_seconds,
        })
        .collect();
    Ok(SampleSummary {
        sample: Sample {
            mode: observation.mode,
            index: observation.index,
            group_count: match observation.mode {
                Mode::MultiRaft => 100,
                Mode::SingleGroup => 1,
            },
            clients: 300,
            process_or_container_ids: observation.process_or_container_ids,
            actual_measure_duration_ms: duration_ms,
            monotonic_start_ns: start,
            monotonic_end_ns: end,
            network_rtt_ms: observation.network_rtt_ms,
            group_membership_after_drain: observation.group_membership_after_drain,
            committed,
            throughput_per_sec: committed as f64 / duration_seconds,
            latency_ms: Latency {
                p50: percentile(&histogram, 0.50) as f64 / 1000.0,
                p95: percentile(&histogram, 0.95) as f64 / 1000.0,
                p99: percentile(&histogram, 0.99) as f64 / 1000.0,
            },
            errors,
            timeouts,
            server_errors,
            transport_errors,
            server_error_reasons,
            cpu_seconds,
            peak_rss_bytes,
            disk_bytes,
            fsync_calls,
            durability_barriers,
            durability_participant_syncs,
            network_bytes,
            oom_killed: observation.oom_killed,
            process_restarted: observation.process_restarted,
            shaper_mismatch: observation.shaper_mismatch,
        },
        per_group,
    })
}

#[derive(Default)]
struct MetricDeltas {
    cpu: f64,
    peak_rss: u64,
    disk: u64,
    fsync: u64,
    durability_barriers: u64,
    durability_participant_syncs: u64,
    network: u64,
}

fn read_metrics(path: &Path, start: u64, end: u64) -> anyhow::Result<MetricDeltas> {
    let text = std::fs::read_to_string(path)?;
    let mut values = text
        .lines()
        .map(serde_json::from_str::<RawMetricsLine>)
        .collect::<Result<Vec<_>, _>>()?;
    values.retain(|value| value.monotonic_ns >= start && value.monotonic_ns <= end);
    ensure!(
        values.len() >= 60,
        "{} lacks one-second metric coverage",
        path.display()
    );
    values.sort_by_key(|value| value.monotonic_ns);
    let first = &values[0];
    let last = values.last().unwrap();
    Ok(MetricDeltas {
        cpu: (last.cpu_seconds - first.cpu_seconds).max(0.0),
        peak_rss: values
            .iter()
            .map(|value| value.rss_bytes)
            .max()
            .unwrap_or(0),
        disk: last
            .disk_read_bytes
            .saturating_add(last.disk_write_bytes)
            .saturating_sub(first.disk_read_bytes.saturating_add(first.disk_write_bytes)),
        fsync: last.fsync_calls.saturating_sub(first.fsync_calls),
        durability_barriers: last
            .durability_barriers
            .saturating_sub(first.durability_barriers),
        durability_participant_syncs: last
            .durability_participant_syncs
            .saturating_sub(first.durability_participant_syncs),
        network: last
            .network_rx_bytes
            .saturating_add(last.network_tx_bytes)
            .saturating_sub(
                first
                    .network_rx_bytes
                    .saturating_add(first.network_tx_bytes),
            ),
    })
}

fn percentile(histogram: &BTreeMap<u64, u64>, percentile: f64) -> u64 {
    let total = histogram.values().sum::<u64>();
    let target = ((total as f64 * percentile).ceil() as u64).max(1);
    let mut seen = 0;
    for (value, count) in histogram {
        seen += count;
        if seen >= target {
            return *value;
        }
    }
    0
}
