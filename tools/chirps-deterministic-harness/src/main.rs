mod coordinator;
mod worker;

use alopex_chirps_mock::deterministic::{FaultSchedule, GENERATOR_VERSION};
use chirps_deterministic_harness::{
    ARTIFACT_SCHEMA, FailureRecord, FinalState, HarnessArtifact, SCENARIO, ScenarioStep,
    default_schedule, format_seed, parse_seed, read_artifact, stable_digest, write_artifact_atomic,
};
use coordinator::{RunEvidence, scenario_fault_schedule};
use std::path::{Path, PathBuf};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    anyhow::ensure!(!arguments.is_empty(), usage());
    match arguments[0].as_str() {
        "run" => run_command(&arguments[1..]).await,
        "replay" => replay_command(&arguments[1..]).await,
        "worker" => worker_command(&arguments[1..]).await,
        _ => anyhow::bail!(usage()),
    }
}

async fn worker_command(arguments: &[String]) -> anyhow::Result<()> {
    let node_id = required_option(arguments, "--node-id")?.parse()?;
    let storage_root = PathBuf::from(required_option(arguments, "--storage-root")?);
    worker::run_worker(node_id, storage_root).await
}

async fn run_command(arguments: &[String]) -> anyhow::Result<()> {
    let scenario = option(arguments, "--scenario").unwrap_or_else(|| SCENARIO.to_owned());
    anyhow::ensure!(scenario == SCENARIO, "unsupported scenario: {scenario}");
    let seed = parse_seed(&required_option(arguments, "--seed")?)?;
    let artifact_path = PathBuf::from(required_option(arguments, "--artifact")?);
    let inject_duplicate_failure = arguments
        .iter()
        .any(|argument| argument == "--inject-duplicate-oracle-failure");
    let minimize = arguments
        .iter()
        .any(|argument| argument == "--minimize-on-failure");
    let schedule = default_schedule();
    let fault_schedule = scenario_fault_schedule(seed);
    coordinator::ensure_fault_coverage(&fault_schedule)?;
    let mut evidence = match coordinator::execute(seed, &schedule, &fault_schedule).await {
        Ok(evidence) => evidence,
        Err(error) => fatal_evidence("harness_execution_failed", error),
    };
    if inject_duplicate_failure && evidence.failure.is_none() {
        inject_duplicate_oracle_failure(&mut evidence);
    }
    let minimized_fault_schedule = if minimize {
        if let Some(failure) = &evidence.failure {
            Some(
                minimize_fault_failure(seed, &schedule, &fault_schedule, &failure.signature)
                    .await?,
            )
        } else {
            None
        }
    } else {
        None
    };
    let minimized_reproduction_digest = if let Some(minimized) = &minimized_fault_schedule {
        let mut minimized_evidence = coordinator::execute(seed, &schedule, minimized).await?;
        inject_duplicate_oracle_failure(&mut minimized_evidence);
        Some(evidence_digest(&minimized_evidence)?)
    } else {
        None
    };
    let artifact = build_artifact(
        ArtifactBuild {
            seed,
            schedule,
            fault_schedule,
            minimized_schedule: None,
            minimized_fault_schedule,
            minimized_reproduction_digest,
            evidence,
        },
        &artifact_path,
    )?;
    write_artifact_atomic(&artifact_path, &artifact)?;
    println!(
        "scenario={} seed={} trace_digest={} artifact={}",
        artifact.scenario,
        artifact.seed,
        artifact.trace_digest,
        artifact_path.display()
    );
    if let Some(failure) = artifact.failure {
        anyhow::bail!("scenario failed: {}", failure.signature);
    }
    Ok(())
}

async fn replay_command(arguments: &[String]) -> anyhow::Result<()> {
    let artifact_path = PathBuf::from(required_option(arguments, "--artifact")?);
    let expected = read_artifact(&artifact_path)?;
    let seed = parse_seed(&expected.seed)?;
    anyhow::ensure!(
        scenario_fault_schedule(seed) == expected.fault_schedule,
        "stored fault schedule differs from the recorded generator expansion"
    );
    let controlled_failure = expected
        .failure
        .as_ref()
        .is_some_and(|failure| failure.signature == "injected_duplicate_delivery_oracle");
    let mut evidence =
        match coordinator::execute(seed, &expected.expanded_schedule, &expected.fault_schedule)
            .await
        {
            Ok(evidence) => evidence,
            Err(error) => fatal_evidence("harness_execution_failed", error),
        };
    if controlled_failure && evidence.failure.is_none() {
        inject_duplicate_oracle_failure(&mut evidence);
    }
    let actual = build_artifact(
        ArtifactBuild {
            seed,
            schedule: expected.expanded_schedule.clone(),
            fault_schedule: expected.fault_schedule.clone(),
            minimized_schedule: expected.minimized_schedule.clone(),
            minimized_fault_schedule: expected.minimized_fault_schedule.clone(),
            minimized_reproduction_digest: expected.minimized_reproduction_digest.clone(),
            evidence,
        },
        &artifact_path,
    )?;
    anyhow::ensure!(
        actual.trace_digest == expected.trace_digest,
        "trace digest mismatch"
    );
    anyhow::ensure!(actual.events == expected.events, "scenario event mismatch");
    anyhow::ensure!(
        actual.network_events == expected.network_events,
        "network event mismatch"
    );
    anyhow::ensure!(
        actual.network_trace == expected.network_trace,
        "network trace mismatch"
    );
    anyhow::ensure!(
        actual.final_state == expected.final_state,
        "final state mismatch"
    );
    anyhow::ensure!(actual.oracles == expected.oracles, "oracle mismatch");
    anyhow::ensure!(
        actual.oracle_batches == expected.oracle_batches,
        "oracle batch mismatch"
    );
    anyhow::ensure!(
        actual.worker_final_states == expected.worker_final_states,
        "worker state mismatch"
    );
    anyhow::ensure!(actual.failure == expected.failure, "failure mismatch");
    // OS PIDs differ between fresh runs and are evidence metadata, not trace identity.
    anyhow::ensure!(
        actual.process_topology.len() == expected.process_topology.len(),
        "process topology size mismatch"
    );
    if controlled_failure {
        let minimized = expected
            .minimized_fault_schedule
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("failure artifact lacks minimized fault schedule"))?;
        let mut minimized_evidence =
            coordinator::execute(seed, &expected.expanded_schedule, minimized).await?;
        inject_duplicate_oracle_failure(&mut minimized_evidence);
        anyhow::ensure!(
            Some(evidence_digest(&minimized_evidence)?) == expected.minimized_reproduction_digest,
            "minimized reproduction digest mismatch"
        );
        anyhow::ensure!(
            minimized_evidence
                .failure
                .as_ref()
                .map(|failure| failure.signature.as_str())
                == Some("injected_duplicate_delivery_oracle"),
            "minimized reproduction changed failure signature"
        );
        assert_fault_schedule_one_minimal(seed, &expected.expanded_schedule, minimized).await?;
    }
    println!(
        "replay=identical seed={} trace_digest={} artifact={}",
        expected.seed,
        expected.trace_digest,
        artifact_path.display()
    );
    Ok(())
}

struct ArtifactBuild {
    seed: u64,
    schedule: Vec<ScenarioStep>,
    fault_schedule: FaultSchedule,
    minimized_schedule: Option<Vec<ScenarioStep>>,
    minimized_fault_schedule: Option<FaultSchedule>,
    minimized_reproduction_digest: Option<String>,
    evidence: RunEvidence,
}

fn build_artifact(input: ArtifactBuild, artifact_path: &Path) -> anyhow::Result<HarnessArtifact> {
    let ArtifactBuild {
        seed,
        schedule,
        fault_schedule,
        minimized_schedule,
        minimized_fault_schedule,
        minimized_reproduction_digest,
        evidence,
    } = input;
    let initial_state_digest = stable_digest(&(
        SCENARIO,
        seed,
        [1u64, 2, 3],
        [0x101u64, 0x202],
        "single-member-groups",
    ))?;
    let trace_digest = stable_digest(&(
        &initial_state_digest,
        &schedule,
        &fault_schedule,
        &minimized_schedule,
        &minimized_fault_schedule,
        &minimized_reproduction_digest,
        &evidence.events,
        &evidence.network_events,
        &evidence.network_trace,
        &evidence.final_state,
        &evidence.oracles,
        &evidence.oracle_batches,
        &evidence.worker_final_states,
        &evidence.failure,
    ))?;
    Ok(HarnessArtifact {
        schema: ARTIFACT_SCHEMA.to_owned(),
        scenario: SCENARIO.to_owned(),
        seed: format_seed(seed),
        generator_version: GENERATOR_VERSION,
        initial_state_digest,
        expanded_schedule: schedule,
        fault_schedule,
        minimized_schedule,
        minimized_fault_schedule,
        minimized_reproduction_digest,
        events: evidence.events,
        network_events: evidence.network_events,
        network_trace: evidence.network_trace,
        trace_digest,
        final_state: evidence.final_state,
        oracles: evidence.oracles,
        oracle_batches: evidence.oracle_batches,
        process_topology: evidence.process_topology,
        worker_final_states: evidence.worker_final_states,
        scope: vec![
            "three real OS worker processes".to_owned(),
            "two single-member Raft groups per worker".to_owned(),
            "seed-expanded application-frame faults".to_owned(),
            "does not prove three-node Raft consensus or real QUIC packet loss".to_owned(),
        ],
        failure: evidence.failure,
        replay_command: format!(
            "rtk cargo run --locked -p chirps-deterministic-harness -- replay --artifact {}",
            artifact_path.display()
        ),
    })
}

fn fatal_evidence(code: &str, error: anyhow::Error) -> RunEvidence {
    RunEvidence {
        events: Vec::new(),
        network_events: Vec::new(),
        network_trace: Vec::new(),
        oracles: Vec::new(),
        oracle_batches: Vec::new(),
        process_topology: Vec::new(),
        worker_final_states: Vec::new(),
        final_state: FinalState {
            active_groups: Vec::new(),
            storage_namespaces: Vec::new(),
            storage_isolated: false,
            virtual_time: 0,
        },
        failure: Some(FailureRecord {
            signature: format!("{code}: {error}"),
            event_ordinal: 0,
        }),
    }
}

fn inject_duplicate_oracle_failure(evidence: &mut RunEvidence) {
    if let Some(event) = evidence.network_events.iter().find(|event| {
        event.kind == alopex_chirps_mock::deterministic::EventKind::Delivered
            && event.parent_packet_id.is_some()
    }) {
        let failed_oracle = chirps_deterministic_harness::OracleRecord {
            event_ordinal: event.sequence as usize,
            subsystem: "network".to_owned(),
            oracle: "injected_duplicate_delivery_oracle".to_owned(),
            passed: false,
        };
        if let Some(batch) = evidence
            .oracle_batches
            .iter_mut()
            .find(|batch| batch.network_event_sequence == event.sequence)
        {
            batch.checks.push(failed_oracle.clone());
        }
        evidence.oracles.push(failed_oracle);
        evidence.failure = Some(FailureRecord {
            signature: "injected_duplicate_delivery_oracle".to_owned(),
            event_ordinal: event.sequence as usize,
        });
    }
}

fn evidence_digest(evidence: &RunEvidence) -> anyhow::Result<String> {
    stable_digest(&(
        &evidence.events,
        &evidence.network_events,
        &evidence.network_trace,
        &evidence.oracles,
        &evidence.oracle_batches,
        &evidence.worker_final_states,
        &evidence.final_state,
        &evidence.failure,
    ))
}

async fn minimize_fault_failure(
    seed: u64,
    schedule: &[ScenarioStep],
    fault_schedule: &FaultSchedule,
    signature: &str,
) -> anyhow::Result<FaultSchedule> {
    let mut current = fault_schedule.clone();
    loop {
        let mut reduced = false;
        for index in 0..current.rules().len() {
            let candidate = current.without_rule(index);
            if reproduces_failure(seed, schedule, &candidate, signature).await? {
                current = candidate;
                reduced = true;
                break;
            }
        }
        if !reduced {
            break;
        }
    }
    loop {
        let mut reduced = false;
        'rules: for rule in 0..current.rules().len() {
            for effect in 0..current.rules()[rule].effects.len() {
                let candidate = current.without_effect(rule, effect);
                if reproduces_failure(seed, schedule, &candidate, signature).await? {
                    current = candidate;
                    reduced = true;
                    break 'rules;
                }
            }
        }
        if !reduced {
            return Ok(current);
        }
    }
}

async fn reproduces_failure(
    seed: u64,
    schedule: &[ScenarioStep],
    fault_schedule: &FaultSchedule,
    signature: &str,
) -> anyhow::Result<bool> {
    let mut evidence = coordinator::execute(seed, schedule, fault_schedule).await?;
    inject_duplicate_oracle_failure(&mut evidence);
    Ok(evidence
        .failure
        .as_ref()
        .map(|failure| failure.signature.as_str())
        == Some(signature))
}

async fn assert_fault_schedule_one_minimal(
    seed: u64,
    schedule: &[ScenarioStep],
    fault_schedule: &FaultSchedule,
) -> anyhow::Result<()> {
    for index in 0..fault_schedule.rules().len() {
        anyhow::ensure!(
            !reproduces_failure(
                seed,
                schedule,
                &fault_schedule.without_rule(index),
                "injected_duplicate_delivery_oracle",
            )
            .await?,
            "minimized fault schedule is not 1-minimal"
        );
    }
    for rule in 0..fault_schedule.rules().len() {
        for effect in 0..fault_schedule.rules()[rule].effects.len() {
            anyhow::ensure!(
                !reproduces_failure(
                    seed,
                    schedule,
                    &fault_schedule.without_effect(rule, effect),
                    "injected_duplicate_delivery_oracle",
                )
                .await?,
                "minimized fault effects are not 1-minimal"
            );
        }
    }
    Ok(())
}

fn option(arguments: &[String], name: &str) -> Option<String> {
    arguments
        .iter()
        .position(|argument| argument == name)
        .and_then(|index| arguments.get(index + 1))
        .cloned()
}

fn required_option(arguments: &[String], name: &str) -> anyhow::Result<String> {
    option(arguments, name).ok_or_else(|| anyhow::anyhow!("missing {name}\n{}", usage()))
}

fn usage() -> &'static str {
    "usage:\n  chirps-deterministic-harness run --scenario multi-raft-v0.6 --seed <u64|0xhex> --artifact <path> [--inject-duplicate-oracle-failure --minimize-on-failure]\n  chirps-deterministic-harness replay --artifact <path>"
}
