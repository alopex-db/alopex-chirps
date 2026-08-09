use alopex_chirps_mock::deterministic::FaultSchedule;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

pub mod protocol;

pub const ARTIFACT_SCHEMA: &str = "chirps.deterministic-fault/v2";
pub const SCENARIO: &str = "multi-raft-v0.6";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ScenarioStep {
    CreateAll {
        group_id: u64,
    },
    ProposeAll {
        group_id: u64,
        command: Vec<u8>,
    },
    EmitVote {
        source: u64,
        target: u64,
        group_id: u64,
        correlation_id: u64,
        term: u64,
    },
    DeliverOne,
    Partition {
        source: u64,
        target: u64,
    },
    Heal {
        source: u64,
        target: u64,
    },
    Drain,
    TickAll,
    Remove {
        node_id: u64,
        group_id: u64,
    },
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEvent {
    pub ordinal: usize,
    pub virtual_time: u64,
    pub component: String,
    pub action: String,
    pub group_id: Option<u64>,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleRecord {
    pub event_ordinal: usize,
    pub subsystem: String,
    pub oracle: String,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleBatch {
    pub network_event_sequence: u64,
    pub worker_observation_digest: String,
    pub checks: Vec<OracleRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessRecord {
    pub node_id: u64,
    pub pid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalState {
    pub active_groups: Vec<u64>,
    pub storage_namespaces: Vec<String>,
    pub storage_isolated: bool,
    pub virtual_time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureRecord {
    pub signature: String,
    pub event_ordinal: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessArtifact {
    pub schema: String,
    pub scenario: String,
    pub seed: String,
    pub generator_version: u32,
    pub initial_state_digest: String,
    pub expanded_schedule: Vec<ScenarioStep>,
    pub fault_schedule: FaultSchedule,
    pub minimized_schedule: Option<Vec<ScenarioStep>>,
    pub minimized_fault_schedule: Option<FaultSchedule>,
    pub minimized_reproduction_digest: Option<String>,
    pub events: Vec<HarnessEvent>,
    pub network_events: Vec<alopex_chirps_mock::deterministic::EventRecord>,
    pub network_trace: Vec<String>,
    pub trace_digest: String,
    pub final_state: FinalState,
    pub oracles: Vec<OracleRecord>,
    pub oracle_batches: Vec<OracleBatch>,
    pub process_topology: Vec<ProcessRecord>,
    pub worker_final_states: Vec<protocol::WorkerObservation>,
    pub scope: Vec<String>,
    pub failure: Option<FailureRecord>,
    pub replay_command: String,
}

pub fn default_schedule() -> Vec<ScenarioStep> {
    let mut schedule = vec![
        ScenarioStep::CreateAll { group_id: 0x101 },
        ScenarioStep::CreateAll { group_id: 0x202 },
        ScenarioStep::ProposeAll {
            group_id: 0x101,
            command: b"sentinel-group-a".to_vec(),
        },
        ScenarioStep::ProposeAll {
            group_id: 0x202,
            command: b"sentinel-group-b".to_vec(),
        },
    ];
    schedule.extend((100..116).map(|correlation_id| ScenarioStep::EmitVote {
        source: 1,
        target: 2,
        group_id: 0x101,
        correlation_id,
        term: 10 + correlation_id,
    }));
    schedule.extend([
        ScenarioStep::DeliverOne,
        ScenarioStep::Partition {
            source: 1,
            target: 2,
        },
        ScenarioStep::Remove {
            node_id: 2,
            group_id: 0x101,
        },
        ScenarioStep::Heal {
            source: 1,
            target: 2,
        },
        ScenarioStep::Drain,
        ScenarioStep::TickAll,
        ScenarioStep::Shutdown,
    ]);
    schedule
}

pub fn parse_seed(value: &str) -> anyhow::Result<u64> {
    if let Some(hex) = value.strip_prefix("0x") {
        Ok(u64::from_str_radix(hex, 16)?)
    } else {
        Ok(value.parse()?)
    }
}

pub fn format_seed(seed: u64) -> String {
    format!("0x{seed:016x}")
}

pub fn stable_digest(value: &impl Serialize) -> anyhow::Result<String> {
    let bytes = serde_json::to_vec(value)?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn read_artifact(path: &Path) -> anyhow::Result<HarnessArtifact> {
    let artifact: HarnessArtifact = serde_json::from_slice(&fs::read(path)?)?;
    anyhow::ensure!(
        artifact.schema == ARTIFACT_SCHEMA,
        "unsupported artifact schema"
    );
    anyhow::ensure!(artifact.scenario == SCENARIO, "unsupported scenario");
    Ok(artifact)
}

pub fn write_artifact_atomic(path: &Path, artifact: &HarnessArtifact) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = temporary_path(path);
    fs::write(&temporary, serde_json::to_vec_pretty(artifact)?)?;
    fs::rename(&temporary, path)?;
    Ok(())
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".tmp");
    PathBuf::from(value)
}

/// One-rule-at-a-time 1-minimization. The evaluator is responsible for
/// constructing fresh system state for every candidate.
pub fn minimize_steps(
    schedule: &[ScenarioStep],
    failure_signature: &str,
    evaluate_fresh: impl Fn(&[ScenarioStep]) -> Option<String>,
) -> Vec<ScenarioStep> {
    let mut current = schedule.to_vec();
    loop {
        let mut reduced = false;
        for index in 0..current.len() {
            let mut candidate = current.clone();
            candidate.remove(index);
            if evaluate_fresh(&candidate).as_deref() == Some(failure_signature) {
                current = candidate;
                reduced = true;
                break;
            }
        }
        if !reduced {
            return current;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execute_from_fresh_state(schedule: &[ScenarioStep]) -> Option<String> {
        let mut partitioned = false;
        for step in schedule {
            match step {
                ScenarioStep::Partition { .. } => partitioned = true,
                ScenarioStep::Heal { .. } => partitioned = false,
                ScenarioStep::EmitVote { .. } if partitioned => {
                    return Some("forward_send_during_partition".to_owned());
                }
                _ => {}
            }
        }
        None
    }

    #[test]
    fn failure_artifact_round_trips_and_minimizes() {
        let schedule = vec![
            ScenarioStep::CreateAll { group_id: 1 },
            ScenarioStep::Partition {
                source: 1,
                target: 2,
            },
            ScenarioStep::EmitVote {
                source: 1,
                target: 2,
                group_id: 1,
                correlation_id: 9,
                term: 1,
            },
            ScenarioStep::Shutdown,
        ];
        let minimized = minimize_steps(
            &schedule,
            "forward_send_during_partition",
            execute_from_fresh_state,
        );
        assert_eq!(
            minimized,
            vec![
                ScenarioStep::Partition {
                    source: 1,
                    target: 2
                },
                ScenarioStep::EmitVote {
                    source: 1,
                    target: 2,
                    group_id: 1,
                    correlation_id: 9,
                    term: 1,
                }
            ]
        );
        for index in 0..minimized.len() {
            let mut candidate = minimized.clone();
            candidate.remove(index);
            assert_ne!(
                execute_from_fresh_state(&candidate).as_deref(),
                Some("forward_send_during_partition")
            );
        }
    }

    #[test]
    fn minimizer_revisits_earlier_steps_after_each_reduction() {
        let schedule = vec![
            ScenarioStep::CreateAll { group_id: 1 },
            ScenarioStep::CreateAll { group_id: 2 },
            ScenarioStep::CreateAll { group_id: 3 },
        ];
        let minimized = minimize_steps(&schedule, "non_monotonic", |candidate| {
            let sequences = candidate
                .iter()
                .filter_map(|step| match step {
                    ScenarioStep::CreateAll { group_id } => Some(*group_id),
                    _ => None,
                })
                .collect::<Vec<_>>();
            matches!(sequences.as_slice(), [1, 2, 3] | [1, 2] | [2])
                .then(|| "non_monotonic".to_owned())
        });
        assert_eq!(minimized, vec![ScenarioStep::CreateAll { group_id: 2 }]);
    }
}
