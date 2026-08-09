use alopex_chirps_mock::deterministic::FaultSchedule;
use chirps_deterministic_harness::{
    ARTIFACT_SCHEMA, FailureRecord, FinalState, HarnessArtifact, SCENARIO, ScenarioStep,
    minimize_steps, read_artifact, write_artifact_atomic,
};

fn execute_fresh(schedule: &[ScenarioStep]) -> Option<String> {
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
    let minimized = minimize_steps(&schedule, "forward_send_during_partition", execute_fresh);
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
            },
        ]
    );
    for index in 0..minimized.len() {
        let mut candidate = minimized.clone();
        candidate.remove(index);
        assert_ne!(
            execute_fresh(&candidate).as_deref(),
            Some("forward_send_during_partition")
        );
    }

    let directory = tempfile::tempdir().unwrap();
    let artifact_path = directory.path().join("failure.json");
    let fault_schedule = FaultSchedule::empty(9);
    let artifact = HarnessArtifact {
        schema: ARTIFACT_SCHEMA.to_owned(),
        scenario: SCENARIO.to_owned(),
        seed: "0x0000000000000009".to_owned(),
        generator_version: 1,
        initial_state_digest: "initial".to_owned(),
        expanded_schedule: schedule,
        fault_schedule: fault_schedule.clone(),
        minimized_schedule: Some(minimized),
        minimized_fault_schedule: Some(fault_schedule),
        minimized_reproduction_digest: Some("minimized".to_owned()),
        events: Vec::new(),
        network_events: Vec::new(),
        network_trace: Vec::new(),
        trace_digest: "trace".to_owned(),
        final_state: FinalState {
            active_groups: Vec::new(),
            storage_namespaces: Vec::new(),
            storage_isolated: true,
            virtual_time: 0,
        },
        oracles: Vec::new(),
        oracle_batches: Vec::new(),
        process_topology: Vec::new(),
        worker_final_states: Vec::new(),
        scope: Vec::new(),
        failure: Some(FailureRecord {
            signature: "forward_send_during_partition".to_owned(),
            event_ordinal: 2,
        }),
        replay_command: "replay".to_owned(),
    };
    write_artifact_atomic(&artifact_path, &artifact).unwrap();
    assert_eq!(read_artifact(&artifact_path).unwrap(), artifact);
}
