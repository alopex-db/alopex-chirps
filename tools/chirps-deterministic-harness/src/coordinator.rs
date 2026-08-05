use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_mock::deterministic::{
    DeterministicBackend, DeterministicNetwork, DirectedLink, EventKind, EventRecord, FaultEffect,
    FaultSchedule,
};
use alopex_chirps_wire::frame::Frame;
use alopex_chirps_wire::node_id::NodeId;
use chirps_deterministic_harness::protocol::{
    ParentMessage, WORKER_PROTOCOL_VERSION, WorkerAction, WorkerFailure, WorkerMessage,
    WorkerObservation, WorkerResult, read_message, write_message,
};
use chirps_deterministic_harness::{
    FailureRecord, FinalState, HarnessEvent, OracleBatch, OracleRecord, ProcessRecord,
    ScenarioStep, default_schedule, stable_digest,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;

const GROUP_A: u64 = 0x101;
const GROUP_B: u64 = 0x202;

pub struct RunEvidence {
    pub events: Vec<HarnessEvent>,
    pub network_events: Vec<EventRecord>,
    pub network_trace: Vec<String>,
    pub oracles: Vec<OracleRecord>,
    pub oracle_batches: Vec<OracleBatch>,
    pub process_topology: Vec<ProcessRecord>,
    pub worker_final_states: Vec<WorkerObservation>,
    pub final_state: FinalState,
    pub failure: Option<FailureRecord>,
}

struct WorkerClient {
    node_id: u64,
    pid: u32,
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    backend: Arc<DeterministicBackend>,
    next_operation: u64,
}

impl WorkerClient {
    async fn spawn(
        executable: &Path,
        node_id: u64,
        storage_root: &Path,
        backend: Arc<DeterministicBackend>,
    ) -> anyhow::Result<Self> {
        let mut child = Command::new(executable)
            .arg("worker")
            .arg("--node-id")
            .arg(node_id.to_string())
            .arg("--storage-root")
            .arg(storage_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("child stdin"))?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("child stdout"))?;
        let ready: WorkerMessage = read_message(&mut stdout).await?;
        let (protocol_version, ready_node, pid) = match ready {
            WorkerMessage::Ready {
                protocol_version,
                node_id,
                pid,
            } => (protocol_version, node_id, pid),
            _ => anyhow::bail!("worker did not send Ready"),
        };
        anyhow::ensure!(
            protocol_version == WORKER_PROTOCOL_VERSION,
            "worker protocol mismatch"
        );
        anyhow::ensure!(ready_node == node_id, "worker node mismatch");
        Ok(Self {
            node_id,
            pid,
            child,
            stdin,
            stdout,
            backend,
            next_operation: 1,
        })
    }

    async fn call(
        &mut self,
        action: WorkerAction,
    ) -> anyhow::Result<Result<WorkerResult, WorkerFailure>> {
        let operation_id = self.next_operation;
        self.next_operation += 1;
        write_message(
            &mut self.stdin,
            &ParentMessage::Command(Box::new(
                chirps_deterministic_harness::protocol::WorkerCommand {
                    operation_id,
                    action,
                },
            )),
        )
        .await?;
        loop {
            match read_message::<WorkerMessage>(&mut self.stdout).await? {
                WorkerMessage::Response {
                    operation_id: response_id,
                    result,
                } if response_id == operation_id => return Ok(result),
                WorkerMessage::OutboundFrame {
                    outbound_id,
                    source,
                    target,
                    frame,
                } => {
                    anyhow::ensure!(source == wire_id(self.node_id), "worker source mismatch");
                    let result = self.backend.send(target, *frame).await;
                    write_message(
                        &mut self.stdin,
                        &ParentMessage::NetworkAccepted {
                            outbound_id,
                            accepted: result.is_ok(),
                            reason: result.err().map(|error| error.to_string()),
                        },
                    )
                    .await?;
                }
                WorkerMessage::Fatal { failure } => {
                    anyhow::bail!("worker fatal {}: {}", failure.code, failure.detail)
                }
                WorkerMessage::Response { operation_id, .. } => {
                    anyhow::bail!("unexpected worker response operation {operation_id}")
                }
                WorkerMessage::Ready { .. } => anyhow::bail!("duplicate worker Ready"),
            }
        }
    }

    fn is_alive(&mut self) -> bool {
        self.child.try_wait().ok().flatten().is_none()
    }

    async fn shutdown(&mut self) {
        let _ = self.call(WorkerAction::Shutdown).await;
        // The worker has acknowledged manager/backend shutdown. Terminate the
        // remaining protocol runtime so a backend Arc retained by an OpenRaft
        // observer cannot keep the stdout channel and OS process alive.
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }

    async fn terminate(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

pub fn scenario_fault_schedule(seed: u64) -> FaultSchedule {
    FaultSchedule::from_seed(seed, [DirectedLink::new(wire_id(1), wire_id(2))], 16)
}

pub async fn execute(
    seed: u64,
    scenario: &[ScenarioStep],
    fault_schedule: &FaultSchedule,
) -> anyhow::Result<RunEvidence> {
    anyhow::ensure!(fault_schedule.seed == seed, "fault schedule seed mismatch");
    anyhow::ensure!(
        scenario == default_schedule(),
        "unsupported scenario schedule"
    );
    let executable = std::env::current_exe()?;
    let root = tempfile::tempdir()?;
    let network = DeterministicNetwork::new(fault_schedule.clone());
    let mut workers = BTreeMap::new();
    let mut receivers = BTreeMap::<u64, mpsc::Receiver<(NodeId, Frame)>>::new();

    for node_id in 1..=3 {
        eprintln!("harness: spawning worker {node_id}");
        let backend = Arc::new(network.add_node(wire_id(node_id)).await);
        receivers.insert(node_id, backend.subscribe().await?);
        let worker = WorkerClient::spawn(
            &executable,
            node_id,
            &root.path().join(format!("node-{node_id}")),
            backend,
        )
        .await?;
        workers.insert(node_id, worker);
    }

    let result = execute_inner(scenario, &network, &mut workers, &mut receivers).await;
    if let Err(error) = &result {
        eprintln!("harness: scenario error before cleanup: {error:#}");
        for worker in workers.values_mut() {
            worker.terminate().await;
        }
    } else {
        for worker in workers.values_mut() {
            eprintln!("harness: shutting down worker {}", worker.node_id);
            worker.shutdown().await;
        }
    }
    result
}

async fn execute_inner(
    _scenario: &[ScenarioStep],
    network: &DeterministicNetwork,
    workers: &mut BTreeMap<u64, WorkerClient>,
    receivers: &mut BTreeMap<u64, mpsc::Receiver<(NodeId, Frame)>>,
) -> anyhow::Result<RunEvidence> {
    let mut events = Vec::new();
    let mut oracle_batches = Vec::new();
    let mut route_outcomes = BTreeMap::<u64, bool>::new();
    let mut next_network_event = 0;
    let mut ordinal = 0usize;

    for node_id in 1..=3 {
        for group_id in [GROUP_A, GROUP_B] {
            expect_ok(
                workers
                    .get_mut(&node_id)
                    .unwrap()
                    .call(WorkerAction::CreateGroup { group_id })
                    .await?,
            )?;
            push_event(
                &mut events,
                ordinal,
                network,
                "multi_process",
                "create",
                Some(group_id),
                format!("node_{node_id}"),
            )
            .await;
            ordinal += 1;
        }
    }
    eprintln!("harness: groups created");
    for node_id in 1..=3 {
        for (group_id, command) in [
            (GROUP_A, b"sentinel-group-a".to_vec()),
            (GROUP_B, b"sentinel-group-b".to_vec()),
        ] {
            let result = expect_ok(
                workers
                    .get_mut(&node_id)
                    .unwrap()
                    .call(WorkerAction::Propose {
                        group_id,
                        command: command.clone(),
                    })
                    .await?,
            )?;
            anyhow::ensure!(
                matches!(result, WorkerResult::Proposed { response, .. } if response == command),
                "sentinel proposal response mismatch"
            );
            push_event(
                &mut events,
                ordinal,
                network,
                "multi_process",
                "propose_sentinel",
                Some(group_id),
                format!("node_{node_id}"),
            )
            .await;
            ordinal += 1;
        }
    }
    eprintln!("harness: sentinels committed");
    let baseline = observe_all(workers).await?;
    assert_nontrivial_group_state(&baseline)?;

    for correlation_id in 100..116 {
        expect_ok(
            workers
                .get_mut(&1)
                .unwrap()
                .call(WorkerAction::EmitRaftVote {
                    target: wire_id(2),
                    group_id: GROUP_A,
                    correlation_id,
                    term: 10 + correlation_id,
                })
                .await?,
        )?;
        push_event(
            &mut events,
            ordinal,
            network,
            "multi_process",
            "emit_raft_vote",
            Some(GROUP_A),
            format!("correlation_{correlation_id}"),
        )
        .await;
        ordinal += 1;
        record_new_network_oracles(
            network,
            workers,
            &baseline,
            &route_outcomes,
            &mut next_network_event,
            &mut oracle_batches,
        )
        .await?;
    }
    eprintln!("harness: votes enqueued");

    // Deliver one packet while the group still exists, proving that a faulted
    // application frame reaches the target manager's real frame-routing API.
    let mut successful_route = false;
    while !successful_route && network.deliver_next().await? {
        let sequence = network.events().await.last().unwrap().sequence;
        successful_route = deliver_ready_frames(workers, receivers, false, sequence).await?;
        route_outcomes.insert(sequence, successful_route);
        if successful_route {
            push_event(
                &mut events,
                ordinal,
                network,
                "multi_process",
                "deliver_raft_frame",
                Some(GROUP_A),
                format!("network_sequence_{sequence}_accepted"),
            )
            .await;
            ordinal += 1;
        }
        record_new_network_oracles(
            network,
            workers,
            &baseline,
            &route_outcomes,
            &mut next_network_event,
            &mut oracle_batches,
        )
        .await?;
    }
    anyhow::ensure!(successful_route, "no faulted Raft frame reached node 2");
    eprintln!("harness: first route delivered");

    let link = DirectedLink::new(wire_id(1), wire_id(2));
    network.partition(link).await;
    eprintln!("harness: link partitioned");
    record_new_network_oracles(
        network,
        workers,
        &baseline,
        &route_outcomes,
        &mut next_network_event,
        &mut oracle_batches,
    )
    .await?;

    let removed = expect_ok(
        workers
            .get_mut(&2)
            .unwrap()
            .call(WorkerAction::RemoveGroup { group_id: GROUP_A })
            .await?,
    )?;
    anyhow::ensure!(matches!(
        removed,
        WorkerResult::Removed { existed: true, .. }
    ));
    push_event(
        &mut events,
        ordinal,
        network,
        "multi_process",
        "remove",
        Some(GROUP_A),
        "node_2".to_owned(),
    )
    .await;
    ordinal += 1;
    eprintln!("harness: group removed");

    network.heal(link).await;
    eprintln!("harness: link healed");
    record_new_network_oracles(
        network,
        workers,
        &baseline,
        &route_outcomes,
        &mut next_network_event,
        &mut oracle_batches,
    )
    .await?;

    let mut unknown_group_observed = false;
    while network.deliver_next().await? {
        let sequence = network.events().await.last().unwrap().sequence;
        let typed_rejection = deliver_ready_frames(workers, receivers, true, sequence).await?;
        route_outcomes.insert(sequence, typed_rejection);
        unknown_group_observed |= typed_rejection;
        if typed_rejection {
            push_event(
                &mut events,
                ordinal,
                network,
                "multi_process",
                "deliver_removed_group_frame",
                Some(GROUP_A),
                format!("network_sequence_{sequence}_unknown_group"),
            )
            .await;
            ordinal += 1;
        }
        record_new_network_oracles(
            network,
            workers,
            &baseline,
            &route_outcomes,
            &mut next_network_event,
            &mut oracle_batches,
        )
        .await?;
    }
    anyhow::ensure!(
        unknown_group_observed,
        "delayed frame did not observe removed group"
    );
    anyhow::ensure!(
        network.pending_count().await == 0,
        "network queue not empty"
    );
    eprintln!("harness: queue drained");

    for node_id in 1..=3 {
        let result = expect_ok(
            workers
                .get_mut(&node_id)
                .unwrap()
                .call(WorkerAction::TickRaft)
                .await?,
        )?;
        anyhow::ensure!(matches!(result, WorkerResult::Ticked { .. }));
        push_event(
            &mut events,
            ordinal,
            network,
            "multi_process",
            "tick_all",
            None,
            format!("node_{node_id}"),
        )
        .await;
        ordinal += 1;
    }

    let final_states = observe_all(workers).await?;
    eprintln!("harness: ticks and observations complete");
    assert_group_b_isolated(&baseline, &final_states)?;
    let process_topology = workers
        .values()
        .map(|worker| ProcessRecord {
            node_id: worker.node_id,
            pid: worker.pid,
        })
        .collect::<Vec<_>>();
    let mut pids = process_topology
        .iter()
        .map(|entry| entry.pid)
        .collect::<Vec<_>>();
    pids.sort_unstable();
    pids.dedup();
    anyhow::ensure!(pids.len() == 3, "workers are not distinct OS processes");

    let network_events = network.events().await;
    let oracles = oracle_batches
        .iter()
        .flat_map(|batch| batch.checks.clone())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        oracle_batches.len() == network_events.len(),
        "not every network event has an oracle batch"
    );
    let storage_isolated = storage_isolation_ok(&final_states);
    anyhow::ensure!(storage_isolated, "storage isolation oracle failed");
    let namespaces = final_states
        .iter()
        .flat_map(|worker| worker.groups.values().map(|group| group.namespace.clone()))
        .collect::<Vec<_>>();
    Ok(RunEvidence {
        events,
        network_trace: network.stable_trace().await,
        network_events,
        oracles,
        oracle_batches,
        process_topology,
        worker_final_states: final_states.clone(),
        final_state: FinalState {
            active_groups: final_states
                .iter()
                .flat_map(|worker| worker.active_groups.iter().copied())
                .collect(),
            storage_namespaces: namespaces,
            storage_isolated,
            virtual_time: network.now().await,
        },
        failure: None,
    })
}

async fn deliver_ready_frames(
    workers: &mut BTreeMap<u64, WorkerClient>,
    receivers: &mut BTreeMap<u64, mpsc::Receiver<(NodeId, Frame)>>,
    removed_expected: bool,
    network_sequence: u64,
) -> anyhow::Result<bool> {
    let mut observed = false;
    for (node_id, receiver) in receivers.iter_mut() {
        while let Ok((source, frame)) = receiver.try_recv() {
            let result = workers
                .get_mut(node_id)
                .unwrap()
                .call(WorkerAction::DeliverFrame {
                    network_sequence,
                    source,
                    frame: Box::new(frame),
                })
                .await?;
            match result {
                Ok(WorkerResult::FrameAccepted { route, .. }) if !removed_expected => {
                    anyhow::ensure!(route.group_id == GROUP_A, "routed wrong group");
                    anyhow::ensure!(
                        (100..116).contains(&route.correlation_id),
                        "routed wrong correlation"
                    );
                    anyhow::ensure!(
                        route.response_kind == "vote_response",
                        "routed response has wrong Raft message kind"
                    );
                    observed = true;
                }
                Err(failure) if removed_expected && failure.code == "unknown_group" => {
                    observed = true;
                }
                Ok(other) => anyhow::bail!("unexpected delivery result: {other:?}"),
                Err(failure) => anyhow::bail!(
                    "unexpected worker failure {}: {}",
                    failure.code,
                    failure.detail
                ),
            }
        }
    }
    Ok(observed)
}

async fn observe_all(
    workers: &mut BTreeMap<u64, WorkerClient>,
) -> anyhow::Result<Vec<WorkerObservation>> {
    let mut observations = Vec::new();
    for worker in workers.values_mut() {
        let result = expect_ok(worker.call(WorkerAction::Observe).await?)?;
        let WorkerResult::Observation { value } = result else {
            anyhow::bail!("worker returned a non-observation")
        };
        observations.push(value);
    }
    Ok(observations)
}

async fn record_new_network_oracles(
    network: &DeterministicNetwork,
    workers: &mut BTreeMap<u64, WorkerClient>,
    baseline: &[WorkerObservation],
    route_outcomes: &BTreeMap<u64, bool>,
    next_sequence: &mut u64,
    batches: &mut Vec<OracleBatch>,
) -> anyhow::Result<()> {
    let new_events = network.events_after(*next_sequence).await;
    if new_events.is_empty() {
        return Ok(());
    }
    let observations = observe_all(workers).await?;
    assert_group_b_isolated(baseline, &observations)?;
    let digest = stable_digest(&observations)?;
    let workers_alive = workers.values_mut().all(WorkerClient::is_alive);
    for event in new_events {
        let frame_event = matches!(
            event.kind,
            EventKind::Scheduled
                | EventKind::Delivered
                | EventKind::Dropped
                | EventKind::StaleGeneration
        );
        let checks = vec![
            oracle(
                event.sequence,
                "process",
                "all_workers_alive",
                workers_alive,
            ),
            oracle(
                event.sequence,
                "raft",
                "group_b_semantic_state_isolated",
                true,
            ),
            oracle(
                event.sequence,
                "network",
                "frame_semantic_identity_recorded",
                !frame_event || event.frame_fingerprint.is_some(),
            ),
            oracle(
                event.sequence,
                "raft",
                "delivered_frame_routed_or_typed_rejected",
                event.kind != EventKind::Delivered
                    || route_outcomes
                        .get(&event.sequence)
                        .copied()
                        .unwrap_or(false),
            ),
        ];
        anyhow::ensure!(
            checks.iter().all(|check| check.passed),
            "network oracle failed"
        );
        batches.push(OracleBatch {
            network_event_sequence: event.sequence,
            worker_observation_digest: digest.clone(),
            checks,
        });
        *next_sequence = event.sequence + 1;
    }
    Ok(())
}

fn assert_group_b_isolated(
    baseline: &[WorkerObservation],
    current: &[WorkerObservation],
) -> anyhow::Result<()> {
    anyhow::ensure!(baseline.len() == current.len(), "worker count changed");
    for (before, after) in baseline.iter().zip(current) {
        anyhow::ensure!(before.node_id == after.node_id, "worker ordering changed");
        let before_group = before
            .groups
            .get(&GROUP_B.to_string())
            .ok_or_else(|| anyhow::anyhow!("baseline group B missing"))?;
        let after_group = after
            .groups
            .get(&GROUP_B.to_string())
            .ok_or_else(|| anyhow::anyhow!("group B missing"))?;
        anyhow::ensure!(after_group.accepting, "group B stopped accepting");
        anyhow::ensure!(
            before_group.namespace == after_group.namespace,
            "group B namespace changed"
        );
        anyhow::ensure!(after_group.wal_exists, "group B WAL directory is absent");
        anyhow::ensure!(
            after_group.snapshot_exists,
            "group B snapshot directory is absent"
        );
        anyhow::ensure!(
            before_group.state_machine_applies == after_group.state_machine_applies
                && before_group.state_machine_digest == after_group.state_machine_digest,
            "group B semantic state changed during group A routing"
        );
        let namespaces = after
            .groups
            .values()
            .map(|group| group.namespace.as_str())
            .collect::<Vec<_>>();
        let mut unique = namespaces.clone();
        unique.sort_unstable();
        unique.dedup();
        anyhow::ensure!(unique.len() == namespaces.len(), "group namespaces overlap");
    }
    Ok(())
}

fn assert_nontrivial_group_state(observations: &[WorkerObservation]) -> anyhow::Result<()> {
    for worker in observations {
        let group_a = worker
            .groups
            .get(&GROUP_A.to_string())
            .ok_or_else(|| anyhow::anyhow!("group A missing after sentinel"))?;
        let group_b = worker
            .groups
            .get(&GROUP_B.to_string())
            .ok_or_else(|| anyhow::anyhow!("group B missing after sentinel"))?;
        anyhow::ensure!(
            group_a.state_machine_applies > 0 && group_b.state_machine_applies > 0,
            "sentinel state was not applied"
        );
        anyhow::ensure!(
            group_a.state_machine_digest != group_b.state_machine_digest,
            "group sentinel digests are not isolated"
        );
    }
    Ok(())
}

fn storage_isolation_ok(observations: &[WorkerObservation]) -> bool {
    observations.iter().all(|worker| {
        let paths = worker
            .groups
            .values()
            .flat_map(|group| [group.wal_path.as_str(), group.snapshot_path.as_str()])
            .collect::<Vec<_>>();
        let mut unique = paths.clone();
        unique.sort_unstable();
        unique.dedup();
        unique.len() == paths.len()
            && worker.groups.values().all(|group| {
                group.wal_exists
                    && group.snapshot_exists
                    && group.wal_path == format!("wal/{}", group.namespace)
                    && group.snapshot_path == format!("snapshot/{}", group.namespace)
            })
    })
}

fn expect_ok(result: Result<WorkerResult, WorkerFailure>) -> anyhow::Result<WorkerResult> {
    result.map_err(|failure| anyhow::anyhow!("{}: {}", failure.code, failure.detail))
}

fn oracle(sequence: u64, subsystem: &str, name: &str, passed: bool) -> OracleRecord {
    OracleRecord {
        event_ordinal: sequence as usize,
        subsystem: subsystem.to_owned(),
        oracle: name.to_owned(),
        passed,
    }
}

async fn push_event(
    events: &mut Vec<HarnessEvent>,
    ordinal: usize,
    network: &DeterministicNetwork,
    component: &str,
    action: &str,
    group_id: Option<u64>,
    outcome: String,
) {
    events.push(HarnessEvent {
        ordinal,
        virtual_time: network.now().await,
        component: component.to_owned(),
        action: action.to_owned(),
        group_id,
        outcome,
    });
}

pub fn ensure_fault_coverage(schedule: &FaultSchedule) -> anyhow::Result<()> {
    let effects = schedule
        .rules()
        .iter()
        .flat_map(|rule| rule.effects.iter())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        effects
            .iter()
            .any(|effect| matches!(effect, FaultEffect::Delay { .. })),
        "seed lacks delay"
    );
    anyhow::ensure!(
        effects
            .iter()
            .any(|effect| matches!(effect, FaultEffect::Drop)),
        "seed lacks loss"
    );
    anyhow::ensure!(
        effects
            .iter()
            .any(|effect| matches!(effect, FaultEffect::Duplicate { .. })),
        "seed lacks duplicate"
    );
    anyhow::ensure!(
        effects
            .iter()
            .any(|effect| matches!(effect, FaultEffect::Reorder { .. })),
        "seed lacks reorder"
    );
    Ok(())
}

pub fn wire_id(node_id: u64) -> NodeId {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&node_id.to_be_bytes());
    NodeId::from(bytes)
}
