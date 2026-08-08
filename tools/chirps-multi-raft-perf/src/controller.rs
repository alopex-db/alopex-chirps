use crate::protocol::{Request, Response, read_frame, write_frame};
use crate::schema::{GroupMembership, RttPhaseObservation};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

// Bootstrap is intentionally sequential so learner catch-up and promotion are
// observable per group. On a constrained container host, the final groups can
// spend longer than the proposal lane's 30 s default waiting for WAL fsync and
// Raft heartbeats; this timeout is harness readiness, not a performance gate.
const GROUP_READY_TIMEOUT: Duration = Duration::from_secs(120);

pub async fn wait_healthy(nodes: &[SocketAddr; 3]) -> anyhow::Result<()> {
    timeout(Duration::from_secs(30), async {
        loop {
            let mut ready = true;
            for address in nodes {
                ready &= matches!(call(*address, Request::Health).await, Ok(Response::Ok));
            }
            if ready {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("nodes did not become healthy"))
}

pub async fn bootstrap(nodes: &[SocketAddr; 3], groups: u64) -> anyhow::Result<()> {
    anyhow::ensure!((1..=100).contains(&groups), "groups must be 1..=100");
    wait_healthy(nodes).await?;
    for group_id in 1..=groups {
        let owner_index = bootstrap_owner(group_id);
        let owner = nodes[owner_index];
        expect_ok(call(owner, Request::CreateSeed { group_id }).await?)?;
        let follower_ids = (1..=3u64).filter(|node_id| *node_id != (owner_index + 1) as u64);
        for node_id in follower_ids {
            let follower = nodes[(node_id - 1) as usize];
            expect_ok(call(follower, Request::CreateUninitialized { group_id }).await?)?;
            retry_membership(owner, Request::AddLearner { group_id, node_id }).await?;
        }
        retry_membership(
            owner,
            Request::Promote {
                group_id,
                voters: vec![1, 2, 3],
            },
        )
        .await?;
        wait_group_ready(nodes, group_id).await?;
    }
    Ok(())
}

fn bootstrap_owner(group_id: u64) -> usize {
    ((group_id - 1) % 3) as usize
}

async fn wait_group_ready(nodes: &[SocketAddr; 3], group_id: u64) -> anyhow::Result<()> {
    let result = timeout(GROUP_READY_TIMEOUT, async {
        loop {
            let mut states = Vec::with_capacity(nodes.len());
            for address in nodes {
                let response = call(*address, Request::State { group_id }).await;
                let Ok(Response::State(state)) = response else {
                    states.clear();
                    break;
                };
                states.push(state);
            }
            if group_is_ready(&states) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    if result.is_ok() {
        return Ok(());
    }

    // Preserve the final control-plane observation so a bootstrap timeout is
    // actionable. This is an error-path diagnostic only and does not alter the
    // readiness contract or the normal performance path.
    let states = nodes
        .iter()
        .map(|address| async move { call(*address, Request::State { group_id }).await })
        .collect::<Vec<_>>();
    let mut observed = Vec::with_capacity(states.len());
    for state in states {
        observed.push(match state.await {
            Ok(Response::State(state)) => format!(
                "node{}:leader={},applied={},digest={}",
                state.node_id, state.leader_id, state.last_applied, state.committed_digest
            ),
            Ok(other) => format!("unexpected={other:?}"),
            Err(error) => format!("error={error:#}"),
        });
    }
    anyhow::bail!(
        "group {group_id} did not converge before measurement; {}",
        observed.join("; ")
    )
}

fn group_is_ready(states: &[crate::schema::ReplicaState]) -> bool {
    let Some(first) = states.first() else {
        return false;
    };
    states.len() == 3
        && states.iter().all(|state| {
            state.voters == vec![1, 2, 3]
                && state.leader_id != 0
                && state.leader_id == first.leader_id
                && state.last_applied == first.last_applied
                && state.last_applied > 0
                && state.committed_digest == first.committed_digest
        })
}

async fn retry_membership(address: SocketAddr, request: Request) -> anyhow::Result<()> {
    let bytes = bincode::serialize(&request)?;
    let description = format!("{request:?}");
    let mut last_error = "operation was not attempted".to_owned();
    timeout(Duration::from_secs(30), async {
        loop {
            let request = bincode::deserialize(&bytes).expect("request round trip");
            match call(address, request).await {
                Ok(Response::Ok) => break,
                Ok(Response::Error(error)) => last_error = error,
                Ok(response) => last_error = format!("unexpected response: {response:?}"),
                Err(error) => last_error = format!("control call failed: {error:#}"),
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "membership operation timed out for {description}; last error: {last_error}"
        )
    })
}

pub async fn probe_all(
    nodes: &[SocketAddr; 3],
    count: u64,
) -> anyhow::Result<Vec<RttPhaseObservation>> {
    let mut result = Vec::with_capacity(6);
    for source in 1..=3u64 {
        for destination in 1..=3u64 {
            if source == destination {
                continue;
            }
            let response = call(
                nodes[(source - 1) as usize],
                Request::Probe {
                    target: destination,
                    count,
                },
            )
            .await?;
            let Response::Probe { mut samples_us } = response else {
                anyhow::bail!("unexpected probe response");
            };
            samples_us.sort_unstable();
            anyhow::ensure!(!samples_us.is_empty(), "probe returned no samples");
            result.push(RttPhaseObservation {
                source,
                destination,
                p50: percentile(&samples_us, 0.50) as f64 / 1000.0,
                p95: percentile(&samples_us, 0.95) as f64 / 1000.0,
            });
        }
    }
    Ok(result)
}

pub async fn collect_membership(
    nodes: &[SocketAddr; 3],
    groups: u64,
) -> anyhow::Result<Vec<GroupMembership>> {
    let mut result = Vec::with_capacity(groups as usize);
    for group_id in 1..=groups {
        let mut replicas = Vec::with_capacity(3);
        for address in nodes {
            let Response::State(state) = call(*address, Request::State { group_id }).await? else {
                anyhow::bail!("unexpected state response");
            };
            replicas.push(state);
        }
        result.push(GroupMembership { group_id, replicas });
    }
    Ok(result)
}

pub async fn call(address: SocketAddr, request: Request) -> anyhow::Result<Response> {
    let mut stream = TcpStream::connect(address).await?;
    write_frame(&mut stream, &request).await?;
    read_frame(&mut stream)
        .await?
        .ok_or_else(|| anyhow::anyhow!("control connection closed"))
}

fn expect_ok(response: Response) -> anyhow::Result<()> {
    match response {
        Response::Ok => Ok(()),
        Response::Error(error) => anyhow::bail!(error),
        _ => anyhow::bail!("unexpected control response"),
    }
}

fn percentile(values: &[u64], percentile: f64) -> u64 {
    let index = ((values.len() as f64 * percentile).ceil() as usize).saturating_sub(1);
    values[index.min(values.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::{bootstrap_owner, group_is_ready};
    use crate::schema::ReplicaState;

    fn state(node_id: u64, leader_id: u64, last_applied: u64, digest: &str) -> ReplicaState {
        ReplicaState {
            node_id,
            voters: vec![1, 2, 3],
            leader_id,
            last_applied,
            committed_digest: digest.to_owned(),
        }
    }

    #[test]
    fn bootstrap_ready_requires_caught_up_replicas() {
        let ready = vec![
            state(1, 2, 8, "digest"),
            state(2, 2, 8, "digest"),
            state(3, 2, 8, "digest"),
        ];
        assert!(group_is_ready(&ready));

        let lagging = vec![
            state(1, 2, 8, "digest"),
            state(2, 2, 8, "digest"),
            state(3, 0, 0, ""),
        ];
        assert!(!group_is_ready(&lagging));
    }

    #[test]
    fn bootstrap_owner_round_robins_groups_across_three_nodes() {
        let owners = (1..=6).map(bootstrap_owner).collect::<Vec<_>>();
        assert_eq!(owners, vec![0, 1, 2, 0, 1, 2]);
    }
}
