use crate::protocol::{Request, Response, read_frame, write_frame};
use crate::schema::{GroupMembership, RttPhaseObservation};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

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
        expect_ok(call(nodes[0], Request::CreateSeed { group_id }).await?)?;
        expect_ok(call(nodes[1], Request::CreateUninitialized { group_id }).await?)?;
        retry_membership(
            nodes[0],
            Request::AddLearner {
                group_id,
                node_id: 2,
            },
        )
        .await?;
        expect_ok(call(nodes[2], Request::CreateUninitialized { group_id }).await?)?;
        retry_membership(
            nodes[0],
            Request::AddLearner {
                group_id,
                node_id: 3,
            },
        )
        .await?;
        retry_membership(
            nodes[0],
            Request::Promote {
                group_id,
                voters: vec![1, 2, 3],
            },
        )
        .await?;
    }
    Ok(())
}

async fn retry_membership(address: SocketAddr, request: Request) -> anyhow::Result<()> {
    let bytes = bincode::serialize(&request)?;
    timeout(Duration::from_secs(30), async {
        loop {
            let request = bincode::deserialize(&bytes).expect("request round trip");
            match call(address, request).await {
                Ok(Response::Ok) => break,
                _ => sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("membership operation timed out"))
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
