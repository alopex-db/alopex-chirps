use crate::node::monotonic_ns;
use crate::protocol::{Request, Response, read_frame, write_frame};
use crate::schema::{LoadgenReport, Mode};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};

#[derive(Clone, Debug)]
pub struct LoadgenArgs {
    pub origin_node: u64,
    pub leader_control: SocketAddr,
    pub mode: Mode,
    pub sample_index: u64,
    pub start_at_ns: u64,
    pub output: PathBuf,
    pub warmup_seconds: u64,
    pub measure_seconds: u64,
    pub drain_seconds: u64,
}

#[derive(Default)]
struct Counters {
    committed: u64,
    errors: u64,
    timeouts: u64,
    per_group: BTreeMap<u64, u64>,
    latency_us: BTreeMap<u64, u64>,
}

pub async fn run(args: LoadgenArgs) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..=3).contains(&args.origin_node),
        "origin node must be 1..=3"
    );
    anyhow::ensure!(
        args.warmup_seconds == 15 && args.measure_seconds == 60 && args.drain_seconds == 5,
        "controlled duration is fixed at 15/60/5"
    );
    let measure_start = args.start_at_ns + args.warmup_seconds * 1_000_000_000;
    let measure_end = measure_start + args.measure_seconds * 1_000_000_000;
    let drain_end = measure_end + args.drain_seconds * 1_000_000_000;
    wait_until(args.start_at_ns).await;
    let counters = Arc::new(Mutex::new(Counters::default()));
    let mut tasks = Vec::with_capacity(100);
    for client_index in 0..100u64 {
        let counters = Arc::clone(&counters);
        let mode = args.mode;
        let address = args.leader_control;
        let origin = args.origin_node;
        tasks.push(tokio::spawn(async move {
            let group_id = match mode {
                Mode::MultiRaft => client_index + 1,
                Mode::SingleGroup => 1,
            };
            let payload = payload(origin, client_index, group_id);
            let mut stream = match TcpStream::connect(address).await {
                Ok(stream) => stream,
                Err(_) => {
                    counters.lock().await.errors += 1;
                    return;
                }
            };
            loop {
                let started = monotonic_ns();
                if started >= measure_end {
                    break;
                }
                let during_measure = started >= measure_start;
                let operation = async {
                    write_frame(
                        &mut stream,
                        &Request::Propose {
                            group_id,
                            payload: payload.clone(),
                        },
                    )
                    .await?;
                    let response = read_frame::<_, Response>(&mut stream)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("proposal connection closed"))?;
                    match response {
                        Response::Proposal(response) if response == payload => Ok(()),
                        Response::Error(error) => Err(anyhow::anyhow!(error)),
                        _ => Err(anyhow::anyhow!("unexpected proposal response")),
                    }
                };
                let remaining = Duration::from_nanos(drain_end.saturating_sub(started).max(1));
                let outcome = timeout(remaining.min(Duration::from_secs(5)), operation).await;
                let timed_out = outcome.is_err();
                let ended = monotonic_ns();
                if during_measure {
                    let mut counters = counters.lock().await;
                    match outcome {
                        Ok(Ok(())) if ended <= measure_end => {
                            counters.committed += 1;
                            *counters.per_group.entry(group_id).or_default() += 1;
                            *counters
                                .latency_us
                                .entry((ended - started) / 1_000)
                                .or_default() += 1;
                        }
                        Err(_) => counters.timeouts += 1,
                        Ok(Err(_)) => counters.errors += 1,
                        Ok(Ok(())) => {}
                    }
                }
                if timed_out {
                    break;
                }
            }
        }));
    }
    for task in tasks {
        task.await?;
    }
    wait_until(drain_end).await;
    let counters = Arc::try_unwrap(counters)
        .map_err(|_| anyhow::anyhow!("loadgen counters still shared"))?
        .into_inner();
    let report = LoadgenReport {
        mode: args.mode,
        sample_index: args.sample_index,
        origin_node: args.origin_node,
        clients: 100,
        payload_bytes: 1024,
        monotonic_start_ns: measure_start,
        monotonic_end_ns: measure_end,
        committed: counters.committed,
        errors: counters.errors,
        timeouts: counters.timeouts,
        per_group_committed: counters.per_group,
        latency_us: counters.latency_us,
    };
    write_json_atomic(&args.output, &report)?;
    Ok(())
}

async fn wait_until(deadline: u64) {
    loop {
        let now = monotonic_ns();
        if now >= deadline {
            break;
        }
        sleep(Duration::from_nanos((deadline - now).min(100_000_000))).await;
    }
}

fn payload(origin: u64, client: u64, group: u64) -> Vec<u8> {
    let mut result = vec![0x5a; 1024];
    result[..8].copy_from_slice(&origin.to_be_bytes());
    result[8..16].copy_from_slice(&client.to_be_bytes());
    result[16..24].copy_from_slice(&group.to_be_bytes());
    let mut state = origin ^ client.rotate_left(17) ^ group.rotate_left(33) ^ 0x600;
    for byte in &mut result[24..] {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *byte = (state >> 56) as u8;
    }
    result
}

fn write_json_atomic(path: &std::path::Path, value: &impl serde::Serialize) -> anyhow::Result<()> {
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent)?;
    let temporary = path.with_extension("tmp");
    let bytes = serde_json::to_vec_pretty(value)?;
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}
