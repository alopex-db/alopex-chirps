use crate::controller::call;
use crate::node::monotonic_ns;
use crate::protocol::{Request, Response, read_frame, write_frame};
use crate::schema::{LoadgenReport, Mode};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};

#[derive(Clone, Debug)]
pub struct LoadgenArgs {
    pub origin_node: u64,
    pub nodes: [SocketAddr; 3],
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
    let peak_rss_bytes = Arc::new(AtomicU64::new(current_rss_bytes()));
    let rss_warmup_peak_bytes = Arc::new(AtomicU64::new(0));
    let rss_measure_peak_bytes = Arc::new(AtomicU64::new(0));
    let rss_drain_peak_bytes = Arc::new(AtomicU64::new(0));
    let rss_start_bytes = peak_rss_bytes.load(Ordering::Relaxed);
    let rss_sampler = {
        let peak_rss_bytes = Arc::clone(&peak_rss_bytes);
        let rss_warmup_peak_bytes = Arc::clone(&rss_warmup_peak_bytes);
        let rss_measure_peak_bytes = Arc::clone(&rss_measure_peak_bytes);
        let rss_drain_peak_bytes = Arc::clone(&rss_drain_peak_bytes);
        tokio::spawn(async move {
            loop {
                let rss = current_rss_bytes();
                peak_rss_bytes.fetch_max(rss, Ordering::Relaxed);
                match phase_for(monotonic_ns(), measure_start, measure_end, drain_end) {
                    LoadgenPhase::Warmup => {
                        rss_warmup_peak_bytes.fetch_max(rss, Ordering::Relaxed);
                    }
                    LoadgenPhase::Measure => {
                        rss_measure_peak_bytes.fetch_max(rss, Ordering::Relaxed);
                    }
                    LoadgenPhase::Drain => {
                        rss_drain_peak_bytes.fetch_max(rss, Ordering::Relaxed);
                    }
                }
                if monotonic_ns() >= drain_end {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
    };
    let mut tasks = Vec::with_capacity(100);
    for client_index in 0..100u64 {
        let counters = Arc::clone(&counters);
        let mode = args.mode;
        let nodes = args.nodes;
        let origin = args.origin_node;
        tasks.push(tokio::spawn(async move {
            let group_id = match mode {
                Mode::MultiRaft => client_index + 1,
                Mode::SingleGroup => 1,
            };
            let mut session = ClientSession::new(nodes, group_id);
            let mut sequence = 0;
            loop {
                let started = monotonic_ns();
                if started >= measure_end {
                    break;
                }
                let during_measure = started >= measure_start;
                let payload = payload(origin, client_index, group_id, sequence);
                sequence = sequence.wrapping_add(1);
                let deadline = started.saturating_add(5_000_000_000).min(drain_end);
                let outcome = session.propose(payload, deadline).await;
                let ended = monotonic_ns();
                if during_measure {
                    let mut counters = counters.lock().await;
                    match outcome {
                        Ok(()) if ended <= measure_end => {
                            counters.committed += 1;
                            *counters.per_group.entry(group_id).or_default() += 1;
                            *counters
                                .latency_us
                                .entry((ended - started) / 1_000)
                                .or_default() += 1;
                        }
                        Err(AttemptFailure::Timeout) => counters.timeouts += 1,
                        Err(AttemptFailure::Error) => counters.errors += 1,
                        Ok(()) => {}
                    }
                }
            }
        }));
    }
    for task in tasks {
        task.await?;
    }
    wait_until(drain_end).await;
    rss_sampler.await?;
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
        peak_rss_bytes: peak_rss_bytes.load(Ordering::Relaxed),
        rss_start_bytes,
        rss_warmup_peak_bytes: rss_warmup_peak_bytes.load(Ordering::Relaxed),
        rss_measure_peak_bytes: rss_measure_peak_bytes.load(Ordering::Relaxed),
        rss_drain_peak_bytes: rss_drain_peak_bytes.load(Ordering::Relaxed),
    };
    write_json_atomic(&args.output, &report)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoadgenPhase {
    Warmup,
    Measure,
    Drain,
}

fn phase_for(now: u64, measure_start: u64, measure_end: u64, drain_end: u64) -> LoadgenPhase {
    if now < measure_start {
        LoadgenPhase::Warmup
    } else if now < measure_end {
        LoadgenPhase::Measure
    } else {
        debug_assert!(now <= drain_end || drain_end == 0);
        LoadgenPhase::Drain
    }
}

fn current_rss_bytes() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:")?.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .map(|kib| kib.saturating_mul(1024))
        .unwrap_or(0)
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

struct ClientSession {
    nodes: [SocketAddr; 3],
    group_id: u64,
    leader: usize,
    stream: Option<TcpStream>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttemptFailure {
    Timeout,
    Error,
}

impl ClientSession {
    fn new(nodes: [SocketAddr; 3], group_id: u64) -> Self {
        Self {
            nodes,
            group_id,
            leader: 0,
            stream: None,
        }
    }

    async fn propose(&mut self, payload: Vec<u8>, deadline: u64) -> Result<(), AttemptFailure> {
        let mut observed_error = false;
        loop {
            let now = monotonic_ns();
            if now >= deadline {
                return Err(if observed_error {
                    AttemptFailure::Error
                } else {
                    AttemptFailure::Timeout
                });
            }
            if self.stream.is_none() && self.connect(deadline).await.is_err() {
                sleep(Duration::from_millis(1)).await;
                continue;
            }
            let remaining = Duration::from_nanos(deadline.saturating_sub(monotonic_ns()).max(1));
            let stream = self.stream.as_mut().expect("connected stream");
            let operation = async {
                write_frame(
                    &mut *stream,
                    &Request::Propose {
                        group_id: self.group_id,
                        payload: payload.clone(),
                    },
                )
                .await?;
                read_frame::<_, Response>(&mut *stream)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("proposal connection closed"))
            };
            match timeout(remaining, operation).await {
                Ok(Ok(Response::Proposal(response))) if response == payload => return Ok(()),
                Ok(Ok(Response::Error(_))) | Ok(Ok(_)) | Ok(Err(_)) => {
                    observed_error = true;
                    self.stream = None;
                    if let Some(leader) = resolve_leader(&self.nodes, self.group_id, deadline).await
                    {
                        self.leader = leader;
                    }
                }
                Err(_) => {
                    self.stream = None;
                    return Err(AttemptFailure::Timeout);
                }
            }
        }
    }

    async fn connect(&mut self, deadline: u64) -> Result<(), ()> {
        let remaining = Duration::from_nanos(deadline.saturating_sub(monotonic_ns()).max(1));
        match timeout(remaining, TcpStream::connect(self.nodes[self.leader])).await {
            Ok(Ok(stream)) => {
                self.stream = Some(stream);
                Ok(())
            }
            _ => {
                if let Some(leader) = resolve_leader(&self.nodes, self.group_id, deadline).await {
                    self.leader = leader;
                }
                Err(())
            }
        }
    }
}

async fn resolve_leader(nodes: &[SocketAddr; 3], group_id: u64, deadline: u64) -> Option<usize> {
    for address in nodes {
        let remaining = Duration::from_nanos(deadline.saturating_sub(monotonic_ns()).max(1))
            .min(Duration::from_millis(200));
        let response = timeout(remaining, call(*address, Request::State { group_id })).await;
        if let Ok(Ok(Response::State(state))) = response
            && (1..=3).contains(&state.leader_id)
        {
            return Some((state.leader_id - 1) as usize);
        }
    }
    None
}

fn payload(origin: u64, client: u64, group: u64, sequence: u64) -> Vec<u8> {
    let mut result = vec![0x5a; 1024];
    result[..8].copy_from_slice(&origin.to_be_bytes());
    result[8..16].copy_from_slice(&client.to_be_bytes());
    result[16..24].copy_from_slice(&group.to_be_bytes());
    result[24..32].copy_from_slice(&sequence.to_be_bytes());
    let mut state =
        origin ^ client.rotate_left(17) ^ group.rotate_left(33) ^ sequence.rotate_left(49) ^ 0x600;
    for byte in &mut result[32..] {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ReplicaState;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::net::TcpListener;

    async fn fake_node(
        node_id: u64,
        leader_id: u64,
        committed: Arc<AtomicU64>,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let committed = Arc::clone(&committed);
                tokio::spawn(async move {
                    loop {
                        let Ok(Some(request)) = read_frame::<_, Request>(&mut stream).await else {
                            break;
                        };
                        let response = match request {
                            Request::State { .. } => Response::State(ReplicaState {
                                node_id,
                                voters: vec![1, 2, 3],
                                leader_id,
                                last_applied: 1,
                                committed_digest: "00".repeat(32),
                            }),
                            Request::Propose { payload, .. } if node_id == leader_id => {
                                committed.fetch_add(1, Ordering::Relaxed);
                                Response::Proposal(payload)
                            }
                            Request::Propose { .. } => {
                                Response::Error(format!("forward to leader {leader_id}"))
                            }
                            _ => Response::Error("unsupported fake request".to_owned()),
                        };
                        if write_frame(&mut stream, &response).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        (address, task)
    }

    #[tokio::test]
    async fn proposal_re_resolves_the_current_leader() {
        let committed = Arc::new(AtomicU64::new(0));
        let (node1, task1) = fake_node(1, 2, Arc::clone(&committed)).await;
        let (node2, task2) = fake_node(2, 2, Arc::clone(&committed)).await;
        let (node3, task3) = fake_node(3, 2, Arc::clone(&committed)).await;
        let mut session = ClientSession::new([node1, node2, node3], 7);
        let value = payload(1, 2, 7, 11);

        assert_eq!(
            session.propose(value, monotonic_ns() + 2_000_000_000).await,
            Ok(())
        );
        assert_eq!(session.leader, 1);
        assert_eq!(committed.load(Ordering::Relaxed), 1);

        task1.abort();
        task2.abort();
        task3.abort();
    }

    #[test]
    fn proposal_payload_includes_the_sequence() {
        assert_ne!(payload(1, 2, 3, 4), payload(1, 2, 3, 5));
    }

    #[test]
    fn one_client_payload_working_set_is_fixed() {
        let payloads: Vec<Vec<u8>> = (0..100).map(|client| payload(1, client, 1, 0)).collect();
        assert_eq!(
            payloads.iter().map(Vec::capacity).sum::<usize>(),
            100 * 1024
        );
    }

    #[test]
    fn rss_phase_boundaries_are_explicit_and_non_overlapping() {
        assert_eq!(phase_for(9, 10, 20, 30), LoadgenPhase::Warmup);
        assert_eq!(phase_for(10, 10, 20, 30), LoadgenPhase::Measure);
        assert_eq!(phase_for(19, 10, 20, 30), LoadgenPhase::Measure);
        assert_eq!(phase_for(20, 10, 20, 30), LoadgenPhase::Drain);
    }

    #[test]
    fn rss_probe_returns_a_byte_count() {
        assert!(current_rss_bytes() > 0);
    }
}
