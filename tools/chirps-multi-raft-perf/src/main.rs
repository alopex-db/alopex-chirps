use chirps_multi_raft_perf::loadgen::{self, LoadgenArgs};
use chirps_multi_raft_perf::node::{self, NodeArgs};
use chirps_multi_raft_perf::protocol::{Request, Response, read_frame, write_frame};
use chirps_multi_raft_perf::schema::{ArtifactInput, Mode, SampleObservation};
use chirps_multi_raft_perf::summary::summarize;
use chirps_multi_raft_perf::{assemble_artifact, verify_artifact};
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::net::TcpStream;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    anyhow::ensure!(!args.is_empty(), usage());
    match args[0].as_str() {
        "node" => node::run(parse_node(&args[1..])?).await,
        "loadgen" => loadgen::run(parse_loadgen(&args[1..])?).await,
        "ctl" => ctl(&args[1..]).await,
        "bootstrap" => bootstrap(&args[1..]).await,
        "probe-all" => probe_all(&args[1..]).await,
        "collect-membership" => collect_membership(&args[1..]).await,
        "monotonic" => {
            println!("{}", node::monotonic_ns());
            Ok(())
        }
        "raw-set-digest" => raw_set_digest(&args[1..]),
        "assemble" => assemble(&args[1..]),
        "summarize-sample" => summarize_sample(&args[1..]),
        "verify" => verify(&args[1..]),
        _ => anyhow::bail!(usage()),
    }
}

fn usage() -> &'static str {
    "usage: chirps-multi-raft-perf node|loadgen|ctl|assemble|verify [options]"
}

fn parse_node(args: &[String]) -> anyhow::Result<NodeArgs> {
    Ok(NodeArgs {
        node_id: required(args, "--node-id")?.parse()?,
        raft_bind: required(args, "--raft-bind")?.parse()?,
        seeds: required(args, "--seeds")?
            .split(',')
            .filter(|value| !value.is_empty())
            .map(str::parse)
            .collect::<Result<_, _>>()?,
        control_bind: required(args, "--control-bind")?.parse()?,
        storage_root: required(args, "--storage-root")?.into(),
        certificate: required(args, "--cert")?.into(),
        private_key: required(args, "--key")?.into(),
        trusted_certificate: required(args, "--trust")?.into(),
        metrics_path: required(args, "--metrics")?.into(),
        send_queue_capacity: option(args, "--send-queue-capacity")
            .unwrap_or("4096")
            .parse()?,
        // Keep automatic snapshot work outside the proposal-throughput lane;
        // snapshot lifecycle is covered by the storage tests separately.
        snapshot_threshold: option(args, "--snapshot-threshold")
            .unwrap_or("10000")
            .parse()?,
        resource_audit: has_flag(args, "--resource-audit"),
        metrics_interval_ms: option(args, "--metrics-interval-ms")
            .unwrap_or("1000")
            .parse()?,
    })
}

fn parse_loadgen(args: &[String]) -> anyhow::Result<LoadgenArgs> {
    Ok(LoadgenArgs {
        origin_node: required(args, "--origin-node")?.parse()?,
        nodes: parse_nodes(required(args, "--nodes")?)?,
        mode: parse_mode(required(args, "--mode")?)?,
        sample_index: required(args, "--sample-index")?.parse()?,
        start_at_ns: required(args, "--start-at-ns")?.parse()?,
        output: required(args, "--output")?.into(),
        warmup_seconds: option(args, "--warmup-seconds").unwrap_or("15").parse()?,
        measure_seconds: option(args, "--measure-seconds").unwrap_or("60").parse()?,
        drain_seconds: option(args, "--drain-seconds").unwrap_or("5").parse()?,
        resource_audit: has_flag(args, "--resource-audit"),
    })
}

async fn ctl(args: &[String]) -> anyhow::Result<()> {
    let address: SocketAddr = required(args, "--address")?.parse()?;
    let request = match required(args, "--operation")? {
        "health" => Request::Health,
        "create-seed" => Request::CreateSeed {
            group_id: required(args, "--group")?.parse()?,
        },
        "create-uninitialized" => Request::CreateUninitialized {
            group_id: required(args, "--group")?.parse()?,
        },
        "add-learner" => Request::AddLearner {
            group_id: required(args, "--group")?.parse()?,
            node_id: required(args, "--target-node")?.parse()?,
        },
        "promote" => Request::Promote {
            group_id: required(args, "--group")?.parse()?,
            voters: required(args, "--voters")?
                .split(',')
                .map(str::parse)
                .collect::<Result<_, _>>()?,
        },
        "state" => Request::State {
            group_id: required(args, "--group")?.parse()?,
        },
        "probe" => Request::Probe {
            target: required(args, "--target-node")?.parse()?,
            count: option(args, "--count").unwrap_or("200").parse()?,
        },
        "shutdown" => Request::Shutdown,
        other => anyhow::bail!("unsupported ctl operation {other}"),
    };
    let mut stream = TcpStream::connect(address).await?;
    write_frame(&mut stream, &request).await?;
    let response = read_frame::<_, Response>(&mut stream)
        .await?
        .ok_or_else(|| anyhow::anyhow!("control connection closed"))?;
    match response {
        Response::Error(error) => anyhow::bail!(error),
        response => println!("{}", serde_json::to_string(&response)?),
    }
    Ok(())
}

async fn bootstrap(args: &[String]) -> anyhow::Result<()> {
    controller::bootstrap(
        &parse_nodes(required(args, "--nodes")?)?,
        required(args, "--groups")?.parse()?,
    )
    .await
}

async fn probe_all(args: &[String]) -> anyhow::Result<()> {
    let values = controller::probe_all(
        &parse_nodes(required(args, "--nodes")?)?,
        option(args, "--count").unwrap_or("200").parse()?,
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&values)?);
    Ok(())
}

async fn collect_membership(args: &[String]) -> anyhow::Result<()> {
    let values = controller::collect_membership(
        &parse_nodes(required(args, "--nodes")?)?,
        required(args, "--groups")?.parse()?,
    )
    .await?;
    let output = PathBuf::from(required(args, "--output")?);
    write_atomic(&output, &serde_json::to_vec_pretty(&values)?)
}

fn assemble(args: &[String]) -> anyhow::Result<()> {
    let input_path = PathBuf::from(required(args, "--input")?);
    let output_path = PathBuf::from(required(args, "--artifact")?);
    let input: ArtifactInput = serde_json::from_slice(&std::fs::read(&input_path)?)?;
    let base = output_path.parent().unwrap_or_else(|| Path::new("."));
    let artifact = assemble_artifact(input, base)?;
    write_atomic(&output_path, &serde_json::to_vec_pretty(&artifact)?)?;
    verify_artifact(&output_path)?;
    println!(
        "artifact={} verdict={:?}",
        output_path.display(),
        artifact.verdict.overall
    );
    Ok(())
}

fn verify(args: &[String]) -> anyhow::Result<()> {
    let artifact = PathBuf::from(required(args, "--artifact")?);
    let verification = verify_artifact(&artifact)?;
    println!(
        "verified={} verdict={:?}",
        artifact.display(),
        verification.computed_verdict.overall
    );
    Ok(())
}

fn summarize_sample(args: &[String]) -> anyhow::Result<()> {
    let input_path = PathBuf::from(required(args, "--input")?);
    let output_path = PathBuf::from(required(args, "--output")?);
    let observation: SampleObservation = serde_json::from_slice(&std::fs::read(&input_path)?)?;
    let base = input_path.parent().unwrap_or_else(|| Path::new("."));
    let summary = summarize(observation, base)?;
    write_atomic(&output_path, &serde_json::to_vec_pretty(&summary)?)
}

fn raw_set_digest(args: &[String]) -> anyhow::Result<()> {
    let path = PathBuf::from(required(args, "--input")?);
    let artifacts: Vec<chirps_multi_raft_perf::schema::RawArtifact> =
        serde_json::from_slice(&std::fs::read(path)?)?;
    let mut hasher = Sha256::new();
    for raw in artifacts {
        hasher.update(raw.path.as_bytes());
        hasher.update([0]);
        hasher.update(raw.sha256.as_bytes());
        hasher.update(b"\n");
    }
    println!("{:x}", hasher.finalize());
    Ok(())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn parse_mode(value: &str) -> anyhow::Result<Mode> {
    match value {
        "multi_raft" => Ok(Mode::MultiRaft),
        "single_group" => Ok(Mode::SingleGroup),
        _ => anyhow::bail!("invalid mode"),
    }
}

fn parse_nodes(value: &str) -> anyhow::Result<[SocketAddr; 3]> {
    let values = value
        .split(',')
        .map(str::parse)
        .collect::<Result<Vec<_>, _>>()?;
    values
        .try_into()
        .map_err(|_| anyhow::anyhow!("--nodes requires exactly three addresses"))
}

fn required<'a>(args: &'a [String], name: &str) -> anyhow::Result<&'a str> {
    option(args, name).ok_or_else(|| anyhow::anyhow!("missing {name}"))
}

fn option<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|arg| arg == name)
}
use chirps_multi_raft_perf::controller;
