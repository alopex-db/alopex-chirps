mod durable_bootstrap_security;

use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    ADMIN_PASSWORD, ADMIN_USERNAME, EvidenceSink, ROOT_PASSWORD, ROOT_USERNAME, RUNTIME_PASSWORD,
    RUNTIME_USERNAME, VerifiedArtifact,
};
use durable_bootstrap_security::{
    DiagnosticServer, assert_invalid_migration_rejected, assert_missing_credential_rejected,
    assert_snapshot_secret_free,
};
use iggy::http::http_client::HttpClient;
use iggy::prelude::{
    AutoLogin, Client, IggyDuration, IggyError, SnapshotCompression, SystemClient,
    SystemSnapshotType, TcpClient, UserClient,
};
use std::process::Command;
use std::str::FromStr;

const CHILD_ENVIRONMENT: &str = "CHIRPS_DIAGNOSTICS_CASE";
const SNAPSHOT_CASES: &[(&str, SystemSnapshotType, &str)] = &[
    (
        "filesystem",
        SystemSnapshotType::FilesystemOverview,
        "filesystem_overview.json",
    ),
    (
        "processes",
        SystemSnapshotType::ProcessList,
        "process_list.json",
    ),
    (
        "resources",
        SystemSnapshotType::ResourceUsage,
        "resource_usage.json",
    ),
    ("test", SystemSnapshotType::Test, "test.json"),
    ("logs", SystemSnapshotType::ServerLogs, "server_logs.json"),
    (
        "config",
        SystemSnapshotType::ServerConfig,
        "server_config.json",
    ),
    ("all", SystemSnapshotType::All, "all"),
];
const ALL_ENTRIES: &[&str] = &[
    "filesystem_overview.json",
    "process_list.json",
    "resource_usage.json",
    "server_logs.json",
    "server_config.json",
];

#[derive(Clone, Copy)]
enum Transport {
    Binary,
    Http,
}

impl Transport {
    fn label(self) -> &'static str {
        match self {
            Self::Binary => "binary",
            Self::Http => "http",
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested compatible server"]
async fn diagnostics_are_authorized_and_secret_free_in_fresh_processes() -> Result<()> {
    let Some(case) = std::env::var_os(CHILD_ENVIRONMENT) else {
        return dispatch_fresh_processes();
    };
    run_child(&case.to_string_lossy()).await
}

fn dispatch_fresh_processes() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_diagnostics")?;
    for case in ["artifact-kind-mismatch", "artifact-digest-mismatch"] {
        run_process(case, Some(&artifact))?;
        evidence.record(case, "rejected")?;
    }

    match artifact.lane.as_str() {
        "production" => {
            for index in 0..6 {
                let case = format!("bootstrap-missing-{index}");
                run_process(&case, None)?;
                evidence.record(&case, "rejected-secret-free")?;
            }
            run_process("migration-runtime", None)?;
            evidence.record("runtime-migration", "rejected-secret-free")?;
            for transport in [Transport::Binary, Transport::Http] {
                for (label, _, _) in SNAPSHOT_CASES {
                    let case = format!("matrix-{}-{label}", transport.label());
                    run_process(&case, None)?;
                }
            }
        }
        "fault" => {
            for transport in [Transport::Binary, Transport::Http] {
                run_process(&format!("collector-{}", transport.label()), None)?;
            }
        }
        _ => anyhow::bail!("unrecognized diagnostics lane"),
    }
    Ok(())
}

fn run_process(case: &str, artifact: Option<&VerifiedArtifact>) -> Result<()> {
    let executable = std::env::current_exe()?;
    let mut command = Command::new(executable);
    command
        .args([
            "--exact",
            "diagnostics_are_authorized_and_secret_free_in_fresh_processes",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD_ENVIRONMENT, case);
    if let Some(artifact) = artifact {
        if case == "artifact-kind-mismatch" {
            let wrong_kind = if artifact.kind == "production" {
                "publish-disabled-test"
            } else {
                "production"
            };
            command.env("CHIRPS_ARTIFACT_KIND", wrong_kind);
        } else {
            command.env("CHIRPS_SERVER_SHA256", "0".repeat(64));
        }
    }
    let status = command
        .status()
        .with_context(|| format!("launch fresh diagnostics process for {case}"))?;
    ensure!(status.success(), "fresh diagnostics process failed: {case}");
    Ok(())
}

async fn run_child(case: &str) -> Result<()> {
    if matches!(case, "artifact-kind-mismatch" | "artifact-digest-mismatch") {
        ensure!(
            VerifiedArtifact::from_runner_environment().is_err(),
            "mismatched artifact identity was accepted"
        );
        return Ok(());
    }

    let artifact = VerifiedArtifact::from_runner_environment()?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_diagnostics")?;
    if let Some(index) = case.strip_prefix("bootstrap-missing-") {
        let index = index.parse().context("invalid bootstrap case")?;
        assert_missing_credential_rejected(&artifact, index)?;
        return Ok(());
    }
    if case == "migration-runtime" {
        assert_invalid_migration_rejected(&artifact)?;
        return Ok(());
    }
    if let Some(label) = case.strip_prefix("matrix-binary-") {
        run_matrix_cell(&artifact, Transport::Binary, label).await?;
        evidence.record(case, "authorized-admin-denied-runtime-restart-secret-free")?;
        return Ok(());
    }
    if let Some(label) = case.strip_prefix("matrix-http-") {
        run_matrix_cell(&artifact, Transport::Http, label).await?;
        evidence.record(case, "authorized-admin-denied-runtime-restart-secret-free")?;
        return Ok(());
    }
    if case == "collector-binary" {
        run_collector_failure(&artifact, Transport::Binary).await?;
        evidence.record(case, "all-or-error")?;
        return Ok(());
    }
    if case == "collector-http" {
        run_collector_failure(&artifact, Transport::Http).await?;
        evidence.record(case, "all-or-error")?;
        return Ok(());
    }
    anyhow::bail!("unknown diagnostics child case")
}

async fn run_matrix_cell(
    artifact: &VerifiedArtifact,
    transport: Transport,
    label: &str,
) -> Result<()> {
    ensure!(
        artifact.lane == "production",
        "matrix requires production artifact"
    );
    let (_, snapshot_type, expected_entry) = SNAPSHOT_CASES
        .iter()
        .find(|(candidate, _, _)| *candidate == label)
        .context("unknown snapshot matrix type")?;
    let requested = vec![snapshot_type.clone()];
    let mut server = DiagnosticServer::new(artifact.clone(), true)?;
    let migration = server.write_migration(1)?;
    server.start(Some(&migration))?;
    assert_runtime_denied(&server, transport, requested.clone()).await?;
    assert_root_denied(&server, transport, requested.clone()).await?;
    let first = admin_snapshot(&server, transport, requested.clone()).await?;
    assert_complete_bundle(&first, expected_entry)?;
    assert_snapshot_secret_free(&first)?;
    server.stop_and_assert_secret_free()?;

    server.start(None)?;
    assert_runtime_denied(&server, transport, requested.clone()).await?;
    assert_root_denied(&server, transport, requested.clone()).await?;
    let restarted = admin_snapshot(&server, transport, requested).await?;
    assert_complete_bundle(&restarted, expected_entry)?;
    assert_snapshot_secret_free(&restarted)?;
    server.stop_and_assert_secret_free()
}

async fn run_collector_failure(artifact: &VerifiedArtifact, transport: Transport) -> Result<()> {
    ensure!(
        artifact.lane == "fault",
        "collector case requires fault artifact"
    );
    let mut server = DiagnosticServer::new(artifact.clone(), false)?;
    let migration = server.write_migration(1)?;
    server.start(Some(&migration))?;
    let requested = vec![SystemSnapshotType::Test, SystemSnapshotType::ServerLogs];
    assert_runtime_denied(&server, transport, requested.clone()).await?;
    let result = snapshot(
        &server,
        transport,
        ADMIN_USERNAME,
        ADMIN_PASSWORD,
        requested,
    )
    .await;
    ensure!(
        result.is_err(),
        "collector-stage failure returned a partial bundle"
    );
    server.stop_and_assert_secret_free()?;

    server.start(None)?;
    let restarted = snapshot(
        &server,
        transport,
        ADMIN_USERNAME,
        ADMIN_PASSWORD,
        vec![SystemSnapshotType::Test, SystemSnapshotType::ServerLogs],
    )
    .await;
    ensure!(
        restarted.is_err(),
        "collector-stage failure returned a partial bundle after restart"
    );
    server.stop_and_assert_secret_free()
}

async fn assert_runtime_denied(
    server: &DiagnosticServer,
    transport: Transport,
    requested: Vec<SystemSnapshotType>,
) -> Result<()> {
    let result = snapshot(
        server,
        transport,
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        requested,
    )
    .await;
    ensure!(
        matches!(result, Err(IggyError::Unauthorized)),
        "runtime diagnostics request was not denied"
    );
    Ok(())
}

async fn assert_root_denied(
    server: &DiagnosticServer,
    transport: Transport,
    requested: Vec<SystemSnapshotType>,
) -> Result<()> {
    let result = snapshot(server, transport, ROOT_USERNAME, ROOT_PASSWORD, requested).await;
    ensure!(
        matches!(result, Err(IggyError::Unauthorized)),
        "ungranted root diagnostics request was not denied"
    );
    Ok(())
}

async fn admin_snapshot(
    server: &DiagnosticServer,
    transport: Transport,
    requested: Vec<SystemSnapshotType>,
) -> Result<Vec<u8>> {
    snapshot(server, transport, ADMIN_USERNAME, ADMIN_PASSWORD, requested)
        .await
        .map_err(|_| anyhow::anyhow!("authorized diagnostics request failed"))
}

async fn snapshot(
    server: &DiagnosticServer,
    transport: Transport,
    username: &str,
    password: &str,
    requested: Vec<SystemSnapshotType>,
) -> std::result::Result<Vec<u8>, IggyError> {
    match transport {
        Transport::Binary => {
            let client = TcpClient::new(
                &server.tcp_address(),
                AutoLogin::Disabled,
                IggyDuration::from_str("1s").expect("static duration is valid"),
            )?;
            client.connect().await?;
            client.login_user(username, password).await?;
            let result = client
                .snapshot(SnapshotCompression::Stored, requested)
                .await
                .map(|snapshot| snapshot.0);
            let _ = client.disconnect().await;
            result
        }
        Transport::Http => {
            let client = HttpClient::new(&server.http_url())?;
            client.connect().await?;
            client.login_user(username, password).await?;
            let result = client
                .snapshot(SnapshotCompression::Stored, requested)
                .await
                .map(|snapshot| snapshot.0);
            let _ = client.disconnect().await;
            result
        }
    }
}

fn assert_complete_bundle(snapshot: &[u8], expected_entry: &str) -> Result<()> {
    ensure!(
        snapshot.starts_with(b"PK"),
        "diagnostic response was not a ZIP bundle"
    );
    if expected_entry == "all" {
        for entry in ALL_ENTRIES {
            ensure!(
                contains(snapshot, entry.as_bytes()),
                "all bundle was incomplete"
            );
        }
        ensure!(
            !contains(snapshot, b"test.json"),
            "all bundle included the development-only projection"
        );
    } else {
        ensure!(
            contains(snapshot, expected_entry.as_bytes()),
            "diagnostic bundle omitted its requested projection"
        );
        for entry in ALL_ENTRIES.iter().copied().chain(["test.json"]) {
            if entry != expected_entry {
                ensure!(
                    !contains(snapshot, entry.as_bytes()),
                    "diagnostic bundle included an unrequested projection"
                );
            }
        }
    }
    Ok(())
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
