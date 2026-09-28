use anyhow::{Context, Result, anyhow, ensure};
use chirps_e2e::v07::{
    ADMIN_PASSWORD, ADMIN_USERNAME, ROOT_PASSWORD, ROOT_USERNAME, RUNTIME_PASSWORD,
    RUNTIME_USERNAME, VerifiedArtifact,
};
use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const CREDENTIAL_ENVIRONMENT: &[(&str, &str)] = &[
    ("IGGY_ROOT_USERNAME", ROOT_USERNAME),
    ("IGGY_ROOT_PASSWORD", ROOT_PASSWORD),
    ("IGGY_ADMIN_USERNAME", ADMIN_USERNAME),
    ("IGGY_ADMIN_PASSWORD", ADMIN_PASSWORD),
    ("IGGY_RUNTIME_USERNAME", RUNTIME_USERNAME),
    ("IGGY_RUNTIME_PASSWORD", RUNTIME_PASSWORD),
];
const JWT_SECRET_CANARY: &str = "jwt-secret-canary-v07-diagnostics-32-bytes";

pub(crate) struct DiagnosticServer {
    artifact: VerifiedArtifact,
    root: TempDir,
    tcp_address: SocketAddr,
    http_address: SocketAddr,
    child: Option<Child>,
    captured_output: Vec<u8>,
}

impl DiagnosticServer {
    pub(crate) fn new(artifact: VerifiedArtifact, working_logs: bool) -> Result<Self> {
        let root = tempfile::tempdir()?;
        fs::create_dir_all(root.path().join("state"))?;
        if working_logs {
            fs::create_dir_all(root.path().join("state/logs"))?;
        } else {
            fs::write(root.path().join("state/logs"), b"collector-failure")?;
        }
        Ok(Self {
            artifact,
            root,
            tcp_address: unused_address()?,
            http_address: unused_address()?,
            child: None,
            captured_output: Vec::new(),
        })
    }

    pub(crate) fn tcp_address(&self) -> String {
        self.tcp_address.to_string()
    }

    pub(crate) fn http_url(&self) -> String {
        format!("http://{}", self.http_address)
    }

    pub(crate) fn write_migration(&self, principal: u32) -> Result<PathBuf> {
        let path = self.root.path().join("diagnostics-migration.bin");
        let mut command = Vec::with_capacity(15);
        command.extend_from_slice(&1_u16.to_le_bytes());
        command.push(1);
        command.extend_from_slice(&principal.to_le_bytes());
        command.extend_from_slice(&1_u64.to_le_bytes());
        fs::write(&path, command)?;
        Ok(path)
    }

    pub(crate) fn start(&mut self, migration: Option<&Path>) -> Result<()> {
        ensure!(self.child.is_none(), "diagnostic server is already running");
        let mut command = self.base_command(None);
        if let Some(path) = migration {
            command.env("IGGY_CHIRPS_DIAGNOSTICS_MIGRATION_FILE", path);
        }
        self.child = Some(command.spawn().context("spawn diagnostic server")?);
        self.wait_ready()
    }

    pub(crate) fn stop_and_assert_secret_free(&mut self) -> Result<()> {
        let mut child = self
            .child
            .take()
            .context("diagnostic server is not running")?;
        let _ = child.kill();
        let output = child
            .wait_with_output()
            .context("collect diagnostic server output")?;
        self.captured_output.extend_from_slice(&output.stdout);
        self.captured_output.extend_from_slice(&output.stderr);
        assert_secret_free(&self.captured_output)
    }

    fn base_command(&self, omitted_environment: Option<&str>) -> Command {
        let mut command = Command::new(&self.artifact.binary);
        command
            .env_clear()
            .current_dir(self.root.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("IGGY_SYSTEM_PATH", self.root.path().join("state"))
            .env("IGGY_TCP_ENABLED", "true")
            .env("IGGY_TCP_ADDRESS", self.tcp_address.to_string())
            .env("IGGY_TCP_TLS_ENABLED", "false")
            .env("IGGY_HTTP_ENABLED", "true")
            .env("IGGY_HTTP_ADDRESS", self.http_address.to_string())
            .env("IGGY_HTTP_JWT_ENCODING_SECRET", JWT_SECRET_CANARY)
            .env("IGGY_HTTP_JWT_DECODING_SECRET", JWT_SECRET_CANARY)
            .env("IGGY_QUIC_ENABLED", "false")
            .env("IGGY_WEBSOCKET_ENABLED", "false")
            .env("IGGY_TELEMETRY_ENABLED", "false")
            .env("IGGY_SYSTEM_LOGGING_LEVEL", "error")
            .env("IGGY_SYSTEM_LOGGING_FILE_ENABLED", "false")
            .env("IGGY_SYSTEM_MEMORY_POOL_ENABLED", "false")
            .env("IGGY_SYSTEM_STATE_ENFORCE_FSYNC", "true")
            .env("IGGY_CHIRPS_PRODUCTION_PROFILE", "true");
        for (name, value) in CREDENTIAL_ENVIRONMENT {
            if Some(*name) != omitted_environment {
                command.env(name, value);
            }
        }
        command
    }

    fn wait_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .context("diagnostic server was not spawned")?
                .try_wait()?
            {
                return Err(anyhow!(
                    "diagnostic server exited before readiness: {status}"
                ));
            }
            if TcpStream::connect_timeout(&self.tcp_address, Duration::from_millis(100)).is_ok()
                && TcpStream::connect_timeout(&self.http_address, Duration::from_millis(100))
                    .is_ok()
            {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "diagnostic server readiness timed out"
            );
            thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for DiagnosticServer {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub(crate) fn assert_missing_credential_rejected(
    artifact: &VerifiedArtifact,
    missing_index: usize,
) -> Result<()> {
    let fixture = DiagnosticServer::new(artifact.clone(), false)?;
    let (missing_name, _) = CREDENTIAL_ENVIRONMENT
        .get(missing_index)
        .context("unknown credential omission")?;
    let mut child = fixture
        .base_command(Some(missing_name))
        .spawn()
        .context("spawn credential-negative server")?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child.wait_with_output()?;
            assert_secret_free_pair(&output.stdout, &output.stderr)?;
            return Err(anyhow!("incomplete production credentials were accepted"));
        }
        thread::sleep(Duration::from_millis(20));
    };
    let output = child.wait_with_output()?;
    assert_secret_free_pair(&output.stdout, &output.stderr)?;
    ensure!(
        !status.success(),
        "incomplete production credentials were accepted"
    );
    Ok(())
}

pub(crate) fn assert_invalid_migration_rejected(artifact: &VerifiedArtifact) -> Result<()> {
    let fixture = DiagnosticServer::new(artifact.clone(), false)?;
    let migration = fixture.write_migration(2)?;
    let mut command = fixture.base_command(None);
    command.env("IGGY_CHIRPS_DIAGNOSTICS_MIGRATION_FILE", migration);
    let mut child = command.spawn().context("spawn migration-negative server")?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child.wait_with_output()?;
            assert_secret_free_pair(&output.stdout, &output.stderr)?;
            return Err(anyhow!("runtime diagnostics migration was accepted"));
        }
        thread::sleep(Duration::from_millis(20));
    };
    let output = child.wait_with_output()?;
    assert_secret_free_pair(&output.stdout, &output.stderr)?;
    ensure!(
        !status.success(),
        "runtime diagnostics migration was accepted"
    );
    Ok(())
}

pub(crate) fn assert_snapshot_secret_free(snapshot: &[u8]) -> Result<()> {
    assert_secret_free(snapshot)?;
    for forbidden in [
        b"password".as_slice(),
        b"access_token".as_slice(),
        b"private_key".as_slice(),
        b"permissions".as_slice(),
    ] {
        ensure!(
            !contains(snapshot, forbidden),
            "diagnostic snapshot contained a forbidden security field"
        );
    }
    Ok(())
}

fn assert_secret_free_pair(stdout: &[u8], stderr: &[u8]) -> Result<()> {
    assert_secret_free(stdout)?;
    assert_secret_free(stderr)
}

fn assert_secret_free(bytes: &[u8]) -> Result<()> {
    for (_, canary) in CREDENTIAL_ENVIRONMENT {
        ensure!(
            !contains(bytes, canary.as_bytes()),
            "server output contained a credential canary"
        );
    }
    ensure!(
        !contains(bytes, JWT_SECRET_CANARY.as_bytes()),
        "server output contained a secret canary"
    );
    Ok(())
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn unused_address() -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}
