use alopex_chirps_wire::frame::Frame;
use alopex_chirps_wire::node_id::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const WORKER_PROTOCOL_VERSION: u32 = 1;
const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ParentMessage {
    Command(Box<WorkerCommand>),
    NetworkAccepted {
        outbound_id: u64,
        accepted: bool,
        reason: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerCommand {
    pub operation_id: u64,
    pub action: WorkerAction,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum WorkerAction {
    CreateGroup {
        group_id: u64,
    },
    Propose {
        group_id: u64,
        command: Vec<u8>,
    },
    EmitRaftVote {
        target: NodeId,
        group_id: u64,
        correlation_id: u64,
        term: u64,
    },
    DeliverFrame {
        network_sequence: u64,
        source: NodeId,
        frame: Box<Frame>,
    },
    TickRaft,
    RemoveGroup {
        group_id: u64,
    },
    Observe,
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerMessage {
    Ready {
        protocol_version: u32,
        node_id: u64,
        pid: u32,
    },
    Response {
        operation_id: u64,
        result: Result<WorkerResult, WorkerFailure>,
    },
    OutboundFrame {
        outbound_id: u64,
        source: NodeId,
        target: NodeId,
        frame: Box<Frame>,
    },
    Fatal {
        failure: WorkerFailure,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum WorkerResult {
    Created {
        group_id: u64,
    },
    Proposed {
        group_id: u64,
        response: Vec<u8>,
    },
    FrameAccepted {
        network_sequence: u64,
        route: RouteObservation,
    },
    Emitted {
        correlation_id: u64,
    },
    Ticked {
        groups: Vec<u64>,
    },
    Removed {
        group_id: u64,
        existed: bool,
    },
    Observation {
        value: WorkerObservation,
    },
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteObservation {
    pub group_id: u64,
    pub correlation_id: u64,
    pub response_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerFailure {
    pub code: String,
    pub detail: String,
    pub group_id: Option<u64>,
    pub network_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerObservation {
    pub node_id: u64,
    pub active_groups: Vec<u64>,
    pub groups: BTreeMap<String, GroupObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupObservation {
    pub group_id: u64,
    pub namespace: String,
    pub accepting: bool,
    pub state_machine_applies: u64,
    pub state_machine_digest: String,
    pub wal_path: String,
    pub snapshot_path: String,
    pub wal_exists: bool,
    pub snapshot_exists: bool,
}

pub async fn write_message<T: Serialize>(
    writer: &mut (impl AsyncWrite + Unpin),
    message: &T,
) -> anyhow::Result<()> {
    let payload = serde_json::to_vec(message)?;
    anyhow::ensure!(
        payload.len() <= MAX_MESSAGE_BYTES,
        "worker message too large"
    );
    writer.write_u32(payload.len() as u32).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_message<T: for<'de> Deserialize<'de>>(
    reader: &mut (impl AsyncRead + Unpin),
) -> anyhow::Result<T> {
    let length = reader.read_u32().await? as usize;
    anyhow::ensure!(length <= MAX_MESSAGE_BYTES, "worker message too large");
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    Ok(serde_json::from_slice(&payload)?)
}
