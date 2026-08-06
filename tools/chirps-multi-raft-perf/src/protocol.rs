use crate::schema::ReplicaState;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_CONTROL_FRAME: usize = 2 * 1024 * 1024;

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    Health,
    CreateSeed { group_id: u64 },
    CreateUninitialized { group_id: u64 },
    AddLearner { group_id: u64, node_id: u64 },
    Promote { group_id: u64, voters: Vec<u64> },
    Propose { group_id: u64, payload: Vec<u8> },
    State { group_id: u64 },
    Probe { target: u64, count: u64 },
    Shutdown,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    Ok,
    Proposal(Vec<u8>),
    State(ReplicaState),
    Probe { samples_us: Vec<u64> },
    Error(String),
}

pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = bincode::serialize(value)?;
    anyhow::ensure!(bytes.len() <= MAX_CONTROL_FRAME, "control frame too large");
    writer.write_u32(bytes.len() as u32).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_frame<R, T>(reader: &mut R) -> anyhow::Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let length = match reader.read_u32().await {
        Ok(length) => length as usize,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(length <= MAX_CONTROL_FRAME, "control frame too large");
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await?;
    Ok(Some(bincode::deserialize(&bytes)?))
}
