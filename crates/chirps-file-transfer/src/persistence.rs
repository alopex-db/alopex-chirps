use crate::TransferSessionId;
use crate::config::FileTransferConfig;
use crate::error::FileTransferError;
use crate::session::TransferSession;
use std::cmp::Reverse;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct SessionPersistence {
    dir: PathBuf,
    retention: Duration,
    max_sessions: usize,
    guard: Mutex<()>,
}

impl SessionPersistence {
    pub fn new(config: &FileTransferConfig) -> Self {
        let dir = config
            .session_dir
            .clone()
            .unwrap_or_else(|| config.base_path.join("sessions"));
        SessionPersistence {
            dir,
            retention: config.session_retention,
            max_sessions: config.max_sessions,
            guard: Mutex::new(()),
        }
    }

    pub async fn save(&self, session: &TransferSession) -> Result<(), FileTransferError> {
        let _guard = self.guard.lock().await;
        fs::create_dir_all(&self.dir).await?;
        let bytes = bincode::serialize(session)
            .map_err(|e| FileTransferError::Serialization(e.to_string()))?;

        let path = self.session_path(&session.id);
        let tmp_path = self.temp_path(&session.id);
        let mut file = fs::File::create(&tmp_path).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        fs::rename(&tmp_path, &path).await?;
        Ok(())
    }

    pub async fn load(&self, id: TransferSessionId) -> Result<TransferSession, FileTransferError> {
        let _guard = self.guard.lock().await;
        let path = self.session_path(&id);
        let bytes = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(FileTransferError::SessionNotFound(id));
            }
            Err(err) => return Err(FileTransferError::Io(err)),
        };
        let session = bincode::deserialize(&bytes)
            .map_err(|e| FileTransferError::Serialization(e.to_string()))?;
        Ok(session)
    }

    pub async fn remove(&self, id: TransferSessionId) -> Result<(), FileTransferError> {
        let _guard = self.guard.lock().await;
        let path = self.session_path(&id);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(FileTransferError::Io(err)),
        }
    }

    pub async fn gc(&self) -> Result<(), FileTransferError> {
        let _guard = self.guard.lock().await;
        fs::create_dir_all(&self.dir).await?;
        let mut entries = fs::read_dir(&self.dir).await?;
        let mut sessions = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let bytes = match fs::read(&path).await {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            let session: TransferSession = match bincode::deserialize(&bytes) {
                Ok(session) => session,
                Err(_) => continue,
            };
            sessions.push((path, session));
        }

        let now = SystemTime::now();
        for (path, session) in &sessions {
            if is_expired(now, session.updated_at, self.retention) {
                let _ = fs::remove_file(path).await;
            }
        }

        let mut remaining: Vec<_> = sessions
            .into_iter()
            .filter(|(path, _)| path.exists())
            .collect();
        remaining.sort_by_key(|(_, session)| Reverse(session.updated_at));

        if remaining.len() > self.max_sessions {
            for (path, _) in remaining.drain(self.max_sessions..) {
                let _ = fs::remove_file(path).await;
            }
        }

        Ok(())
    }

    fn session_path(&self, id: &TransferSessionId) -> PathBuf {
        self.dir.join(format!("session_{}.bin", id))
    }

    fn temp_path(&self, id: &TransferSessionId) -> PathBuf {
        self.dir.join(format!("session_{}.tmp", id))
    }
}

fn is_expired(now: SystemTime, updated_at: SystemTime, retention: Duration) -> bool {
    match now.duration_since(updated_at) {
        Ok(elapsed) => elapsed > retention,
        Err(_) => false,
    }
}
