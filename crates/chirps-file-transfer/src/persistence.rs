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

const PROGRESS_RECORD_BYTES: usize = 8;

/// Persists transfer sessions on disk for resume support.
#[derive(Debug)]
pub struct SessionPersistence {
    dir: PathBuf,
    retention: Duration,
    max_sessions: usize,
    guard: Mutex<()>,
}

impl SessionPersistence {
    /// Creates a persistence store from configuration.
    ///
    /// # Panics
    /// This method does not panic.
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

    /// Saves a session to disk using an atomic write.
    ///
    /// # Errors
    /// Returns `FileTransferError::Serialization` when serialization fails, or
    /// `FileTransferError::Io` when directory creation, file writes, or renames fail.
    ///
    /// # Panics
    /// This method does not panic.
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
        match fs::remove_file(self.progress_path(&session.id)).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(FileTransferError::Io(error)),
        }
        Ok(())
    }

    /// Appends one verified chunk to the durable resume journal.
    ///
    /// The fixed-size record stores the index and its bitwise complement so a
    /// partial or corrupt trailing record is ignored during recovery. Calling
    /// this method repeatedly for the same index is idempotent when replayed.
    ///
    /// # Errors
    /// Returns `FileTransferError::Io` if the journal cannot be created,
    /// written, or flushed.
    pub async fn checkpoint_chunk(
        &self,
        session_id: TransferSessionId,
        index: u32,
    ) -> Result<(), FileTransferError> {
        let _guard = self.guard.lock().await;
        fs::create_dir_all(&self.dir).await?;
        let mut record = [0u8; PROGRESS_RECORD_BYTES];
        record[..4].copy_from_slice(&index.to_le_bytes());
        record[4..].copy_from_slice(&(!index).to_le_bytes());
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.progress_path(&session_id))
            .await?;
        file.write_all(&record).await?;
        file.flush().await?;
        Ok(())
    }

    /// Loads a session from disk.
    ///
    /// # Errors
    /// Returns `FileTransferError::SessionNotFound` when no session exists,
    /// `FileTransferError::Serialization` when deserialization fails, or
    /// `FileTransferError::Io` when reading from disk fails.
    ///
    /// # Panics
    /// This method does not panic.
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
        let mut session: TransferSession = bincode::deserialize(&bytes)
            .map_err(|e| FileTransferError::Serialization(e.to_string()))?;
        match fs::read(self.progress_path(&id)).await {
            Ok(progress) => {
                for record in progress.chunks_exact(PROGRESS_RECORD_BYTES) {
                    let index =
                        u32::from_le_bytes(record[..4].try_into().expect("four index bytes"));
                    let complement =
                        u32::from_le_bytes(record[4..].try_into().expect("four complement bytes"));
                    if complement != !index {
                        break;
                    }
                    if index < session.chunk_tracker.total_chunks {
                        session.chunk_tracker.mark_completed(index);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(FileTransferError::Io(error)),
        }
        Ok(session)
    }

    /// Removes a persisted session if it exists.
    ///
    /// # Errors
    /// Returns `FileTransferError::Io` if removing the session file fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn remove(&self, id: TransferSessionId) -> Result<(), FileTransferError> {
        let _guard = self.guard.lock().await;
        let path = self.session_path(&id);
        let result = match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(FileTransferError::Io(err)),
        };
        if result.is_ok() {
            match fs::remove_file(self.progress_path(&id)).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(FileTransferError::Io(error)),
            }
        }
        result
    }

    /// Runs garbage collection for expired sessions and enforces `max_sessions`.
    ///
    /// # Errors
    /// Returns `FileTransferError::Io` if creating or reading the sessions directory fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn gc(&self) -> Result<(), FileTransferError> {
        let _guard = self.guard.lock().await;
        fs::create_dir_all(&self.dir).await?;
        let mut entries = fs::read_dir(&self.dir).await?;
        let mut sessions = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !path.is_file() || path.extension().and_then(|value| value.to_str()) != Some("bin") {
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
                let _ = fs::remove_file(path.with_extension("progress")).await;
            }
        }

        let mut remaining: Vec<_> = sessions
            .into_iter()
            .filter(|(path, _)| path.exists())
            .collect();
        remaining.sort_by_key(|(_, session)| Reverse(session.updated_at));

        if remaining.len() > self.max_sessions {
            for (path, _) in remaining.drain(self.max_sessions..) {
                let _ = fs::remove_file(&path).await;
                let _ = fs::remove_file(path.with_extension("progress")).await;
            }
        }

        Ok(())
    }

    /// Returns the path for a session file.
    fn session_path(&self, id: &TransferSessionId) -> PathBuf {
        self.dir.join(format!("session_{}.bin", id))
    }

    /// Returns the temporary path used for atomic session writes.
    fn temp_path(&self, id: &TransferSessionId) -> PathBuf {
        self.dir.join(format!("session_{}.tmp", id))
    }

    fn progress_path(&self, id: &TransferSessionId) -> PathBuf {
        self.dir.join(format!("session_{}.progress", id))
    }
}

fn is_expired(now: SystemTime, updated_at: SystemTime, retention: Duration) -> bool {
    match now.duration_since(updated_at) {
        Ok(elapsed) => elapsed > retention,
        Err(_) => false,
    }
}
