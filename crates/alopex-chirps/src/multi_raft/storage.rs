use super::{GroupId, MultiRaftError, group_namespace};
use alopex_chirps_raft_storage::snapshot::{
    NoopSnapshotCompletionHook, SnapshotCompletionEvent, SnapshotCompletionHook,
};
use alopex_chirps_raft_storage::traits::{RaftStorage as ChirpsRaftStorage, StateMachine};
use alopex_chirps_raft_storage::types::{
    ChirpsTypeConfig, Entry, LogFlushed, LogId, LogState, OptionalSend, RaftLogReader, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};
use alopex_chirps_raft_storage::wal_storage::{WalRaftStorage, WalStorageConfig};
use async_trait::async_trait;
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

/// Begins construction of storage isolated to a single Raft group.
#[async_trait]
pub trait RaftStorageFactory: Send + Sync + 'static {
    type StateMachine: StateMachine<Command = Vec<u8>, Response = Vec<u8>>;
    type Storage: ChirpsRaftStorage<ChirpsTypeConfig>;

    /// Replaces the durable snapshot completion sink for existing and future storage instances.
    fn set_snapshot_completion_hook(&self, _hook: Arc<dyn SnapshotCompletionHook>) {}

    async fn begin_storage(
        &self,
        group_id: GroupId,
        state_machine: Self::StateMachine,
    ) -> Result<StorageTransaction<Self::Storage>, MultiRaftError>;
}

/// WAL-backed storage factory using canonical per-group subdirectories.
pub struct WalRaftStorageFactory<SM> {
    base_config: WalStorageConfig,
    node_id: u64,
    state_machine: PhantomData<SM>,
    snapshot_completion_router: Arc<SnapshotCompletionRouter>,
}

struct SnapshotCompletionRouter {
    hook: RwLock<Arc<dyn SnapshotCompletionHook>>,
}

impl SnapshotCompletionRouter {
    fn new() -> Self {
        Self {
            hook: RwLock::new(Arc::new(NoopSnapshotCompletionHook)),
        }
    }

    fn set(&self, hook: Arc<dyn SnapshotCompletionHook>) {
        *self
            .hook
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = hook;
    }
}

impl SnapshotCompletionHook for SnapshotCompletionRouter {
    fn completed(&self, event: SnapshotCompletionEvent) {
        self.hook
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .completed(event);
    }
}

impl<SM> WalRaftStorageFactory<SM> {
    pub fn new(base_config: WalStorageConfig, node_id: u64) -> Self {
        Self {
            base_config,
            node_id,
            state_machine: PhantomData,
            snapshot_completion_router: Arc::new(SnapshotCompletionRouter::new()),
        }
    }

    pub fn with_snapshot_completion_hook(self, hook: Arc<dyn SnapshotCompletionHook>) -> Self {
        self.snapshot_completion_router.set(hook);
        self
    }
}

#[async_trait]
impl<SM> RaftStorageFactory for WalRaftStorageFactory<SM>
where
    SM: StateMachine<Command = Vec<u8>, Response = Vec<u8>>,
{
    type StateMachine = SM;
    type Storage = WalRaftStorage<SM>;

    fn set_snapshot_completion_hook(&self, hook: Arc<dyn SnapshotCompletionHook>) {
        self.snapshot_completion_router.set(hook);
    }

    async fn begin_storage(
        &self,
        group_id: GroupId,
        state_machine: SM,
    ) -> Result<StorageTransaction<Self::Storage>, MultiRaftError> {
        let namespace = group_namespace(group_id);
        let mut config = self.base_config.clone();
        config.wal_dir = config.wal_dir.join(&namespace);
        config.snapshot_dir = config.snapshot_dir.join(&namespace);
        let wal_dir = config.wal_dir.clone();
        let snapshot_dir = config.snapshot_dir.clone();

        match WalRaftStorage::new(config, group_id, self.node_id, state_machine) {
            Ok(mut storage) => {
                let hook: Arc<dyn SnapshotCompletionHook> =
                    Arc::clone(&self.snapshot_completion_router) as Arc<dyn SnapshotCompletionHook>;
                storage.set_snapshot_completion_hook(hook);
                Ok(StorageTransaction {
                    group_id,
                    namespace,
                    wal_dir,
                    snapshot_dir,
                    storage: SharedRaftStorage::new(storage),
                })
            }
            Err(error) => {
                let cleanup = remove_group_dirs(&wal_dir, &snapshot_dir).await;
                let reason = match cleanup {
                    Ok(()) => error.to_string(),
                    Err(cleanup_error) => {
                        format!("{error}; partial-state cleanup failed: {cleanup_error}")
                    }
                };
                Err(MultiRaftError::StorageCreation { group_id, reason })
            }
        }
    }
}

/// A not-yet-committed group storage allocation.
///
/// Dropping it preserves the on-disk state for crash recovery. Call `abort()`
/// explicitly when group creation is rolled back.
pub struct StorageTransaction<S> {
    group_id: GroupId,
    namespace: String,
    wal_dir: PathBuf,
    snapshot_dir: PathBuf,
    storage: SharedRaftStorage<S>,
}

impl<S> StorageTransaction<S> {
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    pub fn snapshot_dir(&self) -> &Path {
        &self.snapshot_dir
    }

    pub fn storage(&self) -> SharedRaftStorage<S> {
        self.storage.clone()
    }

    pub fn commit(self) -> SharedRaftStorage<S> {
        self.storage
    }

    pub async fn abort(self) -> Result<(), MultiRaftError> {
        let Self {
            group_id,
            wal_dir,
            snapshot_dir,
            storage,
            ..
        } = self;
        drop(storage);
        remove_group_dirs(&wal_dir, &snapshot_dir)
            .await
            .map_err(|error| MultiRaftError::StorageCreation {
                group_id,
                reason: format!("partial-state cleanup failed: {error}"),
            })
    }
}

async fn remove_group_dirs(wal_dir: &Path, snapshot_dir: &Path) -> std::io::Result<()> {
    remove_dir_if_present(wal_dir).await?;
    if snapshot_dir != wal_dir {
        remove_dir_if_present(snapshot_dir).await?;
    }
    Ok(())
}

async fn remove_dir_if_present(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Cloneable OpenRaft v2 log/state-machine view over the existing Chirps store.
pub struct SharedRaftStorage<S> {
    inner: Arc<Mutex<S>>,
}

impl<S> SharedRaftStorage<S> {
    fn new(storage: S) -> Self {
        Self {
            inner: Arc::new(Mutex::new(storage)),
        }
    }
}

impl<S> Clone for SharedRaftStorage<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> RaftLogReader<ChirpsTypeConfig> for SharedRaftStorage<S>
where
    S: ChirpsRaftStorage<ChirpsTypeConfig>,
{
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ChirpsTypeConfig>>, StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        self.inner.lock().await.try_get_log_entries(range).await
    }
}

impl<S> RaftLogStorage<ChirpsTypeConfig> for SharedRaftStorage<S>
where
    S: ChirpsRaftStorage<ChirpsTypeConfig>,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<ChirpsTypeConfig>, StorageError<u64>> {
        self.inner.lock().await.get_log_state().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.lock().await.save_vote(vote).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.inner.lock().await.read_vote().await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<ChirpsTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.inner.lock().await.append(entries, callback).await;
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.inner.lock().await.truncate(log_id).await
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.inner.lock().await.purge(log_id).await
    }
}

impl<S> RaftStateMachine<ChirpsTypeConfig> for SharedRaftStorage<S>
where
    S: ChirpsRaftStorage<ChirpsTypeConfig>,
{
    type SnapshotBuilder = S::SnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        self.inner.lock().await.applied_state().await
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Vec<u8>>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.inner.lock().await.apply(entries).await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.inner.lock().await.get_snapshot_builder().await
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<u64>> {
        self.inner.lock().await.begin_receiving_snapshot().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        self.inner
            .lock()
            .await
            .install_snapshot(meta, snapshot)
            .await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ChirpsTypeConfig>>, StorageError<u64>> {
        self.inner.lock().await.get_current_snapshot().await
    }
}
