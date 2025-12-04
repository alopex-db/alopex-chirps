use crate::traits::{RaftStorage, StateMachine};
use crate::types::{
    BasicNode, ChirpsNodeId, ChirpsTypeConfig, Entry, EntryPayload, GroupId, LogFlushed, LogId,
    LogState, OptionalSend, RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    StorageError, StoredMembership, Vote,
};
use alopex_core::log::wal::{WalReader, WalRecord as CoreWalRecord, WalWriter};
use alopex_core::types::TxnId;
use anyhow::{Context, Result, anyhow};
use openraft::{ErrorSubject, ErrorVerb};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
#[cfg(test)]
use std::any::Any;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::{RangeBounds, RangeInclusive};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// WALファイルのマジックナンバー。
const WAL_MAGIC: [u8; 4] = *b"RWAL";
/// 現行のフォーマットバージョン。
pub const CURRENT_FORMAT_VERSION: u32 = 1;
/// WalWriterに埋め込むキー。
const WAL_KEY: &[u8] = b"raft";
/// スナップショットファイルのマジック。
const SNAP_MAGIC: [u8; 4] = *b"SNAP";
/// スナップショットファイルバージョン。
const SNAP_VERSION: u32 = 1;

/// WALストレージの設定値。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalStorageConfig {
    pub wal_dir: PathBuf,
    pub snapshot_dir: PathBuf,
    pub log_cache_size: usize,
    pub fsync_interval: usize,
    pub format_version: u32,
    /// スナップショットを書き出す際のチャンクサイズ（バイト）。
    pub snapshot_chunk_size: usize,
}

impl Default for WalStorageConfig {
    fn default() -> Self {
        Self {
            wal_dir: PathBuf::from("wal"),
            snapshot_dir: PathBuf::from("snapshot"),
            log_cache_size: 1024,
            fsync_interval: 0,
            format_version: CURRENT_FORMAT_VERSION,
            snapshot_chunk_size: 4096,
        }
    }
}

/// WALヘッダ。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalHeader {
    pub magic: [u8; 4],
    pub format_version: u32,
    pub group_id: GroupId,
    pub node_id: ChirpsNodeId,
    pub created_at: u64,
    pub reserved: Vec<u8>,
}

/// Raft向けWALレコード。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RaftWalRecord {
    AppendLog(Entry<ChirpsTypeConfig>),
    Vote(Vote<ChirpsNodeId>),
    SnapshotApplied(SnapshotMeta<ChirpsNodeId, BasicNode>),
    Truncate(LogId<ChirpsNodeId>),
    Purge(LogId<ChirpsNodeId>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum WalFrame {
    Header(WalHeader),
    Record(RaftWalRecord),
}

pub(crate) trait WalSink: Send + Sync {
    fn append(&mut self, record: &CoreWalRecord, sync: bool) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    #[cfg(test)]
    fn as_any(&self) -> &dyn Any;
}

struct RealWalSink {
    inner: Mutex<WalWriter>,
}

impl RealWalSink {
    fn new(inner: WalWriter) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

impl WalSink for RealWalSink {
    fn append(&mut self, record: &CoreWalRecord, _sync: bool) -> Result<()> {
        let mut guard = self.inner.lock().unwrap();
        guard.append_with_sync(record, _sync)?;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        let mut guard = self.inner.lock().unwrap();
        guard.sync()?;
        Ok(())
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// WalRaftStorage本体。
pub struct WalRaftStorage<SM>
where
    SM: StateMachine<Command = Vec<u8>, Response = Vec<u8>>,
{
    config: WalStorageConfig,
    group_id: GroupId,
    node_id: ChirpsNodeId,
    wal_path: PathBuf,
    wal_writer: Box<dyn WalSink>,
    vote: Option<Vote<ChirpsNodeId>>,
    log_cache: BTreeMap<u64, Entry<ChirpsTypeConfig>>,
    log_order: VecDeque<u64>,
    pending_unsynced: usize,
    last_purged_log_id: Option<LogId<ChirpsNodeId>>,
    purgeable_horizon: Option<LogId<ChirpsNodeId>>,
    last_applied: Option<LogId<ChirpsNodeId>>,
    last_membership: StoredMembership<ChirpsNodeId, BasicNode>,
    snapshot_meta: Option<SnapshotMeta<ChirpsNodeId, BasicNode>>,
    snapshot_path: Option<PathBuf>,
    state_machine: SM,
}

impl<SM> WalRaftStorage<SM>
where
    SM: StateMachine<Command = Vec<u8>, Response = Vec<u8>>,
{
    /// 新規または既存WALからストレージを初期化する。
    pub fn new(
        config: WalStorageConfig,
        group_id: GroupId,
        node_id: ChirpsNodeId,
        state_machine: SM,
    ) -> Result<Self> {
        Self::recover(config, group_id, node_id, state_machine)
    }

    /// 既存WALから状態を復元する。
    pub fn recover(
        config: WalStorageConfig,
        group_id: GroupId,
        node_id: ChirpsNodeId,
        state_machine: SM,
    ) -> Result<Self> {
        std::fs::create_dir_all(&config.wal_dir)?;
        std::fs::create_dir_all(&config.snapshot_dir)?;

        let wal_path = wal_path(&config.wal_dir, group_id, node_id);
        let wal_writer = Box::new(RealWalSink::new(WalWriter::new(&wal_path)?));
        Self::recover_with_sink(
            config,
            group_id,
            node_id,
            state_machine,
            wal_path,
            wal_writer,
        )
    }

    fn recover_with_sink(
        config: WalStorageConfig,
        group_id: GroupId,
        node_id: ChirpsNodeId,
        state_machine: SM,
        wal_path: PathBuf,
        wal_writer: Box<dyn WalSink>,
    ) -> Result<Self> {
        let mut storage = Self {
            config,
            group_id,
            node_id,
            wal_path,
            wal_writer,
            vote: None,
            log_cache: BTreeMap::new(),
            log_order: VecDeque::new(),
            pending_unsynced: 0,
            last_purged_log_id: None,
            purgeable_horizon: None,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_meta: None,
            snapshot_path: None,
            state_machine,
        };

        if !storage.wal_path.exists() || std::fs::metadata(&storage.wal_path)?.len() == 0 {
            let header = storage.build_header();
            storage
                .write_frame(&WalFrame::Header(header), true)
                .context("failed to write wal header")?;
            return Ok(storage);
        }

        storage.replay_wal()?;
        Ok(storage)
    }

    #[cfg(test)]
    pub(crate) fn with_sink_for_test(
        config: WalStorageConfig,
        group_id: GroupId,
        node_id: ChirpsNodeId,
        state_machine: SM,
        wal_writer: Box<dyn WalSink>,
    ) -> Result<Self> {
        let wal_path = wal_path(&config.wal_dir, group_id, node_id);
        Self::recover_with_sink(
            config,
            group_id,
            node_id,
            state_machine,
            wal_path,
            wal_writer,
        )
    }

    fn build_header(&self) -> WalHeader {
        WalHeader {
            magic: WAL_MAGIC,
            format_version: self.config.format_version,
            group_id: self.group_id,
            node_id: self.node_id,
            created_at: now_micros(),
            reserved: vec![0; 40],
        }
    }

    fn replay_wal(&mut self) -> Result<()> {
        let reader = WalReader::new(&self.wal_path)?;
        for record in reader {
            match record? {
                CoreWalRecord::Put(_, _, value) => {
                    let frame: WalFrame = decode_frame(&value)?;
                    match frame {
                        WalFrame::Header(header) => {
                            if header.magic != WAL_MAGIC {
                                return Err(anyhow!("invalid wal magic"));
                            }
                            if header.format_version != self.config.format_version {
                                return Err(anyhow!(
                                    "wal format version mismatch: expected {}, got {}",
                                    self.config.format_version,
                                    header.format_version
                                ));
                            }
                        }
                        WalFrame::Record(rec) => self.apply_wal_record(rec)?,
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn apply_wal_record(&mut self, record: RaftWalRecord) -> Result<()> {
        match record {
            RaftWalRecord::AppendLog(entry) => {
                self.last_applied = Some(entry.log_id.clone());
                if let EntryPayload::Membership(m) = &entry.payload {
                    self.last_membership =
                        StoredMembership::new(Some(entry.log_id.clone()), m.clone());
                }
                self.insert_entry(entry);
            }
            RaftWalRecord::Vote(vote) => {
                self.vote = Some(vote);
            }
            RaftWalRecord::SnapshotApplied(meta) => {
                self.snapshot_meta = Some(meta.clone());
                self.last_applied = meta.last_log_id.clone();
                self.last_membership = meta.last_membership.clone();
            }
            RaftWalRecord::Truncate(log_id) => {
                self.truncate_cache(log_id.index);
            }
            RaftWalRecord::Purge(log_id) => {
                self.purge_cache(log_id.index);
                self.last_purged_log_id = Some(log_id);
            }
        }
        Ok(())
    }

    fn insert_entry(&mut self, entry: Entry<ChirpsTypeConfig>) {
        let index = entry.log_id.index;
        self.log_cache.insert(index, entry);
        self.log_order.push_back(index);
        while self.log_cache.len() > self.config.log_cache_size {
            if let Some(oldest) = self.log_order.pop_front() {
                self.log_cache.remove(&oldest);
            }
        }
    }

    fn truncate_cache(&mut self, index: u64) {
        let keys: Vec<u64> = self
            .log_cache
            .range((index + 1)..)
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            self.log_cache.remove(&k);
        }
        self.log_order.retain(|i| *i <= index);
    }

    fn purge_cache(&mut self, index: u64) {
        let keys: Vec<u64> = self.log_cache.range(..=index).map(|(k, _)| *k).collect();
        for k in keys {
            self.log_cache.remove(&k);
        }
        self.log_order.retain(|i| *i > index);
    }

    fn write_frame(&mut self, frame: &WalFrame, sync_now: bool) -> Result<()> {
        let bytes = encode_frame(frame)?;
        self.wal_writer.append(
            &CoreWalRecord::Put(TxnId(0), WAL_KEY.to_vec(), bytes),
            sync_now,
        )?;
        if sync_now {
            self.pending_unsynced = 0;
        } else {
            self.pending_unsynced = self.pending_unsynced.saturating_add(1);
        }
        Ok(())
    }

    fn sync_wal(&mut self) -> Result<()> {
        self.wal_writer.sync()?;
        self.pending_unsynced = 0;
        Ok(())
    }

    fn last_log_id(&self) -> Option<LogId<ChirpsNodeId>> {
        self.log_cache
            .iter()
            .rev()
            .next()
            .map(|(_, entry)| entry.log_id.clone())
    }

    /// LogIdベースの取得ヘルパー。
    pub async fn get_entries_by_log_id(
        &mut self,
        range: RangeInclusive<LogId<ChirpsNodeId>>,
    ) -> Result<Vec<Entry<ChirpsTypeConfig>>, StorageError<ChirpsNodeId>> {
        let start = range.start().index;
        let end = range.end().index;
        self.try_get_log_entries(start..=end).await
    }

    fn to_storage_io_error(
        &self,
        err: impl Into<anyhow::Error>,
        subject: ErrorSubject<ChirpsNodeId>,
        verb: ErrorVerb,
    ) -> StorageError<ChirpsNodeId> {
        let io_err = io::Error::new(io::ErrorKind::Other, err.into().to_string());
        StorageError::from_io_error(subject, verb, io_err)
    }

    fn read_entries_from_wal<RB>(&self, range: RB) -> Result<Vec<Entry<ChirpsTypeConfig>>>
    where
        RB: RangeBounds<u64> + Clone,
    {
        let reader = WalReader::new(&self.wal_path)?;
        let mut entries: BTreeMap<u64, Entry<ChirpsTypeConfig>> = BTreeMap::new();
        for record in reader {
            let frame = match record? {
                CoreWalRecord::Put(_, _, bytes) => decode_frame::<WalFrame>(&bytes)?,
                _ => continue,
            };
            match frame {
                WalFrame::Record(RaftWalRecord::AppendLog(entry)) => {
                    entries.insert(entry.log_id.index, entry);
                }
                WalFrame::Record(RaftWalRecord::Truncate(log_id)) => {
                    let idx = log_id.index;
                    let keys: Vec<u64> = entries.range((idx + 1)..).map(|(k, _)| *k).collect();
                    for k in keys {
                        entries.remove(&k);
                    }
                }
                WalFrame::Record(RaftWalRecord::Purge(log_id)) => {
                    let idx = log_id.index;
                    let keys: Vec<u64> = entries.range(..=idx).map(|(k, _)| *k).collect();
                    for k in keys {
                        entries.remove(&k);
                    }
                }
                WalFrame::Record(RaftWalRecord::SnapshotApplied(meta)) => {
                    if let Some(id) = meta.last_log_id.as_ref() {
                        let idx = id.index;
                        let keys: Vec<u64> = entries.range(..=idx).map(|(k, _)| *k).collect();
                        for k in keys {
                            entries.remove(&k);
                        }
                    }
                }
                _ => {}
            }
        }

        let filtered = entries
            .into_values()
            .filter(|e| range_contains(&range, e.log_id.index))
            .collect();

        Ok(filtered)
    }

    fn append_entry(&mut self, entry: Entry<ChirpsTypeConfig>) -> Result<()> {
        let sync_now = self.config.fsync_interval == 0;
        self.write_frame(
            &WalFrame::Record(RaftWalRecord::AppendLog(entry.clone())),
            sync_now,
        )?;

        self.last_applied = Some(entry.log_id.clone());
        if let EntryPayload::Membership(m) = &entry.payload {
            self.last_membership = StoredMembership::new(Some(entry.log_id.clone()), m.clone());
        }
        self.insert_entry(entry);

        if !sync_now && self.config.fsync_interval > 0 {
            if self.pending_unsynced >= self.config.fsync_interval {
                self.sync_wal()?;
            }
        }

        Ok(())
    }

    #[cfg(test)]
    pub async fn append_for_test(&mut self, entries: Vec<Entry<ChirpsTypeConfig>>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        for entry in entries {
            self.append_entry(entry)?;
        }
        if self.config.fsync_interval > 0 && self.pending_unsynced > 0 {
            self.sync_wal()?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<SM> RaftStorage<ChirpsTypeConfig> for WalRaftStorage<SM>
where
    SM: StateMachine<Command = Vec<u8>, Response = Vec<u8>>,
{
    type LogReader = WalLogReader;
    type SnapshotBuilder = WalSnapshotBuilder;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<ChirpsTypeConfig>, StorageError<ChirpsNodeId>> {
        Ok(LogState {
            last_purged_log_id: self.last_purged_log_id.clone(),
            last_log_id: self.last_log_id(),
        })
    }

    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ChirpsTypeConfig>>, StorageError<ChirpsNodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let mut seen = HashSet::new();
        let mut entries: Vec<Entry<ChirpsTypeConfig>> = self
            .log_cache
            .range(range.clone())
            .map(|(_, e)| {
                seen.insert(e.log_id.index);
                e.clone()
            })
            .collect();

        let wal_entries = self
            .read_entries_from_wal(range.clone())
            .map_err(|e| self.to_storage_io_error(e, ErrorSubject::Logs, ErrorVerb::Read))?;

        for entry in wal_entries {
            if seen.insert(entry.log_id.index) {
                entries.push(entry);
            }
        }

        entries.sort_by_key(|e| e.log_id.index);
        Ok(entries)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<ChirpsTypeConfig>)
    where
        I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let collected: Vec<Entry<ChirpsTypeConfig>> = entries.into_iter().collect();
        if collected.is_empty() {
            callback.log_io_completed(Ok(()));
            return;
        }

        let res = (|| -> Result<()> {
            for entry in collected {
                self.append_entry(entry)?;
            }
            if self.config.fsync_interval > 0 && self.pending_unsynced > 0 {
                self.sync_wal()?;
            }
            Ok(())
        })();

        match res {
            Ok(()) => callback.log_io_completed(Ok(())),
            Err(e) => {
                callback.log_io_completed(Err(io::Error::new(io::ErrorKind::Other, e.to_string())))
            }
        }
    }

    async fn truncate(
        &mut self,
        log_id: LogId<ChirpsNodeId>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        self.write_frame(
            &WalFrame::Record(RaftWalRecord::Truncate(log_id.clone())),
            true,
        )
        .map_err(|e| self.to_storage_io_error(e, ErrorSubject::Logs, ErrorVerb::Write))?;
        self.truncate_cache(log_id.index);
        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<ChirpsNodeId>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        self.write_frame(
            &WalFrame::Record(RaftWalRecord::Purge(log_id.clone())),
            true,
        )
        .map_err(|e| self.to_storage_io_error(e, ErrorSubject::Logs, ErrorVerb::Write))?;
        self.purge_cache(log_id.index);
        self.last_purged_log_id = Some(log_id);
        Ok(())
    }

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<ChirpsNodeId>>,
            StoredMembership<ChirpsNodeId, BasicNode>,
        ),
        StorageError<ChirpsNodeId>,
    > {
        Ok((self.last_applied.clone(), self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Vec<u8>>, StorageError<ChirpsNodeId>>
    where
        I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        for entry in entries {
            self.last_applied = Some(entry.log_id.clone());
            match entry.payload.clone() {
                EntryPayload::Normal(cmd) => {
                    let resp = self
                        .state_machine
                        .apply(entry.log_id.clone(), cmd)
                        .await
                        .map_err(|e| {
                            self.to_storage_io_error(
                                e,
                                ErrorSubject::StateMachine,
                                ErrorVerb::Write,
                            )
                        })?;
                    responses.push(resp);
                }
                EntryPayload::Membership(m) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id.clone()), m);
                }
                EntryPayload::Blank => {}
            }
        }
        Ok(responses)
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<ChirpsNodeId>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        self.write_frame(&WalFrame::Record(RaftWalRecord::Vote(vote.clone())), true)
            .map_err(|e| self.to_storage_io_error(e, ErrorSubject::Vote, ErrorVerb::Write))?;
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<ChirpsNodeId>>, StorageError<ChirpsNodeId>> {
        Ok(self.vote.clone())
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<ChirpsNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<ChirpsNodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<ChirpsNodeId>> {
        let path = snapshot_path(&self.config.snapshot_dir, self.group_id, meta);
        let data = snapshot.into_inner();
        let meta_bytes = bincode::serialize(meta).map_err(|e| {
            self.to_storage_io_error(e, ErrorSubject::Snapshot(None), ErrorVerb::Write)
        })?;
        let chunk_size = self.config.snapshot_chunk_size.max(1);

        let mut buf = Vec::new();
        buf.extend_from_slice(&SNAP_MAGIC);
        buf.extend_from_slice(&SNAP_VERSION.to_le_bytes());

        let mut offset = 0usize;
        let mut first = true;
        loop {
            let meta_part = if first {
                meta_bytes.clone()
            } else {
                Vec::new()
            };
            let take = std::cmp::min(chunk_size, data.len().saturating_sub(offset));
            let data_part = data[offset..offset + take].to_vec();
            let checksum = crc32fast::hash(
                [meta_part.as_slice(), data_part.as_slice()]
                    .concat()
                    .as_slice(),
            );

            buf.extend_from_slice(&(meta_part.len() as u32).to_le_bytes());
            buf.extend_from_slice(&(data_part.len() as u32).to_le_bytes());
            buf.extend_from_slice(&checksum.to_le_bytes());
            buf.extend_from_slice(&meta_part);
            buf.extend_from_slice(&data_part);

            offset += take;
            first = false;
            if offset >= data.len() {
                break;
            }
        }

        // 終端チャンク
        buf.extend_from_slice(&0u32.to_le_bytes()); // meta_len
        buf.extend_from_slice(&0u32.to_le_bytes()); // data_len
        buf.extend_from_slice(&0u32.to_le_bytes()); // checksum

        std::fs::write(&path, buf).map_err(|e| {
            self.to_storage_io_error(e, ErrorSubject::Snapshot(None), ErrorVerb::Write)
        })?;
        self.snapshot_meta = Some(meta.clone());
        self.snapshot_path = Some(path);
        self.last_applied = meta.last_log_id.clone();
        self.last_membership = meta.last_membership.clone();
        // 記録をWALに残し、リカバリで反映できるようにする。
        self.write_frame(
            &WalFrame::Record(RaftWalRecord::SnapshotApplied(meta.clone())),
            true,
        )
        .map_err(|e| self.to_storage_io_error(e, ErrorSubject::Snapshot(None), ErrorVerb::Write))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ChirpsTypeConfig>>, StorageError<ChirpsNodeId>> {
        match (&self.snapshot_meta, &self.snapshot_path) {
            (Some(_meta), Some(path)) => {
                let bytes = std::fs::read(path).map_err(|e| {
                    self.to_storage_io_error(e, ErrorSubject::Snapshot(None), ErrorVerb::Read)
                })?;
                if bytes.len() < 20 || &bytes[..4] != SNAP_MAGIC {
                    return Err(self.to_storage_io_error(
                        anyhow!("invalid snapshot header"),
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Read,
                    ));
                }
                let _version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
                let mut offset = 8;
                let mut meta_bytes: Option<Vec<u8>> = None;
                let mut data = Vec::new();
                loop {
                    if offset + 12 > bytes.len() {
                        return Err(self.to_storage_io_error(
                            anyhow!("snapshot truncated"),
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Read,
                        ));
                    }
                    let meta_len =
                        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                    let data_len =
                        u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap())
                            as usize;
                    let checksum =
                        u32::from_le_bytes(bytes[offset + 8..offset + 12].try_into().unwrap());
                    offset += 12;

                    if meta_len == 0 && data_len == 0 {
                        if checksum != 0 {
                            return Err(self.to_storage_io_error(
                                anyhow!("invalid terminator checksum"),
                                ErrorSubject::Snapshot(None),
                                ErrorVerb::Read,
                            ));
                        }
                        break;
                    }

                    if offset + meta_len + data_len > bytes.len() {
                        return Err(self.to_storage_io_error(
                            anyhow!("snapshot truncated"),
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Read,
                        ));
                    }

                    let body = &bytes[offset..offset + meta_len + data_len];
                    offset += meta_len + data_len;
                    let calc = crc32fast::hash(body);
                    if calc != checksum {
                        return Err(self.to_storage_io_error(
                            anyhow!("snapshot checksum mismatch"),
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Read,
                        ));
                    }

                    if meta_len > 0 {
                        meta_bytes = Some(body[..meta_len].to_vec());
                    }
                    data.extend_from_slice(&body[meta_len..]);
                }

                let meta_bytes = meta_bytes.ok_or_else(|| {
                    self.to_storage_io_error(
                        anyhow!("snapshot meta missing"),
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Read,
                    )
                })?;
                let meta: SnapshotMeta<ChirpsNodeId, BasicNode> = bincode::deserialize(&meta_bytes)
                    .map_err(|e| {
                        self.to_storage_io_error(e, ErrorSubject::Snapshot(None), ErrorVerb::Read)
                    })?;

                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            _ => Ok(None),
        }
    }

    fn set_purgeable_horizon(&mut self, horizon: Option<LogId<ChirpsNodeId>>) {
        self.purgeable_horizon = horizon;
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        WalLogReader {
            entries: self.log_cache.values().cloned().collect(),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let meta = SnapshotMeta {
            last_log_id: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
            snapshot_id: format!("snapshot-{}", now_micros()),
        };
        WalSnapshotBuilder { meta }
    }
}

fn wal_path(base: &Path, group_id: GroupId, node_id: ChirpsNodeId) -> PathBuf {
    base.join(format!("raft-{}-{}.wal", group_id.0, node_id))
}

fn snapshot_path(
    base: &Path,
    group_id: GroupId,
    meta: &SnapshotMeta<ChirpsNodeId, BasicNode>,
) -> PathBuf {
    base.join(format!(
        "snapshot-{}-{}.alopex",
        group_id.0, meta.snapshot_id
    ))
}

fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let payload = bincode::serialize(value)?;
    let checksum = crc32fast::hash(&payload);
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf)
}

fn decode_frame<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() < 4 {
        return Err(anyhow!("frame too small"));
    }
    let stored = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let payload = &bytes[4..];
    let actual = crc32fast::hash(payload);
    if stored != actual {
        return Err(anyhow!("checksum mismatch"));
    }
    Ok(bincode::deserialize(payload)?)
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// ログ読み出し用リーダー。
pub struct WalLogReader {
    entries: Vec<Entry<ChirpsTypeConfig>>,
}

impl RaftLogReader<ChirpsTypeConfig> for WalLogReader {
    fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> impl core::future::Future<
        Output = Result<Vec<Entry<ChirpsTypeConfig>>, StorageError<ChirpsNodeId>>,
    > + Send
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let entries: Vec<Entry<ChirpsTypeConfig>> = self
            .entries
            .iter()
            .filter(|e| range_contains(&range, e.log_id.index))
            .cloned()
            .collect();
        async move { Ok(entries) }
    }
}

fn range_contains<RB>(range: &RB, value: u64) -> bool
where
    RB: RangeBounds<u64>,
{
    use std::ops::Bound;
    let start_ok = match range.start_bound() {
        Bound::Included(v) => value >= *v,
        Bound::Excluded(v) => value > *v,
        Bound::Unbounded => true,
    };
    let end_ok = match range.end_bound() {
        Bound::Included(v) => value <= *v,
        Bound::Excluded(v) => value < *v,
        Bound::Unbounded => true,
    };
    start_ok && end_ok
}

/// スナップショットビルダー。
pub struct WalSnapshotBuilder {
    meta: SnapshotMeta<ChirpsNodeId, BasicNode>,
}

impl RaftSnapshotBuilder<ChirpsTypeConfig> for WalSnapshotBuilder {
    fn build_snapshot(
        &mut self,
    ) -> impl core::future::Future<
        Output = Result<Snapshot<ChirpsTypeConfig>, StorageError<ChirpsNodeId>>,
    > + Send {
        let snapshot = Snapshot {
            meta: self.meta.clone(),
            snapshot: Box::new(Cursor::new(Vec::new())),
        };
        async move { Ok(snapshot) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
    use crate::types::Membership;
    use async_trait::async_trait;
    use bincode;
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[derive(Default)]
    struct MockStateMachine;

    #[async_trait]
    impl StateMachine for MockStateMachine {
        type Command = Vec<u8>;
        type Response = Vec<u8>;

        async fn apply(
            &mut self,
            _log_id: LogId<ChirpsNodeId>,
            command: Self::Command,
        ) -> StateMachineResult<Self::Response> {
            Ok(command)
        }

        async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
            Ok(Box::new(Cursor::new(Vec::new())))
        }

        async fn restore(
            &mut self,
            _snapshot: Box<dyn AsyncSnapshotData>,
        ) -> StateMachineResult<()> {
            Ok(())
        }
    }

    fn base_config(path: &Path) -> WalStorageConfig {
        WalStorageConfig {
            wal_dir: path.join("wal"),
            snapshot_dir: path.join("snapshot"),
            ..Default::default()
        }
    }

    fn sample_entry(index: u64) -> Entry<ChirpsTypeConfig> {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(format!("e{index}").into_bytes()),
        }
    }

    /// WAL書き込みを観測するためのモックシンク。
    struct CountingWalSink {
        inner: RealWalSink,
        appended: Arc<Mutex<usize>>,
        synced: Arc<Mutex<usize>>,
    }

    impl CountingWalSink {
        fn new(path: &Path) -> Self {
            Self {
                inner: RealWalSink::new(WalWriter::new(path).unwrap()),
                appended: Arc::new(Mutex::new(0)),
                synced: Arc::new(Mutex::new(0)),
            }
        }

        fn counts(&self) -> (usize, usize) {
            (*self.appended.lock().unwrap(), *self.synced.lock().unwrap())
        }

        fn reset(&self) {
            *self.appended.lock().unwrap() = 0;
            *self.synced.lock().unwrap() = 0;
        }
    }

    impl WalSink for CountingWalSink {
        fn append(&mut self, record: &CoreWalRecord, sync: bool) -> Result<()> {
            *self.appended.lock().unwrap() += 1;
            if sync {
                *self.synced.lock().unwrap() += 1;
            }
            self.inner.append(record, sync)
        }

        fn sync(&mut self) -> Result<()> {
            *self.synced.lock().unwrap() += 1;
            self.inner.sync()
        }

        #[cfg(test)]
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn append_and_get_entries() {
        let dir = tempdir().unwrap();
        let cfg = base_config(dir.path());
        let mut storage =
            WalRaftStorage::new(cfg, GroupId(1), 1, MockStateMachine::default()).unwrap();

        storage
            .append_for_test(vec![sample_entry(1), sample_entry(2)])
            .await
            .unwrap();

        let entries = storage.try_get_log_entries(1..=2).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].log_id.index, 1);
        assert_eq!(entries[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn truncate_and_purge_updates_state() {
        let dir = tempdir().unwrap();
        let cfg = base_config(dir.path());
        let mut storage =
            WalRaftStorage::new(cfg, GroupId(2), 2, MockStateMachine::default()).unwrap();
        storage
            .append_for_test(vec![sample_entry(1), sample_entry(2)])
            .await
            .unwrap();

        storage
            .truncate(LogId::new(openraft::CommittedLeaderId::new(1, 1), 1))
            .await
            .unwrap();
        assert_eq!(storage.try_get_log_entries(2..=2).await.unwrap().len(), 0);

        storage
            .purge(LogId::new(openraft::CommittedLeaderId::new(1, 1), 1))
            .await
            .unwrap();
        let state = storage.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 1);
    }

    #[tokio::test]
    async fn vote_is_persisted() {
        let dir = tempdir().unwrap();
        let cfg = base_config(dir.path());
        let vote = Vote::new(2, 3);
        {
            let mut storage =
                WalRaftStorage::new(cfg.clone(), GroupId(3), 3, MockStateMachine::default())
                    .unwrap();
            storage.save_vote(&vote).await.unwrap();
        }

        let mut recovered =
            WalRaftStorage::recover(cfg, GroupId(3), 3, MockStateMachine::default()).unwrap();
        let loaded = recovered.read_vote().await.unwrap().unwrap();
        assert_eq!(loaded, vote);
    }

    #[tokio::test]
    async fn recover_replays_logs() {
        let dir = tempdir().unwrap();
        let cfg = base_config(dir.path());
        {
            let mut storage =
                WalRaftStorage::new(cfg.clone(), GroupId(4), 4, MockStateMachine::default())
                    .unwrap();
            storage
                .append_for_test(vec![sample_entry(10)])
                .await
                .unwrap();
        }

        let mut recovered =
            WalRaftStorage::recover(cfg, GroupId(4), 4, MockStateMachine::default()).unwrap();
        let state = recovered.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 10);
    }

    #[tokio::test]
    async fn evicted_entries_can_be_read_from_wal() {
        let dir = tempdir().unwrap();
        let mut cfg = base_config(dir.path());
        cfg.log_cache_size = 1;
        let mut storage =
            WalRaftStorage::new(cfg, GroupId(5), 5, MockStateMachine::default()).unwrap();

        storage
            .append_for_test(vec![sample_entry(1), sample_entry(2)])
            .await
            .unwrap();

        let entries = storage.try_get_log_entries(1..=1).await.unwrap();
        assert_eq!(entries.len(), 1, "evicted log should be fetched from WAL");
        assert_eq!(entries[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn install_snapshot_updates_applied_state() {
        let dir = tempdir().unwrap();
        let cfg = base_config(dir.path());
        let mut storage =
            WalRaftStorage::new(cfg, GroupId(6), 6, MockStateMachine::default()).unwrap();

        let mut voters = std::collections::BTreeSet::new();
        voters.insert(7);
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(
            7,
            BasicNode {
                addr: "127.0.0.1:7000".into(),
            },
        );
        let membership = StoredMembership::new(
            Some(LogId::new(openraft::CommittedLeaderId::new(1, 6), 3)),
            Membership::new(vec![voters], nodes),
        );
        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(2, 6), 4)),
            last_membership: membership.clone(),
            snapshot_id: "snap-1".into(),
        };

        storage
            .install_snapshot(&meta, Box::new(Cursor::new(vec![1, 2, 3])))
            .await
            .unwrap();

        let (applied, applied_membership) = storage.applied_state().await.unwrap();
        assert_eq!(applied, meta.last_log_id);
        assert_eq!(applied_membership, membership);
    }

    #[tokio::test]
    async fn wal_header_and_records_follow_spec() {
        let dir = tempdir().unwrap();
        let cfg = base_config(dir.path());
        let mut storage =
            WalRaftStorage::new(cfg, GroupId(7), 7, MockStateMachine::default()).unwrap();

        storage
            .append_for_test(vec![sample_entry(1), sample_entry(2)])
            .await
            .unwrap();

        let reader = WalReader::new(&storage.wal_path).unwrap();
        let mut frames = Vec::new();
        for rec in reader {
            if let CoreWalRecord::Put(_, _, bytes) = rec.unwrap() {
                frames.push(decode_frame::<WalFrame>(&bytes).unwrap());
            }
        }

        let header = match frames.first().unwrap() {
            WalFrame::Header(h) => h.clone(),
            _ => panic!("first frame is not header"),
        };
        assert_eq!(&header.magic, b"RWAL");
        assert_eq!(header.format_version, CURRENT_FORMAT_VERSION);

        let append_records: Vec<Entry<ChirpsTypeConfig>> = frames
            .iter()
            .filter_map(|f| match f {
                WalFrame::Record(RaftWalRecord::AppendLog(entry)) => Some(entry.clone()),
                _ => None,
            })
            .collect();

        assert_eq!(append_records.len(), 2, "should emit one record per entry");
        assert_eq!(append_records[0].log_id.index, 1);
        assert_eq!(append_records[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn fsync_interval_controls_sync_frequency() {
        let dir = tempdir().unwrap();
        let mut cfg = base_config(dir.path());
        cfg.fsync_interval = 2;
        std::fs::create_dir_all(&cfg.wal_dir).unwrap();
        let wal_path = wal_path(&cfg.wal_dir, GroupId(8), 8);
        let sink = CountingWalSink::new(&wal_path);

        let mut storage = WalRaftStorage::with_sink_for_test(
            cfg,
            GroupId(8),
            8,
            MockStateMachine::default(),
            Box::new(sink),
        )
        .unwrap();

        storage
            .wal_writer
            .as_any()
            .downcast_ref::<CountingWalSink>()
            .unwrap()
            .reset();

        storage
            .append_for_test(vec![sample_entry(1), sample_entry(2), sample_entry(3)])
            .await
            .unwrap();

        let (appends, syncs) = storage
            .wal_writer
            .as_any()
            .downcast_ref::<CountingWalSink>()
            .unwrap()
            .counts();

        assert_eq!(appends, 3);
        assert_eq!(syncs, 2, "fsync_interval should sync every 2 entries");
    }

    #[tokio::test]
    async fn snapshot_files_follow_udf_header() {
        let dir = tempdir().unwrap();
        let cfg = base_config(dir.path());
        let mut storage =
            WalRaftStorage::new(cfg.clone(), GroupId(9), 9, MockStateMachine::default()).unwrap();

        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(1, 9), 2)),
            last_membership: StoredMembership::default(),
            snapshot_id: "udf-1".into(),
        };
        storage
            .install_snapshot(&meta, Box::new(Cursor::new(vec![0u8; 16])))
            .await
            .unwrap();

        let path = storage.snapshot_path.as_ref().unwrap();
        let bytes = std::fs::read(path).unwrap();
        assert!(
            bytes.starts_with(b"SNAP"),
            "snapshot must start with UDF magic"
        );
        assert!(
            bytes.len() >= 8,
            "snapshot file should contain header with version"
        );
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        assert_eq!(version, 1, "snapshot version must be set");
    }

    fn membership_entry(index: u64) -> Entry<ChirpsTypeConfig> {
        let mut voters = std::collections::BTreeSet::new();
        voters.insert(1);
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(
            1,
            BasicNode {
                addr: "127.0.0.1:7001".into(),
            },
        );
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(2, 1), index),
            payload: EntryPayload::Membership(Membership::new(vec![voters], nodes)),
        }
    }

    #[tokio::test]
    async fn membership_replay_updates_state() {
        let dir = tempdir().unwrap();
        let cfg = base_config(dir.path());
        let entry = membership_entry(5);
        let expected_membership = if let EntryPayload::Membership(m) = entry.payload.clone() {
            StoredMembership::new(Some(entry.log_id.clone()), m)
        } else {
            unreachable!()
        };
        {
            let mut storage =
                WalRaftStorage::new(cfg.clone(), GroupId(10), 10, MockStateMachine::default())
                    .unwrap();
            storage.append_for_test(vec![entry]).await.unwrap();
        }

        let mut recovered =
            WalRaftStorage::recover(cfg, GroupId(10), 10, MockStateMachine::default()).unwrap();
        let (_, stored_membership) = recovered.applied_state().await.unwrap();
        assert_eq!(stored_membership, expected_membership);
    }

    #[tokio::test]
    async fn snapshot_applied_persisted_via_wal() {
        let dir = tempdir().unwrap();
        let cfg = base_config(dir.path());
        let mut storage =
            WalRaftStorage::new(cfg.clone(), GroupId(11), 11, MockStateMachine::default()).unwrap();

        let mut voters = std::collections::BTreeSet::new();
        voters.insert(2);
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(
            2,
            BasicNode {
                addr: "127.0.0.1:7002".into(),
            },
        );
        let membership = StoredMembership::new(
            Some(LogId::new(openraft::CommittedLeaderId::new(3, 11), 6)),
            Membership::new(vec![voters], nodes),
        );
        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(3, 11), 6)),
            last_membership: membership.clone(),
            snapshot_id: "snap-wal".into(),
        };

        storage
            .install_snapshot(&meta, Box::new(Cursor::new(vec![9, 9, 9])))
            .await
            .unwrap();

        let mut recovered =
            WalRaftStorage::recover(cfg, GroupId(11), 11, MockStateMachine::default()).unwrap();
        let (applied, stored_membership) = recovered.applied_state().await.unwrap();
        assert_eq!(applied, meta.last_log_id);
        assert_eq!(stored_membership, membership);
    }

    #[tokio::test]
    async fn snapshot_file_uses_udf_structure() {
        let dir = tempdir().unwrap();
        let cfg = base_config(dir.path());
        let mut storage =
            WalRaftStorage::new(cfg.clone(), GroupId(12), 12, MockStateMachine::default()).unwrap();

        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(4, 12), 8)),
            last_membership: StoredMembership::default(),
            snapshot_id: "udf-2".into(),
        };
        let data = vec![1u8, 2, 3, 4];
        storage
            .install_snapshot(&meta, Box::new(Cursor::new(data.clone())))
            .await
            .unwrap();

        let path = storage.snapshot_path.as_ref().unwrap();
        let bytes = std::fs::read(path).unwrap();
        assert!(bytes.starts_with(b"SNAP"));
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        assert_eq!(version, 1);

        let mut offset = 8;
        let mut chunks = 0;
        let mut collected_meta: Option<SnapshotMeta<ChirpsNodeId, BasicNode>> = None;
        let mut collected_data = Vec::new();
        loop {
            let meta_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            let data_len =
                u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
            let checksum = u32::from_le_bytes(bytes[offset + 8..offset + 12].try_into().unwrap());
            offset += 12;
            if meta_len == 0 && data_len == 0 {
                assert_eq!(checksum, 0, "terminator checksum must be zero");
                break;
            }
            let body = &bytes[offset..offset + meta_len + data_len];
            offset += meta_len + data_len;
            let calc = crc32fast::hash(body);
            assert_eq!(checksum, calc, "chunk checksum mismatch");
            if meta_len > 0 {
                let meta_bytes = &body[..meta_len];
                let meta_decoded: SnapshotMeta<ChirpsNodeId, BasicNode> =
                    bincode::deserialize(meta_bytes).unwrap();
                collected_meta = Some(meta_decoded);
            }
            collected_data.extend_from_slice(&body[meta_len..]);
            chunks += 1;
        }

        assert_eq!(collected_meta.unwrap(), meta);
        assert_eq!(collected_data, data);
        assert!(chunks >= 1);
    }

    #[tokio::test]
    async fn snapshot_chunked_with_terminator() {
        let dir = tempdir().unwrap();
        let mut cfg = base_config(dir.path());
        cfg.snapshot_chunk_size = 2;
        let mut storage =
            WalRaftStorage::new(cfg.clone(), GroupId(13), 13, MockStateMachine::default()).unwrap();

        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(5, 13), 3)),
            last_membership: StoredMembership::default(),
            snapshot_id: "chunked".into(),
        };
        let data = vec![10u8, 11, 12, 13, 14];
        storage
            .install_snapshot(&meta, Box::new(Cursor::new(data.clone())))
            .await
            .unwrap();

        let bytes = std::fs::read(storage.snapshot_path.as_ref().unwrap()).unwrap();
        assert!(bytes.starts_with(b"SNAP"));
        let mut offset = 8;
        let mut seen_terminator = false;
        let mut reconstructed = Vec::new();
        let mut meta_seen = false;
        let mut chunk_count = 0;
        while offset < bytes.len() {
            let meta_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            let data_len =
                u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
            let checksum = u32::from_le_bytes(bytes[offset + 8..offset + 12].try_into().unwrap());
            offset += 12;
            if meta_len == 0 && data_len == 0 {
                assert_eq!(checksum, 0);
                seen_terminator = true;
                break;
            }
            let body = &bytes[offset..offset + meta_len + data_len];
            offset += meta_len + data_len;
            assert_eq!(checksum, crc32fast::hash(body));
            if meta_len > 0 {
                let meta_bytes = &body[..meta_len];
                let decoded: SnapshotMeta<ChirpsNodeId, BasicNode> =
                    bincode::deserialize(meta_bytes).unwrap();
                assert_eq!(decoded, meta);
                meta_seen = true;
            }
            reconstructed.extend_from_slice(&body[meta_len..]);
            chunk_count += 1;
        }
        assert!(seen_terminator, "terminator chunk missing");
        assert!(meta_seen, "meta chunk missing");
        assert_eq!(reconstructed, data);
        assert!(
            chunk_count >= 2,
            "data should be split into multiple chunks"
        );
    }
}
