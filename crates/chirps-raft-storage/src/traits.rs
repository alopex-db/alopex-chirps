use crate::types::{
    ChirpsNodeId, LogFlushed, LogId, LogState, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    RaftTypeConfig, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::ops::RangeBounds;
use tokio::io::{AsyncRead, AsyncSeek};

/// スナップショットデータ向けの非同期読み取りトレイト束。
pub trait AsyncSnapshotData: AsyncRead + AsyncSeek + Send + Sync + Unpin {}

impl<T> AsyncSnapshotData for T where T: AsyncRead + AsyncSeek + Send + Sync + Unpin {}

/// StateMachine関連の共通Result型。
pub type StateMachineResult<T> = anyhow::Result<T>;

/// アプリケーション固有のステートマシン抽象。
///
/// # 例
///
/// ```rust,ignore
/// use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
/// use alopex_chirps_raft_storage::types::LogId;
/// use async_trait::async_trait;
/// use tokio::io::Cursor;
///
/// #[derive(Default)]
/// struct KvStateMachine;
///
/// #[async_trait]
/// impl StateMachine for KvStateMachine {
///     type Command = Vec<u8>;
///     type Response = Vec<u8>;
///
///     async fn apply(
///         &mut self,
///         log_id: LogId<u64>,
///         command: Self::Command,
///     ) -> StateMachineResult<Self::Response> {
///         // ここでコマンドをメモリ上の状態に反映する
///         let _ = log_id;
///         Ok(command)
///     }
///
///     async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
///         Ok(Box::new(Cursor::new(Vec::new())))
///     }
///
///     async fn restore(&mut self, snapshot: Box<dyn AsyncSnapshotData>) -> StateMachineResult<()> {
///         let _ = snapshot;
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait StateMachine: Send + Sync + 'static {
    /// コマンド型。シリアライズ可能であること。
    type Command: Send + Sync + Clone + Serialize + DeserializeOwned;
    /// コマンドに対する応答型。
    type Response: Send + Sync + Clone + Serialize + DeserializeOwned;

    /// コミット済みログを適用する。
    async fn apply(
        &mut self,
        log_id: LogId<ChirpsNodeId>,
        command: Self::Command,
    ) -> StateMachineResult<Self::Response>;

    /// 現在状態のスナップショットを生成する。
    async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>>;

    /// スナップショットから状態を復元する。
    async fn restore(&mut self, snapshot: Box<dyn AsyncSnapshotData>) -> StateMachineResult<()>;
}

/// openraft v0.9.17互換のRaftStorage抽象。
///
/// # 例
///
/// ```rust,ignore
/// use alopex_chirps_raft_storage::traits::{RaftStorage, StateMachine};
/// use alopex_chirps_raft_storage::types::{ChirpsTypeConfig, Entry, LogFlushed, LogId};
/// use async_trait::async_trait;
///
/// struct InMemoryStorage {
///     entries: Vec<Entry<ChirpsTypeConfig>>,
/// }
///
/// #[async_trait]
/// impl RaftStorage<ChirpsTypeConfig> for InMemoryStorage {
///     type LogReader = ();
///     type SnapshotBuilder = ();
///
///     async fn get_log_state(
///         &mut self,
///     ) -> Result<openraft::LogState<ChirpsTypeConfig>, openraft::StorageError<u64>> {
///         Ok(openraft::LogState {
///             last_purged_log_id: None,
///             last_log_id: self.entries.last().map(|e| e.log_id.clone()),
///         })
///     }
///
///     async fn try_get_log_entries<RB>(
///         &mut self,
///         _range: RB,
///     ) -> Result<Vec<Entry<ChirpsTypeConfig>>, openraft::StorageError<u64>>
///     where
///         RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
///     {
///         Ok(self.entries.clone())
///     }
///
///     async fn append<I>(&mut self, entries: I, callback: LogFlushed<ChirpsTypeConfig>)
///     where
///         I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + Send,
///         I::IntoIter: Send,
///     {
///         self.entries.extend(entries);
///         callback.log_io_completed(Ok(()));
///     }
///
///     async fn truncate(
///         &mut self,
///         _log_id: LogId<u64>,
///     ) -> Result<(), openraft::StorageError<u64>> {
///         Ok(())
///     }
///
///     async fn purge(
///         &mut self,
///         _log_id: LogId<u64>,
///     ) -> Result<(), openraft::StorageError<u64>> {
///         Ok(())
///     }
///
///     async fn applied_state(
///         &mut self,
///     ) -> Result<
///         (
///             Option<LogId<u64>>,
///             openraft::StoredMembership<u64, openraft::BasicNode>,
///         ),
///         openraft::StorageError<u64>,
///     > {
///         Ok((None, openraft::StoredMembership::default()))
///     }
///
///     async fn apply<I>(
///         &mut self,
///         _entries: I,
///     ) -> Result<Vec<Vec<u8>>, openraft::StorageError<u64>>
///     where
///         I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + Send,
///         I::IntoIter: Send,
///     {
///         Ok(Vec::new())
///     }
///
///     async fn save_vote(
///         &mut self,
///         _vote: &openraft::Vote<u64>,
///     ) -> Result<(), openraft::StorageError<u64>> {
///         Ok(())
///     }
///
///     async fn read_vote(
///         &mut self,
///     ) -> Result<Option<openraft::Vote<u64>>, openraft::StorageError<u64>> {
///         Ok(None)
///     }
///
///     async fn begin_receiving_snapshot(
///         &mut self,
///     ) -> Result<Box<ChirpsTypeConfig::SnapshotData>, openraft::StorageError<u64>> {
///         Ok(Box::new(std::io::Cursor::new(Vec::new())))
///     }
///
///     async fn install_snapshot(
///         &mut self,
///         _meta: &openraft::SnapshotMeta<u64, openraft::BasicNode>,
///         _snapshot: Box<ChirpsTypeConfig::SnapshotData>,
///     ) -> Result<(), openraft::StorageError<u64>> {
///         Ok(())
///     }
///
///     async fn get_current_snapshot(
///         &mut self,
///     ) -> Result<Option<openraft::Snapshot<ChirpsTypeConfig>>, openraft::StorageError<u64>> {
///         Ok(None)
///     }
///
///     fn set_purgeable_horizon(&mut self, _horizon: Option<LogId<u64>>) {}
///
///     async fn get_log_reader(&mut self) -> Self::LogReader {}
///
///     async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {}
/// }
/// ```
#[async_trait]
pub trait RaftStorage<C: RaftTypeConfig>: Send + Sync + 'static {
    /// ログ読み出し用リーダー。
    type LogReader: RaftLogReader<C>;
    /// スナップショット構築用ビルダー。
    type SnapshotBuilder: RaftSnapshotBuilder<C>;

    /// ログ状態（last_purged_log_id, last_log_id）を取得する。
    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>>;

    /// 指定範囲のログエントリを取得する。indexベース。
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend;

    /// ログエントリを追記し、fsync完了時にコールバックを実行する。
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>)
    where
        I: IntoIterator<Item = C::Entry> + Send,
        I::IntoIter: Send;

    /// 指定log_id以降を削除する。
    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;

    /// 指定log_id以前をパージする。
    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;

    /// 適用済み状態を返す（last_applied_log, last_membership）。
    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<C::NodeId>>,
            StoredMembership<C::NodeId, C::Node>,
        ),
        StorageError<C::NodeId>,
    >;

    /// ステートマシンにエントリを適用し、応答を収集する。
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<C::R>, StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + Send,
        I::IntoIter: Send;

    /// Voteを永続化する。
    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;

    /// Voteを読み出す。
    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>>;

    /// スナップショット受信を開始する。
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<C::SnapshotData>, StorageError<C::NodeId>>;

    /// 受信済みスナップショットをインストールする。
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<C::NodeId, C::Node>,
        snapshot: Box<C::SnapshotData>,
    ) -> Result<(), StorageError<C::NodeId>>;

    /// 現在のスナップショットを取得する。
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<C>>, StorageError<C::NodeId>>;

    /// パージ可能な境界を設定する。
    fn set_purgeable_horizon(&mut self, horizon: Option<LogId<C::NodeId>>);

    /// ログリーダーを取得する。
    async fn get_log_reader(&mut self) -> Self::LogReader;

    /// スナップショットビルダーを取得する。
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        BasicNode, ChirpsTypeConfig, CommittedLeaderId, Entry, EntryPayload, Membership,
        SnapshotMeta,
    };
    use async_trait::async_trait;
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::Cursor;
    use std::ops::RangeBounds;

    #[derive(Default)]
    struct MockStateMachine;

    #[async_trait]
    impl StateMachine for MockStateMachine {
        type Command = Vec<u8>;
        type Response = Vec<u8>;

        async fn apply(
            &mut self,
            log_id: LogId<ChirpsNodeId>,
            command: Self::Command,
        ) -> StateMachineResult<Self::Response> {
            let _ = log_id;
            Ok(command)
        }

        async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
            Ok(Box::new(Cursor::new(Vec::new())))
        }

        async fn restore(
            &mut self,
            snapshot: Box<dyn AsyncSnapshotData>,
        ) -> StateMachineResult<()> {
            let _ = snapshot;
            Ok(())
        }
    }

    struct MockLogReader;

    impl RaftLogReader<ChirpsTypeConfig> for MockLogReader {
        async fn try_get_log_entries<RB>(
            &mut self,
            _range: RB,
        ) -> Result<Vec<Entry<ChirpsTypeConfig>>, StorageError<ChirpsNodeId>>
        where
            RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
        {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct MockSnapshotBuilder;

    impl RaftSnapshotBuilder<ChirpsTypeConfig> for MockSnapshotBuilder {
        async fn build_snapshot(
            &mut self,
        ) -> Result<Snapshot<ChirpsTypeConfig>, StorageError<ChirpsNodeId>> {
            Ok(Snapshot {
                meta: SnapshotMeta::default(),
                snapshot: Box::new(Cursor::new(Vec::new())),
            })
        }
    }

    #[derive(Clone, Default)]
    struct MockRaftStorage {
        last_log_id: Option<LogId<ChirpsNodeId>>,
        last_membership: StoredMembership<ChirpsNodeId, BasicNode>,
        vote: Option<Vote<ChirpsNodeId>>,
        purgeable_horizon: Option<LogId<ChirpsNodeId>>,
        snapshot: Option<Snapshot<ChirpsTypeConfig>>,
    }

    #[async_trait]
    impl RaftStorage<ChirpsTypeConfig> for MockRaftStorage {
        type LogReader = MockLogReader;
        type SnapshotBuilder = MockSnapshotBuilder;

        async fn get_log_state(
            &mut self,
        ) -> Result<LogState<ChirpsTypeConfig>, StorageError<ChirpsNodeId>> {
            Ok(LogState {
                last_purged_log_id: self.purgeable_horizon,
                last_log_id: self.last_log_id,
            })
        }

        async fn try_get_log_entries<RB>(
            &mut self,
            _range: RB,
        ) -> Result<Vec<Entry<ChirpsTypeConfig>>, StorageError<ChirpsNodeId>>
        where
            RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
        {
            Ok(Vec::new())
        }

        async fn append<I>(&mut self, entries: I, callback: LogFlushed<ChirpsTypeConfig>)
        where
            I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + Send,
        {
            let last = entries.into_iter().last();
            if let Some(entry) = last {
                self.last_log_id = Some(entry.log_id);
            }
            callback.log_io_completed(Ok(()));
        }

        async fn truncate(
            &mut self,
            log_id: LogId<ChirpsNodeId>,
        ) -> Result<(), StorageError<ChirpsNodeId>> {
            self.last_log_id = Some(log_id);
            Ok(())
        }

        async fn purge(
            &mut self,
            log_id: LogId<ChirpsNodeId>,
        ) -> Result<(), StorageError<ChirpsNodeId>> {
            self.purgeable_horizon = Some(log_id);
            self.last_log_id = Some(log_id);
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
            Ok((self.last_log_id, self.last_membership.clone()))
        }

        async fn apply<I>(&mut self, entries: I) -> Result<Vec<Vec<u8>>, StorageError<ChirpsNodeId>>
        where
            I: IntoIterator<Item = Entry<ChirpsTypeConfig>> + Send,
        {
            let mut responses = Vec::new();
            for entry in entries {
                let log_id = entry.log_id;
                self.last_log_id = Some(log_id);
                match entry.payload {
                    EntryPayload::Normal(cmd) => responses.push(cmd),
                    EntryPayload::Membership(m) => {
                        self.last_membership = StoredMembership::new(Some(log_id), m);
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
            self.vote = Some(*vote);
            Ok(())
        }

        async fn read_vote(
            &mut self,
        ) -> Result<Option<Vote<ChirpsNodeId>>, StorageError<ChirpsNodeId>> {
            Ok(self.vote)
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
            self.snapshot = Some(Snapshot {
                meta: meta.clone(),
                snapshot,
            });
            Ok(())
        }

        async fn get_current_snapshot(
            &mut self,
        ) -> Result<Option<Snapshot<ChirpsTypeConfig>>, StorageError<ChirpsNodeId>> {
            Ok(self.snapshot.clone())
        }

        fn set_purgeable_horizon(&mut self, horizon: Option<LogId<ChirpsNodeId>>) {
            self.purgeable_horizon = horizon;
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            MockLogReader
        }

        async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
            MockSnapshotBuilder
        }
    }

    #[tokio::test]
    async fn state_machine_trait_is_usable() {
        let mut sm = MockStateMachine;
        let log_id = LogId::new(CommittedLeaderId::new(1, 1), 1);
        let resp = sm.apply(log_id, b"ping".to_vec()).await.unwrap();
        assert_eq!(resp, b"ping".to_vec());

        let snapshot = sm.snapshot().await.unwrap();
        sm.restore(snapshot).await.unwrap();
    }

    #[tokio::test]
    async fn raft_storage_trait_is_implementable() {
        let mut storage = MockRaftStorage::default();

        let _ = storage.get_log_state().await.unwrap();
        let _ = storage.try_get_log_entries(0..1).await.unwrap();
        storage
            .truncate(LogId::new(CommittedLeaderId::new(1, 1), 0))
            .await
            .unwrap();
        storage
            .purge(LogId::new(CommittedLeaderId::new(1, 1), 0))
            .await
            .unwrap();
        let _ = storage.applied_state().await.unwrap();

        let entries = vec![Entry {
            log_id: LogId::new(CommittedLeaderId::new(2, 1), 1),
            payload: EntryPayload::Normal(b"data".to_vec()),
        }];
        let _ = storage.apply(entries).await.unwrap();

        storage.save_vote(&Vote::new(3, 1)).await.unwrap();
        let _ = storage.read_vote().await.unwrap();

        let _snapshot_data = storage.begin_receiving_snapshot().await.unwrap();
        let mut voters = BTreeSet::new();
        voters.insert(0);
        let mut nodes = BTreeMap::new();
        nodes.insert(
            0,
            BasicNode {
                addr: "127.0.0.1:0".to_string(),
            },
        );
        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(3, 1), 2)),
            last_membership: StoredMembership::new(None, Membership::new(vec![voters], nodes)),
            snapshot_id: "mock-snapshot".into(),
        };
        storage
            .install_snapshot(&meta, Box::new(Cursor::new(Vec::new())))
            .await
            .unwrap();
        let _ = storage.get_current_snapshot().await.unwrap();

        storage.set_purgeable_horizon(Some(LogId::new(CommittedLeaderId::new(0, 0), 0)));
        let _ = storage
            .get_log_reader()
            .await
            .try_get_log_entries(0..0)
            .await
            .unwrap();
        let mut builder = storage.get_snapshot_builder().await;
        let _ = builder.build_snapshot().await.unwrap();
    }
}
