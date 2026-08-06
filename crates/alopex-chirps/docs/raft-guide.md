# Chirps Raft 使用ガイド

## 概要
- `alopex-chirps` の `raft` モジュールは openraft v0.9.17 をラップし、シンプルな Raft API を提供する。
- ストレージは `chirps-raft-storage` の `WalRaftStorage` を利用し、WAL とスナップショットで耐障害性を確保する。
- 既存の Chirps トランスポート (`MessageBackend` 実装) をそのまま再利用できる。

## 前提
- Rust 1.78+、`tokio` ランタイム。
- 依存クレート: `alopex-chirps`, `chirps-raft-storage`, `chirps-core` (MessageBackend 実装), `chirps-wire`。
- ディスクに書き込み可能な `wal/` と `snapshot/` ディレクトリ。

## クイックスタート
1. ステートマシンを実装する（コマンド/レスポンスは `Vec<u8>` 想定）
   ```rust,ignore
   use async_trait::async_trait;
   use chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
   use chirps_raft_storage::types::LogId;
   use tokio::io::Cursor;

   #[derive(Default)]
   struct KvStateMachine;

   #[async_trait]
   impl StateMachine for KvStateMachine {
       type Command = Vec<u8>;
       type Response = Vec<u8>;

       async fn apply(
           &mut self,
           _log_id: LogId<u64>,
           command: Self::Command,
       ) -> StateMachineResult<Self::Response> {
           // ここで任意の状態更新を行う
           Ok(command)
       }

       async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
           Ok(Box::new(Cursor::new(Vec::new())))
       }

       async fn restore(&mut self, _snapshot: Box<dyn AsyncSnapshotData>) -> StateMachineResult<()> {
           Ok(())
       }
   }
   ```

2. ストレージとトランスポートを組み立てる
   ```rust,ignore
   use alopex_chirps::raft::{RaftConfig, RaftNode};
   use alopex_chirps::raft::transport::ChirpsRaftTransport;
   use chirps_core::backend::MessageBackend;
   use chirps_raft_storage::types::GroupId;
   use std::sync::Arc;

   fn build_backend() -> Arc<dyn MessageBackend> {
       // QUIC など既存のバックエンドを返す
       unimplemented!()
   }

   async fn build_node(group_id: GroupId, node_id: u64) -> anyhow::Result<RaftNode> {
       let transport = Arc::new(ChirpsRaftTransport::new(
           build_backend(),
           group_id,
           node_id,
       ));
       let network = ChirpsRaftTransport::factory(transport.clone());
       let log_store = build_log_store(group_id, node_id)?; // RaftLogStorage<ChirpsTypeConfig> を返す
       let state_machine = build_state_machine(group_id, node_id)?; // RaftStateMachine<ChirpsTypeConfig> を返す

       let cfg = RaftConfig { group_id, node_id, ..Default::default() };
       let mut node = RaftNode::new(cfg, network, log_store, state_machine, transport).await?;
       node.start().await?;
       Ok(node)
   }
   ```
   - `WalRaftStorage` を利用する場合は、`RaftLogStorage` / `RaftStateMachine` として使えるアダプタを介して渡す。

3. クラスタを初期化し、提案を流す
   ```rust,ignore
   use std::collections::BTreeSet;
   use chirps_raft_storage::types::GroupId;

   async fn init_and_propose(mut leader: RaftNode) -> anyhow::Result<()> {
       let members = BTreeSet::from([1, 2, 3]);
       leader.initialize(members).await?;

       let response = leader.propose(b"hello".to_vec()).await?;
       println!("state machine returned: {:?}", response);
       Ok(())
   }
   ```

4. メンバーシップやスナップショットの操作
   ```rust,ignore
   leader.add_learner(4, openraft::BasicNode::default()).await?;
   leader.change_membership(BTreeSet::from([1, 2, 4])).await?;
   leader.trigger_snapshot().await?;
   ```

## 設定チューニング (`RaftConfig`)
- `election_timeout_ms` / `heartbeat_interval_ms`: レイテンシと安定性のバランスを調整。遅延が大きい環境では両方を引き上げる。
- `max_batch_size`: 提案をまとめてレプリケーションする最大件数。高スループットが欲しい場合は増やす。
- `snapshot_threshold` / `max_in_snapshot_log_to_keep`: ログが肥大化しないように適切な値に設定する。
- `group_id` / `node_id`: WAL の識別子にも使われるため、重複しないようにする。

## メトリクスとログ
- `RaftMetricsCollector` で Prometheus メトリクスを生成する。本番の `/metrics` ハンドラは bearer token を検証する `serve_metrics_authorized` を使い、TLS はサービスまたは ingress で終端する。外部で保護済みの環境では互換ラッパー `serve_metrics` も利用できる。
- 主要メトリクス: `chirps_raft_groups_total`, `chirps_raft_state`, `chirps_raft_term`, `chirps_raft_commit_index`, `chirps_raft_applied_index`, `chirps_raft_proposals_total`, `chirps_raft_proposals_latency_seconds`, `chirps_raft_messages_sent_total`, `chirps_raft_messages_received_total`, `chirps_raft_log_entries`, `chirps_raft_snapshot_total`, `chirps_raft_snapshot_size_bytes`。
- 全metric、認証、cardinality方針は [`../../../docs/observability/v0_6_metrics.md`](../../../docs/observability/v0_6_metrics.md)、旧`raft_*`名からの移行は [`../../../docs/migration/v0_5_to_v0_6.md`](../../../docs/migration/v0_5_to_v0_6.md) を参照する。
- 構造化ログイベント（`target="raft"`）:
  - `raft_initialized`, `raft_state_changed`, `raft_leader_elected`, `raft_membership_changed`, `raft_snapshot_created`, `raft_snapshot_installed`, `raft_log_compacted`, `raft_propose_failed`。

## スナップショットとWAL運用
- WAL 形式のバージョンは `chirps_raft_storage::wal_storage::CURRENT_FORMAT_VERSION` で確認できる。
- `WalStorageConfig` の `wal_dir` と `snapshot_dir` はノードごとに分けること。ディスク容量を監視し、古いスナップショットをクリーンアップする。
- クラッシュ後は `WalRaftStorage::recover` が自動でログ・Vote・スナップショットを復元する。ディレクトリが欠損している場合は手動で再作成する。

## トラブルシュート
- `NotLeader` エラー: `leader_id()` で返るリーダーにリダイレクトする。
- `MembershipChangeInProgress`: 直前の `change_membership` が完了してから再実行する。
- スナップショット復元失敗: `format_version` が一致しているか、`snapshot_dir` のパーミッションが正しいか確認。
- WAL 読み出しエラー: `wal_dir` の権限とファイル破損を確認し、必要ならバックアップから復元する。
- 送信タイムアウト: ネットワークの RTT と `election_timeout_ms` のバランスを見直す。トランスポートの接続数 (`connected_peers`) を確認する。
