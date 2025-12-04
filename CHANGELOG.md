# Chirps リリースノート

## [0.5.0] - 2025-12-04
### 追加
- `chirps-raft-storage`: openraft v0.9.17 互換の RaftStorage/StateMachine 抽象と WAL ベース実装 (`WalRaftStorage`)、CRC 付きレコードフォーマット、スナップショット管理ヘルパーを追加。
- `alopex-chirps`: RaftNode ラッパーと ChirpsRaftTransport、メンバーシップ変更 API、提案/クエリ API、一連のユニットテスト・統合テストを整備。
- 観測性: Prometheus メトリクスコレクタ (`RaftMetricsCollector`)、構造化 `tracing` イベントを追加し `/metrics` 連携を実装。
- ベンチマーク: 提案スループット・レイテンシ・リーダー選出・スナップショット性能を測る `raft_bench` を追加。
- ドキュメント: 公開 API 全体の Rustdoc を充実させ、利用者向けガイド `docs/raft-guide.md` を追加。

### 変更
- すべての Chirps クレートのバージョンを `0.5.0` に更新。
