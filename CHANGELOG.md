# Chirps リリースノート

## [0.5.2] - 2026-08-02

### 修正

- File Transfer の成功条件を受信側の完全ハッシュ検証、指定メタデータ適用、原子的配置完了後の `Complete` 応答まで拡張し、Move 元ファイルはこの確認後だけ削除するようにした。
- 実 QUIC 経路で Zstd 圧縮/展開、ストリーム障害再送、改変チャンクの checksum NACK→再送を検証・修正した。
- Pull とリモート側が新しい Bidirectional 同期を、別途 remote `send_file` を呼ばずに完了できる `SyncRequest` wire 契約へ拡張した。
- `FileTransferConfig` の chunk/concurrency/compression default とグローバル同時転送上限を実送信へ適用し、上限の統合テストを追加した。
- 公称する最大 chunk size に対し、短い read を完全 chunk と誤認しないよう `read_exact` で読み取る回帰テストを追加した。
- symlink 方針、`remove(ignore_not_found)`、Unix mode/mtime 保存を wire と受信処理まで接続した。
- Prometheus collector をサービス固有の `Registry` へ登録し、複数サービス作成時のグローバル二重登録を解消した。
- QUIC サーバー側にも `alopex` ALPN を設定し、seed bootstrap の TLS ハンドシェイク失敗を解消した。`trusted_cert_paths` による DER 信頼アンカーを追加し、証明書検証を維持したまま個別ノード証明書のメッシュを構成できるようにした。
- `send_queue_capacity` を実際の送信受付上限へ接続し、優先スケジューラを送信経路へ組み込んだ。
- SWIM の同一 incarnation における古い `Alive` gossip が `Suspect` の liveness を更新する不具合を修正した。
- Quinn 0.11 / Rustls 0.23 / Prometheus 0.14 へ更新し、旧 QUIC/TLS・protobuf・Windows CMake 経路に含まれていた既知脆弱性と Windows ビルド障害を解消した。`alopex` ALPN と証明書検証は新しい Quinn crypto adapter 上でも維持した。

### 検証

- marimo による File Transfer 検証ノートブックを v0.5.2 用へ更新した。
- QUIC 統合テストで、圧縮、NACK 再送、最終配置、Move、Pull、双方向同期、resume、メタデータ、並行数制限を確認した。
- 実 UDP/QUIC の統合テストで、相互信頼する個別自己署名証明書、ALPN、seed 再接続、優先送信、送信キュー飽和を確認した。3ノードの transport/gossip 構成でも参加・再参加と共有証明書の開発経路を確認した。公開 `Mesh::start` の永続 incarnation を伴う再参加は v0.6 の #6 で追跡する。
- RustSec 監査、全 workspace テスト、明示実行する実 UDP/QUIC テスト、3ノード mesh E2E、全 target・全 feature の Clippy を依存更新後に再実行した。

## [0.5.0] - 2025-12-04
### 追加
- `chirps-raft-storage`: openraft v0.9.17 互換の RaftStorage/StateMachine 抽象と WAL ベース実装 (`WalRaftStorage`)、CRC 付きレコードフォーマット、スナップショット管理ヘルパーを追加。
- `alopex-chirps`: RaftNode ラッパーと ChirpsRaftTransport、メンバーシップ変更 API、提案/クエリ API、一連のユニットテスト・統合テストを整備。
- 観測性: Prometheus メトリクスコレクタ (`RaftMetricsCollector`)、構造化 `tracing` イベントを追加し `/metrics` 連携を実装。
- ベンチマーク: 提案スループット・レイテンシ・リーダー選出・スナップショット性能を測る `raft_bench` を追加。
- ドキュメント: 公開 API 全体の Rustdoc を充実させ、利用者向けガイド `docs/raft-guide.md` を追加。

### 変更
- すべての Chirps クレートのバージョンを `0.5.0` に更新。
