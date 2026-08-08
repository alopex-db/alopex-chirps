# 共有WAL実験（不採用）

## 条件

- 実行環境: 本リポジトリのDocker/loopback 3ノード相当
- `run-controlled-container-multi-raft.sh --resource-audit --shared-wal`
- Multi-Raft 100 groups / single-group 1 group、各5サンプル、各60秒、500us netem
- 出力: `/tmp/chirps-shared-wal-2026-08-08c`

## 結果

| lane | throughput samples (/s) | errors | timeouts |
|---|---|---:|---:|
| Multi-Raft | 38.2, 60.3, 42.0, 63.0, 29.1 | 0, 0, 0, 0, 2 | 2,590, 1,725, 2,426, 1,497, 2,942 |
| single-group | 210.8, 210.8, 209.7, 209.7, 213.8 | 0 | 0 |

Multi-Raftはverifierの`loadgen report contains errors or timeouts`で不合格。データ整合性ゲート以前に負荷処理が成立していないため、性能比較の合格値として扱わない。

## 診断と判断

共有ファイルを単一`BufWriter<File>` + Mutexで実装したため、グループ間のappendが直列化された。TiKVのRaft Engineは共有ログ形式だけでなく、BatchSystem、並行書き込み、グループ単位のログ管理を持つため、この単純化は参考実装の性能特性を再現していない。

この実験のコードは `af488fa` で撤回済み。共有WALをリリース版へ有効化しない。残すべき改善は、既存のdetachable durability diagnosticsで特定した個別WAL同期ファンアウトの次段設計であり、共有Mutexを追加することではない。
