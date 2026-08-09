# Harness audit

- 実行コマンド: `scripts/perf/run-controlled-container-multi-raft.sh --output <empty-dir> --resource-audit`
- 固定順: `multi_raft:0, single_group:0, single_group:1, multi_raft:1, multi_raft:2, single_group:2, single_group:3, multi_raft:3, multi_raft:4, single_group:4`
- 1クライアントのpayload working set、RSS境界、phase-metrics、CPU/RSS/fsync/networkを既存の任意監査経路で採取。
- 新規実装は応答送信の有限 semaphore のみ。詳細計測のための本体常駐スレッド、無制限ログ、全proposalのコピーは追加していない。
- 現HEAD測定は Multi-Raft-0 の strict failure 後、swap増加を避けるため中断。保存済み `summaries.ndjson` は実行済みサンプルの全フィールドを保持する。
- 失敗の一次証拠は `summaries.ndjson`、phase別資源証拠は `phase-metrics.ndjson`、ビルド再現性は `image-build.log`。
