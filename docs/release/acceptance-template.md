# vX.Y.Z リリース受入契約

Release readiness: BLOCKED

この文書は、ロードマップ・Issue・実装・検証・証跡を同じ release candidate に固定するための契約です。実装前に作成し、要求を追加・変更した PR では同じ PR で更新します。`未証明`、`BLOCKED`、`TODO` が残る限り `READY` にしてはなりません。

## 要件・検証対応表

| 要件 / 失敗モード | ロードマップ / Issue | モデル property / 対象外根拠 | 実装箇所 | local 検証 | 実機・外部 evidence | 状態 |
| --- | --- | --- | --- | --- | --- | --- |
| 例: chunk 圧縮が送受信経路で復元される | #123 | `WireMetadataMatchesPayload`; 物理性能は対象外 | `crates/...` | component test のコマンドと結果 | artifact URL / 手順 | 未証明 |

各行は「機能がある」ではなく、壊れた場合に利用者が観測する失敗モードで書きます。並行・分散要件には model property を、対象外には理由を必ず記します。mock-only、loopback-only、手作業確認だけの場合は、その制約を evidence 欄に明記します。

## 未証明・除外事項

| 項目 | なぜこの版で証明できないか | 追跡 Issue / milestone | 公開可否への影響 |
| --- | --- | --- | --- |
| 例: 物理 network の payload 改竄復旧 | 三ホスト relay が未配備 | #124 / vX.Y+1 | BLOCKED |

「後で確認する」は除外理由になりません。公開を妨げない項目は、仕様上のスコープ外である根拠と次版の Issue を併記します。

## 変更影響レビュー

- 公開 API / wire format / 永続化 / 設定 / observability への影響:
- 既存テストで覆えない経路:
- 互換性・移行・rollback:
- セキュリティ・性能・実ネットワークの確認:

## 承認

- 実装者:
- 検証者（実装者以外）:
- release captain:
- 対象 commit SHA:
- Evidence artifact / CI run URL:
