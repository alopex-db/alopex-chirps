# vX.Y.Z リリース受入契約

Release readiness: BLOCKED

この文書は、ロードマップ・Issue・実装・検証・証跡を同じ release candidate に固定するための契約です。実装前に作成し、要求を追加・変更した PR では同じ PR で更新します。`未証明`、`BLOCKED`、`TODO` が残る限り `READY` にしてはなりません。

## 要件・検証対応表

| 要件 / 失敗モード | ロードマップ / Issue | 実装箇所 | 独立した検証 | 実行結果・証跡 | 状態 |
| --- | --- | --- | --- | --- | --- |
| 例: chunk 圧縮が送受信経路で復元される | #123 | `crates/...` | unit + real transport integration | CI run URL / artifact | 未証明 |

各行は「機能がある」ではなく、壊れた場合に利用者が観測する失敗モードで書きます。mock-only、loopback-only、手作業確認だけの場合は、その制約を証跡欄に明記します。

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
