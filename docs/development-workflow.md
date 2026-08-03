# 開発・リリース品質 workflow

目的は「テストがあること」ではなく、公開後に発見される実装漏れを公開前の明示的な未証明事項として捕捉することです。機能実装、検証、公開可否を同じ人の暗黙の判断に混ぜません。

## 1. 実装開始前: 要件を検証可能な失敗モードへ分解する

1. Issue と roadmap の各作業項目を、利用者が観測する失敗モードに書き換える。
2. `docs/release/acceptance-template.md` から対象版の受入契約を作成する。
3. 各行に「実装箇所」「unit / integration / multi-process / physical host のどれで証明するか」「証跡」を割り当てる。
4. API、wire format、設定、永続化、圧縮、障害復旧、性能、observability、互換性を影響レビューに必ず含める。

要件に対してテスト方法が決まらない場合は、実装済み扱いにせず `未証明・除外事項` に Issue / milestone とともに残します。後で検証するという予定だけでは release readiness を上げられません。

## 2. 実装中: 先に RED と契約テストを置く

- バグは再現テストを先に追加し、production code を変える前に RED を確認する。
- E2E で初めて見つかった不具合は、対応する unit または component viewpoint も追加する。
- mock、loopback、二プロセス、物理 host の検証範囲を報告と受入契約で区別する。弱い環境の成功を強い環境の成功として扱わない。
- 機能追加・設定追加・wire 変換追加は、設定値が実際の送受信経路に影響する test を必須にする。

## 3. PR / release branch: 実装者と検証者を分離する

PR template の要件対応表、未証明事項、実行したコマンドと artifact を埋めます。実装者以外の検証者は、少なくとも次を独立に確認します。

- 受入契約の全要件に対応するテストまたは証跡があること。
- テストが実際に主張している範囲と、release note / roadmap の主張が一致すること。
- 未証明事項が `READY` や「完了」に誤って変換されていないこと。

release/* branch は通常 CI に加えて、実 QUIC・mesh の ignored acceptance、FileTransfer acceptance、受入契約の構造検査を実行します。これによりタグ作成後ではなく release candidate の段階で漏れを発見します。

## 4. 公開前: evidence を伴う手動承認

1. release captain は対象 tag と commit SHA を受入契約へ固定する。
2. `scripts/verify-release-contract.sh --version X.Y.Z --require-ready` を通す。`BLOCKED`、`未証明`、`TODO` は公開停止条件である。
3. 対象版の CI、package/dry-run、必要な実機・性能・障害復旧 evidence を確認する。
4. 実装者以外の検証者と release captain が evidence artifact / CI run URL を契約に記録する。
5. `Release` workflow を明示的に dispatch し、production environment protection の承認後に publish する。

tag push は公開開始のトリガーにしません。タグを作る行為と crates.io / GitHub Release への配布は別の承認境界です。

## 5. 公開後に漏れが見つかった場合

- まず影響、再現、利用者への回避策を Issue に記録する。
- 欠けていた受入契約の行と最小の regression test を追加する。
- 同じ種類の機能（設定、wire、復旧、性能など）を横断検索し、代表例だけで完了にしない。
- release readiness を再評価し、必要なら次 patch release の契約と evidence を新たに作る。

この振り返りは個人の注意力に依存させず、template、CI、release workflow の変更まで完了して初めて閉じます。
