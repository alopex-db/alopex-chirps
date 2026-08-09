## 要件と検証の対応

| 利用者に見える失敗モード | 分類（pure / local state / distributed / physical） | model property または対象外根拠 | 実装箇所 | 実行した local 検証 |
| --- | --- | --- | --- | --- |
| | | | | |

- [ ] roadmap / Issue の全作業項目を表に対応付けた。
- [ ] wire、設定、永続化、互換性、障害復旧、observability、性能への影響を確認した。
- [ ] distributed の変更は `formal/<capability>/catalog.yaml` と状態モデルを追加・更新した。該当しない場合は表に理由を書いた。
- [ ] E2E でのみ検出した事象には最小の local regression test を追加した。

## 未証明事項・外部 evidence

物理 host、NIC、外部サービスなど local で証明できない項目は、手順・evidence の保存先・追跡 Issue を記す。未証明事項を完了として扱わない。

## レビュー依頼

- [ ] 実装者以外が、要件→property→実装→local 検証の対応を確認する。
