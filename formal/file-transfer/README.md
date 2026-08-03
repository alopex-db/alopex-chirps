# Chirps FileTransfer model

このTLA+モデルは、v0.5.2 FileTransfer の局所的な安全性を、実装前・ローカルで検査するpilotである。自然言語の
roadmapを置き換えず、`catalog.yaml` で requirement、model property、production code、component testを対応付ける。

## ローカル実行

Docker Composeが必要である。イメージはApalache公式イメージの固定digestであり、ローカルにない場合も同じdigestだけを取得する。networkが利用できない環境では、明示的な取得失敗として止まる。

```bash
cd formal/file-transfer
docker compose run --rm apalache typecheck FileTransfer.tla
docker compose run --rm apalache check --config=FileTransfer.cfg --length=12 FileTransfer.tla
```

`check` は `RetryLimit = 2`、最大12遷移の有限モデルにおける全実行を検査する。成功は、無限の実行、実際のNIC性能、
複数物理host、実ネットワーク上のpayload改竄耐性を証明しない。

## 検査する性質

- sender payloadのencodingとwire metadataは一致する。
- receiverはadvertiseされたencoding以外としてpayloadをdecodeしない。
- installとCompleteはvalid hashの後だけに起きる。
- 一度でもcorruptionが起きた成功転送には、少なくとも一度のretryが必要である。

実装変更では、まずこのmodel/propertyと対応するcomponent testをローカルで実行する。物理二ノード性能と実ネットワーク
payload改竄は別evidenceであり、このmodelの成功から満たされたと主張してはならない。
