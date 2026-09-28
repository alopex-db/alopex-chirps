# Alopex Chirps

**A lightweight, secure gossip and messaging mesh built on UDP/QUIC — inspired by the Arctic fox.**

Alopex Chirps is the communication and membership layer for distributed systems. Designed after the fast, light, and adaptive communication patterns of the Arctic fox, Chirps provides node discovery, gossip-based membership, and low-latency messaging over QUIC.

Chirps can be used as the control-plane foundation for:

* distributed databases
* distributed runtimes and virtual machines
* microservices and service meshes
* edge and IoT clusters

It is completely independent from AlopexDB or jjvm — those are *users* of Chirps, not dependencies.

> **Release status:** v0.7.0 is under release-candidate validation and is not published until the v0.7.0 tag and attested artifacts exist. The existing QUIC mesh remains the default; Durable messaging is opt-in.

## Durable messaging（v0.7）

`durable-iggy` featureは、既存QUIC meshとは独立したpoint-to-point Durable planeを追加します。利用者は`DurableBuilder`で明示構成し、send、subscription creation、delivery、local checkpointの境界をtyped resultとして個別に扱います。Durableが未構成またはunavailableでも、別profileやQUIC sendへ暗黙fallbackしません。

v0.7の保証はretention内に限定されます。strong profileの`OsSyncedAccepted`はcompatible serverのmessage/index OS-sync境界を示しますが、device power-loss、replication、delivery、exactly-once application effectは示しません。response loss後の明示retryやeffect完了後・checkpoint前のcrashではduplicate deliveryが起こり得ます。

公開契約、supported matrix、identity horizon、caller責務、およびpoint-to-point例は[Durable messaging profile](docs/durable-profile.md)を参照してください。bounded metrics、health、event correlationの非正本契約は[Durable observability guide](docs/durable-observability.md)を参照してください。Durable broadcast、unlimited replay、global/cross-partition order、sender auto-recovery、HA/failover、cross-host rebalanceはv0.7の対象外です。

---

## Features (v0.1)

* **UDP/QUIC-based transport** using Rust-native QUIC implementation
* **Node identity** with persistent `node_id`
* **Gossip (SWIM-style)** membership: `alive / suspect / dead`
* **Cluster join without DNS** via static seed list
* **Secure-by-default** through QUIC/TLS
* **Lightweight messaging API**:

  * `send_to(node_id, payload)`
  * `broadcast(payload)`
  * `subscribe(handler)`
* **Event hooks** for `on_node_join`, `on_node_leave`, `on_status_change`

---

## クイックスタート（ローカル3ノード）

1. リポジトリ直下から `cd chirps`
2. 自己署名TLSで3ノードをローカル起動するサンプルを実行

```bash
cargo run --example simple-mesh
```

このサンプルは以下を行います。

- 127.0.0.1 上で3ノードを起動し、Node A をシードに Node B/C が接続
- イベントハンドラで join/leave/status_change をログ出力
- `broadcast` で全ピアへメッセージ送信
- `send_to` で特定ピアへ直接メッセージ送信

`examples/simple-mesh.rs` を参照すれば、`start` / `broadcast` / `send_to` / イベント購読の使い方を最小コードで確認できます。

### TLS の運用

`simple-mesh` はローカル開発専用に、1組の自己署名 DER 証明書と秘密鍵を3ノードへ明示的に渡します。本番では各ノードに個別の証明書と秘密鍵を設定し、`NodeConfig::trusted_cert_paths` にクラスタ CA 証明書、または許可する自己署名ピア証明書（DER）を指定してください。検証は常に有効であり、未知の証明書を受け入れる設定はありません。
