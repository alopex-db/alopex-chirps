# v0.5.2 二ノード性能測定・証跡手順

この手順は、実機が今すぐ利用できないことを開発停止の理由にしないための測定契約です。ローカルの in-process fixture は診断値を提供しますが、物理二ノードのリリース根拠にはなりません。

## 測定対象と境界

| 証跡 | 検証するもの | リリース根拠 |
| --- | --- | --- |
| `run-local-v0_5_2-baseline.sh` | 同一ホストの QUIC chunk fixture と `MockNetwork` 制御面 | いいえ |
| `two-node-preflight.sh` | 物理二ホスト間の TCP 帯域、NIC・CPU・OS 情報 | 単独ではいいえ |
| `two_node_transfer` | 実 Chirps QUIC 制御面、実 QUIC chunk stream、128 MiB 転送、原本/宛先 SHA-256 一致 | はい（他の v0.5.2 gate と併用） |

`MeshHandle` から `FileTransferService` を構築する公開 API は v0.5.2 にありません。従ってこのハーネスは `FileTransferServiceImpl::new` に公開 `QuicBackend` と `ChunkStreamOpener` を渡します。これはその API 境界を隠さずに、FileTransfer の直接サービス契約を別プロセス・別物理ホストで検証するためです。`MeshHandle` 統合は v0.6 の作業であり、本測定の成功をその実装済みの証明にはしません。

## 実機の前提

- 同一 L2 の Linux 二ホスト（sender / receiver）。双方の測定 NIC は 1 Gbit/s 以上、同じ MTU、直接到達可能な IP アドレスを使う。NAT は許可しない。
- sender / receiver とも同じ Git SHA、同じ Rust toolchain、`iperf3`、`openssl`、十分な空き容量を用意する。測定用 payload は各ホストのローカル SSD に置く。
- UDP で control と data のポートを、TCP で iperf3 のポートを双方向に許可する。例では UDP `6201`, `6202`, `6301`, `6302` と TCP `5201` を使う。
- controller は SSH で両ホストへ接続でき、両ホストの checkout の作業ツリーが clean であることを確認する。TLS 秘密鍵・payload・Node ID を GitHub artifact に含めない。

測定前に両ホストで SHA を確認します。

```bash
git -C /srv/chirps rev-parse HEAD
git -C /srv/chirps status --short
```

controller の SHA と一致しない、または未コミット変更がある host は測定対象にしません。

## ローカル暫定ベースライン

これは現在の ignored performance fixture を隔離した `CARGO_TARGET_DIR` で実行してログを残します。実行後にその target は削除されます。

```bash
scripts/perf/run-local-v0_5_2-baseline.sh \
  --output /var/tmp/chirps-v0_5_2-local-baseline
```

この結果の `scope` は `single-host-loopback; chunk=data QUIC; control=MockNetwork` です。`100 MB/s` assertion の結果を記録しますが、`release_evidence=false` のままです。

## 二ノード実機測定

controller から次を実行します。アドレスは実際に相手から観測される NIC の IP:port を指定します。`*-bind` は各ホストが bind するアドレス、`*-address` は対向ホストが接続・送信元検証に使うアドレスです。

```bash
scripts/perf/run-two-node-file-transfer.sh \
  --sender perf-sender.example.internal \
  --receiver perf-receiver.example.internal \
  --remote-workdir /srv/chirps \
  --remote-output-root /var/tmp/chirps-performance \
  --output /var/tmp/chirps-v0_5_2-two-node \
  --sender-control-bind 10.0.10.11:6201 \
  --sender-control-address 10.0.10.11:6201 \
  --receiver-control-bind 10.0.10.12:6202 \
  --receiver-control-address 10.0.10.12:6202 \
  --sender-data-bind 10.0.10.11:6301 \
  --sender-data-address 10.0.10.11:6301 \
  --receiver-data-bind 10.0.10.12:6302 \
  --receiver-data-address 10.0.10.12:6302 \
  --receiver-iperf-address 10.0.10.12 \
  --file-bytes 134217728
```

スクリプトの順序は次のとおりです。

1. `iperf3` 30 秒測定を実行し、sender/receiver の host facts と JSON を取得する。
2. `alopex.local` 用の 2 日間だけ有効な test-only DER 証明書を controller 上で生成し、両ホストへコピーする。鍵は実行完了・失敗・中断時に controller と両ホストから削除する。
3. receiver を先に起動する。receiver は atomically finalized destination のサイズと SHA-256 を報告して終了する。
4. sender が 128 MiB（既定値）の payload を送る。計測時間は `send_file` 呼出直前から receiver の検証済み `Complete` を受けるまでであり、source manifest hash と receiver final hash を含む。
5. controller が sender/receiver の result、iperf3 JSON、host facts のみを収集し、`manifest.sha256` 付き bundle を生成する。payload と秘密鍵は取り込まない。

測定対象の FileTransfer は compression `none`、1 MiB chunk、concurrency 4 です。これは v0.5.2 の帯域目標の比較可能な基準です。Zstd の送受信圧縮・復元は既存の機能テストで別途検証し、圧縮率で帯域結果を見かけ上増やしません。

## 判定と証跡

`output/evidence/result.json` と `summary.md` が判定の正本です。`release_eligible=true` になるのは、すべて満たす場合だけです。

- sender 側 iperf3 が `900,000,000 bit/s` 以上。
- 二ノード FileTransfer の end-to-end throughput が `100,000,000 B/s` 以上。
- sender / receiver の SHA-256、Git SHA、測定スコープが一致。
- control plane が `chirps-quic`、data plane が `quic-chunk-stream`、双方の `completed=true`。

`release_eligible=false` は測定資料を無効にせず、「まだリリース性能 gate を通過していない」という正しい記録です。性能以外の workspace test、clippy、QUIC/mesh E2E、公開前確認も別途必要です。

破損 chunk の復旧については、現在の実 QUIC corruption-proxy integration test が wire-level NACK/retry を検証します。物理二ホストハーネスは正常経路の帯域測定専用であり、UDP packet loss とアプリケーション chunk payload 改竄を同一視しません。物理ネットワーク上の payload 改竄を加える三ホスト relay 試験は、v0.5.2 の未証明事項として Issue #1 に残し、正常系の `release_eligible` がそれを完了扱いにすることはありません。

## GitHub Actions へのアップロード

専用 controller runner に次の repository variables を設定します。値は管理ホスト名・パスだけとし、SSH 認証情報は runner の既存 credential または Actions secret で扱います。

- `CHIRPS_PERF_SENDER_HOST`
- `CHIRPS_PERF_RECEIVER_HOST`
- `CHIRPS_PERF_REMOTE_WORKDIR`
- `CHIRPS_PERF_REMOTE_OUTPUT_ROOT`
- `CHIRPS_PERF_SENDER_CONTROL_BIND`, `CHIRPS_PERF_SENDER_CONTROL_ADDRESS`
- `CHIRPS_PERF_RECEIVER_CONTROL_BIND`, `CHIRPS_PERF_RECEIVER_CONTROL_ADDRESS`
- `CHIRPS_PERF_SENDER_DATA_BIND`, `CHIRPS_PERF_SENDER_DATA_ADDRESS`
- `CHIRPS_PERF_RECEIVER_DATA_BIND`, `CHIRPS_PERF_RECEIVER_DATA_ADDRESS`
- 任意: `CHIRPS_PERF_RECEIVER_IPERF_ADDRESS`

GitHub の **Actions → Two-node FileTransfer performance evidence → Run workflow** を実行します。workflow は `chirps-1gbps-controller` ラベルの runner 上で controller script を実行し、常に `two-node-performance-evidence` artifact を 90 日間保存します。artifact の `manifest.sha256` を再計算してから、Issue #1 の release checklist へ run URL、commit SHA、`summary.md` の値を記録します。

公開タグ、crates.io publish、GitHub Release 作成はこの手順では行いません。
