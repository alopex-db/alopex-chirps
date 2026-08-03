# v0.5.2 FileTransfer 性能・配備証跡契約

この契約は、**Chirps 製品の性能**と**実ネットワークへ配備できること**を別の問いとして扱う。`iperf3` の結果、家庭内 LAN の帯域、WSL の仮想ネットワーク、ディスク性能を、Chirps の実装性能へ読み替えてはならない。

| evidence class | 問い | 正本になる環境 | release performance の根拠 |
| --- | --- | --- | --- |
| `product-controlled-container` | 固定された 1 Gbit/s profile で FileTransfer 実装が 100 MB/s を満たすか | 同一 Linux host の隔離された sender / receiver container | はい |
| `deployment-two-host` | 実際の二 host、QUIC/UDP、TLS、routing で完全転送できるか | 直接到達可能な二 host | 配備互換性のみ。製品 throughput の根拠ではない |
| `local-component` | 最小の QUIC/FileTransfer 実装が回帰していないか | 同一 process / local fixture | いいえ |

`two_node_transfer` は公開 `QuicBackend` と `ChunkStreamOpener` を使う。これは `MeshHandle` からの公開構築 API がない v0.5.2 の直接サービス契約を検証するためであり、v0.6 の `MeshHandle` 統合を実装済みと主張しない。

## 1. Product performance: controlled two-container profile `ft-1g-v1`

### 固定する境界

- native Linux x86_64 の calibration host 上で実行する。Docker Desktop、WSL、実 LAN、VPN、host network mode はこの profile の対象外である。
- 同一の immutable image digest と source Git SHA から起動する sender / receiver の **二 container** を使う。image build、image pull、Cargo build は計測区間に含めない。
- run ごとに user-defined Docker bridge を作る。Docker default bridge、公開 port、外部 network 接続を使わない。MTU は `1500` として evidence に記録する。
- sender と receiver に重複しない `cpuset-cpus`、同じ memory limit、128 MiB 以上の独立した tmpfs を与える。payload は sender tmpfs 内で生成し、receiver tmpfs に atomic install する。host filesystem、共有 bind mount、page-cache の差を測定値へ混ぜない。
- sender/receiver 間の各方向の veth に traffic control を設定し、rate `1gbit`、delay `1ms`、jitter `0ms`、loss `0%` に固定する。qdisc 設定と統計を evidence に保存する。`iperf3` の実測値でこの profile を定義しない。
- 測定前に一回の warm-up を行う。計測は fresh process pair で 5 回行い、各回で新しい payload と QUIC endpoint を作る。source SHA、image digest、CPU model、kernel、Docker version、cpuset、memory limit、tmpfs、bridge、MTU、qdisc を記録する。

Docker の user-defined bridge は同一 Docker host 上の container を隔離して接続し、MTU も network option として明示できる。[Docker Bridge network driver](https://docs.docker.com/engine/network/drivers/bridge/)  Traffic control の `netem` は delay/loss を、`tbf` は rate を設定するための Linux の qdisc である。[tc-netem(8)](https://man7.org/linux/man-pages/man8/tc-netem.8.html) [tc-tbf(8)](https://man7.org/linux/man-pages/man8/tc-tbf.8.html)

### 測定する Chirps の契約

固定パラメータは compression `none`、file size `134217728` bytes、chunk size `1048576` bytes、concurrency `4`、performance transport profile（stream receive window `16 MiB`、connection/send window `64 MiB`、max uni streams `256`）である。compression の機能充足・復元・破損時 retry は local integration / model evidence で別に判定し、圧縮率で goodput を水増ししない。

計測区間は sender が `send_file` を呼ぶ直前から、receiver が SHA-256 検証・metadata 適用・atomic rename を完了し、sender が対応する `Complete` を検証するまでとする。各 sample で次を出力する。

- `end_to_end_goodput_bytes_per_second = 134217728 / elapsed_seconds`
- payload progress throughput、wire bytes（利用可能な場合）、retry 数、chunk size、concurrency
- sender / receiver の CPU time・cgroup throttle/OOM 状態
- sender/receiver の SHA-256、`completed`、source SHA、image digest、profile ID

`ft-1g-v1` の **製品性能受入条件**は、5 sample 全てで SHA-256 と source SHA/image digest が一致し、`completed=true` であり、各 sample の end-to-end goodput が `100,000,000 B/s` 以上であることとする。profile が変わる場合（rate、delay、MTU、CPU allocation、payload、chunk/concurrency、transport window）は新しい profile ID を作り、既存値との比較を同一系列として扱わない。

この値は 1 Gbit/s に成形した network の理論上限を製品目標そのものと取り違えない。`100 MB/s` はこの明示的 profile における Chirps FileTransfer の end-to-end SLO であり、実 LAN の最大帯域や `iperf3` の合格値ではない。

### 実装状態と実行方法

現時点の ignored `file_transfer_loopback_reports_diagnostic` は同一 process の `MockNetwork` control plane であり、`ft-1g-v1` の証跡ではない。`two_node_transfer` は `two-container-controlled` scope に profile ID と image digest を必須とし、異なる scope ではそれらを拒否する。

`scripts/perf/run-controlled-container-file-transfer.sh` は、clean な Git SHA を `git archive` して image を作り、internal user-defined bridge、sender/receiver の別 cpuset と tmpfs、双方の `tbf` + `netem` を作成・削除する。warm-up 後、fresh process pair 5 回の report、宛先 hash、container inspect、qdisc、cgroup CPU/memory 統計、manifest を `--output` に保存する。鍵と payload は temporary container/tmpfs にだけ置き、artifact へコピーしない。

```bash
scripts/perf/run-controlled-container-file-transfer.sh \
  --output /var/tmp/chirps-ft-1g-v1
```

`evidence/result.json` の `product_performance_passed=true` はこの profile の SLO と integrity が通ったことだけを表す。これは v0.5.2 全体の `release_eligible` ではない。retry/wire-byte の sample 指標と、release contract verifier が profile/evidence の欠落を拒否する実装は、引き続き未完了要件として追跡する。

実行器は `host_platform`、`host_platform_eligible`、`swap_limit_enforced`、`profile_environment_eligible` を evidence に記録する。`product_performance_passed=true` には、数値・integrity・identity に加え `profile_environment_eligible=true` が必須である。従って WSL/Docker Desktop や、Docker が memory/no-swap を実効的に強制できない host での結果は、実装診断には利用できても `ft-1g-v1` の正本にはならない。

この harness は開発者と calibration runner が **local-first** で実行する。CI は要件を発見する場所ではなく、承認済みの harness と evidence schema を再実行・検証するだけである。

### 性能原因を最終試験へ持ち越さないための層別 harness

`ft-1g-v1` の不合格だけから特定の関数・QUIC・disk が遅いと結論してはならない。一方、最終 binary/container 試験で初めて速度低下を知る構成も不十分である。v0.5.2 では #25 により、同じ 128 MiB / compression `none` / 1 MiB chunk / concurrency 4 の workload を次の層へ分解する。

| 層 | local-first harness | 通常 test で決定的に確認すること | calibration で記録する値 |
| --- | --- | --- | --- |
| function | manifest/hash、chunk read、compression、`ChunkStreamCodec`、receiver write/finalize | 正しい byte 数・checksum、境界、無効 frame 拒否 | operation ごとの bytes/s・allocation/operation count |
| module | sender scheduler、connection reuse、ACK/NACK/retry、receiver session | `concurrency` 上限、全 chunk の一回限り completion、retry と in-flight accounting | chunk read / encode / stream / verify / write の bytes/s と duration |
| service | `FileTransferService` と実 QUIC data plane の local diagnostic | phase observation の全項目、retry/byte/integrity の集計一致 | source prepare、control、chunk pipeline、receiver finalize の各 duration |
| binary | `two_node_transfer` / controlled two-container | source/image/profile/scope と SHA-256 の一致 | 上記 phase 集計と end-to-end goodput |

通常の `cargo test` に host 固有の B/s 閾値を置かない。代わりに byte accounting、phase event count、connection/retry/concurrency の構造的不変条件を検証する。function/module の速度比較は `cargo bench` の Criterion evidence として local calibration host に保存し、前回の同一 profile と比較する。最終 `ft-1g-v1` の `100,000,000 B/s` だけが release SLO であり、下位 benchmark は原因帰属と回帰検知のための入力である。

```bash
CARGO_TARGET_DIR=/var/tmp/chirps-ft-component-bench \
  cargo bench -p alopex-chirps-file-transfer --bench file_transfer_components
```

この bench は source manifest/hash（128 MiB）、実際の source open + 1 MiB chunk read、`compression=none`、接続再利用済み QUIC codec round trip を測定する。結果は同一 host / kernel / Rust / profile の前回値とだけ比較し、`ft-1g-v1` の release 判定値に代用しない。

## 2. Deployment compatibility: two-host diagnostic

二 host の試験は、Chirps が物理経路で機能することを確認する。host、NIC、家庭内 LAN、WSL、VPN、firewall、disk の性能を含むため、`ft-1g-v1` の数値と比較・合算してはならない。

### `PATH-UDP-100` preflight

Chirps を起動する前に、各方向で次を一回実行する。

```bash
iperf3 -c <peer-data-ip> -p 5201 -u -b 100M -t 15 -J --get-server-output
```

この diagnostic の合格条件は、両方向で意図した direct peer IP に接続し、offered load `100 Mbit/s`、duration `15 s`、datagram loss `0%` を JSON から確認できることとする。これは **100 Mbit/s を送ったときに UDP が届く** という到達性の確認であり、link capacity、Chirps goodput、1 Gbit/s の可否は評価しない。失敗した場合は Chirps の再ビルドを繰り返さず、route、firewall、WSL/host steering、LAN policy を調査対象として記録する。

### 二 host FileTransfer

preflight 後、同一 source SHA の sender/receiver で 128 MiB、compression `none` の実 FileTransfer を行う。受入条件は `completed=true`、sender/receiver/destination の SHA-256 一致、`chirps-quic` control plane、`quic-chunk-stream` data plane、設定・route・host facts を含む秘密情報なしの artifact である。goodput、CPU、NIC、RTT は **観測値**として保存するが、product performance の閾値を適用しない。

現在の WSL-to-NucBox WSL 試行（UDP loss 0%、TCP 約 915 Mbit/s、FileTransfer 46.89 MB/s）はこの evidence class の診断結果であり、Chirps の product performance 合否には使わない。

## 3. 証跡と release 判定

| 判定 | 必須 evidence | release への扱い |
| --- | --- | --- |
| product performance | `ft-1g-v1` の 5 sample、全 integrity、profile/image/source/cgroup/qdisc manifest | `100 MB/s` SLO の正本 |
| deployment compatibility | `PATH-UDP-100` 双方向 JSON、二 host FileTransfer integrity artifact | 配備経路の正常性確認。SLO の代替不可 |
| recovery / compression | model と local integration の requirement/property 対応 | performance artifact の代替不可 |

TLS private key、payload 本体、Node ID、SSH credential はいずれの artifact にも含めない。physical diagnostic が未実施でも無関係な次の実装を止めないが、該当環境での deployment evidence を「確認済み」とは主張しない。`ft-1g-v1` が未実装・未測定の間は v0.5.2 の product performance は `未証明` のままである。

公開タグ、crates.io publish、GitHub Release 作成はこの契約の実行では行わない。
