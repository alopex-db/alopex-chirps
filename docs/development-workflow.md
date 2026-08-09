# 開発・リリース品質 workflow

目的は、要件の漏れを **実装着手前** に発見し、実装の回帰を **ローカル** で短く検出することです。CI は既に選んだ検証を再実行する防波堤であり、roadmap の要件を発見する場ではありません。この運用に spec-workflow は用いません。

## 1. 実装開始前: 要件をモデルと検証に割り当てる

1. Issue と roadmap の各作業項目を、利用者が観測する失敗モードへ分解する。
2. 各失敗モードを「純粋関数」「局所状態」「並行・分散状態」「物理・性能」に分類する。
3. API、wire format、設定、永続化、圧縮、障害復旧、性能、observability、互換性を横断して影響を確認する。
4. 並行・再試行・復旧・wire 順序を含む項目には、`formal/<capability>/` に状態モデル、checker 設定、requirement/property/test 対応表を置く。FileTransfer の最初の例は [`formal/file-transfer`](../formal/file-transfer/) である。
5. 各項目に、production の refinement 箇所と最小の local unit/component test を先に割り当てる。物理・性能は別の手順と evidence 形式もこの時点で決める。

モデルにできない単純な局所ロジックを形式化で遅らせません。一方、状態遷移を含む要件を自然言語と E2E だけで済ませることもしません。検証方法が決まらない要求は実装完了にせず、Issue / milestone とともに `未証明・除外事項` として残します。

## 2. 実装中: local-first で RED から証明する

- バグは production code を変える前に、最も狭い再現 test を RED にする。
- E2E で初めて検出した不具合には、対応する unit/component test と、該当すればモデル上の失敗遷移を追加する。
- 設定・wire 変換は、設定値が実際の送受信経路まで伝播し復元される local test を必須にする。
- mock、loopback、二プロセス、物理 host の結果を区別する。弱い環境の成功を実機の成功として扱わない。
- 性能 requirement は、先に profile ID・container/process 境界・CPU/memory/disk・network shaping・payload・sample 集計・測定区間・artifact schema を固定する。`iperf3` は経路の到達性/上限を観測する preflight であり、アプリケーション throughput の代理値にしない。
- 製品 throughput は固定した隔離 container profile で local-first に測る。二物理 host は QUIC 配備互換性を確認する別 evidence とし、家庭内 LAN、VPN、WSL、NIC の値を製品 SLO の合否へ混ぜない。
- 性能バグを性能測定で初めて発見しない。並行性、backpressure、再試行、leader churn、memory growth などの既知の失敗モードは、測定前に unit/property/component test で RED→GREEN にする。性能ハーネス自身（loadgen、queue、buffer、RSS、CPU、timeout、drop/error counters）もUTで上限と再現性を検証する。

実行順は model check（該当時）→ unit → crate component → integration → CI/OS → 物理 evidence である。前の段階で失敗したとき、後段だけを再試行して原因を曖昧にしない。

## 3. PR: 要件と実装を独立に照合する

PR 本文には、要件・モデル property・実装箇所・ローカル検証・未証明事項を記録します。実装者以外の検証者は、少なくとも次を確認します。

- roadmap / Issue の全項目が対応表に一行ずつあり、例外も明示されていること。
- property と local test が、実装の同じ失敗モードを実際に観測していること。
- テストの到達範囲と release note / roadmap の主張が一致すること。
- 未証明事項が `READY` や「完了」に誤って変換されていないこと。

通常 CI は format、lint、既存の unit/integration を再実行します。release branch 専用に重い acceptance を追加して要件漏れを探す運用は採りません。

## 4. 公開前: 既存 evidence を固定して確認する

1. release captain は対象 tag と commit SHA を受入契約へ固定する。
2. `scripts/verify-release-contract.sh --version X.Y.Z --require-ready` で、`BLOCKED`、`未証明`、`TODO` がないことを確認する。
3. 対象版の CI、package/dry-run、必要な実機・性能・障害復旧 evidence を確認する。
4. 実装者以外の検証者と release captain が evidence artifact / CI run URL を契約に記録する。
5. `Release` workflow を明示的に dispatch し、production environment protection の承認後に publish する。

### 4.1 v0.7.0以降の性能測定・リリース判定ポリシー

v0.7.0以降の全リリースは、次の順序と証跡を必須とする。この手順を省略した性能値、公開値の転載、CIだけの合格は release-ready の根拠にしない。

1. **環境固有の対照値を先に実測する。** TiKV等の参照プロジェクトの公開値は絶対閾値にしない。同一 workload、payload、ノード数、CPU/memory、storage、network shaping、client配置、warmup/measure/drain区間、error条件で対象環境の対照値を測定し、差分・不確実性・未再現条件をartifactへ記録する。異なる指標（例: YCSB UPDATE OPSとRaft committed proposals/s）は同一視せず、対応関係を別途定義する。
2. **UT/component gateを性能測定より前に通す。** 各性能シナリオの失敗モードを最小テストで再現し、並行、再試行、復旧、wire、backpressure、queue、RSS、timeoutを検証する。性能測定は既知の不具合を探す場所ではなく、修正済み実装の測定と回帰検出に限定する。
3. **全featureと対象版のlocal gateを先に通す。** `cargo test --locked --workspace --all-features`、対象crateのignored/acceptance test、docs、fmt、clippy、deterministic replay、package/dry-run、registry-only dependency検証を、GitHub Actions dispatch前に同じcommitで実行する。CIで初めて要件漏れ・UT不足・公開順序不備を発見してはならない。
4. **測定範囲を正確に主張する。** 物理3ノードがない場合はloopback、container、論理3-voterなどの範囲を明記し、物理3ノード性能・実ネットワーク障害・外部Prometheus deploymentの証明に昇格させない。性能artifactにはsample数、中央値/CI、errors/timeouts、RTT、RSS/queue、host条件、再生コマンドを固定する。
5. **公開ゲートを冪等にする。** crate公開順は依存DAGに従い、既公開versionは検証付きでskipする。部分公開時はtag SHAとversionを照合して再開し、別commitへのtag再利用や直接`cargo publish`によるenvironment承認の迂回は禁止する。
6. **公開後に直接照合する。** workflow成功だけで完了扱いにせず、tag SHA、GitHub Release、各registryのversion/source、target-version gate artifactを直接確認する。確認結果をrelease契約と対応Issueへコメントし、受入条件を満たしたIssueだけをクローズする。未証明・対象外・残課題は次版Issueへ明示的に移す。

対象版のゲートは公開前に再実行するが、それは commit / package の同一性を確認するためであり、要件設計の代替ではありません。tag push は公開開始のトリガーにせず、タグと crates.io / GitHub Release への配布を別の承認境界にします。

## 5. 公開後に漏れが見つかった場合

- まず影響、再現、利用者への回避策を Issue に記録する。
- 欠けていた対応表の行、状態モデルの遷移（該当時）、最小の local regression test を追加する。
- 同じ種類の機能（設定、wire、復旧、性能など）を横断検索し、代表例だけで完了にしない。
- 次版の受入契約と evidence を更新して release readiness を再評価する。

この振り返りは個人の注意力に依存させず、対応表・モデル・local test・レビュー導線のいずれが欠けたかを直して初めて閉じます。
