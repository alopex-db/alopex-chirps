# Chirps リリース手順書

## 概要

このドキュメントは Alopex Chirps (Distributed Cluster Coordination) のリリース手順を説明します。

## バージョン管理

### Workspace 継承

全クレートのバージョンは `Cargo.toml` の `[workspace.package]` で一元管理されています。

```toml
[workspace.package]
version = "0.5.2"  # ← ここを変更すると全クレートに反映
```

### クレート一覧

| クレート | 説明 | 依存関係 |
|---------|------|---------|
| `alopex-chirps-wire` | ワイヤープロトコル型定義 | なし（最初に公開） |
| `alopex-chirps-core` | コアトレイト・型 | alopex-chirps-wire |
| `alopex-chirps-gossip-swim` | SWIM ゴシッププロトコル | alopex-chirps-wire |
| `alopex-chirps-raft-storage` | Raft ログストレージ | alopex-core |
| `alopex-chirps-mock` | テスト用モック実装 | alopex-chirps-wire, alopex-chirps-core |
| `alopex-chirps-file-transfer` | ファイル転送 API | alopex-chirps-wire, alopex-chirps-core |
| `alopex-chirps-transport-quic` | QUIC トランスポート層 | alopex-chirps-wire, alopex-chirps-core |
| `alopex-chirps` | メインライブラリ | 全上記クレート |

### 公開順序

公開順序を workflow に列挙しません。`scripts/publish-crates.sh` が対象 commit の
workspace manifest から publish 可能な crate と内部依存 DAG を読み、トポロジカルな
層へ分けてから公開します。循環依存、存在しない workspace member、publish=false
crateへの依存があれば、最初の publish 前に失敗します。

## v0.7.0以降の必須ポリシー

v0.7.0以降は、`docs/development-workflow.md`の「性能測定・リリース判定ポリシー」をこの手順書の必須ゲートとして扱います。

- TiKV等の公開ベンチマーク値を絶対閾値にしない。同一環境・同一条件の対照値を先に実測し、workload、payload、ノード数、CPU/memory、storage、network、client配置、warmup/measure/drain、error条件と不確実性をartifactへ固定する。
- 性能測定前に、性能ハーネスを含むUT/property/component testで既知のバグ（leader churn、backpressure、queue、retry、timeout、memory/RSS増大など）を再現して修正する。性能テストをUTの代替にしない。
- GitHub Actions前に、対象版の全feature workspace test、ignored/acceptance test、docs、fmt、clippy、deterministic replay、package/dry-run、registry-only dependency検証を同一commitでローカル実行する。
- 物理3ノードがない環境の結果は、loopback/container/論理voterの範囲でのみ主張する。物理3ノード、実ネットワーク障害、外部Prometheus deploymentの証明とは扱わない。
- 公開順は依存DAGに従う。`alopex-chirps-transport-quic`を`alopex-chirps-file-transfer`より先に公開する。部分公開時は既存tagのSHA一致を確認して再開し、既公開crateは検証付きでskipする。
- 公開後はtag SHA、GitHub Release、全registry version/source、target-version evidenceを直接照合し、結果をvX.Y.Z受入契約とmilestone Issueへコメントする。受入条件を満たしたIssueだけをクローズする。

## リリースワークフロー

### タグ形式

```
chirps-v{major}.{minor}.{patch}
```

例: `chirps-v0.5.2`

### 自動化される処理

タグの push は配布を開始しません。release captain が既存の**注釈付きタグ**を指定し、`Release` workflow を手動 dispatch した場合だけ、GitHub Actions が以下を実行します。

1. **CI Gate**: tag と Cargo.toml の版一致、受入契約が `READY` であること、fmt、clippy、全 workspace test、doc test、File Transfer 受入テスト、実 QUIC/mesh E2E を検証
2. **Version-bound evidence Gate**: `docs/release/evidence/vX.Y.Z/required-evidence.json` がある版だけ、同カタログが指定する target gate runner を実行し、manifest と SHA-256 を検証する。カタログがない版はその旨をログへ明示し、evidence artifact があるのにカタログがない場合は fail closed する
3. **Publish Crate**: 保護された GitHub `release` environment の承認後に crates.io へ依存順で公開
4. **Create Release**: 同 environment の承認後に GitHub Release を作成

`workflow_dispatch` の run は dispatch 時点の workflow 定義を保持します。`release` environment
の承認待ちが 5 日以上になった古い run は承認してはいけません。承認前にその run を
**cancel**し、修正済みの workflow で同じ入力を使って新しい run を dispatch し直してください。
古い run は environment ゲート通過直後にも検査され、公開処理を開始せず失敗します。

controller runner・二台の測定 host・必要な repository variables が未登録、または evidence が不適格な場合、公開ジョブは開始されない。通常の CI、同一プロセス fixture、ローカル二プロセス実行でこの要件を代替してはならない。構成と artifact の確認方法は [二ノード性能測定・証跡手順](docs/v0_5_2_two_node_performance.md) を参照する。

対象版には `docs/release/vX.Y.Z.md` の受入契約が必須です。要件、実装、独立検証、artifact、未証明事項、実装者以外の検証者、release captain を記録し、未証明・`BLOCKED`・`TODO` が一つでもあれば公開できません。作成方法は [開発・リリース品質 workflow](docs/development-workflow.md) と [受入契約テンプレート](docs/release/acceptance-template.md) を参照してください。

## リリース手順

### 1. 事前確認

```bash
cd /path/to/alopex-db/chirps

# ビルド確認
cargo check --workspace

# テスト実行
cargo test --workspace

# clippy チェック
cargo clippy --all-targets --all-features -- -D warnings

# 対象 commit の依存 DAG と公開層を確認（公開はしない）
bash scripts/publish-crates.sh --repo-root . --plan-only

# 必要なら各層の package dry-run を、上の plan 順で実行する
```

### 2. バージョン更新

`Cargo.toml` の workspace バージョンを更新：

```bash
vim Cargo.toml
```

```toml
[workspace.package]
version = "X.Y.Z"  # 新しいバージョン
```

### 3. CHANGELOG 更新（推奨）

```bash
vim CHANGELOG.md
```

### 4. コミット

```bash
git add Cargo.toml CHANGELOG.md
git commit -m "chore: bump chirps version to X.Y.Z"
```

### 5. release branch と受入契約の確認

```bash
git switch -c release/vX.Y.Z
cp docs/release/acceptance-template.md docs/release/vX.Y.Z.md
# 要件、テスト、evidence、未証明事項、承認を記入する
git add docs/release/vX.Y.Z.md
git commit -m "docs: add X.Y.Z release acceptance contract"
git push origin release/vX.Y.Z
```

`release/v*` への push と pull request では通常 CI に加え、受入契約の構造検査、FileTransfer acceptance、実 QUIC、三ノード mesh acceptance が実行されます。実装者以外の検証者が、受入契約の主張と CI/artifact の範囲を確認してください。

### 6. 公開可否の固定

以下を満たしたら、受入契約を `READY` に変更して commit SHA、CI run URL、artifact URL、検証者、release captain を記入します。

- [ ] 対象版の release branch CI が成功している
- [ ] 必要な package dry-run と物理環境の evidence が記録されている
- [ ] `scripts/verify-release-contract.sh --version X.Y.Z --require-ready` が成功する
- [ ] 保護された `release` environment に release captain とは別の必須 reviewer が設定されている

### 7. タグ作成、push、明示的な publish 承認

```bash
# READY の commit に注釈付きタグを作成
git tag -a chirps-vX.Y.Z -m "Release chirps vX.Y.Z"
git push origin chirps-vX.Y.Z

# 公開を明示的に開始する。tag push だけでは publish されない。
gh workflow run Release --ref main \
  -f tag=chirps-vX.Y.Z \
  -f commit="$(git rev-parse HEAD)" \
  -f confirm_publish=true
```

workflow の `release` environment 承認では、受入契約の commit SHA、CI、evidence artifact を再確認してください。`--ref main` は workflow/tooling の revision、`commit` は検証・tag・package の対象 revision です。両者を意図的に指定し、ユーザーから公開許可を得ていない場合、この手順を実行してはいけません。

承認待ちが長引き、dispatch から 5 日以上経過した場合は、古い run を承認せずに **cancel
して dispatch し直す**こと。承認前に workflow 定義と受入契約を再確認し、新しい run の
environment ゲートで承認してください。

### 8. リリース確認

- [ ] GitHub Actions の Release ワークフローが成功
- [ ] GitHub Releases にリリースノートが作成されている
- [ ] crates.io に各クレートが公開されている
  - https://crates.io/crates/alopex-chirps-wire
  - https://crates.io/crates/alopex-chirps-core
  - https://crates.io/crates/alopex-chirps-gossip-swim
  - https://crates.io/crates/alopex-chirps-raft-storage
  - https://crates.io/crates/alopex-chirps-mock
  - https://crates.io/crates/alopex-chirps-transport-quic
  - https://crates.io/crates/alopex-chirps

## 緊急時の扱い

workflow 障害があっても、`cargo publish` を直接実行して CI、受入契約、environment approval を迂回してはいけません。原因を Issue に記録し、workflow を修復して同じ受入契約・tag を手動 dispatch します。すでに一部のクレートが公開された場合は、公開済みの crate、version、tag SHA、未公開 crate を明記して次の修復版の受入契約を作成します。

## トラブルシューティング

### "no matching package named `alopex-chirps-wire` found"

原因: `alopex-chirps-wire` がまだ crates.io にない状態で依存クレートを公開しようとした

対処:
1. `alopex-chirps-wire` を先に公開
2. 30秒待機（crates.io index 更新）
3. 依存クレートを公開

### "crate version already exists"

原因: 同じバージョンが既に公開済み

対処: バージョン番号を上げて再リリース

### CI Gate 失敗

原因: fmt、clippy、test、coverage、security audit、または release branch acceptance のいずれかが失敗

対処:
```bash
# ローカルで修正
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace

# 修正をコミット & プッシュ
git add -A
git commit -m "fix: resolve CI issues"
git push origin release/vX.Y.Z
```

tag は受入契約を固定する不変の参照です。公開前に修正が必要なら新しい commit と注釈付きタグを作成し、古い tag を削除・付け替えないでください。

### Windows テストの失敗

Windows でタイミング依存のテストが失敗する場合があります：

- `ping_timeout_requests_indirect_and_marks_suspect`: タイマー解像度の問題
- QUIC integration tests: ネットワーク環境依存

これらは `#[cfg_attr(windows, ignore)]` で CI では無視されています。

## 依存関係の更新

alopex-chirps-raft-storage は `alopex-core` に依存しています。alopex-core がアップデートされた場合：

1. `crates/chirps-raft-storage/Cargo.toml` の alopex-core バージョンを更新
2. `cargo update` で Cargo.lock を更新
3. テストを実行して互換性を確認
4. 新しいバージョンとしてリリース

```bash
# alopex-core 更新
vim crates/chirps-raft-storage/Cargo.toml  # alopex-core = "0.4" など

# Cargo.lock 更新
cargo update

# テスト
cargo test --workspace
```

## MSRV (Minimum Supported Rust Version)

- 現在の Edition: **2024** (Rust 1.82+)
- alopex-core の MSRV に依存

## 関連ドキュメント

- [GitHub Actions ワークフロー](.github/workflows/release.yml)
- [CI ワークフロー](.github/workflows/ci.yml)
- [Pre-commit フック設定](scripts/setup-hooks.sh)

## 変更履歴

| 日付 | バージョン | 変更内容 |
|------|-----------|---------|
| 2025-12-18 | - | クレート名を `chirps-*` → `alopex-chirps-*` に変更 |
| 2024-12-18 | - | リリース手順書作成、メタデータ追加 |
