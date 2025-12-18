# Chirps リリース手順書

## 概要

このドキュメントは Alopex Chirps (Distributed Cluster Coordination) のリリース手順を説明します。

## バージョン管理

### Workspace 継承

全クレートのバージョンは `Cargo.toml` の `[workspace.package]` で一元管理されています。

```toml
[workspace.package]
version = "0.5.0"  # ← ここを変更すると全クレートに反映
```

### クレート一覧

| クレート | 説明 | 依存関係 |
|---------|------|---------|
| `alopex-chirps-wire` | ワイヤープロトコル型定義 | なし（最初に公開） |
| `alopex-chirps-core` | コアトレイト・型 | alopex-chirps-wire |
| `alopex-chirps-gossip-swim` | SWIM ゴシッププロトコル | alopex-chirps-wire |
| `alopex-chirps-raft-storage` | Raft ログストレージ | alopex-core |
| `alopex-chirps-mock` | テスト用モック実装 | alopex-chirps-wire, alopex-chirps-core |
| `alopex-chirps-transport-quic` | QUIC トランスポート層 | alopex-chirps-wire, alopex-chirps-core |
| `alopex-chirps` | メインライブラリ | 全上記クレート |

### 公開順序

依存関係により、以下の順序で公開する必要があります：

```
Layer 1: alopex-chirps-wire, alopex-chirps-raft-storage
    ↓
Layer 2: alopex-chirps-core, alopex-chirps-gossip-swim
    ↓
Layer 3: alopex-chirps-mock, alopex-chirps-transport-quic
    ↓
Layer 4: alopex-chirps
```

## リリースワークフロー

### タグ形式

```
chirps-v{major}.{minor}.{patch}
```

例: `chirps-v0.5.0`

### 自動化される処理

タグをプッシュすると、GitHub Actions が以下を自動実行します：

1. **CI Gate**: fmt, clippy, test の実行
2. **Publish Crate**: crates.io への公開（依存順）
3. **Create Release**: GitHub Release の作成

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

# dry-run で公開可能か確認（各クレート）
cargo publish --dry-run -p alopex-chirps-wire
cargo publish --dry-run -p alopex-chirps-core
cargo publish --dry-run -p alopex-chirps
```

### 2. バージョン更新

`Cargo.toml` の workspace バージョンを更新：

```bash
vim Cargo.toml
```

```toml
[workspace.package]
version = "0.6.0"  # 新しいバージョン
```

### 3. CHANGELOG 更新（推奨）

```bash
vim CHANGELOG.md
```

### 4. コミット

```bash
git add Cargo.toml CHANGELOG.md
git commit -m "chore: bump chirps version to 0.6.0"
```

### 5. プッシュ & CI 確認

```bash
git push origin main
```

GitHub Actions の CI が成功することを確認してください。

### 6. タグ作成 & プッシュ

```bash
# タグ作成
git tag -a chirps-v0.6.0 -m "Release chirps v0.6.0"

# タグをプッシュ（リリースワークフロー発火）
git push origin chirps-v0.6.0
```

### 7. リリース確認

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

## 手動リリース（緊急時）

自動リリースが失敗した場合の手動手順：

```bash
cd /path/to/alopex-db/chirps

# 1. alopex-chirps-wire を公開
cargo publish -p alopex-chirps-wire
sleep 30

# 2. alopex-chirps-raft-storage を公開（alopex-core 依存のため並行可能）
cargo publish -p alopex-chirps-raft-storage
sleep 30

# 3. alopex-chirps-core を公開
cargo publish -p alopex-chirps-core
sleep 30

# 4. alopex-chirps-gossip-swim を公開
cargo publish -p alopex-chirps-gossip-swim
sleep 30

# 5. alopex-chirps-mock を公開
cargo publish -p alopex-chirps-mock
sleep 30

# 6. alopex-chirps-transport-quic を公開
cargo publish -p alopex-chirps-transport-quic
sleep 30

# 7. alopex-chirps を公開
cargo publish -p alopex-chirps
```

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

原因: fmt, clippy, test のいずれかが失敗

対処:
```bash
# ローカルで修正
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace

# 修正をコミット & プッシュ
git add -A
git commit -m "fix: resolve CI issues"
git push origin main

# 既存タグを削除して再作成（必要な場合）
git tag -d chirps-v0.6.0
git push origin :refs/tags/chirps-v0.6.0
git tag -a chirps-v0.6.0 -m "Release chirps v0.6.0"
git push origin chirps-v0.6.0
```

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
