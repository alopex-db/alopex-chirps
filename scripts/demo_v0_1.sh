#!/usr/bin/env bash
set -euo pipefail

# Chirps v0.1 の主要機能をまとめて確認するデモスクリプト。
# - simple-mesh 例で 3 ノードの send/broadcast とイベントログを確認
# - QUIC トランスポート統合テストで ping/ack・broadcast・再接続を検証

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export TMPDIR="${TMPDIR:-"$ROOT_DIR/target/tmp"}"
mkdir -p "$TMPDIR"

echo "== Chirps v0.1 デモを開始します =="
echo "プロジェクトルート: $ROOT_DIR"

echo ""
echo "== Step 1: simple-mesh 例で 3 ノードの基本動作を確認 =="
echo " - NodeId 永続化（temp dir）"
echo " - QUIC/TLS セッション確立"
echo " - send_to / broadcast とイベントログ出力"
cargo run --manifest-path "$ROOT_DIR/Cargo.toml" --example simple-mesh

echo ""
echo "== Step 2: QUIC 基本テスト (ping/ack, broadcast, 再接続) =="
cargo test --manifest-path "$ROOT_DIR/Cargo.toml" \
  -p chirps-transport-quic \
  --test quic_integration \
  -- ping_ack_roundtrip broadcast_delivers_to_connected_peers reconnects_when_seed_becomes_available

echo ""
echo "== 完了: Chirps v0.1 のデモが終了しました =="
