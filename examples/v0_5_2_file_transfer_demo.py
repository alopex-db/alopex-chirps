# /// script
# requires-python = ">=3.11"
# dependencies = ["marimo>=0.23,<0.24"]
# ///

"""Interactive verification demo for Chirps v0.5.2 file transfer.

Run with:

    uv run marimo edit examples/v0_5_2_file_transfer_demo.py

Each run compiles into a disposable CARGO_TARGET_DIR outside the repository.
"""

import marimo

__generated_with = "0.23.16"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path
    import os
    import subprocess
    import tempfile

    import marimo as mo

    return Path, mo, os, subprocess, tempfile


@app.cell
def _(Path):
    repo_root = Path(__file__).resolve().parents[1]
    manifest_path = repo_root / "crates" / "chirps-file-transfer" / "Cargo.toml"
    return manifest_path, repo_root


@app.cell
def _(mo):
    mo.md(
        """
        # Chirps v0.5.2 File Transfer — verification demo

        このノートブックは、v0.5.2 の File Transfer 信頼性修正を
        実際の統合テストで動かして確認します。制御メッセージには `MockBackend`、
        チャンク本体にはローカル QUIC ストリームを使うため、ファイル転送のプロトコル、
        整合性検証、再送、永続化をネットワーク越しに検証できます。

        実行ごとに一時 `CARGO_TARGET_DIR` を作成・削除するため、リポジトリに
        ビルド成果物を残しません。
        """
    )
    return


@app.cell
def _():
    scenarios = {
        "send": {
            "label": "1対1送信・チャンク整合性",
            "filter": "send_file_transfers_small_file",
            "proves": "64 KiB チャンクを使った送信、受信ファイルの完全一致、完了進捗",
        },
        "retry": {
            "label": "ストリーム障害からの再送",
            "filter": "send_file_retries_on_stream_failure",
            "proves": "最初のチャンクストリーム失敗後に再試行し、受信ファイルを復元",
        },
        "compression": {
            "label": "Zstd 圧縮転送",
            "filter": "send_file_transfers_zstd_compressed_file",
            "proves": "Zstd 圧縮されたチャンクを転送し、受信側で展開して元ファイルと一致",
        },
        "corruption": {
            "label": "破損チャンクからの復旧",
            "filter": "send_file_retries_after_corrupted_chunk",
            "proves": "最初の QUIC フレームを改変し、checksum NACK 後の再送と完全復旧を確認",
        },
        "broadcast": {
            "label": "1対Nブロードキャスト",
            "filter": "broadcast_file_all_success",
            "proves": "送信元から 2 ノードへ同一ファイルを転送し、各ノードの完了状態を確認",
        },
        "sync": {
            "label": "Push 同期",
            "filter": "sync_push_transfers_to_remote",
            "proves": "SyncOptions::Push によるリモートへの同期と完了進捗",
        },
        "pull": {
            "label": "単独 Pull 同期",
            "filter": "sync_pull_receives_from_remote",
            "proves": "remote 側で send_file を別途起動せず、SyncRequest により Pull を完了",
        },
        "bidirectional": {
            "label": "新しい側優先の双方向同期",
            "filter": "sync_bidirectional_pulls_remote_newer_without_manual_send",
            "proves": "remote が新しい場合に local を原子的に置換する Bidirectional 同期",
        },
        "limit": {
            "label": "同時転送上限",
            "filter": "service_applies_max_concurrent_transfer_limit",
            "proves": "max_concurrent_transfers=1 で二本目の送信 stream が待機すること",
        },
        "metadata": {
            "label": "Unix メタデータ保存",
            "filter": "send_file_preserves_unix_permissions_and_modified_time",
            "proves": "receiver の最終配置前に mode と modified time を保存すること",
        },
        "metrics": {
            "label": "サービス固有 Metrics Registry",
            "filter": "each_service_exposes_its_own_metrics_registry",
            "proves": "複数サービスで Prometheus collector の二重グローバル登録が起きないこと",
        },
        "resume": {
            "label": "キャンセル後のレジューム",
            "filter": "resume_transfer_restores_progress",
            "proves": "部分転送をキャンセルして永続化し、別サービス起動後に再開して完全一致",
        },
        "file_ops": {
            "label": "リモートファイル操作",
            "filter": "file_ops_round_trip",
            "proves": "exists / metadata / list_files / remove の往復操作",
        },
        "all": {
            "label": "全シナリオ（時間がかかります）",
            "filter": None,
            "proves": "登録済みの v0.5.2 File Transfer 統合テストをすべて実行（性能テストは除外）",
        },
    }
    return (scenarios,)


@app.cell
def _(mo, scenarios):
    selector = mo.ui.dropdown(
        options={scenario["label"]: key for key, scenario in scenarios.items()},
        value="send",
        label="検証シナリオ",
    )
    run_button = mo.ui.run_button(label="選択シナリオを実行")
    mo.vstack([selector, run_button])
    return run_button, selector


@app.cell
def _(manifest_path, mo, os, repo_root, run_button, scenarios, selector, subprocess, tempfile):
    if not run_button.value:
        mo.md("シナリオを選択して **選択シナリオを実行** を押してください。")
        return

    selected = scenarios[selector.value]
    command = [
        "cargo",
        "test",
        "--manifest-path",
        str(manifest_path),
        "--test",
        "file_transfer",
    ]
    if selected["filter"]:
        command.append(selected["filter"])
    command.extend(["--", "--nocapture"])

    environment = os.environ.copy()
    with tempfile.TemporaryDirectory(prefix="chirps-v0_5_2-demo-") as target_dir:
        environment["CARGO_TARGET_DIR"] = target_dir
        try:
            completed = subprocess.run(
                command,
                cwd=repo_root,
                env=environment,
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
            output = f"{completed.stdout}\n{completed.stderr}".strip()
            exit_code = completed.returncode
        except subprocess.TimeoutExpired as error:
            output = f"タイムアウト（300 秒）:\n{error}"
            exit_code = 124

    result = "✅ 成功" if exit_code == 0 else f"❌ 失敗（exit {exit_code}）"
    output_tail = output[-12_000:] or "（Cargo からの出力はありません）"
    mo.vstack(
        [
            mo.md(
                f"## {result}\n\n"
                f"**検証内容:** {selected['proves']}\n\n"
                f"`{' '.join(command)}`\n\n"
                "一時 Cargo target は実行完了時に削除済みです。"
            ),
            mo.md(f"```text\n{output_tail}\n```"),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## ロードマップとの対応

        | ロードマップ項目 | デモで使う証拠 |
        | --- | --- |
        | Core types / wire message / FileTransfer stream | `file_transfer` 統合テストの実通信 |
        | 1対1送信・チャンク分割・整合性検証・帯域制御 | 1対1送信シナリオ、再送シナリオ |
        | Zstd 圧縮・圧縮レベル指定 | Zstd / ZstdLevel 往復テスト、wire payload 圧縮ユニットテスト |
        | ネットワーク障害・破損チャンクからの復旧 | ストリーム障害再送、改変した QUIC フレームの NACK→再送 |
        | 1対N ブロードキャスト | 1対Nブロードキャストシナリオ |
        | Push / 単独 Pull / 双方向同期 | Push・単独 Pull・新しい側優先の双方向同期シナリオ |
        | セッション永続化・レジューム | キャンセル後のレジュームシナリオ |
        | exists / remove / metadata / list_files | リモートファイル操作シナリオ（`ignore_not_found` を含む） |
        | Unix mode / mtime 保存 | Unix メタデータ保存シナリオ |
        | 設定 default / 同時転送上限 | default の単体テストと、service の同時転送上限シナリオ |
        | Prometheus メトリクス | サービス固有 Registry の複数インスタンス検証 |

        ## 残るリリース判定上の注意

        - 100 MB/s の性能目標は、`file_transfer_throughput_meets_v0_5_2_target` として
          登録されていますが、1 Gbps 専用ランナーでのみ実行する ignored test です。
          通常 CI やこのローカルデモの成功を、性能達成の証跡としては扱いません。
        - このデモは実 QUIC チャンクストリームと MockBackend の制御面を組み合わせます。
          本番の Mesh 起動・相互 TLS 配備を通した環境受入試験は、別の E2E 環境が必要です。

        したがって、コアのファイル転送 API はロードマップ上の主要操作を実証できますが、
        仕様全体を「本番投入可能」と判断するための証跡はまだ不足しています。
        """
    )
    return


if __name__ == "__main__":
    app.run()
