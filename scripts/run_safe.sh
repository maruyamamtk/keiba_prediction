#!/usr/bin/env bash
# run_safe.sh — 任意のコマンドをタイムアウト付きで実行する保険ラッパー
#
# 目的:
#   長時間コマンド（バックテスト・最適化・デプロイ等）が無限ループや応答待ちで
#   ハングしてもセッションを止めないよう、強制的に時間上限を設ける。
#   ※ これは「コマンド自体のハング」への保険であり、ツール呼び出しの記述ミス等
#     モデル側の出力問題は対象外。
#
# 使い方:
#   scripts/run_safe.sh [--timeout SECONDS] -- <command> [args...]
#   例:
#     scripts/run_safe.sh --timeout 1800 -- .venv/bin/pytest tests/
#     scripts/run_safe.sh -- ./infrastructure/scripts/deploy_cloud_run.sh
#
# 終了コード:
#   124 = タイムアウトで強制終了（GNU timeout 準拠）
#   その他 = ラップしたコマンドの終了コードをそのまま返す

set -uo pipefail

TIMEOUT_SECONDS=1800  # デフォルト30分

# --- 引数パース ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --timeout)
      TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "[run_safe] 不明な引数: $1" >&2
      echo "使い方: scripts/run_safe.sh [--timeout SECONDS] -- <command> [args...]" >&2
      exit 2
      ;;
  esac
done

if [[ $# -eq 0 ]]; then
  echo "[run_safe] 実行するコマンドが指定されていません" >&2
  echo "使い方: scripts/run_safe.sh [--timeout SECONDS] -- <command> [args...]" >&2
  exit 2
fi

# --- timeout コマンドの解決（macOS は coreutils の gtimeout の場合あり）---
TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_BIN="gtimeout"
fi

START_TS=$(date +%s)
echo "[run_safe] 実行: $* (timeout=${TIMEOUT_SECONDS}s)" >&2

if [[ -n "$TIMEOUT_BIN" ]]; then
  # --kill-after で SIGTERM 無視時に SIGKILL を送る
  "$TIMEOUT_BIN" --kill-after=10s "${TIMEOUT_SECONDS}s" "$@"
  STATUS=$?
else
  # timeout が無い環境向けフォールバック（バックグラウンド実行 + 監視）
  echo "[run_safe] timeout コマンドが見つからないためフォールバック監視を使用" >&2
  "$@" &
  CMD_PID=$!
  TIMED_OUT_FLAG="$(mktemp)"
  ( sleep "$TIMEOUT_SECONDS"; echo 1 > "$TIMED_OUT_FLAG"; kill -TERM "$CMD_PID" 2>/dev/null; sleep 10; kill -KILL "$CMD_PID" 2>/dev/null ) &
  WATCHDOG_PID=$!
  wait "$CMD_PID"
  STATUS=$?
  kill -TERM "$WATCHDOG_PID" 2>/dev/null
  # ウォッチドッグが発火していれば GNU timeout 準拠で 124 に正規化
  if [[ -s "$TIMED_OUT_FLAG" ]]; then
    STATUS=124
  fi
  rm -f "$TIMED_OUT_FLAG"
fi

ELAPSED=$(( $(date +%s) - START_TS ))

if [[ "$STATUS" -eq 124 ]]; then
  echo "[run_safe] タイムアウト (${TIMEOUT_SECONDS}s 経過) でコマンドを強制終了しました" >&2
else
  echo "[run_safe] 完了 (exit=${STATUS}, 経過=${ELAPSED}s)" >&2
fi

exit "$STATUS"
