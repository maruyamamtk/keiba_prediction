#!/bin/bash
#
# ローカル月次モデル再学習 launchd エントリポイント
#
# launchd（com.keiba.monthly-retrain）から毎週月曜 AM1:00 に起動される。
# 月内最初の月曜（日付が 1〜7）だけ実処理し、それ以外の月曜は即終了する
# → 実質「毎月第1月曜 AM1:00」に scripts/monthly_retrain.py を実行する。
#
# 手動実行:
#   ./scripts/monthly_retrain_local.sh              # フル実行
#   ./scripts/monthly_retrain_local.sh --dry-run    # 引数はそのまま monthly_retrain.py へ渡る
#   FORCE_RUN=1 ./scripts/monthly_retrain_local.sh   # 第1月曜ガードを無視して実行
#

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

# 第1月曜ガード: 日付が 8 以上（＝第2週以降の月曜）なら実処理しない
DAY_OF_MONTH="$(date +%d)"
if [ "${FORCE_RUN:-0}" != "1" ] && [ "${DAY_OF_MONTH#0}" -gt 7 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') 第1月曜ではないためスキップ（day=${DAY_OF_MONTH}）"
    exit 0
fi

# .env 読み込み（GCP_PROJECT_ID / LINE_* など）
if [ -f "${PROJECT_ROOT}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_ROOT}/.env"
    set +a
fi

mkdir -p "${PROJECT_ROOT}/logs"
LOG_FILE="${PROJECT_ROOT}/logs/monthly_retrain_$(date +%Y%m).log"
PYTHON="${PROJECT_ROOT}/.venv/bin/python"

{
    echo "=========================================="
    echo "$(date '+%Y-%m-%d %H:%M:%S') 月次再学習を開始します"
    echo "=========================================="
} >> "${LOG_FILE}" 2>&1

"${PYTHON}" scripts/monthly_retrain.py "$@" >> "${LOG_FILE}" 2>&1
STATUS=$?

echo "$(date '+%Y-%m-%d %H:%M:%S') 月次再学習が終了しました（exit=${STATUS}）" >> "${LOG_FILE}" 2>&1

# macOS 通知（launchd の GUI セッションで表示される）
if command -v osascript >/dev/null 2>&1; then
    if [ "${STATUS}" -eq 0 ]; then
        osascript -e 'display notification "月次再学習が正常終了しました" with title "keiba monthly-retrain"' 2>/dev/null || true
    else
        osascript -e "display notification \"月次再学習が失敗しました（exit=${STATUS}）。logsを確認\" with title \"keiba monthly-retrain\"" 2>/dev/null || true
    fi
fi

exit "${STATUS}"
