#!/usr/bin/env bash
# PreToolUse hook: gh pr create 実行前にモデル比較レポートの存在を確認する
#
# stdin: Claude Code が渡す JSON {"tool_name": "Bash", "tool_input": {"command": "..."}}
# exit 0: 常に続行（警告のみ、ブロックしない）

set -uo pipefail

INPUT=$(cat)
CMD=$(echo "$INPUT" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    print(data.get('tool_input', {}).get('command', ''))
except Exception:
    print('')
" 2>/dev/null || echo "")

# gh pr create を含むコマンドのみ対象
echo "$CMD" | grep -qE "gh pr create" || exit 0

PROJ_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
REPORT=$(ls "$PROJ_DIR/reports/comparison_"*.md 2>/dev/null | sort | tail -1)

if [ -z "$REPORT" ]; then
    echo ""
    echo "⚠️  [pre-pr] モデル比較レポートが reports/ にありません"
    echo "   特徴量・モデル変更を含む PR の場合は先に比較を実行してください:"
    echo "   .venv/bin/python scripts/compare_features.py \\"
    echo "       --project-id keiba-prediction-1768734113 \\"
    echo "       --skip-feature-pipeline --n-trials 50 --timeout 1800"
    echo ""
    echo "   ※ 特徴量・モデル変更を含まない PR はそのまま続行可"
else
    REPORT_NAME=$(basename "$REPORT")
    REPORT_DATE=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M" "$REPORT" 2>/dev/null || date -r "$REPORT" "+%Y-%m-%d %H:%M" 2>/dev/null || echo "不明")
    echo "✅ [pre-pr] モデル比較レポート確認済: $REPORT_NAME (${REPORT_DATE})"
fi

exit 0
