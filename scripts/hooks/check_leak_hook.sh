#!/usr/bin/env bash
# PostToolUse hook: Edit/Write 後に特徴量ファイル変更を検知してリーク確認を促す
#
# stdin: Claude Code が渡す JSON {"tool_name": "...", "tool_input": {"file_path": "..."}, ...}
# exit 0: 正常（ツール実行は続行）

set -uo pipefail

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    print(data.get('tool_input', {}).get('file_path', ''))
except Exception:
    print('')
" 2>/dev/null || echo "")

[ -z "$FILE_PATH" ] && exit 0

FEATURE_PATTERN="(feature_query_raw\.sql|src/ml/features/[^/]+\.py|jrdb_parser\.py)"
if echo "$FILE_PATH" | grep -qE "$FEATURE_PATTERN"; then
    echo ""
    echo "⚠️  [check-leak] 特徴量ファイルを変更しました: $(basename "$FILE_PATH")"
    echo "   → /check-leak を実行してデータリークをレビューしてください"
fi

exit 0
