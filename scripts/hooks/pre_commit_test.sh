#!/usr/bin/env bash
# PreToolUse hook: git commit 実行前に pytest を自動実行する
#
# stdin: Claude Code が渡す JSON {"tool_name": "Bash", "tool_input": {"command": "..."}}
# exit 2: アクションをブロック（テスト失敗時）
# exit 0: アクションを続行

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

# git commit を含むコマンドのみ対象
echo "$CMD" | grep -qE "git commit" || exit 0

PROJ_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$PROJ_DIR"

echo "🧪 [pre-commit] pytest を実行中..."
RESULT=$(.venv/bin/pytest tests/ -q --tb=short -x 2>&1 | tail -15)
echo "$RESULT"

if echo "$RESULT" | grep -qE "^(FAILED|ERROR|[0-9]+ failed)"; then
    echo ""
    echo "❌ テスト失敗 — コミットを中断しました"
    echo "   テストを修正してから再度 git commit を実行してください"
    exit 2
fi

echo ""
echo "✅ テスト通過 — コミットを続行します"
exit 0
