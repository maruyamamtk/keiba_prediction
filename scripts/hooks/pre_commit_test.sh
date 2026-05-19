#!/usr/bin/env bash
# PreToolUse hook: git commit 実行前に変更ファイルに関連するテストのみ実行する
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

# ステージ済みファイルを取得
STAGED=$(git diff --cached --name-only 2>/dev/null || true)

# "git add A B && git commit" 形式のコマンドから git add の引数を抽出
# (PreToolUse は実行前に発火するため、まだ git add が走っていない場合の補完)
ADDED=$(echo "$CMD" | python3 -c "
import sys, re
cmd = sys.stdin.read().strip()
added = []
for m in re.finditer(r'\bgit\s+add\s+(.+?)(?:\s*(?:&&|\|\||;)|\s*\$)', cmd):
    for token in m.group(1).split():
        if token and not token.startswith('-') and '=' not in token:
            added.append(token)
print('\n'.join(added))
" 2>/dev/null || true)

# 全変更ファイルをまとめて重複除去
ALL_CHANGED=$(printf '%s\n%s\n' "$STAGED" "$ADDED" | sort -u | grep -v '^$' || true)

# 変更ファイル → テストファイルのマッピング
# 命名規則が2パターンあるため glob で両方を拾う:
#   src/backtest/strategy.py  → tests/test_backtest_strategy.py (test_{parent}_{basename}.py)
#   src/automation/data/jrdb_parser.py → tests/test_jrdb_parser.py (test_{basename}.py)
TEST_FILES=""
while IFS= read -r f; do
    [ -z "$f" ] && continue
    # テストファイル自体が変更された場合はそのまま追加
    if echo "$f" | grep -qE '^tests/test_.*\.py$' && [ -f "$f" ]; then
        TEST_FILES="$TEST_FILES $f"
        continue
    fi
    # src/**.py または scripts/*.py → tests/test_*<basename>.py を検索
    if echo "$f" | grep -qE '\.(py)$'; then
        BASENAME=$(basename "$f" .py)
        # test_<basename>.py と test_*_<basename>.py の両パターンを検索
        while IFS= read -r match; do
            [ -n "$match" ] && TEST_FILES="$TEST_FILES $match"
        done < <(find tests/ \( -name "test_${BASENAME}.py" -o -name "test_*_${BASENAME}.py" \) 2>/dev/null | sort)
    fi
done <<< "$ALL_CHANGED"

# 重複除去してスペース区切りに整形
TEST_FILES=$(echo "$TEST_FILES" | tr ' ' '\n' | sort -u | grep -v '^$' | tr '\n' ' ' | xargs 2>/dev/null || true)

if [ -z "$TEST_FILES" ]; then
    echo "ℹ️  [pre-commit] 対応するテストファイルなし — テストをスキップ"
    exit 0
fi

echo "🧪 [pre-commit] 関連テストを実行中: $TEST_FILES"
# shellcheck disable=SC2086
RESULT=$(.venv/bin/pytest $TEST_FILES -q --tb=short -x 2>&1 | tail -15)
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
