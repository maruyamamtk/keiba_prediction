引数で指定されたモジュール名またはキーワードに対応するテストを、`.venv/bin/pytest` で実行してください。

## 実行ルール
- 必ず `.venv/bin/pytest` を使用すること（`pytest` や `python3 -m pytest` は不可）
- 作業ディレクトリ: プロジェクトルート（`/Users/michika_maruyama/Desktop/keiba_prediction`）

## 引数パターン

**引数なし** → 全テストを実行:
```
.venv/bin/pytest tests/ -x -q 2>&1 | tail -20
```

**キーワード指定**（例: `$ARGUMENTS` が "strategy" の場合）→ `-k` フィルタで実行:
```
.venv/bin/pytest tests/ -x -q -k "$ARGUMENTS" 2>&1 | tail -20
```

**ファイル名指定**（例: `$ARGUMENTS` が "test_backtest_strategy" の場合）→ ファイル直接指定:
```
.venv/bin/pytest tests/test_backtest_strategy.py -x -q 2>&1 | tail -20
```

## 結果の報告
- 合格/失敗の件数を日本語で報告
- 失敗がある場合はエラーメッセージと原因を日本語で説明
- 失敗したテストがある場合は修正案を提示
