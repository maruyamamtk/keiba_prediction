引数で指定された期間のバックテストを実行します。

## 使い方
- 引数例: `2025-01-01 2025-12-31`
- 引数例（期間省略）: 省略時は過去6ヶ月（今日から180日前〜今日）を自動設定

## 実行手順

1. 引数から開始日・終了日をパース（省略時は自動計算）
2. 以下のコマンドを実行:

```bash
cd /Users/michika_maruyama/Desktop/keiba_prediction
.venv/bin/python scripts/run_backtest.py \
  --start-date {開始日} \
  --end-date {終了日} \
  --budget-per-race 3000 \
  --save-bq
```

3. 実行結果から以下を日本語でサマリーしてください:
   - **回収率** (目標: 100%以上)
   - **的中率**
   - **最大ドローダウン**
   - **シャープレシオ**
   - KPI達成・未達成の判定
   - 未達成の場合は `backtest-analyst` エージェントへのエスカレーション提案
