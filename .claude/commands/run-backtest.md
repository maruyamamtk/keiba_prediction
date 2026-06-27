引数で指定された期間のバックテストを実行します。

## 使い方
- 引数例: `2025-01-01 2025-12-31`
- 引数例（期間省略）: 省略時は過去6ヶ月（今日から180日前〜今日）を自動設定

## 実行手順

1. 引数から開始日・終了日をパース（省略時は自動計算）

2. GCS から最新の lgbm_ranker_multi モデルパスを取得:
```bash
gcloud storage ls gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker_multi/ \
  | sort | tail -1
```
上記で得られた日付フォルダ内のパスを `--model-path` に指定する。

3. 以下のコマンドを実行:

```bash
cd /Users/michika_maruyama/Desktop/keiba_prediction
.venv/bin/python scripts/run_backtest.py \
  --project-id keiba-prediction-1768734113 \
  --model-path gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker_multi/{YYYYMMDD}/lgbm_ranker_multi_{YYYYMMDD}.txt \
  --start-date {開始日} \
  --end-date {終了日} \
  --budget-per-race 3000 \
  --save-bq
```

   - **校正器の適用（Issue #417）**: `run_backtest.py` は指定モデルの保存済み校正器（meta.json の
     アイソトニック→温度→T=1.0）を `apply_model_calibration` で適用し、`win_place_prob` を算出します。
     これは本番予測パス（`predict.py`）と**同一の共通関数**であり、バックテストの確率が本番と一致します。
     最適化したパラメータと本番の確率分布を食い違わせないため、戦略最適化も同じ校正済み確率で行うこと。

4. 実行結果から以下を日本語でサマリーしてください:
   - **回収率** (目標: 100%以上)
   - **的中率**
   - **最大ドローダウン**
   - **シャープレシオ**
   - KPI達成・未達成の判定
   - 未達成の場合は `backtest-analyst` エージェントへのエスカレーション提案
