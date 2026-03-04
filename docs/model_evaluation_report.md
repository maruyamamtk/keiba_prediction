# モデル評価レポート

**生成日時**: 2026-03-04 23:05:12
**モデルパス**: `src/models/lgbm_ranker_20260217.txt`

---

## 1. モデル概要

| 項目 | 内容 |
|------|------|
| アルゴリズム | LightGBM LambdaRank |
| 目的関数 | lambdarank |
| ラベル形式 | 二値ラベル（3着以内=1, それ以外=0）|
| 評価指標 | NDCG@3, Recall@3, AUC |
| グループ単位 | レースID |
| 特徴量数 | 234 |
| Best Iteration | N/A |

---

## 2. 時系列分割設定

| Split | 期間 |
|-------|------|
| 学習 (Train) | 2016-01-05 〜 2025-08-31 |
| 検証 (Valid) | 2025-09-06 〜 2026-03-01 |


---

## 3. 評価指標

| Split | NDCG@3 | Recall@3 | AUC | レース数 | 行数 |
|-------|--------|----------|-----|----------|------|
| 学習 (Train) | 0.5960 | 0.5317 | 0.8172 | 32,708 | 457,292 |
| 検証 (Valid) | 0.5659 | 0.5175 | 0.8084 | 1,487 | 20,839 |


> **指標説明**
> - **NDCG@3**: 上位3頭のランキング品質（1.0が最高）
> - **Recall@3**: 3着以内の馬を予測TOP3に含める割合（1.0が最高）
> - **AUC**: 3着以内の二値分類性能（ROC-AUC、0.5が無作為）

---

## 4. 特徴量重要度 TOP 20

| Rank | Feature | Importance (Gain) |
|------|---------|------------------|
| 1 | surge_index | 208,383.2 |
| 2 | jockey_index | 198,941.8 |
| 3 | base_odds | 123,958.7 |
| 4 | training_index | 120,937.3 |
| 5 | info_index | 51,215.4 |
| 6 | idm_diff | 39,782.7 |
| 7 | ema_finish_position_rate | 28,937.1 |
| 8 | base_popularity | 16,191.3 |
| 9 | horse_age | 12,388.1 |
| 10 | ema_idm_diff | 10,080.4 |
| 11 | hoof_code | 5,799.5 |
| 12 | total_index | 5,407.0 |
| 13 | blinker | 5,095.6 |
| 14 | num_horses | 4,692.5 |
| 15 | jockey_expected_win_rate | 4,643.1 |
| 16 | max_idm_diff | 4,045.2 |
| 17 | win_popularity_1 | 3,799.1 |
| 18 | mean_idm_diff | 3,763.9 |
| 19 | ten_index | 3,757.3 |
| 20 | win_odds_1 | 3,608.0 |

![Feature Importance](figures/feature_importance.png)

---

## 5. 評価指標の月次推移

![Monthly Metrics](figures/monthly_metrics.png)

---

## 6. モデルパラメータ

| Parameter | Value |
|-----------|-------|


---

## 7. 目標指標との比較

| 指標 | 目標値 | 実績（検証） | 達成 |
|------|--------|-------------|------|
| NDCG@3 | 0.70以上 | 0.5659 | ✗ |
| Recall@3 | 0.80以上 | 0.5175 | ✗ |
| AUC | 0.70以上 | 0.8084 | ✓ |

---

*このレポートは `scripts/generate_evaluation_report.py` によって自動生成されました。*
