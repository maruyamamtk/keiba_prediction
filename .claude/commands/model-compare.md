---
description: 特徴量変更後の新旧モデル精度比較を実行し、学習・検証期間と精度を並べて表示する
argument-hint: (オプション) --n-trials 50 --timeout 1800
user-invocable: true
---

新旧モデルの精度比較を実行してください。

## 実行コマンド

引数 `$ARGUMENTS` をそのまま追記する（省略時は `--n-trials 50 --timeout 1800` を使用）:

```bash
.venv/bin/python scripts/compare_features.py \
    --project-id keiba-prediction-1768734113 \
    --skip-feature-pipeline \
    $ARGUMENTS
```

引数が空の場合:
```bash
.venv/bin/python scripts/compare_features.py \
    --project-id keiba-prediction-1768734113 \
    --skip-feature-pipeline \
    --n-trials 50 \
    --timeout 1800
```

> `compare_features.py` は LGBMRanker（二値ラベル）ベースの比較を行います。
> 多段階モデル（ranker_multi / regression / classifier）の精度確認は下記の追加評価を実施してください。

## 多段階モデルの追加評価（特徴量・SQL変更時）

### ranker_multi（JRA賞金多値ラベル）
```bash
.venv/bin/python -m src.models.train \
    --model-type multi \
    --tune \
    --n-trials 20 \
    --tune-timeout 900 \
    --skip-gcs-upload \
    --project-id keiba-prediction-1768734113
```

### regression（着差Zスコア回帰）
```bash
.venv/bin/python -m src.models.train \
    --model-type regression \
    --tune \
    --n-trials 20 \
    --tune-timeout 900 \
    --skip-gcs-upload \
    --project-id keiba-prediction-1768734113
```

### classifier（複勝率直接推定）
```bash
.venv/bin/python -m src.models.train \
    --model-type classifier \
    --tune \
    --n-trials 20 \
    --tune-timeout 900 \
    --skip-gcs-upload \
    --project-id keiba-prediction-1768734113
```

## 結果の提示

実行後、以下の形式でユーザーに提示してください:

### モデル別 学習・検証期間

| | 既存モデル | 新モデル |
|---|---|---|
| **学習期間** | （meta.jsonから取得、なければ「不明」） | 比較実行時の分割結果 |
| **検証期間** | （同上） | 比較実行時の分割結果 |

### 精度比較（LGBMRanker ベースライン）

| 指標 | 既存モデル | 新モデル | 差分 | 判定（±0.005） |
|---|---|---|---|---|
| NDCG@3 | ... | ... | ... | ✅/❌ |
| AUC | ... | ... | ... | ✅/❌ |
| Recall@3 | ... | ... | ... | ✅/❌ |

### 多段階モデル評価（追加実施した場合）

| モデル | 指標 | スコア | 判定 |
|---|---|---|---|
| ranker_multi | NDCG@3 / AUC / Recall@3 | ... | ✅/❌ |
| regression | RMSE / NDCG@3 / Recall@3 / AUC | ... | RMSE低いほど良い、NDCG@3/Recall@3/AUCは高いほど良い |
| classifier | NDCG@3 / AUC / Recall@3 | ... | ✅/❌ |

### 総合判定

- **✅ マージ可**: 全指標が合格基準を満たす → PR 作成を促す
- **❌ 要修正**: 1つ以上の指標が基準を下回る → 悪化した指標の原因と対策を提案する

## 注意事項

- `reports/comparison_YYYYMMDD.md` にレポートが保存される
- 特徴量パイプライン再実行が必要な場合は `--skip-feature-pipeline` を外して実行
- Optuna 試行数を増やす場合は `--n-trials 100 --timeout 3600` を指定
- 多段階モデルの追加評価は `--skip-gcs-upload` を付けてGCSアップロードを省略する
