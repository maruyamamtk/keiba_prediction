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

## 結果の提示

実行後、以下の形式でユーザーに提示してください:

### モデル別 学習・検証期間

| | 既存モデル | 新モデル |
|---|---|---|
| **学習期間** | （meta.jsonから取得、なければ「不明」） | 比較実行時の分割結果 |
| **検証期間** | （同上） | 比較実行時の分割結果 |

### 精度比較

| 指標 | 既存モデル | 新モデル | 差分 | 判定（±0.005） |
|---|---|---|---|---|
| NDCG@3 | ... | ... | ... | ✅/❌ |
| AUC | ... | ... | ... | ✅/❌ |
| Recall@3 | ... | ... | ... | ✅/❌ |

### 総合判定

- **✅ マージ可**: 全指標が合格基準を満たす → PR 作成を促す
- **❌ 要修正**: 1つ以上の指標が基準を下回る → 悪化した指標の原因と対策を提案する

## 注意事項

- `reports/comparison_YYYYMMDD.md` にレポートが保存される
- 特徴量パイプライン再実行が必要な場合は `--skip-feature-pipeline` を外して実行
- Optuna 試行数を増やす場合は `--n-trials 100 --timeout 3600` を指定
