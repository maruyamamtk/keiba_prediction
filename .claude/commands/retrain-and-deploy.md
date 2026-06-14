特徴量・モデルコードの変更を本番に反映する完全フローを実行してください。
以下のステップを順番に実施し、各ステップの完了を確認してから次へ進んでください。
エラーが発生した場合は直ちに停止し、原因を報告してください。

---

## ステップ1: pedigree 再構築 → 特徴量パイプラインを実行（features.training_data を全件再生成）

```bash
.venv/bin/python scripts/generate_features.py \
    --project-id keiba-prediction-1768734113 \
    --start-date 2016-01-01 \
    --end-date $(date +%Y-%m-%d) \
    --truncate
```

- スクリプト実行前に `raw.pedigree` を自動再構築します（母馬TE特徴量の dam_id 解決率を最大化）
- `--truncate` は必須です。TRUNCATE TABLE → WRITE_TRUNCATE の冪等操作で重複を防ぎます。
- SQL（`feature_query_raw.sql`）の変更を training_data に反映するため、全期間を再生成します
- 完了ログから「Inserted X rows」を確認し、前回の行数（約67万行）と大きく乖離していないことを確認してください
- 完了まで10〜30分かかる場合があります

## ステップ2: ローカルで3モデルを学習（ハイブリッドアンサンブル）

多段階ハイブリッドアンサンブルのため、以下の3モデルを順番に学習します。
各学習は完了後に GCS へ自動アップロードされます。

### 2-1. 多値ランク学習モデル（メイン）
```bash
.venv/bin/python -m src.models.train \
    --model-type multi \
    --tune \
    --project-id keiba-prediction-1768734113
```

### 2-2. 着差回帰モデル（アンサンブルのサブスコア）
```bash
.venv/bin/python -m src.models.train \
    --model-type regression \
    --tune \
    --project-id keiba-prediction-1768734113
```

### 2-3. 二値分類モデル（複勝率推定・期待値計算用）
```bash
.venv/bin/python -m src.models.train \
    --model-type classifier \
    --tune \
    --project-id keiba-prediction-1768734113
```

- 各モデルの完了まで数時間かかる場合があります（Optunaチューニングあり）
- 各学習完了後、NDCG@3・AUC・Recall@3 を確認し、前回より大幅に悪化していないことを確認してください
- 回帰モデルは RMSE 評価（値が低いほど良い）

## ステップ3: GCSへのアップロードを確認

```bash
gcloud storage ls gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker_multi/
gcloud storage ls gs://keiba-prediction-1768734113-keiba-models/lgbm_regression/
gcloud storage ls gs://keiba-prediction-1768734113-keiba-models/lgbm_classifier/
```

本日日付のフォルダが3つ全て作成されていることを確認してください。

## ステップ4: Cloud Runへデプロイ

```bash
./infrastructure/scripts/build_and_push.sh
./infrastructure/scripts/setup_cloud_run_jobs.sh
./infrastructure/scripts/deploy_cloud_run.sh
./infrastructure/scripts/setup_scheduler.sh
```

## ステップ5: デプロイ後の動作確認

```bash
./infrastructure/scripts/verify_deployment.sh
```

---

## 完了報告

全ステップが完了したら以下を報告してください：

- 特徴量パイプライン: 生成行数・カラム数
- 学習結果（3モデル）:
  - ranker_multi: NDCG@3 / AUC / Recall@3
  - regression: RMSE
  - classifier: NDCG@3 / AUC / Recall@3
- GCS保存先:
  - `gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker_multi/{YYYYMMDD}/`
  - `gs://keiba-prediction-1768734113-keiba-models/lgbm_regression/{YYYYMMDD}/`
  - `gs://keiba-prediction-1768734113-keiba-models/lgbm_classifier/{YYYYMMDD}/`
- デプロイしたCloud Runリビジョン名
- 翌日 AM 8:00 の `race-day-predict` が3モデルのアンサンブル推論を自動使用することを案内
