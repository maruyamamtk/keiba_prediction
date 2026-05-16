特徴量・モデルコードの変更を本番に反映する完全フローを実行してください。
以下のステップを順番に実施し、各ステップの完了を確認してから次へ進んでください。
エラーが発生した場合は直ちに停止し、原因を報告してください。

---

## ステップ1: 特徴量パイプラインを実行（features.training_data を全件再生成）

```bash
.venv/bin/python scripts/generate_features.py \
    --project-id keiba-prediction-1768734113 \
    --start-date 2016-01-01 \
    --end-date $(date +%Y-%m-%d)
```

- SQL（`feature_query_raw.sql`）の変更を training_data に反映するため、全期間を再生成します
- 完了ログから「Inserted X rows」を確認し、前回の行数（約50万行）と大きく乖離していないことを確認してください
- 完了まで10〜30分かかる場合があります

## ステップ2: ローカルでモデルを学習

```bash
.venv/bin/python -m src.models.train --tune --project-id keiba-prediction-1768734113
```

- 完了まで数時間かかる場合があります（Optunaチューニングあり）
- 学習完了後、モデルは自動的にGCSへアップロードされます
- 出力ログから NDCG@3・AUC・Recall@3 を確認し、前回より大幅に悪化していないことを確認してください

## ステップ3: GCSへのアップロードを確認

```bash
gcloud storage ls gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker/
```

本日日付のフォルダが作成されていることを確認してください。

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
- 学習結果: NDCG@3 / AUC / Recall@3
- GCS保存先: `gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker/{YYYYMMDD}/`
- デプロイしたCloud Runリビジョン名
- 翌日 AM 8:00 の `race-day-predict` が新モデルを自動的に使用することを案内
