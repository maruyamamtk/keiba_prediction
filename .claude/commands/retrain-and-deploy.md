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
- 完了ログから「Inserted X rows」を確認してください。**正常ベースラインは約 442,000 行 / 664 カラム**（2016〜現在）。年あたり約 41,000〜45,000 行・約 2,900〜3,000 レースが一貫していれば正常です（特定の固定値ではなく**年次分布の一貫性**で判断すること）。
- 完了まで10〜30分かかる場合があります

## ステップ2: ローカルでモデルを学習（LGBMRankerMulti）

```bash
.venv/bin/python -m src.models.train \
    --tune \
    --project-id keiba-prediction-1768734113
```

- 現行 train.py は `LGBMRankerMulti` 単一構成のため `--model-type` 引数は存在しません（指定するとエラー）。
- 完了まで数時間かかる場合があります（Optunaチューニングあり）
- 完了後、検証指標 **NDCG@3 / AUC / Recall@3**（参考水準: NDCG@3≈0.57 / AUC≈0.81 / Recall@3≈0.51）を確認し、前回より大幅に悪化していないことを確認してください

## ステップ3: GCSへのアップロードを確認

```bash
gcloud storage ls gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker_multi/
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

- `POST /api/v1/predict/daily` のテストは無効な型を送って **422（バリデーション正常）** を確認するスキーマチェックです。実予測は起動しません。
- 万一 `000`（タイムアウト/接続不可）が出た場合でも、Cloud Run ログで該当リクエストが `200 OK` で完了していれば**デプロイは正常**です（誤検知）。確認コマンド:
  ```bash
  gcloud run services logs read keiba-pipeline --region=asia-northeast1 --limit=30 | grep -iE "predict/daily|日次予測"
  ```
  未来日（今週末）の予測は full SQL フォールバックで約70〜90秒かかるため、短いクライアントタイムアウトでは 000 になりやすい点に注意。

---

## 完了報告

全ステップが完了したら以下を報告してください：

- 特徴量パイプライン: 生成行数・カラム数
- 学習結果（ranker_multi）: NDCG@3 / AUC / Recall@3
- GCS保存先: `gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker_multi/{YYYYMMDD}/`
- デプロイしたCloud Runリビジョン名
- 翌日 AM 8:00 の `race-day-predict` が自動推論を使用することを案内（model_path未指定で最新GCSモデルを自動採用）
