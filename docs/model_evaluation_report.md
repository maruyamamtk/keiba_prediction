# モデル評価レポート

> このファイルは `scripts/generate_evaluation_report.py` によって自動生成されます。
> 手動で編集した内容は上書きされます。

以下のコマンドでレポートを更新してください:

```bash
python scripts/generate_evaluation_report.py \
    --model-path src/models/lgbm_ranker_20260217.txt \
    --project-id <YOUR_PROJECT_ID>
```

GCS上のモデルを使用する場合:

```bash
python scripts/generate_evaluation_report.py \
    --gcs-model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/20260217/lgbm_ranker_20260217.txt \
    --project-id <YOUR_PROJECT_ID>
```

BigQuery接続なし（ローカルCSV）でレポートを生成する場合:

```bash
python scripts/generate_evaluation_report.py \
    --model-path src/models/lgbm_ranker_20260217.txt \
    --local-data-path /path/to/training_data.csv
```
