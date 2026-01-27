# GCS to BigQuery Cloud Function

GCSにファイルがアップロードされた際に自動的にBigQueryにロードするCloud Functionです。

## 概要

- **トリガー**: GCSオブジェクト作成 (google.storage.object.finalize)
- **対象バケット**: `${PROJECT_ID}-keiba-raw-data`
- **処理**: JRDBデータファイル (CSV) を解析してBigQueryにロード

## サポートするデータタイプ

| データタイプ | 説明 | テーブル |
|------------|------|---------|
| BAA/BAB/BAC | 番組データ | `raw.race_info` |
| KYF/KYG/KYH | 競走馬データ | `raw.horse_results` |
| KAA/KAB | 開催データ | `raw.race_info` |
| SEC | 成績データ | `raw.horse_results` |

## ファイル構成

```
cloud_functions/gcs_to_bq/
├── main.py              # Cloud Functionエントリーポイント
├── parser.py            # JRDBデータパーサー
├── requirements.txt     # Python依存パッケージ
├── deploy.sh            # デプロイスクリプト
└── README.md            # このファイル
```

## デプロイ

### 1. 環境変数の設定

`.env`ファイルを確認し、以下の環境変数が設定されていることを確認してください:

```bash
GCP_PROJECT_ID=your-project-id
BQ_DATASET_RAW=raw
```

### 2. デプロイコマンド

```bash
# デプロイスクリプトを使用
cd cloud_functions/gcs_to_bq
./deploy.sh

# または直接デプロイ
gcloud functions deploy gcs-to-bq \
  --gen2 \
  --runtime python39 \
  --region asia-northeast1 \
  --source . \
  --entry-point gcs_to_bq \
  --trigger-bucket ${PROJECT_ID}-keiba-raw-data \
  --set-env-vars GCP_PROJECT_ID=${PROJECT_ID},BQ_DATASET_RAW=raw \
  --memory 512MB \
  --timeout 540s
```

### 3. デプロイ確認

```bash
gcloud functions describe gcs-to-bq --region asia-northeast1
```

## テスト

### 1. ローカルテスト

```bash
# Functions Frameworkを使用してローカル実行
functions-framework --target=http_trigger --debug

# テストリクエスト送信
curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "keiba-prediction-452203-keiba-raw-data",
    "file": "Baa/BAA260104.csv"
  }'
```

### 2. 本番テスト

```bash
# GCSにファイルをアップロード
gsutil cp ../downloaded_files/Baa/BAA260104.csv \
  gs://${PROJECT_ID}-keiba-raw-data/Baa/

# Cloud Functionのログ確認
gcloud functions logs read gcs-to-bq --region asia-northeast1 --limit 50
```

## エラーハンドリング

- ファイル形式エラー: 非CSVファイルはスキップされます
- パースエラー: エラーログが出力され、処理可能な行のみロードされます
- BigQueryエラー: エラーログが出力され、リトライは行われません

## ログ確認

```bash
# Cloud Functionのログをストリーミング表示
gcloud functions logs read gcs-to-bq \
  --region asia-northeast1 \
  --limit 50 \
  --format "table(time, log)"

# 特定の実行ID のログ確認
gcloud functions logs read gcs-to-bq \
  --region asia-northeast1 \
  --execution-id EXECUTION_ID
```

## 監視

Cloud Consoleでメトリクスを確認:
- 実行回数
- エラー率
- 実行時間
- メモリ使用量

## トラブルシューティング

### 問題: ファイルがロードされない

- BigQueryテーブルが作成されているか確認
- バケット名が正しいか確認
- Cloud Functionのトリガーが設定されているか確認

### 問題: パースエラー

- ファイルのエンコーディング (CP932) を確認
- ファイル形式が固定長レコードか確認
- ログでエラー詳細を確認

### 問題: BigQueryエラー

- テーブルスキーマが正しいか確認
- データ型が一致しているか確認
- 権限が設定されているか確認

## 今後の拡張

- [ ] より多くのデータタイプのサポート (OZ など)
- [ ] エラー時のリトライ機能
- [ ] Cloud Pub/Subへの通知機能
- [x] データ品質チェック機能 → `src/data/quality_check.py` で実装済み
- [ ] 重複データのスキップ機能

## 関連スクリプト

- **GCSアップロード**: `src/data/upload_to_gcs.py` - ローカルからGCSへのアップロード
- **データ品質チェック**: `src/data/quality_check.py` - BigQueryデータの品質検証
- **テーブル作成**: `src/data/create_tables.py` - BigQueryテーブルの作成
