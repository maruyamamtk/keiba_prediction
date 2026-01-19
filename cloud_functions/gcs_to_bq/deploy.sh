#!/bin/bash

# GCS to BigQuery Cloud Function デプロイスクリプト

set -e

# スクリプトのディレクトリに移動
cd "$(dirname "$0")"

# .envファイルを読み込み
if [ -f "../../.env" ]; then
    echo "Loading environment variables from .env"
    export $(grep -v '^#' ../../.env | xargs)
else
    echo "Warning: .env file not found"
fi

# 必須環境変数のチェック
if [ -z "$GCP_PROJECT_ID" ]; then
    echo "Error: GCP_PROJECT_ID is not set"
    exit 1
fi

# 必要なAPIを有効化
echo "Checking required APIs..."
REQUIRED_APIS=(
    "cloudfunctions.googleapis.com"
    "cloudbuild.googleapis.com"
    "eventarc.googleapis.com"
    "run.googleapis.com"
    "artifactregistry.googleapis.com"
)

for api in "${REQUIRED_APIS[@]}"; do
    echo "  Enabling $api..."
    gcloud services enable "$api" --project="$GCP_PROJECT_ID" --quiet
done

echo "APIs enabled successfully"
echo ""

# デフォルト値の設定
FUNCTION_NAME="gcs-to-bq"
REGION="${GCP_REGION:-asia-northeast1}"
RUNTIME="python311"  # Python 3.11に変更（3.9は2026年4月サポート終了）
ENTRY_POINT="gcs_to_bq"

# バケット名の設定（既にフルネームの場合はそのまま使用）
if [[ "${GCS_BUCKET_RAW}" == *"${GCP_PROJECT_ID}"* ]]; then
    # 既にプロジェクトIDを含む完全なバケット名の場合
    TRIGGER_BUCKET="${GCS_BUCKET_RAW}"
else
    # プレフィックスのみの場合はプロジェクトIDを追加
    TRIGGER_BUCKET="${GCP_PROJECT_ID}-${GCS_BUCKET_RAW:-keiba-raw-data}"
fi

DATASET_RAW="${BQ_DATASET_RAW:-raw}"
MEMORY="512MB"
TIMEOUT="540s"

# バケット名の長さチェック（GCSの制限は63文字）
BUCKET_NAME_LENGTH=${#TRIGGER_BUCKET}
if [ $BUCKET_NAME_LENGTH -gt 63 ]; then
    echo "Error: Bucket name is too long (${BUCKET_NAME_LENGTH} characters, max 63)"
    echo "Bucket name: ${TRIGGER_BUCKET}"
    echo ""
    echo "Please shorten GCS_BUCKET_RAW in your .env file or use a shorter project ID"
    exit 1
fi

echo "========================================="
echo "Cloud Function Deployment"
echo "========================================="
echo "Project ID: $GCP_PROJECT_ID"
echo "Function Name: $FUNCTION_NAME"
echo "Region: $REGION"
echo "Trigger Bucket: $TRIGGER_BUCKET"
echo "Dataset: $DATASET_RAW"
echo "========================================="

# デプロイ確認
read -p "Deploy Cloud Function? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled"
    exit 0
fi

# Cloud Functionをデプロイ
echo "Deploying Cloud Function..."
gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --runtime $RUNTIME \
  --region $REGION \
  --source . \
  --entry-point $ENTRY_POINT \
  --trigger-bucket $TRIGGER_BUCKET \
  --set-env-vars GCP_PROJECT_ID=$GCP_PROJECT_ID,BQ_DATASET_RAW=$DATASET_RAW \
  --memory $MEMORY \
  --timeout $TIMEOUT \
  --project $GCP_PROJECT_ID

echo "========================================="
echo "Deployment completed successfully!"
echo "========================================="

# デプロイ結果の確認
echo "Function details:"
gcloud functions describe $FUNCTION_NAME --region $REGION --project $GCP_PROJECT_ID

echo ""
echo "To test the function, upload a file to the trigger bucket:"
echo "gsutil cp path/to/file.csv gs://$TRIGGER_BUCKET/"
echo ""
echo "To view logs:"
echo "gcloud functions logs read $FUNCTION_NAME --region $REGION --limit 50"
