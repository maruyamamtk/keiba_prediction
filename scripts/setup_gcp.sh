#!/bin/bash

# GCPプロジェクトとCloud Storageバケットのセットアップスクリプト
# Issue #3: GCPプロジェクトとCloud Storageバケットのセットアップ

set -e  # エラーが発生したら即座に終了

# 色付きログ出力
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 環境変数の読み込み
if [ -f .env ]; then
    log_info ".envファイルを読み込んでいます..."
    source .env
else
    log_error ".envファイルが見つかりません。.env.exampleを参考に作成してください。"
    exit 1
fi

# 必須環境変数のチェック
if [ -z "$GCP_PROJECT_ID" ]; then
    log_error "GCP_PROJECT_IDが設定されていません。"
    exit 1
fi

if [ -z "$GCP_REGION" ]; then
    log_warn "GCP_REGIONが設定されていません。デフォルト値 'asia-northeast1' を使用します。"
    GCP_REGION="asia-northeast1"
fi

log_info "=== GCPプロジェクトのセットアップを開始します ==="
log_info "プロジェクトID: $GCP_PROJECT_ID"
log_info "リージョン: $GCP_REGION"

# 1. GCPプロジェクトの設定
log_info "GCPプロジェクトを設定しています..."
gcloud config set project "$GCP_PROJECT_ID"

# 2. 必要なAPIの有効化
log_info "必要なAPIを有効化しています..."

apis=(
    "storage-api.googleapis.com"           # Cloud Storage
    "bigquery.googleapis.com"              # BigQuery
    "cloudfunctions.googleapis.com"        # Cloud Functions
    "run.googleapis.com"                   # Cloud Run
    "cloudscheduler.googleapis.com"        # Cloud Scheduler
    "cloudbuild.googleapis.com"            # Cloud Build
    "logging.googleapis.com"               # Cloud Logging
    "monitoring.googleapis.com"            # Cloud Monitoring
)

for api in "${apis[@]}"; do
    log_info "APIを有効化中: $api"
    gcloud services enable "$api" --project="$GCP_PROJECT_ID" || log_warn "$api の有効化に失敗しました。"
done

log_info "すべてのAPIの有効化が完了しました。"

# 3. サービスアカウントの作成と権限設定
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-keiba-prediction-sa}"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

log_info "サービスアカウントを作成しています: $SERVICE_ACCOUNT_NAME"

# サービスアカウントが既に存在するかチェック
if gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    log_warn "サービスアカウント $SERVICE_ACCOUNT_EMAIL は既に存在します。スキップします。"
else
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --display-name "Keiba Prediction Service Account" \
        --project="$GCP_PROJECT_ID"
    log_info "サービスアカウントを作成しました: $SERVICE_ACCOUNT_EMAIL"
fi

# サービスアカウントに必要なロールを付与
log_info "サービスアカウントに権限を付与しています..."

roles=(
    "roles/storage.admin"              # Cloud Storage管理者
    "roles/bigquery.admin"             # BigQuery管理者
    "roles/cloudfunctions.developer"   # Cloud Functions開発者
    "roles/run.admin"                  # Cloud Run管理者
    "roles/cloudscheduler.admin"       # Cloud Scheduler管理者
    "roles/logging.logWriter"          # ログ書き込み
    "roles/monitoring.metricWriter"    # メトリクス書き込み
)

for role in "${roles[@]}"; do
    log_info "ロールを付与中: $role"
    gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
        --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
        --role="$role" \
        --condition=None \
        >/dev/null 2>&1 || log_warn "$role の付与に失敗しました。"
done

log_info "サービスアカウントへの権限付与が完了しました。"

# 4. Cloud Storageバケットの作成
log_info "Cloud Storageバケットを作成しています..."

buckets=(
    "${GCS_BUCKET_RAW:-keiba-raw-data}"
    "${GCS_BUCKET_PROCESSED:-keiba-processed-data}"
    "${GCS_BUCKET_MODELS:-keiba-models}"
    "${GCS_BUCKET_PREDICTIONS:-keiba-predictions}"
)

for bucket in "${buckets[@]}"; do
    FULL_BUCKET_NAME="${GCP_PROJECT_ID}-${bucket}"

    # バケットが既に存在するかチェック
    if gsutil ls -b "gs://${FULL_BUCKET_NAME}" >/dev/null 2>&1; then
        log_warn "バケット gs://${FULL_BUCKET_NAME} は既に存在します。スキップします。"
    else
        log_info "バケットを作成中: gs://${FULL_BUCKET_NAME}"
        gsutil mb -p "$GCP_PROJECT_ID" -l "$GCP_REGION" "gs://${FULL_BUCKET_NAME}"

        # バージョニングを有効化（モデル・予測結果用）
        if [[ "$bucket" == *"models"* ]] || [[ "$bucket" == *"predictions"* ]]; then
            log_info "バージョニングを有効化: gs://${FULL_BUCKET_NAME}"
            gsutil versioning set on "gs://${FULL_BUCKET_NAME}"
        fi

        # ライフサイクル設定（rawデータは90日後削除）
        if [[ "$bucket" == *"raw-data"* ]]; then
            log_info "ライフサイクルポリシーを設定: gs://${FULL_BUCKET_NAME}"
            cat > /tmp/lifecycle.json <<EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 90}
      }
    ]
  }
}
EOF
            gsutil lifecycle set /tmp/lifecycle.json "gs://${FULL_BUCKET_NAME}"
            rm /tmp/lifecycle.json
        fi

        log_info "バケットを作成しました: gs://${FULL_BUCKET_NAME}"
    fi
done

log_info "すべてのバケットの作成が完了しました。"

# 5. 設定の確認
log_info "=== セットアップ完了 ==="
log_info "プロジェクトID: $GCP_PROJECT_ID"
log_info "サービスアカウント: $SERVICE_ACCOUNT_EMAIL"
log_info ""
log_info "作成されたバケット:"
for bucket in "${buckets[@]}"; do
    FULL_BUCKET_NAME="${GCP_PROJECT_ID}-${bucket}"
    echo "  - gs://${FULL_BUCKET_NAME}"
done

log_info ""
log_info "次のステップ:"
log_info "1. サービスアカウントキーをダウンロード（必要に応じて）:"
log_info "   gcloud iam service-accounts keys create key.json --iam-account=$SERVICE_ACCOUNT_EMAIL"
log_info "2. .envファイルのバケット名を更新してください。"
log_info "3. Issue #4: BigQueryデータセットとテーブルの作成に進んでください。"
