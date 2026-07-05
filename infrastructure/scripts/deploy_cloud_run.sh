#!/bin/bash
#
# Cloud Runサービスデプロイスクリプト
#
# 使用方法:
#   ./infrastructure/scripts/deploy_cloud_run.sh [image-tag]
#
# 引数:
#   image-tag: Dockerイメージのタグ (デフォルト: latest)
#

set -e

# 色付きログ出力
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# .envファイルを読み込み
if [ -f "${PROJECT_ROOT}/.env" ]; then
    log_info ".envファイルを読み込んでいます..."
    set -a
    source "${PROJECT_ROOT}/.env"
    set +a
else
    log_error ".envファイルが見つかりません: ${PROJECT_ROOT}/.env"
    exit 1
fi

# 必須変数のチェック
if [ -z "${GCP_PROJECT_ID}" ] || [ "${GCP_PROJECT_ID}" = "your-project-id" ]; then
    log_error "GCP_PROJECT_IDが設定されていません。.envファイルを確認してください。"
    exit 1
fi

# デフォルト値の設定
GCP_REGION="${GCP_REGION:-asia-northeast1}"
PIPELINE_SA_NAME="${PIPELINE_SA_NAME:-keiba-pipeline-sa}"
IMAGE_TAG="${1:-latest}"

# 変数設定
SERVICE_NAME="keiba-pipeline"
REPO_NAME="keiba-pipeline"
IMAGE_URI="${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${REPO_NAME}/${SERVICE_NAME}:${IMAGE_TAG}"
PIPELINE_SA_EMAIL="${PIPELINE_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

# GCSバケット名（プロジェクトIDをプレフィックスとして使用）
GCS_BUCKET_RAW_FULL="${GCP_PROJECT_ID}-${GCS_BUCKET_RAW:-keiba-raw-data}"
GCS_BUCKET_MODELS_FULL="${GCP_PROJECT_ID}-${GCS_BUCKET_MODELS:-keiba-models}"

log_info "=========================================="
log_info "Cloud Runサービスをデプロイします"
log_info "=========================================="
log_info "プロジェクトID: ${GCP_PROJECT_ID}"
log_info "リージョン: ${GCP_REGION}"
log_info "サービス名: ${SERVICE_NAME}"
log_info "イメージ: ${IMAGE_URI}"
log_info "サービスアカウント: ${PIPELINE_SA_EMAIL}"
log_info "GCSバケット(raw): ${GCS_BUCKET_RAW_FULL}"
log_info "GCSバケット(models): ${GCS_BUCKET_MODELS_FULL}"
log_info "=========================================="

# イメージの存在確認
log_info "Dockerイメージの存在を確認しています..."
if ! gcloud artifacts docker images describe "${IMAGE_URI}" 2>/dev/null; then
    log_error "Dockerイメージが見つかりません: ${IMAGE_URI}"
    log_error "先にイメージをビルド・プッシュしてください。"
    exit 1
fi
log_info "イメージが見つかりました"

# Cloud Runへデプロイ
log_info "Cloud Runへデプロイしています..."

gcloud run deploy "${SERVICE_NAME}" \
    --image="${IMAGE_URI}" \
    --platform=managed \
    --region="${GCP_REGION}" \
    --service-account="${PIPELINE_SA_EMAIL}" \
    --memory=4Gi \
    --cpu=2 \
    --timeout=900 \
    --concurrency=1 \
    --min-instances=0 \
    --max-instances=5 \
    --no-allow-unauthenticated \
    --set-env-vars="GCP_PROJECT_ID=${GCP_PROJECT_ID},GCP_REGION=${GCP_REGION},GCS_BUCKET_RAW=${GCS_BUCKET_RAW:-keiba-raw-data},GCS_BUCKET_MODELS=${GCS_BUCKET_MODELS:-keiba-models},BQ_DATASET_RAW=raw,BQ_DATASET_FEATURES=features,BQ_DATASET_PREDICTIONS=predictions,LOG_LEVEL=INFO" \
    --set-secrets="JRDB_USER=jrdb-user:latest,JRDB_PASSWORD=jrdb-password:latest,LINE_CHANNEL_ACCESS_TOKEN=line-channel-access-token:latest,LINE_CHANNEL_SECRET=line-channel-secret:latest,LINE_USER_ID=line-user-id:latest,IPAT_MEMBER_ID=ipat-member-id:latest,IPAT_PIN=ipat-pin:latest,IPAT_PAT_NUMBER=ipat-pat-number:latest" \
    --quiet

log_info "デプロイが完了しました"

# サービスアカウントにGCSバケットへのアクセス権限を付与
log_info "サービスアカウントにGCSバケットへのアクセス権限を付与しています..."

gsutil iam ch \
    "serviceAccount:${PIPELINE_SA_EMAIL}:roles/storage.objectAdmin" \
    "gs://${GCS_BUCKET_RAW_FULL}" 2>/dev/null && \
    log_info "GCSバケット権限を設定しました: gs://${GCS_BUCKET_RAW_FULL}" || \
    log_warn "GCSバケット権限の設定をスキップしました（バケットが存在しない可能性があります）"

# モデルバケットへのアクセス権限を付与
# objectAdmin: 日次予測がモデルを読み込み、予測結果をGCSに書き込むため（モデル再学習の
# GCS書き込みはローカル月次フロー scripts/monthly_retrain.py がユーザー権限で行う）
gsutil iam ch \
    "serviceAccount:${PIPELINE_SA_EMAIL}:roles/storage.objectAdmin" \
    "gs://${GCS_BUCKET_MODELS_FULL}" 2>/dev/null && \
    log_info "モデルバケット権限を設定しました: gs://${GCS_BUCKET_MODELS_FULL}" || \
    log_warn "モデルバケット権限の設定をスキップしました（バケットが存在しない可能性があります）"

# サービスURLを取得
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
    --region="${GCP_REGION}" \
    --format="value(status.url)")

log_info "=========================================="
log_info "デプロイ完了"
log_info "=========================================="
log_info "サービスURL: ${SERVICE_URL}"
log_info ""
log_info "次のステップ:"
log_info "  1. デプロイ後の動作確認:"
log_info "     ./infrastructure/scripts/verify_deployment.sh"
log_info "  2. 予測エンドポイントのテスト (任意日付指定):"
log_info "     POST /api/v1/predict/on-demand"
log_info "  3. 日次予測スケジューラーの設定:"
log_info "     POST /api/v1/predict/daily (前日PM 9:00 に実行)"
log_info ""
log_warn "=========================================="
log_warn "【重要】デプロイ後は必ず以下を実行してください"
log_warn "=========================================="
log_warn "Cloud Schedulerジョブを作成・更新するため setup_scheduler.sh を再実行してください:"
log_warn "  ./infrastructure/scripts/setup_scheduler.sh"
log_warn ""
log_warn "これを実行しないと以下のジョブが本番環境に存在しません:"
log_warn "  - race-day-predict、race-day-strategy 等の日次ジョブ"
log_warn "=========================================="
log_info ""
