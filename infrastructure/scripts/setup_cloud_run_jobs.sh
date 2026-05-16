#!/bin/bash
#
# Cloud Run Jobs セットアップスクリプト
#
# keiba-model-retrain Cloud Run Job を作成または更新します。
# このJobはCloud Scheduler（weekly-model-retrain）から週次で起動されます。
#
# 使用方法:
#   ./infrastructure/scripts/setup_cloud_run_jobs.sh [image-tag]
#
# 引数:
#   image-tag: Dockerイメージのタグ (デフォルト: latest)
#
# 前提条件:
#   1. gcloud CLIがインストール済み
#   2. 適切な権限を持つユーザーでログイン済み (gcloud auth login)
#   3. .envファイルにGCP_PROJECT_IDが設定済み
#   4. Artifact Registryにイメージがプッシュ済み
#      （先にbuild_and_push.shを実行してください）
#
# 実行後の手順:
#   Cloud Run Jobs作成後、Cloud Schedulerを設定してください:
#     ./infrastructure/scripts/setup_scheduler.sh
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
JOB_NAME="keiba-model-retrain"
SERVICE_NAME="keiba-pipeline"
REPO_NAME="keiba-pipeline"
IMAGE_URI="${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${REPO_NAME}/${SERVICE_NAME}:${IMAGE_TAG}"
PIPELINE_SA_EMAIL="${PIPELINE_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

log_info "=========================================="
log_info "Cloud Run Jobs keiba-model-retrain をセットアップします"
log_info "=========================================="
log_info "プロジェクトID: ${GCP_PROJECT_ID}"
log_info "リージョン: ${GCP_REGION}"
log_info "ジョブ名: ${JOB_NAME}"
log_info "イメージ: ${IMAGE_URI}"
log_info "サービスアカウント: ${PIPELINE_SA_EMAIL}"
log_info "=========================================="

# イメージの存在確認
log_info "Dockerイメージの存在を確認しています..."
if ! gcloud artifacts docker images describe "${IMAGE_URI}" 2>/dev/null; then
    log_error "Dockerイメージが見つかりません: ${IMAGE_URI}"
    log_error "先にイメージをビルド・プッシュしてください:"
    log_error "  ./infrastructure/scripts/build_and_push.sh"
    exit 1
fi
log_info "イメージが見つかりました"

# Cloud Run Jobs API の有効化確認
log_info "Cloud Run API を確認しています..."
if ! gcloud services list --enabled --filter="name:run.googleapis.com" --format="value(name)" 2>/dev/null | grep -q "run"; then
    log_info "Cloud Run API を有効化しています..."
    gcloud services enable run.googleapis.com --quiet
fi

# サービスアカウントに Cloud Run Jobs 実行権限を付与
log_info "サービスアカウントに Cloud Run Jobs 実行権限を付与しています..."
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${PIPELINE_SA_EMAIL}" \
    --role="roles/run.invoker" \
    --condition=None \
    --quiet 2>/dev/null || {
    log_warn "roles/run.invoker の付与をスキップしました（既に付与済みの可能性があります）"
}

# Cloud Run Jobs の作成または更新
if gcloud run jobs describe "${JOB_NAME}" --region="${GCP_REGION}" > /dev/null 2>&1; then
    log_info "既存のCloud Run Jobs '${JOB_NAME}' を更新します..."

    gcloud run jobs update "${JOB_NAME}" \
        --image="${IMAGE_URI}" \
        --region="${GCP_REGION}" \
        --service-account="${PIPELINE_SA_EMAIL}" \
        --memory=8Gi \
        --cpu=4 \
        --task-timeout=7200 \
        --max-retries=0 \
        --set-env-vars="GCP_PROJECT_ID=${GCP_PROJECT_ID},GCP_REGION=${GCP_REGION},GCS_BUCKET_MODELS=${GCS_BUCKET_MODELS:-keiba-models},BQ_DATASET_FEATURES=features,BQ_DATASET_PREDICTIONS=predictions,LOG_LEVEL=INFO" \
        --set-secrets="JRDB_USER=jrdb-user:latest,JRDB_PASSWORD=jrdb-password:latest" \
        --command="python" \
        --args="-m,src.models.train,--tune,--project-id,${GCP_PROJECT_ID}" \
        --quiet

    log_info "Cloud Run Jobs '${JOB_NAME}' を更新しました"
else
    log_info "Cloud Run Jobs '${JOB_NAME}' を新規作成します..."

    gcloud run jobs create "${JOB_NAME}" \
        --image="${IMAGE_URI}" \
        --region="${GCP_REGION}" \
        --service-account="${PIPELINE_SA_EMAIL}" \
        --memory=8Gi \
        --cpu=4 \
        --task-timeout=7200 \
        --max-retries=0 \
        --set-env-vars="GCP_PROJECT_ID=${GCP_PROJECT_ID},GCP_REGION=${GCP_REGION},GCS_BUCKET_MODELS=${GCS_BUCKET_MODELS:-keiba-models},BQ_DATASET_FEATURES=features,BQ_DATASET_PREDICTIONS=predictions,LOG_LEVEL=INFO" \
        --set-secrets="JRDB_USER=jrdb-user:latest,JRDB_PASSWORD=jrdb-password:latest" \
        --command="python" \
        --args="-m,src.models.train,--tune,--project-id,${GCP_PROJECT_ID}" \
        --quiet

    log_info "Cloud Run Jobs '${JOB_NAME}' を作成しました"
fi

# 作成されたジョブの確認
log_info "ジョブの確認..."
gcloud run jobs describe "${JOB_NAME}" \
    --region="${GCP_REGION}" \
    --format="table(metadata.name,spec.template.spec.template.spec.containers[0].image,spec.template.spec.taskCount,spec.template.spec.maxRetries)"

log_info ""
log_info "=========================================="
log_info "セットアップ完了"
log_info "=========================================="
log_info ""
log_info "次のステップ:"
log_info "  1. Cloud Schedulerを設定してください（weekly-model-retrain ジョブを作成）:"
log_info "     ./infrastructure/scripts/setup_scheduler.sh"
log_info ""
log_info "  2. 手動でモデル再学習をテストする場合:"
log_info "     gcloud run jobs execute ${JOB_NAME} --region=${GCP_REGION}"
log_info ""
log_info "  3. 実行中のジョブを確認する場合:"
log_info "     gcloud run jobs executions list --job=${JOB_NAME} --region=${GCP_REGION}"
log_info ""
