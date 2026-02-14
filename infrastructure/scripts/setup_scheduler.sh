#!/bin/bash
#
# Cloud Schedulerジョブセットアップスクリプト
#
# Cloud Runサービス keiba-pipeline の日次データパイプラインを
# 毎日AM 6:00 JSTに自動実行するCloud Schedulerジョブを作成する。
#
# 使用方法:
#   ./infrastructure/scripts/setup_scheduler.sh
#
# 前提条件:
#   1. gcloud CLIがインストール済み
#   2. 適切な権限を持つユーザーでログイン済み (gcloud auth login)
#   3. .envファイルにGCP_PROJECT_IDが設定済み
#   4. Cloud Runサービス keiba-pipeline がデプロイ済み
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
SERVICE_NAME="keiba-pipeline"
JOB_NAME="daily-data-pipeline"
SCHEDULE="0 6 * * *"
TIME_ZONE="Asia/Tokyo"

# サービスアカウントのメールアドレス
PIPELINE_SA_EMAIL="${PIPELINE_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

log_info "=========================================="
log_info "Cloud Schedulerジョブをセットアップします"
log_info "=========================================="
log_info "プロジェクトID: ${GCP_PROJECT_ID}"
log_info "リージョン: ${GCP_REGION}"
log_info "ジョブ名: ${JOB_NAME}"
log_info "スケジュール: ${SCHEDULE} (${TIME_ZONE})"
log_info "サービスアカウント: ${PIPELINE_SA_EMAIL}"
log_info "=========================================="

# ========================================
# 1. Cloud Runサービスの存在確認とURL取得
# ========================================
log_info "Cloud Runサービスを確認しています..."

SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
    --region="${GCP_REGION}" \
    --format="value(status.url)" 2>/dev/null)

if [ -z "${SERVICE_URL}" ]; then
    log_error "Cloud Runサービスが見つかりません: ${SERVICE_NAME}"
    log_error "先にCloud Runにデプロイしてください:"
    log_error "  ./infrastructure/scripts/deploy_cloud_run.sh"
    exit 1
fi

log_info "サービスURL: ${SERVICE_URL}"

# ターゲットURL（非同期エンドポイント）
TARGET_URI="${SERVICE_URL}/api/v1/load/daily/async"
log_info "ターゲットURI: ${TARGET_URI}"

# ========================================
# 2. Cloud Scheduler APIの有効化確認
# ========================================
log_info "Cloud Scheduler APIを確認しています..."

if ! gcloud services list --enabled --filter="name:cloudscheduler.googleapis.com" --format="value(name)" 2>/dev/null | grep -q "cloudscheduler"; then
    log_info "Cloud Scheduler APIを有効化しています..."
    gcloud services enable cloudscheduler.googleapis.com --quiet
    log_info "Cloud Scheduler APIを有効化しました"
else
    log_info "Cloud Scheduler APIは既に有効です"
fi

# ========================================
# 3. サービスアカウントにCloud Run起動権限を付与
# ========================================
log_info "サービスアカウントにCloud Run起動権限を付与しています..."

# Cloud Run Invoker権限を付与（Cloud SchedulerがCloud Runを呼び出すために必要）
gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
    --region="${GCP_REGION}" \
    --member="serviceAccount:${PIPELINE_SA_EMAIL}" \
    --role="roles/run.invoker" \
    --quiet 2>/dev/null || {
    log_warn "Cloud Run Invoker権限の付与に失敗しました（既に付与されている可能性があります）"
}

log_info "権限設定が完了しました"

# ========================================
# 4. Cloud Schedulerジョブの作成/更新
# ========================================
log_info "Cloud Schedulerジョブを設定しています..."

# 既存ジョブの確認
if gcloud scheduler jobs describe "${JOB_NAME}" \
    --location="${GCP_REGION}" 2>/dev/null; then
    log_info "既存のジョブ ${JOB_NAME} を更新します..."

    gcloud scheduler jobs update http "${JOB_NAME}" \
        --location="${GCP_REGION}" \
        --schedule="${SCHEDULE}" \
        --time-zone="${TIME_ZONE}" \
        --uri="${TARGET_URI}" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --oidc-service-account-email="${PIPELINE_SA_EMAIL}" \
        --oidc-token-audience="${SERVICE_URL}" \
        --attempt-deadline=900s \
        --max-retry-attempts=3 \
        --min-backoff=5s \
        --max-backoff=300s \
        --quiet

    log_info "ジョブを更新しました"
else
    log_info "新しいジョブ ${JOB_NAME} を作成します..."

    gcloud scheduler jobs create http "${JOB_NAME}" \
        --location="${GCP_REGION}" \
        --schedule="${SCHEDULE}" \
        --time-zone="${TIME_ZONE}" \
        --uri="${TARGET_URI}" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --oidc-service-account-email="${PIPELINE_SA_EMAIL}" \
        --oidc-token-audience="${SERVICE_URL}" \
        --attempt-deadline=900s \
        --max-retry-attempts=3 \
        --min-backoff=5s \
        --max-backoff=300s \
        --quiet

    log_info "ジョブを作成しました"
fi

# ========================================
# 5. ジョブの確認
# ========================================
log_info ""
log_info "=========================================="
log_info "セットアップ完了"
log_info "=========================================="
log_info ""
log_info "ジョブ情報:"

gcloud scheduler jobs describe "${JOB_NAME}" \
    --location="${GCP_REGION}" \
    --format="table(name,schedule,timeZone,state,httpTarget.uri)"

log_info ""
log_info "手動実行でテストする場合:"
log_info "  gcloud scheduler jobs run ${JOB_NAME} --location=${GCP_REGION}"
log_info ""
log_info "ジョブの実行履歴を確認する場合:"
log_info "  gcloud logging read 'resource.type=\"cloud_scheduler_job\" AND resource.labels.job_id=\"${JOB_NAME}\"' --limit=10"
log_info ""
log_info "ジョブを一時停止する場合:"
log_info "  gcloud scheduler jobs pause ${JOB_NAME} --location=${GCP_REGION}"
log_info ""
log_info "ジョブを再開する場合:"
log_info "  gcloud scheduler jobs resume ${JOB_NAME} --location=${GCP_REGION}"
log_info ""
