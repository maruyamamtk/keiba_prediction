#!/bin/bash
#
# 月次モデル再学習の環境修正スクリプト
#
# 以下の2つの問題を修正します:
#   1. モデルバケットへの書き込み権限がない（objectViewer → objectAdmin）
#   2. monthly-model-retrain Cloud Schedulerジョブが存在しない場合の作成
#
# 使用方法:
#   ./scripts/fix_retrain_setup.sh
#

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -f "${PROJECT_ROOT}/.env" ]; then
    set -a
    source "${PROJECT_ROOT}/.env"
    set +a
else
    log_error ".envファイルが見つかりません"
    exit 1
fi

if [ -z "${GCP_PROJECT_ID}" ] || [ "${GCP_PROJECT_ID}" = "your-project-id" ]; then
    log_error "GCP_PROJECT_IDが設定されていません"
    exit 1
fi

GCP_REGION="${GCP_REGION:-asia-northeast1}"
PIPELINE_SA_NAME="${PIPELINE_SA_NAME:-keiba-pipeline-sa}"
PIPELINE_SA_EMAIL="${PIPELINE_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
GCS_BUCKET_MODELS_FULL="${GCP_PROJECT_ID}-${GCS_BUCKET_MODELS:-keiba-models}"
SERVICE_NAME="keiba-pipeline"

log_info "=========================================="
log_info "月次モデル再学習の環境修正"
log_info "=========================================="
log_info "プロジェクトID : ${GCP_PROJECT_ID}"
log_info "サービスアカウント: ${PIPELINE_SA_EMAIL}"
log_info "モデルバケット  : gs://${GCS_BUCKET_MODELS_FULL}"
log_info "=========================================="

# ----------------------------------------
# 修正1: モデルバケットへの書き込み権限付与
# ----------------------------------------
log_info "【修正1】モデルバケットに objectAdmin 権限を付与します..."
log_info "  月次再学習(POST /api/v1/model/retrain/async)でGCSにモデルを書き込むために必要です"

gsutil iam ch \
    "serviceAccount:${PIPELINE_SA_EMAIL}:roles/storage.objectAdmin" \
    "gs://${GCS_BUCKET_MODELS_FULL}" && \
    log_info "  権限付与に成功しました: gs://${GCS_BUCKET_MODELS_FULL}" || \
    log_warn "  権限付与に失敗しました（バケットが存在しない、または権限不足の可能性）"

# ----------------------------------------
# 修正2: monthly-model-retrain ジョブの確認と作成
# ----------------------------------------
log_info ""
log_info "【修正2】monthly-model-retrain Cloud Schedulerジョブを確認します..."

SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
    --region="${GCP_REGION}" \
    --format="value(status.url)" 2>/dev/null)

if [ -z "${SERVICE_URL}" ]; then
    log_error "Cloud Runサービスが見つかりません: ${SERVICE_NAME}"
    log_error "先にデプロイしてください: ./infrastructure/scripts/deploy_cloud_run.sh"
    exit 1
fi

RETRAIN_JOB_NAME="monthly-model-retrain"
RETRAIN_TARGET_URI="${SERVICE_URL}/api/v1/model/retrain/async"
RETRAIN_SCHEDULE="0 8 1-7 * 1"
TIME_ZONE="Asia/Tokyo"

if gcloud scheduler jobs describe "${RETRAIN_JOB_NAME}" \
    --location="${GCP_REGION}" > /dev/null 2>&1; then
    log_info "  ジョブは既に存在します: ${RETRAIN_JOB_NAME}"
    gcloud scheduler jobs describe "${RETRAIN_JOB_NAME}" \
        --location="${GCP_REGION}" \
        --format="table(name,schedule,timeZone,state,httpTarget.uri)"
else
    log_warn "  ジョブが存在しません。作成します: ${RETRAIN_JOB_NAME}"

    tmp_body=$(mktemp /tmp/scheduler_body_XXXXXX.json)
    printf '{}' > "${tmp_body}"

    gcloud scheduler jobs create http "${RETRAIN_JOB_NAME}" \
        --location="${GCP_REGION}" \
        --schedule="${RETRAIN_SCHEDULE}" \
        --time-zone="${TIME_ZONE}" \
        --uri="${RETRAIN_TARGET_URI}" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --message-body-from-file="${tmp_body}" \
        --oidc-service-account-email="${PIPELINE_SA_EMAIL}" \
        --oidc-token-audience="${SERVICE_URL}" \
        --attempt-deadline="60s" \
        --max-retry-attempts=3 \
        --min-backoff=5s \
        --max-backoff=300s \
        --quiet

    rm -f "${tmp_body}"
    log_info "  ジョブを作成しました: ${RETRAIN_JOB_NAME} (${RETRAIN_SCHEDULE} JST)"
fi

# ----------------------------------------
# 修正3: Cloud Run に --no-cpu-throttling が設定されているか確認
# ----------------------------------------
log_info ""
log_info "【確認】Cloud Runの--no-cpu-throttlingを確認します..."

CPU_THROTTLING=$(gcloud run services describe "${SERVICE_NAME}" \
    --region="${GCP_REGION}" \
    --format="value(spec.template.metadata.annotations['run.googleapis.com/cpu-throttling'])" 2>/dev/null)

if [ "${CPU_THROTTLING}" = "false" ]; then
    log_info "  --no-cpu-throttling は設定済みです（バックグラウンドタスクが正常に動作します）"
else
    log_warn "  --no-cpu-throttling が設定されていません！"
    log_warn "  バックグラウンドで動作するモデル再学習がCPU throttlingで中断される可能性があります。"
    log_warn "  修正するには再デプロイが必要です:"
    log_warn "    ./infrastructure/scripts/deploy_cloud_run.sh"
    log_warn ""
    log_warn "  または今すぐ gcloud で更新する場合:"
    log_warn "    gcloud run services update ${SERVICE_NAME} \\"
    log_warn "      --region=${GCP_REGION} \\"
    log_warn "      --no-cpu-throttling"
fi

# ----------------------------------------
# 完了サマリ
# ----------------------------------------
log_info ""
log_info "=========================================="
log_info "修正完了"
log_info "=========================================="
log_info ""
log_info "今すぐ再学習をトリガーする場合:"
log_info "  gcloud scheduler jobs run ${RETRAIN_JOB_NAME} --location=${GCP_REGION}"
log_info ""
log_info "または直接APIを呼び出す場合:"
log_info "  SERVICE_URL=\$(gcloud run services describe ${SERVICE_NAME} --region=${GCP_REGION} --format='value(status.url)')"
log_info "  curl -X POST \"\${SERVICE_URL}/api/v1/model/retrain/async\" \\"
log_info "    -H \"Authorization: Bearer \$(gcloud auth print-identity-token)\" \\"
log_info "    -H \"Content-Type: application/json\" \\"
log_info "    -d '{}'"
log_info ""
log_info "再学習ログの確認:"
log_info "  gcloud run services logs read ${SERVICE_NAME} --region=${GCP_REGION} --limit=50"
