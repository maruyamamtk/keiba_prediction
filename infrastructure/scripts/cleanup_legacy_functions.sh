#!/bin/bash
#
# 旧Cloud Run Functions (gcs-to-bq) 削除スクリプト
#
# 使用方法:
#   ./infrastructure/scripts/cleanup_legacy_functions.sh
#
# 概要:
#   旧アーキテクチャで使用していたCloud Run Functions (gcs-to-bq) を削除する。
#   新アーキテクチャではDailyPipeline内でBigQueryロードまで一気通貫で実行するため不要。
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

# .envファイルの読み込み
if [ -f "${PROJECT_ROOT}/.env" ]; then
    log_info ".envファイルを読み込んでいます..."
    set -a
    source "${PROJECT_ROOT}/.env"
    set +a
else
    log_error ".envファイルが見つかりません: ${PROJECT_ROOT}/.env"
    exit 1
fi

if [ -z "${GCP_PROJECT_ID}" ]; then
    log_error "GCP_PROJECT_IDが設定されていません。.envファイルを確認してください。"
    exit 1
fi

# 設定
FUNCTION_NAME="gcs-to-bq"
GCP_REGION="${GCP_REGION:-asia-northeast1}"

log_info "=========================================="
log_info "旧Cloud Run Functions (${FUNCTION_NAME}) を削除します"
log_info "=========================================="
log_info "プロジェクトID: ${GCP_PROJECT_ID}"
log_info "リージョン: ${GCP_REGION}"
log_info "=========================================="

# Step 1: Cloud Run Functionsの状態確認
log_info "Cloud Run Functions (${FUNCTION_NAME}) の状態を確認しています..."

if gcloud functions describe "${FUNCTION_NAME}" \
    --region="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" > /dev/null 2>&1; then
    log_info "Cloud Run Functions (${FUNCTION_NAME}) が見つかりました"

    # 削除確認
    echo ""
    log_warn "この操作はCloud Run Functions (${FUNCTION_NAME}) を完全に削除します。"
    log_warn "新アーキテクチャ (keiba-pipeline) が正常動作していることを確認してから実行してください。"
    echo ""
    read -r -p "削除を続行しますか？ (y/N): " </dev/tty || REPLY="n"

    if [[ ! "${REPLY}" =~ ^[Yy]$ ]]; then
        log_info "削除をキャンセルしました"
        exit 0
    fi

    # Step 2: Cloud Run Functionsの削除
    log_info "Cloud Run Functions (${FUNCTION_NAME}) を削除しています..."

    gcloud functions delete "${FUNCTION_NAME}" \
        --region="${GCP_REGION}" \
        --project="${GCP_PROJECT_ID}" \
        --quiet

    log_info "Cloud Run Functions (${FUNCTION_NAME}) を削除しました"
else
    log_info "Cloud Run Functions (${FUNCTION_NAME}) は存在しません（削除済みまたは未デプロイ）"
fi

# Step 3: 関連するEventarcトリガーの確認・削除
log_info "関連するEventarcトリガーを確認しています..."

EVENTARC_TRIGGERS=$(gcloud eventarc triggers list \
    --location="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    --filter="name:${FUNCTION_NAME}" \
    --format="value(name)" 2>/dev/null || echo "")

if [ -n "${EVENTARC_TRIGGERS}" ]; then
    log_info "関連するEventarcトリガーが見つかりました:"
    echo "${EVENTARC_TRIGGERS}"

    for trigger in ${EVENTARC_TRIGGERS}; do
        trigger_name=$(basename "${trigger}")
        log_info "Eventarcトリガー (${trigger_name}) を削除しています..."
        gcloud eventarc triggers delete "${trigger_name}" \
            --location="${GCP_REGION}" \
            --project="${GCP_PROJECT_ID}" \
            --quiet 2>/dev/null || \
            log_warn "Eventarcトリガー (${trigger_name}) の削除をスキップしました"
    done
else
    log_info "関連するEventarcトリガーはありません"
fi

# Step 4: 完了
log_info "=========================================="
log_info "クリーンアップ完了"
log_info "=========================================="
log_info ""
log_info "削除後の確認:"
log_info "  1. Cloud Run Functions一覧:"
log_info "     gcloud functions list --project=${GCP_PROJECT_ID}"
log_info "  2. Eventarcトリガー一覧:"
log_info "     gcloud eventarc triggers list --location=${GCP_REGION} --project=${GCP_PROJECT_ID}"
log_info "  3. 新パイプラインの動作確認:"
log_info "     gcloud scheduler jobs run daily-data-pipeline --location=${GCP_REGION}"
