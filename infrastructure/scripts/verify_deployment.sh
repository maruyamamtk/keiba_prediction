#!/bin/bash
#
# Cloud Runデプロイ後の動作確認スクリプト
#
# デプロイ済みのCloud Runサービスに対して各エンドポイントの疎通確認を行う。
#
# 使用方法:
#   ./infrastructure/scripts/verify_deployment.sh
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

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
}

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# .envファイルを読み込み
if [ -f "${PROJECT_ROOT}/.env" ]; then
    set -a
    source "${PROJECT_ROOT}/.env"
    set +a
fi

# 必須変数のチェック
GCP_REGION="${GCP_REGION:-asia-northeast1}"
SERVICE_NAME="keiba-pipeline"

log_info "=========================================="
log_info "Cloud Runサービスの動作確認"
log_info "=========================================="

# サービス情報の取得
log_info "サービス情報を取得しています..."
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
    --region="${GCP_REGION}" \
    --format="value(status.url)" 2>/dev/null)

if [ -z "${SERVICE_URL}" ]; then
    log_error "サービスが見つかりません: ${SERVICE_NAME}"
    exit 1
fi

log_info "サービスURL: ${SERVICE_URL}"

# 認証トークンの取得
TOKEN=$(gcloud auth print-identity-token 2>/dev/null)
if [ -z "${TOKEN}" ]; then
    log_error "認証トークンの取得に失敗しました。gcloud auth loginを実行してください。"
    exit 1
fi

PASSED=0
FAILED=0

# ========================================
# 1. ルートエンドポイント (GET /)
# ========================================
log_info ""
log_info "1. ルートエンドポイント (GET /) をテスト..."

HTTP_CODE=$(curl -s -o /tmp/verify_root.json -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" \
    "${SERVICE_URL}/")

if [ "${HTTP_CODE}" = "200" ]; then
    log_success "GET / -> ${HTTP_CODE}"
    cat /tmp/verify_root.json | python3 -m json.tool 2>/dev/null || cat /tmp/verify_root.json
    PASSED=$((PASSED + 1))
else
    log_fail "GET / -> ${HTTP_CODE}"
    cat /tmp/verify_root.json 2>/dev/null
    FAILED=$((FAILED + 1))
fi

# ========================================
# 2. ヘルスチェック (GET /health)
# ========================================
log_info ""
log_info "2. ヘルスチェック (GET /health) をテスト..."

HTTP_CODE=$(curl -s -o /tmp/verify_health.json -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" \
    "${SERVICE_URL}/health")

if [ "${HTTP_CODE}" = "200" ]; then
    log_success "GET /health -> ${HTTP_CODE}"
    cat /tmp/verify_health.json | python3 -m json.tool 2>/dev/null || cat /tmp/verify_health.json
    PASSED=$((PASSED + 1))
else
    log_fail "GET /health -> ${HTTP_CODE}"
    cat /tmp/verify_health.json 2>/dev/null
    FAILED=$((FAILED + 1))
fi

# ========================================
# 3. 日次ロードエンドポイント (POST /api/v1/load/daily)
# ========================================
log_info ""
log_info "3. 日次ロード (POST /api/v1/load/daily) をテスト..."
log_info "   ※ 特定日付を指定してテスト実行（実データのダウンロード・ロードが実行されます）"

read -p "日次ロードのテスト実行をスキップしますか？ (y/N): " SKIP_DAILY
if [ "${SKIP_DAILY}" = "y" ] || [ "${SKIP_DAILY}" = "Y" ]; then
    log_warn "日次ロードテストをスキップしました"
else
    # 直近の土曜日を指定（データが存在する可能性が高い）
    TEST_DATE=$(date -v-saturday +%Y-%m-%d 2>/dev/null || date -d "last saturday" +%Y-%m-%d 2>/dev/null || echo "2026-02-08")
    log_info "   テスト日付: ${TEST_DATE}"

    HTTP_CODE=$(curl -s -o /tmp/verify_daily.json -w "%{http_code}" \
        -X POST \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"target_date\": \"${TEST_DATE}\"}" \
        --max-time 120 \
        "${SERVICE_URL}/api/v1/load/daily")

    if [ "${HTTP_CODE}" = "200" ]; then
        log_success "POST /api/v1/load/daily -> ${HTTP_CODE}"
        cat /tmp/verify_daily.json | python3 -m json.tool 2>/dev/null || cat /tmp/verify_daily.json
        PASSED=$((PASSED + 1))
    else
        log_fail "POST /api/v1/load/daily -> ${HTTP_CODE}"
        cat /tmp/verify_daily.json 2>/dev/null
        FAILED=$((FAILED + 1))
    fi
fi

# ========================================
# 4. OpenAPIドキュメント (GET /docs)
# ========================================
log_info ""
log_info "4. OpenAPIドキュメント (GET /docs) をテスト..."

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" \
    "${SERVICE_URL}/docs")

if [ "${HTTP_CODE}" = "200" ]; then
    log_success "GET /docs -> ${HTTP_CODE}"
    PASSED=$((PASSED + 1))
else
    log_fail "GET /docs -> ${HTTP_CODE}"
    FAILED=$((FAILED + 1))
fi

# ========================================
# 結果サマリ
# ========================================
log_info ""
log_info "=========================================="
log_info "動作確認結果"
log_info "=========================================="
log_info "成功: ${PASSED}"
if [ "${FAILED}" -gt 0 ]; then
    log_error "失敗: ${FAILED}"
else
    log_info "失敗: ${FAILED}"
fi
log_info "=========================================="

# 一時ファイルの削除
rm -f /tmp/verify_root.json /tmp/verify_health.json /tmp/verify_daily.json

if [ "${FAILED}" -gt 0 ]; then
    log_error "一部のテストが失敗しました。Cloud Runのログを確認してください。"
    log_info "  gcloud run services logs read ${SERVICE_NAME} --region=${GCP_REGION} --limit=50"
    exit 1
fi

log_info ""
log_success "すべてのテストに成功しました！"
log_info ""
log_info "次のステップ:"
log_info "  - Cloud Schedulerジョブのエンドポイントを更新 (Issue #60)"
log_info "    旧: POST /daily-load"
log_info "    新: POST /api/v1/load/daily/async"
log_info ""
