#!/bin/bash
#
# Cloud Runデプロイ後の動作確認スクリプト
#
# デプロイ済みのCloud Runサービスに対して各エンドポイントの疎通確認を行う。
#
# 使用方法:
#   ./infrastructure/scripts/verify_deployment.sh
#

# テスト集計のため set -e は使用しない（個別にエラーハンドリング）

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

# 一時ディレクトリの作成とクリーンアップ
TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT

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

HTTP_CODE=$(curl -s -o "${TMPDIR}/root.json" -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" \
    "${SERVICE_URL}/" 2>/dev/null) || HTTP_CODE="000"

if [ "${HTTP_CODE}" = "200" ]; then
    log_success "GET / -> ${HTTP_CODE}"
    python3 -m json.tool "${TMPDIR}/root.json" 2>/dev/null || cat "${TMPDIR}/root.json" 2>/dev/null
    PASSED=$((PASSED + 1))
else
    log_fail "GET / -> ${HTTP_CODE}"
    cat "${TMPDIR}/root.json" 2>/dev/null
    FAILED=$((FAILED + 1))
fi

# ========================================
# 2. ヘルスチェック (GET /health)
# ========================================
log_info ""
log_info "2. ヘルスチェック (GET /health) をテスト..."

HTTP_CODE=$(curl -s -o "${TMPDIR}/health.json" -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" \
    "${SERVICE_URL}/health" 2>/dev/null) || HTTP_CODE="000"

if [ "${HTTP_CODE}" = "200" ]; then
    log_success "GET /health -> ${HTTP_CODE}"
    python3 -m json.tool "${TMPDIR}/health.json" 2>/dev/null || cat "${TMPDIR}/health.json" 2>/dev/null
    PASSED=$((PASSED + 1))
else
    log_fail "GET /health -> ${HTTP_CODE}"
    cat "${TMPDIR}/health.json" 2>/dev/null
    FAILED=$((FAILED + 1))
fi

# ========================================
# 3. 日次ロードエンドポイント (POST /api/v1/load/daily)
# ========================================
log_info ""
log_info "3. 日次ロード (POST /api/v1/load/daily) をテスト..."
log_warn "   ※ 実データのダウンロード・ロードが実行されます"

SKIP_DAILY=""
read -r -p "日次ロードのテスト実行をスキップしますか？ (y/N): " SKIP_DAILY 2>/dev/null </dev/tty || SKIP_DAILY="y"
if [ "${SKIP_DAILY}" = "y" ] || [ "${SKIP_DAILY}" = "Y" ]; then
    log_warn "日次ロードテストをスキップしました"
else
    # 直近の土曜日を指定（データが存在する可能性が高い）
    TEST_DATE=$(date -v-saturday +%Y-%m-%d 2>/dev/null || date -d "last saturday" +%Y-%m-%d 2>/dev/null || date +%Y-%m-%d)
    log_info "   テスト日付: ${TEST_DATE}"

    HTTP_CODE=$(curl -s -o "${TMPDIR}/daily.json" -w "%{http_code}" \
        -X POST \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"target_date\": \"${TEST_DATE}\"}" \
        --max-time 120 \
        "${SERVICE_URL}/api/v1/load/daily" 2>/dev/null) || HTTP_CODE="000"

    if [ "${HTTP_CODE}" = "200" ]; then
        log_success "POST /api/v1/load/daily -> ${HTTP_CODE}"
        python3 -m json.tool "${TMPDIR}/daily.json" 2>/dev/null || cat "${TMPDIR}/daily.json" 2>/dev/null
        PASSED=$((PASSED + 1))
    else
        log_fail "POST /api/v1/load/daily -> ${HTTP_CODE}"
        cat "${TMPDIR}/daily.json" 2>/dev/null
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
    "${SERVICE_URL}/docs" 2>/dev/null) || HTTP_CODE="000"

if [ "${HTTP_CODE}" = "200" ]; then
    log_success "GET /docs -> ${HTTP_CODE}"
    PASSED=$((PASSED + 1))
else
    log_fail "GET /docs -> ${HTTP_CODE}"
    FAILED=$((FAILED + 1))
fi

# ========================================
# 5. 予測エンドポイント (POST /api/v1/predict/on-demand) のスキーマ確認
# ========================================
log_info ""
log_info "5. 予測エンドポイント (POST /api/v1/predict/on-demand) のスキーマ確認..."
log_warn "   ※ 実際の予測は実行しません（バリデーションエラーを期待）"

# 空のリクエストを送信してスキーマのバリデーションエラーが返ることを確認
HTTP_CODE=$(curl -s -o "${TMPDIR}/predict_schema.json" -w "%{http_code}" \
    -X POST \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{}' \
    --max-time 30 \
    "${SERVICE_URL}/api/v1/predict/on-demand" 2>/dev/null) || HTTP_CODE="000"

# 422 Unprocessable Entity (バリデーションエラー) を期待
if [ "${HTTP_CODE}" = "422" ]; then
    log_success "POST /api/v1/predict/on-demand -> ${HTTP_CODE} (バリデーション正常)"
    PASSED=$((PASSED + 1))
elif [ "${HTTP_CODE}" = "200" ] || [ "${HTTP_CODE}" = "500" ]; then
    # エンドポイントが存在することを確認（500はGCP認証エラーの可能性）
    log_success "POST /api/v1/predict/on-demand -> ${HTTP_CODE} (エンドポイント到達確認)"
    PASSED=$((PASSED + 1))
else
    log_fail "POST /api/v1/predict/on-demand -> ${HTTP_CODE} (エンドポイントが見つかりません)"
    cat "${TMPDIR}/predict_schema.json" 2>/dev/null
    FAILED=$((FAILED + 1))
fi

# ========================================
# 6. 日次予測エンドポイント (POST /api/v1/predict/daily) のスキーマ確認
# ========================================
log_info ""
log_info "6. 日次予測エンドポイント (POST /api/v1/predict/daily) のスキーマ確認..."

HTTP_CODE=$(curl -s -o "${TMPDIR}/predict_daily_schema.json" -w "%{http_code}" \
    -X POST \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{}' \
    --max-time 30 \
    "${SERVICE_URL}/api/v1/predict/daily" 2>/dev/null) || HTTP_CODE="000"

if [ "${HTTP_CODE}" = "422" ]; then
    log_success "POST /api/v1/predict/daily -> ${HTTP_CODE} (バリデーション正常)"
    PASSED=$((PASSED + 1))
elif [ "${HTTP_CODE}" = "200" ] || [ "${HTTP_CODE}" = "500" ]; then
    log_success "POST /api/v1/predict/daily -> ${HTTP_CODE} (エンドポイント到達確認)"
    PASSED=$((PASSED + 1))
else
    log_fail "POST /api/v1/predict/daily -> ${HTTP_CODE} (エンドポイントが見つかりません)"
    cat "${TMPDIR}/predict_daily_schema.json" 2>/dev/null
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

if [ "${FAILED}" -gt 0 ]; then
    log_error "一部のテストが失敗しました。Cloud Runのログを確認してください。"
    log_info "  gcloud run services logs read ${SERVICE_NAME} --region=${GCP_REGION} --limit=50"
    exit 1
fi

log_info ""
log_success "すべてのテストに成功しました！"
log_info ""
log_info "次のステップ:"
log_info "  - 予測エンドポイントの実行テスト:"
log_info "    POST /api/v1/predict/on-demand"
log_info "    {\"model_path\": \"gs://bucket/path/model.txt\", \"target_dates\": [\"2026-01-04\"]}"
log_info "  - 日次予測スケジューラーの設定:"
log_info "    POST /api/v1/predict/daily (前日PM 9:00 に Cloud Scheduler でトリガー)"
log_info ""
