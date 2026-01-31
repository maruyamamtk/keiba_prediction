#!/bin/bash
#
# GCPセットアップ確認スクリプト
#
# setup_gcp.sh 実行後に、設定が正しく行われたかを確認します。
#

set -e

# 色付きログ出力
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_ok() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_ng() {
    echo -e "${RED}[NG]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# .envファイルを読み込み
if [ -f "${PROJECT_ROOT}/.env" ]; then
    set -a
    source "${PROJECT_ROOT}/.env"
    set +a
else
    log_ng ".envファイルが見つかりません"
    exit 1
fi

# デフォルト値
GCP_REGION="${GCP_REGION:-asia-northeast1}"
PIPELINE_SA_NAME="${PIPELINE_SA_NAME:-keiba-pipeline-sa}"
PIPELINE_SA_EMAIL="${PIPELINE_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

echo "=========================================="
echo "GCPセットアップ確認"
echo "=========================================="
echo "プロジェクトID: ${GCP_PROJECT_ID}"
echo "リージョン: ${GCP_REGION}"
echo "=========================================="
echo ""

ERRORS=0

# ========================================
# 1. プロジェクト確認
# ========================================
echo "1. プロジェクト確認"
if gcloud projects describe "${GCP_PROJECT_ID}" &>/dev/null; then
    log_ok "プロジェクト ${GCP_PROJECT_ID} が存在します"
else
    log_ng "プロジェクト ${GCP_PROJECT_ID} が見つかりません"
    ERRORS=$((ERRORS + 1))
fi
echo ""

# ========================================
# 2. API確認
# ========================================
echo "2. API有効化確認"

APIS=(
    "run.googleapis.com"
    "cloudbuild.googleapis.com"
    "artifactregistry.googleapis.com"
    "cloudscheduler.googleapis.com"
    "secretmanager.googleapis.com"
    "storage.googleapis.com"
    "bigquery.googleapis.com"
)

for api in "${APIS[@]}"; do
    if gcloud services list --enabled --filter="name:${api}" --format="value(name)" 2>/dev/null | grep -q "${api}"; then
        log_ok "${api}"
    else
        log_ng "${api} が有効化されていません"
        ERRORS=$((ERRORS + 1))
    fi
done
echo ""

# ========================================
# 3. Artifact Registry確認
# ========================================
echo "3. Artifact Registry確認"

REPO_NAME="keiba-pipeline"
if gcloud artifacts repositories describe "${REPO_NAME}" \
    --location="${GCP_REGION}" \
    --format="value(name)" 2>/dev/null; then
    log_ok "リポジトリ ${REPO_NAME} が存在します"
else
    log_ng "リポジトリ ${REPO_NAME} が見つかりません"
    ERRORS=$((ERRORS + 1))
fi
echo ""

# ========================================
# 4. サービスアカウント確認
# ========================================
echo "4. サービスアカウント確認"

if gcloud iam service-accounts describe "${PIPELINE_SA_EMAIL}" 2>/dev/null; then
    log_ok "サービスアカウント ${PIPELINE_SA_NAME} が存在します"
else
    log_ng "サービスアカウント ${PIPELINE_SA_NAME} が見つかりません"
    ERRORS=$((ERRORS + 1))
fi
echo ""

# ========================================
# 5. 権限確認
# ========================================
echo "5. サービスアカウント権限確認"

ROLES=(
    "roles/storage.objectAdmin"
    "roles/bigquery.dataEditor"
    "roles/bigquery.jobUser"
    "roles/secretmanager.secretAccessor"
    "roles/logging.logWriter"
)

IAM_POLICY=$(gcloud projects get-iam-policy "${GCP_PROJECT_ID}" --format=json 2>/dev/null)

for role in "${ROLES[@]}"; do
    if echo "${IAM_POLICY}" | grep -q "\"role\": \"${role}\"" && \
       echo "${IAM_POLICY}" | grep -q "serviceAccount:${PIPELINE_SA_EMAIL}"; then
        log_ok "${role}"
    else
        log_warn "${role} の確認ができませんでした（付与済みの可能性あり）"
    fi
done
echo ""

# ========================================
# 6. Secret Manager確認
# ========================================
echo "6. Secret Manager確認"

SECRETS=("jrdb-user" "jrdb-password")

for secret in "${SECRETS[@]}"; do
    if gcloud secrets describe "${secret}" 2>/dev/null; then
        # バージョン数を確認
        VERSION_COUNT=$(gcloud secrets versions list "${secret}" --format="value(name)" 2>/dev/null | wc -l | tr -d ' ')
        log_ok "シークレット ${secret} が存在します (${VERSION_COUNT} バージョン)"
    else
        log_warn "シークレット ${secret} が見つかりません（手動設定が必要）"
    fi
done
echo ""

# ========================================
# 結果サマリ
# ========================================
echo "=========================================="
if [ ${ERRORS} -eq 0 ]; then
    log_ok "すべてのチェックが完了しました"
    echo "=========================================="
    echo ""
    echo "次のステップ:"
    echo "  - Issue #44: JRDBダウンローダーのコンテナ化"
    echo ""
else
    log_ng "${ERRORS} 件のエラーがあります"
    echo "=========================================="
    echo ""
    echo "エラーを修正してから再度確認してください。"
    exit 1
fi
