#!/bin/bash
#
# GCPインフラセットアップスクリプト
#
# 実行前に以下を確認してください：
# 1. gcloud CLIがインストール済み
# 2. 適切な権限を持つユーザーでログイン済み (gcloud auth login)
# 3. .envファイルにGCP_PROJECT_IDが設定済み
#

set -e

# 色付きログ出力
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

log_info "=========================================="
log_info "GCPインフラセットアップを開始します"
log_info "=========================================="
log_info "プロジェクトID: ${GCP_PROJECT_ID}"
log_info "リージョン: ${GCP_REGION}"
log_info "サービスアカウント: ${PIPELINE_SA_NAME}"
log_info "=========================================="

# プロジェクトを設定
log_info "GCPプロジェクトを設定しています..."
gcloud config set project "${GCP_PROJECT_ID}"

# ========================================
# 1. 必要なAPIの有効化
# ========================================
log_info "必要なGCP APIを有効化しています..."

APIS=(
    "run.googleapis.com"              # Cloud Run
    "cloudbuild.googleapis.com"       # Cloud Build
    "artifactregistry.googleapis.com" # Artifact Registry
    "cloudscheduler.googleapis.com"   # Cloud Scheduler
    "secretmanager.googleapis.com"    # Secret Manager
    "storage.googleapis.com"          # Cloud Storage
    "bigquery.googleapis.com"         # BigQuery
    "cloudfunctions.googleapis.com"   # Cloud Functions
    "logging.googleapis.com"          # Cloud Logging
    "monitoring.googleapis.com"       # Cloud Monitoring
)

for api in "${APIS[@]}"; do
    log_info "  有効化中: ${api}"
    gcloud services enable "${api}" --quiet || {
        log_warn "  ${api} の有効化に失敗しました（既に有効な可能性があります）"
    }
done

log_info "API有効化が完了しました"

# ========================================
# 2. Artifact Registry リポジトリ作成
# ========================================
log_info "Artifact Registryリポジトリを作成しています..."

REPO_NAME="keiba-pipeline"

if gcloud artifacts repositories describe "${REPO_NAME}" \
    --location="${GCP_REGION}" \
    --format="value(name)" 2>/dev/null; then
    log_info "  リポジトリ ${REPO_NAME} は既に存在します"
else
    gcloud artifacts repositories create "${REPO_NAME}" \
        --repository-format=docker \
        --location="${GCP_REGION}" \
        --description="競馬予測パイプライン用Dockerイメージ"
    log_info "  リポジトリ ${REPO_NAME} を作成しました"
fi

# ========================================
# 3. サービスアカウントの作成
# ========================================
log_info "サービスアカウントを作成しています..."

PIPELINE_SA_EMAIL="${PIPELINE_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

if gcloud iam service-accounts describe "${PIPELINE_SA_EMAIL}" 2>/dev/null; then
    log_info "  サービスアカウント ${PIPELINE_SA_NAME} は既に存在します"
else
    gcloud iam service-accounts create "${PIPELINE_SA_NAME}" \
        --display-name="競馬予測パイプライン用サービスアカウント" \
        --description="Cloud Run上でデータパイプラインを実行するためのサービスアカウント"
    log_info "  サービスアカウント ${PIPELINE_SA_NAME} を作成しました"
fi

# ========================================
# 4. サービスアカウントへの権限付与
# ========================================
log_info "サービスアカウントに権限を付与しています..."

ROLES=(
    "roles/storage.objectAdmin"           # GCS読み書き
    "roles/bigquery.dataEditor"           # BigQuery読み書き
    "roles/bigquery.jobUser"              # BigQueryジョブ実行
    "roles/secretmanager.secretAccessor"  # Secret Manager読み取り
    "roles/logging.logWriter"             # Cloud Logging書き込み
    "roles/monitoring.metricWriter"       # Cloud Monitoring書き込み
)

for role in "${ROLES[@]}"; do
    log_info "  付与中: ${role}"
    gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
        --member="serviceAccount:${PIPELINE_SA_EMAIL}" \
        --role="${role}" \
        --quiet \
        --condition=None 2>/dev/null || {
        log_warn "  ${role} の付与に失敗しました（既に付与されている可能性があります）"
    }
done

log_info "権限付与が完了しました"

# ========================================
# 5. Secret Managerへのシークレット登録
# ========================================
log_info "Secret Managerにシークレットを登録しています..."

# JRDB認証情報のチェック
if [ -z "${JRDB_USER}" ] || [ "${JRDB_USER}" = "your-jrdb-user" ]; then
    log_warn "JRDB_USERが設定されていません。後で手動で登録してください。"
    log_warn "  gcloud secrets create jrdb-user --replication-policy=automatic"
    log_warn "  echo -n 'YOUR_USER' | gcloud secrets versions add jrdb-user --data-file=-"
else
    # jrdb-user シークレットの作成/更新
    if gcloud secrets describe jrdb-user 2>/dev/null; then
        log_info "  シークレット jrdb-user は既に存在します。新しいバージョンを追加します..."
        echo -n "${JRDB_USER}" | gcloud secrets versions add jrdb-user --data-file=- --quiet
    else
        log_info "  シークレット jrdb-user を作成しています..."
        echo -n "${JRDB_USER}" | gcloud secrets create jrdb-user \
            --replication-policy="automatic" \
            --data-file=- \
            --quiet
    fi
    log_info "  jrdb-user を登録しました"
fi

if [ -z "${JRDB_PASSWORD}" ] || [ "${JRDB_PASSWORD}" = "your-jrdb-password" ]; then
    log_warn "JRDB_PASSWORDが設定されていません。後で手動で登録してください。"
    log_warn "  gcloud secrets create jrdb-password --replication-policy=automatic"
    log_warn "  echo -n 'YOUR_PASSWORD' | gcloud secrets versions add jrdb-password --data-file=-"
else
    # jrdb-password シークレットの作成/更新
    if gcloud secrets describe jrdb-password 2>/dev/null; then
        log_info "  シークレット jrdb-password は既に存在します。新しいバージョンを追加します..."
        echo -n "${JRDB_PASSWORD}" | gcloud secrets versions add jrdb-password --data-file=- --quiet
    else
        log_info "  シークレット jrdb-password を作成しています..."
        echo -n "${JRDB_PASSWORD}" | gcloud secrets create jrdb-password \
            --replication-policy="automatic" \
            --data-file=- \
            --quiet
    fi
    log_info "  jrdb-password を登録しました"
fi

# ========================================
# 6. 設定の出力
# ========================================
log_info "=========================================="
log_info "セットアップが完了しました"
log_info "=========================================="
log_info ""
log_info "サービスアカウント:"
log_info "  名前: ${PIPELINE_SA_NAME}"
log_info "  メール: ${PIPELINE_SA_EMAIL}"
log_info ""
log_info "Artifact Registry:"
log_info "  リポジトリ: ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${REPO_NAME}"
log_info ""
log_info "次のステップ:"
log_info "  1. ./infrastructure/scripts/verify_setup.sh で設定を確認"
log_info "  2. Issue #44 でDockerfileを作成"
log_info ""
