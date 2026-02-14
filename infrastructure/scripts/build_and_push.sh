#!/bin/bash
#
# Dockerイメージのビルド・プッシュスクリプト
#
# Artifact RegistryにDockerイメージをビルドしてプッシュする。
# Apple Silicon Mac環境ではlinux/amd64プラットフォームでクロスビルドする。
#
# 使用方法:
#   ./infrastructure/scripts/build_and_push.sh [image-tag]
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
IMAGE_TAG="${1:-latest}"

# 変数設定
SERVICE_NAME="keiba-pipeline"
REPO_NAME="keiba-pipeline"
IMAGE_URI="${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${REPO_NAME}/${SERVICE_NAME}:${IMAGE_TAG}"

log_info "=========================================="
log_info "Dockerイメージをビルド・プッシュします"
log_info "=========================================="
log_info "プロジェクトID: ${GCP_PROJECT_ID}"
log_info "リージョン: ${GCP_REGION}"
log_info "イメージ: ${IMAGE_URI}"
log_info "=========================================="

# Artifact Registryへの認証設定
log_info "Artifact Registryへの認証を設定しています..."
gcloud auth configure-docker "${GCP_REGION}-docker.pkg.dev" --quiet

# Dockerイメージのビルド
# Apple Silicon Mac環境ではlinux/amd64プラットフォームを指定
log_info "Dockerイメージをビルドしています（linux/amd64）..."
docker build \
    --platform linux/amd64 \
    -t "${IMAGE_URI}" \
    -f "${PROJECT_ROOT}/Dockerfile" \
    "${PROJECT_ROOT}"

log_info "ビルドが完了しました"

# Artifact Registryへプッシュ
log_info "Artifact Registryにプッシュしています..."
docker push "${IMAGE_URI}"

log_info "=========================================="
log_info "ビルド・プッシュ完了"
log_info "=========================================="
log_info "イメージ: ${IMAGE_URI}"
log_info ""
log_info "次のステップ:"
log_info "  ./infrastructure/scripts/deploy_cloud_run.sh ${IMAGE_TAG}"
log_info ""
