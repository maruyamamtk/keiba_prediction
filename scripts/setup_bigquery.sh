#!/bin/bash

# BigQueryデータセットとテーブルのセットアップスクリプト
# Issue #4: BigQueryデータセットとテーブルの作成

set -e  # エラーが発生したら即座に終了

# 色付きログ出力
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
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

# 環境変数の読み込み
if [ -f .env ]; then
    log_info ".envファイルを読み込んでいます..."
    source .env
else
    log_error ".envファイルが見つかりません。.env.exampleを参考に作成してください。"
    exit 1
fi

# 必須環境変数のチェック
if [ -z "$GCP_PROJECT_ID" ]; then
    log_error "GCP_PROJECT_IDが設定されていません。"
    exit 1
fi

log_info "=== BigQueryセットアップを開始します ==="
log_info "プロジェクトID: $GCP_PROJECT_ID"

# Pythonパッケージのインストール確認
log_info "Pythonパッケージをインストールしています..."
pip install -q -r requirements.txt

# Pythonスクリプトを実行
log_info "BigQueryデータセットとテーブルを作成しています..."
python src/data/create_tables.py

log_info ""
log_info "=== セットアップ完了 ==="
log_info ""
log_info "次のステップ:"
log_info "1. BigQueryコンソールで作成されたデータセットとテーブルを確認:"
log_info "   https://console.cloud.google.com/bigquery?project=$GCP_PROJECT_ID"
log_info "2. Issue #5: GCS→BigQuery自動ロードCloud Functionsの実装に進んでください。"
