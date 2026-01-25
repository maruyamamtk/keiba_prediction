#!/bin/bash
# GCSへのデータ同期スクリプト
#
# ローカルのダウンロードデータをGCSにアップロードする。
# 差分アップロード（MD5ハッシュ比較）により、変更のあるファイルのみアップロード。
#
# 使用方法:
#   ./scripts/sync_to_gcs.sh                    # すべてのデータタイプをアップロード
#   ./scripts/sync_to_gcs.sh --data-type Baa    # 特定のデータタイプのみアップロード
#   ./scripts/sync_to_gcs.sh --force            # 強制アップロード
#   ./scripts/sync_to_gcs.sh --dry-run          # ドライラン
#
# Issue #6: ローカル→GCS自動アップロードスクリプトの実装

set -e

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# プロジェクトルートに移動
cd "$PROJECT_ROOT"

# 仮想環境を有効化（存在する場合）
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
elif [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

# .envファイルの存在確認
if [ ! -f ".env" ]; then
    echo "エラー: .envファイルが見つかりません。"
    echo ".env.exampleをコピーして.envを作成し、GCP_PROJECT_IDを設定してください。"
    echo ""
    echo "  cp .env.example .env"
    echo "  # .envを編集してGCP_PROJECT_IDを設定"
    exit 1
fi

# Pythonスクリプトを実行
echo "========================================"
echo "GCSアップロードスクリプト"
echo "========================================"
echo ""

python src/data/upload_to_gcs.py "$@"

echo ""
echo "========================================"
echo "完了"
echo "========================================"
