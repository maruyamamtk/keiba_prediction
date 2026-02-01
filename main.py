#!/usr/bin/env python3
"""
Cloud Run エントリーポイント

HTTPリクエストを受け付け、データパイプラインを実行する。
Cloud Schedulerからのトリガーで定期実行される。
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, jsonify, request

# ロギング設定
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/", methods=["GET"])
def health_check():
    """ヘルスチェック用エンドポイント"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})


@app.route("/run", methods=["POST"])
def run_pipeline():
    """
    データパイプラインを実行するエンドポイント

    Cloud Schedulerから呼び出される。
    """
    try:
        logger.info("パイプライン実行を開始します")

        # TODO: 以下の処理を実装
        # 1. JRDBからデータをダウンロード
        # 2. GCSにアップロード
        # 3. 特徴量生成パイプラインを実行

        # 現時点ではプレースホルダーとして成功を返す
        result = {
            "status": "success",
            "message": "パイプライン実行が完了しました",
            "timestamp": datetime.utcnow().isoformat(),
        }

        logger.info("パイプライン実行が完了しました")
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"パイプライン実行中にエラーが発生しました: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"サーバーをポート {port} で起動します")
    app.run(host="0.0.0.0", port=port)
