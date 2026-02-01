#!/usr/bin/env python3
"""
Cloud Run エントリーポイント

HTTPリクエストを受け付け、データパイプラインを実行する。
Cloud Schedulerからのトリガーで定期実行される。

Issue #53: JRDBダウンローダーのコンテナ化
"""

import logging
import os
from datetime import datetime, timedelta

from flask import Flask, jsonify, request

from src.data.jrdb_downloader import JRDBDownloader, create_downloader_from_env

# ロギング設定
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def get_default_start_date() -> str:
    """
    デフォルトの開始日付を取得（昨日の日付）

    Returns:
        yymmdd形式の日付文字列
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%y%m%d")


@app.route("/", methods=["GET"])
def health_check():
    """ヘルスチェック用エンドポイント"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})


@app.route("/download", methods=["POST"])
def download_jrdb():
    """
    JRDBからデータをダウンロードするエンドポイント

    リクエストボディ（JSON）:
        start_date: 開始日付（yymmdd形式）。省略時は昨日
        datatype: データタイプ（省略時はすべて）

    Returns:
        ダウンロード結果
    """
    try:
        # リクエストパラメータを取得
        data = request.get_json(silent=True) or {}
        start_date = data.get("start_date", get_default_start_date())
        datatype = data.get("datatype")

        logger.info(f"JRDBダウンロード開始: start_date={start_date}, datatype={datatype}")

        # ダウンローダーを作成
        downloader = create_downloader_from_env()
        if downloader is None:
            return jsonify({
                "status": "error",
                "message": "JRDB認証情報が設定されていません"
            }), 500

        try:
            if datatype:
                # 特定のデータタイプのみ
                result = downloader.download_from_date(datatype.upper(), start_date)
                download_results = {datatype.upper(): {
                    "total_files": result.total_files,
                    "downloaded_files": result.downloaded_files,
                    "skipped_files": result.skipped_files,
                    "failed_files": result.failed_files,
                }}
            else:
                # すべてのデータタイプ
                results = downloader.download_all_from_date(start_date)
                download_results = {
                    dt: {
                        "total_files": r.total_files,
                        "downloaded_files": r.downloaded_files,
                        "skipped_files": r.skipped_files,
                        "failed_files": r.failed_files,
                    }
                    for dt, r in results.items()
                }

            # 合計を計算
            total_downloaded = sum(r["downloaded_files"] for r in download_results.values())
            total_failed = sum(r["failed_files"] for r in download_results.values())

            response = {
                "status": "success" if total_failed == 0 else "partial_success",
                "message": f"ダウンロード完了: {total_downloaded}ファイル",
                "start_date": start_date,
                "output_dir": str(downloader.get_output_dir()),
                "results": download_results,
                "timestamp": datetime.utcnow().isoformat(),
            }

            logger.info(f"JRDBダウンロード完了: {total_downloaded}ファイル")
            return jsonify(response), 200

        finally:
            # 一時ディレクトリの場合はクリーンアップ
            # 注意: GCSアップロード後にクリーンアップする必要がある
            pass

    except Exception as e:
        logger.error(f"JRDBダウンロード中にエラーが発生しました: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/run", methods=["POST"])
def run_pipeline():
    """
    データパイプラインを実行するエンドポイント

    Cloud Schedulerから呼び出される。

    リクエストボディ（JSON）:
        start_date: 開始日付（yymmdd形式）。省略時は昨日
        steps: 実行するステップのリスト（省略時はすべて）
               ["download", "upload", "features"]
    """
    try:
        # リクエストパラメータを取得
        data = request.get_json(silent=True) or {}
        start_date = data.get("start_date", get_default_start_date())
        steps = data.get("steps", ["download", "upload", "features"])

        logger.info(f"パイプライン実行を開始します: start_date={start_date}, steps={steps}")

        results = {
            "start_date": start_date,
            "steps": {},
        }

        # Step 1: JRDBダウンロード
        if "download" in steps:
            logger.info("Step 1: JRDBダウンロード開始")
            downloader = create_downloader_from_env()
            if downloader is None:
                return jsonify({
                    "status": "error",
                    "message": "JRDB認証情報が設定されていません"
                }), 500

            download_results = downloader.download_all_from_date(start_date)
            total_downloaded = sum(r.downloaded_files for r in download_results.values())
            total_failed = sum(r.failed_files for r in download_results.values())

            results["steps"]["download"] = {
                "status": "success" if total_failed == 0 else "partial_success",
                "downloaded_files": total_downloaded,
                "failed_files": total_failed,
                "output_dir": str(downloader.get_output_dir()),
            }
            logger.info(f"Step 1: JRDBダウンロード完了: {total_downloaded}ファイル")

        # Step 2: GCSアップロード（TODO: 後続Issueで実装）
        if "upload" in steps:
            logger.info("Step 2: GCSアップロード（未実装）")
            results["steps"]["upload"] = {
                "status": "skipped",
                "message": "GCSアップロードは後続Issueで実装予定"
            }

        # Step 3: 特徴量生成（TODO: 後続Issueで実装）
        if "features" in steps:
            logger.info("Step 3: 特徴量生成（未実装）")
            results["steps"]["features"] = {
                "status": "skipped",
                "message": "特徴量生成は後続Issueで実装予定"
            }

        # 全体の結果を判定
        step_statuses = [s.get("status") for s in results["steps"].values()]
        if all(s in ("success", "skipped") for s in step_statuses):
            overall_status = "success"
        elif any(s == "error" for s in step_statuses):
            overall_status = "error"
        else:
            overall_status = "partial_success"

        response = {
            "status": overall_status,
            "message": "パイプライン実行が完了しました",
            "timestamp": datetime.utcnow().isoformat(),
            **results,
        }

        logger.info("パイプライン実行が完了しました")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"パイプライン実行中にエラーが発生しました: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"サーバーをポート {port} で起動します")
    app.run(host="0.0.0.0", port=port)
