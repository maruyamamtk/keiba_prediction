#!/usr/bin/env python3
"""
Cloud Run エントリーポイント

HTTPリクエストを受け付け、データパイプラインを実行する。
Cloud Schedulerからのトリガーで定期実行される。

パイプライン全体:
1. JRDBダウンロード (JRDB → 一時ディレクトリ)
2. GCSアップロード (一時ディレクトリ → GCS)
3. BigQueryロード (GCS → BigQuery, Cloud Functionが自動実行)
4. 特徴量生成 (BigQuery raw → features)

Issue #53: JRDBダウンローダーのコンテナ化
Issue #66: データパイプライン統合とCloud Run対応
"""

import logging
import os
from datetime import datetime, timedelta

from flask import Flask, jsonify, request

from src.data.pipeline import create_pipeline_from_env
from src.features.feature_pipeline import FeaturePipeline, FeaturePipelineConfig

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
def download_and_upload():
    """
    JRDBダウンロード→GCSアップロードを実行するエンドポイント

    リクエストボディ（JSON）:
        start_date: 開始日付（yymmdd形式）。省略時は昨日
        datatype: データタイプ（省略時はすべて）

    Returns:
        ダウンロード・アップロード結果
    """
    try:
        # リクエストパラメータを取得
        data = request.get_json(silent=True) or {}
        start_date = data.get("start_date", get_default_start_date())
        datatype = data.get("datatype")

        logger.info(
            f"ダウンロード・アップロード開始: start_date={start_date}, datatype={datatype}"
        )

        # パイプラインを作成
        pipeline = create_pipeline_from_env(use_temp_dir=True)
        if pipeline is None:
            return jsonify({
                "status": "error",
                "message": "パイプラインの初期化に失敗しました（認証情報を確認してください）"
            }), 500

        # パイプライン実行
        result = pipeline.run_download_and_upload(
            start_date=start_date,
            datatype=datatype,
        )

        if result.success:
            response = {
                "status": "success",
                "message": "ダウンロード・アップロードが完了しました",
                "start_date": start_date,
                "download": {
                    "total_files": result.download_result.total_files,
                    "downloaded_files": result.download_result.downloaded_files,
                    "skipped_files": result.download_result.skipped_files,
                    "failed_files": result.download_result.failed_files,
                } if result.download_result else None,
                "upload": {
                    "total_files": result.upload_result.total_files,
                    "uploaded_files": result.upload_result.uploaded_files,
                    "skipped_files": result.upload_result.skipped_files,
                    "failed_files": result.upload_result.failed_files,
                    "uploaded_bytes": result.upload_result.uploaded_bytes,
                } if result.upload_result else None,
                "timestamp": datetime.utcnow().isoformat(),
            }
            logger.info("ダウンロード・アップロードが完了しました")
            return jsonify(response), 200
        else:
            response = {
                "status": "error",
                "message": result.error_message or "パイプライン実行に失敗しました",
                "timestamp": datetime.utcnow().isoformat(),
            }
            logger.error(f"パイプライン実行に失敗: {result.error_message}")
            return jsonify(response), 500

    except Exception as e:
        logger.error(f"パイプライン実行中にエラーが発生しました: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/run", methods=["POST"])
def run_full_pipeline():
    """
    フルデータパイプラインを実行するエンドポイント

    Cloud Schedulerから呼び出される。

    リクエストボディ（JSON）:
        start_date: 開始日付（yymmdd形式）。省略時は昨日
        end_date: 終了日付（YYYY-MM-DD形式、特徴量生成用）。省略時はstart_dateから変換
        steps: 実行するステップのリスト（省略時はすべて）
               ["download_upload", "features"]

    パイプライン全体:
        1. download_upload: JRDB→GCS (このCloud Runで実行)
        2. BigQueryロード: GCS→BigQuery (Cloud Functionが自動実行)
        3. features: 特徴量生成 (このCloud Runで実行)
    """
    try:
        # リクエストパラメータを取得
        data = request.get_json(silent=True) or {}
        start_date = data.get("start_date", get_default_start_date())
        steps = data.get("steps", ["download_upload", "features"])

        logger.info(f"フルパイプライン実行開始: start_date={start_date}, steps={steps}")

        results = {
            "start_date": start_date,
            "steps": {},
        }

        # Step 1+2: JRDBダウンロード→GCSアップロード
        if "download_upload" in steps:
            logger.info("Step 1+2: JRDBダウンロード→GCSアップロード開始")

            pipeline = create_pipeline_from_env(use_temp_dir=True)
            if pipeline is None:
                return jsonify({
                    "status": "error",
                    "message": "パイプラインの初期化に失敗しました"
                }), 500

            result = pipeline.run_download_and_upload(start_date=start_date)

            if result.success:
                results["steps"]["download_upload"] = {
                    "status": "success",
                    "download": {
                        "downloaded_files": result.download_result.downloaded_files,
                        "skipped_files": result.download_result.skipped_files,
                        "failed_files": result.download_result.failed_files,
                    } if result.download_result else None,
                    "upload": {
                        "uploaded_files": result.upload_result.uploaded_files,
                        "skipped_files": result.upload_result.skipped_files,
                        "failed_files": result.upload_result.failed_files,
                    } if result.upload_result else None,
                }
                logger.info("Step 1+2完了")
            else:
                results["steps"]["download_upload"] = {
                    "status": "error",
                    "message": result.error_message,
                }
                logger.error(f"Step 1+2失敗: {result.error_message}")

        # Note: Step 3 (BigQueryロード) はCloud Functionが自動実行するためスキップ

        # Step 4: 特徴量生成
        if "features" in steps:
            logger.info("Step 4: 特徴量生成開始")

            project_id = os.environ.get("GCP_PROJECT_ID")
            if not project_id:
                results["steps"]["features"] = {
                    "status": "error",
                    "message": "GCP_PROJECT_IDが設定されていません",
                }
                logger.error("GCP_PROJECT_IDが設定されていません")
            else:
                try:
                    # start_dateをYYYY-MM-DD形式に変換
                    start_date_formatted = datetime.strptime(start_date, "%y%m%d").strftime("%Y-%m-%d")
                    end_date_formatted = data.get("end_date", start_date_formatted)

                    # 特徴量パイプライン実行
                    config = FeaturePipelineConfig(max_workers=2)
                    feature_pipeline = FeaturePipeline(project_id, config)
                    feature_result = feature_pipeline.run(
                        start_date=start_date_formatted,
                        end_date=end_date_formatted,
                        batch_size=50,
                        parallel=True,
                    )

                    results["steps"]["features"] = {
                        "status": "success" if feature_result["errors"] == 0 else "partial_success",
                        "total_races": feature_result["total_races"],
                        "processed_races": feature_result["processed_races"],
                        "errors": feature_result["errors"],
                        "elapsed_time": feature_result["elapsed_time"],
                    }
                    logger.info(f"Step 4完了: {feature_result['processed_races']}レース処理")

                except Exception as e:
                    results["steps"]["features"] = {
                        "status": "error",
                        "message": str(e),
                    }
                    logger.error(f"Step 4失敗: {e}", exc_info=True)

        # 全体の結果を判定
        step_statuses = [s.get("status") for s in results["steps"].values()]
        if all(s == "success" for s in step_statuses):
            overall_status = "success"
        elif any(s == "error" for s in step_statuses):
            overall_status = "error"
        else:
            overall_status = "partial_success"

        response = {
            "status": overall_status,
            "message": "フルパイプライン実行が完了しました",
            "timestamp": datetime.utcnow().isoformat(),
            **results,
        }

        logger.info(f"フルパイプライン実行完了: {overall_status}")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"フルパイプライン実行中にエラーが発生しました: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"サーバーをポート {port} で起動します")
    app.run(host="0.0.0.0", port=port)
