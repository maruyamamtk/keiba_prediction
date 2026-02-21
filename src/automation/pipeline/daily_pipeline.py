"""
日次パイプライン

その日に追加されたデータのダウンロード→GCS→BigQueryへの格納→特徴量生成を行う。
Cloud Schedulerからトリガーされ、Cloud Run内で完結する。

Issue #57: 日次パイプラインの実装
Issue #59: 特徴量生成パイプラインのCloud Run統合
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from src.automation.data.jrdb_downloader import JRDBDownloader, create_downloader_from_env
from src.automation.data.load_to_bq import BigQueryLoader
from src.automation.data.upload_to_gcs import GCSUploader, create_uploader_from_env

if TYPE_CHECKING:
    from src.ml.features.feature_pipeline import FeaturePipeline

logger = logging.getLogger(__name__)


@dataclass
class StepResult:
    """各ステップの実行結果"""

    step_name: str
    status: str  # "success", "failed", "skipped"
    duration_seconds: float = 0.0
    details: dict = field(default_factory=dict)
    error_message: Optional[str] = None


@dataclass
class PipelineResult:
    """パイプライン実行結果"""

    status: str  # "success", "partial", "failed"
    target_date: str
    files_downloaded: int = 0
    files_uploaded: int = 0
    files_loaded: int = 0
    records_loaded: int = 0
    features_inserted: int = 0
    duration_seconds: float = 0.0
    steps: List[StepResult] = field(default_factory=list)
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        """辞書形式に変換"""
        return {
            "status": self.status,
            "target_date": self.target_date,
            "files_downloaded": self.files_downloaded,
            "files_uploaded": self.files_uploaded,
            "files_loaded": self.files_loaded,
            "records_loaded": self.records_loaded,
            "features_inserted": self.features_inserted,
            "duration_seconds": round(self.duration_seconds, 2),
            "steps": [
                {
                    "step_name": s.step_name,
                    "status": s.status,
                    "duration_seconds": round(s.duration_seconds, 2),
                    "details": s.details,
                    "error_message": s.error_message,
                }
                for s in self.steps
            ],
            "error_message": self.error_message,
        }


class DailyPipeline:
    """
    日次データパイプライン

    処理フロー:
    1. JRDBから指定日のデータをダウンロード
    2. GCSにアップロード
    3. BigQueryにロード
    4. 特徴量生成（オプション）
    """

    def __init__(
        self,
        downloader: Optional[JRDBDownloader] = None,
        uploader: Optional[GCSUploader] = None,
        bq_loader: Optional[BigQueryLoader] = None,
        feature_pipeline: Optional[FeaturePipeline] = None,
    ):
        """
        初期化

        Args:
            downloader: JRDBダウンローダー（Noneの場合は環境変数から作成）
            uploader: GCSアップローダー（Noneの場合は環境変数から作成）
            bq_loader: BigQueryローダー（Noneの場合は環境変数から作成）
            feature_pipeline: 特徴量パイプライン（Noneの場合は環境変数から作成）
        """
        self._downloader = downloader
        self._uploader = uploader
        self._bq_loader = bq_loader
        self._feature_pipeline = feature_pipeline

    @property
    def downloader(self) -> JRDBDownloader:
        """JRDBダウンローダー（遅延初期化）"""
        if self._downloader is None:
            self._downloader = create_downloader_from_env()
            if self._downloader is None:
                raise RuntimeError("JRDBダウンローダーの初期化に失敗しました")
        return self._downloader

    @property
    def uploader(self) -> GCSUploader:
        """GCSアップローダー（遅延初期化）"""
        if self._uploader is None:
            self._uploader = create_uploader_from_env()
            if self._uploader is None:
                raise RuntimeError("GCSアップローダーの初期化に失敗しました")
        return self._uploader

    @property
    def bq_loader(self) -> BigQueryLoader:
        """BigQueryローダー（遅延初期化）"""
        if self._bq_loader is None:
            from src.automation.data.load_to_bq import create_loader_from_env

            self._bq_loader = create_loader_from_env()
            if self._bq_loader is None:
                raise RuntimeError("BigQueryローダーの初期化に失敗しました")
        return self._bq_loader

    @property
    def feature_pipeline(self):
        """特徴量パイプライン（遅延初期化）"""
        if self._feature_pipeline is None:
            import os

            from src.ml.features.feature_pipeline import FeaturePipeline

            project_id = os.environ.get("GCP_PROJECT_ID")
            if not project_id:
                raise RuntimeError(
                    "特徴量パイプラインの初期化に失敗: GCP_PROJECT_IDが未設定"
                )
            self._feature_pipeline = FeaturePipeline(project_id=project_id)
        return self._feature_pipeline

    @staticmethod
    def parse_target_date(target_date: Optional[str] = None) -> date:
        """
        対象日付をパース

        Args:
            target_date: 日付文字列（YYYY-MM-DD形式、Noneの場合は当日）

        Returns:
            dateオブジェクト
        """
        if target_date is None:
            return date.today()

        try:
            return datetime.strptime(target_date, "%Y-%m-%d").date()
        except ValueError as e:
            raise ValueError(f"日付形式が不正です（YYYY-MM-DD形式で指定）: {target_date}") from e

    @staticmethod
    def date_to_yymmdd(d: date) -> str:
        """dateオブジェクトをyymmdd形式に変換"""
        return d.strftime("%y%m%d")

    def _step_download(self, target_date: date) -> StepResult:
        """
        Step 1: JRDBからデータをダウンロード

        Args:
            target_date: 対象日付

        Returns:
            StepResult
        """
        step_name = "download"
        start_time = time.time()

        try:
            yymmdd = self.date_to_yymmdd(target_date)
            logger.info(f"ダウンロード開始: {yymmdd}")

            # 全データタイプをダウンロード
            results = self.downloader.download_all_from_date(yymmdd)

            # 結果集計
            total_downloaded = sum(r.downloaded_files for r in results.values())
            total_skipped = sum(r.skipped_files for r in results.values())
            total_failed = sum(r.failed_files for r in results.values())

            details = {
                "downloaded": total_downloaded,
                "skipped": total_skipped,
                "failed": total_failed,
                "datatypes": list(results.keys()),
            }

            if total_failed > 0:
                logger.warning(f"ダウンロード失敗: {total_failed}ファイル")

            return StepResult(
                step_name=step_name,
                status="success" if total_failed == 0 else "partial",
                duration_seconds=time.time() - start_time,
                details=details,
            )

        except Exception as e:
            logger.error(f"ダウンロードエラー: {e}")
            return StepResult(
                step_name=step_name,
                status="failed",
                duration_seconds=time.time() - start_time,
                error_message=str(e),
            )

    def _step_upload(self) -> StepResult:
        """
        Step 2: GCSにアップロード

        Returns:
            StepResult
        """
        step_name = "upload"
        start_time = time.time()

        try:
            # ダウンローダーの出力ディレクトリを使用
            output_dir = self.downloader.get_output_dir()
            logger.info(f"GCSアップロード開始: {output_dir}")

            # アップローダーのローカルディレクトリを一時的に変更
            original_base_dir = self.uploader.local_base_dir
            self.uploader.local_base_dir = output_dir

            try:
                result = self.uploader.upload_all()
            finally:
                self.uploader.local_base_dir = original_base_dir

            details = {
                "uploaded": result.uploaded_files,
                "skipped": result.skipped_files,
                "failed": result.failed_files,
                "bytes": result.uploaded_bytes,
            }

            if result.failed_files > 0:
                logger.warning(f"アップロード失敗: {result.failed_files}ファイル")

            return StepResult(
                step_name=step_name,
                status="success" if result.failed_files == 0 else "partial",
                duration_seconds=time.time() - start_time,
                details=details,
            )

        except Exception as e:
            logger.error(f"アップロードエラー: {e}")
            return StepResult(
                step_name=step_name,
                status="failed",
                duration_seconds=time.time() - start_time,
                error_message=str(e),
            )

    def _step_load_to_bq(self, target_date: date) -> StepResult:
        """
        Step 3: BigQueryにロード

        GCS上の未ロードファイルをすべてBigQueryにロードする。
        JRDBファイルはレース開催日（主に土日）で命名されるため、
        パイプライン実行日ではなくロード履歴による重複スキップで制御する。

        Args:
            target_date: 対象日付（ログ用）

        Returns:
            StepResult
        """
        step_name = "load_to_bq"
        start_time = time.time()

        try:
            from src.automation.data.load_to_bq import TABLE_MAPPING

            logger.info(f"BigQueryロード開始（未ロードファイル全件）: {target_date}")

            # GCS上のサポート対象データタイプのファイルのみを取得
            supported_types = list(TABLE_MAPPING.keys())
            csv_files = self.bq_loader.list_csv_files(
                prefix="", data_types=supported_types
            )

            if not csv_files:
                logger.info("ロード対象ファイルなし")
                return StepResult(
                    step_name=step_name,
                    status="success",
                    duration_seconds=time.time() - start_time,
                    details={"files": 0, "records": 0, "skipped": 0},
                )

            # バッチロード実行（重複スキップ有効）
            result = self.bq_loader.load_files_batch(
                csv_files,
                continue_on_error=True,
                skip_loaded=True,
                record_history=True,
            )

            details = {
                "files": result.success_count,
                "records": result.total_records,
                "skipped": result.skipped_count,
                "failed": result.failed_count,
            }

            if result.failed_count > 0:
                logger.warning(f"ロード失敗: {result.failed_count}ファイル")
                logger.warning(f"失敗ファイル: {result.failed_files}")

            return StepResult(
                step_name=step_name,
                status="success" if result.failed_count == 0 else "partial",
                duration_seconds=time.time() - start_time,
                details=details,
            )

        except Exception as e:
            logger.error(f"BigQueryロードエラー: {e}")
            return StepResult(
                step_name=step_name,
                status="failed",
                duration_seconds=time.time() - start_time,
                error_message=str(e),
            )

    def _step_generate_features(self, target_date: date) -> StepResult:
        """
        Step 4: 特徴量生成

        BigQuery上のrawデータから特徴量を生成してfeatures.training_dataに書き込む。

        Args:
            target_date: 対象日付

        Returns:
            StepResult
        """
        step_name = "generate_features"
        start_time = time.time()

        try:
            date_str = target_date.strftime("%Y-%m-%d")
            logger.info(f"特徴量生成開始: {date_str}")

            result = self.feature_pipeline.run(
                start_date=date_str,
                end_date=date_str,
            )

            details = {
                "deleted_rows": result.get("deleted_rows", 0),
                "inserted_rows": result.get("inserted_rows", 0),
                "elapsed_time": round(result.get("elapsed_time", 0), 2),
            }

            logger.info(
                f"特徴量生成完了: inserted={details['inserted_rows']}, "
                f"elapsed={details['elapsed_time']}s"
            )

            return StepResult(
                step_name=step_name,
                status="success",
                duration_seconds=time.time() - start_time,
                details=details,
            )

        except Exception as e:
            logger.error(f"特徴量生成エラー: {e}")
            return StepResult(
                step_name=step_name,
                status="failed",
                duration_seconds=time.time() - start_time,
                error_message=str(e),
            )

    def run(self, target_date: Optional[str] = None) -> PipelineResult:
        """
        パイプラインを実行

        Args:
            target_date: 対象日付（YYYY-MM-DD形式、Noneの場合は当日）

        Returns:
            PipelineResult
        """
        start_time = time.time()
        result = PipelineResult(status="success", target_date="")

        try:
            # 日付パース
            parsed_date = self.parse_target_date(target_date)
            result.target_date = parsed_date.strftime("%Y-%m-%d")
            logger.info(f"日次パイプライン開始: {result.target_date}")

            # Step 1: ダウンロード
            download_result = self._step_download(parsed_date)
            result.steps.append(download_result)

            if download_result.status == "failed":
                result.status = "failed"
                result.error_message = f"ダウンロード失敗: {download_result.error_message}"
                result.duration_seconds = time.time() - start_time
                return result

            result.files_downloaded = download_result.details.get("downloaded", 0)

            # Step 2: GCSアップロード
            upload_result = self._step_upload()
            result.steps.append(upload_result)

            if upload_result.status == "failed":
                result.status = "failed"
                result.error_message = f"アップロード失敗: {upload_result.error_message}"
                result.duration_seconds = time.time() - start_time
                return result

            result.files_uploaded = upload_result.details.get("uploaded", 0)

            # Step 3: BigQueryロード
            bq_result = self._step_load_to_bq(parsed_date)
            result.steps.append(bq_result)

            if bq_result.status == "failed":
                result.status = "failed"
                result.error_message = f"BigQueryロード失敗: {bq_result.error_message}"
                result.duration_seconds = time.time() - start_time
                return result

            result.files_loaded = bq_result.details.get("files", 0)
            result.records_loaded = bq_result.details.get("records", 0)

            # Step 4: 特徴量生成
            feature_result = self._step_generate_features(parsed_date)
            result.steps.append(feature_result)

            if feature_result.status == "failed":
                # 特徴量生成失敗はpartialとする（データロードは成功しているため）
                result.status = "partial"
                result.error_message = (
                    f"特徴量生成失敗: {feature_result.error_message}"
                )
                result.duration_seconds = time.time() - start_time
                return result

            result.features_inserted = feature_result.details.get(
                "inserted_rows", 0
            )

            # 全体ステータスの判定
            if any(s.status == "partial" for s in result.steps):
                result.status = "partial"

            result.duration_seconds = time.time() - start_time
            logger.info(
                f"日次パイプライン完了: status={result.status}, "
                f"files_loaded={result.files_loaded}, records={result.records_loaded}, "
                f"features_inserted={result.features_inserted}"
            )

            return result

        except Exception as e:
            logger.error(f"パイプラインエラー: {e}")
            result.status = "failed"
            result.error_message = str(e)
            result.duration_seconds = time.time() - start_time
            return result

        finally:
            # 一時ディレクトリのクリーンアップ
            if self._downloader:
                try:
                    self._downloader.cleanup()
                except Exception as e:
                    logger.warning(f"クリーンアップエラー: {e}")


def create_pipeline_from_env() -> Optional[DailyPipeline]:
    """
    環境変数からDailyPipelineを作成

    Returns:
        DailyPipelineインスタンス（設定不足の場合はNone）
    """
    try:
        return DailyPipeline()
    except Exception as e:
        logger.error(f"パイプラインの作成に失敗: {e}")
        return None


def main():
    """CLIエントリーポイント"""
    import argparse
    import json
    import os
    import sys

    from dotenv import load_dotenv

    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="日次データパイプライン")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="対象日付（YYYY-MM-DD形式、デフォルト: 当日）",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="結果をJSON形式で出力",
    )
    args = parser.parse_args()

    # .envを読み込み
    load_dotenv()

    # パイプライン実行
    pipeline = DailyPipeline()

    try:
        result = pipeline.run(args.date)

        if args.json:
            print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        else:
            print("\n" + "=" * 60)
            print("日次パイプライン結果")
            print("=" * 60)
            print(f"ステータス: {result.status}")
            print(f"対象日付: {result.target_date}")
            print(f"ダウンロードファイル数: {result.files_downloaded}")
            print(f"アップロードファイル数: {result.files_uploaded}")
            print(f"ロードファイル数: {result.files_loaded}")
            print(f"ロードレコード数: {result.records_loaded}")
            print(f"処理時間: {result.duration_seconds:.2f}秒")

            if result.error_message:
                print(f"\nエラー: {result.error_message}")

            print("\nステップ詳細:")
            for step in result.steps:
                print(f"  {step.step_name}: {step.status} ({step.duration_seconds:.2f}秒)")
                if step.error_message:
                    print(f"    エラー: {step.error_message}")

        sys.exit(0 if result.status == "success" else 1)

    except Exception as e:
        logger.error(f"パイプライン実行エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
