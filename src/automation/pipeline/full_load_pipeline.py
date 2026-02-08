"""
過去分全件ロードパイプライン

指定期間のデータをJRDBからダウンロード→GCS→BigQueryに一括ロードする。
HTTP APIで手動トリガーし、初回セットアップやデータ欠損の補完に使用。

Issue #58: 過去分全件ロード処理の実装
"""

import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Optional

from src.automation.data.jrdb_downloader import JRDBDownloader, create_downloader_from_env
from src.automation.data.load_to_bq import BigQueryLoader
from src.automation.data.upload_to_gcs import GCSUploader, create_uploader_from_env

logger = logging.getLogger(__name__)


@dataclass
class FullLoadStepResult:
    """各ステップの実行結果"""

    step_name: str
    status: str  # "success", "failed", "partial"
    duration_seconds: float = 0.0
    details: dict = field(default_factory=dict)
    error_message: Optional[str] = None


@dataclass
class FullLoadResult:
    """全件ロードパイプライン実行結果"""

    status: str  # "success", "partial", "failed"
    job_id: str
    start_date: str
    end_date: str
    files_downloaded: int = 0
    files_uploaded: int = 0
    files_loaded: int = 0
    records_loaded: int = 0
    duration_seconds: float = 0.0
    steps: List[FullLoadStepResult] = field(default_factory=list)
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        """辞書形式に変換"""
        return {
            "status": self.status,
            "job_id": self.job_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "files_downloaded": self.files_downloaded,
            "files_uploaded": self.files_uploaded,
            "files_loaded": self.files_loaded,
            "records_loaded": self.records_loaded,
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


class FullLoadPipeline:
    """
    過去分全件ロードパイプライン

    処理フロー:
    1. 指定期間のデータをJRDBからダウンロード
    2. GCSにアップロード（差分のみ）
    3. BigQueryにロード（重複スキップ）
    """

    def __init__(
        self,
        downloader: Optional[JRDBDownloader] = None,
        uploader: Optional[GCSUploader] = None,
        bq_loader: Optional[BigQueryLoader] = None,
    ):
        self._downloader = downloader
        self._uploader = uploader
        self._bq_loader = bq_loader

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

    @staticmethod
    def parse_date(date_str: Optional[str]) -> Optional[date]:
        """
        日付文字列をパース

        Args:
            date_str: YYYY-MM-DD形式の日付文字列（Noneも許容）

        Returns:
            dateオブジェクトまたはNone
        """
        if date_str is None:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError as e:
            raise ValueError(
                f"日付形式が不正です（YYYY-MM-DD形式で指定）: {date_str}"
            ) from e

    @staticmethod
    def date_to_yymmdd(d: date) -> str:
        """dateオブジェクトをyymmdd形式に変換"""
        return d.strftime("%y%m%d")

    def _step_download(
        self, start_date: Optional[date], end_date: Optional[date]
    ) -> FullLoadStepResult:
        """
        Step 1: JRDBからデータをダウンロード

        start_dateから全データタイプをダウンロード。
        end_dateはJRDBのAPIでは直接フィルタできないため、
        ダウンローダーがサーバー上の利用可能日付をフィルタする。
        """
        step_name = "download"
        start_time = time.time()

        try:
            # start_dateが指定されていない場合はデフォルト（2020年）
            yymmdd = self.date_to_yymmdd(start_date) if start_date else "200101"
            logger.info(f"全件ダウンロード開始: {yymmdd}〜")

            results = self.downloader.download_all_from_date(yymmdd)

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

            return FullLoadStepResult(
                step_name=step_name,
                status="success" if total_failed == 0 else "partial",
                duration_seconds=time.time() - start_time,
                details=details,
            )

        except Exception as e:
            logger.error(f"ダウンロードエラー: {e}")
            return FullLoadStepResult(
                step_name=step_name,
                status="failed",
                duration_seconds=time.time() - start_time,
                error_message=str(e),
            )

    def _step_upload(self) -> FullLoadStepResult:
        """Step 2: GCSにアップロード"""
        step_name = "upload"
        start_time = time.time()

        try:
            output_dir = self.downloader.get_output_dir()
            logger.info(f"GCSアップロード開始: {output_dir}")

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

            return FullLoadStepResult(
                step_name=step_name,
                status="success" if result.failed_files == 0 else "partial",
                duration_seconds=time.time() - start_time,
                details=details,
            )

        except Exception as e:
            logger.error(f"アップロードエラー: {e}")
            return FullLoadStepResult(
                step_name=step_name,
                status="failed",
                duration_seconds=time.time() - start_time,
                error_message=str(e),
            )

    def _step_load_to_bq(
        self, start_date: Optional[date], end_date: Optional[date]
    ) -> FullLoadStepResult:
        """
        Step 3: BigQueryにロード

        GCS上の全CSVファイルをロード（重複スキップ有効）。
        日付フィルタが指定されている場合、ファイル名のyymmdd部分でフィルタする。
        """
        step_name = "load_to_bq"
        start_time = time.time()

        try:
            logger.info("BigQuery全件ロード開始")

            csv_files = self.bq_loader.list_csv_files(prefix="")

            # 日付範囲でフィルタ
            if start_date or end_date:
                csv_files = self._filter_files_by_date(
                    csv_files, start_date, end_date
                )

            if not csv_files:
                logger.info("ロード対象ファイルなし")
                return FullLoadStepResult(
                    step_name=step_name,
                    status="success",
                    duration_seconds=time.time() - start_time,
                    details={"files": 0, "records": 0, "skipped": 0},
                )

            logger.info(f"ロード対象: {len(csv_files)}ファイル")

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
                "total_target": len(csv_files),
            }

            if result.failed_count > 0:
                logger.warning(f"ロード失敗: {result.failed_count}ファイル")

            return FullLoadStepResult(
                step_name=step_name,
                status="success" if result.failed_count == 0 else "partial",
                duration_seconds=time.time() - start_time,
                details=details,
            )

        except Exception as e:
            logger.error(f"BigQueryロードエラー: {e}")
            return FullLoadStepResult(
                step_name=step_name,
                status="failed",
                duration_seconds=time.time() - start_time,
                error_message=str(e),
            )

    @staticmethod
    def _filter_files_by_date(
        files: List[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> List[str]:
        """
        ファイル名のyymmdd部分で日付フィルタ

        Args:
            files: ファイル名のリスト
            start_date: 開始日
            end_date: 終了日

        Returns:
            フィルタ済みファイルリスト
        """
        filtered = []
        pattern = re.compile(r"(\d{6})\.")  # yymmdd部分を抽出

        start_yymmdd = start_date.strftime("%y%m%d") if start_date else None
        end_yymmdd = end_date.strftime("%y%m%d") if end_date else None

        for f in files:
            match = pattern.search(f)
            if not match:
                continue

            file_date = match.group(1)

            # 90以上のyyは1990年代なので除外
            yy = int(file_date[:2])
            if yy >= 90:
                continue

            if start_yymmdd and file_date < start_yymmdd:
                continue
            if end_yymmdd and file_date > end_yymmdd:
                continue

            filtered.append(f)

        return filtered

    def run(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> FullLoadResult:
        """
        全件ロードパイプラインを実行

        Args:
            start_date: 開始日付（YYYY-MM-DD形式、省略時は全期間）
            end_date: 終了日付（YYYY-MM-DD形式、省略時は全期間）

        Returns:
            FullLoadResult
        """
        start_time = time.time()
        job_id = str(uuid.uuid4())[:8]

        parsed_start = self.parse_date(start_date)
        parsed_end = self.parse_date(end_date)

        result = FullLoadResult(
            status="success",
            job_id=job_id,
            start_date=start_date or "全期間",
            end_date=end_date or "全期間",
        )

        logger.info(
            f"全件ロードパイプライン開始: job_id={job_id}, "
            f"期間={result.start_date}〜{result.end_date}"
        )

        try:
            # Step 1: ダウンロード
            download_result = self._step_download(parsed_start, parsed_end)
            result.steps.append(download_result)

            if download_result.status == "failed":
                result.status = "failed"
                result.error_message = (
                    f"ダウンロード失敗: {download_result.error_message}"
                )
                result.duration_seconds = time.time() - start_time
                return result

            result.files_downloaded = download_result.details.get("downloaded", 0)

            # Step 2: GCSアップロード
            upload_result = self._step_upload()
            result.steps.append(upload_result)

            if upload_result.status == "failed":
                result.status = "failed"
                result.error_message = (
                    f"アップロード失敗: {upload_result.error_message}"
                )
                result.duration_seconds = time.time() - start_time
                return result

            result.files_uploaded = upload_result.details.get("uploaded", 0)

            # Step 3: BigQueryロード
            bq_result = self._step_load_to_bq(parsed_start, parsed_end)
            result.steps.append(bq_result)

            if bq_result.status == "failed":
                result.status = "failed"
                result.error_message = (
                    f"BigQueryロード失敗: {bq_result.error_message}"
                )
                result.duration_seconds = time.time() - start_time
                return result

            result.files_loaded = bq_result.details.get("files", 0)
            result.records_loaded = bq_result.details.get("records", 0)

            # 全体ステータス判定
            if any(s.status == "partial" for s in result.steps):
                result.status = "partial"

            result.duration_seconds = time.time() - start_time
            logger.info(
                f"全件ロードパイプライン完了: job_id={job_id}, "
                f"status={result.status}, files={result.files_loaded}, "
                f"records={result.records_loaded}"
            )

            return result

        except Exception as e:
            logger.error(f"パイプラインエラー: {e}")
            result.status = "failed"
            result.error_message = str(e)
            result.duration_seconds = time.time() - start_time
            return result

        finally:
            if self._downloader:
                try:
                    self._downloader.cleanup()
                except Exception as e:
                    logger.warning(f"クリーンアップエラー: {e}")


def main():
    """CLIエントリーポイント"""
    import argparse
    import json
    import sys

    from dotenv import load_dotenv

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="過去分全件ロードパイプライン")
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="開始日付（YYYY-MM-DD形式、省略時は全期間）",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="終了日付（YYYY-MM-DD形式、省略時は全期間）",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="結果をJSON形式で出力",
    )
    args = parser.parse_args()

    load_dotenv()

    pipeline = FullLoadPipeline()

    try:
        result = pipeline.run(args.start_date, args.end_date)

        if args.json:
            print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        else:
            print("\n" + "=" * 60)
            print("全件ロードパイプライン結果")
            print("=" * 60)
            print(f"ジョブID: {result.job_id}")
            print(f"ステータス: {result.status}")
            print(f"期間: {result.start_date} 〜 {result.end_date}")
            print(f"ダウンロードファイル数: {result.files_downloaded}")
            print(f"アップロードファイル数: {result.files_uploaded}")
            print(f"ロードファイル数: {result.files_loaded}")
            print(f"ロードレコード数: {result.records_loaded}")
            print(f"処理時間: {result.duration_seconds:.2f}秒")

            if result.error_message:
                print(f"\nエラー: {result.error_message}")

            print("\nステップ詳細:")
            for step in result.steps:
                print(
                    f"  {step.step_name}: {step.status} "
                    f"({step.duration_seconds:.2f}秒)"
                )
                if step.error_message:
                    print(f"    エラー: {step.error_message}")

        sys.exit(0 if result.status == "success" else 1)

    except Exception as e:
        logger.error(f"パイプライン実行エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
