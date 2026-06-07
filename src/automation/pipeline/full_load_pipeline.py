"""
過去分全件ロードパイプライン

指定期間のデータをJRDBからダウンロード→GCS→BigQuery→特徴量生成を一括実行する。
HTTP APIで手動トリガーし、初回セットアップやデータ欠損の補完に使用。

Issue #58: 過去分全件ロード処理の実装
Issue #59: 特徴量生成パイプラインのCloud Run統合
"""

import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime

from dateutil.relativedelta import relativedelta
from typing import TYPE_CHECKING

from src.automation.data.jrdb_downloader import JRDBDownloader, create_downloader_from_env
from src.automation.data.load_to_bq import BigQueryLoader
from src.automation.data.upload_to_gcs import GCSUploader, create_uploader_from_env

if TYPE_CHECKING:
    from src.ml.features.feature_pipeline import FeaturePipeline

logger = logging.getLogger(__name__)


@dataclass
class FullLoadStepResult:
    """各ステップの実行結果"""

    step_name: str
    status: str  # "success", "failed", "partial"
    duration_seconds: float = 0.0
    details: dict = field(default_factory=dict)
    error_message: str | None = None


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
    features_inserted: int = 0
    duration_seconds: float = 0.0
    steps: list[FullLoadStepResult] = field(default_factory=list)
    error_message: str | None = None

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


class FullLoadPipeline:
    """
    過去分全件ロードパイプライン

    処理フロー:
    1. 指定期間のデータをJRDBからダウンロード
    2. GCSにアップロード（差分のみ）
    3. BigQueryにロード（重複スキップ）
    4. 特徴量生成
    """

    def __init__(
        self,
        downloader: JRDBDownloader | None = None,
        uploader: GCSUploader | None = None,
        bq_loader: BigQueryLoader | None = None,
        feature_pipeline: "FeaturePipeline | None" = None,
    ):
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
    def parse_date(date_str: str | None) -> date | None:
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
        self, start_date: date | None, end_date: date | None
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
            start_yymmdd = self.date_to_yymmdd(start_date) if start_date else "200101"
            end_yymmdd = self.date_to_yymmdd(end_date) if end_date else None
            logger.info(
                f"全件ダウンロード開始: {start_yymmdd}〜{end_yymmdd or '最新'}"
            )

            results = self.downloader.download_all_from_date(start_yymmdd, end_yymmdd)

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
        self, start_date: date | None, end_date: date | None
    ) -> FullLoadStepResult:
        """
        Step 3: BigQueryにロード

        GCS上の全CSVファイルをロード（重複スキップ有効）。
        日付フィルタが指定されている場合、ファイル名のyymmdd部分でフィルタする。
        """
        step_name = "load_to_bq"
        start_time = time.time()

        try:
            from src.automation.data.load_to_bq import TABLE_MAPPING

            logger.info("BigQuery全件ロード開始")

            # BQテーブルの存在確認（未作成テーブルがある場合は警告ログを出す）
            table_exists = self.bq_loader.check_tables_exist()
            missing_tables = [t for t, exists in table_exists.items() if not exists]
            if missing_tables:
                logger.error(
                    f"BQテーブルが未作成のため、以下のデータはロードされません: {missing_tables}。"
                    f"scripts/setup_bigquery.sh を実行してテーブルを作成してください。"
                )

            # サポート対象データタイプのファイルのみを取得（daily_pipelineと同様）
            supported_types = list(TABLE_MAPPING.keys())
            csv_files = self.bq_loader.list_csv_files(
                prefix="", data_types=supported_types
            )

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
        files: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> list[str]:
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

    def _step_rebuild_pedigree(self) -> FullLoadStepResult:
        """
        Step 3.5: raw.pedigree 再構築

        horse_master への UKC ロード後に実行し、dam_id を名前マッチングで解決する。
        失敗してもパイプライン全体は継続（母馬特徴量が NULL になるのみ）。
        """
        step_name = "rebuild_pedigree"
        start_time = time.time()

        try:
            import os as _os
            from google.cloud import bigquery as _bigquery

            project_id = (
                getattr(self.bq_loader, "project_id", None)
                or _os.environ.get("GCP_PROJECT_ID")
            )
            if not project_id:
                logger.warning("project_id が取得できないため raw.pedigree 再構築をスキップ")
                return FullLoadStepResult(
                    step_name=step_name,
                    status="partial",
                    duration_seconds=time.time() - start_time,
                    details={"skipped": True},
                )

            client = _bigquery.Client(project=project_id)
            query = f"""
            CREATE OR REPLACE TABLE `{project_id}`.raw.pedigree AS
            WITH dam_lookup AS (
              SELECT
                horse_name,
                horse_id,
                ROW_NUMBER() OVER (
                  PARTITION BY horse_name
                  ORDER BY sex_code = 2 DESC, horse_id
                ) AS rn
              FROM `{project_id}`.raw.horse_master
              WHERE horse_name IS NOT NULL
            )
            SELECT
              h.horse_id,
              h.horse_name,
              NULL        AS sire_id,
              h.sire_name,
              d.horse_id  AS dam_id,
              h.dam_name,
              NULL        AS dam_sire_id,
              h.broodmare_sire_name AS dam_sire_name,
              CAST(h.sire_line_code AS STRING) AS sire_line,
              EXTRACT(YEAR FROM h.birth_date) AS birth_year,
              h.sex,
              CAST(h.coat_color_code AS STRING) AS coat_color,
              h.breeder_name AS breeder,
              h.owner_name   AS owner,
              CURRENT_TIMESTAMP() AS created_at,
              CURRENT_TIMESTAMP() AS updated_at
            FROM `{project_id}`.raw.horse_master AS h
            LEFT JOIN dam_lookup AS d
              ON h.dam_name = d.horse_name AND d.rn = 1
            """
            job = client.query(query)
            job.result()

            stats_rows = list(
                client.query(
                    f"SELECT COUNT(*) as total, COUNTIF(dam_id IS NOT NULL) as resolved "
                    f"FROM `{project_id}`.raw.pedigree"
                ).result()
            )
            total = stats_rows[0].total
            resolved = stats_rows[0].resolved
            logger.info(
                f"raw.pedigree 再構築完了: total={total}, dam_id解決={resolved} "
                f"({resolved/total*100:.1f}%)"
            )
            return FullLoadStepResult(
                step_name=step_name,
                status="success",
                duration_seconds=time.time() - start_time,
                details={"total": total, "dam_id_resolved": resolved},
            )

        except Exception as e:
            logger.warning(f"raw.pedigree 再構築エラー（パイプライン継続）: {e}")
            # 母馬特徴量がNULLになるのみ。全体statusに影響させない
            return FullLoadStepResult(
                step_name=step_name,
                status="success",
                duration_seconds=time.time() - start_time,
                details={"warning": str(e)},
            )

    def _step_generate_features(
        self,
        start_date: date | None,
        end_date: date | None,
    ) -> FullLoadStepResult:
        """
        Step 4: 特徴量生成

        BigQuery上のrawデータから特徴量を生成してfeatures.training_dataに書き込む。

        Args:
            start_date: 開始日付
            end_date: 終了日付

        Returns:
            FullLoadStepResult
        """
        step_name = "generate_features"
        start_time = time.time()

        try:
            # デフォルト日付範囲の設定
            end_str = (
                end_date.strftime("%Y-%m-%d")
                if end_date
                else date.today().strftime("%Y-%m-%d")
            )
            start_str = (
                start_date.strftime("%Y-%m-%d")
                if start_date
                else (date.fromisoformat(end_str) - relativedelta(years=6)).strftime("%Y-%m-%d")
            )
            logger.info(f"特徴量生成開始: {start_str} 〜 {end_str}")

            result = self.feature_pipeline.run(
                start_date=start_str,
                end_date=end_str,
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

            return FullLoadStepResult(
                step_name=step_name,
                status="success",
                duration_seconds=time.time() - start_time,
                details=details,
            )

        except Exception as e:
            logger.error(f"特徴量生成エラー: {e}")
            return FullLoadStepResult(
                step_name=step_name,
                status="failed",
                duration_seconds=time.time() - start_time,
                error_message=str(e),
            )

    def run(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
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

            # Step 3.5: raw.pedigree 再構築（UKCロード後に dam_id を最新化）
            pedigree_result = self._step_rebuild_pedigree()
            result.steps.append(pedigree_result)
            # 失敗しても継続（母馬特徴量がNULLになるのみでパイプライン全体は動作する）

            # Step 4: 特徴量生成
            feature_result = self._step_generate_features(
                parsed_start, parsed_end
            )
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

            # 全体ステータス判定
            if any(s.status == "partial" for s in result.steps):
                result.status = "partial"

            result.duration_seconds = time.time() - start_time
            logger.info(
                f"全件ロードパイプライン完了: job_id={job_id}, "
                f"status={result.status}, files={result.files_loaded}, "
                f"records={result.records_loaded}, "
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
            if self._downloader:
                try:
                    self._downloader.cleanup()
                except Exception as e:
                    logger.warning(f"クリーンアップエラー: {e}")


def rebuild_pedigree_table(project_id: str) -> dict:
    """
    raw.pedigree を horse_master の最新データで再構築する。

    dam_id は horse_master.dam_name → horse_master.horse_id の名前マッチングで解決。
    特徴量生成前に呼び出すことで母馬TE特徴量の解決率を最大化する。

    Returns:
        {"total": int, "dam_id_resolved": int, "resolution_pct": float}
    """
    from google.cloud import bigquery as _bigquery

    client = _bigquery.Client(project=project_id)
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}`.raw.pedigree AS
    WITH dam_lookup AS (
      SELECT
        horse_name,
        horse_id,
        ROW_NUMBER() OVER (
          PARTITION BY horse_name
          ORDER BY sex_code = 2 DESC, horse_id
        ) AS rn
      FROM `{project_id}`.raw.horse_master
      WHERE horse_name IS NOT NULL
    )
    SELECT
      h.horse_id,
      h.horse_name,
      NULL        AS sire_id,
      h.sire_name,
      d.horse_id  AS dam_id,
      h.dam_name,
      NULL        AS dam_sire_id,
      h.broodmare_sire_name AS dam_sire_name,
      CAST(h.sire_line_code AS STRING) AS sire_line,
      EXTRACT(YEAR FROM h.birth_date) AS birth_year,
      h.sex,
      CAST(h.coat_color_code AS STRING) AS coat_color,
      h.breeder_name AS breeder,
      h.owner_name   AS owner,
      CURRENT_TIMESTAMP() AS created_at,
      CURRENT_TIMESTAMP() AS updated_at
    FROM `{project_id}`.raw.horse_master AS h
    LEFT JOIN dam_lookup AS d
      ON h.dam_name = d.horse_name AND d.rn = 1
    """
    client.query(query).result()

    stats = list(
        client.query(
            f"SELECT COUNT(*) as total, COUNTIF(dam_id IS NOT NULL) as resolved "
            f"FROM `{project_id}`.raw.pedigree"
        ).result()
    )[0]
    total = stats.total
    resolved = stats.resolved
    logger.info(
        f"raw.pedigree 再構築完了: total={total}, dam_id解決={resolved} "
        f"({resolved / total * 100:.1f}%)"
    )
    return {
        "total": total,
        "dam_id_resolved": resolved,
        "resolution_pct": round(resolved / total * 100, 1) if total else 0.0,
    }


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
        "--date",
        type=str,
        default=None,
        help="対象日付（YYYY-MM-DD形式、省略時は当日のみ取得）",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="開始日付（YYYY-MM-DD形式）",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="終了日付（YYYY-MM-DD形式）",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="結果をJSON形式で出力",
    )
    args = parser.parse_args()

    # --date と --start-date/--end-date の同時指定はエラー
    if args.date and (args.start_date or args.end_date):
        parser.error("--date と --start-date/--end-date は同時に指定できません")

    if args.date:
        # --date 指定: その日のみ
        start_date = args.date
        end_date = args.date
    elif args.start_date or args.end_date:
        # 期間指定: 従来の挙動
        start_date = args.start_date
        end_date = args.end_date
    else:
        # 全引数省略: 当日のみ
        today = date.today().strftime("%Y-%m-%d")
        start_date = today
        end_date = today

    load_dotenv()

    pipeline = FullLoadPipeline()

    try:
        result = pipeline.run(start_date, end_date)

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
