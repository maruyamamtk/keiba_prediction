"""
成績データ（raw.race_results）の欠損検知と SEC 再取得

JRDB の SEC（成績データ）は開催当日に速報版が公開され、IDM などは後日（木曜頃）の
確定版で埋まる。速報版のままロードされた日は IDM が大半 NULL・行数不足のまま残るため
（Issue #440: 2026-01〜02 の9開催日で発生）、以下を提供する:

- find_incomplete_result_dates: horse_results（出走表）と比較して成績が不完全な開催日を検知
- refetch_sec_files: 指定日の SEC を JRDB から強制再取得 → GCS 上書き → BigQuery 再ロード
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING

from google.cloud import bigquery

if TYPE_CHECKING:
    from src.automation.data.jrdb_downloader import JRDBDownloader
    from src.automation.data.load_to_bq import BigQueryLoader
    from src.automation.data.upload_to_gcs import GCSUploader

logger = logging.getLogger(__name__)

SEC_DATATYPE = "SEC"

# 平常時の IDM NULL 率は 3〜5%（取消・除外・競走中止）。確定版未反映の日は 90% 前後になる
DEFAULT_IDM_NULL_RATE_THRESHOLD = 0.2
# 平常時の race_results / horse_results の行数比は ≈1.0（取消等で数行差）
DEFAULT_MIN_ROW_RATIO = 0.9


@dataclass
class IncompleteResultDate:
    """成績データが不完全な開催日"""

    race_date: date
    expected_rows: int  # horse_results（出走表）の行数
    actual_rows: int  # race_results の行数
    idm_null_rows: int

    @property
    def idm_null_rate(self) -> float:
        return self.idm_null_rows / self.actual_rows if self.actual_rows else 1.0

    @property
    def yymmdd(self) -> str:
        return self.race_date.strftime("%y%m%d")

    def to_dict(self) -> dict:
        return {
            "race_date": self.race_date.isoformat(),
            "expected_rows": self.expected_rows,
            "actual_rows": self.actual_rows,
            "idm_null_rate": round(self.idm_null_rate, 3),
        }


@dataclass
class RefetchResult:
    """SEC 再取得の結果"""

    reloaded: list[str] = field(default_factory=list)  # 再ロードに成功した yymmdd
    failed: list[str] = field(default_factory=list)  # 取得・アップロード・ロードのいずれかに失敗した yymmdd
    records: int = 0


def find_incomplete_result_dates(
    client: bigquery.Client,
    project_id: str,
    start_date: date,
    end_date: date,
    idm_null_rate_threshold: float = DEFAULT_IDM_NULL_RATE_THRESHOLD,
    min_row_ratio: float = DEFAULT_MIN_ROW_RATIO,
) -> list[IncompleteResultDate]:
    """
    horse_results（出走表）を基準に、成績が不完全な開催日を検出する

    以下のいずれかに該当する開催日を返す:
    - race_results の行数が出走表の min_row_ratio 未満（成績行の欠落・未ロード）
    - race_results の IDM NULL 率が idm_null_rate_threshold 超（速報版のまま）

    Args:
        client: BigQuery クライアント
        project_id: GCP プロジェクトID
        start_date: 検査開始日（含む）
        end_date: 検査終了日（含む）
        idm_null_rate_threshold: IDM NULL 率の閾値
        min_row_ratio: 出走表に対する成績行数の下限比

    Returns:
        不完全な開催日のリスト（日付昇順）
    """
    query = f"""
        with entries as (
          select r_i.race_date, count(*) as expected_rows
          from `{project_id}.raw.horse_results` as h_r
          join `{project_id}.raw.race_info` as r_i using (race_id)
          where r_i.race_date between @start_date and @end_date
          group by 1
        ),
        results as (
          select race_date, count(*) as actual_rows, countif(idm is null) as idm_null_rows
          from `{project_id}.raw.race_results`
          where race_date between @start_date and @end_date
          group by 1
        )
        select
          e.race_date,
          e.expected_rows,
          coalesce(r.actual_rows, 0) as actual_rows,
          coalesce(r.idm_null_rows, 0) as idm_null_rows
        from entries as e
        left join results as r using (race_date)
        where coalesce(r.actual_rows, 0) < e.expected_rows * @min_row_ratio
           or safe_divide(r.idm_null_rows, r.actual_rows) > @idm_null_rate_threshold
        order by e.race_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            bigquery.ScalarQueryParameter("min_row_ratio", "FLOAT64", min_row_ratio),
            bigquery.ScalarQueryParameter(
                "idm_null_rate_threshold", "FLOAT64", idm_null_rate_threshold
            ),
        ]
    )
    rows = client.query(query, job_config=job_config).result()
    return [
        IncompleteResultDate(
            race_date=row.race_date,
            expected_rows=row.expected_rows,
            actual_rows=row.actual_rows,
            idm_null_rows=row.idm_null_rows,
        )
        for row in rows
    ]


def refetch_sec_files(
    downloader: "JRDBDownloader",
    uploader: "GCSUploader",
    loader: "BigQueryLoader",
    yymmdd_list: list[str],
) -> RefetchResult:
    """
    指定日の SEC を JRDB から強制再取得し、GCS を上書きして BigQuery に再ロードする

    ダウンロード先は downloader.output_dir（ローカル運用なら downloaded_files/ の
    速報版ファイルもこのとき確定版に置き換わる）。BigQuery へは MERGE UPSERT のため
    既存行は更新、欠落行は追加される。

    Args:
        downloader: JRDB ダウンローダー
        uploader: GCS アップローダー
        loader: BigQuery ローダー
        yymmdd_list: 再取得する開催日（yymmdd）のリスト

    Returns:
        RefetchResult
    """
    result = RefetchResult()
    folder = downloader.datatype_to_folder(SEC_DATATYPE)

    for yymmdd in yymmdd_list:
        file_name = f"{SEC_DATATYPE}{yymmdd}.csv"
        local_path = downloader.get_output_dir() / folder / file_name
        blob_name = f"{folder}/{file_name}"

        if not downloader.download_single(SEC_DATATYPE, yymmdd, force=True):
            logger.error(f"SEC再取得失敗（ダウンロード）: {file_name}")
            result.failed.append(yymmdd)
            continue

        if not uploader.upload_file(local_path, blob_name):
            logger.error(f"SEC再取得失敗（GCSアップロード）: {blob_name}")
            result.failed.append(yymmdd)
            continue

        load_result = loader.load_file(blob_name)
        if load_result.status != "success":
            logger.error(f"SEC再取得失敗（BQロード）: {blob_name}: {load_result.error}")
            result.failed.append(yymmdd)
            continue

        logger.info(f"SEC再ロード完了: {blob_name} ({load_result.records_processed}行)")
        result.reloaded.append(yymmdd)
        result.records += load_result.records_processed

    return result
