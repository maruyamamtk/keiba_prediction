"""
成績データ（raw.race_results）の欠損検知と SEC 再取得

JRDB の SEC（成績データ）は開催当日に速報版が公開され、IDM などは後日（木曜頃）の
確定版で埋まる。速報版のままロードされた日は IDM が大半 NULL・行数不足のまま残るため
（Issue #440: 2026-01〜02 の9開催日で発生）、以下を提供する:

- find_incomplete_result_dates: 成績が不完全（IDM の大半が NULL、または成績行なし）な開催日を検知
- refetch_sec_files: 指定日の SEC を JRDB から強制再取得 → GCS 上書き → BigQuery 再ロード
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING

from google.cloud import bigquery

from src.automation.data.jrdb_downloader import IDM_NULL_RATE_THRESHOLD, SEC_DATATYPE

if TYPE_CHECKING:
    from src.automation.data.jrdb_downloader import JRDBDownloader
    from src.automation.data.load_to_bq import BigQueryLoader
    from src.automation.data.upload_to_gcs import GCSUploader

logger = logging.getLogger(__name__)


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
    unavailable: list[str] = field(default_factory=list)  # JRDBにSECが公開されていない yymmdd（開催中止等）
    remaining: list[IncompleteResultDate] = field(default_factory=list)  # 再ロード後も不完全な開催日
    records: int = 0


def find_incomplete_result_dates(
    client: bigquery.Client,
    project_id: str,
    start_date: date,
    end_date: date,
    dataset_id: str = "raw",
) -> list[IncompleteResultDate]:
    """
    成績が不完全な開催日を検出する

    horse_results（出走表）がある開催日のうち、以下のいずれかに該当する日を返す:
    - race_results の成績行がない（未ロード）
    - race_results の IDM NULL 率が IDM_NULL_RATE_THRESHOLD 超（速報版のまま）

    行数の不足だけでは判定しない（開催途中の中止など JRDB 側でも成績がない日を毎日取り直さないため）。

    Args:
        client: BigQuery クライアント
        project_id: GCP プロジェクトID
        start_date: 検査開始日（含む）
        end_date: 検査終了日（含む）
        dataset_id: rawデータのデータセットID（再ロード先の BigQueryLoader.dataset_id と揃える）

    Returns:
        不完全な開催日のリスト（日付昇順）
    """
    query = f"""
        with entries as (
          select r_i.race_date, count(*) as expected_rows
          from `{project_id}.{dataset_id}.horse_results` as h_r
          join `{project_id}.{dataset_id}.race_info` as r_i using (race_id)
          where r_i.race_date between @start_date and @end_date
          group by 1
        ),
        results as (
          select race_date, count(*) as actual_rows, countif(idm is null) as idm_null_rows
          from `{project_id}.{dataset_id}.race_results`
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
        where r.actual_rows is null
           or r.idm_null_rows / r.actual_rows > @idm_null_rate_threshold
        order by e.race_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            bigquery.ScalarQueryParameter(
                "idm_null_rate_threshold", "FLOAT64", IDM_NULL_RATE_THRESHOLD
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
    既存行は更新、欠落行は追加される。再ロードした日は再検査し、なお不完全な日を remaining に返す。

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
    available = set(downloader.get_available_dates(SEC_DATATYPE))
    if not available:
        # 一覧ページの取得失敗（認証・通信エラー）を「全日公開なし」と誤判定しない
        logger.error("JRDBのSEC公開日一覧を取得できませんでした")
        result.failed.extend(yymmdd_list)
        return result

    for yymmdd in yymmdd_list:
        if yymmdd not in available:
            logger.warning(f"JRDBにSECが公開されていません（開催中止等）: {SEC_DATATYPE}{yymmdd}")
            result.unavailable.append(yymmdd)
            continue

        local_path = downloader.local_csv_path(SEC_DATATYPE, yymmdd)
        blob_name = f"{folder}/{local_path.name}"
        # 1日分の失敗（GCSの一時エラー等の例外を含む）で残りの日の再取得を止めない
        try:
            if not downloader.download_single(SEC_DATATYPE, yymmdd, force=True):
                raise RuntimeError("ダウンロード失敗")
            if not uploader.upload_file(local_path, blob_name):
                raise RuntimeError("GCSアップロード失敗")
            load_result = loader.load_file(blob_name)
            if load_result.status != "success":
                raise RuntimeError(f"BQロード失敗: {load_result.error}")
        except Exception as e:
            logger.error(f"SEC再取得失敗: {blob_name}: {e}")
            result.failed.append(yymmdd)
            continue

        logger.info(f"SEC再ロード完了: {blob_name} ({load_result.records_processed}行)")
        result.reloaded.append(yymmdd)
        result.records += load_result.records_processed

    reloaded_dates = {datetime.strptime(d, "%y%m%d").date() for d in result.reloaded}
    if reloaded_dates:
        result.remaining = [
            d
            for d in find_incomplete_result_dates(
                loader.bq_client, loader.project_id, min(reloaded_dates), max(reloaded_dates),
                dataset_id=loader.dataset_id,
            )
            if d.race_date in reloaded_dates
        ]
        for d in result.remaining:
            logger.warning(f"SEC再取得後も不完全（JRDB側のデータの可能性）: {d.to_dict()}")
    return result
