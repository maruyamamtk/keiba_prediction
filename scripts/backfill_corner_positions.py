#!/usr/bin/env python3
"""
raw.race_results の corner_position_1〜4 バックフィルスクリプト（Issue #446）

パーサーが corner_position_1〜4 を常に None にしていたため、既存行はこの4列が全件 NULL。
SEC ファイルを再パースしてコーナー順位だけを一時テーブルに積み、**corner_position_1〜4 の列だけを UPDATE** する。
通常の再ロード（MERGE UPSERT）は全列を上書きするため使わない（既存の正常な列を構造上変えないため）。

手順:
  1. 対象期間の開催日を raw.race_results から取得し、各日の SEC を GCS（優先）またはローカルから読む
  2. corner_position_1〜4 を一時テーブルへロード（WRITE_TRUNCATE）
  3. 初回のみ raw.race_results をバックアップテーブルへコピー
  4. corner_position_1〜4 のみ UPDATE
  5. バックアップと突合し、corner 以外の列に差分がないことを検証（差分があれば終了コード1）

使用方法:
    # 対象と SEC ソースの有無を確認のみ
    .venv/bin/python scripts/backfill_corner_positions.py --start-date 2025-01-01 --end-date 2025-01-31 --dry-run

    # 1ヶ月分で実行 → 検証、問題なければ全期間
    .venv/bin/python scripts/backfill_corner_positions.py --start-date 2025-01-01 --end-date 2025-01-31
    .venv/bin/python scripts/backfill_corner_positions.py --start-date 2016-01-01 --end-date 2026-12-31

    # ロールバック（バックアップから corner 列を戻すのではなくテーブルごと復元する）
    bq cp -f <PROJECT_ID>:raw.race_results_backup_issue446 <PROJECT_ID>:raw.race_results

環境変数: GCP_PROJECT_ID（.env から読み込み）
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# プロジェクトルートをパスに追加
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.automation.data.jrdb_parser import JRDBParser  # noqa: E402
from src.automation.data.load_to_bq import create_loader_from_env  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CORNER_COLUMNS = [f"corner_position_{i}" for i in range(1, 5)]
TARGET_TABLE = "race_results"
STAGING_TABLE = "_corner_backfill_issue446"
BACKUP_TABLE = "race_results_backup_issue446"
SEC_FOLDER = "Sec"
DEFAULT_LOCAL_DIR = PROJECT_ROOT / "downloaded_files" / SEC_FOLDER


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="raw.race_results の corner_position_1〜4 をバックフィルする")
    parser.add_argument("--start-date", required=True, help="対象開始日（YYYY-MM-DD）")
    parser.add_argument("--end-date", required=True, help="対象終了日（YYYY-MM-DD）")
    parser.add_argument("--dry-run", action="store_true", help="対象日と SEC ソースの有無の表示のみ行う")
    parser.add_argument("--local-dir", default=str(DEFAULT_LOCAL_DIR), help="ローカル SEC ディレクトリ")
    args = parser.parse_args(argv)
    try:
        args.start_date = date.fromisoformat(args.start_date)
        args.end_date = date.fromisoformat(args.end_date)
    except ValueError as e:
        parser.error(f"日付形式が不正です（YYYY-MM-DD）: {e}")
    if args.start_date > args.end_date:
        parser.error("--start-date が --end-date より後です")
    return args


def extract_corner_rows(content: str, start_date: date, end_date: date) -> list[dict]:
    """SEC ファイル内容から対象期間の (race_id, horse_number, corner_position_1〜4) を抽出する"""
    rows = []
    for record in JRDBParser.parse_file(content, "SEC"):
        race_date = record.get("race_date")
        if not race_date or not (start_date <= date.fromisoformat(race_date) <= end_date):
            continue
        rows.append(
            {
                "race_id": record["race_id"],
                "horse_number": record["horse_number"],
                **{col: record[col] for col in CORNER_COLUMNS},
            }
        )
    return rows


def read_sec_content(bucket, local_dir: Path, yymmdd: str) -> tuple[str | None, str]:
    """開催日の SEC 内容を GCS（優先）→ ローカルの順で読む。戻り値は (内容, ソース表記)"""
    blob = bucket.blob(f"{SEC_FOLDER}/SEC{yymmdd}.csv")
    if blob.exists():
        return blob.download_as_bytes().decode("utf-8"), f"gs://{bucket.name}/{blob.name}"
    local_path = local_dir / f"SEC{yymmdd}.csv"
    if local_path.exists():
        return local_path.read_text(encoding="utf-8"), str(local_path)
    return None, ""


def build_update_sql(table: str, staging: str) -> str:
    """corner_position_1〜4 の列だけを更新する UPDATE 文"""
    set_clause = ", ".join(f"{col} = s.{col}" for col in CORNER_COLUMNS)
    return f"""
        UPDATE `{table}` t
        SET {set_clause}
        FROM `{staging}` s
        WHERE t.race_id = s.race_id AND t.horse_number = s.horse_number
          AND t.race_date BETWEEN @start_date AND @end_date
    """


def build_verify_sql(table: str, backup: str) -> str:
    """対象期間で corner 以外の列がバックアップと一致しない行数などを数える SELECT 文"""
    except_clause = ", ".join(CORNER_COLUMNS)
    return f"""
        WITH cur AS (
          SELECT race_id, horse_number, TO_JSON_STRING(x) AS row_json
          FROM (SELECT * EXCEPT({except_clause}) FROM `{table}`
                WHERE race_date BETWEEN @start_date AND @end_date) x
        ),
        bak AS (
          SELECT race_id, horse_number, TO_JSON_STRING(x) AS row_json
          FROM (SELECT * EXCEPT({except_clause}) FROM `{backup}`
                WHERE race_date BETWEEN @start_date AND @end_date) x
        )
        SELECT
          COUNTIF(cur.race_id IS NOT NULL AND bak.race_id IS NOT NULL
                  AND cur.row_json != bak.row_json) AS n_changed,
          COUNTIF(bak.race_id IS NULL) AS n_only_current,
          COUNTIF(cur.race_id IS NULL) AS n_only_backup
        FROM cur FULL OUTER JOIN bak USING (race_id, horse_number)
    """


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args(argv)

    loader = create_loader_from_env()
    if loader is None:
        return 1
    client = loader.bq_client
    dataset = f"{loader.project_id}.{loader.dataset_id}"
    table, staging, backup = (f"{dataset}.{name}" for name in (TARGET_TABLE, STAGING_TABLE, BACKUP_TABLE))
    date_params = [
        bigquery.ScalarQueryParameter("start_date", "DATE", args.start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", args.end_date),
    ]
    query_config = bigquery.QueryJobConfig(query_parameters=date_params)

    race_dates = [
        row.race_date
        for row in client.query(
            f"SELECT DISTINCT race_date FROM `{table}` "
            "WHERE race_date BETWEEN @start_date AND @end_date ORDER BY race_date",
            job_config=query_config,
        ).result()
    ]
    logger.info(f"対象開催日: {len(race_dates)}日 ({args.start_date} 〜 {args.end_date})")

    bucket = loader.storage_client.bucket(loader.bucket_name)
    local_dir = Path(args.local_dir)
    rows: list[dict] = []
    missing: list[str] = []
    for race_date in race_dates:
        yymmdd = race_date.strftime("%y%m%d")
        content, source = read_sec_content(bucket, local_dir, yymmdd)
        if content is None:
            missing.append(race_date.isoformat())
            continue
        day_rows = extract_corner_rows(content, race_date, race_date)
        logger.debug(f"{source}: {len(day_rows)}行")
        rows.extend(day_rows)

    if missing:
        logger.warning(f"SEC ソースが見つからない開催日（corner は NULL のまま）: {missing}")
    logger.info(f"抽出行数: {len(rows)}")
    if args.dry_run or not rows:
        return 1 if missing else 0

    # 一時テーブルへロード
    staging_schema = [
        bigquery.SchemaField("race_id", "STRING"),
        bigquery.SchemaField("horse_number", "INTEGER"),
        *[bigquery.SchemaField(col, "INTEGER") for col in CORNER_COLUMNS],
    ]
    client.load_table_from_json(
        rows,
        staging,
        job_config=bigquery.LoadJobConfig(
            schema=staging_schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    ).result()

    # 初回のみバックアップ（2回目以降の段階実行でも最初の状態と突合できるよう上書きしない）
    try:
        client.get_table(backup)
        logger.info(f"バックアップ既存: {backup}")
    except NotFound:
        client.copy_table(table, backup).result()
        logger.info(f"バックアップ作成: {backup}")

    update_job = client.query(build_update_sql(table, staging), job_config=query_config)
    update_job.result()
    logger.info(f"UPDATE 行数: {update_job.num_dml_affected_rows}")

    verify = next(iter(client.query(build_verify_sql(table, backup), job_config=query_config).result()))
    fill = next(
        iter(
            client.query(
                f"SELECT COUNT(*) AS n, "
                + ", ".join(f"COUNTIF({col} IS NOT NULL) AS {col}" for col in CORNER_COLUMNS)
                + f" FROM `{table}` WHERE race_date BETWEEN @start_date AND @end_date",
                job_config=query_config,
            ).result()
        )
    )
    logger.info(
        "corner 非NULL率: "
        + ", ".join(f"{col}={fill[col] / fill['n']:.3f}" for col in CORNER_COLUMNS if fill["n"])
    )
    logger.info(
        f"突合（corner 以外の列）: 変化={verify.n_changed} / "
        f"現行のみ={verify.n_only_current} / バックアップのみ={verify.n_only_backup}"
    )
    client.delete_table(staging, not_found_ok=True)

    if verify.n_changed or verify.n_only_backup:
        logger.error("corner 以外の列に差分があります。バックアップからの復元を検討してください")
        return 1
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
