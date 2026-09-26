#!/usr/bin/env python3
"""
raw.race_results の horse_weight_diff バックフィルスクリプト（Issue #450）

パーサーが馬体重増減の「- 2」形式（符号と数字の間に空白がある負の1桁）を int に変換できず、既存行では NULL になっている。
SEC ファイルを修正後のパーサーで再パースし、**horse_weight_diff の列だけを UPDATE** する。
通常の再ロード（MERGE UPSERT）は全列を上書きするため使わない。
SEC の探し方（GCS 優先→ローカル）は scripts/backfill_corner_positions.py（#446）と同じ。

手順:
  1. 対象期間の開催日を raw.race_results から取得し、各日の SEC を GCS（優先）またはローカルから読む
  2. (race_id, horse_number, horse_weight_diff) を一時テーブルへロード（WRITE_TRUNCATE）
  3. 初回のみ raw.race_results をバックアップテーブルへコピー
  4. horse_weight_diff のみ UPDATE
  5. バックアップと突合し、horse_weight_diff 以外の列に差分がないこと、
     horse_weight_diff の既存の非NULL値が変わっていないことを検証（違反があれば終了コード1）

使用方法:
    # 対象と SEC ソースの有無を確認のみ
    .venv/bin/python scripts/backfill_horse_weight_diff.py --start-date 2025-01-01 --end-date 2025-01-31 --dry-run

    # 1ヶ月分で実行 → 検証、問題なければ全期間
    .venv/bin/python scripts/backfill_horse_weight_diff.py --start-date 2025-01-01 --end-date 2025-01-31
    .venv/bin/python scripts/backfill_horse_weight_diff.py --start-date 2016-01-01 --end-date 2026-12-31

    # ロールバック（テーブルごと復元する）
    bq cp -f <PROJECT_ID>:raw.race_results_backup_issue450 <PROJECT_ID>:raw.race_results

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

from scripts.backfill_corner_positions import DEFAULT_LOCAL_DIR, read_sec_content  # noqa: E402
from src.automation.data.jrdb_parser import JRDBParser  # noqa: E402
from src.automation.data.load_to_bq import create_loader_from_env  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TARGET_COLUMN = "horse_weight_diff"
TARGET_TABLE = "race_results"
STAGING_TABLE = "_horse_weight_diff_backfill_issue450"
BACKUP_TABLE = "race_results_backup_issue450"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="raw.race_results の horse_weight_diff をバックフィルする")
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


def extract_rows(content: str, start_date: date, end_date: date) -> list[dict]:
    """SEC ファイル内容から対象期間の (race_id, horse_number, horse_weight_diff) を抽出する"""
    rows = []
    for record in JRDBParser.parse_file(content, "SEC"):
        race_date = record.get("race_date")
        if not race_date or not (start_date <= date.fromisoformat(race_date) <= end_date):
            continue
        rows.append(
            {
                "race_id": record["race_id"],
                "horse_number": record["horse_number"],
                TARGET_COLUMN: record[TARGET_COLUMN],
            }
        )
    return rows


def build_update_sql(table: str, staging: str) -> str:
    """horse_weight_diff の列だけを更新する UPDATE 文"""
    return f"""
        UPDATE `{table}` t
        SET {TARGET_COLUMN} = s.{TARGET_COLUMN}
        FROM `{staging}` s
        WHERE t.race_id = s.race_id AND t.horse_number = s.horse_number
          AND t.race_date BETWEEN @start_date AND @end_date
    """


def build_verify_sql(table: str, backup: str) -> str:
    """対象期間で、horse_weight_diff 以外の列の不一致行数と、horse_weight_diff の変化の内訳を数える SELECT 文"""
    return f"""
        WITH cur AS (
          SELECT race_id, horse_number, {TARGET_COLUMN} AS v,
                 TO_JSON_STRING((SELECT AS STRUCT t.* EXCEPT({TARGET_COLUMN}))) AS rest_json
          FROM `{table}` t WHERE race_date BETWEEN @start_date AND @end_date
        ),
        bak AS (
          SELECT race_id, horse_number, {TARGET_COLUMN} AS v,
                 TO_JSON_STRING((SELECT AS STRUCT b.* EXCEPT({TARGET_COLUMN}))) AS rest_json
          FROM `{backup}` b WHERE race_date BETWEEN @start_date AND @end_date
        )
        SELECT
          COUNTIF(cur.race_id IS NOT NULL AND bak.race_id IS NOT NULL
                  AND cur.rest_json != bak.rest_json) AS n_changed,
          COUNTIF(bak.race_id IS NULL) AS n_only_current,
          COUNTIF(cur.race_id IS NULL) AS n_only_backup,
          -- 既存の非NULL値が書き換わった行（修正はNULL→負の1桁のみのはず）
          COUNTIF(bak.v IS NOT NULL AND (cur.v IS NULL OR cur.v != bak.v)) AS n_overwritten,
          COUNTIF(bak.v IS NULL AND cur.v IS NOT NULL) AS n_filled,
          COUNTIF(bak.v IS NULL AND cur.v IS NOT NULL AND NOT (cur.v BETWEEN -9 AND -1)) AS n_filled_unexpected,
          COUNTIF(cur.race_id IS NOT NULL) AS n_rows,
          COUNTIF(cur.race_id IS NOT NULL AND cur.v IS NULL) AS n_null_current,
          COUNTIF(bak.race_id IS NOT NULL AND bak.v IS NULL) AS n_null_backup
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
    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", args.start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", args.end_date),
        ]
    )

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
        content, source = read_sec_content(bucket, local_dir, race_date.strftime("%y%m%d"))
        if content is None:
            missing.append(race_date.isoformat())
            continue
        day_rows = extract_rows(content, race_date, race_date)
        logger.debug(f"{source}: {len(day_rows)}行")
        rows.extend(day_rows)

    if missing:
        logger.warning(f"SEC ソースが見つからない開催日（horse_weight_diff は旧値のまま）: {missing}")
    logger.info(f"抽出行数: {len(rows)}（うち非NULL {sum(r[TARGET_COLUMN] is not None for r in rows)}行）")
    if args.dry_run or not rows:
        return 1 if missing else 0

    client.load_table_from_json(
        rows,
        staging,
        job_config=bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("race_id", "STRING"),
                bigquery.SchemaField("horse_number", "INTEGER"),
                bigquery.SchemaField(TARGET_COLUMN, "INTEGER"),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
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

    v = next(iter(client.query(build_verify_sql(table, backup), job_config=query_config).result()))
    logger.info(
        f"突合（horse_weight_diff 以外の列）: 変化={v.n_changed} / 現行のみ={v.n_only_current} / "
        f"バックアップのみ={v.n_only_backup}"
    )
    logger.info(
        f"horse_weight_diff: NULL→値={v.n_filled}（うち負の1桁以外={v.n_filled_unexpected}） / "
        f"既存値の書き換え={v.n_overwritten} / NULL率 {v.n_null_backup / max(v.n_rows, 1):.4f} → "
        f"{v.n_null_current / max(v.n_rows, 1):.4f}（{v.n_rows}行）"
    )
    client.delete_table(staging, not_found_ok=True)

    if v.n_changed or v.n_only_backup or v.n_overwritten or v.n_filled_unexpected:
        logger.error("想定外の差分があります。バックアップからの復元を検討してください")
        return 1
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
