#!/usr/bin/env python3
"""
raw.horse_results の KYF 位置ズレ列のバックフィルスクリプト（Issue #452）

parse_kyf_line が総合指数〜馬主名の多くの列を JRDB 仕様からズレた文字位置で読んでいたため、
KYF/KYG/KYH を修正後のパーサーで再パースし、**位置を修正した列（TARGET_COLUMNS）だけを UPDATE** する。
通常の再ロード（MERGE UPSERT）は全列を上書きし、raw.load_history の成功済みファイルはスキップされるため使わない。
KY ファイルの探し方（GCS 優先→ローカル、レース網羅で判定）は scripts/backfill_blinker.py（#441）と同じ。

手順:
  1. 対象期間の開催日・レースを raw.race_info（horse_results に存在するレース）から取得し、
     各日の KYF/KYG/KYH を対象レースが揃うまで読む（揃わなかったレースは警告して終了コード1）
  2. (race_id, horse_number, TARGET_COLUMNS) を一時テーブルへロード（WRITE_TRUNCATE）
  3. 初回のみ raw.horse_results をバックアップテーブルへコピー
  4. TARGET_COLUMNS のみ UPDATE
  5. バックアップと突合し、TARGET_COLUMNS 以外の列に差分がないことを検証（差分があれば終了コード1）。
     あわせて列ごとの変化行数を表示する

使用方法:
    # 対象と KY ソースの有無を確認のみ
    .venv/bin/python scripts/backfill_kyf_columns.py --start-date 2025-01-01 --end-date 2025-01-31 --dry-run

    # 1ヶ月分で実行 → 検証、問題なければ全期間
    .venv/bin/python scripts/backfill_kyf_columns.py --start-date 2025-01-01 --end-date 2025-01-31
    .venv/bin/python scripts/backfill_kyf_columns.py --start-date 2016-01-01 --end-date 2026-12-31

    # ロールバック（テーブルごと復元する）
    bq cp -f <PROJECT_ID>:raw.horse_results_backup_issue452 <PROJECT_ID>:raw.horse_results

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

from scripts.backfill_blinker import DEFAULT_LOCAL_DIR, iter_ky_contents  # noqa: E402
from src.automation.data.jrdb_parser import JRDBParser  # noqa: E402
from src.automation.data.load_to_bq import create_loader_from_env  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TARGET_TABLE = "horse_results"
STAGING_TABLE = "_kyf_columns_backfill_issue452"
BACKUP_TABLE = "horse_results_backup_issue452"
KEY_COLUMNS = ("race_id", "horse_number")
# Issue #452 で読み取り位置を修正した列（base_odds/base_place_odds は5文字化で100倍台のオッズが正しくなる）
TARGET_COLUMNS = (
    "total_index", "running_style", "distance_aptitude", "improvement", "rotation",
    "base_odds", "base_place_odds",
    "specific_mark_circle", "specific_mark_circle2", "specific_mark_triangle",
    "specific_mark_triangle2", "specific_mark_x",
    "total_mark_circle", "total_mark_circle2", "total_mark_triangle", "total_mark_triangle2", "total_mark_x",
    "popularity_index", "training_index", "stable_index", "training_arrow_code", "stable_eval_code",
    "jockey_expected_win_rate", "surge_index", "hoof_code", "heavy_aptitude_code", "class_code",
    "bracket_number",
    "overall_mark", "idm_mark", "info_mark", "jockey_mark", "stable_mark", "training_mark", "surge_mark",
    "turf_aptitude", "dirt_aptitude",
    "position_index",
    "mid_position", "mid_gap", "mid_inside_outside",
    "last_3f_position", "last_3f_gap", "last_3f_inside_outside",
    "goal_position", "goal_gap", "goal_inside_outside", "development_code",
    "confirmed_weight", "confirmed_weight_diff", "sex_code", "owner_name",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="raw.horse_results の KYF 位置ズレ列をバックフィルする")
    parser.add_argument("--start-date", required=True, help="対象開始日（YYYY-MM-DD）")
    parser.add_argument("--end-date", required=True, help="対象終了日（YYYY-MM-DD）")
    parser.add_argument("--dry-run", action="store_true", help="対象日と KY ソースの網羅状況の表示のみ行う")
    parser.add_argument(
        "--local-dir", default=str(DEFAULT_LOCAL_DIR), help="ローカルのダウンロード先ルート（Kyf/, Jrdb/ を含む）"
    )
    args = parser.parse_args(argv)
    try:
        args.start_date = date.fromisoformat(args.start_date)
        args.end_date = date.fromisoformat(args.end_date)
    except ValueError as e:
        parser.error(f"日付形式が不正です（YYYY-MM-DD）: {e}")
    if args.start_date > args.end_date:
        parser.error("--start-date が --end-date より後です")
    return args


def extract_rows(content: str, data_type: str, race_ids: set[str] | None = None) -> list[dict]:
    """KY ファイル内容から (キー列 + TARGET_COLUMNS) を抽出する（race_ids 指定時はそのレースのみ）"""
    rows = []
    for record in JRDBParser.parse_file(content, data_type):
        if race_ids is not None and record["race_id"] not in race_ids:
            continue
        rows.append({col: record[col] for col in (*KEY_COLUMNS, *TARGET_COLUMNS)})
    return rows


def collect_day_rows(sources, race_ids: set[str]) -> tuple[list[dict], set[str]]:
    """対象レースが揃うまでソースを順に読み、(抽出行, どのソースにもなかったレース) を返す"""
    rows: list[dict] = []
    remaining = set(race_ids)
    for content, data_type, source in sources:
        found = extract_rows(content, data_type, remaining)
        logger.debug(f"{source}: {len(found)}行")
        rows.extend(found)
        remaining -= {r["race_id"] for r in found}
        if not remaining:
            break
    return rows, remaining


def build_update_sql(table: str, staging: str) -> str:
    """TARGET_COLUMNS だけを更新する UPDATE 文"""
    assignments = ",\n            ".join(f"{col} = s.{col}" for col in TARGET_COLUMNS)
    return f"""
        UPDATE `{table}` t
        SET {assignments}
        FROM `{staging}` s
        WHERE t.race_id = s.race_id AND t.horse_number = s.horse_number
    """


def build_verify_sql(table: str, backup: str) -> str:
    """TARGET_COLUMNS 以外の列の不一致行数と、TARGET_COLUMNS の列ごとの変化行数を数える SELECT 文"""
    except_cols = ", ".join(TARGET_COLUMNS)
    changed_cols = ",\n          ".join(
        f"COUNTIF(TO_JSON_STRING(cur.{col}) != TO_JSON_STRING(bak.{col})) AS changed_{col}"
        for col in TARGET_COLUMNS
    )
    return f"""
        WITH cur AS (
          SELECT t.*, TO_JSON_STRING((SELECT AS STRUCT t.* EXCEPT({except_cols}))) AS rest_json
          FROM `{table}` t
        ),
        bak AS (
          SELECT b.*, TO_JSON_STRING((SELECT AS STRUCT b.* EXCEPT({except_cols}))) AS rest_json
          FROM `{backup}` b
        )
        SELECT
          COUNTIF(cur.race_id IS NOT NULL AND bak.race_id IS NOT NULL
                  AND cur.rest_json != bak.rest_json) AS n_changed,
          COUNTIF(bak.race_id IS NULL) AS n_only_current,
          COUNTIF(cur.race_id IS NULL) AS n_only_backup,
          {changed_cols}
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

    # horse_results には日付列がないため、race_info から対象期間の開催日とレースを引く
    races_by_date: dict[date, set[str]] = {}
    for row in client.query(
        f"SELECT DISTINCT r.race_date, r.race_id FROM `{dataset}.race_info` r "
        f"WHERE r.race_date BETWEEN @start_date AND @end_date "
        f"AND r.race_id IN (SELECT race_id FROM `{table}`)",
        job_config=query_config,
    ).result():
        races_by_date.setdefault(row.race_date, set()).add(row.race_id)
    logger.info(f"対象開催日: {len(races_by_date)}日 ({args.start_date} 〜 {args.end_date})")

    bucket = loader.storage_client.bucket(loader.bucket_name)
    local_dir = Path(args.local_dir)
    rows_by_key: dict[tuple[str, int], dict] = {}
    missing: dict[str, int] = {}
    for race_date in sorted(races_by_date):
        yymmdd = race_date.strftime("%y%m%d")
        day_rows, missing_races = collect_day_rows(
            iter_ky_contents(bucket, local_dir, yymmdd), races_by_date[race_date]
        )
        if missing_races:
            missing[race_date.isoformat()] = len(missing_races)
        # UPDATE ... FROM は1行に複数のソース行が一致するとエラーになるため、キーで一意化する
        for r in day_rows:
            rows_by_key[(r["race_id"], r["horse_number"])] = r

    if missing:
        logger.warning(f"KY ソースにないレースがある開催日（{{日付: レース数}}、対象列は旧値のまま）: {missing}")
    rows = list(rows_by_key.values())
    logger.info(f"抽出行数: {len(rows)}")
    if args.dry_run or not rows:
        return 1 if missing else 0

    # 一時テーブルへロード（型は本テーブルのスキーマに合わせる）
    schema_by_name = {f.name: f for f in client.get_table(table).schema}
    client.load_table_from_json(
        rows,
        staging,
        job_config=bigquery.LoadJobConfig(
            schema=[schema_by_name[col] for col in (*KEY_COLUMNS, *TARGET_COLUMNS)],
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

    update_job = client.query(build_update_sql(table, staging))
    update_job.result()
    logger.info(f"UPDATE 行数: {update_job.num_dml_affected_rows}")

    verify = next(iter(client.query(build_verify_sql(table, backup)).result()))
    logger.info(
        f"突合（対象列以外）: 変化={verify.n_changed} / "
        f"現行のみ={verify.n_only_current} / バックアップのみ={verify.n_only_backup}"
    )
    logger.info(
        "対象列の変化行数（バックアップ比・累計）: "
        + ", ".join(f"{col}={verify[f'changed_{col}']}" for col in TARGET_COLUMNS)
    )
    client.delete_table(staging, not_found_ok=True)

    if verify.n_changed or verify.n_only_backup:
        logger.error("対象列以外に差分があります。バックアップからの復元を検討してください")
        return 1
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
