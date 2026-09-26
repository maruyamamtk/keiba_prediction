#!/usr/bin/env python3
"""
raw.horse_results の blinker バックフィルスクリプト（Issue #441）

パーサーがブリンカーを誤った位置（文字位置131）から読んでいたため、既存行の blinker は NULL か '-' しかない。
KYF ファイルを再パースしてブリンカーだけを一時テーブルに積み、**blinker の列だけを UPDATE** する。
通常の再ロード（MERGE UPSERT）は全列を上書きし、raw.load_history の成功済みファイルはスキップされるため使わない。

手順:
  1. 対象期間の開催日・レースを raw.race_info（horse_results に存在するレース）から取得し、
     各日の KYF/KYG/KYH を GCS（優先）→ ローカルの順に、対象レースが揃うまで読む
     （日によって KYG から取り込まれている・ローカル KYF がメインレースのみの部分ファイル、があるため
      ファイルの有無ではなく中身のレース網羅で判定し、揃わなかったレースは警告して終了コード1）
  2. (race_id, horse_number, blinker) を一時テーブルへロード（WRITE_TRUNCATE）
  3. 初回のみ raw.horse_results をバックアップテーブルへコピー
  4. blinker のみ UPDATE
  5. バックアップと突合し、blinker 以外の列に差分がないことを検証（差分があれば終了コード1）

使用方法:
    # 対象と KYF ソースの有無を確認のみ
    .venv/bin/python scripts/backfill_blinker.py --start-date 2025-01-01 --end-date 2025-01-31 --dry-run

    # 1ヶ月分で実行 → 検証、問題なければ全期間
    .venv/bin/python scripts/backfill_blinker.py --start-date 2025-01-01 --end-date 2025-01-31
    .venv/bin/python scripts/backfill_blinker.py --start-date 2016-01-01 --end-date 2026-12-31

    # ロールバック（テーブルごと復元する）
    bq cp -f <PROJECT_ID>:raw.horse_results_backup_issue441 <PROJECT_ID>:raw.horse_results

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

TARGET_TABLE = "horse_results"
STAGING_TABLE = "_blinker_backfill_issue441"
BACKUP_TABLE = "horse_results_backup_issue441"
# (GCS/ローカルのフォルダ, データタイプ)。KYF/KYG/KYH はいずれも parse_kyf_line で同じ位置にブリンカーを持つ
KY_SOURCES = [("Kyf", "KYF"), ("Jrdb", "KYG"), ("Jrdb", "KYH")]
DEFAULT_LOCAL_DIR = PROJECT_ROOT / "downloaded_files"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="raw.horse_results の blinker をバックフィルする")
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


def extract_blinker_rows(content: str, data_type: str, race_ids: set[str] | None = None) -> list[dict]:
    """KY ファイル内容から (race_id, horse_number, blinker) を抽出する（race_ids 指定時はそのレースのみ）"""
    rows = []
    for record in JRDBParser.parse_file(content, data_type):
        if race_ids is not None and record["race_id"] not in race_ids:
            continue
        rows.append(
            {
                "race_id": record["race_id"],
                "horse_number": record["horse_number"],
                "blinker": record["blinker"],
            }
        )
    return rows


def iter_ky_contents(bucket, local_dir: Path, yymmdd: str):
    """開催日の KYF/KYG/KYH 内容を GCS（優先）→ ローカルの順に yield する。要素は (内容, データタイプ, ソース表記)"""
    for folder, data_type in KY_SOURCES:
        blob = bucket.blob(f"{folder}/{data_type}{yymmdd}.csv")
        if blob.exists():
            yield blob.download_as_bytes().decode("utf-8"), data_type, f"gs://{bucket.name}/{blob.name}"
        local_path = local_dir / folder / f"{data_type}{yymmdd}.csv"
        if local_path.exists():
            yield local_path.read_text(encoding="utf-8"), data_type, str(local_path)


def collect_day_rows(sources, race_ids: set[str]) -> tuple[list[dict], set[str]]:
    """対象レースが揃うまでソースを順に読み、(抽出行, どのソースにもなかったレース) を返す"""
    rows: list[dict] = []
    remaining = set(race_ids)
    for content, data_type, source in sources:
        found = extract_blinker_rows(content, data_type, remaining)
        logger.debug(f"{source}: {len(found)}行")
        rows.extend(found)
        remaining -= {r["race_id"] for r in found}
        if not remaining:
            break
    return rows, remaining


def build_update_sql(table: str, staging: str) -> str:
    """blinker の列だけを更新する UPDATE 文"""
    return f"""
        UPDATE `{table}` t
        SET blinker = s.blinker
        FROM `{staging}` s
        WHERE t.race_id = s.race_id AND t.horse_number = s.horse_number
    """


def build_verify_sql(table: str, backup: str) -> str:
    """blinker 以外の列がバックアップと一致しない行数などを数える SELECT 文"""
    return f"""
        WITH cur AS (
          SELECT race_id, horse_number, TO_JSON_STRING(x) AS row_json
          FROM (SELECT * EXCEPT(blinker) FROM `{table}`) x
        ),
        bak AS (
          SELECT race_id, horse_number, TO_JSON_STRING(x) AS row_json
          FROM (SELECT * EXCEPT(blinker) FROM `{backup}`) x
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
        logger.warning(f"KY ソースにないレースがある開催日（{{日付: レース数}}、blinker は旧値のまま）: {missing}")
    rows = list(rows_by_key.values())
    n_blinker = sum(r["blinker"] is not None for r in rows)
    logger.info(f"抽出行数: {len(rows)}（ブリンカー装着 {n_blinker}行）")
    if args.dry_run or not rows:
        return 1 if missing else 0

    # 一時テーブルへロード
    client.load_table_from_json(
        rows,
        staging,
        job_config=bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("race_id", "STRING"),
                bigquery.SchemaField("horse_number", "INTEGER"),
                bigquery.SchemaField("blinker", "STRING"),
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

    update_job = client.query(build_update_sql(table, staging))
    update_job.result()
    logger.info(f"UPDATE 行数: {update_job.num_dml_affected_rows}")

    verify = next(iter(client.query(build_verify_sql(table, backup)).result()))
    logger.info(
        f"突合（blinker 以外の列）: 変化={verify.n_changed} / "
        f"現行のみ={verify.n_only_current} / バックアップのみ={verify.n_only_backup}"
    )
    client.delete_table(staging, not_found_ok=True)

    if verify.n_changed or verify.n_only_backup:
        logger.error("blinker 以外の列に差分があります。バックアップからの復元を検討してください")
        return 1
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
