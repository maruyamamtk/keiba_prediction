#!/usr/bin/env python3
"""
raw.horse_master の horse_id 重複を解消するスクリプト（Issue #449）

同一 horse_id の行のうち、最新 data_date（同値なら最新 updated_at）の1行だけを残す。
特徴量SQLの JOIN（qualify row_number() ... order by data_date desc, updated_at desc）と同じ選び方。
テーブル定義（スキーマ・説明）を保つため、置き換えではなくトランザクション内の DELETE + INSERT で書き換える。

手順:
  1. raw.horse_master_backup_issue449 へバックアップ（既存なら上書きしない）
  2. 重複を解消して書き換え
  3. 検証: 行数 = ユニーク horse_id 数、重複のなかった馬の行はバックアップと完全一致、
     重複のあった馬は最新 data_date の行が残っていること（違反があれば終了コード1）

使用方法:
    .venv/bin/python scripts/dedupe_horse_master.py --dry-run   # 重複件数の確認のみ
    .venv/bin/python scripts/dedupe_horse_master.py

    # ロールバック
    bq cp -f <PROJECT_ID>:raw.horse_master_backup_issue449 <PROJECT_ID>:raw.horse_master

環境変数: GCP_PROJECT_ID（.env から読み込み）
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TARGET_TABLE = "horse_master"
BACKUP_TABLE = "horse_master_backup_issue449"
DEDUP_ORDER = "order by data_date desc, updated_at desc"


def build_rewrite_sql(table: str) -> str:
    """重複を除いた行で書き換えるトランザクション"""
    return f"""
        BEGIN TRANSACTION;
        CREATE TEMP TABLE dedup AS
          SELECT * FROM `{table}`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY horse_id {DEDUP_ORDER}) = 1;
        DELETE FROM `{table}` WHERE TRUE;
        INSERT INTO `{table}` SELECT * FROM dedup;
        COMMIT TRANSACTION;
    """


def build_verify_sql(table: str, backup: str) -> str:
    """書き換え後の検証（期待と異なる行数を数える）"""
    return f"""
        WITH cur AS (SELECT horse_id, TO_JSON_STRING(t) AS j FROM `{table}` t),
        expected AS (
          SELECT horse_id, TO_JSON_STRING(b) AS j FROM `{backup}` b
          QUALIFY ROW_NUMBER() OVER (PARTITION BY horse_id {DEDUP_ORDER}) = 1
        )
        SELECT
          (SELECT COUNT(*) FROM cur) AS n_rows,
          (SELECT COUNT(DISTINCT horse_id) FROM `{backup}`) AS n_expected,
          (SELECT COUNT(*) FROM (SELECT horse_id FROM cur GROUP BY 1 HAVING COUNT(*) > 1)) AS n_dup,
          (SELECT COUNT(*) FROM cur FULL JOIN expected USING (horse_id)
            WHERE cur.j IS DISTINCT FROM expected.j) AS n_mismatch
    """


def count_duplicates(client: bigquery.Client, table: str) -> tuple[int, int]:
    row = next(iter(client.query(
        f"SELECT COUNT(*) AS ids, COALESCE(SUM(c), 0) AS rows_ FROM "
        f"(SELECT horse_id, COUNT(*) AS c FROM `{table}` GROUP BY 1 HAVING c > 1)"
    ).result()))
    return row.ids, row.rows_


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    parser = argparse.ArgumentParser(description="raw.horse_master の horse_id 重複を解消する")
    parser.add_argument("--dry-run", action="store_true", help="重複件数の表示のみ行う")
    args = parser.parse_args(argv)

    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        logger.error("GCP_PROJECT_ID環境変数が設定されていません")
        return 1
    dataset = os.environ.get("BQ_DATASET_RAW", "raw")
    client = bigquery.Client(project=project_id)
    table = f"{project_id}.{dataset}.{TARGET_TABLE}"
    backup = f"{project_id}.{dataset}.{BACKUP_TABLE}"

    dup_ids, dup_rows = count_duplicates(client, table)
    logger.info(f"重複 horse_id: {dup_ids}件（{dup_rows}行）")
    if args.dry_run or dup_ids == 0:
        return 0

    try:
        client.get_table(backup)
        logger.info(f"バックアップ既存: {backup}")
    except NotFound:
        client.copy_table(table, backup).result()
        logger.info(f"バックアップ作成: {backup}")

    client.query(build_rewrite_sql(table)).result()
    logger.info("重複解消の書き換え完了")

    v = next(iter(client.query(build_verify_sql(table, backup)).result()))
    logger.info(
        f"検証: 行数={v.n_rows} / 期待={v.n_expected} / 残存重複={v.n_dup} / 不一致={v.n_mismatch}"
    )
    if v.n_rows != v.n_expected or v.n_dup or v.n_mismatch:
        logger.error("検証に失敗しました。バックアップからの復元を検討してください")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
