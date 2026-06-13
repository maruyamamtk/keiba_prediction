#!/usr/bin/env python3
"""
features.entity_te_daily テーブルを作成するスクリプト

エンティティ（騎手・調教師・種牡馬・母馬・馬自身）の
Target Encoding 値を日次でキャッシュする中間テーブル。

Usage:
    .venv/bin/python scripts/create_entity_te_daily_table.py --project-id <PROJECT_ID>
"""

import argparse
import logging
import os

from dotenv import load_dotenv
from google.cloud import bigquery

logger = logging.getLogger(__name__)


SCHEMA = [
    bigquery.SchemaField("entity_type", "STRING", mode="REQUIRED",
                         description="エンティティ種別: jockey/trainer/sire/mare/horse"),
    bigquery.SchemaField("entity_id", "STRING", mode="REQUIRED",
                         description="エンティティID: 騎手コード/調教師コード/種牡馬名/母馬名/馬ID"),
    bigquery.SchemaField("condition_type", "STRING", mode="REQUIRED",
                         description="条件種別: base/course_type/venue/distance_band/distance/direction/cv/cd/cdv/jockey/season/distance_change/weight_carried_change/age_band"),
    bigquery.SchemaField("condition_key", "STRING", mode="REQUIRED",
                         description="条件値: '' (base), 'turf', '01', 'sprint', 'turf_01', 'turf_1200_01', '2yo', etc."),
    bigquery.SchemaField("as_of_date", "DATE", mode="REQUIRED",
                         description="この日付時点でのTE値（当日レースを除く）"),
    bigquery.SchemaField("cnt", "INT64", mode="REQUIRED",
                         description="条件に合致する出走数（低頻度マスク判定用）"),
    bigquery.SchemaField("sum_top3", "INT64", mode="REQUIRED",
                         description="条件に合致する3着以内回数"),
    bigquery.SchemaField("sum_top1", "INT64", mode="NULLABLE",
                         description="条件に合致する1着回数（horse+distance_band/distance のみ）"),
]


def create_table(project_id: str, dry_run: bool = False) -> None:
    table_ref = f"{project_id}.features.entity_te_daily"

    if dry_run:
        logger.info(f"[dry-run] テーブル作成: {table_ref}")
        return

    client = bigquery.Client(project=project_id)

    table = bigquery.Table(table_ref, schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="as_of_date",
    )
    table.clustering_fields = ["entity_type", "entity_id", "condition_type"]
    table.description = (
        "エンティティ別 Target Encoding 日次キャッシュテーブル。\n"
        "te_daily_query.sql で毎朝 7:45 JST に計算・追記される。\n"
        "entity_type × entity_id × condition_type × condition_key × as_of_date が一意キー。"
    )

    try:
        table = client.create_table(table)
        logger.info(f"テーブルを作成しました: {table_ref}")
    except Exception as e:
        if "Already Exists" in str(e):
            logger.info(f"テーブルはすでに存在します: {table_ref}")
        else:
            raise


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="features.entity_te_daily テーブルを作成する")
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCPプロジェクトID",
    )
    parser.add_argument("--dry-run", action="store_true", help="実際には作成しない")
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細ログ")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if not args.project_id:
        logger.error("GCP_PROJECT_ID が設定されていません")
        return 1

    create_table(args.project_id, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    exit(main())
