#!/usr/bin/env python3
"""
raw.combo_odds テーブルを作成するスクリプト。
JRDBオッズデータ（OZ馬連・OW・OT）を格納する。

Usage:
    python scripts/create_raw_combo_odds_table.py --project-id <PROJECT_ID>
"""
import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_table(project_id: str) -> None:
    client = bigquery.Client(project=project_id)

    schema = [
        bigquery.SchemaField("race_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("race_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("bet_type", "STRING", mode="REQUIRED"),   # 'umaren' / 'wide' / 'sanrenpuku'
        bigquery.SchemaField("horse_number_1", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("horse_number_2", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("horse_number_3", "INTEGER", mode="NULLABLE"),  # 三連複のみ
        bigquery.SchemaField("odds_value", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("odds_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ]

    table_ref = f"{project_id}.raw.combo_odds"
    table = bigquery.Table(table_ref, schema=schema)

    # パーティション設定
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="race_date",
    )

    # クラスタリング設定
    table.clustering_fields = ["race_id", "bet_type"]

    table = client.create_table(table, exists_ok=True)
    logger.info(f"テーブルを作成しました: {table_ref}")


def main():
    parser = argparse.ArgumentParser(description="raw.combo_oddsテーブルを作成")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID を設定してください")

    create_table(args.project_id)


if __name__ == "__main__":
    main()
