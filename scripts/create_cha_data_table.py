#!/usr/bin/env python3
"""
raw.cha_data テーブルを作成するスクリプト。
JRDBの調教本追切データ (CHA ファイル) を格納する。

Usage:
    python scripts/create_cha_data_table.py --project-id <PROJECT_ID>
"""
import argparse
import json
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

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


def load_schema(schema_file: str) -> list:
    schema_path = CONFIG_DIR / schema_file
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_json = json.load(f)
    return [
        bigquery.SchemaField(
            name=field["name"],
            field_type=field["type"],
            mode=field.get("mode", "NULLABLE"),
            description=field.get("description", ""),
        )
        for field in schema_json
    ]


def create_table(project_id: str) -> None:
    client = bigquery.Client(project=project_id)

    schema = load_schema("bq_schema_cha_data.json")

    table_ref = f"{project_id}.raw.cha_data"
    table = bigquery.Table(table_ref, schema=schema)

    # クラスタリング設定 (パーティションなし: training_date は欠損あり)
    table.clustering_fields = ["race_id"]

    table = client.create_table(table, exists_ok=True)
    logger.info(f"テーブルを作成しました: {table_ref}")


def main():
    parser = argparse.ArgumentParser(description="raw.cha_data テーブルを作成")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID を設定してください")

    create_table(args.project_id)


if __name__ == "__main__":
    main()
