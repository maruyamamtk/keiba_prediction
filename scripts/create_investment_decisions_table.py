#!/usr/bin/env python3
"""
predictions.investment_decisions テーブル作成スクリプト

日次投資戦略策定の結果を保存するテーブルを作成する。
1馬券を1行で保存する形式（horse_numbers はカンマ区切り文字列）。

このスクリプトは初回セットアップ時または既存テーブルを再作成する際に実行してください。

Usage:
    python scripts/create_investment_decisions_table.py --project-id <PROJECT_ID>

    # 既存テーブルを削除して再作成する場合
    python scripts/create_investment_decisions_table.py --project-id <PROJECT_ID> --recreate

Issue #105: 投資戦略モジュールをバックテストパイプラインに統合
Issue #161: 全馬券種対応のため horse_numbers カラムへ変更
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
    dataset_id = "predictions"
    table_id = "investment_decisions"
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = "US"
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)
        logger.info(f"データセット作成: {dataset_id}")

    schema = load_schema("bq_schema_investment_decisions.json")
    table = bigquery.Table(full_table_id, schema=schema)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="race_date",
    )
    table.clustering_fields = ["race_id"]

    client.create_table(table, exists_ok=True)
    logger.info(f"テーブル作成完了: {full_table_id}")
    logger.info(f"  パーティション: race_date (DAY)")
    logger.info(f"  クラスタリング: race_id")


def main() -> None:
    parser = argparse.ArgumentParser(description="predictions.investment_decisions テーブル作成")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="既存テーブルを削除して再作成する（データが失われます）",
    )
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID 環境変数を設定してください")

    if args.recreate:
        client = bigquery.Client(project=args.project_id)
        full_table_id = f"{args.project_id}.predictions.investment_decisions"
        client.delete_table(full_table_id, not_found_ok=True)
        logger.info(f"既存テーブルを削除しました: {full_table_id}")

    create_table(args.project_id)


if __name__ == "__main__":
    main()
