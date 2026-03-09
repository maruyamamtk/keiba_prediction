#!/usr/bin/env python3
"""
predictions.daily_odds テーブル作成スクリプト

netkeibaからスクレイピングしたリアルタイムオッズを保存するBigQueryテーブルを作成する。

Usage:
    python3 scripts/create_daily_odds_table.py --project-id <PROJECT_ID>

Issue #131: netkeibaリアルタイムオッズスクレイパーの実装
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import Conflict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
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


def create_daily_odds_table(project_id: str, dataset: str = "predictions") -> None:
    """
    predictions.daily_odds テーブルを作成する

    パーティション: race_date（日付パーティション）
    クラスタリング: race_id

    Args:
        project_id: GCPプロジェクトID
        dataset: データセット名（デフォルト: predictions）
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.daily_odds"

    schema = load_schema("bq_schema_daily_odds.json")

    table = bigquery.Table(table_id, schema=schema)

    # 日付パーティション（race_dateでパーティション）
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="race_date",
    )

    # クラスタリング（race_idで検索を高速化）
    table.clustering_fields = ["race_id"]

    table.description = (
        "netkeibaからスクレイピングしたリアルタイム単勝・複勝オッズ。"
        "race_id + horse_number をキーにUPSERTで更新される。"
        "Issue #131 で実装。"
    )

    try:
        table = client.create_table(table)
        logger.info(f"テーブルを作成しました: {table_id}")
        logger.info(f"  パーティション: race_date (DAY)")
        logger.info(f"  クラスタリング: race_id")
    except Conflict:
        logger.info(f"テーブルはすでに存在します: {table_id}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="predictions.daily_odds テーブルを作成する"
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="GCPプロジェクトID",
    )
    parser.add_argument(
        "--dataset",
        default="predictions",
        help="データセット名（デフォルト: predictions）",
    )
    args = parser.parse_args()

    try:
        create_daily_odds_table(args.project_id, args.dataset)
        logger.info("完了")
    except Exception as e:
        logger.error(f"テーブル作成に失敗しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
