#!/usr/bin/env python3
"""
predictions.purchase_history テーブル作成スクリプト

IPAT自動購入の購入履歴を保存するBigQueryテーブルを作成する。

Usage:
    python3 scripts/create_purchase_history_table.py
    python3 scripts/create_purchase_history_table.py --project-id <PROJECT_ID>

Issue #213: 発走5分前JRA IPAT自動馬券購入パイプラインの実装
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import Conflict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT_DIR / "config"

sys.path.insert(0, str(ROOT_DIR))


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


def create_purchase_history_table(project_id: str, dataset: str = "predictions") -> None:
    """
    predictions.purchase_history テーブルを作成する

    - パーティション: race_date（日付パーティション）
    - クラスタリング: race_id
    """
    client = bigquery.Client(project=project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset)

    # データセットが存在しなければ作成
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        logger.info(f"データセット {dataset} を作成します")
        client.create_dataset(bigquery.Dataset(dataset_ref))

    schema = load_schema("bq_schema_purchase_history.json")
    table_ref = dataset_ref.table("purchase_history")
    table = bigquery.Table(table_ref, schema=schema)

    # 日付パーティション設定
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="race_date",
    )

    # クラスタリング設定
    table.clustering_fields = ["race_id"]

    try:
        client.create_table(table)
        logger.info(f"テーブル作成完了: {project_id}.{dataset}.purchase_history")
    except Conflict:
        logger.info(f"テーブルは既に存在します: {project_id}.{dataset}.purchase_history")


def main() -> None:
    from dotenv import load_dotenv
    load_dotenv(ROOT_DIR / ".env")

    parser = argparse.ArgumentParser(
        description="predictions.purchase_history テーブルを作成する"
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCP プロジェクト ID (デフォルト: 環境変数 GCP_PROJECT_ID)",
    )
    args = parser.parse_args()

    if not args.project_id:
        print("[ERROR] --project-id を指定するか、環境変数 GCP_PROJECT_ID を設定してください。")
        sys.exit(1)

    create_purchase_history_table(args.project_id)


if __name__ == "__main__":
    main()
