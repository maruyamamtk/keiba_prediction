#!/usr/bin/env python3
"""
predictions.daily_predictions テーブルを作成するスクリプト

Issue #117: 予測結果の保存先テーブルを作成する

Usage:
    python scripts/create_predictions_table.py
    python scripts/create_predictions_table.py --project-id my-project-id
"""

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound

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


def create_predictions_table(project_id: str) -> None:
    """
    predictions.daily_predictions テーブルを作成する

    Args:
        project_id: GCPプロジェクトID
    """
    client = bigquery.Client(project=project_id)
    dataset_id = "predictions"
    table_id = "daily_predictions"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # データセットが存在しない場合は作成する
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"✓ データセット {dataset_ref} は既に存在します")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-northeast1"
        dataset.description = "予測結果データ"
        client.create_dataset(dataset, timeout=30)
        print(f"✓ データセット {dataset_ref} を作成しました")

    # テーブルが既に存在するか確認
    try:
        client.get_table(table_ref)
        print(f"✓ テーブル {table_ref} は既に存在します。スキップします。")
        return
    except NotFound:
        pass

    schema = load_schema("bq_schema_daily_predictions.json")

    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="race_date",
    )
    table.clustering_fields = ["venue_code", "race_number"]
    table.description = "日次予測結果テーブル（Issue #117）"

    try:
        client.create_table(table)
        print(f"✓ テーブル {table_ref} を作成しました")
        print(f"  パーティション: race_date (DAY)")
        print(f"  クラスタリング: venue_code, race_number")
    except Conflict:
        print(f"✓ テーブル {table_ref} は既に存在します（同時作成の競合）")


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="predictions.daily_predictions テーブルを作成する"
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCPプロジェクトID（省略時は環境変数 GCP_PROJECT_ID を使用）",
    )
    args = parser.parse_args()

    if not args.project_id:
        print("エラー: GCP_PROJECT_IDが指定されていません。--project-id オプションまたは環境変数を設定してください。")
        return 1

    print(f"プロジェクト: {args.project_id}")
    print("predictions.daily_predictions テーブルを作成します...")
    create_predictions_table(args.project_id)
    print("\n完了しました。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
