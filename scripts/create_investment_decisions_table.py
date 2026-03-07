#!/usr/bin/env python3
"""
predictions.investment_decisions テーブル作成スクリプト

日次投資戦略策定の結果を保存するテーブルを作成する。
このスクリプトは初回セットアップ時に一度だけ実行してください。

Usage:
    python scripts/create_investment_decisions_table.py --project-id <PROJECT_ID>

Issue #105: 投資戦略モジュールをバックテストパイプラインに統合
"""

from __future__ import annotations

import argparse
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

SCHEMA = [
    bigquery.SchemaField("race_id", "STRING", mode="REQUIRED", description="JRDB形式のレースID"),
    bigquery.SchemaField("race_date", "DATE", mode="REQUIRED", description="開催日（パーティションキー）"),
    bigquery.SchemaField("horse_id", "STRING", mode="NULLABLE", description="馬ID"),
    bigquery.SchemaField("horse_number", "INTEGER", mode="REQUIRED", description="馬番"),
    bigquery.SchemaField("horse_name", "STRING", mode="NULLABLE", description="馬名"),
    bigquery.SchemaField("venue_code", "STRING", mode="NULLABLE", description="競馬場コード"),
    bigquery.SchemaField("race_number", "INTEGER", mode="NULLABLE", description="レース番号"),
    bigquery.SchemaField(
        "race_pattern", "STRING", mode="NULLABLE",
        description="レースパターン (one_dominant / competitive / standard)",
    ),
    bigquery.SchemaField("bet_type", "STRING", mode="NULLABLE", description="馬券種 (place など)"),
    bigquery.SchemaField("bet_amount", "FLOAT64", mode="NULLABLE", description="賭け金（円）"),
    bigquery.SchemaField("win_place_prob", "FLOAT64", mode="NULLABLE", description="複勝予測確率（0〜1）"),
    bigquery.SchemaField("place_odds", "FLOAT64", mode="NULLABLE", description="複勝オッズ"),
    bigquery.SchemaField(
        "expected_return", "FLOAT64", mode="NULLABLE",
        description="期待回収率（win_place_prob × place_odds）",
    ),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="レコード作成日時（UTC）"),
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

    table = bigquery.Table(full_table_id, schema=SCHEMA)

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
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID 環境変数を設定してください")

    create_table(args.project_id)


if __name__ == "__main__":
    main()
