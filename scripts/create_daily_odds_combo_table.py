#!/usr/bin/env python3
"""
predictions.daily_odds_combo テーブル作成スクリプト

馬連・馬単・ワイド・三連複の組み合わせ馬券オッズを保存するテーブルを作成する。
このスクリプトは初回セットアップ時に一度だけ実行してください。

Usage:
    python scripts/create_daily_odds_combo_table.py --project-id <PROJECT_ID>

Issue #134: netkeibaスクレイパー拡張 - 組み合わせ馬券オッズ取得
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
    bigquery.SchemaField(
        "ticket_type", "STRING", mode="REQUIRED",
        description="馬券種 (umaren / umatan / wide / sanrenpuku)",
    ),
    bigquery.SchemaField("horse_number_1", "INTEGER", mode="REQUIRED", description="馬番1（小さい方）"),
    bigquery.SchemaField("horse_number_2", "INTEGER", mode="REQUIRED", description="馬番2"),
    bigquery.SchemaField("horse_number_3", "INTEGER", mode="NULLABLE", description="馬番3（三連複のみ）"),
    bigquery.SchemaField("odds", "FLOAT64", mode="NULLABLE", description="オッズ"),
    bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="REQUIRED", description="スクレイプ日時（UTC）"),
]


def create_table(project_id: str) -> None:
    client = bigquery.Client(project=project_id)
    dataset_id = "predictions"
    table_id = "daily_odds_combo"
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # データセットが存在しない場合は作成
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = "US"
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)
        logger.info(f"データセット作成: {dataset_id}")

    table = bigquery.Table(full_table_id, schema=SCHEMA)

    # パーティション設定
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="race_date",
    )
    # クラスタリング: race_id + ticket_type で絞り込み高速化
    table.clustering_fields = ["race_id", "ticket_type"]

    table = client.create_table(table, exists_ok=True)
    logger.info(f"テーブル作成完了: {full_table_id}")
    logger.info(f"  パーティション: race_date (DAY)")
    logger.info(f"  クラスタリング: race_id, ticket_type")
    logger.info(f"  ticket_type の値: umaren / umatan / wide / sanrenpuku")


def main() -> None:
    parser = argparse.ArgumentParser(description="predictions.daily_odds_combo テーブル作成")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID 環境変数を設定してください")

    create_table(args.project_id)


if __name__ == "__main__":
    main()
