#!/usr/bin/env python3
"""
raw.odds テーブルの horse_id カラムを REQUIRED → NULLABLE に変更する

OZ（基準オッズ）ファイルには horse_id が含まれないため、
BigQuery テーブルのカラム制約を緩和する必要がある。

Usage:
    python3 scripts/alter_odds_horse_id_nullable.py
    python3 scripts/alter_odds_horse_id_nullable.py --project-id YOUR_PROJECT_ID
"""

import argparse
import os
import sys

from dotenv import load_dotenv
from google.cloud import bigquery


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="raw.odds の horse_id を NULLABLE に変更する"
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCP プロジェクト ID",
    )
    parser.add_argument(
        "--dataset",
        default="raw",
        help="データセット名 (デフォルト: raw)",
    )
    args = parser.parse_args()

    if not args.project_id:
        print("エラー: GCP_PROJECT_ID が設定されていません", file=sys.stderr)
        return 1

    client = bigquery.Client(project=args.project_id)
    table_ref = f"{args.project_id}.{args.dataset}.odds"

    # BigQuery は REQUIRED → NULLABLE の緩和を ALTER COLUMN ... DROP NOT NULL でサポート
    query = f"ALTER TABLE `{table_ref}` ALTER COLUMN horse_id DROP NOT NULL"

    print(f"実行: {query}")
    job = client.query(query)
    job.result()
    print(f"✓ {table_ref} の horse_id を NULLABLE に変更しました")
    return 0


if __name__ == "__main__":
    sys.exit(main())
