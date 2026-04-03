#!/usr/bin/env python3
"""
raw.race_info テーブルに start_time カラムを追加するスクリプト

Issue #214: BAAパーサーで読み取った発走時刻をBQに保存するための前提として、
raw.race_info テーブルに start_time (STRING, HHMM形式) カラムを追加する。

既存データは NULL になる。以降の BAA ファイルロード時に自動的に値が入る。

Usage:
    python scripts/add_start_time_to_race_info.py
    python scripts/add_start_time_to_race_info.py --project-id my-project-id
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")


def add_start_time_column(project_id: str) -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.raw.race_info"

    # 既にカラムが存在するか確認
    table = client.get_table(table_ref)
    existing_columns = {field.name for field in table.schema}

    if "start_time" in existing_columns:
        print(f"[SKIP] start_time カラムはすでに存在します: {table_ref}")
        return

    # ALTER TABLE でカラム追加
    sql = f"""
    ALTER TABLE `{table_ref}`
    ADD COLUMN IF NOT EXISTS start_time STRING
    """
    print(f"[INFO] start_time カラムを追加します: {table_ref}")
    client.query(sql).result()
    print(f"[OK] start_time カラムの追加が完了しました: {table_ref}")
    print("[INFO] 既存データの start_time は NULL です。")
    print("[INFO] 以降の BAA ファイルロード時に発走時刻 (HHMM形式) が自動的に保存されます。")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="raw.race_info テーブルに start_time カラムを追加する"
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

    add_start_time_column(args.project_id)


if __name__ == "__main__":
    main()
