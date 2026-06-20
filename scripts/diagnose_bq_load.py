#!/usr/bin/env python3
"""
BQロード診断スクリプト

race_info以外のテーブルが更新されない問題を診断する。
以下を確認する:
1. BQのrawデータセットに存在するテーブル一覧
2. GCS上のデータタイプ別ファイル数
3. load_historyの成功/失敗/スキップ状況
4. 失敗ファイルのエラー内容

使用方法:
    python3 scripts/diagnose_bq_load.py
    python3 scripts/diagnose_bq_load.py --show-errors
    python3 scripts/diagnose_bq_load.py --table race_results
"""

import argparse
import logging
import os
import sys
from collections import defaultdict

from dotenv import load_dotenv

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def check_bq_tables(project_id: str, dataset_id: str = "raw"):
    """BQのrawデータセットのテーブル一覧を確認"""
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    tables = list(client.list_tables(f"{project_id}.{dataset_id}"))

    print("\n" + "=" * 60)
    print(f"BQ テーブル一覧 ({dataset_id}データセット)")
    print("=" * 60)

    expected_tables = [
        "race_info", "horse_results", "race_results", "horse_master",
        "horse_extended", "venue_info", "payouts", "odds", "load_history",
    ]

    existing = {t.table_id for t in tables}
    for table_name in expected_tables:
        status = "✅ 存在" if table_name in existing else "❌ 未作成"
        print(f"  {status}: {table_name}")

    extra = existing - set(expected_tables)
    if extra:
        print(f"\n  その他のテーブル: {', '.join(sorted(extra))}")

    return existing


def check_gcs_files(project_id: str, bucket_suffix: str = "keiba-raw-data"):
    """GCS上のデータタイプ別ファイル数を確認"""
    from google.cloud import storage
    import re

    bucket_name = f"{project_id}-{bucket_suffix}"
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    print("\n" + "=" * 60)
    print(f"GCS ファイル一覧 ({bucket_name})")
    print("=" * 60)

    # TABLE_MAPPINGのインポート
    from src.automation.data.load_to_bq import TABLE_MAPPING, extract_data_type

    data_type_counts = defaultdict(int)
    unknown_types = defaultdict(int)
    total = 0

    for blob in bucket.list_blobs():
        if not blob.name.lower().endswith(".csv"):
            continue
        total += 1
        data_type = extract_data_type(blob.name)
        if data_type:
            if data_type in TABLE_MAPPING:
                data_type_counts[data_type] += 1
            else:
                unknown_types[data_type] += 1

    print(f"\n  CSV合計ファイル数: {total}")
    print("\n  TABLE_MAPPING対象ファイル数 (データタイプ別):")

    for data_type in sorted(TABLE_MAPPING.keys()):
        count = data_type_counts.get(data_type, 0)
        table = TABLE_MAPPING[data_type]
        status = "✅" if count > 0 else "⚠️  0件"
        print(f"  {status} {data_type} → {table}: {count}ファイル")

    if unknown_types:
        print(f"\n  TABLE_MAPPINGにないデータタイプ:")
        for dt, cnt in sorted(unknown_types.items()):
            print(f"    {dt}: {cnt}ファイル")

    return data_type_counts


def check_load_history(project_id: str, dataset_id: str = "raw", show_errors: bool = False, target_table: str = None):
    """load_historyの状況を確認"""
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.load_history"

    print("\n" + "=" * 60)
    print("ロード履歴サマリー")
    print("=" * 60)

    # テーブル別のステータスサマリー
    query = f"""
    SELECT
        table_name,
        status,
        COUNT(*) AS file_count,
        MAX(loaded_at) AS last_loaded,
        SUM(records_count) AS total_records
    FROM `{table_ref}`
    {f"WHERE table_name = '{target_table}'" if target_table else ""}
    GROUP BY table_name, status
    ORDER BY table_name, status
    """

    try:
        results = list(client.query(query).result())
    except Exception as e:
        print(f"  ❌ load_historyテーブルの取得に失敗: {e}")
        return

    if not results:
        print("  ロード履歴が空です")
        return

    # テーブル別に整理
    table_stats = defaultdict(lambda: {"success": 0, "failed": 0, "skipped": 0, "last_loaded": None, "total_records": 0})
    for row in results:
        tname = row.table_name or "(None)"
        table_stats[tname][row.status] = row.file_count
        table_stats[tname]["total_records"] += row.total_records or 0
        if row.last_loaded:
            if table_stats[tname]["last_loaded"] is None or row.last_loaded > table_stats[tname]["last_loaded"]:
                table_stats[tname]["last_loaded"] = row.last_loaded

    print("\n  テーブル別ロード状況:")
    print(f"  {'テーブル名':<20} {'成功':>6} {'失敗':>6} {'スキップ':>8} {'最終更新':<20} {'総レコード':>10}")
    print("  " + "-" * 80)
    for table_name, stats in sorted(table_stats.items()):
        last = stats["last_loaded"].strftime("%Y-%m-%d %H:%M") if stats["last_loaded"] else "N/A"
        failed_mark = " ⚠️" if stats["failed"] > 0 else ""
        print(f"  {table_name:<20} {stats['success']:>6} {stats['failed']:>6}{failed_mark} {stats['skipped']:>8} {last:<20} {stats['total_records']:>10}")

    # 失敗ファイルの詳細
    if show_errors:
        print("\n" + "=" * 60)
        print("失敗ファイルの詳細 (最新20件)")
        print("=" * 60)

        error_query = f"""
        SELECT
            file_name,
            table_name,
            data_type,
            error_message,
            loaded_at,
            duration_seconds
        FROM `{table_ref}`
        WHERE status = 'failed'
        {f"AND table_name = '{target_table}'" if target_table else ""}
        ORDER BY loaded_at DESC
        LIMIT 20
        """
        try:
            error_results = list(client.query(error_query).result())
            if error_results:
                for row in error_results:
                    print(f"\n  ファイル: {row.file_name}")
                    print(f"  テーブル: {row.table_name}, データタイプ: {row.data_type}")
                    print(f"  エラー: {row.error_message}")
                    print(f"  日時: {row.loaded_at}")
            else:
                print("  失敗ファイルなし")
        except Exception as e:
            print(f"  エラー詳細の取得に失敗: {e}")

    # 直近7日間の更新状況
    print("\n" + "=" * 60)
    print("直近7日間の更新状況 (テーブル別)")
    print("=" * 60)

    recent_query = f"""
    SELECT
        table_name,
        DATE(loaded_at) AS load_date,
        COUNT(*) AS files,
        SUM(records_count) AS records,
        COUNTIF(status = 'failed') AS failed_count
    FROM `{table_ref}`
    WHERE loaded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    {f"AND table_name = '{target_table}'" if target_table else ""}
    GROUP BY table_name, load_date
    ORDER BY load_date DESC, table_name
    """
    try:
        recent_results = list(client.query(recent_query).result())
        if recent_results:
            for row in recent_results:
                failed_mark = f" (失敗{row.failed_count}件)" if row.failed_count > 0 else ""
                print(f"  {row.load_date} | {row.table_name:<20} | {row.files}ファイル | {row.records}レコード{failed_mark}")
        else:
            print("  直近7日間のロード履歴なし")
    except Exception as e:
        print(f"  直近履歴の取得に失敗: {e}")


def check_bq_table_row_counts(project_id: str, dataset_id: str = "raw", existing_tables: set = None):
    """各BQテーブルのレコード数を確認"""
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)

    target_tables = [
        "race_info", "horse_results", "race_results", "horse_master",
        "horse_extended", "venue_info", "payouts", "odds",
    ]
    if existing_tables:
        target_tables = [t for t in target_tables if t in existing_tables]

    print("\n" + "=" * 60)
    print("BQ テーブルレコード数")
    print("=" * 60)

    for table_name in target_tables:
        try:
            table = client.get_table(f"{project_id}.{dataset_id}.{table_name}")
            count = table.num_rows
            print(f"  {table_name:<25}: {count:>10,} 行")
        except Exception as e:
            print(f"  {table_name:<25}: ❌ エラー ({e})")


def main():
    parser = argparse.ArgumentParser(description="BQロード診断スクリプト")
    parser.add_argument(
        "--show-errors",
        action="store_true",
        help="失敗ファイルの詳細エラーメッセージを表示",
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="特定テーブルのみ診断 (例: race_results)",
    )
    parser.add_argument(
        "--project-id",
        type=str,
        default=None,
        help="GCPプロジェクトID (環境変数GCP_PROJECT_IDからも取得可能)",
    )
    args = parser.parse_args()

    load_dotenv()

    project_id = args.project_id or os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        print("ERROR: GCP_PROJECT_IDが設定されていません")
        print("  --project-id オプションか、GCP_PROJECT_ID環境変数で設定してください")
        sys.exit(1)

    print(f"\n診断対象: プロジェクト={project_id}")

    # 1. BQテーブル確認
    existing_tables = check_bq_tables(project_id)

    # 2. BQテーブルレコード数確認
    check_bq_table_row_counts(project_id, existing_tables=existing_tables)

    # 3. GCSファイル確認
    check_gcs_files(project_id)

    # 4. ロード履歴確認
    if "load_history" in existing_tables:
        check_load_history(
            project_id,
            show_errors=args.show_errors,
            target_table=args.table,
        )
    else:
        print("\n⚠️  load_historyテーブルが存在しません。")
        print("  src/manual/create_tables.py を実行してテーブルを作成してください。")

    print("\n" + "=" * 60)
    print("診断完了")
    print("=" * 60)
    print("\n失敗ファイルの詳細を確認するには --show-errors オプションを使用してください:")
    print("  python3 scripts/diagnose_bq_load.py --show-errors")
    print("\n特定テーブルの診断には --table オプションを使用してください:")
    print("  python3 scripts/diagnose_bq_load.py --table race_results --show-errors")


if __name__ == "__main__":
    main()
