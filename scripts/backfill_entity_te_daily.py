#!/usr/bin/env python3
"""
features.entity_te_daily 過去分バックフィルスクリプト

te_daily_query.sql を使って指定期間の全日付分を features.entity_te_daily に追記する。
本番運用前の初回セットアップや、テーブル再構築時に使用する。

Usage:
    # 全期間バックフィル（例: 2023-01-01 から today まで）
    .venv/bin/python scripts/backfill_entity_te_daily.py \
        --project-id <PROJECT_ID> \
        --start-date 2023-01-01 \
        --end-date 2026-06-13

    # dry-run（対象日付を確認するだけ）
    .venv/bin/python scripts/backfill_entity_te_daily.py \
        --project-id <PROJECT_ID> \
        --start-date 2024-01-01 \
        --end-date 2024-12-31 \
        --dry-run

注意:
    - 既に as_of_date が存在する場合は重複追記される（WRITE_APPEND）。
    - 再実行前に DELETE FROM features.entity_te_daily WHERE as_of_date BETWEEN ... で
      対象期間を削除してから実行すること。
    - BigQuery の書き込みコストが発生するため、初回のみ実行すること。
"""

import argparse
import datetime
import logging
import os
import time

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def _date_range(start: datetime.date, end: datetime.date):
    d = start
    while d <= end:
        yield d
        d += datetime.timedelta(days=1)


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="features.entity_te_daily をバックフィルする")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--start-date", required=True, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="対象日付を表示するだけ")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.project_id:
        logger.error("GCP_PROJECT_ID が未設定です")
        return 1

    start = datetime.date.fromisoformat(args.start_date)
    end = datetime.date.fromisoformat(args.end_date)
    dates = list(_date_range(start, end))

    logger.info(f"バックフィル対象: {args.start_date} 〜 {args.end_date} ({len(dates)}日)")

    if args.dry_run:
        for d in dates:
            print(d.isoformat())
        logger.info("[dry-run] 実行を終了します")
        return 0

    from src.ml.features.feature_pipeline import FeaturePipeline

    pipeline = FeaturePipeline(args.project_id)
    total_inserted = 0
    failed = []

    for i, d in enumerate(dates, 1):
        date_str = d.isoformat()
        try:
            logger.info(f"[{i}/{len(dates)}] {date_str} 計算中...")
            t0 = time.time()
            result = pipeline.run_te_daily(date_str)
            elapsed = time.time() - t0
            total_inserted += result["inserted_rows"]
            logger.info(f"  → {result['inserted_rows']} rows, {elapsed:.1f}s")
        except Exception as e:
            logger.error(f"  → エラー: {e}")
            failed.append(date_str)

    logger.info(f"\nバックフィル完了: {total_inserted} rows 挿入")
    if failed:
        logger.error(f"失敗日付: {failed}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
