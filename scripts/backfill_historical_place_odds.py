#!/usr/bin/env python3
"""
過去単複オッズ バックフィルスクリプト

netkeibaスクレイパー（Issue #131）稼働前の期間（2025-11-15〜2026-03-06）について、
predictions.daily_odds にデータが存在しない race_id を対象に
netkeibaから単複オッズを取得して保存する。

すでに predictions.daily_odds にデータが存在する race_id は自動的にスキップするため、
中断後の再実行でも安全に続きから再開できる。

Usage:
    # 対象期間のバックフィル（ドライラン）
    .venv/bin/python scripts/backfill_historical_place_odds.py \\
        --project-id keiba-prediction-1768734113 \\
        --start-date 2025-11-15 \\
        --end-date 2026-03-06 \\
        --dry-run

    # 実際に実行
    .venv/bin/python scripts/backfill_historical_place_odds.py \\
        --project-id keiba-prediction-1768734113 \\
        --start-date 2025-11-15 \\
        --end-date 2026-03-06
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.scrape_historical_odds import (
    fetch_already_scraped_race_ids,
    fetch_target_races,
    jrdb_to_netkeiba_race_id,
    scrape_win_place_odds_batch,
)
from src.automation.data.netkeiba_scraper import save_odds_to_bq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_missing_race_ids(
    project_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    predictions.daily_odds にデータが存在しない race_id を返す。

    raw.race_info の全 race_id を取得し、predictions.daily_odds に存在しない
    ものだけを返すことで、バックフィル対象を絞り込む。

    Args:
        project_id: GCP プロジェクト ID
        start_date: 取得開始日（YYYY-MM-DD）
        end_date: 取得終了日（YYYY-MM-DD）

    Returns:
        [{"race_id": str, "race_date": date}] のリスト
    """
    all_races = fetch_target_races(project_id, start_date, end_date)
    already_scraped = fetch_already_scraped_race_ids(project_id, "daily_odds")

    missing = [r for r in all_races if r["race_id"] not in already_scraped]
    logger.info(
        f"バックフィル対象: {len(missing):,}レース "
        f"（全{len(all_races):,}レースのうち既スクレイプ済み: {len(already_scraped):,}）"
    )
    return missing


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "predictions.daily_odds に未存在の race_id について "
            "netkeibaから単複オッズを取得してバックフィルする"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--project-id", required=True, help="GCPプロジェクトID")
    parser.add_argument(
        "--start-date",
        default="2025-11-15",
        help="バックフィル開始日（YYYY-MM-DD, デフォルト: 2025-11-15）",
    )
    parser.add_argument(
        "--end-date",
        default="2026-03-06",
        help="バックフィル終了日（YYYY-MM-DD, デフォルト: 2026-03-06）",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=2.0,
        help="ページ間スリープ秒数（デフォルト: 2.0、netkeibaへの負荷軽減のため変更不推奨）",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="BQへのバッチ保存単位のレース数（デフォルト: 50）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="対象レース数の確認のみ行い、スクレイプは実行しない",
    )
    args = parser.parse_args()

    missing_races = fetch_missing_race_ids(
        args.project_id,
        args.start_date,
        args.end_date,
    )

    if not missing_races:
        logger.info("バックフィル対象レースがありません（すべて取得済みです）")
        sys.exit(0)

    # netkeiba race_id を付与
    converted: list[dict] = []
    error_count = 0
    for race in missing_races:
        try:
            netkeiba_id = jrdb_to_netkeiba_race_id(race["race_id"])
            converted.append({**race, "netkeiba_race_id": netkeiba_id})
        except ValueError as e:
            logger.warning(f"race_id変換スキップ: {race['race_id']} - {e}")
            error_count += 1

    if error_count > 0:
        logger.warning(f"変換エラー: {error_count}件スキップ")

    logger.info(f"変換後のバックフィル対象: {len(converted):,}レース")

    est_hours = len(converted) * args.sleep_sec * 2 / 3600
    logger.info(f"推定所要時間: 約{est_hours:.1f}時間（sleep_sec={args.sleep_sec}）")

    if args.dry_run:
        logger.info("[DRY RUN] ここで終了します（--dry-run オプション）")
        sys.exit(0)

    total_saved = 0
    for batch_start in range(0, len(converted), args.batch_size):
        batch = converted[batch_start : batch_start + args.batch_size]
        batch_end = min(batch_start + args.batch_size, len(converted))
        logger.info(
            f"バッチ処理: {batch_start + 1}〜{batch_end} / {len(converted):,}レース "
            f"（{batch[0]['race_date']} ～ {batch[-1]['race_date']}）"
        )

        rows = scrape_win_place_odds_batch(batch, sleep_sec=args.sleep_sec)
        if not rows:
            logger.warning("  バッチ内でオッズが取得できませんでした")
            continue

        df = pd.DataFrame(rows)
        saved = save_odds_to_bq(df, args.project_id)
        total_saved += saved
        logger.info(f"  保存: {saved}行 / 累計: {total_saved:,}行")

    logger.info(f"バックフィル完了: 累計{total_saved:,}行保存")


if __name__ == "__main__":
    main()
