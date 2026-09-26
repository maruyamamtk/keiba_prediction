#!/usr/bin/env python3
"""
SEC（成績データ）再取得スクリプト（Issue #440）

速報版（IDM未確定・行不足）のままロードされた開催日の SEC を JRDB から取り直し、
ローカル downloaded_files/Sec/ と GCS を確定版で上書きして raw.race_results に再ロードする。

ローカルの速報版ファイルを残したままにすると、全件ロード時に不完全なデータが再投入されるため、
ローカルファイルも必ず置き換える。

使用方法:
    # 検知のみ（全期間）
    .venv/bin/python scripts/refetch_sec.py --detect --start-date 2016-01-01 --dry-run

    # 検知した日を再取得
    .venv/bin/python scripts/refetch_sec.py --detect --start-date 2016-01-01

    # 日付を指定して再取得
    .venv/bin/python scripts/refetch_sec.py --dates 2026-01-24,2026-01-25

環境変数: GCP_PROJECT_ID, JRDB_USER, JRDB_PASSWORD（.env から読み込み）。JRDB_OUTPUT_DIR 設定時は --output-dir より優先
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

# プロジェクトルートをパスに追加
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.automation.data.jrdb_downloader import create_downloader_from_env  # noqa: E402
from src.automation.data.load_to_bq import create_loader_from_env  # noqa: E402
from src.automation.data.result_integrity import (  # noqa: E402
    find_incomplete_result_dates,
    refetch_sec_files,
)
from src.automation.data.upload_to_gcs import create_uploader_from_env  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SEC（成績データ）をJRDBから再取得して再ロードする")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--dates", help="再取得する開催日（YYYY-MM-DD のカンマ区切り）")
    target.add_argument("--detect", action="store_true", help="成績が不完全な開催日を自動検知する")
    parser.add_argument("--start-date", help="--detect の検査開始日（YYYY-MM-DD）")
    parser.add_argument(
        "--end-date", help="--detect の検査終了日（YYYY-MM-DD、省略時は8日前）"
    )
    parser.add_argument("--dry-run", action="store_true", help="対象日の表示のみ行う")
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "downloaded_files"),
        help="ダウンロード先（既存の速報版ファイルを上書きする）",
    )
    args = parser.parse_args(argv)
    if args.detect and not args.start_date:
        parser.error("--detect には --start-date が必要です")
    if args.dates and (args.start_date or args.end_date):
        parser.error("--start-date / --end-date は --detect と併用してください")
    try:
        args.start_date = date.fromisoformat(args.start_date) if args.start_date else None
        args.end_date = date.fromisoformat(args.end_date) if args.end_date else None
    except ValueError as e:
        parser.error(f"--start-date / --end-date の日付形式が不正です（YYYY-MM-DD）: {e}")
    if args.dates:
        try:
            args.dates = [date.fromisoformat(s.strip()) for s in args.dates.split(",") if s.strip()]
        except ValueError as e:
            parser.error(f"--dates の日付形式が不正です（YYYY-MM-DD）: {e}")
    return args


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args(argv)

    loader = create_loader_from_env()
    if loader is None:
        return 1

    if args.detect:
        end_date = args.end_date or date.today() - timedelta(days=8)
        incomplete = find_incomplete_result_dates(
            loader.bq_client, loader.project_id, args.start_date, end_date,
            dataset_id=loader.dataset_id,
        )
        for d in incomplete:
            logger.info(f"不完全: {d.to_dict()}")
        target_dates = [d.race_date for d in incomplete]
    else:
        target_dates = args.dates

    if not target_dates:
        logger.info("再取得対象の開催日はありません")
        return 0

    logger.info(f"再取得対象: {[d.isoformat() for d in target_dates]}")
    if args.dry_run:
        return 0

    downloader = create_downloader_from_env(default_output_dir=Path(args.output_dir))
    uploader = create_uploader_from_env()
    if downloader is None or uploader is None:
        return 1

    result = refetch_sec_files(
        downloader, uploader, loader, [d.strftime("%y%m%d") for d in target_dates]
    )
    logger.info(
        f"再ロード完了: {len(result.reloaded)}件 / 失敗: {result.failed} / "
        f"JRDBに公開なし: {result.unavailable} / {result.records}行"
    )

    # 再取得後の検証（対象期間を再検査）
    remaining = find_incomplete_result_dates(
        loader.bq_client, loader.project_id, min(target_dates), max(target_dates),
        dataset_id=loader.dataset_id,
    )
    target_set = set(target_dates)
    remaining = [d for d in remaining if d.race_date in target_set]
    for d in remaining:
        logger.warning(f"再取得後も不完全（JRDB側のデータの可能性）: {d.to_dict()}")

    return 1 if result.failed or remaining else 0


if __name__ == "__main__":
    sys.exit(main())
