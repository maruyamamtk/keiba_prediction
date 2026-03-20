#!/usr/bin/env python3
"""
Issue #167 対応: predictions.daily_odds_combo テーブルのデータ修正スクリプト

Issue #167 の修正前（旧コード）は COMBO_TICKET_TYPES マッピングが誤っており、
以下のデータが誤った ticket_type で保存されていた可能性がある。

旧マッピング（誤）          新マッピング（正）
  b5 → umatan (馬単)   →   b5 → wide     (ワイド)
  b6 → sanrenpuku(三連複)→  b6 → umatan   (馬単)
  b7 → wide   (ワイド)  →   b7 → sanrenpuku(三連複)

修正内容:
  1. ticket_type='wide' を削除
       旧コードで b7（三連複ページ）を 2 頭パターン(is_trio=False)でパースしようとしたが
       三連複 HTML は 6 桁 span のため空 DataFrame になりスキップされたはず。
       万一残っていた場合はデータが不正なので削除する。
  2. ticket_type='sanrenpuku' を削除
       旧コードで b6（馬単ページ）を 3 頭パターン(is_trio=True)でパースしようとしたが
       馬単 HTML は 4 桁 span のため空 DataFrame になりスキップされたはず。
       万一残っていた場合はデータが不正なので削除する。
  3. ticket_type='umatan' を 'wide' にリネーム
       旧コードで b5（ワイドページ）を 2 頭パターン(is_trio=False)でパースしており、
       データ構造（2 頭組み合わせ、horse_number_3=NULL）は正しい。
       ラベルだけ "umatan" になっていたため "wide" に修正する。

Usage:
    python3 scripts/fix_daily_odds_combo_issue167.py \\
        --project-id keiba-prediction-1768734113

    # 実行前に影響件数を確認するだけ
    python3 scripts/fix_daily_odds_combo_issue167.py \\
        --project-id keiba-prediction-1768734113 \\
        --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TABLE = "predictions.daily_odds_combo"


def count_by_ticket_type(client: bigquery.Client, project_id: str) -> dict[str, int]:
    """ticket_type 別の件数を返す"""
    query = f"""
    SELECT ticket_type, COUNT(*) AS cnt
    FROM `{project_id}.{TABLE}`
    GROUP BY ticket_type
    ORDER BY ticket_type
    """
    rows = list(client.query(query).result())
    return {row.ticket_type: row.cnt for row in rows}


def run_fix(project_id: str, dry_run: bool) -> None:
    client = bigquery.Client(project=project_id)

    logger.info("=== 修正前の件数 ===")
    before = count_by_ticket_type(client, project_id)
    for ticket_type, cnt in before.items():
        logger.info(f"  {ticket_type}: {cnt:,} 件")

    target_counts = {
        "wide": before.get("wide", 0),
        "sanrenpuku": before.get("sanrenpuku", 0),
        "umatan": before.get("umatan", 0),
    }
    logger.info(
        f"\n修正予定:\n"
        f"  DELETE ticket_type='wide'       : {target_counts['wide']:,} 件\n"
        f"  DELETE ticket_type='sanrenpuku' : {target_counts['sanrenpuku']:,} 件\n"
        f"  UPDATE ticket_type='umatan'→'wide': {target_counts['umatan']:,} 件"
    )

    if dry_run:
        logger.info("[DRY RUN] ここで終了します（--dry-run オプション）")
        return

    # Step 1: 誤った wide レコードを削除（b7 から誤パースされた可能性があるもの）
    if target_counts["wide"] > 0:
        logger.info(f"Step 1: ticket_type='wide' を削除 ({target_counts['wide']:,} 件)...")
        client.query(
            f"DELETE FROM `{project_id}.{TABLE}` WHERE ticket_type = 'wide'"
        ).result()
        logger.info("  → 削除完了")
    else:
        logger.info("Step 1: ticket_type='wide' のレコードなし → スキップ")

    # Step 2: 誤った sanrenpuku レコードを削除（b6 から誤パースされた可能性があるもの）
    if target_counts["sanrenpuku"] > 0:
        logger.info(f"Step 2: ticket_type='sanrenpuku' を削除 ({target_counts['sanrenpuku']:,} 件)...")
        client.query(
            f"DELETE FROM `{project_id}.{TABLE}` WHERE ticket_type = 'sanrenpuku'"
        ).result()
        logger.info("  → 削除完了")
    else:
        logger.info("Step 2: ticket_type='sanrenpuku' のレコードなし → スキップ")

    # Step 3: umatan → wide にリネーム（b5 から取得した正しいワイドデータ）
    if target_counts["umatan"] > 0:
        logger.info(f"Step 3: ticket_type='umatan' → 'wide' にリネーム ({target_counts['umatan']:,} 件)...")
        client.query(
            f"UPDATE `{project_id}.{TABLE}` SET ticket_type = 'wide' WHERE ticket_type = 'umatan'"
        ).result()
        logger.info("  → リネーム完了")
    else:
        logger.info("Step 3: ticket_type='umatan' のレコードなし → スキップ")

    logger.info("\n=== 修正後の件数 ===")
    after = count_by_ticket_type(client, project_id)
    for ticket_type, cnt in after.items():
        logger.info(f"  {ticket_type}: {cnt:,} 件")

    logger.info("修正完了")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Issue #167 に伴う daily_odds_combo テーブルのデータ修正",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--project-id", required=True, help="GCPプロジェクトID")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="影響件数の確認のみ行い、実際の修正は実行しない",
    )
    args = parser.parse_args()

    run_fix(args.project_id, args.dry_run)


if __name__ == "__main__":
    main()
