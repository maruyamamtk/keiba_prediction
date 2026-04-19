"""
IPAT 自動購入 エンドツーエンド動作確認スクリプト

実際の IPAT SP版に接続し、1件の馬券購入（最低100円）が
正常に完了することを確認する。

実行:
  IPAT_MEMBER_ID=xxx IPAT_PIN=xxx IPAT_PAT_NUMBER=xxx \
  .venv/bin/python scripts/test_ipat_e2e.py \
    --venue "福島(土)" --race 9 --bet-type place --horse 3 --amount 100
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.automation.data.ipat_purchaser import IpatPurchaser, IpatLoginError, IpatPurchaseError


async def run_test(venue: str, race: int, bet_type: str, horses: list[int], amount: int, dry_run: bool):
    member_id = os.environ["IPAT_MEMBER_ID"]
    pin = os.environ["IPAT_PIN"]
    pat_number = os.environ["IPAT_PAT_NUMBER"]

    print(f"=== IPAT E2E テスト ===")
    print(f"会場: {venue} / {race}R / {bet_type} / 馬番:{horses} / {amount}円")
    print(f"dry_run: {dry_run}")
    print()

    async with IpatPurchaser(member_id, pin, pat_number) as purchaser:
        # ---- ログイン ----
        print("1. ログイン中...")
        ok = await purchaser.login()
        if not ok:
            print("❌ ログイン失敗")
            return False
        print(f"✅ ログイン成功 (URL: {purchaser._page.url})")

        if dry_run:
            print("\n[dry_run=True] 購入はスキップします")
            return True

        # ---- 購入 ----
        print(f"\n2. 馬券購入: {venue} {race}R {bet_type} {horses} {amount}円")
        result = await purchaser.purchase_bet(
            bet_type=bet_type,
            horse_numbers=horses,
            amount=amount,
            venue_name=venue,
            race_number=race,
        )
        print(f"   結果: {result}")

        if result["status"] == "success":
            print("✅ 購入成功")
            return True
        else:
            print(f"❌ 購入失敗: {result['error_message']}")
            return False


def main():
    parser = argparse.ArgumentParser(description="IPAT E2Eテスト")
    parser.add_argument("--venue", default="福島(土)", help="競馬場名 (例: 福島(土))")
    parser.add_argument("--race", type=int, default=9, help="レース番号")
    parser.add_argument("--bet-type", default="place", help="馬券種 (place/win/wide/umaren/sanrenpuku)")
    parser.add_argument("--horses", nargs="+", type=int, default=[3], help="馬番 (複数可)")
    parser.add_argument("--amount", type=int, default=100, help="購入金額 (100円単位)")
    parser.add_argument("--dry-run", action="store_true", help="ログインのみ (購入しない)")
    args = parser.parse_args()

    ok = asyncio.run(run_test(
        venue=args.venue,
        race=args.race,
        bet_type=args.bet_type,
        horses=args.horses,
        amount=args.amount,
        dry_run=args.dry_run,
    ))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
