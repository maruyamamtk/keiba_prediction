"""
IPAT 自動購入 エンドツーエンド動作確認スクリプト

実際の IPAT SP版に接続し、馬券購入（最低100円）が正常に完了することを確認する。
同一レースの複数馬券を一括購入する purchase_bets_for_race もテスト可能。

実行例（単一馬券）:
  IPAT_MEMBER_ID=xxx IPAT_PIN=xxx IPAT_PAT_NUMBER=xxx \
  .venv/bin/python scripts/test_ipat_e2e.py \
    --venue "福島(土)" --race 9 --bet-type place --horses 3 --amount 100

実行例（複数馬券一括購入）:
  IPAT_MEMBER_ID=xxx IPAT_PIN=xxx IPAT_PAT_NUMBER=xxx \
  .venv/bin/python scripts/test_ipat_e2e.py \
    --venue "中山(土)" --race 11 \
    --bets "place:3:100" "wide:2-10:100"

--bets フォーマット: "BET_TYPE:HORSES:AMOUNT"
  BET_TYPE: place / win / wide / umaren / umatan / sanrenpuku
  HORSES:   馬番をハイフンまたはカンマ区切り (例: 3 / 2-10 / 1,5,9)
  AMOUNT:   購入金額（100円単位）
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.automation.data.ipat_purchaser import IpatPurchaser, IpatLoginError, IpatPurchaseError


def _parse_bet_spec(spec: str) -> dict:
    """
    "BET_TYPE:HORSES:AMOUNT" 形式の文字列を馬券辞書に変換する。

    例:
      "place:3:100"    → {"bet_type": "place", "horse_numbers": [3], "amount": 100}
      "wide:2-10:200"  → {"bet_type": "wide",  "horse_numbers": [2, 10], "amount": 200}
      "sanrenpuku:1,5,9:100" → {"bet_type": "sanrenpuku", "horse_numbers": [1,5,9], "amount": 100}
    """
    parts = spec.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            f"--bets の形式が不正です: '{spec}' (正しい形式: BET_TYPE:HORSES:AMOUNT)"
        )
    bet_type, horses_str, amount_str = parts
    horse_numbers = [int(h) for h in horses_str.replace("-", ",").split(",") if h]
    amount = int(amount_str)
    return {"bet_type": bet_type, "horse_numbers": horse_numbers, "amount": amount}


async def run_test(venue: str, race: int, bets: list[dict], dry_run: bool):
    member_id = os.environ["IPAT_MEMBER_ID"]
    pin = os.environ["IPAT_PIN"]
    pat_number = os.environ["IPAT_PAT_NUMBER"]

    print(f"=== IPAT E2E テスト ===")
    print(f"会場: {venue} / {race}R / {len(bets)}件")
    for i, bet in enumerate(bets, 1):
        horse_str = "-".join(str(h) for h in bet["horse_numbers"])
        print(f"  [{i}] {bet['bet_type']} 馬番:{horse_str} {bet['amount']}円")
    total = sum(b["amount"] for b in bets)
    print(f"  合計: {total}円")
    print(f"dry_run: {dry_run}")
    print()

    async with IpatPurchaser(member_id, pin, pat_number) as purchaser:
        print("1. ログイン中...")
        ok = await purchaser.login()
        if not ok:
            print("❌ ログイン失敗")
            return False
        print(f"✅ ログイン成功 (URL: {purchaser._page.url})")

        if dry_run:
            print("\n[dry_run=True] 購入はスキップします")
            return True

        print(f"\n2. 馬券一括購入: {venue} {race}R ({len(bets)}件 合計{total}円)")
        result = await purchaser.purchase_bets_for_race(
            bets=bets,
            venue_name=venue,
            race_number=race,
        )
        print(f"   結果: {result}")

        if result["status"] == "success":
            print("✅ 購入成功")
            return True
        else:
            print(f"❌ 購入失敗: {result.get('error_message')}")
            return False


def main():
    parser = argparse.ArgumentParser(description="IPAT E2Eテスト")
    parser.add_argument("--venue", default="福島(土)", help="競馬場名 (例: 福島(土))")
    parser.add_argument("--race", type=int, default=9, help="レース番号")
    parser.add_argument("--dry-run", action="store_true", help="ログインのみ (購入しない)")

    # 複数馬券一括指定
    parser.add_argument(
        "--bets",
        nargs="+",
        metavar="BET_TYPE:HORSES:AMOUNT",
        help="馬券指定 (例: place:3:100 wide:2-10:100)",
    )
    # 単一馬券（後方互換）
    parser.add_argument("--bet-type", default="place", help="馬券種 (--bets 未使用時)")
    parser.add_argument("--horses", nargs="+", type=int, default=[3], help="馬番 (--bets 未使用時)")
    parser.add_argument("--amount", type=int, default=100, help="購入金額 (--bets 未使用時)")

    args = parser.parse_args()

    if args.bets:
        bets = [_parse_bet_spec(spec) for spec in args.bets]
    else:
        bets = [{"bet_type": args.bet_type, "horse_numbers": args.horses, "amount": args.amount}]

    ok = asyncio.run(run_test(
        venue=args.venue,
        race=args.race,
        bets=bets,
        dry_run=args.dry_run,
    ))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
