#!/usr/bin/env python3
"""
過去レース一括オッズ取得スクリプト

netkeibaから指定期間の全レースの単複・組み合わせ馬券オッズを取得して
BigQueryに保存する。途中で停止しても再実行で続きから再開できる。

JRDB race_id（8文字）と netkeiba race_id（12桁）の変換:
  JRDB: {venue(2)}{year_2d(2)}{kai_hex(1)}{day_hex(1)}{race(2)}
  netkeiba: {YYYY(4)}{venue(2)}{kai_0pad(2)}{day_0pad(2)}{race_0pad(2)}
  例: "0516121" → "201605010201"

Usage:
    # 単複オッズのみ
    python3 scripts/scrape_historical_odds.py \\
        --project-id keiba-prediction-1768734113 \\
        --start-date 2016-01-01 \\
        --mode win_place

    # 組み合わせ馬券オッズのみ（馬連・ワイド・三連複）
    python3 scripts/scrape_historical_odds.py \\
        --project-id keiba-prediction-1768734113 \\
        --start-date 2016-01-01 \\
        --mode combo \\
        --ticket-types b4 b7 b6

    # 全種（単複＋組み合わせ）
    python3 scripts/scrape_historical_odds.py \\
        --project-id keiba-prediction-1768734113 \\
        --start-date 2016-01-01 \\
        --mode all

    # ドライラン（スクレイプ件数の確認のみ）
    python3 scripts/scrape_historical_odds.py \\
        --project-id keiba-prediction-1768734113 \\
        --start-date 2016-01-01 \\
        --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from google.cloud import bigquery

# プロジェクトルートをPATHに追加
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.automation.data.netkeiba_scraper import (
    COMBO_TICKET_TYPES,
    _parse_combo_odds_html,
    _parse_win_place_odds_html,
    save_combo_odds_to_bq,
    save_odds_to_bq,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

NETKEIBA_BASE_URL = "https://race.netkeiba.com"


# ---------------------------------------------------------------------------
# race_id 変換
# ---------------------------------------------------------------------------

def jrdb_to_netkeiba_race_id(jrdb_race_id: str) -> str:
    """
    JRDB race_id（8文字）を netkeiba race_id（12桁）に変換する

    JRDB形式: {venue(2)}{year_2d(2)}{kai_hex(1)}{day_hex(1)}{race(2)}
    netkeiba形式: {YYYY(4)}{venue(2)}{kai_0pad(2)}{day_0pad(2)}{race_0pad(2)}

    Args:
        jrdb_race_id: 8文字のJRDB race_id（例: "05161201"）

    Returns:
        12桁のnetkeiba race_id（例: "201605010201"）

    Raises:
        ValueError: フォーマットが不正な場合
    """
    if len(jrdb_race_id) != 8:
        raise ValueError(f"JRDB race_idは8文字である必要があります: '{jrdb_race_id}'")

    venue = jrdb_race_id[0:2]
    year_2d = int(jrdb_race_id[2:4])
    kai = int(jrdb_race_id[4:5], 16)   # 16進数（10回以上に対応）
    day = int(jrdb_race_id[5:6], 16)   # 16進数（10日以上に対応）
    race_num = int(jrdb_race_id[6:8])

    year = year_2d + 2000 if year_2d <= 50 else year_2d + 1900

    return f"{year:04d}{venue}{kai:02d}{day:02d}{race_num:02d}"


# ---------------------------------------------------------------------------
# BQからの対象レース取得
# ---------------------------------------------------------------------------

def fetch_target_races(
    project_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    raw.race_info から指定期間の全レースを取得する

    Returns:
        [{"race_id": str, "race_date": date}] のリスト（race_date昇順）
    """
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT
        race_id,
        race_date
    FROM `{project_id}.raw.race_info`
    WHERE race_date >= '{start_date}'
      AND race_date <= '{end_date}'
    GROUP BY race_id, race_date
    ORDER BY race_date ASC, race_id ASC
    """
    logger.info(f"対象レースを取得中: {start_date} ～ {end_date}")
    rows = list(client.query(query).result())
    result = [{"race_id": row.race_id, "race_date": row.race_date} for row in rows]
    logger.info(f"対象レース数: {len(result):,}レース")
    return result


def fetch_already_scraped_race_ids(
    project_id: str,
    table: str,
) -> set[str]:
    """
    既にスクレイプ済みのrace_idをBQから取得する（単複オッズのスキップ用）

    Args:
        project_id: GCPプロジェクトID
        table: テーブル名（"daily_odds"）

    Returns:
        スクレイプ済み race_id の set
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.predictions.{table}"

    try:
        client.get_table(table_ref)
    except Exception:
        logger.info(f"テーブルがまだ存在しません（0件スキップ）: {table_ref}")
        return set()

    query = f"SELECT DISTINCT race_id FROM `{table_ref}`"
    try:
        rows = list(client.query(query).result())
        scraped = {row.race_id for row in rows}
        logger.info(f"既スクレイプ済み: {len(scraped):,}レース（{table}）")
        return scraped
    except Exception as e:
        logger.warning(f"スキップリスト取得失敗（スキップなしで続行）: {e}")
        return set()


def fetch_already_scraped_combo_pairs(project_id: str) -> set[tuple[str, str]]:
    """
    既にスクレイプ済みの (race_id, ticket_type) ペアをBQから取得する（コンボオッズのスキップ用）

    race_id 単位ではなく (race_id, ticket_type) 単位でスキップ判定することで、
    特定の券種だけ再取得する場合（Issue #167 修正後の wide/sanrenpuku 再取得など）に
    既取得済みの umaren 等を無駄に再スクレイプしない。

    Args:
        project_id: GCPプロジェクトID

    Returns:
        スクレイプ済み (race_id, ticket_type) ペアの set
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.predictions.daily_odds_combo"

    try:
        client.get_table(table_ref)
    except Exception:
        logger.info(f"テーブルがまだ存在しません（0件スキップ）: {table_ref}")
        return set()

    query = f"SELECT DISTINCT race_id, ticket_type FROM `{table_ref}`"
    try:
        rows = list(client.query(query).result())
        pairs = {(row.race_id, row.ticket_type) for row in rows}
        logger.info(f"既スクレイプ済みコンボペア: {len(pairs):,}件（daily_odds_combo）")
        return pairs
    except Exception as e:
        logger.warning(f"スキップリスト取得失敗（スキップなしで続行）: {e}")
        return set()


# ---------------------------------------------------------------------------
# Playwrightブラウザを使ったオッズ取得（ブラウザ再利用）
# ---------------------------------------------------------------------------

def _fetch_html_with_page(page, url: str, wait_selector: Optional[str], sleep_sec: float) -> str:
    """
    既存のPlaywrightページオブジェクトを使ってHTMLを取得する

    Args:
        page: Playwright page オブジェクト
        url: 取得対象URL
        wait_selector: 待機するCSSセレクタ（Noneの場合はスキップ）
        sleep_sec: ページロード後の追加待機秒数

    Returns:
        ページのHTML文字列（取得失敗時は空文字列）
    """
    try:
        page.goto(url, wait_until="load", timeout=60_000)
        if wait_selector:
            try:
                page.wait_for_selector(wait_selector, timeout=10_000)
            except Exception:
                pass  # タイムアウトしても続行
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        return page.content()
    except Exception as e:
        logger.warning(f"HTMLの取得失敗: {url} - {e}")
        return ""


def scrape_win_place_odds_batch(
    races: list[dict],
    sleep_sec: float = 1.5,
) -> list[dict]:
    """
    複数レースの単複オッズをまとめて取得する（ブラウザを1回のみ起動）

    Args:
        races: [{"race_id": str, "race_date": date, "netkeiba_race_id": str}] のリスト
        sleep_sec: ページ間のスリープ秒数

    Returns:
        [{"race_id", "race_date", "horse_number", "win_odds", "place_odds_min", "place_odds_max", "scraped_at"}]
        のリスト（取得できたレース・馬のみ）
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "playwright が未インストールです。"
            "`pip install playwright && playwright install chromium` を実行してください。"
        ) from e

    scraped_at = datetime.datetime.now(datetime.timezone.utc)
    all_rows: list[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            for i, race in enumerate(races):
                netkeiba_id = race["netkeiba_race_id"]
                url = f"{NETKEIBA_BASE_URL}/odds/index.html?race_id={netkeiba_id}&type=b1"

                if i > 0:
                    time.sleep(sleep_sec)

                html = _fetch_html_with_page(
                    page, url,
                    wait_selector="#odds_tan_block, #odds_fuku_block",
                    sleep_sec=sleep_sec,
                )

                if not html:
                    continue

                df = _parse_win_place_odds_html(html)
                if df.empty:
                    logger.debug(f"  単複オッズ取得なし: {netkeiba_id}")
                    continue

                for _, row in df.iterrows():
                    all_rows.append({
                        "race_id": race["race_id"],
                        "race_date": race["race_date"],
                        "horse_number": int(row["horse_number"]),
                        "win_odds": row.get("win_odds"),
                        "place_odds_min": row.get("place_odds_min"),
                        "place_odds_max": row.get("place_odds_max"),
                        "scraped_at": scraped_at,
                    })

                logger.debug(f"  [{i+1}/{len(races)}] {netkeiba_id}: {len(df)}頭")
        finally:
            browser.close()

    return all_rows


def scrape_combo_odds_batch(
    races: list[dict],
    ticket_types: list[str],
    sleep_sec: float = 1.5,
    already_scraped_pairs: Optional[set[tuple[str, str]]] = None,
) -> list[tuple[str, datetime.date, pd.DataFrame]]:
    """
    複数レースの組み合わせ馬券オッズをまとめて取得する（ブラウザ1回起動）

    Args:
        races: [{"race_id", "race_date", "netkeiba_race_id"}] のリスト
        ticket_types: 取得する馬券種（例: ["b4", "b5", "b7"]）
        sleep_sec: ページ間スリープ秒数
        already_scraped_pairs: スキップ対象の (race_id, ticket_type) ペア集合。
                               指定すると既取得済みの券種はネットワークアクセスをスキップする。

    Returns:
        [(race_id, race_date, combo_df)] のリスト（取得できたレースのみ）
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "playwright が未インストールです。"
            "`pip install playwright && playwright install chromium` を実行してください。"
        ) from e

    results: list[tuple[str, datetime.date, pd.DataFrame]] = []
    first_request = True  # スリープ制御用

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()

            for i, race in enumerate(races):
                netkeiba_id = race["netkeiba_race_id"]
                race_id = race["race_id"]
                all_dfs: list[pd.DataFrame] = []

                for ttype in ticket_types:
                    if ttype not in COMBO_TICKET_TYPES:
                        continue

                    # 既取得済みの (race_id, ticket_type) はスキップ
                    ticket_name = COMBO_TICKET_TYPES[ttype]
                    if already_scraped_pairs and (race_id, ticket_name) in already_scraped_pairs:
                        logger.debug(f"    スキップ（既取得済み）: {race_id} / {ticket_name}")
                        continue

                    url = f"{NETKEIBA_BASE_URL}/odds/index.html?race_id={netkeiba_id}&type={ttype}"

                    if not first_request:
                        time.sleep(sleep_sec)
                    first_request = False

                    html = _fetch_html_with_page(page, url, wait_selector=None, sleep_sec=sleep_sec)
                    if not html:
                        continue

                    df = _parse_combo_odds_html(html, ttype)
                    if not df.empty:
                        all_dfs.append(df)

                if all_dfs:
                    combined = pd.concat(all_dfs, ignore_index=True)
                    results.append((race_id, race["race_date"], combined))
                    logger.debug(
                        f"  [{i+1}/{len(races)}] {netkeiba_id}: "
                        f"{len(combined)}件"
                    )
        finally:
            browser.close()

    return results


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def run_win_place(
    project_id: str,
    races: list[dict],
    sleep_sec: float,
    batch_size: int,
) -> None:
    """単複オッズの一括取得・保存"""
    # 既スクレイプ済みをスキップ
    already_scraped = fetch_already_scraped_race_ids(project_id, "daily_odds")
    pending = [r for r in races if r["race_id"] not in already_scraped]

    logger.info(
        f"単複オッズ取得対象: {len(pending):,}レース "
        f"（スキップ: {len(races) - len(pending):,}）"
    )

    if not pending:
        logger.info("取得対象レースがありません")
        return

    total_saved = 0
    for batch_start in range(0, len(pending), batch_size):
        batch = pending[batch_start: batch_start + batch_size]
        batch_end = min(batch_start + batch_size, len(pending))

        logger.info(
            f"バッチ処理: {batch_start + 1}〜{batch_end} / {len(pending):,}レース "
            f"（{batch[0]['race_date']} ～ {batch[-1]['race_date']}）"
        )

        rows = scrape_win_place_odds_batch(batch, sleep_sec=sleep_sec)
        if not rows:
            logger.warning(f"  バッチ内でオッズが取得できませんでした")
            continue

        df = pd.DataFrame(rows)
        saved = save_odds_to_bq(df, project_id)
        total_saved += saved
        logger.info(
            f"  保存: {saved}行 / 累計: {total_saved:,}行"
        )

    logger.info(f"単複オッズ取得完了: 累計{total_saved:,}行")


def run_combo(
    project_id: str,
    races: list[dict],
    ticket_types: list[str],
    sleep_sec: float,
    batch_size: int,
) -> None:
    """組み合わせ馬券オッズの一括取得・保存"""
    # (race_id, ticket_type) ペア単位でスキップ判定
    # → 特定券種だけ再取得する場合（Issue #167 後の wide/sanrenpuku 再取得など）に
    #   既取得済みの他券種を無駄に再スクレイプしない
    already_pairs = fetch_already_scraped_combo_pairs(project_id)
    ticket_type_names = {COMBO_TICKET_TYPES[t] for t in ticket_types if t in COMBO_TICKET_TYPES}

    # 少なくとも1つの ticket_type が未取得のレースのみ対象
    pending = [
        r for r in races
        if any((r["race_id"], name) not in already_pairs for name in ticket_type_names)
    ]
    skipped = len(races) - len(pending)

    ticket_names = [COMBO_TICKET_TYPES.get(t, t) for t in ticket_types]
    logger.info(
        f"コンボオッズ取得対象: {len(pending):,}レース "
        f"（全券種スキップ: {skipped:,}）"
        f" 馬券種: {ticket_names}"
    )

    if not pending:
        logger.info("取得対象レースがありません")
        return

    scraped_at = datetime.datetime.now(datetime.timezone.utc)
    total_saved = 0

    for batch_start in range(0, len(pending), batch_size):
        batch = pending[batch_start: batch_start + batch_size]
        batch_end = min(batch_start + batch_size, len(pending))

        logger.info(
            f"バッチ処理: {batch_start + 1}〜{batch_end} / {len(pending):,}レース"
        )

        results = scrape_combo_odds_batch(
            batch,
            ticket_types=ticket_types,
            sleep_sec=sleep_sec,
            already_scraped_pairs=already_pairs,
        )

        for race_id, race_date, combo_df in results:
            try:
                saved = save_combo_odds_to_bq(
                    df=combo_df,
                    race_id=race_id,
                    race_date=race_date,
                    scraped_at=scraped_at,
                    project_id=project_id,
                )
                total_saved += saved
            except Exception as e:
                logger.error(f"  BQ保存失敗: race_id={race_id}, {e}")

        logger.info(f"  バッチ完了 / 累計保存: {total_saved:,}行")

    logger.info(f"コンボオッズ取得完了: 累計{total_saved:,}行")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="過去レースの単複・組み合わせ馬券オッズをnetkeibaから一括取得してBQに保存する",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--project-id", required=True, help="GCPプロジェクトID")
    parser.add_argument(
        "--start-date",
        default="2016-01-01",
        help="取得開始日（YYYY-MM-DD形式、デフォルト: 2016-01-01）",
    )
    parser.add_argument(
        "--end-date",
        default=datetime.date.today().isoformat(),
        help="取得終了日（YYYY-MM-DD形式、デフォルト: 本日）",
    )
    parser.add_argument(
        "--mode",
        choices=["win_place", "combo", "all"],
        default="all",
        help="取得する馬券種モード（win_place=単複のみ, combo=組み合わせのみ, all=全種）",
    )
    parser.add_argument(
        "--ticket-types",
        nargs="+",
        default=["b4", "b5", "b7"],
        choices=["b4", "b5", "b6", "b7"],
        help="comboモードで取得する馬券種（b4=馬連 b5=ワイド b6=馬単 b7=三連複, デフォルト: b4 b5 b7）",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=2.0,
        help="ページ間スリープ秒数（デフォルト: 2.0）",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="1バッチあたりのレース数（デフォルト: 50）。BQへのバッチ保存間隔。",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="対象レース数の確認のみ行い、スクレイプは実行しない",
    )

    args = parser.parse_args()

    # 対象レースを取得
    races = fetch_target_races(args.project_id, args.start_date, args.end_date)
    if not races:
        logger.warning("対象レースが見つかりません")
        sys.exit(0)

    # netkeiba race_idを付与
    converted: list[dict] = []
    error_count = 0
    for race in races:
        try:
            netkeiba_id = jrdb_to_netkeiba_race_id(race["race_id"])
            converted.append({**race, "netkeiba_race_id": netkeiba_id})
        except ValueError as e:
            logger.warning(f"race_id変換スキップ: {race['race_id']} - {e}")
            error_count += 1

    if error_count > 0:
        logger.warning(f"変換エラー: {error_count}件スキップ")

    logger.info(f"変換後の取得対象: {len(converted):,}レース")

    # 推定時間を表示
    if args.mode in ("win_place", "all"):
        est_win = len(converted) * args.sleep_sec * 2 / 3600
        logger.info(f"単複取得の推定時間: 約{est_win:.1f}時間")
    if args.mode in ("combo", "all"):
        n_ticket = len(args.ticket_types)
        est_combo = len(converted) * n_ticket * args.sleep_sec * 2 / 3600
        logger.info(f"コンボ取得の推定時間: 約{est_combo:.1f}時間（{n_ticket}種）")

    if args.dry_run:
        logger.info("[DRY RUN] ここで終了します（--dry-run オプション）")
        sys.exit(0)

    # 実行
    if args.mode in ("win_place", "all"):
        logger.info("=== 単複オッズ取得開始 ===")
        run_win_place(
            project_id=args.project_id,
            races=converted,
            sleep_sec=args.sleep_sec,
            batch_size=args.batch_size,
        )

    if args.mode in ("combo", "all"):
        logger.info("=== コンボオッズ取得開始 ===")
        run_combo(
            project_id=args.project_id,
            races=converted,
            ticket_types=args.ticket_types,
            sleep_sec=args.sleep_sec,
            batch_size=args.batch_size,
        )

    logger.info("全処理完了")


if __name__ == "__main__":
    main()
