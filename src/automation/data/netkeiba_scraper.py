"""
netkeibaオッズスクレイパー

netkeibaからリアルタイムの単勝・複勝および組み合わせ馬券オッズを取得し、
JRDB形式のrace_idと紐付けてBigQueryに保存する。

URL構造:
  https://race.netkeiba.com/odds/index.html?race_id={YYYY}{PP}{NN}{DD}{RR}&type=b{KIND}
  - YYYY: 開催年
  - PP: 競馬場コード（JRDBのvenue_codeと同一）
  - NN: 回次
  - DD: 日次
  - RR: レース番号
  - KIND: 1=単複, 4=馬連, 5=馬単, 7=ワイド, 6=三連複

組み合わせ馬券のHTMLパース:
  2頭組み合わせ（馬連・馬単・ワイド）: <span id="odds-{code}-XXYY">
  3頭組み合わせ（三連複）: <span id="odds-{code}-XXYYZZ">
  ここで XX/YY/ZZ は馬番の2桁ゼロ埋め表現

Issue #131: netkeibaリアルタイムオッズスクレイパーの実装
Issue #134: netkeibaスクレイパー拡張 - 組み合わせ馬券オッズ取得
"""

from __future__ import annotations

import datetime
import logging
import re
import time
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import bigquery

logger = logging.getLogger(__name__)

NETKEIBA_BASE_URL = "https://race.netkeiba.com"

# 競馬場コード（JRDBのvenue_codeと完全一致）
VENUE_CODE_MAP: dict[str, str] = {
    "01": "札幌",
    "02": "函館",
    "03": "福島",
    "04": "新潟",
    "05": "東京",
    "06": "中山",
    "07": "中京",
    "08": "京都",
    "09": "阪神",
    "10": "小倉",
}

_EMPTY_ODDS_COLUMNS = ["horse_number", "win_odds", "place_odds_min", "place_odds_max"]

# 組み合わせ馬券の type コードと BQ 保存用 ticket_type 名の対応
COMBO_TICKET_TYPES: dict[str, str] = {
    "b4": "umaren",     # 馬連（2頭・順不同）
    "b5": "umatan",     # 馬単（2頭・順あり）
    "b7": "wide",       # ワイド（2頭・順不同）
    "b6": "sanrenpuku", # 三連複（3頭・順不同）
}

# b4/b5/b7 の span id における数値コード
_COMBO_TYPE_CODE: dict[str, str] = {
    "b4": "4",
    "b5": "5",
    "b7": "7",
    "b6": "6",
}

_EMPTY_COMBO_COLUMNS = ["ticket_type", "horse_number_1", "horse_number_2", "horse_number_3", "odds"]


def parse_netkeiba_race_id(netkeiba_race_id: str) -> dict:
    """
    netkeibaのrace_id（12桁）をパースして各要素を返す

    フォーマット: {YYYY}{PP}{NN}{DD}{RR}
      - YYYY: 開催年
      - PP  : 競馬場コード（JRDBのvenue_codeと同一）
      - NN  : 回次
      - DD  : 日次
      - RR  : レース番号

    Args:
        netkeiba_race_id: 12桁の数字文字列（例: "202405021211"）

    Returns:
        {"year", "venue_code", "kai", "nichi", "race_number"} の辞書

    Raises:
        ValueError: 12桁でない、または数字でない場合
    """
    if len(netkeiba_race_id) != 12 or not netkeiba_race_id.isdigit():
        raise ValueError(
            f"netkeiba race_id は12桁の数字である必要があります: '{netkeiba_race_id}'"
        )
    return {
        "year": netkeiba_race_id[0:4],
        "venue_code": netkeiba_race_id[4:6],
        "kai": netkeiba_race_id[6:8],
        "nichi": netkeiba_race_id[8:10],
        "race_number": int(netkeiba_race_id[10:12]),
    }


def _parse_race_list_html(html: str) -> list[dict]:
    """
    レース一覧ページのHTMLからレース情報を抽出する（テスト可能な純粋関数）

    Args:
        html: レース一覧ページのHTML文字列

    Returns:
        [{"netkeiba_race_id": str, "venue_code": str, "race_number": int}] のリスト
        重複を除去して race_id の昇順で返す
    """
    race_id_pattern = re.compile(r'race_id=(\d{12})')
    found_ids: set[str] = set(race_id_pattern.findall(html))

    races = []
    for race_id in sorted(found_ids):
        try:
            parsed = parse_netkeiba_race_id(race_id)
            races.append({
                "netkeiba_race_id": race_id,
                "venue_code": parsed["venue_code"],
                "race_number": parsed["race_number"],
            })
        except ValueError:
            logger.warning(f"不正なrace_idをスキップ: {race_id}")

    return races


def _parse_win_place_odds_html(html: str) -> pd.DataFrame:
    """
    単複オッズページ（type=b1）のHTMLから単勝・複勝オッズを抽出する（テスト可能な純粋関数）

    netkeibaの単複オッズテーブル構造:
      - 単勝: <div id="odds_tan_block"> 内の <table>
      - 複勝: <div id="odds_fuku_block"> 内の <table>
      各行: <td>枠</td> <td>馬番</td> <td>印</td> <td>選択</td> <td>馬名</td> <td>オッズ</td>
      複勝オッズは「1.3 - 1.8」形式（スペース+ハイフン）または「1.3〜1.8」形式

    Args:
        html: 単複オッズページのHTML文字列

    Returns:
        DataFrame[horse_number(int), win_odds(float), place_odds_min(float), place_odds_max(float)]
        データが取得できない場合は空のDataFrameを返す
    """
    soup = BeautifulSoup(html, "lxml")

    win_odds_map: dict[int, float] = {}
    place_odds_map: dict[int, tuple[float, float]] = {}

    # 単勝テーブル: id="odds_tan_block" は <div> であり、その中の <table> を取得する
    win_div = soup.find("div", {"id": "odds_tan_block"})
    win_table = win_div.find("table") if win_div else None
    if win_table:
        for row in win_table.find_all("tr")[1:]:  # ヘッダー行をスキップ
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            horse_num, odds = _extract_win_odds_cells(cells)
            if horse_num is not None and odds is not None:
                win_odds_map[horse_num] = odds

    # 複勝テーブル: id="odds_fuku_block" は <div> であり、その中の <table> を取得する
    place_div = soup.find("div", {"id": "odds_fuku_block"})
    place_table = place_div.find("table") if place_div else None
    if place_table:
        for row in place_table.find_all("tr")[1:]:  # ヘッダー行をスキップ
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            horse_num, min_odds, max_odds = _extract_place_odds_cells(cells)
            if horse_num is not None and min_odds is not None:
                place_odds_map[horse_num] = (min_odds, max_odds or min_odds)

    all_horse_nums = set(win_odds_map.keys()) | set(place_odds_map.keys())
    if not all_horse_nums:
        return pd.DataFrame(columns=_EMPTY_ODDS_COLUMNS)

    rows = []
    for horse_num in sorted(all_horse_nums):
        rows.append({
            "horse_number": horse_num,
            "win_odds": win_odds_map.get(horse_num),
            "place_odds_min": place_odds_map.get(horse_num, (None, None))[0],
            "place_odds_max": place_odds_map.get(horse_num, (None, None))[1],
        })

    return pd.DataFrame(rows)


def _extract_win_odds_cells(
    cells: list,
) -> tuple[Optional[int], Optional[float]]:
    """
    単勝テーブルの1行のセルリストから馬番とオッズを抽出する

    Returns:
        (horse_number, odds) または (None, None)
    """
    try:
        # テーブル列: 枠, 馬番, 印, 選択, 馬名, オッズ → cells[1] が馬番
        horse_num = int(cells[1].get_text(strip=True))
        if not (1 <= horse_num <= 18):
            return None, None
        # 最後のセルがオッズ
        odds_text = cells[-1].get_text(strip=True).replace(",", "")
        odds = float(odds_text)
        if odds <= 1.0:
            return None, None
        return horse_num, odds
    except (ValueError, IndexError):
        return None, None


def _extract_place_odds_cells(
    cells: list,
) -> tuple[Optional[int], Optional[float], Optional[float]]:
    """
    複勝テーブルの1行のセルリストから馬番・最小オッズ・最大オッズを抽出する

    Returns:
        (horse_number, min_odds, max_odds) または (None, None, None)
    """
    try:
        # テーブル列: 枠, 馬番, 印, 選択, 馬名, オッズ → cells[1] が馬番
        horse_num = int(cells[1].get_text(strip=True))
        if not (1 <= horse_num <= 18):
            return None, None, None
        odds_text = cells[-1].get_text(strip=True).replace(",", "")
        # netkeibaの複勝オッズ区切り文字: "〜" または " - " の両方に対応
        if "〜" in odds_text:
            parts = odds_text.split("〜")
            min_odds = float(parts[0].strip())
            max_odds = float(parts[1].strip())
        elif " - " in odds_text:
            parts = odds_text.split(" - ")
            min_odds = float(parts[0].strip())
            max_odds = float(parts[1].strip())
        else:
            min_odds = max_odds = float(odds_text)
        return horse_num, min_odds, max_odds
    except (ValueError, IndexError):
        return None, None, None


def get_today_race_list(date: datetime.date, sleep_sec: float = 1.0) -> list[dict]:
    """
    当日の全レース情報をnetkeibaから取得する

    URL: https://race.netkeiba.com/top/race_list.html?kaisai_date=YYYYMMDD

    Args:
        date: 対象日付
        sleep_sec: ページロード後の追加待機秒数

    Returns:
        [{"netkeiba_race_id": str, "venue_code": str, "race_number": int}] のリスト

    Raises:
        ImportError: playwright がインストールされていない場合
        RuntimeError: ページ取得に失敗した場合
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "playwright が未インストールです。"
            "`pip install playwright && playwright install chromium` を実行してください。"
        ) from e

    url = f"{NETKEIBA_BASE_URL}/top/race_list.html?kaisai_date={date.strftime('%Y%m%d')}"
    logger.info(f"レース一覧取得: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=30_000)
            if sleep_sec > 0:
                time.sleep(sleep_sec)
            html = page.content()
        except Exception as e:
            raise RuntimeError(f"レース一覧ページの取得に失敗しました: {e}") from e
        finally:
            browser.close()

    races = _parse_race_list_html(html)
    logger.info(f"レース一覧取得完了: {len(races)}レース")
    return races


def get_win_place_odds(
    netkeiba_race_id: str,
    sleep_sec: float = 1.5,
) -> pd.DataFrame:
    """
    指定レースの単勝・複勝オッズをnetkeibaから取得する

    URL: https://race.netkeiba.com/odds/index.html?race_id={ID}&type=b1

    Args:
        netkeiba_race_id: 12桁のnetkeibaレースID
        sleep_sec: ページロード後の追加待機秒数（JavaScript レンダリング待ち）

    Returns:
        DataFrame[horse_number, win_odds, place_odds_min, place_odds_max]
        取得できない場合は空のDataFrame

    Raises:
        ImportError: playwright がインストールされていない場合
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "playwright が未インストールです。"
            "`pip install playwright && playwright install chromium` を実行してください。"
        ) from e

    url = (
        f"{NETKEIBA_BASE_URL}/odds/index.html"
        f"?race_id={netkeiba_race_id}&type=b1"
    )
    logger.debug(f"オッズ取得: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=30_000)

            # JavaScriptによるオッズテーブルの描画を待つ
            try:
                page.wait_for_selector(
                    "#odds_tan_block, #odds_fuku_block",
                    timeout=10_000,
                )
            except Exception:
                logger.warning(
                    f"オッズテーブルの描画待機タイムアウト: race_id={netkeiba_race_id}"
                )

            if sleep_sec > 0:
                time.sleep(sleep_sec)

            html = page.content()
        except Exception as e:
            logger.error(f"オッズページの取得に失敗: race_id={netkeiba_race_id}, error={e}")
            return pd.DataFrame(columns=_EMPTY_ODDS_COLUMNS)
        finally:
            browser.close()

    df = _parse_win_place_odds_html(html)
    logger.debug(f"オッズ取得完了: race_id={netkeiba_race_id}, {len(df)}頭")
    return df


def netkeiba_to_jrdb_race_id(
    race_date: datetime.date,
    venue_code: str,
    race_number: int,
    bq_client: bigquery.Client,
    project_id: str,
) -> Optional[str]:
    """
    日付・venue_code・race_numberでBigQueryを照合してJRDB形式のrace_idを返す

    netkeibaとJRDBはvenue_codeが共通のため、変換不要。
    BigQueryの raw.race_info テーブルを date + venue_code + race_number でJOINする。

    Args:
        race_date: 開催日
        venue_code: 競馬場コード（netkeibaのPP = JRDBのvenue_code）
        race_number: レース番号
        bq_client: BigQuery Clientインスタンス
        project_id: GCPプロジェクトID

    Returns:
        JRDB形式のrace_id（見つからない場合はNone）
    """
    query = f"""
    SELECT race_id
    FROM `{project_id}.raw.race_info`
    WHERE race_date = '{race_date.isoformat()}'
      AND venue_code = '{venue_code}'
      AND race_number = {race_number}
    LIMIT 1
    """
    try:
        result = list(bq_client.query(query).result())
        if result:
            return str(result[0].race_id)
        return None
    except Exception as e:
        logger.warning(
            f"JRDB race_id の照合失敗: date={race_date}, venue={venue_code}, "
            f"race={race_number}, error={e}"
        )
        return None


def scrape_today_odds(
    date: datetime.date,
    project_id: str,
    sleep_sec: float = 1.5,
) -> pd.DataFrame:
    """
    当日の全レースの単勝・複勝オッズを取得してJRDB race_idに変換して返す

    処理フロー:
      1. get_today_race_list() で当日のnetkeibaレースIDを取得
      2. 各レースの単複オッズを get_win_place_odds() で取得
      3. netkeiba_to_jrdb_race_id() でJRDB race_idに変換
      4. DataFrame を返す

    Args:
        date: 対象日付
        project_id: GCPプロジェクトID（BQ照合に使用）
        sleep_sec: 各レースのスクレイプ間隔（秒）

    Returns:
        DataFrame[race_id, race_date, horse_number, win_odds, place_odds_min, place_odds_max, scraped_at]
        スクレイプできたレースのみを含む（対応するJRDB race_idが見つからないレースは除外）
    """
    bq_client = bigquery.Client(project=project_id)
    scraped_at = datetime.datetime.now(datetime.timezone.utc)

    races = get_today_race_list(date, sleep_sec=sleep_sec)
    if not races:
        logger.warning(f"当日のレースが見つかりません: {date}")
        return pd.DataFrame()

    all_rows: list[dict] = []

    for i, race_info in enumerate(races):
        netkeiba_race_id = race_info["netkeiba_race_id"]
        venue_code = race_info["venue_code"]
        race_number = race_info["race_number"]

        venue_name = VENUE_CODE_MAP.get(venue_code, venue_code)
        logger.info(
            f"[{i + 1}/{len(races)}] {venue_name} {race_number}R "
            f"(netkeiba_id={netkeiba_race_id})"
        )

        jrdb_race_id = netkeiba_to_jrdb_race_id(
            race_date=date,
            venue_code=venue_code,
            race_number=race_number,
            bq_client=bq_client,
            project_id=project_id,
        )
        if jrdb_race_id is None:
            logger.warning(
                f"JRDB race_id が見つかりません: {venue_name} {race_number}R → スキップ"
            )
            continue

        if i > 0:
            time.sleep(sleep_sec)

        odds_df = get_win_place_odds(netkeiba_race_id, sleep_sec=sleep_sec)
        if odds_df.empty:
            logger.warning(f"オッズが取得できませんでした: {venue_name} {race_number}R")
            continue

        for _, row in odds_df.iterrows():
            all_rows.append({
                "race_id": jrdb_race_id,
                "race_date": date,
                "horse_number": int(row["horse_number"]),
                "win_odds": row.get("win_odds"),
                "place_odds_min": row.get("place_odds_min"),
                "place_odds_max": row.get("place_odds_max"),
                "scraped_at": scraped_at,
            })

        logger.info(f"  → race_id={jrdb_race_id}, {len(odds_df)}頭のオッズを取得")

    if not all_rows:
        logger.warning("オッズを取得できたレースがありませんでした")
        return pd.DataFrame()

    result_df = pd.DataFrame(all_rows)
    logger.info(
        f"スクレイプ完了: {result_df['race_id'].nunique()}レース, "
        f"{len(result_df)}頭のオッズを取得"
    )
    return result_df


def scrape_today_combo_odds(
    date: datetime.date,
    project_id: str,
    ticket_types: list[str] | None = None,
    sleep_sec: float = 1.5,
) -> int:
    """
    当日の全レースの組み合わせ馬券オッズを取得してBigQueryに保存する

    処理フロー:
      1. get_today_race_list() で当日のnetkeibaレースIDを取得
      2. 各レースの組み合わせオッズを get_combo_odds() で取得
      3. netkeiba_to_jrdb_race_id() でJRDB race_idに変換
      4. save_combo_odds_to_bq() で保存

    Args:
        date: 対象日付
        project_id: GCPプロジェクトID（BQ照合・保存に使用）
        ticket_types: 取得する馬券種リスト（例: ['b4', 'b6']）
                      None の場合は全種（馬連・馬単・ワイド・三連複）を取得
        sleep_sec: 各レース・各馬券種のリクエスト間隔（秒）

    Returns:
        BigQueryに保存した合計行数
    """
    bq_client = bigquery.Client(project=project_id)
    scraped_at = datetime.datetime.now(datetime.timezone.utc)

    races = get_today_race_list(date, sleep_sec=sleep_sec)
    if not races:
        logger.warning(f"当日のレースが見つかりません: {date}")
        return 0

    total_saved = 0
    for race_info in races:
        netkeiba_race_id = race_info["netkeiba_race_id"]
        venue_code = race_info["venue_code"]
        race_number = race_info["race_number"]
        venue_name = VENUE_CODE_MAP.get(venue_code, venue_code)

        jrdb_race_id = netkeiba_to_jrdb_race_id(
            race_date=date,
            venue_code=venue_code,
            race_number=race_number,
            bq_client=bq_client,
            project_id=project_id,
        )
        if jrdb_race_id is None:
            logger.warning(
                f"JRDB race_id が見つかりません: {venue_name} {race_number}R → スキップ"
            )
            continue

        combo_df = get_combo_odds(
            netkeiba_race_id,
            ticket_types=ticket_types,
            sleep_sec=sleep_sec,
        )
        if combo_df.empty:
            logger.warning(f"組み合わせオッズが取得できませんでした: {venue_name} {race_number}R")
            continue

        try:
            saved = save_combo_odds_to_bq(
                df=combo_df,
                race_id=jrdb_race_id,
                race_date=date,
                scraped_at=scraped_at,
                project_id=project_id,
            )
            total_saved += saved
            logger.info(f"  → {venue_name} {race_number}R 組み合わせオッズ: {saved}件保存")
        except Exception as e:
            logger.error(f"BQ保存失敗: {venue_name} {race_number}R, {e}")

    logger.info(f"組み合わせオッズ取得完了: 合計{total_saved}件保存")
    return total_saved


def _parse_combo_odds_html(html: str, ticket_type: str) -> pd.DataFrame:
    """
    組み合わせ馬券（馬連・馬単・ワイド・三連複）のオッズページ HTML をパースする
    （テスト可能な純粋関数）

    HTMLの span ID 形式:
      2頭組み合わせ: <span id="odds-{code}-XXYY">
      3頭組み合わせ: <span id="odds-{code}-XXYYZZ">

    Args:
        html: オッズページのHTML文字列
        ticket_type: 'b4'(馬連) / 'b5'(馬単) / 'b7'(ワイド) / 'b6'(三連複)

    Returns:
        DataFrame[ticket_type(str), horse_number_1(int), horse_number_2(int),
                  horse_number_3(int or None), odds(float)]
        パース失敗時は空のDataFrameを返す
    """
    code = _COMBO_TYPE_CODE.get(ticket_type)
    if code is None:
        logger.warning(f"未対応のticket_type: {ticket_type}")
        return pd.DataFrame(columns=_EMPTY_COMBO_COLUMNS)

    soup = BeautifulSoup(html, "lxml")
    is_trio = (ticket_type == "b6")
    # 三連複は6桁、2頭組は4桁
    pattern = re.compile(rf'^odds-{code}-(\d{{6}})$' if is_trio else rf'^odds-{code}-(\d{{4}})$')

    rows = []
    for span in soup.find_all("span", id=pattern):
        match = pattern.match(span["id"])
        if not match:
            continue
        digits = match.group(1)
        odds_text = span.get_text(strip=True).replace(",", "")
        try:
            odds_val = float(odds_text)
        except ValueError:
            continue
        if odds_val <= 1.0:
            continue

        if is_trio:
            h1, h2, h3 = int(digits[0:2]), int(digits[2:4]), int(digits[4:6])
            rows.append({
                "ticket_type": COMBO_TICKET_TYPES[ticket_type],
                "horse_number_1": h1,
                "horse_number_2": h2,
                "horse_number_3": h3,
                "odds": odds_val,
            })
        else:
            h1, h2 = int(digits[0:2]), int(digits[2:4])
            rows.append({
                "ticket_type": COMBO_TICKET_TYPES[ticket_type],
                "horse_number_1": h1,
                "horse_number_2": h2,
                "horse_number_3": None,
                "odds": odds_val,
            })

    if not rows:
        return pd.DataFrame(columns=_EMPTY_COMBO_COLUMNS)

    return pd.DataFrame(rows)


def get_combo_odds(
    netkeiba_race_id: str,
    ticket_types: list[str] | None = None,
    sleep_sec: float = 1.5,
) -> pd.DataFrame:
    """
    指定レースの組み合わせ馬券オッズをnetkeibaから取得する

    Args:
        netkeiba_race_id: 12桁のnetkeibaレースID
        ticket_types: 取得する馬券種リスト（例: ['b4', 'b6']）
                      None の場合は全種（馬連・馬単・ワイド・三連複）を取得
        sleep_sec: 各ページのリクエスト間隔（秒）

    Returns:
        DataFrame[ticket_type, horse_number_1, horse_number_2, horse_number_3, odds]

    Raises:
        ImportError: playwright が未インストールの場合
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "playwright が未インストールです。"
            "`pip install playwright && playwright install chromium` を実行してください。"
        ) from e

    if ticket_types is None:
        ticket_types = list(COMBO_TICKET_TYPES.keys())

    all_dfs: list[pd.DataFrame] = []

    for i, ttype in enumerate(ticket_types):
        if ttype not in COMBO_TICKET_TYPES:
            logger.warning(f"未対応の馬券種をスキップ: {ttype}")
            continue

        url = (
            f"{NETKEIBA_BASE_URL}/odds/index.html"
            f"?race_id={netkeiba_race_id}&type={ttype}"
        )
        logger.debug(f"組み合わせオッズ取得: {url}")

        if i > 0:
            time.sleep(sleep_sec)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.goto(url, wait_until="load", timeout=30_000)
                time.sleep(sleep_sec)
                html = page.content()
            except Exception as e:
                logger.error(f"オッズページ取得失敗: {ttype} race_id={netkeiba_race_id}, {e}")
                html = ""
            finally:
                browser.close()

        df = _parse_combo_odds_html(html, ttype)
        if not df.empty:
            all_dfs.append(df)
            logger.debug(
                f"  {COMBO_TICKET_TYPES[ttype]}: {len(df)}件"
            )

    if not all_dfs:
        return pd.DataFrame(columns=_EMPTY_COMBO_COLUMNS)

    return pd.concat(all_dfs, ignore_index=True)


def save_combo_odds_to_bq(
    df: pd.DataFrame,
    race_id: str,
    race_date: datetime.date,
    scraped_at: datetime.datetime,
    project_id: str,
    dataset: str = "predictions",
    table: str = "daily_odds_combo",
) -> int:
    """
    組み合わせ馬券オッズ DataFrame を BigQuery に UPSERT 保存する

    Args:
        df: get_combo_odds() が返す DataFrame
        race_id: JRDB形式のレースID
        race_date: 開催日
        scraped_at: スクレイプ日時（UTC）
        project_id: GCP プロジェクト ID
        dataset: データセット名
        table: テーブル名

    Returns:
        保存した行数

    Raises:
        ValueError: df が空の場合
    """
    if df.empty:
        raise ValueError("保存するデータがありません（空のDataFrame）")

    df = df.copy()
    df["race_id"] = race_id
    df["race_date"] = race_date
    df["scraped_at"] = scraped_at

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"
    temp_table = f"{table_ref}_temp_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("race_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("race_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("ticket_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("horse_number_1", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("horse_number_2", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("horse_number_3", "INTEGER"),
            bigquery.SchemaField("odds", "FLOAT64"),
            bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    )
    client.load_table_from_dataframe(df, temp_table, job_config=job_config).result()

    merge_query = f"""
    MERGE `{table_ref}` AS target
    USING `{temp_table}` AS source
    ON target.race_id = source.race_id
       AND target.ticket_type = source.ticket_type
       AND target.horse_number_1 = source.horse_number_1
       AND target.horse_number_2 = source.horse_number_2
       AND (target.horse_number_3 = source.horse_number_3
            OR (target.horse_number_3 IS NULL AND source.horse_number_3 IS NULL))
    WHEN MATCHED THEN
      UPDATE SET odds = source.odds, scraped_at = source.scraped_at
    WHEN NOT MATCHED THEN
      INSERT (race_id, race_date, ticket_type, horse_number_1, horse_number_2,
              horse_number_3, odds, scraped_at)
      VALUES (source.race_id, source.race_date, source.ticket_type,
              source.horse_number_1, source.horse_number_2,
              source.horse_number_3, source.odds, source.scraped_at)
    """
    client.query(merge_query).result()
    client.delete_table(temp_table, not_found_ok=True)

    logger.info(f"BQへの保存完了: {len(df)}行 → {table_ref}")
    return len(df)


def save_odds_to_bq(
    df: pd.DataFrame,
    project_id: str,
    dataset: str = "predictions",
    table: str = "daily_odds",
) -> int:
    """
    オッズDataFrameをBigQueryにUPSERTで保存する

    race_id + horse_number をキーにMERGEを実行する。

    Args:
        df: scrape_today_odds() が返すDataFrame
        project_id: GCPプロジェクトID
        dataset: データセット名
        table: テーブル名

    Returns:
        保存した行数

    Raises:
        ValueError: dfが空の場合
    """
    if df.empty:
        raise ValueError("保存するデータがありません（空のDataFrame）")

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"

    temp_table = f"{table_ref}_temp_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("race_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("race_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("horse_number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("win_odds", "FLOAT"),
            bigquery.SchemaField("place_odds_min", "FLOAT"),
            bigquery.SchemaField("place_odds_max", "FLOAT"),
            bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    )

    load_job = client.load_table_from_dataframe(df, temp_table, job_config=job_config)
    load_job.result()
    logger.info(f"一時テーブルにロード完了: {temp_table}")

    merge_query = f"""
    MERGE `{table_ref}` AS target
    USING `{temp_table}` AS source
    ON target.race_id = source.race_id AND target.horse_number = source.horse_number
    WHEN MATCHED THEN
      UPDATE SET
        race_date = source.race_date,
        win_odds = source.win_odds,
        place_odds_min = source.place_odds_min,
        place_odds_max = source.place_odds_max,
        scraped_at = source.scraped_at
    WHEN NOT MATCHED THEN
      INSERT (race_id, race_date, horse_number, win_odds, place_odds_min, place_odds_max, scraped_at)
      VALUES (source.race_id, source.race_date, source.horse_number,
              source.win_odds, source.place_odds_min, source.place_odds_max, source.scraped_at)
    """
    merge_job = client.query(merge_query)
    merge_job.result()

    client.delete_table(temp_table, not_found_ok=True)

    saved_rows = len(df)
    logger.info(f"BQへの保存完了: {saved_rows}行 → {table_ref}")
    return saved_rows
