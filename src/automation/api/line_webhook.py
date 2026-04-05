"""
LINE Messaging API Webhook ハンドラー

ユーザーが「日付 競馬場名 レース番号」形式でメッセージを送信した場合のみ返答する。

返答内容:
  1. 予測テーブル（予測順・馬番・馬名・スコア・複勝率・オッズ・期待値）
  2. 推奨馬券リスト（馬券種・馬番・馬名・オッズ・賭け金）

環境変数:
  LINE_CHANNEL_SECRET         : Webhook 署名検証用シークレット
  LINE_CHANNEL_ACCESS_TOKEN   : リプライ送信用アクセストークン
  GCP_PROJECT_ID              : BigQuery プロジェクト ID
"""

import hashlib
import hmac
import logging
import re
from base64 import b64encode
from datetime import date
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# 競馬場名 → venue_code 変換マップ
VENUE_NAME_TO_CODE: dict[str, str] = {
    "札幌": "01",
    "函館": "02",
    "福島": "03",
    "新潟": "04",
    "東京": "05",
    "中山": "06",
    "中京": "07",
    "京都": "08",
    "阪神": "09",
    "小倉": "10",
}

VENUE_CODE_TO_NAME: dict[str, str] = {v: k for k, v in VENUE_NAME_TO_CODE.items()}

# 馬券種の日本語ラベル
BET_TYPE_LABELS: dict[str, str] = {
    "place": "複勝",
    "win": "単勝",
    "wide": "ワイド",
    "umaren": "馬連",
    "sanrenpuku": "三連複",
}

# レースパターンの日本語ラベル
PATTERN_LABELS: dict[str, str] = {
    "one_dominant": "突出型",
    "standard": "標準型",
}


def verify_line_signature(
    channel_secret: str,
    body: bytes,
    signature: str,
) -> bool:
    """
    LINE Webhook 署名を HMAC-SHA256 で検証する

    Args:
        channel_secret: LINE チャネルシークレット
        body: リクエストボディ（生バイト列）
        signature: X-Line-Signature ヘッダー値（Base64エンコード済み）

    Returns:
        署名が正しければ True
    """
    hash_val = hmac.new(
        channel_secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).digest()
    expected = b64encode(hash_val).decode("utf-8")
    return hmac.compare_digest(expected, signature)


# --- メッセージパース -----------------------------------------------------------

# 受け付けるパターン例: "2026-03-08 阪神 12R" / "03/08 阪神 12" / "阪神 12R 2026-03-08"
_DATE_PATTERN_FULL = r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})"   # YYYY-MM-DD / YYYY/MM/DD
_DATE_PATTERN_SHORT = r"(\d{1,2})[/-](\d{1,2})"              # MM-DD / MM/DD (年は今年)
_VENUE_PATTERN = "|".join(VENUE_NAME_TO_CODE.keys())
# R/r サフィックスが付いているものを優先、なければ末尾の独立した数値
_RACE_NUM_WITH_R = r"(\d{1,2})[Rr]"
_RACE_NUM_STANDALONE = r"(?<!\d)(\d{1,2})(?!\d)"


def parse_race_query(text: str) -> dict[str, Any] | None:
    """
    「日付 競馬場名 レース番号」形式のテキストを解析する。

    戻り値:
        {"race_date": date, "venue_code": str, "race_number": int} または None
    """
    # 競馬場名を抽出
    venue_match = re.search(_VENUE_PATTERN, text)
    if not venue_match:
        return None

    venue_name = venue_match.group(0)
    venue_code = VENUE_NAME_TO_CODE[venue_name]

    # 日付を先に抽出して日付部分を除去（レース番号との誤マッチ防止）
    race_date: date | None = None
    date_span: tuple[int, int] | None = None

    m_full = re.search(_DATE_PATTERN_FULL, text)
    if m_full:
        year, month, day = int(m_full.group(1)), int(m_full.group(2)), int(m_full.group(3))
        try:
            race_date = date(year, month, day)
            date_span = m_full.span()
        except ValueError:
            return None
    else:
        m_short = re.search(_DATE_PATTERN_SHORT, text)
        if m_short:
            month, day = int(m_short.group(1)), int(m_short.group(2))
            try:
                race_date = date(date.today().year, month, day)
                date_span = m_short.span()
            except ValueError:
                return None
        else:
            return None

    # 日付部分を除去したテキストでレース番号を探す
    text_without_date = text[: date_span[0]] + " " * (date_span[1] - date_span[0]) + text[date_span[1] :]

    # 「12R」「12r」形式を優先
    race_num_match = re.search(_RACE_NUM_WITH_R, text_without_date)
    if race_num_match:
        race_number = int(race_num_match.group(1))
    else:
        # 独立した数値（1〜12桁）を探す
        candidates = list(re.finditer(_RACE_NUM_STANDALONE, text_without_date))
        # 競馬場名以外の数値のみを対象にする
        venue_start, venue_end = venue_match.span()
        num_matches = [m for m in candidates if not (venue_start <= m.start() < venue_end)]
        if not num_matches:
            return None
        # 最後に現れた数値をレース番号として採用
        race_number = int(num_matches[-1].group(1))

    return {
        "race_date": race_date,
        "venue_code": venue_code,
        "venue_name": venue_name,
        "race_number": race_number,
    }


# --- BigQuery データ取得 -------------------------------------------------------

def _fetch_race_predictions(
    project_id: str,
    race_date: date,
    venue_code: str,
    race_number: int,
) -> pd.DataFrame:
    """
    predictions.daily_predictions から指定レースの予測データを取得する
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT
        horse_number,
        horse_name,
        win_place_prob,
        pred_score,
        rank_in_race
    FROM `{project_id}.predictions.daily_predictions`
    WHERE race_date = '{race_date.isoformat()}'
      AND venue_code = '{venue_code}'
      AND race_number = {race_number}
    ORDER BY rank_in_race ASC
    """
    return client.query(query).to_dataframe()


def _fetch_race_odds(
    project_id: str,
    race_date: date,
    venue_code: str,
    race_number: int,
) -> pd.DataFrame:
    """
    predictions.daily_odds から指定レースの単複オッズを取得する
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT
        horse_number,
        win_odds,
        place_odds_min AS place_odds
    FROM `{project_id}.predictions.daily_odds`
    WHERE race_date = '{race_date.isoformat()}'
      AND venue_code = '{venue_code}'
      AND race_number = {race_number}
    """
    return client.query(query).to_dataframe()


def _fetch_race_combo_odds(
    project_id: str,
    race_date: date,
    venue_code: str,
    race_number: int,
) -> pd.DataFrame:
    """
    predictions.daily_odds_combo から組み合わせオッズを取得する（なければ空）
    """
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id)
        query = f"""
        SELECT
            bet_type,
            horse_number_1,
            horse_number_2,
            horse_number_3,
            odds_value
        FROM `{project_id}.predictions.daily_odds_combo`
        WHERE race_date = '{race_date.isoformat()}'
          AND venue_code = '{venue_code}'
          AND race_number = {race_number}
        """
        return client.query(query).to_dataframe()
    except Exception as e:
        logger.warning(f"combo_odds 取得失敗（スキップ）: {e}")
        return pd.DataFrame()


# --- メッセージフォーマット -------------------------------------------------------

def _format_prediction_table(
    race_date: date,
    venue_name: str,
    race_number: int,
    merged_df: pd.DataFrame,
) -> str:
    """
    予測順・馬番・馬名・スコア・複勝率・オッズ・期待値 の表を生成する
    """
    header = (
        f"{'='*44}\n"
        f"Race: {venue_name} {race_number}R ({race_date.isoformat()})\n"
        f"{'='*44}\n"
        f"  予測順  馬番 馬名              スコア    複勝率  オッズ  期待値\n"
        f"{'-'*44}"
    )

    rows = []
    for _, row in merged_df.sort_values("rank_in_race").iterrows():
        rank = int(row["rank_in_race"])
        num = int(row["horse_number"])
        name = str(row.get("horse_name", ""))[:8].ljust(8)
        score = float(row.get("pred_score", 0.0))
        prob = float(row.get("win_place_prob", 0.0))
        odds_val = row.get("place_odds")
        if odds_val is None or pd.isna(odds_val):
            odds_str = "  -  "
            ev_str = "  -  "
        else:
            odds_f = float(odds_val)
            ev_f = prob * odds_f
            odds_str = f"{odds_f:5.1f}倍"
            ev_str = f"{ev_f:5.2f}"
        rows.append(
            f"  {rank:3d}  {num:3d} {name}  {score:+7.4f}  {prob*100:5.1f}%  {odds_str}  {ev_str}"
        )

    return header + "\n" + "\n".join(rows)


def _format_bet_recommendations(
    race_date: date,
    venue_name: str,
    race_number: int,
    bets: list[dict[str, Any]],
    race_pattern: str,
    merged_df: pd.DataFrame,
) -> str:
    """
    推奨馬券リストのメッセージを生成する
    """
    pattern_label = PATTERN_LABELS.get(race_pattern, race_pattern)
    title = (
        f"🏇 推奨馬券リスト\n"
        f"【{venue_name} {race_number}R】({race_date.isoformat()})\n"
        f"パターン: {pattern_label}\n"
        f"{'─'*30}"
    )

    if not bets:
        return title + "\n推奨馬券なし（期待値基準を満たす馬券が見つかりませんでした）"

    # horse_number → horse_name のマップ
    name_map: dict[int, str] = {}
    if not merged_df.empty and "horse_name" in merged_df.columns:
        for _, r in merged_df.iterrows():
            name_map[int(r["horse_number"])] = str(r.get("horse_name", ""))

    lines = []
    total_bet = 0.0
    for bet in bets:
        bet_type_label = BET_TYPE_LABELS.get(bet.get("bet_type", ""), bet.get("bet_type", ""))
        horse_numbers = bet.get("horse_numbers", [])
        horse_parts = []
        for hn in horse_numbers:
            hn_name = name_map.get(int(hn), "")
            horse_parts.append(f"馬番{hn} {hn_name}")
        horse_str = " / ".join(horse_parts)
        odds_val = float(bet.get("odds", 0.0))
        bet_amount = float(bet.get("bet_amount", 0.0))
        total_bet += bet_amount
        lines.append(
            f"  [{bet_type_label}] {horse_str}\n"
            f"    オッズ: {odds_val:.1f}倍  推奨額: ¥{bet_amount:,.0f}"
        )

    footer = f"{'─'*30}\n合計投資額: ¥{total_bet:,.0f}"
    return title + "\n" + "\n".join(lines) + "\n" + footer


# --- 投資戦略実行 ---------------------------------------------------------------

def _run_strategy_for_race(
    project_id: str,
    race_date: date,
    venue_code: str,
    race_number: int,
    race_df: pd.DataFrame,
    combo_odds_df: pd.DataFrame,
) -> tuple[list[dict[str, Any]], str]:
    """
    select_bets_for_race() を呼び出して推奨馬券を生成する

    Returns:
        (bets, race_pattern_str)
    """
    import yaml
    from pathlib import Path
    from src.backtest.strategy import select_bets_for_race

    config_path = Path(__file__).resolve().parents[3] / "config" / "strategy_config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        p1 = float(cfg.get("p1", 0.2))
        expected_return_threshold = float(cfg.get("expected_return_threshold", 1.2))
        budget_per_race = float(cfg.get("budget_per_race", 3000.0))
        min_bet_amount = float(cfg.get("min_bet_amount", 100.0))
        min_prob_threshold = float(cfg.get("min_prob_threshold", 0.10))
        prob_weight_r_dominant = float(cfg.get("prob_weight_r_dominant", 1.0))
        prob_weight_r_standard = float(cfg.get("prob_weight_r_standard", 1.0))
        _legacy_top_n = int(cfg.get("top_n", 5))
        top_n_dominant = int(cfg.get("top_n_dominant", _legacy_top_n))
        top_n_standard = int(cfg.get("top_n_standard", _legacy_top_n))
    else:
        p1, budget_per_race, min_bet_amount = 0.2, 3000.0, 100.0
        min_prob_threshold = 0.10
        expected_return_threshold = 1.2
        prob_weight_r_dominant, prob_weight_r_standard = 1.0, 1.0
        top_n_dominant, top_n_standard = 5, 5

    try:
        bets, pattern = select_bets_for_race(
            race_df=race_df,
            combo_odds_df=combo_odds_df if not combo_odds_df.empty else None,
            budget_per_race=budget_per_race,
            p1=p1,
            expected_return_threshold=expected_return_threshold,
            min_bet_amount=min_bet_amount,
            min_prob_threshold=min_prob_threshold,
            prob_weight_r_dominant=prob_weight_r_dominant,
            prob_weight_r_standard=prob_weight_r_standard,
            top_n_dominant=top_n_dominant,
            top_n_standard=top_n_standard,
        )
        return bets, pattern.pattern
    except ValueError as e:
        logger.warning(f"select_bets_for_race エラー: {e}")
        return [], "standard"


# --- インテントハンドラ ---------------------------------------------------------

def handle_race_query(
    text: str,
    project_id: str,
) -> list[dict[str, str]] | None:
    """
    「日付 競馬場名 レース番号」形式のメッセージを処理して返答メッセージリストを返す。

    Args:
        text: ユーザーから受信したメッセージテキスト
        project_id: GCP プロジェクト ID

    Returns:
        LINE メッセージオブジェクトのリスト（2件）、またはパース失敗時は None
    """
    query = parse_race_query(text)
    if query is None:
        return None

    race_date = query["race_date"]
    venue_code = query["venue_code"]
    venue_name = query["venue_name"]
    race_number = query["race_number"]

    logger.info(
        f"レースクエリ: date={race_date}, venue={venue_name}({venue_code}), "
        f"race_number={race_number}"
    )

    # BigQuery からデータ取得
    pred_df = _fetch_race_predictions(project_id, race_date, venue_code, race_number)
    odds_df = _fetch_race_odds(project_id, race_date, venue_code, race_number)
    combo_df = _fetch_race_combo_odds(project_id, race_date, venue_code, race_number)

    if pred_df.empty:
        return [{"type": "text", "text": f"{venue_name} {race_number}R ({race_date.isoformat()}) の予測データが見つかりませんでした。"}]

    # 予測 + オッズを結合
    if not odds_df.empty:
        merged = pd.merge(pred_df, odds_df, on="horse_number", how="left")
    else:
        merged = pred_df.copy()
        for col in ("win_odds", "place_odds"):
            merged[col] = None

    # 複勝オッズを strategy 用にカラム名を揃える
    if "place_odds" in merged.columns:
        merged["odds"] = merged["place_odds"]
    else:
        merged["odds"] = None

    # horse_id カラムがなければダミーを追加（select_bets_for_race の必須カラム）
    if "horse_id" not in merged.columns:
        merged["horse_id"] = merged["horse_number"].astype(str)

    # メッセージ1: 予測テーブル
    msg1_text = _format_prediction_table(race_date, venue_name, race_number, merged)

    # 推奨馬券の計算
    bets, pattern_str = _run_strategy_for_race(
        project_id=project_id,
        race_date=race_date,
        venue_code=venue_code,
        race_number=race_number,
        race_df=merged,
        combo_odds_df=combo_df,
    )

    # メッセージ2: 推奨馬券リスト
    msg2_text = _format_bet_recommendations(
        race_date, venue_name, race_number, bets, pattern_str, merged
    )

    return [
        {"type": "text", "text": msg1_text},
        {"type": "text", "text": msg2_text},
    ]


# --- Webhook イベント処理 -------------------------------------------------------

def process_webhook_events(
    events: list[dict[str, Any]],
    project_id: str,
    channel_access_token: str,
) -> None:
    """
    LINE Webhook イベントリストを処理してリプライを送信する

    「日付 競馬場名 レース番号」形式のテキストメッセージのみ返答する。

    Args:
        events: Webhook の events 配列
        project_id: GCP プロジェクト ID
        channel_access_token: LINE チャネルアクセストークン
    """
    from src.utils.line_notify import reply_messages

    for event in events:
        if event.get("type") != "message":
            continue
        message = event.get("message", {})
        if message.get("type") != "text":
            continue

        text = message.get("text", "").strip()
        reply_token = event.get("replyToken", "")

        messages = handle_race_query(text, project_id)
        if messages is None:
            # 「日付 競馬場名 レース番号」形式以外は無視
            logger.debug(f"レースクエリ以外のメッセージを無視: {text!r}")
            continue

        try:
            reply_messages(channel_access_token, reply_token, messages)
        except Exception as e:
            logger.error(f"LINE リプライ送信失敗: {e}", exc_info=True)


# --- 日次プッシュ通知（土日限定）--------------------------------------------------

def _fetch_investment_decisions(
    project_id: str,
    target_date: date,
) -> list[dict[str, Any]]:
    """
    predictions.investment_decisions から当日の投資判断を取得する。

    推奨馬券が存在するレースを競馬場コード順・レース番号順で全件返す。

    Args:
        project_id: GCP プロジェクト ID
        target_date: 対象日
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT
      venue_code,
      race_number,
      race_pattern,
      horse_numbers,
      horse_names,
      bet_type,
      bet_amount,
      win_place_prob,
      place_odds,
      expected_return
    FROM `{project_id}.predictions.investment_decisions`
    WHERE race_date = '{target_date.isoformat()}'
    ORDER BY venue_code ASC, race_number ASC, bet_type, horse_numbers
    """
    rows = client.query(query).result()
    return [dict(row) for row in rows]


def _format_single_bet_line(bet: dict[str, Any]) -> str:
    """
    1馬券分の表示行を生成する。

    単複（単一馬番）は複勝率・期待値を表示し、
    マルチ馬券（ワイド/馬連/三連複）は馬番と馬名のみを表示する。

    Args:
        bet: investment_decisions の1行（horse_numbers はカンマ区切り文字列）

    Returns:
        表示行テキスト（複数行になる場合あり）
    """
    bet_type = str(bet.get("bet_type", ""))
    bet_label = BET_TYPE_LABELS.get(bet_type, bet_type)
    odds = float(bet.get("place_odds", 0) or 0)
    amount = float(bet.get("bet_amount", 0) or 0)

    # horse_numbers / horse_names をパース（カンマ区切り文字列）
    hn_str = str(bet.get("horse_numbers", "") or "")
    nm_str = str(bet.get("horse_names", "") or "")
    horse_numbers = [h.strip() for h in hn_str.split(",") if h.strip()]
    horse_names = [n.strip() for n in nm_str.split(",") if n.strip()]

    # 馬番+馬名のペアを組み立て
    horse_parts = []
    for i, hn in enumerate(horse_numbers):
        name = horse_names[i] if i < len(horse_names) else ""
        horse_parts.append(f"馬番{hn} {name}".strip())
    horse_str = " / ".join(horse_parts)

    if len(horse_numbers) == 1:
        prob = float(bet.get("win_place_prob", 0) or 0) * 100
        # 単勝(win): win_prob が存在しないため win_place_prob(複勝率)を参考表示。期待値は非表示
        prob_label = "複勝率(参考)" if bet_type == "win" else "複勝率"
        prob_line = f"    {prob_label}:{prob:.1f}% / オッズ:{odds:.1f}倍"
        if bet_type != "win":
            ev = float(bet.get("expected_return", 0) or 0)
            prob_line += f" / 期待値:{ev:.2f}"
        return (
            f"  [{bet_label}] {horse_str}\n"
            f"{prob_line}\n"
            f"    推奨: ¥{amount:,.0f}"
        )
    else:
        # マルチ馬券: 馬番・馬名・オッズのみ
        return (
            f"  [{bet_label}] {horse_str}\n"
            f"    オッズ:{odds:.1f}倍 / 推奨: ¥{amount:,.0f}"
        )


_RACES_PER_MESSAGE = 10


def format_push_notification(
    target_date: date,
    decisions: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """
    投資判断リストから LINE プッシュ通知用のメッセージを生成する。

    全馬券種（複勝/ワイド/三連複/単勝/馬連）に対応し、
    1馬券を1行のデータとして扱う。
    競馬場コード順・レース番号順でソートし、
    10レースごとに1メッセージに分割する。
    11レース以上の場合は複数メッセージを返す（LINE 5件制限は呼び出し側で対応）。

    Args:
        target_date: 対象日
        decisions: investment_decisions の行リスト（1行/馬券形式）

    Returns:
        LINE メッセージオブジェクトのリスト（先頭がヘッダー）
    """
    if not decisions:
        return [{"type": "text", "text": f"🏇 {target_date.isoformat()} の推奨馬券はありません。"}]

    # レースごとにグループ化しながら総投資額も計算（1パスで処理）
    races: dict[tuple[str, int], list[dict]] = {}
    total_bet = 0.0
    for d in decisions:
        key = (str(d.get("venue_code", "")), int(d.get("race_number", 0)))
        races.setdefault(key, []).append(d)
        total_bet += float(d.get("bet_amount", 0) or 0)

    # ヘッダーメッセージ
    header = (
        f"🏇 本日の推奨馬券 ({target_date.isoformat()})\n"
        f"レース数: {len(races)}  総投資額: ¥{total_bet:,.0f}"
    )
    messages: list[dict[str, str]] = [{"type": "text", "text": header}]

    # 競馬場コード順・レース番号順でレースブロックを生成
    sorted_races = sorted(races.items(), key=lambda x: x[0])
    race_blocks: list[str] = []
    for (venue_code, race_number), bets in sorted_races:
        venue_name = VENUE_CODE_TO_NAME.get(venue_code, venue_code)
        pattern_label = PATTERN_LABELS.get(str(bets[0].get("race_pattern", "")), "")
        lines = [f"【{venue_name} {race_number}R】{pattern_label}"]
        for bet in bets:
            lines.append(_format_single_bet_line(bet))
        race_blocks.append("\n".join(lines))

    # 10レースごとに1メッセージに分割
    for i in range(0, len(race_blocks), _RACES_PER_MESSAGE):
        chunk = race_blocks[i:i + _RACES_PER_MESSAGE]
        messages.append({"type": "text", "text": "\n\n".join(chunk)})

    return messages


def is_weekend_in_jst(target_date: date) -> bool:
    """土曜（weekday=5）または日曜（weekday=6）か判定する"""
    return target_date.weekday() in (5, 6)


def send_daily_push_notification(
    project_id: str,
    channel_access_token: str,
    user_id: str,
    target_date: date,
) -> dict[str, Any]:
    """
    当日が土日の場合のみ、投資判断をLINEプッシュ通知で送信する。

    Args:
        project_id: GCP プロジェクト ID
        channel_access_token: LINE チャネルアクセストークン
        user_id: 送信先 LINE ユーザー ID
        target_date: 対象日

    Returns:
        {"status": "sent"|"skipped"|"no_decisions", "messages_sent": int}
    """
    from src.utils.line_notify import push_messages, _MAX_MESSAGES_PER_CALL

    if not is_weekend_in_jst(target_date):
        logger.info(f"平日のためLINE通知をスキップ: {target_date} (weekday={target_date.weekday()})")
        return {"status": "skipped", "messages_sent": 0}

    decisions = _fetch_investment_decisions(project_id, target_date)
    if not decisions:
        logger.info(f"投資判断なし: {target_date}")
        return {"status": "no_decisions", "messages_sent": 0}

    messages = format_push_notification(target_date, decisions)
    # LINE API の上限に合わせて分割して送信
    for i in range(0, len(messages), _MAX_MESSAGES_PER_CALL):
        push_messages(channel_access_token, user_id, messages[i:i + _MAX_MESSAGES_PER_CALL])
    logger.info(f"LINE プッシュ通知送信完了: {len(messages)}件")
    return {"status": "sent", "messages_sent": len(messages)}
