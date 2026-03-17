"""
投資戦略モジュール

レースの複勝率分布パターンを分析し、パターンに応じた最適な投資戦略を選定する。

パターン分類:
  - one_dominant（突出型）: top1 確率が top2 を大きく上回る → 単複一点買い + 馬連
  - standard（標準型）: それ以外 → 期待回収率フィルタによる複勝選定
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from itertools import combinations

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class RacePattern:
    """
    レースの複勝率分布パターン

    Attributes:
        pattern: パターン名 ('one_dominant' | 'standard')
        top1_prob: 1位馬の複勝率
        top2_prob: 2位馬の複勝率
        top3_prob: 3位馬の複勝率
        gap_top1_top2: top1 と top2 の複勝率差
        gap_top1_top3: top1 と top3 の複勝率差
    """

    pattern: str
    top1_prob: float
    top2_prob: float
    top3_prob: float
    gap_top1_top2: float
    gap_top1_top3: float


def classify_race_pattern(
    probs: list[float],
    p1: float = 0.2,
) -> RacePattern:
    """
    複勝率リストからレースパターンを分類する

    判定ロジック:
      1. top1 - top2 > p1 → 'one_dominant'（突出型）
      2. それ以外         → 'standard'（標準型）

    Args:
        probs: 各馬の予測複勝率（降順ソート済みを期待するが、関数内でソートする）
        p1: 突出型の判定閾値（top1 と top2 の差がこれを超えると突出型）

    Returns:
        RacePattern インスタンス

    Raises:
        ValueError: probs が 3 頭未満の場合
    """
    if len(probs) < 3:
        raise ValueError(
            f"probs は最低 3 要素必要です（現在: {len(probs)} 要素）"
        )

    # 降順ソート（ソート済みでない場合にも対応）
    sorted_probs = sorted(probs, reverse=True)

    top1 = sorted_probs[0]
    top2 = sorted_probs[1]
    top3 = sorted_probs[2]
    gap_12 = top1 - top2
    gap_13 = top1 - top3

    # パターン判定
    if gap_12 > p1:
        pattern = "one_dominant"
    else:
        pattern = "standard"

    logger.debug(
        f"パターン分類: top1={top1:.3f} top2={top2:.3f} top3={top3:.3f}"
        f" gap_12={gap_12:.3f} → {pattern}"
    )

    return RacePattern(
        pattern=pattern,
        top1_prob=top1,
        top2_prob=top2,
        top3_prob=top3,
        gap_top1_top2=gap_12,
        gap_top1_top3=gap_13,
    )


def _allocate_bets(
    selected_bets: list[dict],
    capital: float,
    max_bet_ratio: float,
    min_bet_amount: float,
) -> list[dict]:
    """
    選定済みbet候補にオッズ逆数比率で賭け金を配分する

    総予算 = capital × max_bet_ratio を各betのオッズ逆数の比率で割り当てる。
    100円単位に切り捨て後、min_bet_amount 未満のbetは除外する。

    Args:
        selected_bets: betの候補リスト。各要素は少なくとも 'odds' キーを持つ dict
        capital: 現在の資金 (円)
        max_bet_ratio: 1レースあたりの最大賭け金比率
        min_bet_amount: 最低賭け金 (円)

    Returns:
        bet_amount が付与され、min_bet_amount 以上のbetのみを含むリスト
    """
    if not selected_bets:
        return []

    total_budget = capital * max_bet_ratio

    # オッズ逆数を計算
    inv_odds = []
    for bet in selected_bets:
        odds = float(bet.get("odds", 1.0))
        inv_odds.append(1.0 / max(odds, 0.01))

    total_inv = sum(inv_odds)
    if total_inv <= 0:
        return []

    result = []
    for bet, inv in zip(selected_bets, inv_odds):
        ratio = inv / total_inv
        raw_amount = total_budget * ratio
        # 100円単位切り捨て
        bet_amount = float(np.floor(raw_amount / 100.0) * 100.0)
        if bet_amount < min_bet_amount:
            continue
        bet_copy = dict(bet)
        bet_copy["bet_amount"] = bet_amount
        result.append(bet_copy)

    return result


def select_base_bets(
    race_df: pd.DataFrame,
    combo_odds_df: pd.DataFrame | None,
    expected_return_threshold: float,
    top_n: int = 5,
    min_prob_threshold: float = 0.0,
    prob_weight_r: float = 1.0,
) -> list[dict]:
    """
    複勝/ワイド/三連複の候補を選定する（配分前）

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds（複勝オッズ）
        combo_odds_df: コンボオッズ DataFrame
            カラム: bet_type, horse_number_1, horse_number_2, horse_number_3, odds_value
            None または空の場合は複勝のみ選定
        expected_return_threshold: 期待回収率の最低閾値
        top_n: 組み合わせ候補とする上位馬数
        min_prob_threshold: 軸馬の最低複勝率。これ未満の馬は軸馬（複勝単体買い）から除外
        prob_weight_r: 選定スコアの確率ウェイト係数。スコア = odds * prob^r
            r=1 で通常の期待値と同等。r>1 で高確率馬が有利になる

    Returns:
        bet dict のリスト（bet_amount は未設定）
    """
    if len(race_df) == 0:
        return []

    # 選定スコア (odds * prob^r) 降順でソート
    df_sorted = race_df.copy()
    df_sorted["_selection_score"] = (
        df_sorted["odds"] * df_sorted["win_place_prob"].pow(prob_weight_r)
    )
    sorted_df = df_sorted.sort_values("_selection_score", ascending=False).drop(
        columns=["_selection_score"]
    )

    # 複勝候補選定（min_prob_threshold 以上の馬のみ軸馬として選定）
    place_bets = []
    for _, row in sorted_df.iterrows():
        prob = float(row["win_place_prob"])
        place_odds = float(row["odds"])
        if prob < min_prob_threshold:
            continue
        if prob * place_odds > expected_return_threshold:
            place_bets.append({
                "bet_type": "place",
                "horse_numbers": [int(row["horse_number"])],
                "horse_id": str(row["horse_id"]),
                "odds": place_odds,
            })

    # top_n頭候補（流し馬券の相手馬は min_prob_threshold フィルタ対象外）
    top_candidates = sorted_df.head(top_n)
    top_horse_numbers = top_candidates["horse_number"].tolist()
    top_prob_map = dict(zip(
        top_candidates["horse_number"].tolist(),
        top_candidates["win_place_prob"].tolist(),
    ))

    combo_bets = []

    # combo_odds_dfがある場合のみワイド/三連複を選定
    has_combo = (
        combo_odds_df is not None
        and isinstance(combo_odds_df, pd.DataFrame)
        and len(combo_odds_df) > 0
    )

    if has_combo:
        # ワイド
        wide_df = combo_odds_df[combo_odds_df["bet_type"] == "wide"]
        for _, row in wide_df.iterrows():
            h1 = int(row["horse_number_1"])
            h2 = int(row["horse_number_2"])
            if h1 not in top_horse_numbers or h2 not in top_horse_numbers:
                continue
            prob_i = float(top_prob_map.get(h1, 0))
            prob_j = float(top_prob_map.get(h2, 0))
            wide_odds = float(row["odds_value"])
            if prob_i * prob_j * wide_odds > expected_return_threshold:
                combo_bets.append({
                    "bet_type": "wide",
                    "horse_numbers": sorted([h1, h2]),
                    "horse_id": None,
                    "odds": wide_odds,
                })

        # 三連複
        san_df = combo_odds_df[combo_odds_df["bet_type"] == "sanrenpuku"]
        for _, row in san_df.iterrows():
            h1 = int(row["horse_number_1"])
            h2 = int(row["horse_number_2"])
            h3_raw = row.get("horse_number_3", None)
            if pd.isna(h3_raw):
                continue
            h3 = int(h3_raw)
            if h1 not in top_horse_numbers or h2 not in top_horse_numbers or h3 not in top_horse_numbers:
                continue
            prob_i = float(top_prob_map.get(h1, 0))
            prob_j = float(top_prob_map.get(h2, 0))
            prob_k = float(top_prob_map.get(h3, 0))
            san_odds = float(row["odds_value"])
            if prob_i * prob_j * prob_k * san_odds > expected_return_threshold:
                combo_bets.append({
                    "bet_type": "sanrenpuku",
                    "horse_numbers": sorted([h1, h2, h3]),
                    "horse_id": None,
                    "odds": san_odds,
                })

    return place_bets + combo_bets


def select_pattern_a_extra_bets(
    race_df: pd.DataFrame,
    combo_odds_df: pd.DataFrame | None,
    expected_return_threshold: float,
    top_n: int = 5,
) -> list[dict]:
    """
    one_dominant 時の追加ベット（単勝 + 馬連）を選定する

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds
            オプション: win_odds（単勝オッズ）
        combo_odds_df: コンボオッズ DataFrame
        expected_return_threshold: 期待回収率の最低閾値
        top_n: 馬連の相手候補とする上位馬数

    Returns:
        bet dict のリスト（bet_amount は未設定）
    """
    if len(race_df) == 0:
        return []

    sorted_df = race_df.sort_values("win_place_prob", ascending=False)
    top1_row = sorted_df.iloc[0]
    top1_prob = float(top1_row["win_place_prob"])
    top1_horse_number = int(top1_row["horse_number"])
    top1_horse_id = str(top1_row["horse_id"])

    extra_bets = []

    # 単勝
    if "win_odds" in race_df.columns:
        win_odds_val = top1_row.get("win_odds", None)
        if win_odds_val is not None and not pd.isna(win_odds_val):
            win_odds = float(win_odds_val)
            if top1_prob * win_odds > expected_return_threshold:
                extra_bets.append({
                    "bet_type": "win",
                    "horse_numbers": [top1_horse_number],
                    "horse_id": top1_horse_id,
                    "odds": win_odds,
                })

    # 馬連
    has_combo = (
        combo_odds_df is not None
        and isinstance(combo_odds_df, pd.DataFrame)
        and len(combo_odds_df) > 0
    )
    if has_combo:
        top_candidates = sorted_df.head(top_n)
        top_horse_numbers = top_candidates["horse_number"].tolist()
        top_prob_map = dict(zip(
            top_candidates["horse_number"].tolist(),
            top_candidates["win_place_prob"].tolist(),
        ))

        umaren_df = combo_odds_df[combo_odds_df["bet_type"] == "umaren"]
        for _, row in umaren_df.iterrows():
            h1 = int(row["horse_number_1"])
            h2 = int(row["horse_number_2"])
            # top1軸が含まれているか
            if top1_horse_number not in (h1, h2):
                continue
            # 相手馬がtop_n候補内か
            other = h2 if h1 == top1_horse_number else h1
            if other not in top_horse_numbers:
                continue
            prob_other = float(top_prob_map.get(other, 0))
            umaren_odds = float(row["odds_value"])
            if top1_prob * prob_other * umaren_odds > expected_return_threshold:
                extra_bets.append({
                    "bet_type": "umaren",
                    "horse_numbers": sorted([top1_horse_number, other]),
                    "horse_id": None,
                    "odds": umaren_odds,
                })

    return extra_bets


def select_bets_for_race(
    race_df: pd.DataFrame,
    combo_odds_df: pd.DataFrame | None = None,
    capital: float = 100_000.0,
    p1: float = 0.2,
    expected_return_threshold: float = 1.2,
    max_bet_ratio: float = 0.05,
    min_bet_amount: float = 100.0,
    top_n: int = 5,
    min_prob_threshold: float = 0.0,
    prob_weight_r: float = 1.0,
) -> tuple[list[dict], RacePattern]:
    """
    レースパターンを判定し、最適な投資戦略で賭けを選定する（統合関数）

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds
        combo_odds_df: コンボオッズ DataFrame（None の場合は複勝のみ）
        capital: 現在の資金 (円)
        p1: 突出型の判定閾値（top1 と top2 の複勝率差）
        expected_return_threshold: 期待回収率閾値
        max_bet_ratio: 1レースあたりの最大賭け金比率
        min_bet_amount: 最低賭け金 (円)
        top_n: ワイド/三連複/馬連の候補数
        min_prob_threshold: 軸馬の最低複勝率（複勝単体買いの最低条件）
        prob_weight_r: 選定スコアの確率ウェイト係数（odds * prob^r）

    Returns:
        (bets, pattern) のタプル:
          - bets: 賭け選定リスト
          - pattern: RacePattern インスタンス

    Raises:
        ValueError: race_df が 3 頭未満の場合
    """
    if len(race_df) < 3:
        raise ValueError(
            f"race_df は最低 3 頭必要です（現在: {len(race_df)} 頭）"
        )

    # combo_odds_dfがNoneの場合は空DataFrameとして扱う
    if combo_odds_df is None:
        combo_odds_df = pd.DataFrame()

    # NaN オッズを除外
    valid_df = race_df.dropna(subset=["win_place_prob", "odds"]).copy()
    valid_df = valid_df[pd.to_numeric(valid_df["odds"], errors="coerce") > 0]

    if len(valid_df) < 3:
        probs_for_pattern = sorted(
            race_df["win_place_prob"].dropna().tolist(), reverse=True
        )
        if len(probs_for_pattern) < 3:
            raise ValueError(
                f"有効な複勝率データが 3 頭未満です（現在: {len(probs_for_pattern)} 頭）"
            )
    else:
        probs_for_pattern = valid_df["win_place_prob"].tolist()

    # パターン分類
    race_pattern = classify_race_pattern(probs_for_pattern, p1=p1)

    logger.debug(
        f"レースパターン: {race_pattern.pattern}"
        f" (top1={race_pattern.top1_prob:.3f}, top2={race_pattern.top2_prob:.3f},"
        f" top3={race_pattern.top3_prob:.3f})"
    )

    # ベースベット選定
    base_bets = select_base_bets(
        race_df=valid_df,
        combo_odds_df=combo_odds_df,
        expected_return_threshold=expected_return_threshold,
        top_n=top_n,
        min_prob_threshold=min_prob_threshold,
        prob_weight_r=prob_weight_r,
    )

    # one_dominant ならパターンA追加ベット
    extra_bets = []
    if race_pattern.pattern == "one_dominant":
        extra_bets = select_pattern_a_extra_bets(
            race_df=valid_df,
            combo_odds_df=combo_odds_df,
            expected_return_threshold=expected_return_threshold,
            top_n=top_n,
        )

    all_bets = base_bets + extra_bets

    # 賭け金配分
    bets = _allocate_bets(
        selected_bets=all_bets,
        capital=capital,
        max_bet_ratio=max_bet_ratio,
        min_bet_amount=min_bet_amount,
    )

    return bets, race_pattern
