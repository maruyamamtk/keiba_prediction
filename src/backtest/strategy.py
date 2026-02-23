"""
投資戦略モジュール

レースの複勝率分布パターンを分析し、パターンに応じた最適な投資戦略を選定する。

パターン分類:
  - one_dominant（突出型）: top1 確率が top2 を大きく上回る → 単複一点買い、Kelly最大化
  - competitive（拮抗型）: top1〜top3 が接近している → 複勝複数買い、穴狙い
  - standard（標準型）: 中間 → 期待回収率フィルタによる選定
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .simulator import fractional_kelly

logger = logging.getLogger(__name__)


def _calc_bet_amount(
    capital: float,
    kf: float,
    max_bet_ratio: float,
    min_bet_amount: float,
) -> float:
    """
    賭け金を計算する共通ヘルパー（100円単位切り捨て・上限適用）

    Args:
        capital: 現在の資金 (円)
        kf: Fractional Kelly の賭け金比率
        max_bet_ratio: 1レースあたりの最大賭け金比率
        min_bet_amount: 最低賭け金 (円)

    Returns:
        賭け金 (円)
    """
    bet_amount = capital * kf
    bet_amount = min(bet_amount, capital * max_bet_ratio)
    bet_amount = np.floor(bet_amount / 100.0) * 100.0
    return max(min_bet_amount, bet_amount)


@dataclass
class RacePattern:
    """
    レースの複勝率分布パターン

    Attributes:
        pattern: パターン名 ('one_dominant' | 'competitive' | 'standard')
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
    p2: float = 0.15,
) -> RacePattern:
    """
    複勝率リストからレースパターンを分類する

    判定ロジック（優先順位順）:
      1. top1 - top2 > p1 → 'one_dominant'（突出型）
      2. top1 - top3 < p2 → 'competitive'（拮抗型）
      3. それ以外         → 'standard'（標準型）

    Args:
        probs: 各馬の予測複勝率（降順ソート済みを期待するが、関数内でソートする）
        p1: 突出型の判定閾値（top1 と top2 の差がこれを超えると突出型）
        p2: 拮抗型の判定閾値（top1 と top3 の差がこれ未満だと拮抗型）

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
    elif gap_13 < p2:
        pattern = "competitive"
    else:
        pattern = "standard"

    logger.debug(
        f"パターン分類: top1={top1:.3f} top2={top2:.3f} top3={top3:.3f}"
        f" gap_12={gap_12:.3f} gap_13={gap_13:.3f} → {pattern}"
    )

    return RacePattern(
        pattern=pattern,
        top1_prob=top1,
        top2_prob=top2,
        top3_prob=top3,
        gap_top1_top2=gap_12,
        gap_top1_top3=gap_13,
    )


def select_bets_one_dominant(
    race_df: pd.DataFrame,
    capital: float,
    kelly_fraction: float = 0.25,
    max_bet_ratio: float = 0.05,
    min_bet_amount: float = 100.0,
) -> list[dict]:
    """
    突出型レースの賭け選定（単複一点買い、Kelly最大化）

    最も複勝率が高い馬 1 頭を選定し、Fractional Kelly で賭け金を計算する。
    Kelly 値が正の場合のみ賭け対象とする。

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds
        capital: 現在の資金 (円)
        kelly_fraction: Fractional Kelly の係数
        max_bet_ratio: 1レースあたりの最大賭け金比率
        min_bet_amount: 最低賭け金 (円)

    Returns:
        賭け選定リスト。各要素は dict:
            {"horse_id", "horse_number", "bet_amount", "odds"}
        賭け対象なしの場合は空リストを返す
    """
    if len(race_df) == 0:
        return []

    # 複勝率最上位の馬を選定
    sorted_df = race_df.sort_values("win_place_prob", ascending=False)
    top_row = sorted_df.iloc[0]

    win_prob = float(top_row["win_place_prob"])
    odds = float(top_row["odds"])

    # Kelly 値が 0 以下（期待値マイナス）なら賭けない
    kf = fractional_kelly(win_prob, odds, kelly_fraction)
    if kf <= 0:
        logger.debug(
            f"突出型: 馬番{top_row['horse_number']} Kelly={kf:.4f} ≤ 0 → スキップ"
        )
        return []

    # 賭け金計算（100円単位切り捨て・上限チェック）
    bet_amount = _calc_bet_amount(capital, kf, max_bet_ratio, min_bet_amount)

    if bet_amount > capital:
        logger.debug(f"突出型: 賭け金 {bet_amount:.0f}円 が残高 {capital:.0f}円 を超過 → スキップ")
        return []

    return [
        {
            "horse_id": str(top_row["horse_id"]),
            "horse_number": int(top_row["horse_number"]),
            "bet_amount": bet_amount,
            "odds": odds,
        }
    ]


def select_bets_competitive(
    race_df: pd.DataFrame,
    capital: float,
    top_n: int = 3,
    expected_return_threshold: float = 1.0,
    kelly_fraction: float = 0.25,
    max_bet_ratio: float = 0.05,
    min_bet_amount: float = 100.0,
) -> list[dict]:
    """
    拮抗型レースの賭け選定（複勝複数買い・穴狙い）

    複勝率上位 top_n 頭の中から、期待回収率（複勝率 × オッズ）が
    expected_return_threshold を超える馬を選定する。
    拮抗型では閾値を低め（デフォルト 1.0）に設定し、複数馬を購入する。

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds
        capital: 現在の資金 (円)
        top_n: 複勝率上位から選定する候補数
        expected_return_threshold: 期待回収率の最低閾値
        kelly_fraction: Fractional Kelly の係数
        max_bet_ratio: 1レースあたりの最大賭け金比率
        min_bet_amount: 最低賭け金 (円)

    Returns:
        賭け選定リスト。各要素は dict:
            {"horse_id", "horse_number", "bet_amount", "odds"}
        賭け対象なしの場合は空リストを返す
    """
    if len(race_df) == 0:
        return []

    # 複勝率上位 top_n 頭を候補にする
    candidates = race_df.sort_values("win_place_prob", ascending=False).head(top_n)

    bets = []
    for _, row in candidates.iterrows():
        win_prob = float(row["win_place_prob"])
        odds = float(row["odds"])
        expected_return = win_prob * odds

        # 期待回収率フィルタ
        if expected_return <= expected_return_threshold:
            continue

        kf = fractional_kelly(win_prob, odds, kelly_fraction)
        if kf <= 0:
            continue

        # 賭け金計算（100円単位切り捨て・上限チェック）
        bet_amount = _calc_bet_amount(capital, kf, max_bet_ratio, min_bet_amount)

        if bet_amount > capital:
            logger.debug(
                f"拮抗型: 馬番{row['horse_number']} 賭け金 {bet_amount:.0f}円"
                f" が残高 {capital:.0f}円 を超過 → スキップ"
            )
            continue

        bets.append(
            {
                "horse_id": str(row["horse_id"]),
                "horse_number": int(row["horse_number"]),
                "bet_amount": bet_amount,
                "odds": odds,
            }
        )

    return bets


def select_bets_standard(
    race_df: pd.DataFrame,
    capital: float,
    expected_return_threshold: float = 1.2,
    kelly_fraction: float = 0.25,
    max_bet_ratio: float = 0.05,
    min_bet_amount: float = 100.0,
) -> list[dict]:
    """
    標準型レースの賭け選定（期待回収率フィルタ）

    全馬から期待回収率（複勝率 × オッズ）が expected_return_threshold を超える
    馬を選定し、Fractional Kelly で賭け金を計算する。

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds
        capital: 現在の資金 (円)
        expected_return_threshold: 期待回収率の最低閾値（デフォルト: 1.2）
        kelly_fraction: Fractional Kelly の係数
        max_bet_ratio: 1レースあたりの最大賭け金比率
        min_bet_amount: 最低賭け金 (円)

    Returns:
        賭け選定リスト。各要素は dict:
            {"horse_id", "horse_number", "bet_amount", "odds"}
        賭け対象なしの場合は空リストを返す
    """
    if len(race_df) == 0:
        return []

    bets = []
    for _, row in race_df.iterrows():
        win_prob = float(row["win_place_prob"])
        odds = float(row["odds"])
        expected_return = win_prob * odds

        # 期待回収率フィルタ
        if expected_return <= expected_return_threshold:
            continue

        kf = fractional_kelly(win_prob, odds, kelly_fraction)
        if kf <= 0:
            continue

        # 賭け金計算（100円単位切り捨て・上限チェック）
        bet_amount = _calc_bet_amount(capital, kf, max_bet_ratio, min_bet_amount)

        if bet_amount > capital:
            logger.debug(
                f"標準型: 馬番{row['horse_number']} 賭け金 {bet_amount:.0f}円"
                f" が残高 {capital:.0f}円 を超過 → スキップ"
            )
            continue

        bets.append(
            {
                "horse_id": str(row["horse_id"]),
                "horse_number": int(row["horse_number"]),
                "bet_amount": bet_amount,
                "odds": odds,
            }
        )

    return bets


def select_bets_for_race(
    race_df: pd.DataFrame,
    capital: float,
    p1: float = 0.2,
    p2: float = 0.15,
    expected_return_threshold: float = 1.2,
    competitive_threshold_offset: float = 0.2,
    kelly_fraction: float = 0.25,
    max_bet_ratio: float = 0.05,
    min_bet_amount: float = 100.0,
) -> tuple[list[dict], RacePattern]:
    """
    レースパターンを判定し、最適な投資戦略で賭けを選定する（統合関数）

    内部で classify_race_pattern を呼び出してパターンを判定し、
    パターンに応じた select_bets_* 関数を実行する。

    パターンと戦略の対応:
      - one_dominant: select_bets_one_dominant（単複一点買い）
      - competitive:  select_bets_competitive（複勝複数買い、閾値を低め設定）
      - standard:     select_bets_standard（期待回収率フィルタ）

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds
        capital: 現在の資金 (円)
        p1: 突出型の判定閾値（top1 と top2 の複勝率差）
        p2: 拮抗型の判定閾値（top1 と top3 の複勝率差）
        expected_return_threshold: 標準型・拮抗型の期待回収率閾値
        competitive_threshold_offset: 拮抗型で期待回収率閾値を下げる幅
            `max(1.0, expected_return_threshold - competitive_threshold_offset)` が
            拮抗型の実際の閾値となる（デフォルト: 0.2）
        kelly_fraction: Fractional Kelly の係数
        max_bet_ratio: 1レースあたりの最大賭け金比率
        min_bet_amount: 最低賭け金 (円)

    Returns:
        (bets, pattern) のタプル:
          - bets: 賭け選定リスト（空リストの場合は賭け対象なし）
          - pattern: RacePattern インスタンス

    Raises:
        ValueError: race_df が 3 頭未満の場合
    """
    if len(race_df) < 3:
        raise ValueError(
            f"race_df は最低 3 頭必要です（現在: {len(race_df)} 頭）"
        )

    # NaN オッズを除外した上でパターン判定（NaN があっても複勝率でパターン判定は可能）
    valid_df = race_df.dropna(subset=["win_place_prob", "odds"]).copy()
    valid_df = valid_df[valid_df["odds"] > 0]

    if len(valid_df) < 3:
        # 有効データが 3 頭未満の場合はパターン判定不可 → 標準型として扱う
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
    race_pattern = classify_race_pattern(probs_for_pattern, p1=p1, p2=p2)

    logger.debug(
        f"レースパターン: {race_pattern.pattern}"
        f" (top1={race_pattern.top1_prob:.3f}, top2={race_pattern.top2_prob:.3f},"
        f" top3={race_pattern.top3_prob:.3f})"
    )

    # パターンに応じた賭け選定
    if race_pattern.pattern == "one_dominant":
        bets = select_bets_one_dominant(
            race_df=valid_df,
            capital=capital,
            kelly_fraction=kelly_fraction,
            max_bet_ratio=max_bet_ratio,
            min_bet_amount=min_bet_amount,
        )
    elif race_pattern.pattern == "competitive":
        # 拮抗型は期待回収率閾値を低めに設定（標準型の閾値より下げる）
        competitive_threshold = max(1.0, expected_return_threshold - competitive_threshold_offset)
        bets = select_bets_competitive(
            race_df=valid_df,
            capital=capital,
            top_n=3,
            expected_return_threshold=competitive_threshold,
            kelly_fraction=kelly_fraction,
            max_bet_ratio=max_bet_ratio,
            min_bet_amount=min_bet_amount,
        )
    else:
        # standard
        bets = select_bets_standard(
            race_df=valid_df,
            capital=capital,
            expected_return_threshold=expected_return_threshold,
            kelly_fraction=kelly_fraction,
            max_bet_ratio=max_bet_ratio,
            min_bet_amount=min_bet_amount,
        )

    return bets, race_pattern
