"""
投資戦略モジュール

レースの複勝率分布パターンを分析し、パターンに応じた最適な投資戦略を選定する。

パターン分類:
  - one_dominant（突出型）: 複勝率分布のジニ係数 > p1 → 単複一点買い + 馬連
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
        gini_coefficient: 複勝率分布のジニ係数（0=均等, 1=集中）
    """

    pattern: str
    top1_prob: float
    top2_prob: float
    top3_prob: float
    gap_top1_top2: float
    gap_top1_top3: float
    gini_coefficient: float = 0.0


def _gini_coefficient(probs: list[float]) -> float:
    """
    複勝率リストのジニ係数を計算する

    ジニ係数: 0=完全均等分布, 1=完全集中（1頭に全確率）
    離散版の公式を使用:
        G = (2 * Σ(i * x_i)) / (n * Σ x_i) - (n+1)/n
    where x_i は昇順ソート後の値, i は 1-indexed

    Args:
        probs: 各馬の予測複勝率リスト（合計が1になる必要はない）

    Returns:
        ジニ係数（float, 0.0〜1.0）
    """
    n = len(probs)
    if n == 0:
        return 0.0
    sorted_p = sorted(probs)
    total = sum(sorted_p)
    if total == 0:
        return 0.0
    cumsum = sum((i + 1) * p for i, p in enumerate(sorted_p))
    return (2 * cumsum) / (n * total) - (n + 1) / n


def classify_race_pattern(
    probs: list[float],
    p1: float = 0.3,
) -> RacePattern:
    """
    複勝率リストからレースパターンを分類する

    判定ロジック:
      ジニ係数 > p1 → 'one_dominant'（突出型: 1頭に確率が集中）
      それ以外       → 'standard'（標準型: 実力伯仲）

    Args:
        probs: 各馬の予測複勝率（降順ソート済みを期待するが、関数内でソートする）
        p1: 突出型の判定閾値（ジニ係数がこれを超えると突出型）
            値域: 0〜1。小さいほど突出型と判定されやすい。

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

    # ジニ係数計算
    gini = _gini_coefficient(probs)

    # パターン判定: ジニ係数が p1 を超えると突出型
    if gini > p1:
        pattern = "one_dominant"
    else:
        pattern = "standard"

    logger.debug(
        f"パターン分類: top1={top1:.3f} top2={top2:.3f} top3={top3:.3f}"
        f" gini={gini:.3f} → {pattern}"
    )

    return RacePattern(
        pattern=pattern,
        top1_prob=top1,
        top2_prob=top2,
        top3_prob=top3,
        gap_top1_top2=gap_12,
        gap_top1_top3=gap_13,
        gini_coefficient=gini,
    )


def _allocate_bets(
    selected_bets: list[dict],
    budget_per_race: float,
    min_bet_amount: float,
) -> list[dict]:
    """
    選定済みbet候補にオッズ逆数比率で賭け金を配分する

    総予算 = budget_per_race を各betのオッズ逆数の比率で割り当てる。
    100円単位に切り捨て後、min_bet_amount 未満のbetは除外する。
    除外が発生した場合は残ったbetに対して予算を再配分し、
    追加の除外が発生しなくなるまで繰り返す（収束まで反復）。

    Args:
        selected_bets: betの候補リスト。各要素は少なくとも 'odds' キーを持つ dict
        budget_per_race: 1レースあたりの固定予算 (円)
        min_bet_amount: 最低賭け金 (円)

    Returns:
        bet_amount が付与され、min_bet_amount 以上のbetのみを含むリスト
    """
    if not selected_bets:
        return []

    active_bets = list(selected_bets)

    while active_bets:
        inv_odds = [1.0 / max(float(bet.get("odds", 1.0)), 0.01) for bet in active_bets]
        total_inv = sum(inv_odds)
        if total_inv <= 0:
            return []

        kept_bets = []
        for bet, inv in zip(active_bets, inv_odds):
            amt = float(np.floor(budget_per_race * inv / total_inv / 100.0) * 100.0)
            if amt >= min_bet_amount:
                kept_bets.append((bet, amt))

        if len(kept_bets) == len(active_bets):  # 除外なし → 収束
            return [dict(bet) | {"bet_amount": amt} for bet, amt in kept_bets]

        if not kept_bets:
            return []

        active_bets = [bet for bet, _ in kept_bets]

    return []


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
    base_bets: list[dict],
) -> list[dict]:
    """
    one_dominant 時の追加ベット（単勝 + 複勝 + 馬連）を選定する

    Step 3 のロジック（自動購入方式）:
      - 単勝: 複勝率が最も高い馬の単勝を1点のみ自動購入（期待値フィルタなし）
      - 複勝: 単勝対象馬（top1）の複勝を期待値フィルタなしで1点自動購入
      - 馬連: Step2（select_base_bets）で選定したワイドと同じ組み合わせを自動購入

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds
            オプション: win_odds（単勝オッズ。存在する場合のみ単勝選定が有効化）
        combo_odds_df: コンボオッズ DataFrame
        base_bets: select_base_bets() の戻り値。
            ワイドの組み合わせを馬連の購入対象として流用する。

    Returns:
        bet dict のリスト（bet_amount は未設定）
    """
    if len(race_df) == 0:
        return []

    sorted_df = race_df.sort_values("win_place_prob", ascending=False)
    top1_row = sorted_df.iloc[0]
    top1_horse_number = int(top1_row["horse_number"])
    top1_horse_id = str(top1_row["horse_id"])

    extra_bets = []

    # ── 複勝: 軸馬（複勝率1位）を期待値フィルタなしで1点購入 ──
    # base_bets に既に top1 の複勝が含まれている場合は追加しない（重複防止）
    top1_place_already_in_base = any(
        bet["bet_type"] == "place" and bet["horse_numbers"] == [top1_horse_number]
        for bet in base_bets
    )
    if not top1_place_already_in_base:
        place_odds_val = float(top1_row["odds"])
        extra_bets.append({
            "bet_type": "place",
            "horse_numbers": [top1_horse_number],
            "horse_id": top1_horse_id,
            "odds": place_odds_val,
        })

    # ── 単勝: win_odds カラムがあれば軸馬（複勝率1位）を無条件で1点購入 ──
    if "win_odds" in race_df.columns:
        win_odds_val = top1_row.get("win_odds", None)
        if win_odds_val is not None and not pd.isna(win_odds_val):
            extra_bets.append({
                "bet_type": "win",
                "horse_numbers": [top1_horse_number],
                "horse_id": top1_horse_id,
                "odds": float(win_odds_val),
            })

    # ── 馬連: Step2 のワイドと同じ馬番ペアを自動購入 ──
    has_combo = (
        combo_odds_df is not None
        and isinstance(combo_odds_df, pd.DataFrame)
        and len(combo_odds_df) > 0
    )

    if has_combo:
        # base_bets からワイドの組み合わせを抽出
        wide_pairs = [
            tuple(sorted(bet["horse_numbers"]))
            for bet in base_bets
            if bet["bet_type"] == "wide"
        ]

        umaren_df = combo_odds_df[combo_odds_df["bet_type"] == "umaren"]
        for h1, h2 in wide_pairs:
            # ワイドと同じ馬番ペアの馬連を探す
            match = umaren_df[
                (umaren_df["horse_number_1"] == h1) & (umaren_df["horse_number_2"] == h2)
            ]
            if match.empty:
                # horse_number_1/2 の順序が逆の場合も確認
                match = umaren_df[
                    (umaren_df["horse_number_1"] == h2) & (umaren_df["horse_number_2"] == h1)
                ]
            if not match.empty:
                umaren_odds = float(match.iloc[0]["odds_value"])
                extra_bets.append({
                    "bet_type": "umaren",
                    "horse_numbers": sorted([h1, h2]),
                    "horse_id": None,
                    "odds": umaren_odds,
                })

    return extra_bets


def select_bets_for_race(
    race_df: pd.DataFrame,
    combo_odds_df: pd.DataFrame | None = None,
    budget_per_race: float = 3000.0,
    p1: float = 0.3,
    expected_return_threshold: float = 1.2,
    min_bet_amount: float = 100.0,
    top_n: int = 5,
    min_prob_threshold: float = 0.0,
    prob_weight_r: float = 1.0,
    # パターン別パラメータ（指定時は expected_return_threshold / top_n より優先）
    threshold_dominant: float | None = None,
    threshold_standard: float | None = None,
    top_n_dominant: int | None = None,
    top_n_standard: int | None = None,
    # 後方互換性のための旧パラメータ（無視される）
    capital: float | None = None,
    max_bet_ratio: float | None = None,
) -> tuple[list[dict], RacePattern]:
    """
    レースパターンを判定し、最適な投資戦略で賭けを選定する（統合関数）

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds
        combo_odds_df: コンボオッズ DataFrame（None の場合は複勝のみ）
        budget_per_race: 1レースあたりの固定予算 (円)
        p1: 突出型の判定閾値（ジニ係数がこれを超えると one_dominant）
        expected_return_threshold: 期待回収率閾値（両パターン共通のデフォルト値）
        min_bet_amount: 最低賭け金 (円)
        top_n: ワイド/三連複/馬連の候補数（両パターン共通のデフォルト値）
        min_prob_threshold: 軸馬の最低複勝率（複勝単体買いの最低条件）
        prob_weight_r: 選定スコアの確率ウェイト係数（odds * prob^r）
        threshold_dominant: 突出型の期待回収率閾値（指定時は expected_return_threshold より優先）
        threshold_standard: 標準型の期待回収率閾値（指定時は expected_return_threshold より優先）
        top_n_dominant: 突出型の候補馬数（指定時は top_n より優先）
        top_n_standard: 標準型の候補馬数（指定時は top_n より優先）

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
        f" top3={race_pattern.top3_prob:.3f}, gini={race_pattern.gini_coefficient:.3f})"
    )

    # パターン別パラメータの解決（指定なしの場合は共通値にフォールバック）
    if race_pattern.pattern == "one_dominant":
        active_threshold = threshold_dominant if threshold_dominant is not None else expected_return_threshold
        active_top_n = top_n_dominant if top_n_dominant is not None else top_n
    else:
        active_threshold = threshold_standard if threshold_standard is not None else expected_return_threshold
        active_top_n = top_n_standard if top_n_standard is not None else top_n

    # ベースベット選定
    base_bets = select_base_bets(
        race_df=valid_df,
        combo_odds_df=combo_odds_df,
        expected_return_threshold=active_threshold,
        top_n=active_top_n,
        min_prob_threshold=min_prob_threshold,
        prob_weight_r=prob_weight_r,
    )

    # one_dominant ならパターンA追加ベット（自動購入方式）
    extra_bets = []
    if race_pattern.pattern == "one_dominant":
        extra_bets = select_pattern_a_extra_bets(
            race_df=valid_df,
            combo_odds_df=combo_odds_df,
            base_bets=base_bets,
        )

    all_bets = base_bets + extra_bets

    # 賭け金配分
    bets = _allocate_bets(
        selected_bets=all_bets,
        budget_per_race=budget_per_race,
        min_bet_amount=min_bet_amount,
    )

    return bets, race_pattern
