"""
投資戦略モジュール

全レースを統一ロジックで処理し、複勝＋ワイド＋三連複＋馬連（ワイドと同組み合わせ）を選定する。
"""



import logging
from itertools import combinations

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


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
    複勝/ワイド/三連複/馬連の候補を選定する（配分前）

    ワイドが選定された組み合わせには必ず馬連を追加する。

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds（複勝オッズ）
        combo_odds_df: コンボオッズ DataFrame
            カラム: bet_type, horse_number_1, horse_number_2, horse_number_3, odds_value
            None または空の場合は複勝のみ選定
        expected_return_threshold: 期待回収率の最低閾値
        top_n: 組み合わせ候補とする上位馬数
        min_prob_threshold: 最低複勝率フィルタ基準（出走頭数18頭換算）。
            実際の比較値: prob × N/18 >= min_prob_threshold（N = 出走頭数）
            少頭数レースでは理論複勝率が高くなるため、頭数で補正した上で基準と比較する。
            複勝単体買いだけでなく、ワイド・三連複の候補馬（top_n）にも同フィルタを適用する。
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
    # 出走頭数補正: prob × N/18 >= min_prob_threshold
    N = len(race_df)
    place_bets = []
    for _, row in sorted_df.iterrows():
        prob = float(row["win_place_prob"])
        place_odds = float(row["odds"])
        if prob * N / 18 < min_prob_threshold:
            continue
        if prob * place_odds > expected_return_threshold:
            place_bets.append({
                "bet_type": "place",
                "horse_numbers": [int(row["horse_number"])],
                "horse_id": str(row["horse_id"]),
                "odds": place_odds,
            })

    # top_n頭候補（min_prob_threshold フィルタ適用済みの馬から上位N頭を選択）
    if min_prob_threshold > 0:
        candidates_df = sorted_df[
            sorted_df["win_place_prob"] * N / 18 >= min_prob_threshold
        ]
    else:
        candidates_df = sorted_df
    top_candidates = candidates_df.head(top_n)
    top_horse_numbers = top_candidates["horse_number"].tolist()
    top_prob_map = dict(zip(
        top_candidates["horse_number"].tolist(),
        top_candidates["win_place_prob"].tolist(),
    ))

    combo_bets = []

    # combo_odds_dfがある場合のみワイド/三連複/馬連を選定
    has_combo = (
        combo_odds_df is not None
        and isinstance(combo_odds_df, pd.DataFrame)
        and len(combo_odds_df) > 0
    )

    if has_combo:
        # ワイド
        wide_df = combo_odds_df[combo_odds_df["bet_type"] == "wide"]
        selected_wide_pairs: list[tuple[int, int]] = []
        for _, row in wide_df.iterrows():
            h1 = int(row["horse_number_1"])
            h2 = int(row["horse_number_2"])
            if h1 not in top_horse_numbers or h2 not in top_horse_numbers:
                continue
            prob_i = float(top_prob_map.get(h1, 0))
            prob_j = float(top_prob_map.get(h2, 0))
            wide_odds = float(row["odds_value"])
            if prob_i * prob_j * wide_odds > expected_return_threshold:
                pair = tuple(sorted([h1, h2]))
                combo_bets.append({
                    "bet_type": "wide",
                    "horse_numbers": list(pair),
                    "horse_id": None,
                    "odds": wide_odds,
                })
                selected_wide_pairs.append(pair)

        # 馬連: ワイドで選定した組み合わせに必ず追加
        if selected_wide_pairs:
            umaren_df = combo_odds_df[combo_odds_df["bet_type"] == "umaren"]
            for h1, h2 in selected_wide_pairs:
                match = umaren_df[
                    (umaren_df["horse_number_1"] == h1) & (umaren_df["horse_number_2"] == h2)
                ]
                if match.empty:
                    match = umaren_df[
                        (umaren_df["horse_number_1"] == h2) & (umaren_df["horse_number_2"] == h1)
                    ]
                if not match.empty:
                    umaren_odds = float(match.iloc[0]["odds_value"])
                    combo_bets.append({
                        "bet_type": "umaren",
                        "horse_numbers": sorted([h1, h2]),
                        "horse_id": None,
                        "odds": umaren_odds,
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


def select_bets_for_race(
    race_df: pd.DataFrame,
    combo_odds_df: pd.DataFrame | None = None,
    budget_per_race: float = 3000.0,
    expected_return_threshold: float = 1.2,
    min_bet_amount: float = 100.0,
    top_n: int = 5,
    min_prob_threshold: float = 0.0,
    prob_weight_r: float = 1.0,
    # 後方互換性のための旧パラメータ（無視される）
    p1: float | None = None,
    top_n_dominant: int | None = None,
    top_n_standard: int | None = None,
    prob_weight_r_dominant: float | None = None,
    prob_weight_r_standard: float | None = None,
    capital: float | None = None,
    max_bet_ratio: float | None = None,
) -> list[dict]:
    """
    全レース統一ロジックで最適な投資戦略を選定する（統合関数）

    複勝＋ワイド＋三連複を選定し、ワイドが選定された組み合わせには馬連を自動追加する。
    パターン分類（突出型/標準型）は廃止し、全レースを同一ロジックで処理する。

    Args:
        race_df: レースの予測データ DataFrame
            必須カラム: horse_id, horse_number, win_place_prob, odds
        combo_odds_df: コンボオッズ DataFrame（None の場合は複勝のみ）
        budget_per_race: 1レースあたりの固定予算 (円)
        expected_return_threshold: 期待回収率閾値
        min_bet_amount: 最低賭け金 (円)
        top_n: ワイド/三連複/馬連の候補数
        min_prob_threshold: 軸馬の最低複勝率（複勝単体買いの最低条件）
        prob_weight_r: 選定スコアの確率ウェイト係数（odds * prob^r）
        p1: 廃止済み（無視される）
        top_n_dominant: 廃止済み（無視される）
        top_n_standard: 廃止済み（無視される）
        prob_weight_r_dominant: 廃止済み（無視される）
        prob_weight_r_standard: 廃止済み（無視される）

    Returns:
        賭け選定リスト（bet_amount 付き）

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
        probs_for_check = race_df["win_place_prob"].dropna().tolist()
        if len(probs_for_check) < 3:
            raise ValueError(
                f"有効な複勝率データが 3 頭未満です（現在: {len(probs_for_check)} 頭）"
            )

    # ベースベット選定（複勝＋ワイド＋三連複＋馬連）
    all_bets = select_base_bets(
        race_df=valid_df,
        combo_odds_df=combo_odds_df,
        expected_return_threshold=expected_return_threshold,
        top_n=top_n,
        min_prob_threshold=min_prob_threshold,
        prob_weight_r=prob_weight_r,
    )

    # 同一 (bet_type, horse_numbers) の重複排除（先着優先）
    seen: set[tuple] = set()
    deduped_bets = []
    for bet in all_bets:
        key = (bet["bet_type"], frozenset(bet["horse_numbers"]))
        if key not in seen:
            seen.add(key)
            deduped_bets.append(bet)

    # 賭け金配分
    return _allocate_bets(
        selected_bets=deduped_bets,
        budget_per_race=budget_per_race,
        min_bet_amount=min_bet_amount,
    )
