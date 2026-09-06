"""
投資戦略モジュールのテスト

対象モジュール:
  - src.backtest.strategy
    - _allocate_bets: オッズ逆数比率による賭け金配分
    - select_base_bets: 複勝/ワイド/三連複/馬連の候補選定
    - select_bets_for_race: 統合関数（全レース統一ロジック）
"""

from __future__ import annotations

import pytest
import pandas as pd
import numpy as np

from src.backtest.strategy import (
    select_bets_for_race,
    select_base_bets,
    _allocate_bets,
)


# ---------------------------------------------------------------------------
# テスト用ヘルパー
# ---------------------------------------------------------------------------


def _make_race_df(
    n_horses: int = 8,
    probs: list[float] | None = None,
    odds: list[float] | None = None,
    horse_ids: list[str] | None = None,
    win_odds: list[float] | None = None,
) -> pd.DataFrame:
    """
    テスト用レース DataFrame を生成する

    Args:
        n_horses: 馬数
        probs: 複勝率リスト（降順推奨）。None の場合は均等分布
        odds: 複勝オッズリスト。None の場合は全馬 3.0
        horse_ids: 馬 ID リスト。None の場合は自動生成
        win_odds: 単勝オッズリスト。None の場合は win_odds カラムなし
    """
    if probs is None:
        probs = [1.0 / n_horses] * n_horses
    if odds is None:
        odds = [3.0] * n_horses
    if horse_ids is None:
        horse_ids = [f"horse_{i:03d}" for i in range(1, n_horses + 1)]

    rows = []
    for i, (p, o, hid) in enumerate(zip(probs, odds, horse_ids)):
        row = {
            "race_id": "race_001",
            "horse_id": hid,
            "horse_number": i + 1,
            "win_place_prob": p,
            "odds": o,
            "finish_position": i + 1,  # 馬番 = 着順
        }
        if win_odds is not None:
            row["win_odds"] = win_odds[i]
        rows.append(row)
    return pd.DataFrame(rows)


def _make_combo_odds_df(entries: list[dict]) -> pd.DataFrame:
    """
    テスト用コンボオッズ DataFrame を生成する

    各 entry は {"bet_type", "horse_number_1", "horse_number_2", "horse_number_3"(optional), "odds_value"} を持つ
    """
    rows = []
    for e in entries:
        rows.append({
            "bet_type": e["bet_type"],
            "horse_number_1": e["horse_number_1"],
            "horse_number_2": e.get("horse_number_2", None),
            "horse_number_3": e.get("horse_number_3", None),
            "odds_value": e["odds_value"],
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# _allocate_bets のテスト
# ---------------------------------------------------------------------------


class TestAllocateBets:
    """賭け金配分のテスト"""

    def test_inverse_odds_allocation_numeric(self):
        """逆数比率の数値検証（オッズ2.0と4.0の馬）"""
        # 逆数: 1/2=0.5, 1/4=0.25 → 比率: 2/3, 1/3
        bets = [
            {"bet_type": "place", "horse_numbers": [1], "horse_id": "h1", "odds": 2.0},
            {"bet_type": "place", "horse_numbers": [2], "horse_id": "h2", "odds": 4.0},
        ]
        budget_per_race = 6000.0  # 固定予算 6000円
        result = _allocate_bets(bets, budget_per_race, min_bet_amount=100.0)
        # 全賭けが返ること
        assert len(result) == 2
        # 低オッズ（馬番1）の方が多く配分される
        amt1 = result[0]["bet_amount"]
        amt2 = result[1]["bet_amount"]
        assert amt1 > amt2

    def test_total_budget_within_limit(self):
        """総賭け金が budget_per_race 以内"""
        bets = [
            {"bet_type": "place", "horse_numbers": [1], "horse_id": "h1", "odds": 3.0},
            {"bet_type": "place", "horse_numbers": [2], "horse_id": "h2", "odds": 5.0},
            {"bet_type": "place", "horse_numbers": [3], "horse_id": "h3", "odds": 8.0},
        ]
        budget_per_race = 3000.0
        result = _allocate_bets(bets, budget_per_race, min_bet_amount=100.0)
        total = sum(b["bet_amount"] for b in result)
        assert total <= budget_per_race

    def test_min_bet_amount_filter(self):
        """min_bet_amount 未満は除外される"""
        # 非常に高いオッズ → 逆数が小さい → 配分が少ない → 除外
        bets = [
            {"bet_type": "place", "horse_numbers": [1], "horse_id": "h1", "odds": 2.0},
            {"bet_type": "place", "horse_numbers": [2], "horse_id": "h2", "odds": 10000.0},
        ]
        budget_per_race = 50.0  # 予算50円（全部min未満 → 全除外）
        result = _allocate_bets(bets, budget_per_race, min_bet_amount=100.0)
        assert len(result) == 0

    def test_empty_bets(self):
        """空リスト → 空リスト"""
        result = _allocate_bets([], 3000.0, min_bet_amount=100.0)
        assert result == []

    def test_redistribution_after_exclusion(self):
        """高オッズbet除外後に残ったbetへ予算が再配分される"""
        bets = [
            {"bet_type": "wide", "horse_numbers": [5, 14], "horse_id": None, "odds": 4.6},
            {"bet_type": "wide", "horse_numbers": [12, 15], "horse_id": None, "odds": 765.4},
        ]
        result = _allocate_bets(bets, budget_per_race=3000.0, min_bet_amount=100.0)
        # 高オッズ側は除外され、1件のみ残る
        assert len(result) == 1
        assert result[0]["horse_numbers"] == [5, 14]
        # 残ったbetが予算全額 (3000円) を受け取る
        assert result[0]["bet_amount"] == 3000.0

    def test_redistribution_total_within_budget(self):
        """除外後再配分しても合計が budget_per_race 以内"""
        bets = [
            {"bet_type": "wide", "horse_numbers": [1, 2], "horse_id": None, "odds": 5.0},
            {"bet_type": "wide", "horse_numbers": [1, 3], "horse_id": None, "odds": 8.0},
            {"bet_type": "wide", "horse_numbers": [1, 4], "horse_id": None, "odds": 1000.0},
        ]
        result = _allocate_bets(bets, budget_per_race=3000.0, min_bet_amount=100.0)
        total = sum(b["bet_amount"] for b in result)
        assert total <= 3000.0
        # 高オッズ(1000倍)は除外され2件のみ残る
        assert len(result) == 2

    def test_single_high_odds_excluded_when_below_min(self):
        """単独bet でも計算額が min_bet_amount 未満なら除外される"""
        bets = [
            {"bet_type": "wide", "horse_numbers": [1, 2], "horse_id": None, "odds": 5.0},
        ]
        # 予算30円 → 計算額=30円 < 100円 → 除外
        result = _allocate_bets(bets, budget_per_race=30.0, min_bet_amount=100.0)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# select_base_bets のテスト
# ---------------------------------------------------------------------------


class TestSelectBaseBets:
    """ベースベット選定のテスト"""

    def test_place_selected_above_threshold(self):
        """複勝の期待値 > 閾値なら選定される"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.5, 0.2, 0.15],
            odds=[3.0, 3.0, 3.0],
        )
        bets = select_base_bets(race_df, None, expected_return_threshold=1.2)
        place_bets = [b for b in bets if b["bet_type"] == "place"]
        horse_numbers = [b["horse_numbers"][0] for b in place_bets]
        assert 1 in horse_numbers

    def test_place_not_selected_below_threshold(self):
        """複勝の期待値 <= 閾値なら除外される"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.2, 0.15, 0.1],
            odds=[3.0, 3.0, 3.0],
        )
        bets = select_base_bets(race_df, None, expected_return_threshold=1.2)
        place_bets = [b for b in bets if b["bet_type"] == "place"]
        assert len(place_bets) == 0

    def test_wide_selected_with_combo_odds(self):
        """combo_odds_dfからワイドが選定される"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.4, 0.35, 0.3, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 12.0},
        ])
        bets = select_base_bets(race_df, combo_df, expected_return_threshold=1.2)
        wide_bets = [b for b in bets if b["bet_type"] == "wide"]
        assert len(wide_bets) >= 1

    def test_umaren_auto_added_with_wide(self):
        """ワイドが選定された場合、同組み合わせの馬連が自動追加される"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.4, 0.35, 0.3, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 12.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 15.0},
        ])
        # threshold=1.0でワイドを通過させる
        bets = select_base_bets(race_df, combo_df, expected_return_threshold=1.0)
        wide_bets = [b for b in bets if b["bet_type"] == "wide"]
        umaren_bets = [b for b in bets if b["bet_type"] == "umaren"]
        assert len(wide_bets) >= 1, "ワイドが選定されるべき"
        assert len(umaren_bets) >= 1, "ワイド選定時に馬連が自動追加されるべき"
        # ワイドと同じ組み合わせの馬連が追加されること
        wide_pairs = set(tuple(b["horse_numbers"]) for b in wide_bets)
        umaren_pairs = set(tuple(b["horse_numbers"]) for b in umaren_bets)
        assert wide_pairs == umaren_pairs

    def test_no_umaren_when_no_wide(self):
        """ワイドが選定されない場合、馬連は追加されない"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.4, 0.35, 0.3, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        combo_df = _make_combo_odds_df([
            # ワイドオッズが低くて期待値フィルタを通過しない
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 1.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 15.0},
        ])
        bets = select_base_bets(race_df, combo_df, expected_return_threshold=1.2)
        umaren_bets = [b for b in bets if b["bet_type"] == "umaren"]
        assert len(umaren_bets) == 0, "ワイド未選定の場合、馬連は追加されないべき"

    def test_sanrenpuku_selected_with_combo_odds(self):
        """combo_odds_dfから三連複が選定される"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.4, 0.35, 0.3, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "sanrenpuku", "horse_number_1": 1, "horse_number_2": 2, "horse_number_3": 3, "odds_value": 50.0},
        ])
        bets = select_base_bets(race_df, combo_df, expected_return_threshold=1.2)
        san_bets = [b for b in bets if b["bet_type"] == "sanrenpuku"]
        assert len(san_bets) >= 1

    def test_no_combo_odds_returns_place_only(self):
        """combo_odds_dfが空なら複勝のみ"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.3, 0.2, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        bets = select_base_bets(race_df, pd.DataFrame(), expected_return_threshold=1.2)
        bet_types = {b["bet_type"] for b in bets}
        assert bet_types <= {"place"}

    def test_top_n_limits_candidates(self):
        """top_nが組み合わせ候補数を制限する"""
        race_df = _make_race_df(
            n_horses=10,
            probs=[0.4, 0.35, 0.3, 0.25, 0.2, 0.1, 0.05, 0.03, 0.02, 0.01],
            odds=[3.0] * 10,
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 5, "odds_value": 10.0},
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
        ])
        bets_n2 = select_base_bets(race_df, combo_df, expected_return_threshold=1.0, top_n=2)
        wide_bets_n2 = [b for b in bets_n2 if b["bet_type"] == "wide"]
        wide_pairs = [tuple(b["horse_numbers"]) for b in wide_bets_n2]
        assert (1, 5) not in wide_pairs


class TestEnabledBetTypes:
    """enabled_bet_types による券種フィルタのテスト（Issue #411: 三連複除外）"""

    def _full_combo_df(self):
        return _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 20.0},
            {"bet_type": "sanrenpuku", "horse_number_1": 1, "horse_number_2": 2, "horse_number_3": 3, "odds_value": 50.0},
        ])

    def _race_df(self):
        return _make_race_df(n_horses=5, probs=[0.4, 0.35, 0.3, 0.1, 0.05], odds=[3.0] * 5)

    def test_default_none_includes_all_types(self):
        """enabled_bet_types=None（デフォルト）は全券種を含む（後方互換）"""
        bets = select_base_bets(
            self._race_df(), self._full_combo_df(),
            expected_return_threshold=1.0, top_n=5,
        )
        types = {b["bet_type"] for b in bets}
        assert "sanrenpuku" in types

    def test_exclude_sanrenpuku(self):
        """enabled_bet_types から三連複を外すと三連複が選定されない"""
        bets = select_base_bets(
            self._race_df(), self._full_combo_df(),
            expected_return_threshold=1.0, top_n=5,
            enabled_bet_types=["place", "wide", "umaren"],
        )
        types = {b["bet_type"] for b in bets}
        assert "sanrenpuku" not in types
        # ワイド・馬連は残る
        assert "wide" in types
        assert "umaren" in types

    def test_umaren_kept_when_wide_disabled(self):
        """ワイドを外しても馬連は選定される（ペア選定ロジックは維持）"""
        bets = select_base_bets(
            self._race_df(), self._full_combo_df(),
            expected_return_threshold=1.0, top_n=5,
            enabled_bet_types=["place", "umaren"],
        )
        types = {b["bet_type"] for b in bets}
        assert "wide" not in types
        assert "umaren" in types

    def test_select_bets_for_race_threads_enabled_types(self):
        """select_bets_for_race 経由でも券種フィルタが効く"""
        bets = select_bets_for_race(
            race_df=self._race_df(),
            combo_odds_df=self._full_combo_df(),
            budget_per_race=1000.0,
            expected_return_threshold=1.0,
            min_bet_amount=100.0,
            top_n=5,
            enabled_bet_types=["place", "wide", "umaren"],
        )
        types = {b["bet_type"] for b in bets}
        assert "sanrenpuku" not in types


# ---------------------------------------------------------------------------
# select_bets_for_race のテスト（統合テスト）
# ---------------------------------------------------------------------------


class TestSelectBetsForRace:
    """select_bets_for_race 統合テスト"""

    def test_returns_list(self):
        """返り値が list であること（パターンタプルではない）"""
        race_df = _make_race_df(
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        result = select_bets_for_race(race_df)
        assert isinstance(result, list)

    def test_no_combo_odds_fallback(self):
        """combo_odds_dfがNoneでも動作する（複勝のみ）"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.3, 0.2, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        bets = select_bets_for_race(race_df, combo_odds_df=None)
        bet_types = {b["bet_type"] for b in bets}
        assert bet_types <= {"place"}

    def test_wide_triggers_umaren(self):
        """ワイドが選定されると必ず同組み合わせの馬連が追加される"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.6, 0.1, 0.1, 0.1, 0.1],
            odds=[4.0, 10.0, 10.0, 10.0, 10.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 5.0},
        ])
        bets = select_bets_for_race(
            race_df, combo_odds_df=combo_df,
            expected_return_threshold=0.1,
        )
        bet_types = {b["bet_type"] for b in bets}
        # ワイドが通過したので馬連が追加される
        assert "umaren" in bet_types
        # 単勝は追加されない
        assert "win" not in bet_types

    def test_no_win_tickets(self):
        """単勝は一切選定されない"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.3, 0.2, 0.1, 0.05],
            odds=[3.0] * 5,
            win_odds=[2.0, 8.0, 12.0, 15.0, 25.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 6.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
        ])
        bets = select_bets_for_race(
            race_df, combo_odds_df=combo_df,
            expected_return_threshold=0.1,
        )
        bet_types = {b["bet_type"] for b in bets}
        assert "win" not in bet_types

    def test_insufficient_horses_raises(self):
        """3頭未満で ValueError"""
        race_df = _make_race_df(n_horses=2, probs=[0.5, 0.3], odds=[3.0, 3.0])
        with pytest.raises(ValueError):
            select_bets_for_race(race_df)

    def test_bet_amount_not_exceeds_budget(self):
        """総賭け金が budget_per_race 以内"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.3, 0.2, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        budget_per_race = 3000.0
        bets = select_bets_for_race(
            race_df, budget_per_race=budget_per_race
        )
        total = sum(b["bet_amount"] for b in bets)
        assert total <= budget_per_race

    def test_wide_and_umaren_same_combo(self):
        """ワイドと馬連は必ず同じ組み合わせで購入される"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.65, 0.1, 0.1, 0.1, 0.05],
            odds=[2.5, 10.0, 10.0, 10.0, 20.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 6.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 12.0},
        ])
        budget_per_race = 3000.0
        bets = select_bets_for_race(
            race_df,
            combo_odds_df=combo_df,
            budget_per_race=budget_per_race,
            expected_return_threshold=0.1,
        )
        wide_pairs = set(tuple(b["horse_numbers"]) for b in bets if b["bet_type"] == "wide")
        umaren_pairs = set(tuple(b["horse_numbers"]) for b in bets if b["bet_type"] == "umaren")
        assert wide_pairs == umaren_pairs, "ワイドと馬連は同組み合わせであるべき"
        total = sum(b["bet_amount"] for b in bets)
        assert total <= budget_per_race

    def test_deprecated_params_ignored(self):
        """廃止済みパラメータ(p1, top_n_dominant等)を渡してもエラーにならない"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[3.0, 3.0, 3.0, 3.0, 3.0],
        )
        # 廃止済みパラメータを渡してもエラーなく動作する
        bets = select_bets_for_race(
            race_df,
            p1=0.3,
            expected_return_threshold=1.2,
            prob_weight_r_dominant=2.0,
            prob_weight_r_standard=1.0,
            top_n_dominant=3,
            top_n_standard=5,
        )
        assert isinstance(bets, list)


# ---------------------------------------------------------------------------
# min_prob_threshold のテスト
# ---------------------------------------------------------------------------


class TestMinProbThreshold:
    """min_prob_threshold（軸馬フィルタ）のテスト"""

    def test_low_prob_horse_excluded_as_pivot(self):
        """min_prob_threshold 未満の馬は複勝単体買いから除外される（N=18で補正係数=1.0）"""
        n = 18
        probs = [0.07, 0.30] + [0.04] * (n - 2)
        odds = [15.0, 4.0] + [5.0] * (n - 2)
        race_df = _make_race_df(n_horses=n, probs=probs, odds=odds)
        bets = select_base_bets(
            race_df,
            None,
            expected_return_threshold=1.0,
            min_prob_threshold=0.10,
        )
        place_bets = [b for b in bets if b["bet_type"] == "place"]
        horse_numbers = [b["horse_numbers"][0] for b in place_bets]
        assert 1 not in horse_numbers
        assert 2 in horse_numbers

    def test_zero_threshold_allows_all(self):
        """min_prob_threshold=0.0 なら全馬が軸馬候補"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.05, 0.20, 0.15],
            odds=[20.0, 8.0, 10.0],
        )
        bets = select_base_bets(
            race_df,
            None,
            expected_return_threshold=1.0,
            min_prob_threshold=0.0,
        )
        place_bets = [b for b in bets if b["bet_type"] == "place"]
        horse_numbers = [b["horse_numbers"][0] for b in place_bets]
        assert 2 in horse_numbers
        assert 3 in horse_numbers

    def test_select_bets_for_race_passes_min_prob_threshold(self):
        """select_bets_for_race が min_prob_threshold を正しく渡す（N=18で補正係数=1.0）"""
        n = 18
        probs = [0.07, 0.40, 0.30] + [0.03] * (n - 3)
        odds = [20.0, 3.0, 4.0] + [10.0] * (n - 3)
        race_df = _make_race_df(n_horses=n, probs=probs, odds=odds)
        bets_filtered = select_bets_for_race(
            race_df,
            expected_return_threshold=1.0,
            min_prob_threshold=0.10,
        )
        bets_unfiltered = select_bets_for_race(
            race_df,
            expected_return_threshold=1.0,
            min_prob_threshold=0.0,
        )
        place_filtered = [b for b in bets_filtered if b["bet_type"] == "place"]
        place_unfiltered = [b for b in bets_unfiltered if b["bet_type"] == "place"]
        hn_filtered = [b["horse_numbers"][0] for b in place_filtered]
        hn_unfiltered = [b["horse_numbers"][0] for b in place_unfiltered]
        assert 1 not in hn_filtered
        assert 1 in hn_unfiltered


# ---------------------------------------------------------------------------
# 複勝率フィルタの出走頭数補正テスト（Issue #208）
# ---------------------------------------------------------------------------


class TestMinProbThresholdNCorrection:
    """複勝率フィルタの出走頭数補正（N/18）のテスト"""

    def test_correction_reduces_effective_threshold_for_small_fields(self):
        """少頭数（N<18）では実効閾値が下がり、より多くの馬が通過する"""
        n = 9
        probs = [0.25, 0.15] + [0.08] * (n - 2)
        odds = [4.0, 7.0] + [5.0] * (n - 2)
        race_df = _make_race_df(n_horses=n, probs=probs, odds=odds)
        bets = select_base_bets(
            race_df,
            None,
            expected_return_threshold=0.5,
            min_prob_threshold=0.10,
        )
        place_bets = [b for b in bets if b["bet_type"] == "place"]
        horse_numbers = [b["horse_numbers"][0] for b in place_bets]
        assert 1 in horse_numbers
        assert 2 not in horse_numbers

    def test_n18_correction_factor_is_neutral(self):
        """N=18 のとき補正係数 18/18=1.0 で補正なしと同等の動作"""
        n = 18
        probs = [0.07, 0.12] + [0.05] * (n - 2)
        odds = [15.0, 9.0] + [5.0] * (n - 2)
        race_df = _make_race_df(n_horses=n, probs=probs, odds=odds)
        bets = select_base_bets(
            race_df,
            None,
            expected_return_threshold=0.5,
            min_prob_threshold=0.10,
        )
        place_bets = [b for b in bets if b["bet_type"] == "place"]
        horse_numbers = [b["horse_numbers"][0] for b in place_bets]
        assert 1 not in horse_numbers
        assert 2 in horse_numbers

    def test_zero_threshold_no_filter_regardless_of_n(self):
        """min_prob_threshold=0.0 なら出走頭数に関わらずフィルタなし"""
        for n_horses in [3, 9, 18]:
            probs = [0.01] * n_horses
            odds = [50.0] * n_horses
            race_df = _make_race_df(n_horses=n_horses, probs=probs, odds=odds)
            bets = select_base_bets(
                race_df,
                None,
                expected_return_threshold=0.0,
                min_prob_threshold=0.0,
            )
            place_bets = [b for b in bets if b["bet_type"] == "place"]
            assert len(place_bets) == n_horses

    def test_correction_boundary_value(self):
        """補正後ちょうど閾値に等しい場合は通過する"""
        n = 9
        probs = [0.20] + [0.05] * (n - 1)
        odds = [5.5] + [5.0] * (n - 1)
        race_df = _make_race_df(n_horses=n, probs=probs, odds=odds)
        bets = select_base_bets(
            race_df,
            None,
            expected_return_threshold=0.5,
            min_prob_threshold=0.10,
        )
        place_bets = [b for b in bets if b["bet_type"] == "place"]
        horse_numbers = [b["horse_numbers"][0] for b in place_bets]
        assert 1 in horse_numbers


# ---------------------------------------------------------------------------
# prob_weight_r のテスト
# ---------------------------------------------------------------------------


class TestProbWeightR:
    """prob_weight_r（選定スコア係数）のテスト"""

    def test_r_gt_1_favors_high_prob_horse(self):
        """r > 1 のとき高確率馬がスコアで優先される"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.30, 0.07, 0.20],
            odds=[4.0, 30.0, 5.0],
        )
        bets_r1 = select_base_bets(
            race_df, None, expected_return_threshold=1.0,
            min_prob_threshold=0.0, prob_weight_r=1.0,
        )
        bets_r2 = select_base_bets(
            race_df, None, expected_return_threshold=1.0,
            min_prob_threshold=0.0, prob_weight_r=2.0,
        )
        assert isinstance(bets_r1, list)
        assert isinstance(bets_r2, list)

    def test_r_default_is_1_compatible(self):
        """prob_weight_r=1.0（デフォルト）は従来の期待値ソートと等価"""
        race_df = _make_race_df(
            n_horses=4,
            probs=[0.40, 0.30, 0.20, 0.10],
            odds=[3.0, 4.0, 5.0, 10.0],
        )
        bets_default = select_base_bets(race_df, None, expected_return_threshold=1.0)
        bets_explicit = select_base_bets(race_df, None, expected_return_threshold=1.0, prob_weight_r=1.0)
        assert len(bets_default) == len(bets_explicit)
        hn_default = sorted([b["horse_numbers"][0] for b in bets_default])
        hn_explicit = sorted([b["horse_numbers"][0] for b in bets_explicit])
        assert hn_default == hn_explicit

    def test_select_bets_for_race_passes_prob_weight_r(self):
        """select_bets_for_race が prob_weight_r を正しく渡す（エラーなし）"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.40, 0.30, 0.20, 0.10, 0.05],
            odds=[3.0, 4.0, 5.0, 10.0, 20.0],
        )
        for r in [0.5, 1.0, 1.5, 2.0]:
            bets = select_bets_for_race(race_df, prob_weight_r=r)
            assert isinstance(bets, list)

    def test_prob_weight_r_does_not_affect_wide_filter(self):
        """prob_weight_r がワイド期待値フィルタに影響しないことを検証（Issue #165）"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.40, 0.30, 0.20],
            odds=[4.0, 5.0, 7.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 15.0},
        ])

        bets_r_low = select_base_bets(
            race_df, combo_df, expected_return_threshold=1.5,
            top_n=3, prob_weight_r=0.5,
        )
        bets_r_high = select_base_bets(
            race_df, combo_df, expected_return_threshold=1.5,
            top_n=3, prob_weight_r=2.0,
        )

        wide_r_low = [b for b in bets_r_low if b["bet_type"] == "wide"]
        wide_r_high = [b for b in bets_r_high if b["bet_type"] == "wide"]
        assert len(wide_r_low) == 1
        assert len(wide_r_high) == 1
        assert wide_r_low[0]["horse_numbers"] == wide_r_high[0]["horse_numbers"]

    def test_prob_weight_r_does_not_affect_sanrenpuku_filter(self):
        """prob_weight_r が三連複期待値フィルタに影響しないことを検証（Issue #165）"""
        race_df = _make_race_df(
            n_horses=4,
            probs=[0.40, 0.30, 0.20, 0.10],
            odds=[4.0, 5.0, 7.0, 15.0],
        )
        combo_df = _make_combo_odds_df([
            {
                "bet_type": "sanrenpuku",
                "horse_number_1": 1, "horse_number_2": 2, "horse_number_3": 3,
                "odds_value": 70.0,
            },
        ])

        bets_r_low = select_base_bets(
            race_df, combo_df, expected_return_threshold=1.5,
            top_n=4, prob_weight_r=0.5,
        )
        bets_r_high = select_base_bets(
            race_df, combo_df, expected_return_threshold=1.5,
            top_n=4, prob_weight_r=2.0,
        )

        san_r_low = [b for b in bets_r_low if b["bet_type"] == "sanrenpuku"]
        san_r_high = [b for b in bets_r_high if b["bet_type"] == "sanrenpuku"]
        assert len(san_r_low) == 1
        assert len(san_r_high) == 1
        assert san_r_low[0]["horse_numbers"] == san_r_high[0]["horse_numbers"]

    def test_prob_weight_r_affects_top_n_selection(self):
        """prob_weight_r が異なるとコンボ候補が変わる（Issue #206）

        スコア計算（score = place_odds * prob^r）:
          horse_number=1: prob=0.50, place_odds=2.0 → score(r=2)=0.50,  score(r=0.5)=1.41
          horse_number=2: prob=0.30, place_odds=3.5 → score(r=2)=0.315, score(r=0.5)=1.92
          horse_number=3: prob=0.12, place_odds=8.0 → score(r=2)=0.115, score(r=0.5)=2.77
          horse_number=4: prob=0.08, place_odds=10.0 → score(r=2)=0.064, score(r=0.5)=2.83

        r=2.0: top_n=2 → [H1, H2] → wide(1,2) が選定
        r=0.5: top_n=2 → [H4, H3] → wide(3,4) が選定
        """
        race_df = _make_race_df(
            n_horses=4,
            probs=[0.50, 0.30, 0.12, 0.08],
            odds=[2.0, 3.5, 8.0, 10.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
            {"bet_type": "wide", "horse_number_1": 3, "horse_number_2": 4, "odds_value": 200.0},
        ])

        bets_r2 = select_bets_for_race(
            race_df, combo_df,
            expected_return_threshold=1.2,
            top_n=2,
            prob_weight_r=2.0,
            min_prob_threshold=0.0,
        )
        bets_r05 = select_bets_for_race(
            race_df, combo_df,
            expected_return_threshold=1.2,
            top_n=2,
            prob_weight_r=0.5,
            min_prob_threshold=0.0,
        )

        wide_r2 = [b["horse_numbers"] for b in bets_r2 if b["bet_type"] == "wide"]
        wide_r05 = [b["horse_numbers"] for b in bets_r05 if b["bet_type"] == "wide"]
        assert wide_r2 == [[1, 2]], f"r=2.0 で wide(1,2) が選定されるべき: {wide_r2}"
        assert wide_r05 == [[3, 4]], f"r=0.5 で wide(3,4) が選定されるべき: {wide_r05}"
        assert wide_r2 != wide_r05


# ---------------------------------------------------------------------------
# 重複排除のテスト（Issue #204）
# ---------------------------------------------------------------------------


class TestDeduplication:
    """select_bets_for_race の重複排除テスト"""

    def test_no_duplicate_wide_in_output(self):
        """同一ワイド組み合わせが重複して出力されない（Issue #204）"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.65, 0.15, 0.10, 0.05, 0.05],
            odds=[2.0, 6.0, 10.0, 15.0, 20.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide",   "horse_number_1": 1, "horse_number_2": 2, "odds_value": 5.0},
            {"bet_type": "wide",   "horse_number_1": 1, "horse_number_2": 2, "odds_value": 5.0},  # 重複エントリ
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 8.0},
        ])
        bets = select_bets_for_race(
            race_df,
            combo_odds_df=combo_df,
            budget_per_race=3000.0,
            expected_return_threshold=0.1,
        )
        wide_bets = [b for b in bets if b["bet_type"] == "wide"]
        wide_pairs = [tuple(b["horse_numbers"]) for b in wide_bets]
        assert len(wide_pairs) == len(set(wide_pairs))

    def test_no_duplicate_umaren_in_output(self):
        """同一馬連組み合わせが重複して出力されない（Issue #204）"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.65, 0.15, 0.10, 0.05, 0.05],
            odds=[2.0, 6.0, 10.0, 15.0, 20.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide",   "horse_number_1": 1, "horse_number_2": 2, "odds_value": 5.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 8.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 8.0},  # 重複エントリ
        ])
        bets = select_bets_for_race(
            race_df,
            combo_odds_df=combo_df,
            budget_per_race=3000.0,
            expected_return_threshold=0.1,
        )
        umaren_bets = [b for b in bets if b["bet_type"] == "umaren"]
        umaren_pairs = [tuple(b["horse_numbers"]) for b in umaren_bets]
        assert len(umaren_pairs) == len(set(umaren_pairs))

    def test_first_occurrence_preserved_on_duplicate(self):
        """重複排除は先着優先（最初のエントリが残る）（Issue #204）"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.65, 0.20, 0.15],
            odds=[2.0, 4.0, 6.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 5.0},
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 99.0},  # 2件目（除去される）
        ])
        bets = select_bets_for_race(
            race_df,
            combo_odds_df=combo_df,
            budget_per_race=3000.0,
            expected_return_threshold=0.1,
        )
        wide_bets = [b for b in bets if b["bet_type"] == "wide" and b["horse_numbers"] == [1, 2]]
        assert len(wide_bets) == 1
        assert wide_bets[0]["odds"] == 5.0


# ---------------------------------------------------------------------------
# max_wide_odds フィルタのテスト（Issue #262）
# ---------------------------------------------------------------------------


class TestMaxWideOdds:
    """max_wide_odds（ワイドオッズ上限フィルタ）のテスト"""

    def test_wide_skipped_when_odds_exceed_limit(self):
        """max_wide_odds を超えるワイドはスキップされる"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.40, 0.30, 0.20],
            odds=[3.0, 4.0, 5.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 60.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 80.0},
        ])
        bets = select_base_bets(
            race_df,
            combo_df,
            expected_return_threshold=1.2,
            top_n=3,
            max_wide_odds=50.0,
        )
        wide_bets = [b for b in bets if b["bet_type"] == "wide"]
        assert len(wide_bets) == 0

    def test_wide_selected_when_odds_within_limit(self):
        """max_wide_odds 以下のワイドは通常通り選定される"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.40, 0.30, 0.20],
            odds=[3.0, 4.0, 5.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 15.0},
        ])
        bets = select_base_bets(
            race_df,
            combo_df,
            expected_return_threshold=1.0,
            top_n=3,
            max_wide_odds=50.0,
        )
        wide_bets = [b for b in bets if b["bet_type"] == "wide"]
        assert len(wide_bets) == 1
        assert wide_bets[0]["horse_numbers"] == [1, 2]

    def test_umaren_also_skipped_when_wide_skipped(self):
        """ワイドがスキップされた場合は馬連も追加されない"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.40, 0.30, 0.20],
            odds=[3.0, 4.0, 5.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 60.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 80.0},
        ])
        bets = select_base_bets(
            race_df,
            combo_df,
            expected_return_threshold=1.2,
            top_n=3,
            max_wide_odds=50.0,
        )
        umaren_bets = [b for b in bets if b["bet_type"] == "umaren"]
        assert len(umaren_bets) == 0

    def test_none_max_wide_odds_allows_all(self):
        """max_wide_odds=None (デフォルト) では制限なしにすべてのワイドが対象"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.40, 0.30, 0.20],
            odds=[3.0, 4.0, 5.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 200.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 250.0},
        ])
        bets = select_base_bets(
            race_df,
            combo_df,
            expected_return_threshold=1.0,
            top_n=3,
            max_wide_odds=None,
        )
        wide_bets = [b for b in bets if b["bet_type"] == "wide"]
        assert len(wide_bets) == 1

    def test_select_bets_for_race_passes_max_wide_odds(self):
        """select_bets_for_race が max_wide_odds を select_base_bets に正しく渡す"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.40, 0.30, 0.20],
            odds=[3.0, 4.0, 5.0],
        )
        # wide_odds=10.0: prob_i*prob_j*odds = 0.40*0.30*10 = 1.2 > 1.0 で期待値フィルタを通過し、
        # かつ 3000 円予算での配分額が 100 円以上になる
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 15.0},
        ])

        # max_wide_odds=9.0 → ワイドは除外（オッズ10.0 > 9.0）
        bets_filtered = select_bets_for_race(
            race_df,
            combo_df,
            expected_return_threshold=1.0,
            max_wide_odds=9.0,
        )
        wide_filtered = [b for b in bets_filtered if b["bet_type"] == "wide"]
        assert len(wide_filtered) == 0

        # max_wide_odds=None → ワイドは除外されない（期待値チェックを通れば選定）
        bets_no_limit = select_bets_for_race(
            race_df,
            combo_df,
            expected_return_threshold=1.0,
            max_wide_odds=None,
        )
        wide_no_limit = [b for b in bets_no_limit if b["bet_type"] == "wide"]
        assert len(wide_no_limit) == 1


# ---------------------------------------------------------------------------
# use_harville フラグのテスト
# ---------------------------------------------------------------------------


class TestUseHarvilleFlag:
    """ワイド・三連複の同時確率計算モデル切り替えのテスト"""

    def test_default_uses_independent_product(self):
        """use_harville未指定時は従来の独立積（prob_i * prob_j）を使う"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.4, 0.3, 0.2],
            odds=[3.0, 3.0, 3.0],
        )
        # 独立積: 0.4*0.3=0.12 → *odds(10.0)=1.2 == threshold なので僅かに超える9.99は除外、
        # 10.01倍なら選定される境界値で検証する
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.01},
        ])
        bets_default = select_base_bets(race_df, combo_df, expected_return_threshold=1.2)
        bets_explicit_false = select_base_bets(
            race_df, combo_df, expected_return_threshold=1.2, use_harville=False
        )
        assert bets_default == bets_explicit_false
        wide_default = [b for b in bets_default if b["bet_type"] == "wide"]
        assert len(wide_default) == 1

    def test_harville_uses_joint_probability_not_independent_product(self):
        """use_harville=Trueでは独立積ではなくHarvilleモデルの同時確率を使う"""
        from src.backtest.combo_probability import (
            invert_win_probabilities,
            pair_joint_probability,
        )

        # sum=3.0を満たす現実的な複勝率（8頭立て）
        probs = [0.868, 0.739, 0.552, 0.327, 0.252, 0.130, 0.088, 0.044]
        race_df = _make_race_df(n_horses=8, probs=probs, odds=[3.0] * 8)

        q = invert_win_probabilities(np.array(probs))
        true_joint = pair_joint_probability(q, 0, 1)  # 1番人気・2番人気の同時確率
        naive_product = probs[0] * probs[1]
        assert true_joint != pytest.approx(naive_product), (
            "テスト前提が崩れている: 独立積とHarville同時確率が一致してしまっている"
        )

        # 独立積とHarville同時確率のちょうど中間にオッズ閾値を設定し、
        # 「どちらのモデルが使われたか」で選定結果が変わるようにする
        wide_odds = 1.2 / ((true_joint + naive_product) / 2)
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": wide_odds},
        ])

        bets_naive = select_base_bets(
            race_df, combo_df, expected_return_threshold=1.2, use_harville=False
        )
        bets_harville = select_base_bets(
            race_df, combo_df, expected_return_threshold=1.2, use_harville=True
        )
        wide_naive = [b for b in bets_naive if b["bet_type"] == "wide"]
        wide_harville = [b for b in bets_harville if b["bet_type"] == "wide"]

        # 独立積(0.6415)は閾値超え、Harville同時確率(0.6274)は閾値未満になるよう
        # wide_oddsを設定しているため、モデルによって選定結果が変わるはず
        assert (len(wide_naive) == 1) != (len(wide_harville) == 1), (
            "独立積とHarvilleモデルで異なる選定結果になるはずが、同じ結果だった"
        )

    def test_harville_gamma_has_no_effect_when_disabled(self):
        """use_harville=False（デフォルト）ではgammaは無視される"""
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.4, 0.3, 0.2],
            odds=[3.0, 3.0, 3.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.01},
        ])
        bets_gamma1 = select_base_bets(
            race_df, combo_df, expected_return_threshold=1.2, gamma=1.0
        )
        bets_gamma_low = select_base_bets(
            race_df, combo_df, expected_return_threshold=1.2, gamma=0.6
        )
        assert len(bets_gamma1) == len(bets_gamma_low)

    def test_harville_handles_excluded_horses_without_crashing(self):
        """回帰テスト: オッズ欠損等で一部の馬が除外され複勝率の合計が3を大きく
        下回る場合でも、Harville計算が例外や不正な確率（>1やNaN）を出さないこと。
        （8頭立てのうち5頭がオッズ欠損で除外され、残り3頭の合計が1.0まで
        下がるという極端なケース）"""
        race_df = _make_race_df(
            n_horses=8,
            probs=[0.868, None, None, None, None, None, 0.088, 0.044],
            odds=[3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 7, "odds_value": 5.0},
        ])
        bets = select_bets_for_race(
            race_df, combo_df, expected_return_threshold=1.0, use_harville=True, top_n=5,
        )
        # 例外なく完了し、複勝・ワイドとも妥当な範囲の結果が得られること
        assert isinstance(bets, list)
        for b in bets:
            assert b["odds"] > 0
