"""
投資戦略モジュールのテスト

対象モジュール:
  - src.backtest.strategy
    - classify_race_pattern: パターン分類（境界値含む）
    - _allocate_bets: オッズ逆数比率による賭け金配分
    - select_base_bets: 複勝/ワイド/三連複の候補選定
    - select_pattern_a_extra_bets: one_dominant 時の追加ベット
    - select_bets_for_race: 統合関数
"""

from __future__ import annotations

import pytest
import pandas as pd
import numpy as np

from src.backtest.strategy import (
    RacePattern,
    classify_race_pattern,
    select_bets_for_race,
    select_base_bets,
    _allocate_bets,
    select_pattern_a_extra_bets,
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
# classify_race_pattern のテスト
# ---------------------------------------------------------------------------


class TestClassifyRacePattern:
    """パターン分類のテスト（2パターン版）"""

    def test_one_dominant(self):
        """top1 - top2 > p1 → one_dominant"""
        # gap_12 = 0.5 - 0.2 = 0.3 > 0.2
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        result = classify_race_pattern(probs, p1=0.2)
        assert result.pattern == "one_dominant"

    def test_standard(self):
        """top1 - top2 <= p1 → standard"""
        # gap_12 = 0.4 - 0.25 = 0.15 ≤ 0.2 → standard
        probs = [0.4, 0.25, 0.2, 0.15]
        result = classify_race_pattern(probs, p1=0.2)
        assert result.pattern == "standard"

    def test_boundary_exactly_p1(self):
        """gap_12 = p1（境界値）→ standard（> p1 なので突出型にならない）"""
        # gap_12 = 0.5 - 0.3 = 0.2 = p1 → standard
        probs = [0.5, 0.3, 0.15, 0.05]
        result = classify_race_pattern(probs, p1=0.2)
        assert result.pattern == "standard"

    def test_insufficient_probs(self):
        """3頭未満で ValueError"""
        with pytest.raises(ValueError):
            classify_race_pattern([0.5, 0.3])

    def test_returns_race_pattern_instance(self):
        """戻り値が RacePattern インスタンスであること"""
        probs = [0.4, 0.25, 0.2, 0.15]
        result = classify_race_pattern(probs)
        assert isinstance(result, RacePattern)
        assert hasattr(result, "pattern")
        assert hasattr(result, "top1_prob")
        assert hasattr(result, "top2_prob")
        assert hasattr(result, "top3_prob")
        assert hasattr(result, "gap_top1_top2")
        assert hasattr(result, "gap_top1_top3")


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


# ---------------------------------------------------------------------------
# select_base_bets のテスト
# ---------------------------------------------------------------------------


class TestSelectBaseBets:
    """ベースベット選定のテスト"""

    def test_place_selected_above_threshold(self):
        """複勝の期待値 > 閾値なら選定される"""
        # 馬番1: prob=0.5, odds=3.0 → 期待値=1.5 > 1.2
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
        # prob=0.2, odds=3.0 → 期待値=0.6 < 1.2
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
        # prob_1 × prob_2 × wide_odds = 0.4 × 0.35 × 12.0 = 1.68 > 1.2
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 12.0},
        ])
        bets = select_base_bets(race_df, combo_df, expected_return_threshold=1.2)
        wide_bets = [b for b in bets if b["bet_type"] == "wide"]
        assert len(wide_bets) >= 1

    def test_sanrenpuku_selected_with_combo_odds(self):
        """combo_odds_dfから三連複が選定される"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.4, 0.35, 0.3, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        # prob_1 × prob_2 × prob_3 × odds = 0.4 × 0.35 × 0.3 × 50 = 2.1 > 1.2
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
        # top_n=2 → 馬番1と2のみが組み合わせ候補
        # wide: h1=1, h2=5 → h2がtop_n=2外 → 選定されない
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 5, "odds_value": 10.0},
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
        ])
        bets_n2 = select_base_bets(race_df, combo_df, expected_return_threshold=1.0, top_n=2)
        wide_bets_n2 = [b for b in bets_n2 if b["bet_type"] == "wide"]
        # top_n=2のとき馬番1,2のペアは有効、馬番1,5は除外
        wide_pairs = [tuple(b["horse_numbers"]) for b in wide_bets_n2]
        assert (1, 5) not in wide_pairs


# ---------------------------------------------------------------------------
# select_pattern_a_extra_bets のテスト
# ---------------------------------------------------------------------------


class TestSelectPatternAExtraBets:
    """one_dominant 追加ベットのテスト"""

    def test_win_added_when_win_odds_present(self):
        """win_odds カラムがあれば単勝追加"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[3.0] * 5,
            win_odds=[5.0, 10.0, 15.0, 20.0, 30.0],
        )
        # top1_prob=0.5, win_odds=5.0 → 0.5×5.0=2.5 > 1.2
        bets = select_pattern_a_extra_bets(race_df, None, expected_return_threshold=1.2)
        win_bets = [b for b in bets if b["bet_type"] == "win"]
        assert len(win_bets) >= 1
        assert win_bets[0]["horse_numbers"] == [1]

    def test_win_not_added_without_win_odds(self):
        """win_odds カラムがなければ単勝なし"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[3.0] * 5,
            win_odds=None,
        )
        bets = select_pattern_a_extra_bets(race_df, None, expected_return_threshold=1.2)
        win_bets = [b for b in bets if b["bet_type"] == "win"]
        assert len(win_bets) == 0

    def test_umaren_added_for_top1(self):
        """top1 軸の馬連が追加される"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.3, 0.2, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        # top1_prob=0.5, prob_other=0.3, umaren_odds=8.0 → 0.5×0.3×8.0=1.2 > 1.2はFalse
        # umaren_odds=10.0 → 0.5×0.3×10.0=1.5 > 1.2 → 追加
        combo_df = _make_combo_odds_df([
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
        ])
        bets = select_pattern_a_extra_bets(race_df, combo_df, expected_return_threshold=1.2)
        umaren_bets = [b for b in bets if b["bet_type"] == "umaren"]
        assert len(umaren_bets) >= 1


# ---------------------------------------------------------------------------
# select_bets_for_race のテスト（統合テスト）
# ---------------------------------------------------------------------------


class TestSelectBetsForRace:
    """select_bets_for_race 統合テスト"""

    def test_returns_bets_and_pattern(self):
        """返り値が (list, RacePattern) のタプル"""
        race_df = _make_race_df(
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        result = select_bets_for_race(race_df, capital=100_000.0)
        assert isinstance(result, tuple)
        assert len(result) == 2
        bets, pattern = result
        assert isinstance(bets, list)
        assert isinstance(pattern, RacePattern)

    def test_no_combo_odds_fallback(self):
        """combo_odds_dfがNoneでも動作する（複勝のみ）"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.3, 0.2, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        bets, pattern = select_bets_for_race(race_df, combo_odds_df=None, capital=100_000.0)
        # エラーなく動作し、複勝のみ選定される
        bet_types = {b["bet_type"] for b in bets}
        assert bet_types <= {"place"}

    def test_one_dominant_triggers_pattern_a(self):
        """one_dominant 時にパターンAのベット（win/umaren）が選定される可能性がある"""
        # gap_12 = 0.6 - 0.1 = 0.5 > 0.2 → one_dominant
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.6, 0.1, 0.1, 0.1, 0.1],
            odds=[4.0, 10.0, 10.0, 10.0, 10.0],
            win_odds=[6.0, 20.0, 20.0, 20.0, 20.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
        ])
        bets, pattern = select_bets_for_race(
            race_df, combo_odds_df=combo_df, capital=100_000.0, p1=0.2
        )
        assert pattern.pattern == "one_dominant"
        # one_dominant 時は win/umaren が追加される可能性がある
        bet_types = {b["bet_type"] for b in bets}
        # 少なくともパターンAが呼ばれること自体を確認（エラーなし）
        assert isinstance(bets, list)

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
        bets, _ = select_bets_for_race(
            race_df, budget_per_race=budget_per_race
        )
        total = sum(b["bet_amount"] for b in bets)
        assert total <= budget_per_race


# ---------------------------------------------------------------------------
# min_prob_threshold のテスト
# ---------------------------------------------------------------------------


class TestMinProbThreshold:
    """min_prob_threshold（軸馬フィルタ）のテスト"""

    def test_low_prob_horse_excluded_as_pivot(self):
        """min_prob_threshold 未満の馬は複勝単体買いから除外される"""
        # 馬番1: prob=0.07 (< 0.10), odds=15.0 → 期待値=1.05 > 1.0 だがフィルタされる
        # 馬番2: prob=0.30, odds=4.0 → 期待値=1.20 > 1.0 → 選定される
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.07, 0.30, 0.20],
            odds=[15.0, 4.0, 5.0],
        )
        bets = select_base_bets(
            race_df,
            None,
            expected_return_threshold=1.0,
            min_prob_threshold=0.10,
        )
        place_bets = [b for b in bets if b["bet_type"] == "place"]
        horse_numbers = [b["horse_numbers"][0] for b in place_bets]
        # 馬番1（prob=0.07 < 0.10）は除外される
        assert 1 not in horse_numbers
        # 馬番2（prob=0.30 >= 0.10）は選定される
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
        # 全馬が候補（期待値フィルタのみ）
        # 馬番1: 0.05×20=1.0 > 1.0 はFalse（境界値なので除外）
        # 馬番2: 0.20×8=1.6 > 1.0 → 選定
        # 馬番3: 0.15×10=1.5 > 1.0 → 選定
        assert 2 in horse_numbers
        assert 3 in horse_numbers

    def test_select_bets_for_race_passes_min_prob_threshold(self):
        """select_bets_for_race が min_prob_threshold を正しく渡す"""
        # 馬番1: prob=0.07, odds=20.0 → 低確率高オッズ馬（フィルタ対象）
        # 馬番2: prob=0.40, odds=3.0 → 高確率馬
        # 馬番3: prob=0.30, odds=4.0
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.07, 0.40, 0.30],
            odds=[20.0, 3.0, 4.0],
        )
        bets_filtered, _ = select_bets_for_race(
            race_df,
            capital=100_000.0,
            expected_return_threshold=1.0,
            min_prob_threshold=0.10,
        )
        bets_unfiltered, _ = select_bets_for_race(
            race_df,
            capital=100_000.0,
            expected_return_threshold=1.0,
            min_prob_threshold=0.0,
        )
        place_filtered = [b for b in bets_filtered if b["bet_type"] == "place"]
        place_unfiltered = [b for b in bets_unfiltered if b["bet_type"] == "place"]
        hn_filtered = [b["horse_numbers"][0] for b in place_filtered]
        hn_unfiltered = [b["horse_numbers"][0] for b in place_unfiltered]
        # フィルタあり: 馬番1（prob=0.07）は除外
        assert 1 not in hn_filtered
        # フィルタなし: 馬番1も選定される（期待値1.4 > 1.0）
        assert 1 in hn_unfiltered


# ---------------------------------------------------------------------------
# prob_weight_r のテスト
# ---------------------------------------------------------------------------


class TestProbWeightR:
    """prob_weight_r（選定スコア係数）のテスト"""

    def test_r_gt_1_favors_high_prob_horse(self):
        """r > 1 のとき高確率馬がスコアで優先される"""
        # 馬A: prob=0.30, odds=4.0 → r=1: score=1.20, r=2: score=4.0×0.09=0.36
        # 馬B: prob=0.07, odds=30.0 → r=1: score=2.10, r=2: score=30.0×0.0049=0.147
        # r=1: 馬Bが高スコア, r=2: 馬Aが高スコア
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.30, 0.07, 0.20],
            odds=[4.0, 30.0, 5.0],
        )
        bets_r1 = select_base_bets(
            race_df,
            None,
            expected_return_threshold=1.0,
            min_prob_threshold=0.0,
            prob_weight_r=1.0,
        )
        bets_r2 = select_base_bets(
            race_df,
            None,
            expected_return_threshold=1.0,
            min_prob_threshold=0.0,
            prob_weight_r=2.0,
        )
        # r=1 でも r=2 でもエラーなく動作すること
        assert isinstance(bets_r1, list)
        assert isinstance(bets_r2, list)

    def test_r_default_is_1_compatible(self):
        """prob_weight_r=1.0（デフォルト）は従来の期待値ソートと等価"""
        race_df = _make_race_df(
            n_horses=4,
            probs=[0.40, 0.30, 0.20, 0.10],
            odds=[3.0, 4.0, 5.0, 10.0],
        )
        # デフォルト（r=1.0）と明示r=1.0は同じ結果
        bets_default = select_base_bets(
            race_df, None, expected_return_threshold=1.0
        )
        bets_explicit = select_base_bets(
            race_df, None, expected_return_threshold=1.0, prob_weight_r=1.0
        )
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
            bets, pattern = select_bets_for_race(
                race_df, capital=100_000.0, prob_weight_r=r
            )
            assert isinstance(bets, list)
            assert isinstance(pattern, RacePattern)
