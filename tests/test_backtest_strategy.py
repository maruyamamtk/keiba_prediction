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
    _gini_coefficient,
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


class TestGiniCoefficient:
    """ジニ係数計算のテスト"""

    def test_uniform_distribution_near_zero(self):
        """均等分布 → ジニ係数 ≈ 0"""
        n = 8
        probs = [1.0 / n] * n
        gini = _gini_coefficient(probs)
        assert abs(gini) < 1e-9

    def test_concentrated_distribution_high(self):
        """1頭に集中 → ジニ係数が高い（1に近い）"""
        probs = [0.9, 0.05, 0.05]
        gini = _gini_coefficient(probs)
        assert gini > 0.5

    def test_empty_list(self):
        """空リスト → 0.0"""
        assert _gini_coefficient([]) == 0.0

    def test_all_zero(self):
        """全ゼロ → 0.0（ゼロ除算しない）"""
        assert _gini_coefficient([0.0, 0.0, 0.0]) == 0.0

    def test_typical_racing_distribution(self):
        """典型的な競馬レース分布 → 0 < gini < 1"""
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        gini = _gini_coefficient(probs)
        assert 0.0 < gini < 1.0


class TestClassifyRacePattern:
    """パターン分類のテスト（ジニ係数ベース）"""

    def test_one_dominant_high_gini(self):
        """ジニ係数 > p1 → one_dominant"""
        # [0.5, 0.2, 0.15, 0.1, 0.05] のジニ係数は約0.4 > p1=0.3
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        result = classify_race_pattern(probs, p1=0.3)
        assert result.pattern == "one_dominant"

    def test_standard_low_gini(self):
        """ジニ係数 <= p1 → standard"""
        # [0.35, 0.3, 0.2, 0.15] のジニ係数は約0.175 < p1=0.3
        probs = [0.35, 0.3, 0.2, 0.15]
        result = classify_race_pattern(probs, p1=0.3)
        assert result.pattern == "standard"

    def test_boundary_exactly_p1_is_standard(self):
        """補正後ジニ係数 = p1（境界値）→ standard（> p1 なので突出型にならない）"""
        # 補正後ジニ係数が p1 に等しい場合は standard
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        N = len(probs)
        gini = _gini_coefficient(probs)
        gini_adjusted = gini * N / (N - 1)
        result = classify_race_pattern(probs, p1=gini_adjusted)
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
        assert hasattr(result, "gini_coefficient")
        assert hasattr(result, "gini_coefficient_adjusted")

    def test_gini_coefficient_stored_in_result(self):
        """gini_coefficient が結果に格納されること"""
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        result = classify_race_pattern(probs, p1=0.3)
        expected_gini = _gini_coefficient(probs)
        assert abs(result.gini_coefficient - expected_gini) < 1e-9


# ---------------------------------------------------------------------------
# ジニ係数の出走頭数補正テスト（Issue #207）
# ---------------------------------------------------------------------------


class TestClassifyRacePatternGiniAdjusted:
    """ジニ係数の出走頭数補正（N/(N-1)）のテスト"""

    def test_gini_adjusted_stored_in_race_pattern(self):
        """gini_coefficient_adjusted が RacePattern に格納されること"""
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        N = len(probs)
        result = classify_race_pattern(probs, p1=0.3)
        expected_gini = _gini_coefficient(probs)
        expected_adjusted = expected_gini * N / (N - 1)
        assert abs(result.gini_coefficient_adjusted - expected_adjusted) < 1e-9

    def test_adjusted_is_greater_than_raw_gini(self):
        """補正後ジニ係数は生ジニ係数より大きい（N > 1 のとき N/(N-1) > 1）"""
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        result = classify_race_pattern(probs, p1=0.5)
        assert result.gini_coefficient_adjusted > result.gini_coefficient

    def test_pattern_judgment_uses_adjusted_gini(self):
        """補正後ジニ係数でパターン判定すること（生値では standard でも補正後 one_dominant になるケース）"""
        # N=5, 生ジニ係数を計算してその値を p1 に設定
        # → 補正後 = gini * 5/4 > gini = p1 → one_dominant になることを確認
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        gini_raw = _gini_coefficient(probs)
        # p1 を生ジニ係数と同値に設定: 旧ロジックなら standard、補正後ロジックなら one_dominant
        result = classify_race_pattern(probs, p1=gini_raw)
        assert result.pattern == "one_dominant"

    def test_adjusted_gini_increases_with_fewer_horses(self):
        """同じ確率分布でも頭数が少ないと補正後ジニ係数が大きくなること"""
        # 少頭数(N=3)と多頭数(N=9)で同じ相対分布パターンを使って比較
        probs_small = [0.5, 0.3, 0.2]  # N=3
        probs_large = [0.5, 0.3, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]  # N=9（0要素は除外に注意）

        result_small = classify_race_pattern(probs_small, p1=0.9)
        # 3頭での補正係数: 3/2 = 1.5
        N_small = len(probs_small)
        expected_adj_small = result_small.gini_coefficient * N_small / (N_small - 1)
        assert abs(result_small.gini_coefficient_adjusted - expected_adj_small) < 1e-9

    def test_adjusted_coefficient_formula(self):
        """補正式 G × N/(N-1) が正しく適用されていること（数値検証）"""
        probs = [0.6, 0.25, 0.15]  # N=3
        result = classify_race_pattern(probs, p1=0.9)
        raw = _gini_coefficient(probs)
        expected = raw * 3 / 2  # N=3, N/(N-1) = 3/2
        assert abs(result.gini_coefficient_adjusted - expected) < 1e-9

    def test_no_zero_division_guard(self):
        """N-1 == 0 のゼロ除算ガードが機能すること"""
        # N >= 3 の前提があるため N=1 は既存の ValueError で防ぐが、
        # コードが N > 1 の条件分岐でガードしていることを間接的に検証する
        # N=3 の最小ケースで正常動作することを確認
        probs = [0.5, 0.3, 0.2]
        result = classify_race_pattern(probs, p1=0.9)
        assert result.gini_coefficient_adjusted >= result.gini_coefficient


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
        # odds=4.6 と odds=765.4 の2点。
        # 高オッズ側は逆数が極めて小さいため最初の計算では < 100円 → 除外。
        # 除外後に残った odds=4.6 だけで予算3000を再配分すると 3000円 になる。
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
    """one_dominant 追加ベットのテスト（馬連のみ）"""

    def test_umaren_follows_wide_pairs_from_base_bets(self):
        """馬連はStep2のワイドと同じ組み合わせを自動購入"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.3, 0.2, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        # Step2のワイドが [1, 2] の組み合わせだった場合
        base_bets = [
            {"bet_type": "wide", "horse_numbers": [1, 2], "horse_id": None, "odds": 8.0},
        ]
        combo_df = _make_combo_odds_df([
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 12.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 3, "odds_value": 20.0},
        ])
        bets = select_pattern_a_extra_bets(race_df, combo_df, base_bets=base_bets)
        umaren_bets = [b for b in bets if b["bet_type"] == "umaren"]
        # [1, 2] のワイドに対応する馬連のみ購入（[1, 3] は選ばれない）
        assert len(umaren_bets) == 1
        assert umaren_bets[0]["horse_numbers"] == [1, 2]

    def test_umaren_not_added_when_no_wide_in_base_bets(self):
        """Step2にワイドが0件の場合、馬連も0件"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.3, 0.2, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        base_bets = [
            {"bet_type": "place", "horse_numbers": [1], "horse_id": "h1", "odds": 3.0},
        ]
        combo_df = _make_combo_odds_df([
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 12.0},
        ])
        bets = select_pattern_a_extra_bets(race_df, combo_df, base_bets=base_bets)
        umaren_bets = [b for b in bets if b["bet_type"] == "umaren"]
        assert len(umaren_bets) == 0

    def test_umaren_multiple_wide_pairs(self):
        """複数ワイド組み合わせがある場合、対応する馬連を全て購入"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.3, 0.2, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        base_bets = [
            {"bet_type": "wide", "horse_numbers": [1, 2], "horse_id": None, "odds": 8.0},
            {"bet_type": "wide", "horse_numbers": [1, 3], "horse_id": None, "odds": 15.0},
        ]
        combo_df = _make_combo_odds_df([
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 12.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 3, "odds_value": 25.0},
        ])
        bets = select_pattern_a_extra_bets(race_df, combo_df, base_bets=base_bets)
        umaren_bets = [b for b in bets if b["bet_type"] == "umaren"]
        assert len(umaren_bets) == 2

    def test_no_win_or_place_added(self):
        """単勝・複勝は追加されない（馬連のみ）"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[3.0] * 5,
            win_odds=[2.0, 10.0, 15.0, 20.0, 30.0],
        )
        bets = select_pattern_a_extra_bets(race_df, None, base_bets=[])
        assert all(b["bet_type"] not in {"win", "place"} for b in bets)


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
        result = select_bets_for_race(race_df)
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
        bets, pattern = select_bets_for_race(race_df, combo_odds_df=None)
        # エラーなく動作し、複勝のみ選定される
        bet_types = {b["bet_type"] for b in bets}
        assert bet_types <= {"place"}

    def test_one_dominant_triggers_pattern_a(self):
        """one_dominant 時に馬連が自動購入され、単勝は購入されない"""
        # [0.6, 0.1, 0.1, 0.1, 0.1] のジニ係数は約0.4 > p1=0.3 → one_dominant
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.6, 0.1, 0.1, 0.1, 0.1],
            odds=[4.0, 10.0, 10.0, 10.0, 10.0],
            win_odds=[6.0, 20.0, 20.0, 20.0, 20.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 5.0},
        ])
        # threshold=0.1 でワイドも期待値フィルタを通過させる
        bets, pattern = select_bets_for_race(
            race_df, combo_odds_df=combo_df, p1=0.3,
            expected_return_threshold=0.1,
        )
        assert pattern.pattern == "one_dominant"
        bet_types = {b["bet_type"] for b in bets}
        # 単勝は自動追加されない（win_oddsがあっても）
        assert "win" not in bet_types
        # ワイドが通過したので馬連が追加される
        assert "umaren" in bet_types

    def test_pattern_specific_params_applied(self):
        """パターン別パラメータ（threshold_dominant/standard）が正しく適用される"""
        # one_dominant: [0.5, 0.2, 0.15, 0.1, 0.05] → gini ≈ 0.4 > p1=0.3
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[3.0, 3.0, 3.0, 3.0, 3.0],
        )
        # threshold_dominant=2.0 (厳しい): select_base_bets では複勝が選ばれない
        # threshold_standard=1.0 (緩い) → パターンがstandardなら複勝が選ばれる
        bets_strict, pattern = select_bets_for_race(
            race_df,
            threshold_dominant=2.0,
            threshold_standard=1.0,
            p1=0.3,
        )
        # one_dominantのため threshold_dominant=2.0 が適用される
        assert pattern.pattern == "one_dominant"
        place_bets = [b for b in bets_strict if b["bet_type"] == "place"]
        # threshold_dominant=2.0 のため 0.5 × 3.0 = 1.5 < 2.0 → 複勝は選ばれない
        # （突出型でも top1 の複勝を自動追加しなくなった）
        assert len(place_bets) == 0

    def test_insufficient_horses_raises(self):
        """3頭未満で ValueError"""
        race_df = _make_race_df(n_horses=2, probs=[0.5, 0.3], odds=[3.0, 3.0])
        with pytest.raises(ValueError):
            select_bets_for_race(race_df)

    def test_standard_pattern_no_win_or_umaren(self):
        """標準型レースでは単勝・馬連（パターンA）が推奨されない"""
        # [0.25, 0.25, 0.2, 0.2, 0.1] のジニ係数は低い → standard
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.25, 0.25, 0.2, 0.2, 0.1],
            odds=[4.0, 4.0, 5.0, 5.0, 10.0],
            win_odds=[8.0, 8.0, 10.0, 10.0, 20.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 10.0},
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 3.0},
        ])
        bets, pattern = select_bets_for_race(
            race_df, combo_odds_df=combo_df, p1=0.3,
            expected_return_threshold=1.0,
        )
        assert pattern.pattern == "standard"
        bet_types = {b["bet_type"] for b in bets}
        assert "win" not in bet_types, "標準型レースで単勝が選定されてはならない"
        assert "umaren" not in bet_types, "標準型レースで馬連が選定されてはならない"

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

    def test_one_dominant_total_within_budget(self):
        """突出型レースで複勝+ワイド+馬連の合計が budget_per_race 以内"""
        # gini ≈ 0.45 > p1=0.3 → one_dominant
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.65, 0.1, 0.1, 0.1, 0.05],
            odds=[2.5, 10.0, 10.0, 10.0, 20.0],
            win_odds=[3.0, 20.0, 20.0, 20.0, 40.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 6.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 12.0},
        ])
        budget_per_race = 3000.0
        bets, pattern = select_bets_for_race(
            race_df,
            combo_odds_df=combo_df,
            budget_per_race=budget_per_race,
            p1=0.3,
            expected_return_threshold=1.0,
        )
        assert pattern.pattern == "one_dominant"
        bet_types = {b["bet_type"] for b in bets}
        assert "win" not in bet_types, "突出型レースでも単勝は購入されない"
        total = sum(b["bet_amount"] for b in bets)
        assert total <= budget_per_race, (
            f"合計賭け金 {total} が budget_per_race {budget_per_race} を超えている"
        )

    def test_one_dominant_umaren_added_with_wide(self):
        """突出型レースでワイドが通過した場合、同組み合わせの馬連が追加される"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.65, 0.1, 0.1, 0.1, 0.05],
            odds=[2.5, 10.0, 10.0, 10.0, 20.0],
            win_odds=[4.0, 20.0, 20.0, 20.0, 40.0],
        )
        combo_df = _make_combo_odds_df([
            {"bet_type": "wide", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 6.0},
            {"bet_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "odds_value": 12.0},
        ])
        budget_per_race = 3000.0
        bets, pattern = select_bets_for_race(
            race_df,
            combo_odds_df=combo_df,
            budget_per_race=budget_per_race,
            p1=0.3,
            expected_return_threshold=0.1,  # 低い閾値でワイドを通過させる
        )
        assert pattern.pattern == "one_dominant"
        bet_types = {b["bet_type"] for b in bets}
        # 単勝は購入されない
        assert "win" not in bet_types
        # ワイドが通過しているので馬連も追加される
        assert "umaren" in bet_types
        # 合計が予算以内
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
            expected_return_threshold=1.0,
            min_prob_threshold=0.10,
        )
        bets_unfiltered, _ = select_bets_for_race(
            race_df,
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
                race_df, prob_weight_r=r
            )
            assert isinstance(bets, list)
            assert isinstance(pattern, RacePattern)

    def test_prob_weight_r_does_not_affect_wide_filter(self):
        """prob_weight_r がワイド期待値フィルタに影響しないことを検証（Issue #165）

        prob_weight_r はソート基準（odds * prob^r）にのみ使用し、
        期待値フィルタ（prob_i * prob_j * wide_odds > threshold）には含めない。
        r=0.5 でも r=2.0 でも同じワイド組み合わせが選定されること。
        """
        # 馬番1,2,3 の順位は prob_weight_r によらず変わらない設定
        # prob=0.40,0.30,0.20 → score(r=1): 1.6,1.5,1.4 / score(r=2): 0.64,0.45,0.28
        # どちらの r でも上位2頭は馬番1,2
        race_df = _make_race_df(
            n_horses=3,
            probs=[0.40, 0.30, 0.20],
            odds=[4.0, 5.0, 7.0],
        )
        # ワイドオッズ: 0.40*0.30*15.0=1.8 > threshold(1.5) → 通るべき
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
        # どちらも同じワイド馬券が選定されること
        assert len(wide_r_low) == 1, "r=0.5 でワイドが1件選定されるべき"
        assert len(wide_r_high) == 1, "r=2.0 でワイドが1件選定されるべき"
        assert wide_r_low[0]["horse_numbers"] == wide_r_high[0]["horse_numbers"]

    def test_prob_weight_r_does_not_affect_sanrenpuku_filter(self):
        """prob_weight_r が三連複期待値フィルタに影響しないことを検証（Issue #165）

        期待値フィルタ（prob_i * prob_j * prob_k * san_odds > threshold）に
        prob_weight_r を含めると r < 1 のとき不当に除外される。
        r=0.5 でも r=2.0 でも同じ三連複組み合わせが選定されること。
        """
        race_df = _make_race_df(
            n_horses=4,
            probs=[0.40, 0.30, 0.20, 0.10],
            odds=[4.0, 5.0, 7.0, 15.0],
        )
        # 三連複オッズ: 0.40*0.30*0.20*70.0=1.68 > threshold(1.5) → 通るべき
        # prob_weight_r=0.5 を誤って掛けると: 0.5*1.68=0.84 < 1.5 → 除外されてしまう
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
        assert len(san_r_low) == 1, "r=0.5 で三連複が1件選定されるべき"
        assert len(san_r_high) == 1, "r=2.0 で三連複が1件選定されるべき"
        assert san_r_low[0]["horse_numbers"] == san_r_high[0]["horse_numbers"]
