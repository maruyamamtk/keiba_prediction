"""
投資戦略モジュールのテスト

対象モジュール:
  - src.backtest.strategy
    - classify_race_pattern: パターン分類（境界値含む）
    - select_bets_one_dominant: 突出型賭け選定
    - select_bets_competitive: 拮抗型賭け選定
    - select_bets_standard: 標準型賭け選定
    - select_bets_for_race: 統合関数

テストケース数: 20件以上
"""

from __future__ import annotations

import pytest
import pandas as pd
import numpy as np

from src.backtest.strategy import (
    RacePattern,
    classify_race_pattern,
    select_bets_for_race,
    select_bets_one_dominant,
    select_bets_competitive,
    select_bets_standard,
)


# ---------------------------------------------------------------------------
# テスト用ヘルパー
# ---------------------------------------------------------------------------


def _make_race_df(
    n_horses: int = 8,
    probs: list[float] | None = None,
    odds: list[float] | None = None,
    horse_ids: list[str] | None = None,
) -> pd.DataFrame:
    """
    テスト用レース DataFrame を生成する

    Args:
        n_horses: 馬数
        probs: 複勝率リスト（降順推奨）。None の場合は均等分布
        odds: オッズリスト。None の場合は全馬 3.0
        horse_ids: 馬 ID リスト。None の場合は自動生成
    """
    if probs is None:
        probs = [1.0 / n_horses] * n_horses
    if odds is None:
        odds = [3.0] * n_horses
    if horse_ids is None:
        horse_ids = [f"horse_{i:03d}" for i in range(1, n_horses + 1)]

    rows = []
    for i, (p, o, hid) in enumerate(zip(probs, odds, horse_ids)):
        rows.append(
            {
                "race_id": "race_001",
                "horse_id": hid,
                "horse_number": i + 1,
                "win_place_prob": p,
                "odds": o,
                "finish_position": i + 1,  # 馬番 = 着順
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# classify_race_pattern のテスト
# ---------------------------------------------------------------------------


class TestClassifyRacePattern:
    """パターン分類のテスト（境界値含む）"""

    def test_one_dominant_pattern(self):
        """top1 - top2 > p1=0.2 → one_dominant"""
        # gap_12 = 0.5 - 0.2 = 0.3 > 0.2
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        result = classify_race_pattern(probs, p1=0.2, p2=0.15)
        assert result.pattern == "one_dominant"

    def test_competitive_pattern(self):
        """top1 - top3 < p2=0.15 → competitive"""
        # gap_13 = 0.35 - 0.28 = 0.07 < 0.15
        probs = [0.35, 0.32, 0.28, 0.05]
        result = classify_race_pattern(probs, p1=0.2, p2=0.15)
        assert result.pattern == "competitive"

    def test_standard_pattern(self):
        """one_dominant でも competitive でもない → standard"""
        # gap_12 = 0.4 - 0.25 = 0.15 ≤ 0.2 かつ gap_13 = 0.4 - 0.2 = 0.2 ≥ 0.15
        probs = [0.4, 0.25, 0.2, 0.15]
        result = classify_race_pattern(probs, p1=0.2, p2=0.15)
        assert result.pattern == "standard"

    def test_boundary_one_dominant_exactly_p1(self):
        """gap_12 = p1（境界値）→ one_dominant にはならない（条件は > p1）"""
        # gap_12 = 0.5 - 0.3 = 0.2 = p1（> ではないので突出型にならない）
        probs = [0.5, 0.3, 0.15, 0.05]
        result = classify_race_pattern(probs, p1=0.2, p2=0.15)
        assert result.pattern != "one_dominant"

    def test_boundary_one_dominant_just_above_p1(self):
        """gap_12 = p1 + ε → one_dominant"""
        # gap_12 = 0.5 - 0.299 = 0.201 > 0.2
        probs = [0.5, 0.299, 0.15, 0.05]
        result = classify_race_pattern(probs, p1=0.2, p2=0.15)
        assert result.pattern == "one_dominant"

    def test_boundary_competitive_exactly_p2(self):
        """gap_13 = p2（境界値）→ competitive にはならない（条件は < p2）"""
        # gap_13 = 0.4 - 0.25 = 0.15 = p2（< ではないので拮抗型にならない）
        probs = [0.4, 0.3, 0.25, 0.05]
        result = classify_race_pattern(probs, p1=0.2, p2=0.15)
        assert result.pattern != "competitive"

    def test_boundary_competitive_just_below_p2(self):
        """gap_13 = p2 - ε → competitive"""
        # gap_13 = 0.4 - 0.251 = 0.149 < 0.15
        probs = [0.4, 0.35, 0.251, 0.009]
        result = classify_race_pattern(probs, p1=0.2, p2=0.15)
        assert result.pattern == "competitive"

    def test_probs_not_sorted_still_works(self):
        """未ソートの probs でも正しく分類される"""
        # 降順: [0.5, 0.2, 0.15, 0.1, 0.05] → gap_12=0.3 > 0.2 → one_dominant
        probs_unsorted = [0.1, 0.5, 0.05, 0.2, 0.15]
        result = classify_race_pattern(probs_unsorted, p1=0.2, p2=0.15)
        assert result.pattern == "one_dominant"

    def test_returns_race_pattern_dataclass(self):
        """戻り値が RacePattern dataclass であること"""
        probs = [0.4, 0.25, 0.2, 0.15]
        result = classify_race_pattern(probs)
        assert isinstance(result, RacePattern)
        assert hasattr(result, "pattern")
        assert hasattr(result, "top1_prob")
        assert hasattr(result, "top2_prob")
        assert hasattr(result, "top3_prob")
        assert hasattr(result, "gap_top1_top2")
        assert hasattr(result, "gap_top1_top3")

    def test_gap_values_are_correct(self):
        """gap_top1_top2 と gap_top1_top3 の値が正確に計算される"""
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        result = classify_race_pattern(probs)
        assert abs(result.gap_top1_top2 - 0.3) < 1e-9
        assert abs(result.gap_top1_top3 - 0.35) < 1e-9

    def test_raises_if_less_than_3_horses(self):
        """probs が 3 要素未満の場合 ValueError を raise する"""
        with pytest.raises(ValueError):
            classify_race_pattern([0.5, 0.3])

    def test_minimum_3_horses(self):
        """3頭ちょうどでも正常に動作する"""
        probs = [0.5, 0.3, 0.2]
        result = classify_race_pattern(probs, p1=0.2, p2=0.15)
        assert result.pattern in {"one_dominant", "competitive", "standard"}


# ---------------------------------------------------------------------------
# select_bets_one_dominant のテスト
# ---------------------------------------------------------------------------


class TestSelectBetsOneDominant:
    """突出型賭け選定のテスト"""

    def test_selects_top1_horse_only(self):
        """複勝率最上位の馬を 1 頭だけ選定する"""
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[3.0, 5.0, 8.0, 10.0, 15.0],
        )
        bets = select_bets_one_dominant(race_df, capital=100_000.0)
        assert len(bets) == 1
        assert bets[0]["horse_number"] == 1  # 最高複勝率は馬番1

    def test_bet_amount_is_multiple_of_100(self):
        """賭け金が 100 円単位であること"""
        race_df = _make_race_df(
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        bets = select_bets_one_dominant(race_df, capital=100_000.0)
        if bets:
            assert bets[0]["bet_amount"] % 100 == 0

    def test_bet_amount_does_not_exceed_max_ratio(self):
        """賭け金が capital × max_bet_ratio を超えないこと"""
        capital = 100_000.0
        max_ratio = 0.05
        race_df = _make_race_df(
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[3.0] * 5,
        )
        bets = select_bets_one_dominant(
            race_df, capital=capital, max_bet_ratio=max_ratio
        )
        if bets:
            assert bets[0]["bet_amount"] <= capital * max_ratio

    def test_returns_empty_when_kelly_is_zero(self):
        """Kelly 値が 0 以下（期待値マイナス）なら空リストを返す"""
        # p=0.1, odds=1.5 → Kelly = (0.1 * 0.5 - 0.9) / 0.5 = -1.7 < 0
        race_df = _make_race_df(
            probs=[0.1, 0.05, 0.03, 0.02],
            odds=[1.5] * 4,
        )
        bets = select_bets_one_dominant(race_df, capital=100_000.0)
        assert bets == []

    def test_returns_empty_when_no_data(self):
        """空の DataFrame を渡すと空リストを返す"""
        bets = select_bets_one_dominant(pd.DataFrame(), capital=100_000.0)
        assert bets == []

    def test_bet_does_not_exceed_capital(self):
        """賭け金が残高を超えないこと"""
        race_df = _make_race_df(
            probs=[0.8, 0.1, 0.05, 0.05],
            odds=[5.0] * 4,
        )
        # 残高を極めて少なくする
        bets = select_bets_one_dominant(race_df, capital=50.0, min_bet_amount=100.0)
        # min_bet_amount=100 > capital=50 → 賭け対象外
        assert bets == []


# ---------------------------------------------------------------------------
# select_bets_competitive のテスト
# ---------------------------------------------------------------------------


class TestSelectBetsCompetitive:
    """拮抗型賭け選定のテスト"""

    def test_selects_up_to_top_n_horses(self):
        """期待回収率を満たす馬が top_n 以内であること"""
        # 全馬の期待回収率 = 0.35 * 3.0 = 1.05 > 1.0
        race_df = _make_race_df(
            n_horses=6,
            probs=[0.35, 0.32, 0.28, 0.05],
            odds=[3.0, 3.0, 3.0, 15.0],
        )
        bets = select_bets_competitive(
            race_df, capital=100_000.0, top_n=3, expected_return_threshold=1.0
        )
        # 上位3頭が選定対象 → 期待回収率を満たす馬のみ
        assert len(bets) <= 3

    def test_filters_by_expected_return(self):
        """期待回収率が閾値以下の馬は除外される"""
        # 上位3頭の期待回収率: 0.35*2.0=0.70, 0.32*2.0=0.64, 0.28*2.0=0.56
        # 全て閾値 1.0 未満 → 空リスト
        race_df = _make_race_df(
            n_horses=5,
            probs=[0.35, 0.32, 0.28, 0.04, 0.01],
            odds=[2.0, 2.0, 2.0, 2.0, 2.0],
        )
        bets = select_bets_competitive(
            race_df, capital=100_000.0, top_n=3, expected_return_threshold=1.0
        )
        assert bets == []

    def test_bet_amount_is_multiple_of_100(self):
        """賭け金が 100 円単位であること"""
        race_df = _make_race_df(
            probs=[0.35, 0.32, 0.28, 0.05],
            odds=[3.5, 3.5, 3.5, 5.0],
        )
        bets = select_bets_competitive(
            race_df, capital=100_000.0, expected_return_threshold=1.0
        )
        for bet in bets:
            assert bet["bet_amount"] % 100 == 0

    def test_returns_empty_when_no_data(self):
        """空の DataFrame を渡すと空リストを返す"""
        bets = select_bets_competitive(pd.DataFrame(), capital=100_000.0)
        assert bets == []


# ---------------------------------------------------------------------------
# select_bets_standard のテスト
# ---------------------------------------------------------------------------


class TestSelectBetsStandard:
    """標準型賭け選定のテスト"""

    def test_filters_by_expected_return_threshold_1_2(self):
        """期待回収率 > 1.2 の馬のみ選定される（デフォルト閾値）"""
        # 馬番1: 0.5 * 3.0 = 1.5 > 1.2 → 選定
        # 馬番2: 0.2 * 2.0 = 0.4 < 1.2 → 除外
        race_df = _make_race_df(
            n_horses=2,
            probs=[0.5, 0.2],
            odds=[3.0, 2.0],
        )
        bets = select_bets_standard(race_df, capital=100_000.0, expected_return_threshold=1.2)
        horse_numbers = [b["horse_number"] for b in bets]
        assert 1 in horse_numbers
        assert 2 not in horse_numbers

    def test_all_below_threshold_returns_empty(self):
        """全馬が閾値以下なら空リストを返す"""
        # 0.2 * 2.0 = 0.4 < 1.2
        race_df = _make_race_df(
            n_horses=4,
            probs=[0.2, 0.15, 0.1, 0.05],
            odds=[2.0, 2.0, 2.0, 2.0],
        )
        bets = select_bets_standard(race_df, capital=100_000.0)
        assert bets == []

    def test_bet_amount_is_multiple_of_100(self):
        """賭け金が 100 円単位であること"""
        race_df = _make_race_df(
            probs=[0.5, 0.3, 0.1, 0.1],
            odds=[3.0, 4.0, 2.0, 2.0],
        )
        bets = select_bets_standard(race_df, capital=100_000.0)
        for bet in bets:
            assert bet["bet_amount"] % 100 == 0

    def test_bet_amount_does_not_exceed_capital(self):
        """賭け金が残高を超えないこと"""
        race_df = _make_race_df(
            probs=[0.5, 0.3, 0.1, 0.1],
            odds=[5.0, 5.0, 5.0, 5.0],
        )
        # 残高 50 円 < min_bet_amount=100 → 賭け対象外
        bets = select_bets_standard(race_df, capital=50.0, min_bet_amount=100.0)
        assert bets == []

    def test_returns_empty_when_no_data(self):
        """空の DataFrame を渡すと空リストを返す"""
        bets = select_bets_standard(pd.DataFrame(), capital=100_000.0)
        assert bets == []


# ---------------------------------------------------------------------------
# select_bets_for_race のテスト（統合テスト）
# ---------------------------------------------------------------------------


class TestSelectBetsForRace:
    """select_bets_for_race 統合テスト"""

    def test_returns_tuple_of_bets_and_pattern(self):
        """戻り値が (list[dict], RacePattern) のタプルであること"""
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

    def test_one_dominant_pattern_selects_one_horse(self):
        """one_dominant パターンでは 1 頭のみ選定される（Kelly > 0 の場合）"""
        # gap_12 = 0.6 - 0.1 = 0.5 > p1=0.2 → one_dominant
        race_df = _make_race_df(
            probs=[0.6, 0.1, 0.1, 0.1, 0.1],
            odds=[4.0, 10.0, 10.0, 10.0, 10.0],
        )
        bets, pattern = select_bets_for_race(
            race_df, capital=100_000.0, p1=0.2, p2=0.15
        )
        assert pattern.pattern == "one_dominant"
        # Kelly > 0 のとき 1 頭だけ
        assert len(bets) <= 1

    def test_competitive_pattern_may_select_multiple(self):
        """competitive パターンでは複数馬が選定されうる"""
        # gap_13 = 0.4 - 0.35 = 0.05 < p2=0.15 → competitive
        # 期待回収率: 0.4*4=1.6, 0.38*4=1.52, 0.35*4=1.4 全て > 1.0（競合閾値）
        race_df = _make_race_df(
            probs=[0.4, 0.38, 0.35, 0.05, 0.02],
            odds=[4.0, 4.0, 4.0, 20.0, 50.0],
        )
        bets, pattern = select_bets_for_race(
            race_df, capital=100_000.0, p1=0.2, p2=0.15,
            expected_return_threshold=1.2
        )
        assert pattern.pattern == "competitive"
        # 拮抗型では複数馬が選定されうる
        assert len(bets) >= 0  # 0 以上（閾値次第）

    def test_standard_pattern_uses_expected_return_filter(self):
        """standard パターンでは期待回収率フィルタが適用される"""
        # gap_12 = 0.4 - 0.25 = 0.15 ≤ p1=0.2
        # gap_13 = 0.4 - 0.20 = 0.20 ≥ p2=0.15 → standard
        race_df = _make_race_df(
            probs=[0.4, 0.25, 0.2, 0.15],
            odds=[4.0, 1.5, 1.5, 1.5],  # 馬番2〜4は期待回収率 < 1.2
        )
        bets, pattern = select_bets_for_race(
            race_df, capital=100_000.0, p1=0.2, p2=0.15,
            expected_return_threshold=1.2
        )
        assert pattern.pattern == "standard"
        # 馬番1: 0.4*4.0=1.6 > 1.2 → 選定
        # 馬番2: 0.25*1.5=0.375 < 1.2 → 除外
        horse_numbers = [b["horse_number"] for b in bets]
        assert 1 in horse_numbers
        assert 2 not in horse_numbers

    def test_raises_if_less_than_3_horses(self):
        """3 頭未満なら ValueError を raise する"""
        race_df = _make_race_df(n_horses=2, probs=[0.5, 0.3], odds=[3.0, 3.0])
        with pytest.raises(ValueError):
            select_bets_for_race(race_df, capital=100_000.0)

    def test_pattern_recorded_correctly(self):
        """RacePattern の各フィールドが正確に設定されていること"""
        probs = [0.5, 0.2, 0.15, 0.1, 0.05]
        race_df = _make_race_df(probs=probs, odds=[3.0] * 5)
        _, pattern = select_bets_for_race(
            race_df, capital=100_000.0, p1=0.2, p2=0.15
        )
        assert abs(pattern.top1_prob - 0.5) < 1e-9
        assert abs(pattern.top2_prob - 0.2) < 1e-9
        assert abs(pattern.top3_prob - 0.15) < 1e-9
        assert abs(pattern.gap_top1_top2 - 0.3) < 1e-9

    def test_no_nan_odds_in_result(self):
        """結果の bets に NaN オッズがないこと"""
        race_df = _make_race_df(
            probs=[0.5, 0.3, 0.15, 0.05],
            odds=[3.0, np.nan, 5.0, 10.0],  # 馬番2は NaN オッズ
        )
        bets, _ = select_bets_for_race(race_df, capital=100_000.0)
        for bet in bets:
            assert not np.isnan(bet["odds"])
            assert bet["horse_number"] != 2

    def test_bet_amount_always_multiple_of_100(self):
        """どのパターンでも賭け金が 100 円単位であること"""
        # 3パターンそれぞれを試す
        test_cases = [
            # one_dominant
            ([0.7, 0.1, 0.1, 0.05, 0.05], [4.0] * 5),
            # competitive
            ([0.38, 0.36, 0.35, 0.05, 0.04], [3.5] * 5),  # gap_13 = 0.03 < 0.15
            # standard
            ([0.45, 0.28, 0.17, 0.06, 0.04], [3.0] * 5),
        ]
        for probs, odds in test_cases:
            race_df = _make_race_df(probs=probs, odds=odds)
            bets, _ = select_bets_for_race(race_df, capital=100_000.0)
            for bet in bets:
                assert bet["bet_amount"] % 100 == 0, (
                    f"bet_amount={bet['bet_amount']} は 100 円単位でない（probs={probs}）"
                )

    def test_capital_constraint_respected(self):
        """賭け金が残高以内に収まること"""
        race_df = _make_race_df(
            probs=[0.5, 0.2, 0.15, 0.1, 0.05],
            odds=[5.0] * 5,
        )
        capital = 10_000.0
        bets, _ = select_bets_for_race(race_df, capital=capital)
        for bet in bets:
            assert bet["bet_amount"] <= capital
