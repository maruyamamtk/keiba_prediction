"""
BacktestSimulator のテスト
"""

from __future__ import annotations

import datetime

import numpy as np
import pandas as pd
import pytest

from src.backtest.simulator import (
    BacktestSimulator,
    BetRecord,
    fractional_kelly,
    kelly_criterion,
)


# ---------------------------------------------------------------------------
# kelly_criterion のテスト
# ---------------------------------------------------------------------------


class TestKellyCriterion:
    def test_positive_expected_value(self):
        """期待値がプラスの場合に正の値を返す"""
        result = kelly_criterion(win_prob=0.5, odds=3.0)
        assert result > 0

    def test_negative_expected_value_returns_zero(self):
        """期待値がマイナスの場合は 0 を返す"""
        # p=0.2, odds=2.0 → Kelly = (0.2 * 1 - 0.8) / 1 = -0.6 < 0
        result = kelly_criterion(win_prob=0.2, odds=2.0)
        assert result == 0.0

    def test_odds_equal_one_returns_zero(self):
        """オッズが 1.0 の場合は 0 を返す（賭けても意味なし）"""
        result = kelly_criterion(win_prob=0.8, odds=1.0)
        assert result == 0.0

    def test_odds_less_than_one_returns_zero(self):
        """オッズが 1 未満は 0 を返す"""
        result = kelly_criterion(win_prob=0.9, odds=0.9)
        assert result == 0.0

    def test_known_value(self):
        """既知の計算値を確認する
        p=0.4, odds=3.0
        Kelly = (0.4 * 2 - 0.6) / 2 = (0.8 - 0.6) / 2 = 0.1
        """
        result = kelly_criterion(win_prob=0.4, odds=3.0)
        assert abs(result - 0.1) < 1e-9

    def test_result_between_zero_and_one(self):
        """結果が 0〜1 の範囲に収まる"""
        result = kelly_criterion(win_prob=0.8, odds=2.0)
        assert 0.0 <= result <= 1.0


class TestFractionalKelly:
    def test_fraction_applied(self):
        """fraction を掛けた値になること"""
        full = kelly_criterion(win_prob=0.4, odds=3.0)
        frac = fractional_kelly(win_prob=0.4, odds=3.0, fraction=0.25)
        assert abs(frac - full * 0.25) < 1e-9

    def test_default_fraction_is_025(self):
        """デフォルト係数が 0.25 であること"""
        full = kelly_criterion(win_prob=0.4, odds=3.0)
        frac = fractional_kelly(win_prob=0.4, odds=3.0)
        assert abs(frac - full * 0.25) < 1e-9

    def test_zero_fraction_returns_zero(self):
        """fraction=0 は常に 0"""
        result = fractional_kelly(win_prob=0.5, odds=3.0, fraction=0.0)
        assert result == 0.0


# ---------------------------------------------------------------------------
# BacktestSimulator のテスト用データ生成
# ---------------------------------------------------------------------------


def _make_predictions(
    n_races: int = 3,
    n_horses: int = 5,
    win_place_prob: float = 0.35,
    odds: float = 2.8,
    finish_position_top3: bool = True,
) -> pd.DataFrame:
    """テスト用予測 DataFrame を生成する"""
    rows = []
    for r in range(n_races):
        race_id = f"race_{r:03d}"
        race_date = datetime.date(2023, 1, 7 + r)
        for h in range(1, n_horses + 1):
            finish = h if h <= 3 else h  # 1,2,3 が3着以内
            rows.append({
                "race_id": race_id,
                "race_date": race_date,
                "horse_id": f"horse_{r}_{h}",
                "horse_number": h,
                "win_place_prob": win_place_prob,
                "odds_yesterday": odds,
                "finish_position": finish,
            })
    return pd.DataFrame(rows)


def _make_payouts(
    race_ids: list[str],
    horse_numbers: list[int],
    payout_amount: int = 280,
) -> pd.DataFrame:
    """テスト用払戻 DataFrame を生成する"""
    rows = []
    for race_id in race_ids:
        for hn in horse_numbers:
            rows.append({
                "race_id": race_id,
                "horse_number_1": hn,
                "payout_amount": payout_amount,
                "bet_type": "place",
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# BacktestSimulator のテスト
# ---------------------------------------------------------------------------


class TestBacktestSimulatorRun:
    def test_missing_odds_column_returns_empty(self):
        """odds_column が存在しない場合は空 DataFrame を返す"""
        df = _make_predictions()
        df = df.drop(columns=["odds_yesterday"])
        sim = BacktestSimulator()
        result = sim.run(df)
        assert len(result) == 0

    def test_below_threshold_no_bets(self):
        """期待回収率が閾値未満の馬は全て除外される"""
        # win_place_prob=0.1, odds=2.0 → expected_return=0.2 < 1.2
        df = _make_predictions(win_place_prob=0.1, odds=2.0)
        sim = BacktestSimulator(expected_return_threshold=1.2)
        result = sim.run(df)
        assert len(result) == 0

    def test_above_threshold_produces_bets(self):
        """期待回収率が閾値超の馬は賭け対象になる"""
        # win_place_prob=0.5, odds=3.0 → expected_return=1.5 > 1.2
        df = _make_predictions(win_place_prob=0.5, odds=3.0, n_races=2, n_horses=3)
        sim = BacktestSimulator(expected_return_threshold=1.2)
        result = sim.run(df)
        assert len(result) > 0

    def test_bet_amount_is_multiple_of_100(self):
        """賭け金は 100 円単位であること"""
        df = _make_predictions(win_place_prob=0.5, odds=3.0)
        sim = BacktestSimulator()
        result = sim.run(df)
        for bet in result["bet_amount"]:
            assert bet % 100 == 0, f"bet_amount={bet} is not a multiple of 100"

    def test_bet_amount_does_not_exceed_max_ratio(self):
        """賭け金が max_bet_ratio × 資金を超えない"""
        initial = 100_000.0
        max_ratio = 0.05
        df = _make_predictions(win_place_prob=0.5, odds=3.0, n_races=1, n_horses=3)
        sim = BacktestSimulator(
            initial_capital=initial,
            max_bet_ratio=max_ratio,
        )
        result = sim.run(df)
        assert len(result) > 0
        first_bet = result["bet_amount"].iloc[0]
        assert first_bet <= initial * max_ratio

    def test_hit_when_finish_position_lte_3(self):
        """着順 ≤ 3 の馬は is_hit=True"""
        df = _make_predictions(win_place_prob=0.5, odds=3.0, n_races=1, n_horses=5)
        sim = BacktestSimulator()
        result = sim.run(df)
        for _, row in result.iterrows():
            if row["finish_position"] is not None and row["finish_position"] <= 3:
                assert row["is_hit"] is True or row["is_hit"] == 1

    def test_not_hit_when_finish_position_gt_3(self):
        """着順 > 3 の馬は is_hit=False"""
        df = _make_predictions(win_place_prob=0.5, odds=3.0, n_races=1, n_horses=5)
        sim = BacktestSimulator()
        result = sim.run(df)
        for _, row in result.iterrows():
            if row["finish_position"] is not None and row["finish_position"] > 3:
                assert row["is_hit"] is False or row["is_hit"] == 0

    def test_capital_decreases_on_miss(self):
        """外れた場合は資金が減少する"""
        # 全馬が着外になるようにする（finish_position=10）
        df = _make_predictions(win_place_prob=0.5, odds=3.0, n_races=1, n_horses=3)
        df["finish_position"] = 10  # 全馬着外
        sim = BacktestSimulator(initial_capital=100_000.0)
        result = sim.run(df)
        if len(result) > 0:
            assert result["capital_after"].iloc[-1] < 100_000.0

    def test_capital_increases_on_hit_with_payouts(self):
        """的中し払戻データがある場合は資金が増加しうる"""
        df = _make_predictions(win_place_prob=0.5, odds=3.0, n_races=1, n_horses=3)
        # 馬番1が1着
        df.loc[df["horse_number"] == 1, "finish_position"] = 1
        df.loc[df["horse_number"] != 1, "finish_position"] = 10

        payouts_df = _make_payouts(
            race_ids=["race_000"],
            horse_numbers=[1],
            payout_amount=300,  # 3.0倍
        )
        sim = BacktestSimulator(
            initial_capital=100_000.0,
            expected_return_threshold=1.0,
        )
        result = sim.run(df, payouts_df=payouts_df)
        # 馬番1が賭け対象で的中 → return_amount > 0
        hits = result[result["is_hit"] == True]
        if len(hits) > 0:
            assert hits["return_amount"].iloc[0] > 0

    def test_return_amount_uses_payout_data_when_available(self):
        """払戻データがある場合は payout_per_100 で return_amount を計算する"""
        df = _make_predictions(win_place_prob=0.5, odds=2.0, n_races=1, n_horses=2)
        df.loc[df["horse_number"] == 1, "finish_position"] = 1
        df.loc[df["horse_number"] == 2, "finish_position"] = 5

        # payout_amount=500 (5.0倍) なので odds=2.0 より高い払戻
        payouts_df = _make_payouts(["race_000"], [1], payout_amount=500)
        sim = BacktestSimulator(
            initial_capital=100_000.0,
            expected_return_threshold=0.5,
            odds_column="odds_yesterday",
        )
        result = sim.run(df, payouts_df=payouts_df)
        hit_row = result[result["horse_number"] == 1]
        if len(hit_row) > 0 and hit_row["is_hit"].iloc[0]:
            bet = hit_row["bet_amount"].iloc[0]
            expected = bet * (500 / 100.0)
            assert abs(hit_row["return_amount"].iloc[0] - expected) < 1.0

    def test_return_amount_uses_odds_when_no_payout_data(self):
        """払戻データがない場合は odds で return_amount を計算する"""
        df = _make_predictions(win_place_prob=0.5, odds=3.0, n_races=1, n_horses=2)
        df.loc[df["horse_number"] == 1, "finish_position"] = 1
        df.loc[df["horse_number"] == 2, "finish_position"] = 5

        sim = BacktestSimulator(
            initial_capital=100_000.0,
            expected_return_threshold=0.5,
        )
        result = sim.run(df, payouts_df=None)
        hit_row = result[result["horse_number"] == 1]
        if len(hit_row) > 0 and hit_row["is_hit"].iloc[0]:
            bet = hit_row["bet_amount"].iloc[0]
            odds = hit_row["odds"].iloc[0]
            expected = bet * odds
            assert abs(hit_row["return_amount"].iloc[0] - expected) < 1.0

    def test_output_columns(self):
        """出力 DataFrame が必要なカラムを持つ"""
        df = _make_predictions(win_place_prob=0.5, odds=3.0)
        sim = BacktestSimulator()
        result = sim.run(df)
        if len(result) > 0:
            expected_cols = {
                "race_id", "race_date", "horse_id", "horse_number",
                "win_place_prob", "odds", "expected_return", "kelly_frac",
                "bet_amount", "finish_position", "is_hit",
                "payout_per_100", "return_amount", "profit", "capital_after",
            }
            assert expected_cols.issubset(set(result.columns))

    def test_nan_odds_skipped(self):
        """オッズが NaN の馬はスキップされる"""
        df = _make_predictions(win_place_prob=0.5, odds=3.0, n_races=1, n_horses=3)
        df.loc[df["horse_number"] == 2, "odds_yesterday"] = float("nan")
        sim = BacktestSimulator(expected_return_threshold=0.5)
        result = sim.run(df)
        if len(result) > 0:
            assert 2 not in result["horse_number"].values

    def test_zero_odds_skipped(self):
        """オッズが 0 の馬はスキップされる"""
        df = _make_predictions(win_place_prob=0.5, odds=3.0, n_races=1, n_horses=3)
        df.loc[df["horse_number"] == 1, "odds_yesterday"] = 0.0
        sim = BacktestSimulator(expected_return_threshold=0.5)
        result = sim.run(df)
        if len(result) > 0:
            assert 1 not in result["horse_number"].values

    def test_capital_monotone_with_hit(self):
        """的中したレコードで capital_after が増加する"""
        df = _make_predictions(win_place_prob=0.5, odds=5.0, n_races=1, n_horses=2)
        df.loc[df["horse_number"] == 1, "finish_position"] = 1
        df.loc[df["horse_number"] == 2, "finish_position"] = 5

        sim = BacktestSimulator(
            initial_capital=100_000.0,
            expected_return_threshold=0.5,
        )
        result = sim.run(df, payouts_df=None)
        hit_rows = result[result["is_hit"] == True]
        for _, row in hit_rows.iterrows():
            assert row["profit"] > 0, "的中時は profit > 0 のはず"
