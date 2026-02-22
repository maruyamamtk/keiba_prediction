"""
バックテスト評価指標のテスト
"""

from __future__ import annotations

import datetime

import numpy as np
import pandas as pd
import pytest

from src.backtest.metrics import (
    compute_metrics,
    hit_rate,
    max_drawdown,
    recovery_rate,
    sharpe_ratio,
)


# ---------------------------------------------------------------------------
# テスト用データ生成
# ---------------------------------------------------------------------------


def _make_history(
    n: int = 5,
    bet_amount: float = 1000.0,
    return_amounts: list[float] | None = None,
    capital_values: list[float] | None = None,
    is_hits: list[bool] | None = None,
) -> pd.DataFrame:
    """テスト用賭け記録 DataFrame を生成する"""
    if return_amounts is None:
        return_amounts = [0.0] * n
    if capital_values is None:
        capital_values = [100_000.0 - bet_amount * (i + 1) for i in range(n)]
    if is_hits is None:
        is_hits = [r > 0 for r in return_amounts]

    rows = []
    for i in range(n):
        rows.append({
            "race_id": f"race_{i:03d}",
            "race_date": datetime.date(2023, 1, 7 + i),
            "horse_id": f"horse_{i}",
            "horse_number": 1,
            "bet_amount": bet_amount,
            "return_amount": return_amounts[i],
            "profit": return_amounts[i] - bet_amount,
            "capital_after": capital_values[i],
            "is_hit": is_hits[i],
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# recovery_rate のテスト
# ---------------------------------------------------------------------------


class TestRecoveryRate:
    def test_zero_bets_returns_zero(self):
        df = pd.DataFrame({"bet_amount": [], "return_amount": []})
        assert recovery_rate(df) == 0.0

    def test_zero_total_bet_returns_zero(self):
        df = pd.DataFrame({"bet_amount": [0.0], "return_amount": [100.0]})
        assert recovery_rate(df) == 0.0

    def test_all_miss(self):
        """全外れ → 回収率 0%"""
        df = _make_history(n=3, return_amounts=[0.0, 0.0, 0.0])
        assert recovery_rate(df) == 0.0

    def test_breakeven(self):
        """回収率 100% (損益ゼロ)"""
        df = _make_history(n=2, bet_amount=1000.0, return_amounts=[1000.0, 1000.0])
        assert abs(recovery_rate(df) - 100.0) < 1e-6

    def test_positive_return(self):
        """賭け金より払戻が多い場合 > 100%"""
        df = _make_history(n=2, bet_amount=1000.0, return_amounts=[1500.0, 1200.0])
        # 合計 bet=2000, return=2700 → 135%
        assert abs(recovery_rate(df) - 135.0) < 1e-6

    def test_partial_hit(self):
        """一部的中の場合"""
        df = _make_history(n=4, bet_amount=1000.0, return_amounts=[2000.0, 0.0, 0.0, 0.0])
        # bet=4000, return=2000 → 50%
        assert abs(recovery_rate(df) - 50.0) < 1e-6


# ---------------------------------------------------------------------------
# hit_rate のテスト
# ---------------------------------------------------------------------------


class TestHitRate:
    def test_zero_bets_returns_zero(self):
        df = pd.DataFrame({"is_hit": []})
        assert hit_rate(df) == 0.0

    def test_all_miss(self):
        df = _make_history(n=3, is_hits=[False, False, False])
        assert hit_rate(df) == 0.0

    def test_all_hit(self):
        df = _make_history(n=4, is_hits=[True, True, True, True])
        assert abs(hit_rate(df) - 100.0) < 1e-6

    def test_half_hit(self):
        df = _make_history(n=4, is_hits=[True, False, True, False])
        assert abs(hit_rate(df) - 50.0) < 1e-6

    def test_one_hit_out_of_five(self):
        df = _make_history(n=5, is_hits=[True, False, False, False, False])
        assert abs(hit_rate(df) - 20.0) < 1e-6


# ---------------------------------------------------------------------------
# max_drawdown のテスト
# ---------------------------------------------------------------------------


class TestMaxDrawdown:
    def test_empty_returns_zero(self):
        df = pd.DataFrame({"capital_after": []})
        assert max_drawdown(df, initial_capital=100_000.0) == 0.0

    def test_monotone_increase_no_drawdown(self):
        """資金が単調増加の場合はドローダウン 0"""
        capitals = [100_100.0, 100_200.0, 100_300.0]
        df = pd.DataFrame({"capital_after": capitals})
        result = max_drawdown(df, initial_capital=100_000.0)
        assert result == 0.0

    def test_known_drawdown(self):
        """
        initial=100000, 推移: 110000 → 90000 → 100000
        ピーク=110000 からの最大下落: (110000-90000)/110000 * 100 ≈ 18.18%
        """
        capitals = [110_000.0, 90_000.0, 100_000.0]
        df = pd.DataFrame({"capital_after": capitals})
        result = max_drawdown(df, initial_capital=100_000.0)
        expected = (110_000.0 - 90_000.0) / 110_000.0 * 100.0
        assert abs(result - expected) < 1e-6

    def test_initial_capital_included(self):
        """初期資金からの下落も考慮される"""
        # 初期100000 → 80000 → 90000
        capitals = [80_000.0, 90_000.0]
        df = pd.DataFrame({"capital_after": capitals})
        result = max_drawdown(df, initial_capital=100_000.0)
        # ピーク=100000, 最安=80000 → (100000-80000)/100000*100 = 20%
        assert abs(result - 20.0) < 1e-6


# ---------------------------------------------------------------------------
# sharpe_ratio のテスト
# ---------------------------------------------------------------------------


class TestSharpeRatio:
    def test_empty_returns_zero(self):
        df = pd.DataFrame({"race_date": [], "profit": []})
        assert sharpe_ratio(df) == 0.0

    def test_single_record_returns_zero(self):
        """1週分のデータでは標準偏差が計算できず 0"""
        df = _make_history(n=1)
        result = sharpe_ratio(df)
        assert result == 0.0

    def test_constant_profit_returns_zero(self):
        """毎週同じ利益（標準偏差 0）の場合は 0"""
        df = _make_history(
            n=4,
            return_amounts=[200.0, 200.0, 200.0, 200.0],
            capital_values=[100_200.0, 100_400.0, 100_600.0, 100_800.0],
        )
        # 同じ利益 → 週次利益の std=0 → sharpe=0
        result = sharpe_ratio(df)
        assert result == 0.0

    def test_positive_sharpe_for_consistent_gains(self):
        """安定した利益が続く場合はシャープレシオが正"""
        rows = []
        cap = 100_000.0
        # 毎週 +500, +600, +550 の利益（異なる週）
        for i in range(6):
            profit = 500.0 + (i % 3) * 50.0
            cap += profit
            rows.append({
                "race_id": f"race_{i}",
                "race_date": datetime.date(2023, 1, 1) + datetime.timedelta(weeks=i),
                "horse_id": f"h_{i}",
                "horse_number": 1,
                "bet_amount": 1000.0,
                "return_amount": 1000.0 + profit,
                "profit": profit,
                "capital_after": cap,
                "is_hit": True,
            })
        df = pd.DataFrame(rows)
        result = sharpe_ratio(df)
        assert result > 0


# ---------------------------------------------------------------------------
# compute_metrics のテスト
# ---------------------------------------------------------------------------


class TestComputeMetrics:
    def test_empty_returns_default_structure(self):
        """空の DataFrame は全 0 の指標を返す"""
        result = compute_metrics(pd.DataFrame(), initial_capital=100_000.0)
        assert result["total_bets"] == 0
        assert result["recovery_rate"] == 0.0
        assert result["final_capital"] == 100_000.0

    def test_returns_all_required_keys(self):
        """必要なキーが全て含まれること"""
        df = _make_history(
            n=3,
            return_amounts=[0.0, 1500.0, 0.0],
            capital_values=[99_000.0, 100_400.0, 99_400.0],
            is_hits=[False, True, False],
        )
        result = compute_metrics(df, initial_capital=100_000.0)
        required_keys = {
            "recovery_rate", "hit_rate", "max_drawdown", "sharpe_ratio",
            "total_bets", "total_hits", "total_bet_amount",
            "total_return_amount", "profit", "final_capital",
        }
        assert required_keys.issubset(set(result.keys()))

    def test_total_bets_count(self):
        """total_bets が賭け数と一致する"""
        df = _make_history(n=7)
        result = compute_metrics(df, initial_capital=100_000.0)
        assert result["total_bets"] == 7

    def test_final_capital_is_last_capital_after(self):
        """final_capital が最後の capital_after と一致する"""
        capitals = [99_000.0, 98_000.0, 102_000.0]
        df = _make_history(
            n=3,
            return_amounts=[0.0, 0.0, 3_000.0],
            capital_values=capitals,
            is_hits=[False, False, True],
        )
        result = compute_metrics(df, initial_capital=100_000.0)
        assert result["final_capital"] == 102_000.0

    def test_profit_equals_return_minus_bet(self):
        """profit = total_return - total_bet"""
        df = _make_history(
            n=3,
            bet_amount=1_000.0,
            return_amounts=[1_500.0, 0.0, 0.0],
            capital_values=[100_500.0, 99_500.0, 98_500.0],
            is_hits=[True, False, False],
        )
        result = compute_metrics(df, initial_capital=100_000.0)
        assert abs(result["profit"] - (result["total_return_amount"] - result["total_bet_amount"])) < 1e-6
