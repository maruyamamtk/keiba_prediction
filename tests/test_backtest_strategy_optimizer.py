"""
StrategyOptimizer のユニットテスト

テスト対象:
  - __init__: 払戻マップの構築
  - _run_simulation: シミュレーション実行（パターン別集計含む）
  - best_params: 最良パラメータの選定
  - filter_by_goals: 目標達成フィルタ
  - summary_by_pattern: パターン別サマリー
"""

from __future__ import annotations

import datetime

import pandas as pd
import pytest

from src.backtest.strategy_optimizer import OptimizationResult, StrategyOptimizer


# ---------------------------------------------------------------------------
# テストデータ生成ヘルパー
# ---------------------------------------------------------------------------


def _make_predictions_df(
    n_races: int = 3,
    n_horses: int = 5,
    win_place_prob: float = 0.35,
    odds: float = 2.8,
    finish_offset: int = 0,
) -> pd.DataFrame:
    """
    テスト用予測 DataFrame を生成する

    Args:
        n_races: レース数
        n_horses: 1レースあたり馬数
        win_place_prob: 複勝率（全馬共通）
        odds: 複勝オッズ（全馬共通）
        finish_offset: 着順の基準オフセット（0 なら馬番そのまま）
    """
    rows = []
    for r in range(n_races):
        race_id = f"race_{r:03d}"
        race_date = datetime.date(2024, 1, 7 + r)
        for h in range(1, n_horses + 1):
            rows.append(
                {
                    "race_id": race_id,
                    "race_date": race_date,
                    "horse_id": f"horse_{r}_{h}",
                    "horse_number": h,
                    "win_place_prob": win_place_prob,
                    "place_odds": odds,
                    "finish_position": h + finish_offset,
                }
            )
    return pd.DataFrame(rows)


def _make_payouts_df(
    race_ids: list[str],
    horse_numbers: list[int],
    payout_amount: int = 280,
) -> pd.DataFrame:
    """テスト用払戻 DataFrame を生成する"""
    rows = []
    for race_id in race_ids:
        for hn in horse_numbers:
            rows.append(
                {
                    "race_id": race_id,
                    "horse_number_1": hn,
                    "payout_amount": payout_amount,
                    "bet_type": "place",
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# __init__ のテスト
# ---------------------------------------------------------------------------


class TestStrategyOptimizerInit:
    def test_payout_map_is_built_from_place_bets(self):
        """place の払戻マップが正しく構築される"""
        predictions_df = _make_predictions_df()
        payouts_df = _make_payouts_df(
            ["race_000", "race_001"], horse_numbers=[1, 2], payout_amount=300
        )
        optimizer = StrategyOptimizer(predictions_df, payouts_df)
        assert ("race_000", 1) in optimizer._payout_map
        assert optimizer._payout_map[("race_000", 1)] == 300

    def test_non_place_bets_are_excluded_from_payout_map(self):
        """複勝以外（単勝など）は払戻マップに含まれない"""
        predictions_df = _make_predictions_df()
        payouts_df = pd.DataFrame(
            [
                {
                    "race_id": "race_000",
                    "horse_number_1": 1,
                    "payout_amount": 500,
                    "bet_type": "win",
                }
            ]
        )
        optimizer = StrategyOptimizer(predictions_df, payouts_df)
        assert len(optimizer._payout_map) == 0

    def test_none_payouts_gives_empty_payout_map(self):
        """payouts_df が None でも初期化できる"""
        predictions_df = _make_predictions_df()
        optimizer = StrategyOptimizer(predictions_df, None)
        assert optimizer._payout_map == {}


# ---------------------------------------------------------------------------
# _run_simulation のテスト
# ---------------------------------------------------------------------------


class TestStrategyOptimizerRunSimulation:
    def test_returns_tuple_of_dataframe_and_pattern_stats(self):
        """戻り値が (DataFrame, dict) のタプルであること"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None)
        result = optimizer._run_simulation(
            p1=0.2, p2=0.15, expected_return_threshold=1.2,
            kelly_fraction=0.25, max_bet_ratio=0.05,
        )
        assert isinstance(result, tuple) and len(result) == 2
        history_df, pattern_stats = result
        assert isinstance(history_df, pd.DataFrame)
        assert isinstance(pattern_stats, dict)

    def test_pattern_stats_has_three_keys(self):
        """pattern_stats に3パターンのキーが存在する"""
        df = _make_predictions_df()
        optimizer = StrategyOptimizer(df, None)
        _, pattern_stats = optimizer._run_simulation(
            p1=0.2, p2=0.15, expected_return_threshold=1.2,
            kelly_fraction=0.25, max_bet_ratio=0.05,
        )
        assert set(pattern_stats.keys()) == {"one_dominant", "competitive", "standard"}

    def test_bets_produced_when_threshold_is_low(self):
        """閾値が低ければ賭けが発生する"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None)
        history_df, _ = optimizer._run_simulation(
            p1=0.2, p2=0.15, expected_return_threshold=1.0,
            kelly_fraction=0.25, max_bet_ratio=0.05,
        )
        assert len(history_df) > 0

    def test_no_bets_when_expected_return_too_high(self):
        """期待回収率閾値が高すぎると賭けが発生しない"""
        # win_place_prob=0.2, odds=2.0 → expected_return=0.4
        df = _make_predictions_df(win_place_prob=0.2, odds=2.0)
        optimizer = StrategyOptimizer(df, None)
        history_df, _ = optimizer._run_simulation(
            p1=0.2, p2=0.15, expected_return_threshold=5.0,
            kelly_fraction=0.25, max_bet_ratio=0.05,
        )
        assert len(history_df) == 0

    def test_race_with_all_nan_finish_is_skipped(self):
        """finish_position が全て NaN のレースはスキップされる"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        # race_000 の着順を全て NaN にする
        df.loc[df["race_id"] == "race_000", "finish_position"] = float("nan")
        optimizer = StrategyOptimizer(df, None)
        history_df, _ = optimizer._run_simulation(
            p1=0.2, p2=0.15, expected_return_threshold=1.0,
            kelly_fraction=0.25, max_bet_ratio=0.05,
        )
        # race_001 のみ処理されるため、race_000 の馬は記録されない
        if len(history_df) > 0:
            assert "race_000" not in history_df["race_id"].values

    def test_horse_with_finish_zero_is_skipped(self):
        """着順 0 の馬（出走取消等）は賭け対象に含まれない"""
        df = _make_predictions_df(n_races=1, n_horses=5, win_place_prob=0.6, odds=3.0)
        # 1番馬の着順を 0 にする
        df.loc[df["horse_number"] == 1, "finish_position"] = 0
        optimizer = StrategyOptimizer(df, None)
        history_df, _ = optimizer._run_simulation(
            p1=0.2, p2=0.15, expected_return_threshold=1.0,
            kelly_fraction=0.25, max_bet_ratio=0.05,
        )
        if len(history_df) > 0:
            assert 1 not in history_df["horse_number"].values

    def test_payout_map_used_when_available(self):
        """払戻マップが存在する場合はオッズ推定ではなく払戻額を使用する"""
        df = _make_predictions_df(n_races=1, n_horses=5, win_place_prob=0.6, odds=3.0)
        payouts_df = _make_payouts_df(["race_000"], [1, 2, 3], payout_amount=400)
        optimizer = StrategyOptimizer(df, payouts_df)
        history_df, _ = optimizer._run_simulation(
            p1=0.2, p2=0.15, expected_return_threshold=1.0,
            kelly_fraction=0.25, max_bet_ratio=0.05,
        )
        if len(history_df) > 0:
            hit_rows = history_df[history_df["is_hit"] & (history_df["payout_per_100"].notna())]
            for _, row in hit_rows.iterrows():
                expected = row["bet_amount"] * (row["payout_per_100"] / 100.0)
                assert abs(row["return_amount"] - expected) < 1e-6

    def test_capital_changes_after_simulation(self):
        """シミュレーション後に資金が変動する（賭けが発生した場合）"""
        df = _make_predictions_df(n_races=3, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, initial_capital=100_000.0)
        history_df, _ = optimizer._run_simulation(
            p1=0.2, p2=0.15, expected_return_threshold=1.0,
            kelly_fraction=0.25, max_bet_ratio=0.05,
        )
        if len(history_df) > 0:
            final_capital = history_df["capital_after"].iloc[-1]
            assert final_capital != 100_000.0


# ---------------------------------------------------------------------------
# best_params のテスト
# ---------------------------------------------------------------------------


class TestStrategyOptimizerBestParams:
    def _make_results(self) -> list[OptimizationResult]:
        return [
            OptimizationResult(
                params={"p1": 0.1}, recovery_rate=90.0, hit_rate=30.0,
                max_drawdown=20.0, sharpe_ratio=0.5, total_bets=100,
            ),
            OptimizationResult(
                params={"p1": 0.2}, recovery_rate=110.0, hit_rate=35.0,
                max_drawdown=15.0, sharpe_ratio=0.8, total_bets=80,
            ),
            OptimizationResult(
                params={"p1": 0.3}, recovery_rate=95.0, hit_rate=28.0,
                max_drawdown=25.0, sharpe_ratio=0.3, total_bets=60,
            ),
        ]

    def test_best_params_by_recovery_rate(self):
        """recovery_rate が最大のパラメータが返る"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None)
        results = self._make_results()
        best = optimizer.best_params(results, metric="recovery_rate")
        assert best.recovery_rate == 110.0

    def test_best_params_by_max_drawdown_is_minimum(self):
        """max_drawdown は小さいほど良い（最小値が選ばれる）"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None)
        results = self._make_results()
        best = optimizer.best_params(results, metric="max_drawdown")
        assert best.max_drawdown == 15.0

    def test_best_params_raises_on_empty_results(self):
        """空リストを渡すと ValueError が発生する"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None)
        with pytest.raises(ValueError):
            optimizer.best_params([], metric="recovery_rate")


# ---------------------------------------------------------------------------
# filter_by_goals のテスト
# ---------------------------------------------------------------------------


class TestStrategyOptimizerFilterByGoals:
    def _make_results(self) -> list[OptimizationResult]:
        return [
            OptimizationResult(
                params={"p1": 0.1}, recovery_rate=105.0, hit_rate=32.0,
                max_drawdown=25.0, sharpe_ratio=0.6, total_bets=100,
            ),
            OptimizationResult(
                params={"p1": 0.2}, recovery_rate=98.0, hit_rate=28.0,
                max_drawdown=20.0, sharpe_ratio=0.4, total_bets=80,
            ),
            OptimizationResult(
                params={"p1": 0.3}, recovery_rate=112.0, hit_rate=36.0,
                max_drawdown=35.0, sharpe_ratio=0.9, total_bets=60,
            ),
            OptimizationResult(
                params={"p1": 0.4}, recovery_rate=103.0, hit_rate=31.0,
                max_drawdown=28.0, sharpe_ratio=0.7, total_bets=70,
            ),
        ]

    def test_returns_only_results_meeting_both_goals(self):
        """回収率≥100% かつ ドローダウン≤30% の結果のみ返す"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None)
        filtered = optimizer.filter_by_goals(self._make_results())
        assert all(r.recovery_rate >= 100.0 for r in filtered)
        assert all(r.max_drawdown <= 30.0 for r in filtered)

    def test_returns_empty_when_no_results_meet_goals(self):
        """条件を満たす結果がない場合は空リストを返す"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None)
        results = [
            OptimizationResult(
                params={}, recovery_rate=80.0, hit_rate=20.0,
                max_drawdown=40.0, sharpe_ratio=0.1, total_bets=50,
            )
        ]
        assert optimizer.filter_by_goals(results) == []

    def test_filtered_results_are_sorted_by_recovery_rate(self):
        """フィルタ後の結果は回収率降順でソートされる"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None)
        filtered = optimizer.filter_by_goals(self._make_results())
        rates = [r.recovery_rate for r in filtered]
        assert rates == sorted(rates, reverse=True)

    def test_custom_thresholds_work(self):
        """カスタム閾値が正しく機能する"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None)
        # min_recovery_rate=110, max_max_drawdown=30 → recovery_rate=112 かつ drawdown=35 の結果は除外
        filtered = optimizer.filter_by_goals(
            self._make_results(), min_recovery_rate=110.0, max_max_drawdown=30.0
        )
        # recovery_rate=112 だが max_drawdown=35 > 30 なので対象外
        assert all(r.max_drawdown <= 30.0 for r in filtered)


# ---------------------------------------------------------------------------
# summary_by_pattern のテスト
# ---------------------------------------------------------------------------


class TestStrategyOptimizerSummaryByPattern:
    def _make_result_with_breakdown(
        self, one_dominant_rr: float, competitive_rr: float, standard_rr: float
    ) -> OptimizationResult:
        return OptimizationResult(
            params={},
            recovery_rate=100.0,
            hit_rate=30.0,
            max_drawdown=20.0,
            sharpe_ratio=0.5,
            total_bets=100,
            pattern_breakdown={
                "one_dominant": {
                    "bets": 10, "hits": 3, "bet_amount": 10000.0,
                    "return_amount": 10000.0 * one_dominant_rr / 100,
                    "recovery_rate": one_dominant_rr,
                },
                "competitive": {
                    "bets": 20, "hits": 6, "bet_amount": 20000.0,
                    "return_amount": 20000.0 * competitive_rr / 100,
                    "recovery_rate": competitive_rr,
                },
                "standard": {
                    "bets": 15, "hits": 5, "bet_amount": 15000.0,
                    "return_amount": 15000.0 * standard_rr / 100,
                    "recovery_rate": standard_rr,
                },
            },
        )

    def test_returns_dataframe_with_pattern_index(self):
        """DataFrame がパターン名をインデックスに持つ"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None)
        results = [self._make_result_with_breakdown(110.0, 95.0, 105.0)]
        summary = optimizer.summary_by_pattern(results)
        assert isinstance(summary, pd.DataFrame)
        assert "one_dominant" in summary.index

    def test_empty_results_returns_empty_dataframe(self):
        """空リストを渡すと空 DataFrame を返す"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None)
        summary = optimizer.summary_by_pattern([])
        assert isinstance(summary, pd.DataFrame)
        assert len(summary) == 0

    def test_avg_recovery_rate_is_calculated(self):
        """avg_recovery_rate カラムが正しく集計される"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None)
        results = [
            self._make_result_with_breakdown(100.0, 90.0, 110.0),
            self._make_result_with_breakdown(120.0, 80.0, 100.0),
        ]
        summary = optimizer.summary_by_pattern(results)
        assert "avg_recovery_rate" in summary.columns
        # one_dominant: (100 + 120) / 2 = 110
        assert abs(summary.loc["one_dominant", "avg_recovery_rate"] - 110.0) < 1e-6
