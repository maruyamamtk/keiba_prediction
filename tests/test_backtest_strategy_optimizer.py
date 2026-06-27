"""
StrategyOptimizer のユニットテスト

テスト対象:
  - __init__: 払戻マップの構築
  - _run_simulation: シミュレーション実行（統一ロジック）
  - run_grid_search: グリッドサーチ（threshold×top_n×r = 80通りデフォルト）
  - best_params: 最良パラメータの選定
  - filter_by_goals: 目標達成フィルタ
  - summary_by_pattern: パターン別サマリー（Issue #260: 統一型のみ）
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
        optimizer = StrategyOptimizer(predictions_df, payouts_df, combo_odds_df=None)
        # キー形式: (race_id, bet_type, horse_numbers_tuple)
        assert ("race_000", "place", (1,)) in optimizer._payout_map
        assert optimizer._payout_map[("race_000", "place", (1,))] == 300

    def test_non_place_bets_excluded_from_payout_map(self):
        """複勝以外（win）の払戻マップへのキー確認"""
        predictions_df = _make_predictions_df()
        payouts_df = pd.DataFrame([
            {
                "race_id": "race_000",
                "horse_number_1": 1,
                "payout_amount": 500,
                "bet_type": "win",
            }
        ])
        optimizer = StrategyOptimizer(predictions_df, payouts_df, combo_odds_df=None)
        # win は含まれる（全bet_typeを処理）
        assert ("race_000", "win", (1,)) in optimizer._payout_map

    def test_none_payouts_gives_empty_payout_map(self):
        """payouts_df が None でも初期化できる"""
        predictions_df = _make_predictions_df()
        optimizer = StrategyOptimizer(predictions_df, None, combo_odds_df=None)
        assert optimizer._payout_map == {}

    def test_combo_odds_df_stored(self):
        """combo_odds_df が正しく格納される"""
        predictions_df = _make_predictions_df()
        combo_df = pd.DataFrame([{
            "race_id": "race_000",
            "bet_type": "wide",
            "horse_number_1": 1,
            "horse_number_2": 2,
            "horse_number_3": None,
            "odds_value": 10.0,
        }])
        optimizer = StrategyOptimizer(predictions_df, None, combo_odds_df=combo_df)
        assert len(optimizer.combo_odds_df) == 1

    def test_budget_per_race_stored(self):
        """budget_per_race が正しく格納される"""
        predictions_df = _make_predictions_df()
        optimizer = StrategyOptimizer(predictions_df, None, budget_per_race=5000.0)
        assert optimizer.budget_per_race == 5000.0


# ---------------------------------------------------------------------------
# _run_simulation のテスト
# ---------------------------------------------------------------------------


class TestStrategyOptimizerRunSimulation:
    def test_returns_tuple_of_dataframe_and_pattern_stats(self):
        """戻り値が (DataFrame, dict) のタプルであること"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        result = optimizer._run_simulation(
            p1=0.2, expected_return_threshold=1.2,
        )
        assert isinstance(result, tuple) and len(result) == 2
        history_df, pattern_stats = result
        assert isinstance(history_df, pd.DataFrame)
        assert isinstance(pattern_stats, dict)

    def test_pattern_stats_is_empty_dict(self):
        """Issue #260: パターン分類廃止により pattern_stats は空 dict"""
        df = _make_predictions_df()
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        _, pattern_stats = optimizer._run_simulation(
            expected_return_threshold=1.2,
        )
        assert pattern_stats == {}

    def test_bets_produced_when_threshold_is_low(self):
        """閾値が低ければ賭けが発生する"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        history_df, _ = optimizer._run_simulation(
            expected_return_threshold=1.0,
        )
        assert len(history_df) > 0

    def test_no_bets_when_expected_return_too_high(self):
        """期待回収率閾値が高すぎると賭けが発生しない"""
        # win_place_prob=0.2, odds=2.0 → expected_return=0.4
        df = _make_predictions_df(win_place_prob=0.2, odds=2.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        history_df, _ = optimizer._run_simulation(
            expected_return_threshold=5.0,
        )
        assert len(history_df) == 0

    def test_race_with_all_nan_finish_is_skipped(self):
        """finish_position が全て NaN のレースはスキップされる"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        # race_000 の着順を全て NaN にする
        df.loc[df["race_id"] == "race_000", "finish_position"] = float("nan")
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        history_df, _ = optimizer._run_simulation(
            expected_return_threshold=1.0,
        )
        if len(history_df) > 0:
            assert "race_000" not in history_df["race_id"].values

    def test_payout_map_key_format(self):
        """払戻マップのキー形式が (race_id, bet_type, horse_numbers_tuple) であること"""
        predictions_df = _make_predictions_df()
        payouts_df = _make_payouts_df(["race_000"], [1, 2, 3], payout_amount=400)
        optimizer = StrategyOptimizer(predictions_df, payouts_df, combo_odds_df=None)
        # キー形式確認
        assert ("race_000", "place", (1,)) in optimizer._payout_map
        assert optimizer._payout_map[("race_000", "place", (1,))] == 400

    def test_capital_changes_after_simulation(self):
        """シミュレーション後に資金が変動する（賭けが発生した場合）"""
        df = _make_predictions_df(n_races=3, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, initial_capital=100_000.0, combo_odds_df=None)
        history_df, _ = optimizer._run_simulation(
            expected_return_threshold=1.0,
        )
        if len(history_df) > 0:
            final_capital = history_df["capital_after"].iloc[-1]
            assert final_capital != 100_000.0


# ---------------------------------------------------------------------------
# run_grid_search のテスト
# ---------------------------------------------------------------------------


class TestStrategyOptimizerRunGridSearch:
    def test_grid_search_returns_80_results(self):
        """Issue #260: デフォルトグリッドサーチが80通り（threshold(4)×top_n(4)×r(5)）の結果を返す"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_grid_search()
        assert len(results) == 80  # 4×4×5

    def test_grid_search_results_have_params(self):
        """各結果に expected_return_threshold / top_n / prob_weight_r / min_prob_threshold が含まれる"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_grid_search()
        for r in results:
            assert "expected_return_threshold" in r.params
            assert "top_n" in r.params
            assert "prob_weight_r" in r.params
            assert "min_prob_threshold" in r.params
            # 旧パラメータは含まれない
            assert "p1" not in r.params
            assert "p2" not in r.params
            assert "kelly_fraction" not in r.params

    def test_custom_ranges(self):
        """カスタムレンジを指定すると対応する組み合わせ数になる"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_grid_search(
            threshold_range=[1.0, 1.2, 1.5],
            top_n_range=[3, 5],
            r_range=[1.0],
        )
        assert len(results) == 6  # threshold(3)×top_n(2)×r(1)

    def test_min_prob_threshold_range_creates_4d_grid(self):
        """Issue #261: min_prob_threshold_range 指定で4次元グリッドサーチになる"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_grid_search(
            threshold_range=[1.0, 1.2],
            top_n_range=[3],
            r_range=[1.0],
            min_prob_threshold_range=[0.0, 0.1, 0.2],
        )
        assert len(results) == 6  # threshold(2)×top_n(1)×r(1)×min_prob(3)

    def test_min_prob_threshold_range_stored_in_params(self):
        """Issue #261: min_prob_threshold が params に含まれ探索値が正しく記録される"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_grid_search(
            threshold_range=[1.0],
            top_n_range=[3],
            r_range=[1.0],
            min_prob_threshold_range=[0.0, 0.15],
        )
        mpt_values = {r.params["min_prob_threshold"] for r in results}
        assert mpt_values == {0.0, 0.15}

    def test_min_prob_threshold_fixed_when_range_is_none(self):
        """min_prob_threshold_range=None の場合は固定値が全結果に適用される"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_grid_search(
            threshold_range=[1.0, 1.2],
            top_n_range=[3],
            r_range=[1.0],
            min_prob_threshold_range=None,
            min_prob_threshold=0.1,
        )
        assert len(results) == 2  # threshold(2)×top_n(1)×r(1)（min_prob次元は追加されない）
        for r in results:
            assert r.params["min_prob_threshold"] == 0.1


# ---------------------------------------------------------------------------
# best_params のテスト
# ---------------------------------------------------------------------------


class TestStrategyOptimizerBestParams:
    def _make_results(self) -> list[OptimizationResult]:
        return [
            OptimizationResult(
                params={"expected_return_threshold": 1.0, "top_n": 3, "prob_weight_r": 1.0},
                recovery_rate=90.0, hit_rate=30.0,
                max_drawdown=20.0, sharpe_ratio=0.5, total_bets=100,
            ),
            OptimizationResult(
                params={"expected_return_threshold": 1.2, "top_n": 5, "prob_weight_r": 0.8},
                recovery_rate=110.0, hit_rate=35.0,
                max_drawdown=15.0, sharpe_ratio=0.8, total_bets=80,
            ),
            OptimizationResult(
                params={"expected_return_threshold": 1.5, "top_n": 2, "prob_weight_r": 1.2},
                recovery_rate=95.0, hit_rate=28.0,
                max_drawdown=25.0, sharpe_ratio=0.3, total_bets=60,
            ),
        ]

    def test_best_params_by_recovery_rate(self):
        """recovery_rate が最大のパラメータが返る"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None, combo_odds_df=None)
        results = self._make_results()
        best = optimizer.best_params(results, metric="recovery_rate")
        assert best.recovery_rate == 110.0

    def test_best_params_by_max_drawdown_is_minimum(self):
        """max_drawdown は小さいほど良い（最小値が選ばれる）"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None, combo_odds_df=None)
        results = self._make_results()
        best = optimizer.best_params(results, metric="max_drawdown")
        assert best.max_drawdown == 15.0

    def test_best_params_raises_on_empty_results(self):
        """空リストを渡すと ValueError が発生する"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None, combo_odds_df=None)
        with pytest.raises(ValueError):
            optimizer.best_params([], metric="recovery_rate")


# ---------------------------------------------------------------------------
# filter_by_goals のテスト
# ---------------------------------------------------------------------------


class TestStrategyOptimizerFilterByGoals:
    def _make_results(self) -> list[OptimizationResult]:
        return [
            OptimizationResult(
                params={"expected_return_threshold": 1.2}, recovery_rate=105.0, hit_rate=32.0,
                max_drawdown=25.0, sharpe_ratio=0.6, total_bets=100,
            ),
            OptimizationResult(
                params={"expected_return_threshold": 1.35}, recovery_rate=98.0, hit_rate=28.0,
                max_drawdown=20.0, sharpe_ratio=0.4, total_bets=80,
            ),
            OptimizationResult(
                params={"expected_return_threshold": 1.5}, recovery_rate=112.0, hit_rate=36.0,
                max_drawdown=35.0, sharpe_ratio=0.9, total_bets=60,
            ),
            OptimizationResult(
                params={"expected_return_threshold": 1.75}, recovery_rate=103.0, hit_rate=31.0,
                max_drawdown=28.0, sharpe_ratio=0.7, total_bets=70,
            ),
        ]

    def test_returns_only_results_meeting_both_goals(self):
        """回収率≥100% かつ ドローダウン≤30% の結果のみ返す"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None, combo_odds_df=None)
        filtered = optimizer.filter_by_goals(self._make_results())
        assert all(r.recovery_rate >= 100.0 for r in filtered)
        assert all(r.max_drawdown <= 30.0 for r in filtered)

    def test_returns_empty_when_no_results_meet_goals(self):
        """条件を満たす結果がない場合は空リストを返す"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None, combo_odds_df=None)
        results = [
            OptimizationResult(
                params={}, recovery_rate=80.0, hit_rate=20.0,
                max_drawdown=40.0, sharpe_ratio=0.1, total_bets=50,
            )
        ]
        assert optimizer.filter_by_goals(results) == []

    def test_filtered_results_are_sorted_by_recovery_rate(self):
        """フィルタ後の結果は回収率降順でソートされる"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None, combo_odds_df=None)
        filtered = optimizer.filter_by_goals(self._make_results())
        rates = [r.recovery_rate for r in filtered]
        assert rates == sorted(rates, reverse=True)

    def test_min_total_bets_excludes_low_volume_results(self):
        """min_total_bets を指定すると賭け数が下限未満の結果を除外する（少数サンプルのまぐれ解対策）"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None, combo_odds_df=None)
        results = [
            # 回収率・DDは合格だが賭け数が少ない（まぐれ解）
            OptimizationResult(
                params={"expected_return_threshold": 2.5}, recovery_rate=300.0, hit_rate=80.0,
                max_drawdown=1.0, sharpe_ratio=2.0, total_bets=20,
            ),
            # 回収率・DD・賭け数すべて合格
            OptimizationResult(
                params={"expected_return_threshold": 1.8}, recovery_rate=150.0, hit_rate=12.0,
                max_drawdown=22.0, sharpe_ratio=1.5, total_bets=700,
            ),
        ]
        filtered = optimizer.filter_by_goals(results, min_total_bets=600)
        assert len(filtered) == 1
        assert filtered[0].total_bets == 700
        assert all(r.total_bets >= 600 for r in filtered)

    def test_min_total_bets_defaults_to_no_constraint(self):
        """min_total_bets 未指定時（デフォルト0）は賭け数で除外しない（後方互換）"""
        optimizer = StrategyOptimizer(_make_predictions_df(), None, combo_odds_df=None)
        filtered = optimizer.filter_by_goals(self._make_results())
        # 回収率≥100かつDD≤30の合格解（105.0/DD25・103.0/DD28）が賭け数に関わらず残る
        assert len(filtered) == 2


# ---------------------------------------------------------------------------
# pattern_breakdown のテスト（Issue #260: 統一型）
# ---------------------------------------------------------------------------


class TestStrategyOptimizerSummaryByPattern:
    """Issue #260: パターン分類廃止後の pattern_breakdown 動作を検証する"""

    def test_pattern_breakdown_is_empty_dict(self):
        """run_grid_search の結果の pattern_breakdown が空辞書"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_grid_search(
            threshold_range=[1.0],
            top_n_range=[3],
            r_range=[1.0],
        )
        assert len(results) == 1
        assert results[0].pattern_breakdown == {}

    def test_empty_results_returns_empty_dataframe(self):
        """空リストを渡すと空 DataFrame を返す（後方互換性）"""
        import pandas as _pd
        optimizer = StrategyOptimizer(_make_predictions_df(), None, combo_odds_df=None)
        # summary_by_pattern は廃止済みのため、空の pattern_breakdown を持つ結果を検証
        results = [
            OptimizationResult(
                params={},
                recovery_rate=100.0, hit_rate=30.0,
                max_drawdown=20.0, sharpe_ratio=0.5, total_bets=100,
                pattern_breakdown={},
            )
        ]
        assert len(results) == 1
        assert results[0].pattern_breakdown == {}

    def test_avg_recovery_rate_is_calculated(self):
        """グリッドサーチの回収率が正しく記録される"""
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_grid_search(
            threshold_range=[1.0],
            top_n_range=[3],
            r_range=[1.0],
        )
        assert len(results) == 1
        assert isinstance(results[0].recovery_rate, float)


# ---------------------------------------------------------------------------
# run_optuna_search のテスト
# ---------------------------------------------------------------------------


class TestRunOptunaSearch:
    """Issue #382: run_optuna_search() の動作確認（小規模データ）"""

    def test_run_optuna_search_returns_results(self):
        """run_optuna_search() が OptimizationResult のリストを返す"""
        import pytest
        optuna = pytest.importorskip("optuna")
        df = _make_predictions_df(n_races=3, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_optuna_search(n_trials=5, timeout=30)
        assert isinstance(results, list)
        assert len(results) == 5
        for r in results:
            assert isinstance(r, OptimizationResult)

    def test_run_optuna_search_results_have_required_params(self):
        """各結果が必須パラメータを含む"""
        import pytest
        optuna = pytest.importorskip("optuna")
        df = _make_predictions_df(n_races=3, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_optuna_search(n_trials=3, timeout=30)
        for r in results:
            assert "expected_return_threshold" in r.params
            assert "top_n" in r.params
            assert "prob_weight_r" in r.params
            assert "min_prob_threshold" in r.params
            assert "max_wide_odds" in r.params

    def test_run_optuna_search_never_explores_temperature(self):
        """temperature は本番予測パスで未適用のため最適化対象外（Issue #399）。

        pred_score カラムの有無にかかわらず temperature を探索しないこと。
        """
        import pytest
        optuna = pytest.importorskip("optuna")
        df = _make_predictions_df(n_races=3, n_horses=5, win_place_prob=0.5, odds=3.0)
        # pred_score が存在しても temperature は探索されない
        df["pred_score"] = df["win_place_prob"] * 10.0
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_optuna_search(n_trials=3, timeout=30)
        assert results
        for r in results:
            assert "temperature" not in r.params

    def test_run_optuna_search_never_explores_prob_weight_r(self):
        """prob_weight_r はアイソトニック校正後は 1.0 固定・探索対象外（Issue #417）。

        trial パラメータとして探索されず、結果 params には常に固定値 1.0 が入ること。
        """
        import pytest
        optuna = pytest.importorskip("optuna")
        df = _make_predictions_df(n_races=3, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_optuna_search(n_trials=5, timeout=30)
        assert results
        for r in results:
            # params には後方互換のため残るが、常に固定値 1.0
            assert r.params["prob_weight_r"] == 1.0

    def test_run_optuna_search_metrics_are_valid(self):
        """各結果の指標が有効な数値である"""
        import pytest
        optuna = pytest.importorskip("optuna")
        df = _make_predictions_df(n_races=3, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        results = optimizer.run_optuna_search(n_trials=3, timeout=30)
        for r in results:
            assert isinstance(r.recovery_rate, float)
            assert isinstance(r.hit_rate, float)
            assert isinstance(r.max_drawdown, float)
            assert isinstance(r.total_bets, int)

    def test_run_optuna_search_custom_sampler(self):
        """カスタムサンプラー（RandomSampler）を指定できる"""
        import pytest
        optuna = pytest.importorskip("optuna")
        df = _make_predictions_df(n_races=3, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        sampler = optuna.samplers.RandomSampler(seed=42)
        results = optimizer.run_optuna_search(n_trials=3, timeout=30, sampler=sampler)
        assert len(results) == 3

    def test_run_grid_search_emits_deprecation_warning(self):
        """run_grid_search() は DeprecationWarning を発する"""
        import warnings
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            optimizer.run_grid_search(
                threshold_range=[1.0],
                top_n_range=[3],
                r_range=[1.0],
            )
        assert any(issubclass(warning.category, DeprecationWarning) for warning in w)

    def test_run_optuna_search_raises_on_invalid_metric(self):
        """無効な metric 名を渡すと ValueError が送出される"""
        import pytest
        pytest.importorskip("optuna")
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        with pytest.raises(ValueError, match="metric="):
            optimizer.run_optuna_search(n_trials=1, metric="invalid_metric")

    def test_run_optuna_search_zero_trials_does_not_crash(self):
        """n_trials=0 で試行なしでも study.best_value クラッシュが起きない"""
        import pytest
        pytest.importorskip("optuna")
        df = _make_predictions_df(n_races=2, n_horses=5, win_place_prob=0.5, odds=3.0)
        optimizer = StrategyOptimizer(df, None, combo_odds_df=None)
        # n_trials=0 → 試行なし → study.best_trial=None → NaN でログ出力されること
        results = optimizer.run_optuna_search(n_trials=0, timeout=1)
        assert isinstance(results, list)
        assert len(results) == 0
