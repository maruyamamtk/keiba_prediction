"""
run_backtest.py のユニットテスト

BigQuery アクセスをモックして、オッズ取得・マージ処理を検証する。
"""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# BigQuery をモックするためモジュールをインポート
import scripts.run_backtest as rb


# ---------------------------------------------------------------------------
# テスト用ヘルパー
# ---------------------------------------------------------------------------

def _make_predictions(n_horses: int = 4) -> pd.DataFrame:
    """テスト用の予測 DataFrame を生成する（オッズカラムなし）"""
    rows = []
    for i in range(1, n_horses + 1):
        rows.append({
            "race_id": "race_001",
            "race_date": datetime.date(2025, 1, 5),
            "horse_id": f"horse_{i:03d}",
            "horse_number": i,
            "win_place_prob": 0.4,
            "pred_score": float(i),
            "finish_position": i,
        })
    return pd.DataFrame(rows)


def _make_odds_df(n_horses: int = 4, odds_val: float = 2.5) -> pd.DataFrame:
    """テスト用の複勝オッズ DataFrame を生成する。
    OZ ファイルには horse_id が含まれないため horse_number をキーとする。"""
    rows = []
    for i in range(1, n_horses + 1):
        rows.append({
            "race_id": "race_001",
            "horse_number": i,
            "place_odds": odds_val,
        })
    return pd.DataFrame(rows)


def _make_payouts_df(n_horses: int = 3, payout: int = 250) -> pd.DataFrame:
    """テスト用の払戻 DataFrame を生成する（raw.payoutsフォーマット）"""
    rows = []
    for i in range(1, n_horses + 1):
        rows.append({
            "race_id": "race_001",
            "horse_number_1": i,
            "payout_amount": payout,
            "bet_type": "place",
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# fetch_place_odds のテスト
# ---------------------------------------------------------------------------

class TestFetchPlaceOdds:
    def test_empty_race_ids_returns_empty(self):
        """race_ids が空の場合は BigQuery を呼ばず空 DataFrame を返す"""
        result = rb.fetch_place_odds(project_id="test", race_ids=[])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_bq_error_returns_empty(self):
        """両ステージの BigQuery クエリが失敗した場合は空 DataFrame を返す"""
        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("BQ error")
        with patch("scripts.run_backtest.bigquery.Client", return_value=mock_client):
            result = rb.fetch_place_odds(project_id="test", race_ids=["r001"])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_stage1_predictions_daily_odds_is_prioritized(self):
        """predictions.daily_odds にデータがある場合はそれを優先して返す"""
        predictions_df = pd.DataFrame({
            "race_id": ["r001"],
            "horse_number": [1],
            "place_odds": [3.2],
        })
        mock_job = MagicMock()
        mock_job.to_dataframe.return_value = predictions_df
        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        with patch("scripts.run_backtest.bigquery.Client", return_value=mock_client):
            result = rb.fetch_place_odds(project_id="test", race_ids=["r001"])
        assert "place_odds" in result.columns
        assert len(result) == 1
        assert result.iloc[0]["place_odds"] == 3.2
        # Stage 1 でカバーできたので Stage 2 (raw.odds) は呼ばれない（query 呼び出しは1回）
        assert mock_client.query.call_count == 1

    def test_stage2_raw_odds_used_when_stage1_empty(self):
        """predictions.daily_odds が空の場合は raw.odds にフォールバックする"""
        raw_odds_df = pd.DataFrame({
            "race_id": ["r001"],
            "horse_number": [1],
            "place_odds": [2.5],
        })
        mock_job_empty = MagicMock()
        mock_job_empty.to_dataframe.return_value = pd.DataFrame()
        mock_job_raw = MagicMock()
        mock_job_raw.to_dataframe.return_value = raw_odds_df
        mock_client = MagicMock()
        mock_client.query.side_effect = [mock_job_empty, mock_job_raw]
        with patch("scripts.run_backtest.bigquery.Client", return_value=mock_client):
            result = rb.fetch_place_odds(project_id="test", race_ids=["r001"])
        assert "place_odds" in result.columns
        assert len(result) == 1
        assert result.iloc[0]["place_odds"] == 2.5

    def test_stage1_and_stage2_combined_for_different_races(self):
        """レースごとに predictions.daily_odds と raw.odds を補完合成する"""
        stage1_df = pd.DataFrame({
            "race_id": ["r001"],
            "horse_number": [1],
            "place_odds": [3.2],
        })
        stage2_df = pd.DataFrame({
            "race_id": ["r002"],
            "horse_number": [2],
            "place_odds": [4.0],
        })
        mock_job1 = MagicMock()
        mock_job1.to_dataframe.return_value = stage1_df
        mock_job2 = MagicMock()
        mock_job2.to_dataframe.return_value = stage2_df
        mock_client = MagicMock()
        mock_client.query.side_effect = [mock_job1, mock_job2]
        with patch("scripts.run_backtest.bigquery.Client", return_value=mock_client):
            result = rb.fetch_place_odds(project_id="test", race_ids=["r001", "r002"])
        assert len(result) == 2
        assert set(result["race_id"]) == {"r001", "r002"}


# ---------------------------------------------------------------------------
# run_full_strategy_backtest_pipeline のテスト（オッズなし中断の確認）
# ---------------------------------------------------------------------------

class TestRunFullStrategyAbortWhenNoOdds:
    """オッズデータがない場合にパイプラインが中断することを確認する"""

    def test_aborts_when_odds_empty(self):
        """place_odds が空の場合はパイプラインが空結果を返す"""
        preds = _make_predictions(n_horses=3)
        config = {
            "data": {
                "dataset": "features",
                "table": "training_data",
                "exclude_columns": [],
                "categorical_columns": [],
            }
        }

        with (
            patch("scripts.run_backtest.fetch_historical_features", return_value=preds),
            patch("scripts.run_backtest.fetch_historical_results", return_value=pd.DataFrame()),
            patch("scripts.run_backtest.fetch_place_payouts", return_value=pd.DataFrame()),
            patch("scripts.run_backtest.fetch_place_odds", return_value=pd.DataFrame()),
            patch("scripts.run_backtest.fetch_combo_odds", return_value=pd.DataFrame()),
            patch("scripts.run_backtest._fetch_all_payouts", return_value=pd.DataFrame()),
            patch("scripts.run_backtest.generate_predictions", return_value=preds.copy()),
        ):
            history_df, metrics = rb.run_full_strategy_backtest_pipeline(
                project_id="test",
                model_path="dummy.txt",
                start_date=datetime.date(2025, 1, 1),
                end_date=datetime.date(2025, 12, 31),
                config=config,
                p1=0.1,
                expected_return_threshold=1.2,
                budget_per_race=3000,
                min_prob_threshold=0.0,
            )

        assert len(history_df) == 0
        assert metrics == {}


# ---------------------------------------------------------------------------
# run_full_strategy_backtest_pipeline のテスト
# ---------------------------------------------------------------------------

class TestRunFullStrategyBacktestPipeline:
    """run_full_strategy_backtest_pipeline の動作を検証する"""

    def _make_preds_with_odds(self, n_horses: int = 4) -> pd.DataFrame:
        rows = []
        for i in range(1, n_horses + 1):
            rows.append({
                "race_id": "race_001",
                "race_date": datetime.date(2025, 1, 5),
                "horse_id": f"horse_{i:03d}",
                "horse_number": i,
                "win_place_prob": 0.4,
                "pred_score": float(i),
                "finish_position": i,
                "place_odds": 2.5,
            })
        return pd.DataFrame(rows)

    def test_full_strategy_mode_calls_strategy_optimizer(self):
        """フル戦略モードで StrategyOptimizer._run_simulation が呼ばれることを検証"""
        preds = self._make_preds_with_odds(n_horses=4)
        odds_df = _make_odds_df(n_horses=4, odds_val=2.5)
        payouts_df = _make_payouts_df(n_horses=3)
        config = {
            "data": {
                "dataset": "features",
                "table": "training_data",
                "exclude_columns": [],
                "categorical_columns": [],
            }
        }
        strategy_params = {
            "p1": 0.1,
            "expected_return_threshold": 1.2,
            "prob_weight_r_dominant": 1.0,
            "prob_weight_r_standard": 1.0,
            "min_prob_threshold": 0.10,
            "top_n": 5,
            "budget_per_race": 3000,
        }

        captured_sim_calls = []

        with (
            patch("scripts.run_backtest.fetch_historical_features", return_value=preds),
            patch("scripts.run_backtest.fetch_historical_results", return_value=pd.DataFrame()),
            patch("scripts.run_backtest.fetch_place_payouts", return_value=payouts_df),
            patch("scripts.run_backtest.fetch_place_odds", return_value=odds_df),
            patch("scripts.run_backtest.fetch_combo_odds", return_value=pd.DataFrame()),
            patch("scripts.run_backtest._fetch_all_payouts", return_value=payouts_df),
            patch("scripts.run_backtest.generate_predictions", return_value=preds.copy()),
            patch("scripts.run_backtest.StrategyOptimizer") as MockOptimizer,
        ):
            mock_instance = MockOptimizer.return_value
            mock_instance._run_simulation.side_effect = lambda **kw: (
                captured_sim_calls.append(kw) or (pd.DataFrame(), {})
            )

            rb.run_full_strategy_backtest_pipeline(
                project_id="test",
                model_path="dummy.txt",
                start_date=datetime.date(2025, 1, 1),
                end_date=datetime.date(2025, 12, 31),
                config=config,
                p1=strategy_params["p1"],
                expected_return_threshold=strategy_params["expected_return_threshold"],
                budget_per_race=strategy_params["budget_per_race"],
                min_prob_threshold=strategy_params["min_prob_threshold"],
                prob_weight_r_dominant=strategy_params["prob_weight_r_dominant"],
                prob_weight_r_standard=strategy_params["prob_weight_r_standard"],
                top_n=strategy_params["top_n"],
            )

        assert len(captured_sim_calls) == 1, "StrategyOptimizer._run_simulation が呼ばれていない"
        call_kwargs = captured_sim_calls[0]
        assert call_kwargs["p1"] == 0.1
        assert call_kwargs["expected_return_threshold"] == 1.2

    def test_full_strategy_aborts_when_no_odds(self):
        """place_odds が空の場合はフル戦略モードも空結果を返す"""
        preds = _make_predictions(n_horses=3)
        config = {
            "data": {
                "dataset": "features",
                "table": "training_data",
                "exclude_columns": [],
                "categorical_columns": [],
            }
        }
        with (
            patch("scripts.run_backtest.fetch_historical_features", return_value=preds),
            patch("scripts.run_backtest.fetch_historical_results", return_value=pd.DataFrame()),
            patch("scripts.run_backtest.fetch_place_payouts", return_value=pd.DataFrame()),
            patch("scripts.run_backtest.fetch_place_odds", return_value=pd.DataFrame()),
            patch("scripts.run_backtest.fetch_combo_odds", return_value=pd.DataFrame()),
            patch("scripts.run_backtest._fetch_all_payouts", return_value=pd.DataFrame()),
            patch("scripts.run_backtest.generate_predictions", return_value=preds.copy()),
        ):
            history_df, metrics = rb.run_full_strategy_backtest_pipeline(
                project_id="test",
                model_path="dummy.txt",
                start_date=datetime.date(2025, 1, 1),
                end_date=datetime.date(2025, 12, 31),
                config=config,
                p1=0.1,
                expected_return_threshold=1.2,
                budget_per_race=3000,
                min_prob_threshold=0.10,
                top_n=5,
            )

        assert len(history_df) == 0
        assert metrics == {}


# ---------------------------------------------------------------------------
# _load_strategy_config のテスト
# ---------------------------------------------------------------------------

class TestLoadStrategyConfig:
    """_load_strategy_config の動作を検証する"""

    def test_returns_empty_when_file_not_found(self, tmp_path):
        """存在しないファイルパスを渡した場合は空 dict を返す"""
        result = rb._load_strategy_config(tmp_path / "nonexistent.yaml")
        assert result == {}

    def test_loads_yaml_values(self, tmp_path):
        """有効な YAML ファイルから値を読み込めることを検証"""
        import yaml as _yaml
        config_data = {
            "p1": 0.15,
            "expected_return_threshold": 1.3,
            "budget_per_race": 5000,
        }
        config_file = tmp_path / "strategy_config.yaml"
        config_file.write_text(_yaml.dump(config_data))

        result = rb._load_strategy_config(config_file)
        assert result["p1"] == 0.15
        assert result["expected_return_threshold"] == 1.3
        assert result["budget_per_race"] == 5000


# ---------------------------------------------------------------------------
# fetch_place_odds の win_odds 取得テスト（Issue #176）
# ---------------------------------------------------------------------------

class TestFetchPlaceOddsWinOdds:
    """fetch_place_odds が win_odds を正しく返すことを検証する（Issue #176）"""

    def test_win_odds_included_from_stage1(self):
        """predictions.daily_odds から win_odds が返される"""
        stage1_df = pd.DataFrame({
            "race_id": ["r001"],
            "horse_number": [1],
            "place_odds": [3.2],
            "win_odds": [5.0],
        })
        mock_job = MagicMock()
        mock_job.to_dataframe.return_value = stage1_df
        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        with patch("scripts.run_backtest.bigquery.Client", return_value=mock_client):
            result = rb.fetch_place_odds(project_id="test", race_ids=["r001"])
        assert "win_odds" in result.columns
        assert result.iloc[0]["win_odds"] == 5.0
        assert result.iloc[0]["place_odds"] == 3.2

    def test_win_odds_included_from_stage2(self):
        """raw.odds から win_odds が返される"""
        stage2_df = pd.DataFrame({
            "race_id": ["r001"],
            "horse_number": [1],
            "place_odds": [2.5],
            "win_odds": [4.0],
        })
        mock_job_empty = MagicMock()
        mock_job_empty.to_dataframe.return_value = pd.DataFrame()
        mock_job_raw = MagicMock()
        mock_job_raw.to_dataframe.return_value = stage2_df
        mock_client = MagicMock()
        mock_client.query.side_effect = [mock_job_empty, mock_job_raw]
        with patch("scripts.run_backtest.bigquery.Client", return_value=mock_client):
            result = rb.fetch_place_odds(project_id="test", race_ids=["r001"])
        assert "win_odds" in result.columns
        assert result.iloc[0]["win_odds"] == 4.0

    def test_win_odds_nan_when_not_in_source(self):
        """win_odds カラムを返さないソースの場合でも place_odds は取得できる"""
        stage1_df = pd.DataFrame({
            "race_id": ["r001"],
            "horse_number": [1],
            "place_odds": [3.2],
            # win_odds なし（旧形式・後方互換）
        })
        mock_job = MagicMock()
        mock_job.to_dataframe.return_value = stage1_df
        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        with patch("scripts.run_backtest.bigquery.Client", return_value=mock_client):
            result = rb.fetch_place_odds(project_id="test", race_ids=["r001"])
        assert "place_odds" in result.columns
        assert result.iloc[0]["place_odds"] == 3.2
        # win_odds がないソースの場合は列自体が存在しないか NaN
        if "win_odds" in result.columns:
            assert pd.isna(result.iloc[0]["win_odds"])


# ---------------------------------------------------------------------------
# win_odds が predictions_df にマージされ単勝購入が発生するテスト（Issue #176）
# ---------------------------------------------------------------------------

class TestWinOddsMergeEnablesWinBets:
    """
    run_full_strategy_backtest_pipeline() で win_odds がマージされた結果、
    one_dominant パターン時に単勝が購入されることを検証する（Issue #176）
    """

    def test_win_odds_merged_into_predictions_df(self):
        """
        fetch_place_odds が win_odds を返した場合に predictions_df に win_odds カラムが付与される
        """
        from src.backtest.strategy import select_bets_for_race

        # one_dominant パターン：top1馬の複勝率が突出している
        race_df = pd.DataFrame({
            "race_id": ["r001"] * 3,
            "race_date": [datetime.date(2025, 1, 5)] * 3,
            "horse_id": ["h1", "h2", "h3"],
            "horse_number": [1, 2, 3],
            "win_place_prob": [0.6, 0.25, 0.15],
            "odds": [2.5, 4.0, 6.0],     # place_odds（複勝）
            "win_odds": [4.0, 8.0, 12.0],  # win_odds（単勝）
        })

        bets, pattern = select_bets_for_race(
            race_df=race_df,
            combo_odds_df=pd.DataFrame(),
            budget_per_race=3000,
            p1=0.1,              # top1 - top2 = 0.35 > 0.1 → one_dominant
            expected_return_threshold=1.0,  # 低めに設定して単勝が通るように
        )

        bet_types = [b["bet_type"] for b in bets]
        assert pattern.pattern == "one_dominant", f"パターン判定が one_dominant でない: {pattern}"
        assert "win" in bet_types, (
            f"one_dominant パターンで win_odds が存在するのに単勝が購入されない。"
            f"実際の馬券種: {bet_types}"
        )
