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
    """テスト用の複勝オッズ DataFrame を生成する"""
    rows = []
    for i in range(1, n_horses + 1):
        rows.append({
            "race_id": "race_001",
            "horse_id": f"horse_{i:03d}",
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
        """BigQuery クエリが失敗した場合は空 DataFrame を返す"""
        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("BQ error")
        with patch("scripts.run_backtest.bigquery.Client", return_value=mock_client):
            result = rb.fetch_place_odds(project_id="test", race_ids=["r001"])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_returns_place_odds_column(self):
        """正常取得時は place_odds カラムを持つ DataFrame を返す"""
        mock_df = pd.DataFrame({
            "race_id": ["r001"],
            "horse_id": ["h001"],
            "horse_number": [1],
            "place_odds": [2.5],
        })
        mock_job = MagicMock()
        mock_job.to_dataframe.return_value = mock_df
        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        with patch("scripts.run_backtest.bigquery.Client", return_value=mock_client):
            result = rb.fetch_place_odds(project_id="test", race_ids=["r001"])
        assert "place_odds" in result.columns
        assert len(result) == 1


# ---------------------------------------------------------------------------
# run_backtest_pipeline のオッズマージ処理テスト
# ---------------------------------------------------------------------------

class TestOddsMergeInPipeline:
    """
    run_backtest_pipeline 内でオッズが predictions_df に正しく
    マージされることを検証する。
    BigQuery の全呼び出しをモックする。
    """

    def _run_with_mocks(
        self,
        features_df,
        results_df,
        payouts_df,
        odds_df,
        captured_predictions,
    ):
        """
        パイプラインをモックで実行し、シミュレーターに渡された
        predictions_df を captured_predictions リストに格納する。
        """
        config = {
            "data": {
                "dataset": "features",
                "table": "training_data",
                "exclude_columns": [],
                "categorical_columns": [],
            }
        }

        with (
            patch("scripts.run_backtest.fetch_historical_features", return_value=features_df),
            patch("scripts.run_backtest.fetch_historical_results", return_value=results_df),
            patch("scripts.run_backtest.fetch_place_payouts", return_value=payouts_df),
            patch("scripts.run_backtest.fetch_place_odds", return_value=odds_df),
            patch("scripts.run_backtest.generate_predictions", return_value=features_df.copy()),
            patch("scripts.run_backtest.BacktestSimulator") as MockSim,
            patch("scripts.run_backtest.compute_metrics", return_value={}),
        ):
            mock_instance = MockSim.return_value
            mock_instance.run.side_effect = lambda predictions_df, **kw: (
                captured_predictions.append(predictions_df) or pd.DataFrame()
            )

            rb.run_backtest_pipeline(
                project_id="test",
                model_path="dummy.txt",
                start_date=datetime.date(2025, 1, 1),
                end_date=datetime.date(2025, 12, 31),
                config=config,
            )

    def test_place_odds_from_raw_odds_merged(self):
        """raw.odds にデータがある場合、place_odds カラムが predictions_df に付与される"""
        preds = _make_predictions(n_horses=3)
        odds_df = _make_odds_df(n_horses=3, odds_val=3.0)
        payouts_df = _make_payouts_df(n_horses=3)
        captured = []

        self._run_with_mocks(
            features_df=preds,
            results_df=pd.DataFrame(),
            payouts_df=payouts_df,
            odds_df=odds_df,
            captured_predictions=captured,
        )

        assert len(captured) == 1, "シミュレーターが呼ばれていない"
        merged = captured[0]
        assert "place_odds" in merged.columns, "place_odds カラムが付与されていない"
        assert (merged["place_odds"] == 3.0).all()

    def test_fallback_to_payouts_when_odds_empty(self):
        """raw.odds が空の場合、raw.payouts から place_odds が計算される"""
        preds = _make_predictions(n_horses=3)
        payouts_df = _make_payouts_df(n_horses=3, payout=280)
        captured = []

        self._run_with_mocks(
            features_df=preds,
            results_df=pd.DataFrame(),
            payouts_df=payouts_df,
            odds_df=pd.DataFrame(),   # empty → fallback
            captured_predictions=captured,
        )

        assert len(captured) == 1
        merged = captured[0]
        assert "place_odds" in merged.columns
        # payout_amount=280 → place_odds=2.8
        non_null = merged["place_odds"].dropna()
        assert (non_null == 2.8).all()

    def test_pipeline_aborts_when_both_empty(self):
        """raw.odds・raw.payouts 両方空の場合はパイプラインが空結果を返す"""
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
            patch("scripts.run_backtest.generate_predictions", return_value=preds.copy()),
        ):
            history_df, metrics = rb.run_backtest_pipeline(
                project_id="test",
                model_path="dummy.txt",
                start_date=datetime.date(2025, 1, 1),
                end_date=datetime.date(2025, 12, 31),
                config=config,
            )

        assert len(history_df) == 0
        assert metrics == {}
