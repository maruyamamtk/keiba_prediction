"""
_refresh_investment_decisions_for_race のユニットテスト

テスト対象:
  - _refresh_investment_decisions_for_race: 最新オッズで investment_decisions を上書き
    - 正常系: 予測・オッズあり → 計算・保存 → True を返す
    - 異常系: 予測データなし → False（フォールバック）
    - 異常系: オッズデータなし → False（フォールバック）
    - 異常系: BQ例外 → False（フォールバック）

Issue #231: 発走5分前に最新オッズで investment_decisions を上書きしてから IPAT 購入する
"""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.automation.api.app import _refresh_investment_decisions_for_race

TARGET_DATE = datetime.date(2026, 4, 12)
RACE_ID = "202609050112"
PROJECT_ID = "test_project"


def _make_pred_df() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "race_id": RACE_ID, "race_date": TARGET_DATE,
            "horse_id": f"h{i:04d}", "horse_number": i,
            "horse_name": f"ウマ{i}", "venue_code": "09",
            "race_number": 12, "win_place_prob": 0.3 if i == 1 else 0.12,
            "pred_score": 1.0 - i * 0.1, "rank_in_race": i,
        }
        for i in range(1, 9)
    ])


def _make_odds_df() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "race_id": RACE_ID, "horse_number": i,
            "win_odds": 3.0 * i, "place_odds_min": max(1.1, 1.5 * i),
            "place_odds_max": 2.5 * i, "scraped_at": "2026-04-12 08:30:00",
        }
        for i in range(1, 9)
    ])


def _make_merged_df() -> pd.DataFrame:
    """build_race_df が返す想定の DataFrame"""
    df = _make_pred_df().copy()
    df["odds"] = df["horse_number"].apply(lambda i: max(1.1, 1.5 * i))
    df["win_odds"] = df["horse_number"].apply(lambda i: 3.0 * i)
    return df


class TestRefreshInvestmentDecisions:
    """_refresh_investment_decisions_for_race のテスト"""

    @patch("scripts.run_strategy.save_decisions_to_bq", return_value=1)
    @patch("scripts.run_strategy.fetch_combo_odds_for_date", return_value=pd.DataFrame())
    @patch("scripts.run_strategy.load_strategy_config", return_value={
        "expected_return_threshold": 1.2, "budget_per_race": 3000,
        "top_n": 5, "min_bet_amount": 100, "min_prob_threshold": 0.10,
        "prob_weight_r": 1.0,
    })
    @patch("scripts.run_strategy.build_race_df")
    @patch("src.backtest.strategy.select_bets_for_race")
    @patch("google.cloud.bigquery.Client")
    def test_success_returns_true(
        self,
        mock_bq_cls,
        mock_select,
        mock_build,
        mock_config,
        mock_combo,
        mock_save,
    ):
        """予測・オッズが取得でき計算・保存に成功した場合は True を返すこと"""
        pred_df = _make_pred_df()
        odds_df = _make_odds_df()
        merged_df = _make_merged_df()

        mock_client = MagicMock()
        mock_bq_cls.return_value = mock_client
        mock_client.query.return_value.to_dataframe.side_effect = [pred_df, odds_df]
        mock_build.return_value = merged_df

        mock_select.return_value = [
            {"horse_numbers": [1], "bet_type": "place", "odds": 2.0, "bet_amount": 300.0}
        ]

        result = _refresh_investment_decisions_for_race(PROJECT_ID, RACE_ID, TARGET_DATE)

        assert result is True
        mock_save.assert_called_once()

    @patch("google.cloud.bigquery.Client")
    def test_empty_predictions_returns_false(self, mock_bq_cls):
        """予測データが空の場合は False（フォールバック）を返すこと"""
        mock_client = MagicMock()
        mock_bq_cls.return_value = mock_client
        mock_client.query.return_value.to_dataframe.return_value = pd.DataFrame()

        result = _refresh_investment_decisions_for_race(PROJECT_ID, RACE_ID, TARGET_DATE)

        assert result is False

    @patch("google.cloud.bigquery.Client")
    def test_empty_odds_returns_false(self, mock_bq_cls):
        """オッズデータが空の場合は False（フォールバック）を返すこと"""
        pred_df = _make_pred_df()
        mock_client = MagicMock()
        mock_bq_cls.return_value = mock_client
        mock_client.query.return_value.to_dataframe.side_effect = [pred_df, pd.DataFrame()]

        result = _refresh_investment_decisions_for_race(PROJECT_ID, RACE_ID, TARGET_DATE)

        assert result is False

    @patch("google.cloud.bigquery.Client")
    def test_bq_exception_returns_false(self, mock_bq_cls):
        """BigQuery例外時は False（フォールバック）を返し、例外を送出しないこと"""
        mock_bq_cls.side_effect = Exception("BQ connection error")

        result = _refresh_investment_decisions_for_race(PROJECT_ID, RACE_ID, TARGET_DATE)

        assert result is False
