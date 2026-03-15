"""
ダッシュボード data.py のユニットテスト

BigQuery クライアントをモックして、データ取得ロジックのみを検証する。
"""

from __future__ import annotations

import os
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# GCP_PROJECT_ID を設定してからインポート
os.environ.setdefault("GCP_PROJECT_ID", "test-project")


class TestFetchTodayInvestmentDecisions:
    """fetch_today_investment_decisions のテスト"""

    def test_returns_dataframe_with_expected_columns(self):
        mock_df = pd.DataFrame({
            "race_date": ["2026-03-15"],
            "venue_code": ["09"],
            "race_number": [12],
            "horse_number": [5],
            "horse_name": ["テスト馬"],
            "bet_type": ["place"],
            "bet_amount": [500.0],
            "place_odds": [3.5],
            "expected_return": [1.4],
            "win_place_prob": [0.4],
            "race_pattern": ["standard"],
        })

        with patch("src.dashboard.data._get_bq_client") as mock_client_fn:
            mock_client = MagicMock()
            mock_client.query.return_value.to_dataframe.return_value = mock_df
            mock_client_fn.return_value = mock_client

            from src.dashboard import data as dash_data
            # キャッシュをクリア
            dash_data.fetch_today_investment_decisions.clear()

            result = dash_data.fetch_today_investment_decisions("2026-03-15")

        assert not result.empty
        assert "venue_name" in result.columns
        assert "bet_type_label" in result.columns
        assert result.iloc[0]["venue_name"] == "阪神"
        assert result.iloc[0]["bet_type_label"] == "複勝"

    def test_returns_empty_df_when_no_project_id(self):
        with patch("src.dashboard.data.get_project_id", return_value=""):
            from src.dashboard import data as dash_data
            dash_data.fetch_today_investment_decisions.clear()
            result = dash_data.fetch_today_investment_decisions("2026-03-15")

        assert result.empty

    def test_returns_empty_df_on_bq_error(self):
        with patch("src.dashboard.data._get_bq_client") as mock_client_fn:
            mock_client_fn.side_effect = Exception("BQ connection error")

            from src.dashboard import data as dash_data
            dash_data.fetch_today_investment_decisions.clear()
            result = dash_data.fetch_today_investment_decisions("2026-03-15")

        assert result.empty


class TestFetchDailySummary:
    """fetch_daily_summary のテスト"""

    def test_returns_correct_summary(self):
        mock_df = pd.DataFrame({
            "race_date": ["2026-03-15", "2026-03-15"],
            "venue_code": ["09", "09"],
            "race_number": [1, 2],
            "horse_number": [1, 3],
            "horse_name": ["馬A", "馬B"],
            "bet_type": ["place", "place"],
            "bet_amount": [300.0, 500.0],
            "place_odds": [2.5, 3.0],
            "expected_return": [1.2, 1.5],
            "win_place_prob": [0.4, 0.35],
            "race_pattern": ["standard", "standard"],
        })

        with patch("src.dashboard.data._get_bq_client") as mock_client_fn:
            mock_client = MagicMock()
            mock_client.query.return_value.to_dataframe.return_value = mock_df
            mock_client_fn.return_value = mock_client

            from src.dashboard import data as dash_data
            dash_data.fetch_today_investment_decisions.clear()
            dash_data.fetch_daily_summary.clear()

            result = dash_data.fetch_daily_summary("2026-03-15")

        assert result["total_bets"] == 2
        assert result["total_amount"] == 800
        assert result["race_count"] == 2

    def test_returns_zeros_when_no_data(self):
        with patch("src.dashboard.data.fetch_today_investment_decisions") as mock_fn:
            mock_fn.return_value = pd.DataFrame()

            from src.dashboard import data as dash_data
            dash_data.fetch_daily_summary.clear()
            result = dash_data.fetch_daily_summary("2026-03-15")

        assert result["total_bets"] == 0
        assert result["total_amount"] == 0


class TestFetchRaceList:
    """fetch_race_list のテスト"""

    def test_returns_dataframe_with_venue_name(self):
        mock_df = pd.DataFrame({
            "race_date": ["2026-03-15"] * 3,
            "race_id": ["202609030512"] * 3,
            "venue_code": ["09"] * 3,
            "race_number": [5] * 3,
            "horse_number": [1, 2, 3],
            "horse_name": ["馬A", "馬B", "馬C"],
            "win_place_prob": [0.45, 0.30, 0.25],
            "rank_in_race": [1, 2, 3],
            "pos": [1, 2, 3],
        })

        with patch("src.dashboard.data._get_bq_client") as mock_client_fn:
            mock_client = MagicMock()
            mock_client.query.return_value.to_dataframe.return_value = mock_df
            mock_client_fn.return_value = mock_client

            from src.dashboard import data as dash_data
            dash_data.fetch_race_list.clear()
            result = dash_data.fetch_race_list("2026-03-15")

        assert "venue_name" in result.columns
        assert result.iloc[0]["venue_name"] == "阪神"

    def test_returns_empty_on_error(self):
        with patch("src.dashboard.data._get_bq_client") as mock_client_fn:
            mock_client_fn.side_effect = Exception("error")

            from src.dashboard import data as dash_data
            dash_data.fetch_race_list.clear()
            result = dash_data.fetch_race_list("2026-03-15")

        assert result.empty


class TestFetchRaceDetail:
    """fetch_race_detail のテスト"""

    def test_returns_all_horses(self):
        mock_df = pd.DataFrame({
            "horse_number": [1, 2, 3, 4, 5],
            "horse_name": [f"馬{i}" for i in range(1, 6)],
            "win_place_prob": [0.45, 0.30, 0.15, 0.06, 0.04],
            "pred_score": [0.9, 0.6, 0.3, 0.1, 0.05],
            "rank_in_race": [1, 2, 3, 4, 5],
            "win_odds": [2.5, 4.0, 8.0, 20.0, 30.0],
            "place_odds": [1.5, 2.0, 3.5, 8.0, 10.0],
            "expected_return": [0.675, 0.6, 0.525, 0.48, 0.4],
        })

        with patch("src.dashboard.data._get_bq_client") as mock_client_fn:
            mock_client = MagicMock()
            mock_client.query.return_value.to_dataframe.return_value = mock_df
            mock_client_fn.return_value = mock_client

            from src.dashboard import data as dash_data
            dash_data.fetch_race_detail.clear()
            result = dash_data.fetch_race_detail("2026-03-15", "09", 5)

        assert len(result) == 5
        assert result.iloc[0]["rank_in_race"] == 1


class TestFetchBacktestSummary:
    """fetch_backtest_summary のテスト"""

    def test_returns_metrics(self):
        mock_df = pd.DataFrame({
            "total_bets": [100],
            "total_invested": [50000.0],
            "total_payout": [53000.0],
            "recovery_rate": [106.0],
            "hit_count": [40],
            "hit_rate": [40.0],
        })

        with patch("src.dashboard.data._get_bq_client") as mock_client_fn:
            mock_client = MagicMock()
            mock_client.query.return_value.to_dataframe.return_value = mock_df
            mock_client_fn.return_value = mock_client

            from src.dashboard import data as dash_data
            dash_data.fetch_backtest_summary.clear()
            result = dash_data.fetch_backtest_summary("2026-01-01", "2026-03-15")

        assert result["recovery_rate"] == pytest.approx(106.0)
        assert result["total_bets"] == 100
        assert result["hit_rate"] == pytest.approx(40.0)

    def test_returns_empty_dict_on_error(self):
        with patch("src.dashboard.data._get_bq_client") as mock_client_fn:
            mock_client_fn.side_effect = Exception("BQ error")

            from src.dashboard import data as dash_data
            dash_data.fetch_backtest_summary.clear()
            result = dash_data.fetch_backtest_summary("2026-01-01", "2026-03-15")

        assert result == {}


class TestVenueMapping:
    """venue_code <-> venue_name マッピングのテスト"""

    def test_venue_code_to_name_covers_all_jra(self):
        from src.dashboard.data import VENUE_CODE_TO_NAME
        expected_venues = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
        assert expected_venues == set(VENUE_CODE_TO_NAME.keys())

    def test_bet_type_labels(self):
        from src.dashboard.data import BET_TYPE_LABELS
        assert BET_TYPE_LABELS["place"] == "複勝"
        assert BET_TYPE_LABELS["win"] == "単勝"
        assert BET_TYPE_LABELS["wide"] == "ワイド"
        assert BET_TYPE_LABELS["umaren"] == "馬連"
        assert BET_TYPE_LABELS["sanrenpuku"] == "三連複"
