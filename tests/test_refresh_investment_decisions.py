"""
_refresh_investment_decisions_for_race / _predict_race_on_the_fly のユニットテスト

テスト対象:
  - _refresh_investment_decisions_for_race: 最新オッズで investment_decisions を上書き
    - 正常系: 予測・オッズあり → 計算・保存 → True を返す
    - 異常系: daily_predictions 空 かつ features.training_data 空 → False（フォールバック）
    - 異常系: daily_predictions 空 かつ features.training_data あり → オンザフライ予測を試みる
    - 異常系: オッズデータなし → False（フォールバック）
    - 異常系: BQ例外 → False（フォールバック）

  - _predict_race_on_the_fly: features.training_data から予測を生成して daily_predictions に保存
    - 正常系: features にデータあり → 予測生成・保存 → True
    - 異常系: features にデータなし → False
    - 異常系: モデルロード失敗 → False

Issue #231: 発走5分前に最新オッズで investment_decisions を上書きしてから IPAT 購入する
"""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.automation.api.app import (
    _predict_race_on_the_fly,
    _refresh_investment_decisions_for_race,
)

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


def _make_cnt_df(cnt: int) -> pd.DataFrame:
    """features.training_data の COUNT クエリ結果"""
    return pd.DataFrame([{"cnt": cnt}])


def _make_features_df() -> pd.DataFrame:
    """features.training_data から取得する DataFrame（カラムは最小限）"""
    return pd.DataFrame([
        {
            "race_id": RACE_ID, "race_date": TARGET_DATE,
            "horse_id": f"h{i:04d}", "horse_number": i,
            "horse_name": f"ウマ{i}", "venue_code": "09",
            "race_number": 12,
            "dummy_feature": float(i),
        }
        for i in range(1, 9)
    ])


class TestRefreshInvestmentDecisions:
    """_refresh_investment_decisions_for_race のテスト"""

    @patch("scripts.run_strategy.save_decisions_to_bq", return_value=1)
    @patch("scripts.run_strategy.fetch_combo_odds_for_date", return_value=pd.DataFrame())
    @patch("scripts.run_strategy.load_strategy_config", return_value={
        "p1": 0.3, "expected_return_threshold": 1.2, "budget_per_race": 3000,
        "top_n": 5, "min_bet_amount": 100, "min_prob_threshold": 0.10,
        "prob_weight_r_dominant": 1.0, "prob_weight_r_standard": 1.0,
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

        fake_pattern = MagicMock()
        fake_pattern.pattern = "standard"
        mock_select.return_value = (
            [{"horse_numbers": [1], "bet_type": "place", "odds": 2.0, "bet_amount": 300.0}],
            fake_pattern,
        )

        result = _refresh_investment_decisions_for_race(PROJECT_ID, RACE_ID, TARGET_DATE)

        assert result is True
        mock_save.assert_called_once()

    @patch("google.cloud.bigquery.Client")
    def test_empty_predictions_and_empty_features_returns_false(self, mock_bq_cls):
        """daily_predictions も features.training_data も空 → False（フォールバック）"""
        mock_client = MagicMock()
        mock_bq_cls.return_value = mock_client
        # 1回目: daily_predictions が空
        # 2回目: features.training_data の COUNT が 0
        mock_client.query.return_value.to_dataframe.side_effect = [
            pd.DataFrame(),
            _make_cnt_df(0),
        ]

        result = _refresh_investment_decisions_for_race(PROJECT_ID, RACE_ID, TARGET_DATE)

        assert result is False
        assert mock_client.query.call_count == 2

    @patch("src.automation.api.app._predict_race_on_the_fly", return_value=False)
    @patch("google.cloud.bigquery.Client")
    def test_empty_predictions_but_features_exist_triggers_on_the_fly(
        self,
        mock_bq_cls,
        mock_predict_on_the_fly,
    ):
        """daily_predictions 空 かつ features.training_data にデータあり → オンザフライ予測を呼ぶこと"""
        mock_client = MagicMock()
        mock_bq_cls.return_value = mock_client
        # 1回目: daily_predictions が空
        # 2回目: features.training_data に cnt=8 (データあり)
        # 3回目: オンザフライ後の再取得でも空（mock_predict_on_the_fly が False を返すため）
        mock_client.query.return_value.to_dataframe.side_effect = [
            pd.DataFrame(),
            _make_cnt_df(8),
        ]

        result = _refresh_investment_decisions_for_race(PROJECT_ID, RACE_ID, TARGET_DATE)

        assert result is False
        mock_predict_on_the_fly.assert_called_once_with(
            PROJECT_ID, RACE_ID, TARGET_DATE, mock_client
        )

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


class TestPredictRaceOnTheFly:
    """_predict_race_on_the_fly のテスト"""

    @patch("src.models.predict.save_predictions_to_bq", return_value=8)
    @patch("src.automation.api.app._resolve_model_path")
    @patch("src.models.train.build_feature_matrix")
    @patch("src.models.train.load_config")
    def test_success_when_features_exist(
        self,
        mock_load_config,
        mock_build_feature_matrix,
        mock_resolve_model_path,
        mock_save_predictions_to_bq,
    ):
        """features.training_data にデータあり → 予測を生成して True を返すこと"""
        mock_load_config.return_value = {
            "data": {
                "exclude_columns": ["race_id", "horse_id"],
                "categorical_columns": [],
            }
        }
        mock_resolve_model_path.return_value = ("/tmp/model.txt", None)

        features_df = _make_features_df()
        mock_client = MagicMock()
        mock_client.query.return_value.to_dataframe.return_value = features_df

        fake_ranker = MagicMock()
        fake_ranker.predict.return_value = np.array([1.0 - i * 0.1 for i in range(len(features_df))])
        mock_build_feature_matrix.return_value = MagicMock()

        with patch("src.models.lgbm_ranker.LGBMRanker", return_value=fake_ranker):
            result = _predict_race_on_the_fly(PROJECT_ID, RACE_ID, TARGET_DATE, mock_client)

        assert result is True
        mock_save_predictions_to_bq.assert_called_once()

    def test_returns_false_when_features_empty(self):
        """features.training_data が空 → False を返すこと"""
        mock_client = MagicMock()
        mock_client.query.return_value.to_dataframe.return_value = pd.DataFrame()

        result = _predict_race_on_the_fly(PROJECT_ID, RACE_ID, TARGET_DATE, mock_client)

        assert result is False

    @patch("src.automation.api.app._resolve_model_path", side_effect=Exception("GCS error"))
    @patch("src.models.train.load_config")
    def test_returns_false_on_model_load_failure(
        self, mock_load_config, mock_resolve
    ):
        """モデルのロードに失敗した場合 → False を返し例外を送出しないこと"""
        mock_load_config.return_value = {
            "data": {
                "exclude_columns": ["race_id", "horse_id"],
                "categorical_columns": [],
            }
        }
        features_df = _make_features_df()
        mock_client = MagicMock()
        mock_client.query.return_value.to_dataframe.return_value = features_df

        result = _predict_race_on_the_fly(PROJECT_ID, RACE_ID, TARGET_DATE, mock_client)

        assert result is False
