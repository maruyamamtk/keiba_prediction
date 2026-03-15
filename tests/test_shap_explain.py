"""
SHAP説明コンポーネントのユニットテスト

_compute_shap の計算ロジックを中心に検証する。
戻り値: (DataFrame, None) 成功 / (None, str) 失敗 のタプル形式。
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("GCP_PROJECT_ID", "test-project")


def _make_mock_ranker(n_features: int = 5):
    """テスト用 LGBMRanker モックを作る"""
    mock_ranker = MagicMock()
    mock_ranker.model.feature_name.return_value = [f"feat_{i}" for i in range(n_features)]
    return mock_ranker


class TestComputeShap:
    """_compute_shap のテスト"""

    def test_returns_dataframe_with_required_columns(self):
        from src.dashboard.components.shap_explain import _compute_shap

        n = 5
        feature_names = [f"feat_{i}" for i in range(n)]
        horse_df = pd.DataFrame(
            {f: [float(i)] for i, f in enumerate(feature_names)}
        )

        ranker = _make_mock_ranker(n)

        mock_shap_values = np.array([[0.1, -0.2, 0.3, -0.05, 0.15]])
        with patch("shap.TreeExplainer") as mock_explainer_cls:
            mock_explainer = MagicMock()
            mock_explainer.shap_values.return_value = mock_shap_values
            mock_explainer_cls.return_value = mock_explainer

            result, err = _compute_shap(ranker, horse_df, feature_names)

        assert err is None
        assert result is not None
        assert {"feature", "shap_value", "feature_value", "abs_shap"}.issubset(result.columns)
        assert len(result) == n

    def test_sorted_by_abs_shap_descending(self):
        from src.dashboard.components.shap_explain import _compute_shap

        feature_names = ["a", "b", "c"]
        horse_df = pd.DataFrame({"a": [1.0], "b": [2.0], "c": [3.0]})
        ranker = _make_mock_ranker(3)

        # |shap| が b > c > a となるよう設定
        mock_shap_values = np.array([[0.05, -0.8, 0.3]])
        with patch("shap.TreeExplainer") as mock_explainer_cls:
            mock_explainer = MagicMock()
            mock_explainer.shap_values.return_value = mock_shap_values
            mock_explainer_cls.return_value = mock_explainer

            result, err = _compute_shap(ranker, horse_df, feature_names)

        assert err is None
        assert result.iloc[0]["feature"] == "b"   # |shap|=0.8 が最大
        assert result.iloc[1]["feature"] == "c"   # |shap|=0.3
        assert result.iloc[2]["feature"] == "a"   # |shap|=0.05

    def test_handles_1d_shap_values(self):
        """shap_values が 1D 配列で返ってくる場合も正常処理する"""
        from src.dashboard.components.shap_explain import _compute_shap

        feature_names = ["x", "y"]
        horse_df = pd.DataFrame({"x": [1.0], "y": [2.0]})
        ranker = _make_mock_ranker(2)

        mock_shap_values = np.array([0.5, -0.3])  # 1D
        with patch("shap.TreeExplainer") as mock_explainer_cls:
            mock_explainer = MagicMock()
            mock_explainer.shap_values.return_value = mock_shap_values
            mock_explainer_cls.return_value = mock_explainer

            result, err = _compute_shap(ranker, horse_df, feature_names)

        assert err is None
        assert len(result) == 2

    def test_handles_3d_shap_values(self):
        """shap_values が 3D (n_outputs, n_samples, n_features) で返る場合を処理する"""
        from src.dashboard.components.shap_explain import _compute_shap

        feature_names = ["x", "y"]
        horse_df = pd.DataFrame({"x": [1.0], "y": [2.0]})
        ranker = _make_mock_ranker(2)

        mock_shap_values = np.array([[[0.5, -0.3]]])  # 3D: (1, 1, 2)
        with patch("shap.TreeExplainer") as mock_explainer_cls:
            mock_explainer = MagicMock()
            mock_explainer.shap_values.return_value = mock_shap_values
            mock_explainer_cls.return_value = mock_explainer

            result, err = _compute_shap(ranker, horse_df, feature_names)

        assert err is None
        assert len(result) == 2

    def test_returns_none_with_error_message_on_exception(self):
        """SHAP 計算で例外が発生した場合は (None, エラーメッセージ) を返す"""
        from src.dashboard.components.shap_explain import _compute_shap

        feature_names = ["x"]
        horse_df = pd.DataFrame({"x": [1.0]})
        ranker = _make_mock_ranker(1)

        with patch("shap.TreeExplainer", side_effect=RuntimeError("SHAP error")):
            result, err = _compute_shap(ranker, horse_df, feature_names)

        assert result is None
        assert err is not None
        assert "RuntimeError" in err
        assert "SHAP error" in err

    def test_returns_error_when_no_available_features(self):
        """モデルの特徴量が horse_df に一切ない場合はエラーメッセージを返す"""
        from src.dashboard.components.shap_explain import _compute_shap

        feature_names = ["model_feat_A", "model_feat_B"]
        horse_df = pd.DataFrame({"completely_different_col": [1.0]})
        ranker = _make_mock_ranker(2)

        result, err = _compute_shap(ranker, horse_df, feature_names)

        assert result is None
        assert err is not None
        assert "一致しません" in err

    def test_skips_missing_features_gracefully(self):
        """training_data に存在しない特徴量はスキップして残りで計算する"""
        from src.dashboard.components.shap_explain import _compute_shap

        feature_names = ["feat_0", "feat_1", "feat_missing"]
        horse_df = pd.DataFrame({"feat_0": [1.0], "feat_1": [2.0]})
        ranker = _make_mock_ranker(3)

        mock_shap_values = np.array([[0.1, -0.2]])  # 2特徴量分
        with patch("shap.TreeExplainer") as mock_explainer_cls:
            mock_explainer = MagicMock()
            mock_explainer.shap_values.return_value = mock_shap_values
            mock_explainer_cls.return_value = mock_explainer

            result, err = _compute_shap(ranker, horse_df, feature_names)

        assert err is None
        assert len(result) == 2
        assert "feat_missing" not in result["feature"].values


class TestFetchHorseFeatures:
    """fetch_horse_features のテスト"""

    def test_returns_dataframe(self):
        mock_df = pd.DataFrame({
            "race_date": ["2026-03-15"],
            "venue_code": ["09"],
            "race_number": [5],
            "horse_number": [3],
            "feat_0": [1.5],
        })

        with patch("src.dashboard.data._get_bq_client") as mock_client_fn:
            mock_client = MagicMock()
            mock_client.query.return_value.to_dataframe.return_value = mock_df
            mock_client_fn.return_value = mock_client

            from src.dashboard import data as dash_data
            dash_data.fetch_horse_features.clear()
            result = dash_data.fetch_horse_features("2026-03-15", "09", 5, 3)

        assert not result.empty
        assert result.iloc[0]["horse_number"] == 3

    def test_returns_empty_on_error(self):
        with patch("src.dashboard.data._get_bq_client") as mock_client_fn:
            mock_client_fn.side_effect = Exception("BQ error")

            from src.dashboard import data as dash_data
            dash_data.fetch_horse_features.clear()
            result = dash_data.fetch_horse_features("2026-03-15", "09", 5, 3)

        assert result.empty
