"""
SHAP説明コンポーネントのユニットテスト

_compute_shap の計算ロジックを中心に検証する。
LightGBM の pred_contrib=True を直接使う実装のテスト。
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

        # pred_contrib=True は (n_samples, n_features+1) を返す
        mock_contrib = np.array([[0.1, -0.2, 0.3, -0.05, 0.15, -0.05]])  # +base_value
        ranker.model.predict.return_value = mock_contrib

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

        # |shap| が b > c > a となるよう設定（最後の列はbase value）
        mock_contrib = np.array([[0.05, -0.8, 0.3, 0.0]])
        ranker.model.predict.return_value = mock_contrib

        result, err = _compute_shap(ranker, horse_df, feature_names)

        assert err is None
        assert result.iloc[0]["feature"] == "b"   # |shap|=0.8 が最大
        assert result.iloc[1]["feature"] == "c"   # |shap|=0.3
        assert result.iloc[2]["feature"] == "a"   # |shap|=0.05

    def test_encodes_object_columns_to_numeric(self):
        """object dtype の列（BQからの文字列）を数値化して処理できる"""
        from src.dashboard.components.shap_explain import _compute_shap

        feature_names = ["num_feat", "course_type"]
        horse_df = pd.DataFrame({
            "num_feat": [1.5],
            "course_type": ["turf"],  # object dtype (BQ由来)
        })
        ranker = _make_mock_ranker(2)

        mock_contrib = np.array([[0.3, -0.1, 0.0]])  # +base_value
        ranker.model.predict.return_value = mock_contrib

        result, err = _compute_shap(ranker, horse_df, feature_names)

        assert err is None
        assert len(result) == 2
        # course_type の feature_value は元の文字列 "turf" が保持されている
        course_row = result[result["feature"] == "course_type"].iloc[0]
        assert course_row["feature_value"] == "turf"

    def test_returns_none_with_error_message_on_exception(self):
        """例外が発生した場合は (None, エラーメッセージ) を返す"""
        from src.dashboard.components.shap_explain import _compute_shap

        feature_names = ["x"]
        horse_df = pd.DataFrame({"x": [1.0]})
        ranker = _make_mock_ranker(1)
        ranker.model.predict.side_effect = RuntimeError("predict error")

        result, err = _compute_shap(ranker, horse_df, feature_names)

        assert result is None
        assert err is not None
        assert "RuntimeError" in err
        assert "predict error" in err

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

        mock_contrib = np.array([[0.1, -0.2, 0.0]])  # 2特徴量 + base_value
        ranker.model.predict.return_value = mock_contrib

        result, err = _compute_shap(ranker, horse_df, feature_names)

        assert err is None
        assert len(result) == 2
        assert "feat_missing" not in result["feature"].values

    def test_uses_numpy_array_for_prediction(self):
        """ranker.model.predict に numpy 配列が渡されることを確認する"""
        from src.dashboard.components.shap_explain import _compute_shap

        feature_names = ["x", "course_type"]
        horse_df = pd.DataFrame({"x": [1.0], "course_type": ["turf"]})
        ranker = _make_mock_ranker(2)

        mock_contrib = np.array([[0.1, 0.2, 0.0]])
        ranker.model.predict.return_value = mock_contrib

        _compute_shap(ranker, horse_df, feature_names)

        # predict に渡された第1引数が numpy 配列であること
        call_args = ranker.model.predict.call_args
        passed_data = call_args[0][0]
        assert isinstance(passed_data, np.ndarray), "numpy 配列が渡されるべき"


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


class TestComputeRealtimePredictions:
    """_compute_realtime_predictions のテスト"""

    def _make_race_df(self, n: int = 5):
        return pd.DataFrame({
            "horse_number": list(range(1, n + 1)),
            "horse_name": [f"馬{i}" for i in range(1, n + 1)],
            **{f"feat_{i}": [float(j) for j in range(n)] for i in range(3)},
        })

    def test_returns_required_columns(self):
        from src.dashboard.components.shap_explain import _compute_realtime_predictions

        feature_names = ["feat_0", "feat_1", "feat_2"]
        race_df = self._make_race_df(5)
        ranker = MagicMock()
        ranker.model.predict.return_value = np.array([0.9, 0.7, 0.5, 0.3, 0.1])

        result = _compute_realtime_predictions(ranker, feature_names, race_df)

        assert {"horse_number", "horse_name", "pred_score", "win_place_prob", "rank_in_race"}.issubset(result.columns)
        assert len(result) == 5

    def test_rank_is_assigned_by_descending_score(self):
        from src.dashboard.components.shap_explain import _compute_realtime_predictions

        feature_names = ["feat_0"]
        race_df = pd.DataFrame({
            "horse_number": [1, 2, 3],
            "horse_name": ["馬A", "馬B", "馬C"],
            "feat_0": [1.0, 2.0, 3.0],
        })
        ranker = MagicMock()
        ranker.model.predict.return_value = np.array([0.3, 0.8, 0.5])

        result = _compute_realtime_predictions(ranker, feature_names, race_df)

        horse2 = result[result["horse_number"] == 2].iloc[0]
        horse1 = result[result["horse_number"] == 1].iloc[0]
        assert horse2["rank_in_race"] == 1  # スコア最大
        assert horse1["rank_in_race"] == 3  # スコア最小

    def test_win_place_prob_sums_to_n_places(self):
        """win_place_prob の合計が min(3, 出走頭数) になる"""
        from src.dashboard.components.shap_explain import _compute_realtime_predictions

        feature_names = ["feat_0"]
        race_df = pd.DataFrame({
            "horse_number": list(range(1, 9)),
            "horse_name": [f"馬{i}" for i in range(1, 9)],
            "feat_0": [float(i) for i in range(8)],
        })
        ranker = MagicMock()
        ranker.model.predict.return_value = np.array([0.9, 0.8, 0.7, 0.4, 0.3, 0.2, 0.1, 0.05])

        result = _compute_realtime_predictions(ranker, feature_names, race_df)

        assert result["win_place_prob"].sum() == pytest.approx(3.0, abs=1e-6)

    def test_returns_nan_scores_when_no_features_available(self):
        """モデル特徴量が race_df に存在しない場合は NaN を返す"""
        from src.dashboard.components.shap_explain import _compute_realtime_predictions

        feature_names = ["model_feat_A", "model_feat_B"]
        race_df = pd.DataFrame({
            "horse_number": [1, 2],
            "horse_name": ["馬A", "馬B"],
            "unrelated_col": [1.0, 2.0],
        })
        ranker = MagicMock()

        result = _compute_realtime_predictions(ranker, feature_names, race_df)

        assert len(result) == 2
        assert result["pred_score"].isna().all()
        assert result["win_place_prob"].isna().all()
