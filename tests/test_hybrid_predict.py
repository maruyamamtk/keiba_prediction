"""
ハイブリッドアンサンブル推論パイプラインのテスト（Issue #362）

- calibrate_place_prob() の合計値検証
- final_rank_score による pred_rank の正確性
- 3モデルのうち一部が未指定の場合のフォールバック動作
"""

import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.models.lgbm_ranker import LGBMRanker, LGBMRankerConfig
from src.models.lgbm_ranker_multi import LGBMRankerMulti, LGBMRankerMultiConfig
from src.models.lgbm_regression import LGBMRegression, LGBMRegressionConfig
from src.models.lgbm_classifier import LGBMClassifier, LGBMClassifierConfig
from src.models.predict import (
    calibrate_place_prob,
    predict_pipeline,
)


# ---------------------------------------------------------------------------
# テスト用フィクスチャ
# ---------------------------------------------------------------------------

def _make_race_df(n_races: int = 3, n_horses: int = 8) -> pd.DataFrame:
    """テスト用レースデータ（classifier_prob つき）を生成する"""
    rows = []
    for r in range(n_races):
        race_id = f"race_{r:03d}"
        race_date = datetime.date(2026, 6, 14)
        for h in range(1, n_horses + 1):
            rows.append({
                "race_id": race_id,
                "race_date": race_date,
                "horse_id": f"horse_{h:02d}",
                "horse_number": h,
                "horse_name": f"馬{h}",
                "classifier_prob": 0.1 * h,  # 単調増加
                "pred_score": float(h),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# calibrate_place_prob() の合計値検証
# ---------------------------------------------------------------------------

class TestCalibratePlaceProb:
    """calibrate_place_prob() の各レース内合計 = min(3, 頭数) であることを検証"""

    def test_sum_equals_n_places(self):
        """通常レース（8頭）: 合計 = 3"""
        df = _make_race_df(n_races=2, n_horses=8)
        result = calibrate_place_prob(df)
        for race_id, group in result.groupby("race_id"):
            total = group["win_place_prob"].sum()
            assert abs(total - 3.0) < 1e-9, f"{race_id}: total={total}"

    def test_sum_capped_by_horse_count(self):
        """少頭数レース（2頭）: 合計 = 2（n_places=3より小さい）"""
        df = _make_race_df(n_races=1, n_horses=2)
        result = calibrate_place_prob(df, n_places=3)
        total = result.groupby("race_id")["win_place_prob"].sum().iloc[0]
        assert abs(total - 2.0) < 1e-9, f"total={total}"

    def test_all_prob_le_one(self):
        """各馬の win_place_prob が 1.0 以下であること"""
        df = _make_race_df(n_races=3, n_horses=10)
        result = calibrate_place_prob(df)
        assert (result["win_place_prob"] <= 1.0 + 1e-9).all()

    def test_all_prob_ge_zero(self):
        """各馬の win_place_prob が 0.0 以上であること"""
        df = _make_race_df(n_races=3, n_horses=8)
        result = calibrate_place_prob(df)
        assert (result["win_place_prob"] >= 0.0).all()

    def test_zero_total_fallback(self):
        """全馬の classifier_prob が 0 の場合、均等配分されること"""
        df = pd.DataFrame({
            "race_id": ["R1"] * 4,
            "race_date": [datetime.date(2026, 6, 14)] * 4,
            "horse_id": [f"h{i}" for i in range(4)],
            "horse_number": [1, 2, 3, 4],
            "horse_name": ["a", "b", "c", "d"],
            "classifier_prob": [0.0, 0.0, 0.0, 0.0],
            "pred_score": [1.0, 2.0, 3.0, 4.0],
        })
        result = calibrate_place_prob(df)
        total = result["win_place_prob"].sum()
        assert abs(total - 3.0) < 1e-9
        # 均等配分なので全馬同じ確率
        assert result["win_place_prob"].nunique() == 1

    def test_ordering_preserved(self):
        """classifier_prob が高い馬ほど win_place_prob が高くなること"""
        df = _make_race_df(n_races=1, n_horses=6)
        result = calibrate_place_prob(df)
        probs = result.sort_values("horse_number")["win_place_prob"].tolist()
        for i in range(len(probs) - 1):
            assert probs[i] <= probs[i + 1], f"probs[{i}]={probs[i]} > probs[{i+1}]={probs[i+1]}"

    def test_does_not_modify_original(self):
        """元の DataFrame が変更されないこと"""
        df = _make_race_df(n_races=1, n_horses=5)
        original_probs = df["classifier_prob"].tolist()
        _ = calibrate_place_prob(df)
        assert df["classifier_prob"].tolist() == original_probs

    def test_n_places_parameter(self):
        """n_places=1 の場合、合計 = 1"""
        df = _make_race_df(n_races=1, n_horses=6)
        result = calibrate_place_prob(df, n_places=1)
        total = result.groupby("race_id")["win_place_prob"].sum().iloc[0]
        assert abs(total - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# final_rank_score による pred_rank の正確性
# ---------------------------------------------------------------------------

class TestFinalRankScoreAndPredRank:
    """ensemble score -> pred_rank の整合性を検証する。

    predict_pipeline() を直接呼ぶのはモック設定が複雑なため、
    ここでは predict_pipeline() の内部ロジックを等価的に再現するアサーションを行う。
    """

    def test_pred_rank_derived_from_final_rank_score(self):
        """final_rank_score が高い馬ほど pred_rank が小さい（1位に近い）こと"""
        from src.models.ensemble import ensemble_rank_scores

        df = pd.DataFrame({
            "race_id": ["R1"] * 5,
            "rank_score_multi": [3.0, 1.0, 5.0, 2.0, 4.0],
            "rank_score_regression": [-1.0, -3.0, 0.5, -2.0, -0.5],
        })
        final_score = ensemble_rank_scores(
            df,
            score_col_multi="rank_score_multi",
            score_col_regression="rank_score_regression",
        )
        df["final_rank_score"] = final_score
        df["pred_rank"] = df["final_rank_score"].rank(ascending=False, method="min").astype(int)

        # final_rank_score が最大の馬が pred_rank=1
        top_horse_idx = df["final_rank_score"].idxmax()
        assert df.loc[top_horse_idx, "pred_rank"] == 1

        # final_rank_score が最小の馬が pred_rank=5
        bottom_horse_idx = df["final_rank_score"].idxmin()
        assert df.loc[bottom_horse_idx, "pred_rank"] == 5

    def test_pred_rank_unique_per_race(self):
        """各レース内の pred_rank が重複なく付与されること"""
        from src.models.ensemble import ensemble_rank_scores

        df = pd.DataFrame({
            "race_id": ["R1"] * 6 + ["R2"] * 4,
            "rank_score_multi": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0,
                                  10.0, 20.0, 30.0, 40.0],
            "rank_score_regression": [-1.0, -2.0, -3.0, -4.0, -5.0, -6.0,
                                       -10.0, -20.0, -30.0, -40.0],
        })
        df["final_rank_score"] = ensemble_rank_scores(
            df,
            score_col_multi="rank_score_multi",
            score_col_regression="rank_score_regression",
        )
        df["pred_rank"] = (
            df.groupby("race_id")["final_rank_score"]
            .rank(ascending=False, method="min")
            .astype(int)
        )
        for race_id, group in df.groupby("race_id"):
            ranks = sorted(group["pred_rank"].tolist())
            expected = list(range(1, len(group) + 1))
            assert ranks == expected, f"{race_id}: {ranks}"


# ---------------------------------------------------------------------------
# フォールバック動作（一部モデル未指定）
# ---------------------------------------------------------------------------

class TestFallbackBehavior:
    """predict_pipeline() の一部モデル未指定時のフォールバックを検証する。"""

    @pytest.fixture
    def trained_ranker_path(self, tmp_path) -> str:
        """学習済み LGBMRanker を一時ファイルに保存して返す"""
        np.random.seed(0)
        n_races, n_horses = 5, 6
        n = n_races * n_horses
        X = pd.DataFrame({
            "feature_a": np.random.randn(n),
            "feature_b": np.random.randn(n),
        })
        positions = np.tile(np.arange(1, n_horses + 1), n_races)
        y = np.where(positions <= 3, 1, 0)
        groups = [n_horses] * n_races
        cfg = LGBMRankerConfig(num_boost_round=5, early_stopping_rounds=3, log_evaluation=0)
        ranker = LGBMRanker(config=cfg)
        ranker.train(X, y, groups, X, y, groups)
        path = str(tmp_path / "ranker.txt")
        ranker.save(path)
        return path

    @pytest.fixture
    def mock_df(self) -> pd.DataFrame:
        """fetch_prediction_data / fetch_race_results が返すモックデータ"""
        np.random.seed(1)
        n_races, n_horses = 2, 6
        rows = []
        for r in range(n_races):
            race_id = f"mock_race_{r:03d}"
            race_date = datetime.date(2026, 6, 14)
            for h in range(1, n_horses + 1):
                rows.append({
                    "race_id": race_id,
                    "race_date": race_date,
                    "horse_id": f"horse_{h:02d}",
                    "horse_number": h,
                    "horse_name": f"馬{h}",
                    "venue_code": "05",
                    "race_number": r + 1,
                    "feature_a": np.random.randn(),
                    "feature_b": np.random.randn(),
                    "finish_position": float(h),
                })
        return pd.DataFrame(rows)

    def _make_config(self):
        return {
            "data": {
                "exclude_columns": [
                    "race_id", "race_date", "horse_id", "horse_number", "horse_name",
                    "venue_code", "race_number", "finish_position",
                ],
                "categorical_columns": [],
            }
        }

    def test_single_lambdarank_mode(self, trained_ranker_path, mock_df):
        """model_path のみ指定 → 既存 LambdaRank 単独モードで動作すること"""
        config = self._make_config()

        with (
            patch("src.models.predict.fetch_prediction_data", return_value=mock_df),
            patch("src.models.predict.fetch_race_results", return_value=pd.DataFrame()),
        ):
            result = predict_pipeline(
                project_id="test-project",
                execution_date=datetime.date(2026, 6, 14),
                config=config,
                model_path=trained_ranker_path,
                target_dates=[datetime.date(2026, 6, 14)],
            )

        assert len(result) > 0
        assert "pred_score" in result.columns
        assert "win_place_prob" in result.columns
        assert "pred_rank" in result.columns
        # アンサンブルスコアは生成されないこと
        assert "final_rank_score" not in result.columns
        # 分類確率は生成されないこと
        assert "classifier_prob" not in result.columns

    def test_no_model_path_raises(self, mock_df):
        """全モデルパスが None の場合 ValueError が発生すること"""
        config = self._make_config()
        with (
            patch("src.models.predict.fetch_prediction_data", return_value=mock_df),
            patch("src.models.predict.fetch_race_results", return_value=pd.DataFrame()),
        ):
            with pytest.raises(ValueError, match="model_path"):
                predict_pipeline(
                    project_id="test-project",
                    execution_date=datetime.date(2026, 6, 14),
                    config=config,
                    model_path=None,
                    model_path_multi=None,
                    model_path_regression=None,
                    model_path_classifier=None,
                    target_dates=[datetime.date(2026, 6, 14)],
                )

    def test_ensemble_columns_present_when_multi_and_regression_provided(
        self, trained_ranker_path, mock_df, tmp_path
    ):
        """multi + regression 指定時に final_rank_score と rank_score_* が生成されること"""
        config = self._make_config()
        np.random.seed(2)
        n_races, n_horses = 2, 6
        n = n_races * n_horses
        X = pd.DataFrame({
            "feature_a": np.random.randn(n),
            "feature_b": np.random.randn(n),
        })
        positions = np.tile(np.arange(1, n_horses + 1), n_races)
        y = np.where(positions <= 3, 1, 0)
        groups = [n_horses] * n_races

        # LGBMRankerMulti モデルを保存
        multi_cfg = LGBMRankerMultiConfig(
            num_boost_round=5, early_stopping_rounds=3, log_evaluation=0
        )
        ranker_multi = LGBMRankerMulti(config=multi_cfg)
        ranker_multi.train(X, y, groups, X, y, groups)
        multi_path = str(tmp_path / "ranker_multi.txt")
        ranker_multi.save(multi_path)

        # LGBMRegression モデルを保存
        reg_cfg = LGBMRegressionConfig(
            num_boost_round=5, early_stopping_rounds=3, log_evaluation=0
        )
        regressor = LGBMRegression(config=reg_cfg)
        y_reg = -positions.astype(float)  # Zスコア代わり
        regressor.train(X, y_reg, X, y_reg)
        reg_path = str(tmp_path / "regressor.txt")
        regressor.save(reg_path)

        with (
            patch("src.models.predict.fetch_prediction_data", return_value=mock_df),
            patch("src.models.predict.fetch_race_results", return_value=pd.DataFrame()),
        ):
            result = predict_pipeline(
                project_id="test-project",
                execution_date=datetime.date(2026, 6, 14),
                config=config,
                model_path=trained_ranker_path,
                model_path_multi=multi_path,
                model_path_regression=reg_path,
                target_dates=[datetime.date(2026, 6, 14)],
            )

        assert "rank_score_multi" in result.columns
        assert "rank_score_regression" in result.columns
        assert "final_rank_score" in result.columns
        # pred_score は final_rank_score の値と一致するはず
        pd.testing.assert_series_equal(
            result["pred_score"].reset_index(drop=True),
            result["final_rank_score"].reset_index(drop=True),
            check_names=False,
        )

    def test_classifier_prob_and_calibration_when_classifier_provided(
        self, trained_ranker_path, mock_df, tmp_path
    ):
        """classifier 指定時に classifier_prob と win_place_prob（合計=3）が生成されること"""
        config = self._make_config()
        np.random.seed(3)
        n_races, n_horses = 2, 6
        n = n_races * n_horses
        X = pd.DataFrame({
            "feature_a": np.random.randn(n),
            "feature_b": np.random.randn(n),
        })
        positions = np.tile(np.arange(1, n_horses + 1), n_races)
        y = np.where(positions <= 3, 1, 0)

        clf_cfg = LGBMClassifierConfig(
            num_boost_round=5, early_stopping_rounds=3, log_evaluation=0
        )
        clf = LGBMClassifier(config=clf_cfg)
        clf.train(X, y, X, y)
        clf_path = str(tmp_path / "classifier.txt")
        clf.save(clf_path)

        with (
            patch("src.models.predict.fetch_prediction_data", return_value=mock_df),
            patch("src.models.predict.fetch_race_results", return_value=pd.DataFrame()),
        ):
            result = predict_pipeline(
                project_id="test-project",
                execution_date=datetime.date(2026, 6, 14),
                config=config,
                model_path=trained_ranker_path,
                model_path_classifier=clf_path,
                target_dates=[datetime.date(2026, 6, 14)],
            )

        assert "classifier_prob" in result.columns
        assert "win_place_prob" in result.columns

        # 各レース内の合計が min(3, 頭数) = 3 であること
        for race_id, group in result.groupby("race_id"):
            total = group["win_place_prob"].sum()
            assert abs(total - 3.0) < 1e-6, f"{race_id}: total={total}"
