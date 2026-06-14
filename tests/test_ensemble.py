"""
アンサンブルモジュールのテスト

ensemble_rank_scores() の動作を検証する。
"""

import math

import numpy as np
import pandas as pd
import pytest

from src.models.ensemble import ensemble_rank_scores


class TestZscoreNormalization:
    """レース内Zスコア正規化の検証"""

    def test_zscore_mean_zero_std_one(self):
        """Zスコア正規化後、各レース内の平均≈0・標準偏差≈1になること"""
        df = pd.DataFrame({
            "race_id": ["R1"] * 5 + ["R2"] * 5,
            "pred_score_multi": [10.0, 20.0, 30.0, 40.0, 50.0,
                                  1.0, 2.0, 3.0, 4.0, 5.0],
            "pred_score_regression": [0.1, 0.2, 0.3, 0.4, 0.5,
                                       1.0, 2.0, 3.0, 4.0, 5.0],
        })
        result = ensemble_rank_scores(df, weight_multi=0.5)
        assert isinstance(result, pd.Series)
        assert len(result) == 10

    def test_zscore_each_group_normalized(self):
        """weight_multi=1.0 のとき、結果は multi のZスコアのみ反映される"""
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "pred_score_multi": [1.0, 2.0, 3.0],
            "pred_score_regression": [10.0, 20.0, 30.0],
        })
        result = ensemble_rank_scores(df, weight_multi=1.0)
        # multi Zスコア: mean=2, std=1 -> [-1, 0, 1]
        expected = pd.Series([-1.0, 0.0, 1.0])
        pd.testing.assert_series_equal(result.reset_index(drop=True), expected)

    def test_zscore_regression_only(self):
        """weight_multi=0.0 のとき、結果は regression のZスコアのみ反映される
        regression は高い値ほど強い馬（符号反転不要）"""
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "pred_score_multi": [1.0, 2.0, 3.0],
            "pred_score_regression": [-1.0, -2.0, -3.0],
        })
        result = ensemble_rank_scores(df, weight_multi=0.0)
        # regression Zスコア: z of [-1,-2,-3] -> mean=-2, std=1 -> [1, 0, -1]
        # horse0(regression=-1) が最強 -> 最高スコア
        expected = pd.Series([1.0, 0.0, -1.0])
        pd.testing.assert_series_equal(result.reset_index(drop=True), expected)


class TestWeightedAverage:
    """加重平均の正確性検証"""

    def test_weighted_average_default(self):
        """weight_multi=0.7 のとき正しい加重平均になること"""
        df = pd.DataFrame({
            "race_id": ["R1", "R1"],
            "pred_score_multi": [1.0, -1.0],
            "pred_score_regression": [2.0, -2.0],
        })
        result = ensemble_rank_scores(df, weight_multi=0.7)
        # z_multi: z of [1,-1] -> [1/sqrt(2), -1/sqrt(2)]
        # z_regression: z of [2,-2] -> [1/sqrt(2), -1/sqrt(2)]
        # final = (0.7 + 0.3) * [1/sqrt(2), -1/sqrt(2)] = [1/sqrt(2), -1/sqrt(2)]
        expected_0 = 1.0 / math.sqrt(2)
        assert result.iloc[0] > 0
        assert result.iloc[1] < 0
        assert abs(result.iloc[0] - expected_0) < 1e-9

    def test_weight_multi_equals_one(self):
        """weight_multi=1.0 のとき multi スコアのみ使われること"""
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "pred_score_multi": [3.0, 6.0, 9.0],
            "pred_score_regression": [100.0, 200.0, 300.0],
        })
        result_multi_only = ensemble_rank_scores(df, weight_multi=1.0)
        # regression スコアが変化しても結果は同じ
        df2 = df.copy()
        df2["pred_score_regression"] = [1.0, 1.0, 1.0]
        result_multi_only2 = ensemble_rank_scores(df2, weight_multi=1.0)
        pd.testing.assert_series_equal(result_multi_only, result_multi_only2)

    def test_ordering_preserved(self):
        """スコアが高い馬ほど final_score が高くなること"""
        df = pd.DataFrame({
            "race_id": ["R1"] * 4,
            "pred_score_multi": [4.0, 3.0, 2.0, 1.0],
            "pred_score_regression": [-1.0, -2.0, -3.0, -4.0],
        })
        result = ensemble_rank_scores(df)
        assert result.iloc[0] > result.iloc[1] > result.iloc[2] > result.iloc[3]


class TestNaNFallback:
    """片方のスコアがNaNの場合のフォールバック動作"""

    def test_regression_nan_uses_multi_only(self):
        """regression が全NaNのとき、multi の Zスコアのみで最終スコアを算出"""
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "pred_score_multi": [1.0, 2.0, 3.0],
            "pred_score_regression": [np.nan, np.nan, np.nan],
        })
        result = ensemble_rank_scores(df)
        # multi Zスコアと同じになる
        expected = pd.Series([-1.0, 0.0, 1.0])
        pd.testing.assert_series_equal(result.reset_index(drop=True), expected)

    def test_multi_nan_uses_regression_only(self):
        """multi が全NaNのとき、regression の Zスコアのみで算出（高い値ほど強い）"""
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "pred_score_multi": [np.nan, np.nan, np.nan],
            "pred_score_regression": [-3.0, -2.0, -1.0],
        })
        result = ensemble_rank_scores(df)
        # regression Zスコア: z of [-3,-2,-1] -> [-1, 0, 1]
        # horse2(regression=-1) が最強 -> 最高スコア
        assert result.iloc[0] < result.iloc[1] < result.iloc[2]

    def test_row_level_nan_fallback(self):
        """行レベルで一方がNaNのとき、存在するスコアのみ使用"""
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "pred_score_multi": [1.0, np.nan, 3.0],
            "pred_score_regression": [np.nan, -2.0, -3.0],
        })
        result = ensemble_rank_scores(df)
        assert not result.isna().any()

    def test_both_nan_returns_nan(self):
        """両方NaNの行はNaNを返し、片方が存在する行はNaNを返さないこと"""
        df = pd.DataFrame({
            "race_id": ["R1", "R1"],
            "pred_score_multi": [np.nan, 2.0],
            "pred_score_regression": [np.nan, -2.0],
        })
        result = ensemble_rank_scores(df)
        assert np.isnan(result.iloc[0])
        assert not np.isnan(result.iloc[1])


class TestEdgeCases:
    """エッジケースの検証"""

    def test_std_zero_all_same_score(self):
        """全馬スコアが同じ場合（std=0）、0.0を返すこと"""
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "pred_score_multi": [5.0, 5.0, 5.0],
            "pred_score_regression": [-2.0, -2.0, -2.0],
        })
        result = ensemble_rank_scores(df)
        # 両方std=0 -> z_multi=0, z_regression=0 -> final=0
        assert (result == 0.0).all()

    def test_single_horse_per_race(self):
        """1頭しかいないレース（std未定義）でも0.0を返すこと"""
        df = pd.DataFrame({
            "race_id": ["R1"],
            "pred_score_multi": [10.0],
            "pred_score_regression": [-5.0],
        })
        result = ensemble_rank_scores(df)
        assert len(result) == 1
        assert result.iloc[0] == 0.0

    def test_multiple_races_independent(self):
        """複数レースが独立してZスコア正規化されること"""
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R2", "R2"],
            "pred_score_multi": [1.0, 3.0, 100.0, 300.0],
            "pred_score_regression": [-1.0, -3.0, -100.0, -300.0],
        })
        result = ensemble_rank_scores(df)
        # R1 と R2 で z スコアは同じ（同じ相対差）
        assert abs(result.iloc[0] - result.iloc[2]) < 1e-9
        assert abs(result.iloc[1] - result.iloc[3]) < 1e-9

    def test_invalid_weight_raises(self):
        """weight_multi が [0,1] 外の値の場合 ValueError を発生させること"""
        df = pd.DataFrame({
            "race_id": ["R1"],
            "pred_score_multi": [1.0],
            "pred_score_regression": [-1.0],
        })
        with pytest.raises(ValueError, match="weight_multi"):
            ensemble_rank_scores(df, weight_multi=1.5)
        with pytest.raises(ValueError, match="weight_multi"):
            ensemble_rank_scores(df, weight_multi=-0.1)
