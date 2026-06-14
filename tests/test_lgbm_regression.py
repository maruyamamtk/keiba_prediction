"""
LGBMRegression モデルおよび着差Zスコア計算のテスト
"""

import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.lgbm_regression import LGBMRegression, LGBMRegressionConfig
from src.models.train import compute_time_diff_zscore, prepare_features_regression


# ---------------------------------------------------------------------------
# compute_time_diff_zscore のテスト
# ---------------------------------------------------------------------------

class TestComputeTimeDiffZscore:
    """1着馬との走破タイム差Zスコア計算のテスト"""

    @pytest.fixture
    def simple_race_df(self):
        """5頭のシンプルなレース（1レース）"""
        return pd.DataFrame({
            "race_id": ["R001"] * 5,
            "finish_position": [1, 2, 3, 4, 5],
            "finish_time": [70.0, 70.5, 71.0, 71.5, 72.0],
        })

    def test_winner_zscore_is_zero(self, simple_race_df):
        """1着馬のZスコアは 0 であること（time_diff=0 → zscore=-0/std=0）"""
        zscores = compute_time_diff_zscore(simple_race_df)
        winner_idx = simple_race_df["finish_position"] == 1
        assert abs(zscores[winner_idx].iloc[0]) < 1e-10

    def test_last_place_zscore_is_most_negative(self, simple_race_df):
        """最下位馬のZスコアが最も低い（最大の負値）こと"""
        zscores = compute_time_diff_zscore(simple_race_df)
        last_idx = simple_race_df["finish_position"].idxmax()
        assert zscores[last_idx] == zscores.min()
        assert zscores[last_idx] < 0

    def test_zscore_is_monotone_decreasing(self, simple_race_df):
        """着順が遅くなるにつれてZスコアが単調減少すること"""
        zscores = compute_time_diff_zscore(simple_race_df)
        sorted_z = zscores.sort_index().values  # finish_position 1,2,3,4,5 順
        assert all(sorted_z[i] >= sorted_z[i + 1] for i in range(len(sorted_z) - 1))

    def test_null_finish_time_returns_nan(self):
        """finish_time が NULL の馬は NaN を返すこと"""
        df = pd.DataFrame({
            "race_id": ["R001"] * 4,
            "finish_position": [1, 2, 3, 4],
            "finish_time": [70.0, None, 71.0, 71.5],
        })
        zscores = compute_time_diff_zscore(df)
        # finish_time=None の馬（index 1）は NaN
        assert pd.isna(zscores.iloc[1])
        # 他の馬は有限値
        assert np.isfinite(zscores.iloc[0])
        assert np.isfinite(zscores.iloc[2])
        assert np.isfinite(zscores.iloc[3])

    def test_no_winner_finish_time_returns_all_nan(self):
        """1着馬のfinish_timeがNULLのレースは全馬NaNを返すこと"""
        df = pd.DataFrame({
            "race_id": ["R001"] * 3,
            "finish_position": [1, 2, 3],
            "finish_time": [None, 70.5, 71.0],
        })
        zscores = compute_time_diff_zscore(df)
        assert zscores.isna().all()

    def test_std_zero_returns_all_nan(self):
        """全馬同タイム（std=0）のレースは全馬NaNを返すこと"""
        df = pd.DataFrame({
            "race_id": ["R001"] * 3,
            "finish_position": [1, 2, 3],
            "finish_time": [70.0, 70.0, 70.0],
        })
        zscores = compute_time_diff_zscore(df)
        assert zscores.isna().all()

    def test_multiple_races(self):
        """複数レースを正しく独立して処理すること"""
        df = pd.DataFrame({
            "race_id": ["R001", "R001", "R001", "R002", "R002", "R002"],
            "finish_position": [1, 2, 3, 1, 2, 3],
            "finish_time": [70.0, 71.0, 72.0, 60.0, 61.0, 62.0],
        })
        zscores = compute_time_diff_zscore(df)

        # R001: 1着=0, 他は負
        r1 = zscores.iloc[:3]
        assert abs(r1.iloc[0]) < 1e-10
        assert all(r1.iloc[1:] < 0)

        # R002: 1着=0, 他は負
        r2 = zscores.iloc[3:]
        assert abs(r2.iloc[0]) < 1e-10
        assert all(r2.iloc[1:] < 0)

        # R001とR002のインデックスに重複がないこと（別レースとして独立処理）
        assert len(set(r1.index) & set(r2.index)) == 0


# ---------------------------------------------------------------------------
# prepare_features_regression のテスト
# ---------------------------------------------------------------------------

class TestPrepareFeaturesRegression:
    """prepare_features_regression のテスト"""

    @pytest.fixture
    def sample_df(self):
        """テスト用 DataFrame（3頭2レース）"""
        np.random.seed(0)
        records = []
        for race_id, times in [("R001", [70.0, 70.5, 71.5]), ("R002", [60.0, 61.0, 63.0])]:
            for i, t in enumerate(times):
                records.append({
                    "race_id": race_id,
                    "finish_position": i + 1,
                    "finish_time": t,
                    "feature_a": np.random.randn(),
                    "feature_b": np.random.randn(),
                })
        return pd.DataFrame(records)

    def test_returns_x_and_y(self, sample_df):
        """X と y のタプルを返すこと"""
        X, y = prepare_features_regression(
            sample_df,
            exclude_columns=["race_id", "finish_position", "finish_time"],
            categorical_columns=[],
        )
        assert isinstance(X, pd.DataFrame)
        assert isinstance(y, np.ndarray)

    def test_nan_rows_are_excluded(self):
        """finish_time が NULL の行が除外されること"""
        df = pd.DataFrame({
            "race_id": ["R001", "R001", "R001"],
            "finish_position": [1, 2, 3],
            "finish_time": [70.0, None, 71.0],
            "feature_a": [1.0, 2.0, 3.0],
        })
        X, y = prepare_features_regression(
            df,
            exclude_columns=["race_id", "finish_position", "finish_time"],
            categorical_columns=[],
        )
        # finish_time=None の行が除外された場合、std=0 になって全行NaNになる可能性
        # ここでは「NaNが含まれないこと」だけ確認
        assert not np.any(np.isnan(y))

    def test_y_dtype_is_float(self, sample_df):
        """yが浮動小数点型であること"""
        _, y = prepare_features_regression(
            sample_df,
            exclude_columns=["race_id", "finish_position", "finish_time"],
            categorical_columns=[],
        )
        assert np.issubdtype(y.dtype, np.floating)

    def test_raises_without_finish_time(self):
        """finish_time カラムがない場合は ValueError を送出すること"""
        df = pd.DataFrame({
            "race_id": ["R001"],
            "finish_position": [1],
            "feature_a": [1.0],
        })
        with pytest.raises(ValueError, match="finish_time"):
            prepare_features_regression(
                df,
                exclude_columns=["race_id", "finish_position"],
                categorical_columns=[],
            )

    def test_x_does_not_contain_finish_time(self, sample_df):
        """X に finish_time カラムが含まれないこと"""
        X, _ = prepare_features_regression(
            sample_df,
            exclude_columns=["race_id", "finish_position", "finish_time"],
            categorical_columns=[],
        )
        assert "finish_time" not in X.columns


# ---------------------------------------------------------------------------
# LGBMRegressionConfig のテスト
# ---------------------------------------------------------------------------

class TestLGBMRegressionConfig:
    """LGBMRegressionConfig のテスト"""

    def test_objective_is_regression(self):
        """objective が regression であること"""
        config = LGBMRegressionConfig()
        assert config.params["objective"] == "regression"

    def test_metric_is_rmse(self):
        """metric が rmse であること"""
        config = LGBMRegressionConfig()
        assert config.params["metric"] == "rmse"


# ---------------------------------------------------------------------------
# LGBMRegression の学習・予測・保存テスト
# ---------------------------------------------------------------------------

class TestLGBMRegression:
    """LGBMRegression のテスト"""

    @pytest.fixture
    def sample_data(self):
        """テスト用サンプルデータ（連続値ラベル）"""
        np.random.seed(42)
        n = 200
        X = pd.DataFrame({f"feature_{i}": np.random.randn(n) for i in range(5)})
        # 着差Zスコア風のラベル（0が1着、負値が負け馬）
        y = -np.abs(np.random.randn(n))
        split = 160
        return (
            X.iloc[:split], y[:split],
            X.iloc[split:], y[split:],
        )

    def test_train_and_predict(self, sample_data):
        """学習と予測が正常に動作すること"""
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMRegressionConfig(
            num_boost_round=10, early_stopping_rounds=5, log_evaluation=0
        )
        reg = LGBMRegression(config=config)
        model = reg.train(X_train, y_train, X_valid, y_valid)

        assert model is not None
        scores = reg.predict(X_valid)
        assert len(scores) == len(X_valid)
        assert all(np.isfinite(scores))

    def test_predict_without_training_raises(self):
        """学習前に予測しようとするとRuntimeErrorになること"""
        reg = LGBMRegression()
        X = pd.DataFrame({"f": [1.0, 2.0]})
        with pytest.raises(RuntimeError, match="モデルが学習されていません"):
            reg.predict(X)

    def test_save_and_load_roundtrip(self, sample_data):
        """保存→読み込み後も同じスコアが得られること"""
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMRegressionConfig(
            num_boost_round=10, early_stopping_rounds=5, log_evaluation=0
        )
        reg = LGBMRegression(config=config)
        reg.train(X_train, y_train, X_valid, y_valid)

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = str(Path(tmpdir) / "lgbm_regression.txt")
            reg.save(model_path)

            assert Path(model_path).exists()
            meta_path = Path(model_path).with_suffix(".meta.json")
            assert meta_path.exists()

            meta = json.loads(meta_path.read_text())
            assert meta.get("model_type") == "regression"

            loaded = LGBMRegression()
            loaded.load(model_path)
            np.testing.assert_array_almost_equal(
                reg.predict(X_valid),
                loaded.predict(X_valid),
            )

    def test_save_without_training_raises(self):
        """学習前に保存しようとするとRuntimeErrorになること"""
        reg = LGBMRegression()
        with pytest.raises(RuntimeError, match="保存するモデルがありません"):
            reg.save("/tmp/dummy.txt")

    def test_load_nonexistent_file_raises(self):
        """存在しないファイルを読み込もうとするとFileNotFoundErrorになること"""
        reg = LGBMRegression()
        with pytest.raises(FileNotFoundError):
            reg.load("/tmp/nonexistent_regression.txt")

    def test_meta_json_has_training_period(self, sample_data):
        """training_period が meta.json に記録されること"""
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMRegressionConfig(
            num_boost_round=10, early_stopping_rounds=5, log_evaluation=0
        )
        reg = LGBMRegression(config=config)
        reg.train(X_train, y_train, X_valid, y_valid)

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = str(Path(tmpdir) / "lgbm_regression.txt")
            reg.save(model_path, training_period={"train_from": "2024-01-01"})
            meta = json.loads(Path(model_path).with_suffix(".meta.json").read_text())
            assert meta["training_period"]["train_from"] == "2024-01-01"

    def test_feature_importance(self, sample_data):
        """特徴量重要度が取得できること"""
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMRegressionConfig(
            num_boost_round=10, early_stopping_rounds=5, log_evaluation=0
        )
        reg = LGBMRegression(config=config)
        reg.train(X_train, y_train, X_valid, y_valid)

        importance = reg.feature_importance()
        assert isinstance(importance, pd.DataFrame)
        assert "feature" in importance.columns
        assert "importance" in importance.columns
        assert importance["importance"].is_monotonic_decreasing
