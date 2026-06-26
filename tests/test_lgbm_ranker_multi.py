"""
LGBMRankerMulti モデルのテスト
"""

import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.lgbm_ranker_multi import (
    JRA_PRIZE_WEIGHTS,
    LABEL_GAIN_MULTI,
    LGBMRankerMulti,
    LGBMRankerMultiConfig,
)
from src.models.train import prepare_features_multi_label


# ---------------------------------------------------------------------------
# 定数の検証
# ---------------------------------------------------------------------------

class TestJraPrizeWeights:
    """JRA賞金ウェイト定数のテスト"""

    def test_prize_weights_keys(self):
        """着順1〜10のキーが存在すること"""
        assert set(JRA_PRIZE_WEIGHTS.keys()) == set(range(1, 11))

    def test_prize_weights_values(self):
        """指定された賞金ウェイト値と一致すること"""
        expected = {
            1: 120, 2: 90, 3: 70, 4: 15, 5: 10,
            6: 8,   7: 7,  8: 6,  9: 4, 10: 2,
        }
        assert JRA_PRIZE_WEIGHTS == expected

    def test_prize_weights_descending(self):
        """着順が大きくなるにつれてウェイトが単調減少すること"""
        values = [JRA_PRIZE_WEIGHTS[i] for i in range(1, 11)]
        assert all(values[i] > values[i + 1] for i in range(len(values) - 1))

    def test_label_gain_length(self):
        """LABEL_GAIN_MULTI は 121 要素（0〜120）あること"""
        assert len(LABEL_GAIN_MULTI) == 121

    def test_label_gain_identity(self):
        """label_gain[i] = i であること（ラベル値=ゲイン値）"""
        for i, gain in enumerate(LABEL_GAIN_MULTI):
            assert gain == i


# ---------------------------------------------------------------------------
# 多値ラベル変換の検証
# ---------------------------------------------------------------------------

class TestPrepareFeatureMultiLabel:
    """prepare_features_multi_label のテスト"""

    @pytest.fixture
    def sample_df(self):
        """テスト用 DataFrame（5頭2レース）"""
        np.random.seed(0)
        records = []
        for race_id in ["R001", "R002"]:
            for pos in range(1, 6):
                records.append({
                    "race_id": race_id,
                    "finish_position": pos,
                    "feature_a": np.random.randn(),
                    "feature_b": np.random.randn(),
                })
        return pd.DataFrame(records)

    def test_label_mapping_top10(self, sample_df):
        """着順1〜5 が正しいJRA賞金ウェイトラベルに変換されること"""
        _, y, _ = prepare_features_multi_label(
            sample_df,
            exclude_columns=["race_id", "finish_position"],
            categorical_columns=[],
        )
        # 2レース分、同じ着順パターンが繰り返される
        for race_offset in [0, 5]:
            assert y[race_offset + 0] == JRA_PRIZE_WEIGHTS[1]   # 1着=120
            assert y[race_offset + 1] == JRA_PRIZE_WEIGHTS[2]   # 2着=90
            assert y[race_offset + 2] == JRA_PRIZE_WEIGHTS[3]   # 3着=70
            assert y[race_offset + 3] == JRA_PRIZE_WEIGHTS[4]   # 4着=15
            assert y[race_offset + 4] == JRA_PRIZE_WEIGHTS[5]   # 5着=10

    def test_label_zero_for_11th_or_more(self):
        """11着以下はラベル0であること"""
        df = pd.DataFrame({
            "race_id": ["R001"] * 3,
            "finish_position": [11, 16, 18],
            "feature_a": [0.0, 0.0, 0.0],
        })
        _, y, _ = prepare_features_multi_label(
            df,
            exclude_columns=["race_id", "finish_position"],
            categorical_columns=[],
        )
        assert all(y == 0)

    def test_label_zero_for_null_position(self):
        """finish_position が NULL / 0（取消等）はラベル0であること"""
        df = pd.DataFrame({
            "race_id": ["R001"] * 3,
            "finish_position": [None, 0, float("nan")],
            "feature_a": [0.0, 0.0, 0.0],
        })
        _, y, _ = prepare_features_multi_label(
            df,
            exclude_columns=["race_id", "finish_position"],
            categorical_columns=[],
        )
        assert all(y == 0)

    def test_label_dtype_is_int(self, sample_df):
        """ラベルが整数型であること"""
        _, y, _ = prepare_features_multi_label(
            sample_df,
            exclude_columns=["race_id", "finish_position"],
            categorical_columns=[],
        )
        assert np.issubdtype(y.dtype, np.integer)

    def test_groups_match_race_sizes(self, sample_df):
        """グループサイズがレースごとの馬数と一致すること"""
        _, _, groups = prepare_features_multi_label(
            sample_df,
            exclude_columns=["race_id", "finish_position"],
            categorical_columns=[],
        )
        assert groups == [5, 5]


# ---------------------------------------------------------------------------
# LGBMRankerMultiConfig のテスト
# ---------------------------------------------------------------------------

class TestLGBMRankerMultiConfig:
    """LGBMRankerMultiConfig のテスト"""

    def test_objective_is_lambdarank(self):
        """objective が lambdarank であること"""
        config = LGBMRankerMultiConfig()
        assert config.params["objective"] == "lambdarank"

    def test_label_gain_in_params(self):
        """params に label_gain が設定されていること"""
        config = LGBMRankerMultiConfig()
        assert config.params["label_gain"] == LABEL_GAIN_MULTI


# ---------------------------------------------------------------------------
# LGBMRankerMulti の学習・予測・保存テスト
# ---------------------------------------------------------------------------

class TestLGBMRankerMulti:
    """LGBMRankerMulti のテスト"""

    @pytest.fixture
    def sample_data(self):
        """テスト用サンプルデータ（10レース × 10頭）"""
        np.random.seed(42)
        n_races = 10
        horses_per_race = 10
        n_total = n_races * horses_per_race

        X = pd.DataFrame({
            f"feature_{i}": np.random.randn(n_total) for i in range(5)
        })

        # 各レースで着順1〜10を繰り返し、JRA賞金ウェイトラベルを付与
        positions = np.tile(np.arange(1, horses_per_race + 1), n_races)
        y = np.array([JRA_PRIZE_WEIGHTS.get(int(p), 0) for p in positions], dtype=int)
        groups = [horses_per_race] * n_races

        return X, y, groups

    def test_train_and_predict(self, sample_data):
        """学習と予測が正常に動作すること"""
        X, y, groups = sample_data
        split = 8 * 10
        X_train, X_valid = X.iloc[:split], X.iloc[split:]
        y_train, y_valid = y[:split], y[split:]

        config = LGBMRankerMultiConfig(
            num_boost_round=10,
            early_stopping_rounds=5,
            log_evaluation=0,
        )
        ranker = LGBMRankerMulti(config=config)
        model = ranker.train(
            X_train, y_train, groups[:8],
            X_valid, y_valid, groups[8:],
        )

        assert model is not None
        scores = ranker.predict(X_valid)
        assert len(scores) == len(X_valid)
        assert all(np.isfinite(scores))

    def test_save_and_load_roundtrip(self, sample_data):
        """保存→読み込み後も同じ予測スコアが得られること"""
        X, y, groups = sample_data
        split = 8 * 10
        X_train, X_valid = X.iloc[:split], X.iloc[split:]
        y_train, y_valid = y[:split], y[split:]

        config = LGBMRankerMultiConfig(
            num_boost_round=10,
            early_stopping_rounds=5,
            log_evaluation=0,
        )
        ranker = LGBMRankerMulti(config=config)
        ranker.train(
            X_train, y_train, groups[:8],
            X_valid, y_valid, groups[8:],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = str(Path(tmpdir) / "model_multi.txt")
            ranker.save(model_path)

            assert Path(model_path).exists()
            meta_path = Path(model_path).with_suffix(".meta.json")
            assert meta_path.exists()

            # meta.json に model_type と jra_prize_weights が記録されていること
            meta = json.loads(meta_path.read_text())
            assert meta.get("model_type") == "ranker_multi"
            # JSONは整数キーを文字列に変換するため str(k) で比較
            assert meta.get("jra_prize_weights") == {str(k): v for k, v in JRA_PRIZE_WEIGHTS.items()}

            # 読み込み後に同じスコアが得られること
            loaded = LGBMRankerMulti()
            loaded.load(model_path)
            np.testing.assert_array_almost_equal(
                ranker.predict(X_valid),
                loaded.predict(X_valid),
            )

    def test_meta_json_has_training_period(self, sample_data):
        """training_period が meta.json に記録されること"""
        X, y, groups = sample_data
        split = 8 * 10
        X_train, X_valid = X.iloc[:split], X.iloc[split:]
        y_train, y_valid = y[:split], y[split:]

        config = LGBMRankerMultiConfig(num_boost_round=10, early_stopping_rounds=5, log_evaluation=0)
        ranker = LGBMRankerMulti(config=config)
        ranker.train(X_train, y_train, groups[:8], X_valid, y_valid, groups[8:])

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = str(Path(tmpdir) / "model_multi.txt")
            training_period = {
                "train_from": "2024-01-01",
                "train_to": "2025-12-31",
                "train_rows": len(X_train),
                "valid_from": "2026-01-01",
                "valid_to": "2026-06-14",
                "valid_rows": len(X_valid),
            }
            ranker.save(model_path, training_period=training_period)

            meta = json.loads(Path(model_path).with_suffix(".meta.json").read_text())
            assert meta["training_period"]["train_from"] == "2024-01-01"
            assert meta["training_period"]["valid_rows"] == len(X_valid)

    def test_calibration_temperature_roundtrip(self, sample_data):
        """calibration_temperature が meta.json に保存・読込されること（Issue #414）"""
        X, y, groups = sample_data
        split = 8 * 10
        X_train, X_valid = X.iloc[:split], X.iloc[split:]
        y_train, y_valid = y[:split], y[split:]

        config = LGBMRankerMultiConfig(num_boost_round=10, early_stopping_rounds=5, log_evaluation=0)
        ranker = LGBMRankerMulti(config=config)
        ranker.train(X_train, y_train, groups[:8], X_valid, y_valid, groups[8:])
        ranker.calibration_temperature = 1.73

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = str(Path(tmpdir) / "model_multi.txt")
            ranker.save(model_path)

            meta = json.loads(Path(model_path).with_suffix(".meta.json").read_text())
            assert meta["calibration_temperature"] == 1.73

            loaded = LGBMRankerMulti()
            loaded.load(model_path)
            assert loaded.calibration_temperature == 1.73

    def test_calibration_temperature_backward_compat(self, sample_data):
        """温度未設定の旧モデルは calibration_temperature=None で読み込まれること"""
        X, y, groups = sample_data
        split = 8 * 10
        X_train, X_valid = X.iloc[:split], X.iloc[split:]
        y_train, y_valid = y[:split], y[split:]

        config = LGBMRankerMultiConfig(num_boost_round=10, early_stopping_rounds=5, log_evaluation=0)
        ranker = LGBMRankerMulti(config=config)
        ranker.train(X_train, y_train, groups[:8], X_valid, y_valid, groups[8:])

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = str(Path(tmpdir) / "model_multi.txt")
            ranker.save(model_path)  # calibration_temperature 未設定

            meta = json.loads(Path(model_path).with_suffix(".meta.json").read_text())
            assert "calibration_temperature" not in meta

            loaded = LGBMRankerMulti()
            loaded.load(model_path)
            assert loaded.calibration_temperature is None
