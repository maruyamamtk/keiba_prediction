"""
LGBMRanker モデルのテスト
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

from src.models.lgbm_ranker import LGBMRanker, LGBMRankerConfig


class TestLGBMRankerConfig:
    """LGBMRankerConfigのテスト"""

    def test_default_config(self):
        """デフォルト設定が正しく初期化されること"""
        config = LGBMRankerConfig()
        assert config.params["objective"] == "lambdarank"
        assert config.params["metric"] == "ndcg"
        assert config.params["ndcg_eval_at"] == [3]
        assert config.num_boost_round == 1000
        assert config.early_stopping_rounds == 50
        assert config.log_evaluation == 100

    def test_custom_config(self):
        """カスタム設定が正しく反映されること"""
        config = LGBMRankerConfig(
            params={"objective": "lambdarank", "num_leaves": 64},
            num_boost_round=500,
            early_stopping_rounds=20,
        )
        assert config.params["num_leaves"] == 64
        assert config.num_boost_round == 500
        assert config.early_stopping_rounds == 20


class TestLGBMRanker:
    """LGBMRankerのテスト"""

    @pytest.fixture
    def sample_data(self):
        """テスト用のサンプルデータを生成"""
        np.random.seed(42)
        n_races = 20
        horses_per_race = 10
        n_total = n_races * horses_per_race

        X = pd.DataFrame({
            f"feature_{i}": np.random.randn(n_total)
            for i in range(5)
        })

        # 着順ベースの整数relevanceスコア（1着=3, 2着=2, 3着=1, 4着以下=0）
        positions = np.tile(np.arange(1, horses_per_race + 1), n_races)
        y = np.where(positions == 1, 3,
             np.where(positions == 2, 2,
              np.where(positions == 3, 1, 0)))

        groups = [horses_per_race] * n_races

        return X, y, groups

    def test_train_and_predict(self, sample_data):
        """学習と予測が正常に動作すること"""
        X, y, groups = sample_data

        # 学習/検証に分割
        split = 15 * 10  # 15レース分を学習
        X_train, X_valid = X.iloc[:split], X.iloc[split:]
        y_train, y_valid = y[:split], y[split:]
        groups_train = groups[:15]
        groups_valid = groups[15:]

        config = LGBMRankerConfig(
            num_boost_round=10,
            early_stopping_rounds=5,
            log_evaluation=0,
        )
        ranker = LGBMRanker(config=config)
        model = ranker.train(X_train, y_train, groups_train, X_valid, y_valid, groups_valid)

        assert model is not None
        assert ranker.model is not None
        assert ranker.feature_names == list(X_train.columns)

        # 予測
        predictions = ranker.predict(X_valid)
        assert len(predictions) == len(X_valid)
        assert all(np.isfinite(predictions))

    def test_predict_without_training(self):
        """学習前に予測しようとするとエラーになること"""
        ranker = LGBMRanker()
        X = pd.DataFrame({"feature_0": [1.0, 2.0]})
        with pytest.raises(RuntimeError, match="モデルが学習されていません"):
            ranker.predict(X)

    def test_save_and_load(self, sample_data):
        """モデルの保存と読み込みが正常に動作すること"""
        X, y, groups = sample_data
        split = 15 * 10
        X_train, X_valid = X.iloc[:split], X.iloc[split:]
        y_train, y_valid = y[:split], y[split:]

        config = LGBMRankerConfig(num_boost_round=10, early_stopping_rounds=5, log_evaluation=0)
        ranker = LGBMRanker(config=config)
        ranker.train(X_train, y_train, groups[:15], X_valid, y_valid, groups[15:])

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = str(Path(tmpdir) / "model.txt")
            ranker.save(model_path)

            # ファイルが作成されていること
            assert Path(model_path).exists()
            assert Path(model_path).with_suffix(".meta.json").exists()

            # メタデータの中身を確認
            meta = json.loads(Path(model_path).with_suffix(".meta.json").read_text())
            assert "feature_names" in meta
            assert meta["feature_names"] == list(X_train.columns)
            assert "best_iteration" in meta

            # 読み込み
            loaded_ranker = LGBMRanker()
            loaded_ranker.load(model_path)
            assert loaded_ranker.model is not None
            assert loaded_ranker.feature_names == list(X_train.columns)

            # 読み込んだモデルで予測して結果が一致すること
            pred_original = ranker.predict(X_valid)
            pred_loaded = loaded_ranker.predict(X_valid)
            np.testing.assert_array_almost_equal(pred_original, pred_loaded)

    def test_save_without_training(self):
        """学習前に保存しようとするとエラーになること"""
        ranker = LGBMRanker()
        with pytest.raises(RuntimeError, match="保存するモデルがありません"):
            ranker.save("/tmp/dummy.txt")

    def test_load_nonexistent_file(self):
        """存在しないファイルを読み込もうとするとエラーになること"""
        ranker = LGBMRanker()
        with pytest.raises(FileNotFoundError):
            ranker.load("/tmp/nonexistent_model.txt")

    def test_feature_importance(self, sample_data):
        """特徴量重要度が取得できること"""
        X, y, groups = sample_data
        split = 15 * 10
        X_train, X_valid = X.iloc[:split], X.iloc[split:]
        y_train, y_valid = y[:split], y[split:]

        config = LGBMRankerConfig(num_boost_round=10, early_stopping_rounds=5, log_evaluation=0)
        ranker = LGBMRanker(config=config)
        ranker.train(X_train, y_train, groups[:15], X_valid, y_valid, groups[15:])

        importance = ranker.feature_importance()
        assert isinstance(importance, pd.DataFrame)
        assert "feature" in importance.columns
        assert "importance" in importance.columns
        assert len(importance) == X_train.shape[1]
        # 降順にソートされていること
        assert importance["importance"].is_monotonic_decreasing

    def test_feature_importance_without_training(self):
        """学習前に重要度を取得しようとするとエラーになること"""
        ranker = LGBMRanker()
        with pytest.raises(RuntimeError, match="モデルが学習されていません"):
            ranker.feature_importance()
