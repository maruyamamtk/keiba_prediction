"""
LGBMClassifier モデルのテスト
"""

import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.lgbm_classifier import LGBMClassifier, LGBMClassifierConfig


# ---------------------------------------------------------------------------
# LGBMClassifierConfig のテスト
# ---------------------------------------------------------------------------

class TestLGBMClassifierConfig:
    """LGBMClassifierConfig のテスト"""

    def test_objective_is_binary(self):
        """objective が binary であること"""
        config = LGBMClassifierConfig()
        assert config.params["objective"] == "binary"

    def test_metric_is_auc(self):
        """metric が auc であること"""
        config = LGBMClassifierConfig()
        assert config.params["metric"] == "auc"


# ---------------------------------------------------------------------------
# LGBMClassifier の学習・予測テスト
# ---------------------------------------------------------------------------

class TestLGBMClassifier:
    """LGBMClassifier のテスト"""

    @pytest.fixture
    def sample_data(self):
        """テスト用サンプルデータ（二値ラベル: 3着以内=1, それ以外=0）"""
        np.random.seed(42)
        n = 200
        X = pd.DataFrame({f"feature_{i}": np.random.randn(n) for i in range(5)})
        # 約1/4が正例（複勝率相当）
        y = (np.random.rand(n) < 0.25).astype(int)
        split = 160
        return (
            X.iloc[:split], y[:split],
            X.iloc[split:], y[split:],
        )

    def test_train_and_predict(self, sample_data):
        """学習と予測が正常に動作すること"""
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMClassifierConfig(
            num_boost_round=10, early_stopping_rounds=5, log_evaluation=0
        )
        clf = LGBMClassifier(config=config)
        model = clf.train(X_train, y_train, X_valid, y_valid)

        assert model is not None
        scores = clf.predict(X_valid)
        assert len(scores) == len(X_valid)

    def test_predict_returns_probabilities_in_range(self, sample_data):
        """predict() の出力が [0, 1] の範囲に収まること"""
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMClassifierConfig(
            num_boost_round=10, early_stopping_rounds=5, log_evaluation=0
        )
        clf = LGBMClassifier(config=config)
        clf.train(X_train, y_train, X_valid, y_valid)

        probs = clf.predict(X_valid)
        assert np.all(probs >= 0.0), "確率が 0 未満の値を含む"
        assert np.all(probs <= 1.0), "確率が 1 超の値を含む"

    def test_auc_above_random(self, sample_data):
        """AUC が 0.5 以上であること（ランダムより良い精度）"""
        from sklearn.metrics import roc_auc_score
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMClassifierConfig(
            num_boost_round=50, early_stopping_rounds=10, log_evaluation=0
        )
        clf = LGBMClassifier(config=config)
        clf.train(X_train, y_train, X_valid, y_valid)

        probs = clf.predict(X_valid)
        if len(np.unique(y_valid)) >= 2:
            auc = roc_auc_score(y_valid, probs)
            assert auc >= 0.5, f"AUC が 0.5 未満: {auc}"

    def test_predict_without_training_raises(self):
        """学習前に予測しようとするとRuntimeErrorになること"""
        clf = LGBMClassifier()
        X = pd.DataFrame({"f": [1.0, 2.0]})
        with pytest.raises(RuntimeError, match="モデルが学習されていません"):
            clf.predict(X)

    def test_save_and_load_roundtrip(self, sample_data):
        """保存→読み込み後も同じ確率が得られること"""
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMClassifierConfig(
            num_boost_round=10, early_stopping_rounds=5, log_evaluation=0
        )
        clf = LGBMClassifier(config=config)
        clf.train(X_train, y_train, X_valid, y_valid)

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = str(Path(tmpdir) / "lgbm_classifier.txt")
            clf.save(model_path)

            assert Path(model_path).exists()
            meta_path = Path(model_path).with_suffix(".meta.json")
            assert meta_path.exists()

            meta = json.loads(meta_path.read_text())
            assert meta.get("model_type") == "classifier"

            loaded = LGBMClassifier()
            loaded.load(model_path)
            np.testing.assert_array_almost_equal(
                clf.predict(X_valid),
                loaded.predict(X_valid),
            )

    def test_loaded_model_probabilities_in_range(self, sample_data):
        """読み込んだモデルの出力も [0, 1] の範囲に収まること"""
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMClassifierConfig(
            num_boost_round=10, early_stopping_rounds=5, log_evaluation=0
        )
        clf = LGBMClassifier(config=config)
        clf.train(X_train, y_train, X_valid, y_valid)

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = str(Path(tmpdir) / "lgbm_classifier.txt")
            clf.save(model_path)

            loaded = LGBMClassifier()
            loaded.load(model_path)
            probs = loaded.predict(X_valid)
            assert np.all(probs >= 0.0)
            assert np.all(probs <= 1.0)

    def test_save_without_training_raises(self):
        """学習前に保存しようとするとRuntimeErrorになること"""
        clf = LGBMClassifier()
        with pytest.raises(RuntimeError, match="保存するモデルがありません"):
            clf.save("/tmp/dummy_classifier.txt")

    def test_load_nonexistent_file_raises(self):
        """存在しないファイルを読み込もうとするとFileNotFoundErrorになること"""
        clf = LGBMClassifier()
        with pytest.raises(FileNotFoundError):
            clf.load("/tmp/nonexistent_classifier.txt")

    def test_meta_json_has_model_type_classifier(self, sample_data):
        """meta.json に model_type='classifier' が記録されること"""
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMClassifierConfig(
            num_boost_round=10, early_stopping_rounds=5, log_evaluation=0
        )
        clf = LGBMClassifier(config=config)
        clf.train(X_train, y_train, X_valid, y_valid)

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = str(Path(tmpdir) / "lgbm_classifier.txt")
            clf.save(model_path, training_period={"train_from": "2024-01-01"})
            meta = json.loads(Path(model_path).with_suffix(".meta.json").read_text())
            assert meta["model_type"] == "classifier"
            assert meta["training_period"]["train_from"] == "2024-01-01"

    def test_feature_importance(self, sample_data):
        """特徴量重要度が取得できること"""
        X_train, y_train, X_valid, y_valid = sample_data
        config = LGBMClassifierConfig(
            num_boost_round=10, early_stopping_rounds=5, log_evaluation=0
        )
        clf = LGBMClassifier(config=config)
        clf.train(X_train, y_train, X_valid, y_valid)

        importance = clf.feature_importance()
        assert isinstance(importance, pd.DataFrame)
        assert "feature" in importance.columns
        assert "importance" in importance.columns
        assert importance["importance"].is_monotonic_decreasing
