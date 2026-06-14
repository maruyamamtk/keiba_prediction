"""
Optuna ハイパーパラメータ調整モジュールのテスト
"""

import json
import tempfile
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest

from src.models.tuning import (
    DEFAULT_SEARCH_SPACE,
    create_objective,
    run_tuning,
    save_best_params,
)


@pytest.fixture
def sample_data():
    """LambdaRank用のサンプルデータ（二値ラベル）"""
    np.random.seed(42)
    n_races = 10
    n_horses = 8
    n_total = n_races * n_horses

    X_train = pd.DataFrame({
        "distance": np.random.choice([1200, 1600, 2000], n_total),
        "num_horses": [n_horses] * n_total,
        "bracket_number": np.tile(np.arange(1, n_horses + 1), n_races),
        "horse_number": np.tile(np.arange(1, n_horses + 1), n_races),
        "weight": 55.0 + np.random.randn(n_total),
        "feature_a": np.random.randn(n_total),
        "feature_b": np.random.randn(n_total),
        "feature_c": np.random.randn(n_total),
    })

    positions = np.tile(np.arange(1, n_horses + 1), n_races)
    y_train = np.where((positions >= 1) & (positions <= 3), 1, 0)
    groups_train = [n_horses] * n_races

    X_valid = X_train.copy()
    y_valid = y_train.copy()
    groups_valid = groups_train.copy()

    return {
        "X_train": X_train,
        "y_train": y_train,
        "groups_train": groups_train,
        "X_valid": X_valid,
        "y_valid": y_valid,
        "groups_valid": groups_valid,
    }


class TestCreateObjective:
    """create_objectiveのテスト"""

    def test_objective_returns_valid_auc(self, sample_data):
        """objective関数が0~1のAUC値を返すこと"""
        base_params = {
            "objective": "lambdarank",
            "metric": "ndcg",
            "ndcg_eval_at": [3],
            "boosting_type": "gbdt",
            "verbose": -1,
            "seed": 42,
        }
        training_config = {
            "num_boost_round": 10,
            "early_stopping_rounds": 5,
        }

        objective = create_objective(
            X_train=sample_data["X_train"],
            y_train=sample_data["y_train"],
            groups_train=sample_data["groups_train"],
            X_valid=sample_data["X_valid"],
            y_valid=sample_data["y_valid"],
            groups_valid=sample_data["groups_valid"],
            base_params=base_params,
            training_config=training_config,
            search_space=DEFAULT_SEARCH_SPACE,
        )

        import optuna
        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=1)

        assert 0.0 <= study.best_value <= 1.0


class TestRunTuning:
    """run_tuningのテスト"""

    def test_run_tuning_finds_params(self, sample_data):
        """少数trialで最適パラメータが取得できること"""
        config = {
            "model": {
                "params": {
                    "objective": "lambdarank",
                    "metric": "ndcg",
                    "ndcg_eval_at": [3],
                    "boosting_type": "gbdt",
                    "verbose": -1,
                    "seed": 42,
                },
                "training": {
                    "num_boost_round": 10,
                    "early_stopping_rounds": 5,
                },
            },
            "tuning": {
                "n_trials": 3,
                "timeout": 60,
            },
        }

        result = run_tuning(
            X_train=sample_data["X_train"],
            y_train=sample_data["y_train"],
            groups_train=sample_data["groups_train"],
            X_valid=sample_data["X_valid"],
            y_valid=sample_data["y_valid"],
            groups_valid=sample_data["groups_valid"],
            config=config,
        )

        assert "best_params" in result
        assert "best_value" in result
        assert "best_trial_number" in result
        assert "n_trials" in result
        assert result["n_trials"] == 3
        assert 0.0 <= result["best_value"] <= 1.0

    def test_best_params_structure(self, sample_data):
        """返却パラメータに固定パラメータと探索パラメータが含まれること"""
        config = {
            "model": {
                "params": {
                    "objective": "lambdarank",
                    "metric": "ndcg",
                    "ndcg_eval_at": [3],
                    "boosting_type": "gbdt",
                    "verbose": -1,
                    "seed": 42,
                },
                "training": {
                    "num_boost_round": 10,
                    "early_stopping_rounds": 5,
                },
            },
            "tuning": {
                "n_trials": 2,
                "timeout": 60,
            },
        }

        result = run_tuning(
            X_train=sample_data["X_train"],
            y_train=sample_data["y_train"],
            groups_train=sample_data["groups_train"],
            X_valid=sample_data["X_valid"],
            y_valid=sample_data["y_valid"],
            groups_valid=sample_data["groups_valid"],
            config=config,
        )

        best_params = result["best_params"]
        # 固定パラメータが含まれること
        assert best_params["objective"] == "lambdarank"
        assert best_params["metric"] == "ndcg"
        assert best_params["seed"] == 42
        # 探索パラメータが含まれること
        assert "num_leaves" in best_params
        assert "learning_rate" in best_params
        assert "feature_fraction" in best_params
        assert "min_child_samples" in best_params
        assert "reg_alpha" in best_params
        assert "reg_lambda" in best_params

    def test_custom_search_space(self, sample_data):
        """カスタム探索範囲が適用されること"""
        config = {
            "model": {
                "params": {
                    "objective": "lambdarank",
                    "metric": "ndcg",
                    "ndcg_eval_at": [3],
                    "boosting_type": "gbdt",
                    "verbose": -1,
                    "seed": 42,
                    "num_leaves": 31,
                    "learning_rate": 0.05,
                },
                "training": {
                    "num_boost_round": 10,
                    "early_stopping_rounds": 5,
                },
            },
            "tuning": {
                "n_trials": 2,
                "timeout": 60,
                "search_space": {
                    "feature_fraction": {
                        "type": "float",
                        "low": 0.5,
                        "high": 0.9,
                    },
                },
            },
        }

        result = run_tuning(
            X_train=sample_data["X_train"],
            y_train=sample_data["y_train"],
            groups_train=sample_data["groups_train"],
            X_valid=sample_data["X_valid"],
            y_valid=sample_data["y_valid"],
            groups_valid=sample_data["groups_valid"],
            config=config,
        )

        best_params = result["best_params"]
        # 探索範囲外のパラメータは固定値のまま
        assert best_params["num_leaves"] == 31
        assert best_params["learning_rate"] == 0.05
        # 探索対象のパラメータは範囲内
        assert 0.5 <= best_params["feature_fraction"] <= 0.9


class TestCreateObjectiveModelTypes:
    """model_type 別 create_objective のテスト"""

    def _make_config(self, objective: str, extra: dict | None = None) -> dict:
        params = {
            "objective": objective,
            "boosting_type": "gbdt",
            "verbose": -1,
            "seed": 42,
        }
        if extra:
            params.update(extra)
        return {
            "params": params,
            "training": {"num_boost_round": 5, "early_stopping_rounds": 3},
        }

    def test_ranker_multi_returns_auc(self, sample_data):
        """ranker_multi が AUC を返すこと"""
        import optuna

        base_params = {
            "objective": "lambdarank",
            "metric": "ndcg",
            "ndcg_eval_at": [3],
            "boosting_type": "gbdt",
            "verbose": -1,
            "seed": 42,
        }
        obj = create_objective(
            X_train=sample_data["X_train"],
            y_train=sample_data["y_train"],
            X_valid=sample_data["X_valid"],
            y_valid=sample_data["y_valid"],
            base_params=base_params,
            training_config={"num_boost_round": 5, "early_stopping_rounds": 3},
            search_space={"feature_fraction": {"type": "float", "low": 0.5, "high": 1.0}},
            model_type="ranker_multi",
            groups_train=sample_data["groups_train"],
            groups_valid=sample_data["groups_valid"],
        )
        study = optuna.create_study(direction="maximize")
        study.optimize(obj, n_trials=1)
        assert 0.0 <= study.best_value <= 1.0

    def test_regression_returns_negative_rmse(self, sample_data):
        """regression が -RMSE（負の値）を返すこと"""
        import optuna

        y_continuous = sample_data["y_train"].astype(float) + np.random.randn(len(sample_data["y_train"])) * 0.1
        base_params = {
            "objective": "regression",
            "metric": "rmse",
            "boosting_type": "gbdt",
            "verbose": -1,
            "seed": 42,
        }
        obj = create_objective(
            X_train=sample_data["X_train"],
            y_train=y_continuous,
            X_valid=sample_data["X_valid"],
            y_valid=y_continuous,
            base_params=base_params,
            training_config={"num_boost_round": 5, "early_stopping_rounds": 3},
            search_space={"feature_fraction": {"type": "float", "low": 0.5, "high": 1.0}},
            model_type="regression",
        )
        study = optuna.create_study(direction="maximize")
        study.optimize(obj, n_trials=1)
        assert study.best_value <= 0.0  # -RMSE は常に 0 以下

    def test_classifier_returns_auc(self, sample_data):
        """classifier が AUC を返すこと"""
        import optuna

        base_params = {
            "objective": "binary",
            "metric": "auc",
            "boosting_type": "gbdt",
            "verbose": -1,
            "seed": 42,
        }
        obj = create_objective(
            X_train=sample_data["X_train"],
            y_train=sample_data["y_train"].astype(float),
            X_valid=sample_data["X_valid"],
            y_valid=sample_data["y_valid"].astype(float),
            base_params=base_params,
            training_config={"num_boost_round": 5, "early_stopping_rounds": 3},
            search_space={"feature_fraction": {"type": "float", "low": 0.5, "high": 1.0}},
            model_type="classifier",
        )
        study = optuna.create_study(direction="maximize")
        study.optimize(obj, n_trials=1)
        assert 0.0 <= study.best_value <= 1.0

    def test_ranker_without_groups_raises(self, sample_data):
        """ranker 系で groups を省略すると ValueError が発生すること"""
        base_params = {
            "objective": "lambdarank",
            "metric": "ndcg",
            "ndcg_eval_at": [3],
            "boosting_type": "gbdt",
            "verbose": -1,
        }
        with pytest.raises(ValueError, match="groups_train"):
            create_objective(
                X_train=sample_data["X_train"],
                y_train=sample_data["y_train"],
                X_valid=sample_data["X_valid"],
                y_valid=sample_data["y_valid"],
                base_params=base_params,
                training_config={"num_boost_round": 5, "early_stopping_rounds": 3},
                search_space={},
                model_type="ranker",
            )


class TestRunTuningModelTypes:
    """model_type 別 run_tuning のテスト"""

    def _base_config(self, objective: str, metric: str, extra_params: dict | None = None) -> dict:
        params = {
            "objective": objective,
            "metric": metric,
            "boosting_type": "gbdt",
            "verbose": -1,
            "seed": 42,
        }
        if extra_params:
            params.update(extra_params)
        return {
            "model": {
                "params": params,
                "training": {"num_boost_round": 5, "early_stopping_rounds": 3},
            },
            "tuning": {
                "n_trials": 2,
                "timeout": 30,
                "search_space": {
                    "feature_fraction": {"type": "float", "low": 0.5, "high": 1.0},
                },
            },
        }

    def test_run_tuning_ranker_multi(self, sample_data):
        """ranker_multi で run_tuning が動作すること"""
        config = self._base_config(
            "lambdarank", "ndcg",
            extra_params={"ndcg_eval_at": [3]},
        )
        result = run_tuning(
            X_train=sample_data["X_train"],
            y_train=sample_data["y_train"],
            X_valid=sample_data["X_valid"],
            y_valid=sample_data["y_valid"],
            config=config,
            model_type="ranker_multi",
            groups_train=sample_data["groups_train"],
            groups_valid=sample_data["groups_valid"],
        )
        assert "best_params" in result
        assert result["n_trials"] == 2
        assert 0.0 <= result["best_value"] <= 1.0

    def test_run_tuning_regression(self, sample_data):
        """regression で run_tuning が動作し best_value が負であること"""
        y_cont = sample_data["y_train"].astype(float)
        config = self._base_config("regression", "rmse")
        result = run_tuning(
            X_train=sample_data["X_train"],
            y_train=y_cont,
            X_valid=sample_data["X_valid"],
            y_valid=y_cont,
            config=config,
            model_type="regression",
        )
        assert "best_params" in result
        assert result["n_trials"] == 2
        assert result["best_value"] <= 0.0  # -RMSE

    def test_run_tuning_classifier(self, sample_data):
        """classifier で run_tuning が動作すること"""
        config = self._base_config("binary", "auc")
        result = run_tuning(
            X_train=sample_data["X_train"],
            y_train=sample_data["y_train"].astype(float),
            X_valid=sample_data["X_valid"],
            y_valid=sample_data["y_valid"].astype(float),
            config=config,
            model_type="classifier",
        )
        assert "best_params" in result
        assert result["n_trials"] == 2
        assert 0.0 <= result["best_value"] <= 1.0

    def test_model_type_study_name_override(self, sample_data):
        """tuning.<model_type>.study_name でスタディ名が上書きされること"""
        config = self._base_config("binary", "auc")
        config["tuning"]["classifier"] = {"study_name": "custom_classifier_study"}
        result = run_tuning(
            X_train=sample_data["X_train"],
            y_train=sample_data["y_train"].astype(float),
            X_valid=sample_data["X_valid"],
            y_valid=sample_data["y_valid"].astype(float),
            config=config,
            model_type="classifier",
        )
        assert result["n_trials"] == 2


class TestSaveBestParams:
    """save_best_paramsのテスト"""

    def test_save_and_load(self):
        """パラメータの保存・読み込みが正しく動作すること"""
        params = {
            "objective": "lambdarank",
            "num_leaves": 50,
            "learning_rate": 0.1,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(Path(tmpdir) / "best_params.json")
            save_best_params(params, path)

            assert Path(path).exists()
            loaded = json.loads(Path(path).read_text())
            assert loaded == params
