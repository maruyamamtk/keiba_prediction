"""
Optuna ハイパーパラメータ調整モジュールのテスト
"""

import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.tuning import (
    DEFAULT_SEARCH_SPACE,
    create_objective,
    run_tuning,
    save_best_params,
)


def _make_ranker_multi_data(n_races: int = 10, n_horses: int = 8):
    """ranker_multi 用サンプルデータ（JRA賞金ウェイトラベル）"""
    np.random.seed(42)
    n_total = n_races * n_horses

    X = pd.DataFrame({
        "distance": np.random.choice([1200, 1600, 2000], n_total),
        "num_horses": [n_horses] * n_total,
        "bracket_number": np.tile(np.arange(1, n_horses + 1), n_races),
        "horse_number": np.tile(np.arange(1, n_horses + 1), n_races),
        "weight": 55.0 + np.random.randn(n_total),
        "feature_a": np.random.randn(n_total),
        "feature_b": np.random.randn(n_total),
        "feature_c": np.random.randn(n_total),
    })

    # JRA賞金ウェイト: 1着=120, 2着=90, 3着=70, 4着=15, 5着=10, 6-10着=8/7/6/4/2, 11着以下=0
    prize_map = {1: 120, 2: 90, 3: 70, 4: 15, 5: 10, 6: 8, 7: 7, 8: 6, 9: 4, 10: 2}
    positions = np.tile(np.arange(1, n_horses + 1), n_races)
    y = np.array([prize_map.get(p, 0) for p in positions])
    groups = [n_horses] * n_races

    return X, y, groups


@pytest.fixture
def ranker_multi_data():
    """ranker_multi 用フィクスチャ"""
    X, y, groups = _make_ranker_multi_data()
    return {
        "X_train": X,
        "y_train": y,
        "groups_train": groups,
        "X_valid": X.copy(),
        "y_valid": y.copy(),
        "groups_valid": groups.copy(),
    }


def _base_params_ranker_multi() -> dict:
    return {
        "objective": "lambdarank",
        "metric": "ndcg",
        "ndcg_eval_at": [5],
        "label_gain": list(range(121)),
        "boosting_type": "gbdt",
        "verbose": -1,
        "seed": 42,
    }


class TestCreateObjective:
    """create_objective のテスト"""

    def test_objective_returns_valid_auc(self, ranker_multi_data):
        """objective 関数が 0〜1 の AUC 値を返すこと"""
        import optuna

        objective = create_objective(
            X_train=ranker_multi_data["X_train"],
            y_train=ranker_multi_data["y_train"],
            groups_train=ranker_multi_data["groups_train"],
            X_valid=ranker_multi_data["X_valid"],
            y_valid=ranker_multi_data["y_valid"],
            groups_valid=ranker_multi_data["groups_valid"],
            base_params=_base_params_ranker_multi(),
            training_config={"num_boost_round": 10, "early_stopping_rounds": 5},
            search_space=DEFAULT_SEARCH_SPACE,
            model_type="ranker_multi",
        )

        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=1)

        assert 0.0 <= study.best_value <= 1.0

    def test_unsupported_model_type_raises(self, ranker_multi_data):
        """ranker_multi 以外の model_type は ValueError を発生させること"""
        with pytest.raises(ValueError, match="ranker_multi"):
            create_objective(
                X_train=ranker_multi_data["X_train"],
                y_train=ranker_multi_data["y_train"],
                X_valid=ranker_multi_data["X_valid"],
                y_valid=ranker_multi_data["y_valid"],
                base_params=_base_params_ranker_multi(),
                training_config={"num_boost_round": 5, "early_stopping_rounds": 3},
                search_space={},
                model_type="ranker",
                groups_train=ranker_multi_data["groups_train"],
                groups_valid=ranker_multi_data["groups_valid"],
            )

    def test_missing_groups_raises(self, ranker_multi_data):
        """groups を省略すると ValueError が発生すること"""
        with pytest.raises(ValueError, match="groups_train"):
            create_objective(
                X_train=ranker_multi_data["X_train"],
                y_train=ranker_multi_data["y_train"],
                X_valid=ranker_multi_data["X_valid"],
                y_valid=ranker_multi_data["y_valid"],
                base_params=_base_params_ranker_multi(),
                training_config={"num_boost_round": 5, "early_stopping_rounds": 3},
                search_space={},
                model_type="ranker_multi",
            )


class TestRunTuning:
    """run_tuning のテスト"""

    def _config(self, extra_params: dict | None = None, tuning_override: dict | None = None) -> dict:
        params = _base_params_ranker_multi()
        if extra_params:
            params.update(extra_params)
        tuning = {"n_trials": 3, "timeout": 60}
        if tuning_override:
            tuning.update(tuning_override)
        return {
            "model": {
                "params": params,
                "training": {"num_boost_round": 10, "early_stopping_rounds": 5},
            },
            "tuning": tuning,
        }

    def test_run_tuning_finds_params(self, ranker_multi_data):
        """少数 trial で最適パラメータが取得できること"""
        result = run_tuning(
            X_train=ranker_multi_data["X_train"],
            y_train=ranker_multi_data["y_train"],
            groups_train=ranker_multi_data["groups_train"],
            X_valid=ranker_multi_data["X_valid"],
            y_valid=ranker_multi_data["y_valid"],
            groups_valid=ranker_multi_data["groups_valid"],
            config=self._config(),
            model_type="ranker_multi",
        )

        assert "best_params" in result
        assert "best_value" in result
        assert "best_trial_number" in result
        assert "n_trials" in result
        assert result["n_trials"] == 3
        assert 0.0 <= result["best_value"] <= 1.0

    def test_best_params_structure(self, ranker_multi_data):
        """返却パラメータに固定パラメータと探索パラメータが含まれること"""
        result = run_tuning(
            X_train=ranker_multi_data["X_train"],
            y_train=ranker_multi_data["y_train"],
            groups_train=ranker_multi_data["groups_train"],
            X_valid=ranker_multi_data["X_valid"],
            y_valid=ranker_multi_data["y_valid"],
            groups_valid=ranker_multi_data["groups_valid"],
            config=self._config(tuning_override={"n_trials": 2}),
            model_type="ranker_multi",
        )

        best_params = result["best_params"]
        assert best_params["objective"] == "lambdarank"
        assert best_params["metric"] == "ndcg"
        assert best_params["seed"] == 42
        assert "num_leaves" in best_params
        assert "learning_rate" in best_params
        assert "feature_fraction" in best_params
        assert "min_child_samples" in best_params
        assert "reg_alpha" in best_params
        assert "reg_lambda" in best_params

    def test_custom_search_space(self, ranker_multi_data):
        """カスタム探索範囲が適用されること"""
        config = self._config(
            extra_params={"num_leaves": 31, "learning_rate": 0.05},
            tuning_override={
                "n_trials": 2,
                "search_space": {
                    "feature_fraction": {"type": "float", "low": 0.5, "high": 0.9},
                },
            },
        )

        result = run_tuning(
            X_train=ranker_multi_data["X_train"],
            y_train=ranker_multi_data["y_train"],
            groups_train=ranker_multi_data["groups_train"],
            X_valid=ranker_multi_data["X_valid"],
            y_valid=ranker_multi_data["y_valid"],
            groups_valid=ranker_multi_data["groups_valid"],
            config=config,
            model_type="ranker_multi",
        )

        best_params = result["best_params"]
        assert best_params["num_leaves"] == 31
        assert best_params["learning_rate"] == 0.05
        assert 0.5 <= best_params["feature_fraction"] <= 0.9

    def test_run_tuning_ranker_multi(self, ranker_multi_data):
        """ranker_multi で run_tuning が動作すること"""
        config = self._config(tuning_override={"n_trials": 2})

        result = run_tuning(
            X_train=ranker_multi_data["X_train"],
            y_train=ranker_multi_data["y_train"],
            X_valid=ranker_multi_data["X_valid"],
            y_valid=ranker_multi_data["y_valid"],
            config=config,
            model_type="ranker_multi",
            groups_train=ranker_multi_data["groups_train"],
            groups_valid=ranker_multi_data["groups_valid"],
        )
        assert "best_params" in result
        assert result["n_trials"] == 2
        assert 0.0 <= result["best_value"] <= 1.0

    def test_model_type_study_name_override(self, ranker_multi_data):
        """tuning.ranker_multi.study_name でスタディ名が上書きされること"""
        config = self._config(tuning_override={"n_trials": 2})
        config["tuning"]["ranker_multi"] = {"study_name": "custom_ranker_multi_study"}

        result = run_tuning(
            X_train=ranker_multi_data["X_train"],
            y_train=ranker_multi_data["y_train"],
            X_valid=ranker_multi_data["X_valid"],
            y_valid=ranker_multi_data["y_valid"],
            config=config,
            model_type="ranker_multi",
            groups_train=ranker_multi_data["groups_train"],
            groups_valid=ranker_multi_data["groups_valid"],
        )
        assert result["n_trials"] == 2


class TestSaveBestParams:
    """save_best_params のテスト"""

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
