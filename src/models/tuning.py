"""
Optuna によるハイパーパラメータ調整モジュール

LightGBM LambdaRank モデルのハイパーパラメータを
Optuna のベイズ最適化で探索する。

Usage:
    from src.models.tuning import run_tuning
    best_params = run_tuning(X_train, y_train, groups_train,
                             X_valid, y_valid, groups_valid, config)
"""



import json
import logging
from pathlib import Path


import lightgbm as lgb
import numpy as np
import optuna
import pandas as pd
from sklearn.metrics import roc_auc_score

logger = logging.getLogger(__name__)

# Optuna のログレベルを WARNING に設定（trial ごとの詳細出力を抑制）
optuna.logging.set_verbosity(optuna.logging.WARNING)

# デフォルトの探索範囲
DEFAULT_SEARCH_SPACE = {
    "num_leaves": {"type": "int", "low": 15, "high": 127},
    "learning_rate": {"type": "float", "low": 0.01, "high": 0.3, "log": True},
    "feature_fraction": {"type": "float", "low": 0.4, "high": 1.0},
    "bagging_fraction": {"type": "float", "low": 0.4, "high": 1.0},
    "bagging_freq": {"type": "int", "low": 1, "high": 10},
    "min_child_samples": {"type": "int", "low": 5, "high": 100},
    "reg_alpha": {"type": "float", "low": 1e-8, "high": 10.0, "log": True},
    "reg_lambda": {"type": "float", "low": 1e-8, "high": 10.0, "log": True},
}


def _suggest_param(trial: optuna.Trial, name: str, spec: dict):
    """探索範囲の仕様に基づいてパラメータをサンプリングする"""
    if spec["type"] == "int":
        return trial.suggest_int(name, spec["low"], spec["high"])
    elif spec["type"] == "float":
        return trial.suggest_float(
            name, spec["low"], spec["high"], log=spec.get("log", False)
        )
    else:
        raise ValueError(f"Unknown param type: {spec['type']}")


def create_objective(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    groups_train: list[int],
    X_valid: pd.DataFrame,
    y_valid: np.ndarray,
    groups_valid: list[int],
    base_params: dict,
    training_config: dict,
    search_space: dict,
    categorical_feature: list[str] | None = None,
):
    """
    Optuna の目的関数を生成する（クロージャ）

    Args:
        X_train: 学習用特徴量
        y_train: 学習用ラベル
        groups_train: 学習用グループサイズ
        X_valid: 検証用特徴量
        y_valid: 検証用ラベル
        groups_valid: 検証用グループサイズ
        base_params: LightGBM の固定パラメータ（objective, metric 等）
        training_config: 学習設定（num_boost_round, early_stopping_rounds 等）
        search_space: 探索範囲の辞書
        categorical_feature: カテゴリカル特徴量リスト

    Returns:
        objective 関数
    """
    train_data = lgb.Dataset(
        X_train,
        label=y_train,
        group=groups_train,
        categorical_feature=categorical_feature or "auto",
        free_raw_data=False,
    )
    valid_data = lgb.Dataset(
        X_valid,
        label=y_valid,
        group=groups_valid,
        categorical_feature=categorical_feature or "auto",
        reference=train_data,
        free_raw_data=False,
    )

    def objective(trial: optuna.Trial) -> float:
        # 固定パラメータ + 探索パラメータ
        params = dict(base_params)
        for param_name, spec in search_space.items():
            params[param_name] = _suggest_param(trial, param_name, spec)

        model = lgb.train(
            params,
            train_data,
            num_boost_round=training_config.get("num_boost_round", 1000),
            valid_sets=[valid_data],
            valid_names=["valid"],
            callbacks=[
                lgb.early_stopping(
                    training_config.get("early_stopping_rounds", 50),
                    verbose=False,
                ),
                lgb.log_evaluation(period=0),
            ],
        )

        # 検証データでAUCを計算
        y_pred = model.predict(X_valid)
        y_true_positions = y_valid
        binary_labels = np.where(
            (y_true_positions >= 1) & (y_true_positions <= 3), 1, 0
        )
        # ラベルが既に二値（0/1）の場合はそのまま使用
        if set(np.unique(y_true_positions)).issubset({0, 1}):
            binary_labels = y_true_positions.astype(int)

        if len(np.unique(binary_labels)) < 2:
            return 0.0

        auc = roc_auc_score(binary_labels, y_pred)
        return auc

    return objective


def run_tuning(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    groups_train: list[int],
    X_valid: pd.DataFrame,
    y_valid: np.ndarray,
    groups_valid: list[int],
    config: dict,
    categorical_feature: list[str] | None = None,
) -> dict:
    """
    Optuna でハイパーパラメータ調整を実行する

    Args:
        X_train: 学習用特徴量
        y_train: 学習用ラベル
        groups_train: 学習用グループサイズ
        X_valid: 検証用特徴量
        y_valid: 検証用ラベル
        groups_valid: 検証用グループサイズ
        config: 設定辞書（model, tuning セクションを含む）
        categorical_feature: カテゴリカル特徴量リスト

    Returns:
        最適パラメータの辞書（base_params にマージ済み）
    """
    model_config = config["model"]
    tuning_config = config.get("tuning", {})

    n_trials = tuning_config.get("n_trials", 100)
    timeout = tuning_config.get("timeout", 3600)
    study_name = tuning_config.get("study_name", "keiba_lgbm_lambdarank")
    storage = tuning_config.get("storage", None)
    search_space = tuning_config.get("search_space", DEFAULT_SEARCH_SPACE)

    # 固定パラメータ（探索対象外）
    base_params = {
        k: v for k, v in model_config["params"].items()
        if k not in search_space
    }

    objective = create_objective(
        X_train=X_train,
        y_train=y_train,
        groups_train=groups_train,
        X_valid=X_valid,
        y_valid=y_valid,
        groups_valid=groups_valid,
        base_params=base_params,
        training_config=model_config["training"],
        search_space=search_space,
        categorical_feature=categorical_feature,
    )

    study = optuna.create_study(
        study_name=study_name,
        storage=storage,
        direction="maximize",
        load_if_exists=True,
    )

    logger.info(
        f"Tuning started: n_trials={n_trials}, timeout={timeout}s, "
        f"search_space={list(search_space.keys())}"
    )

    study.optimize(objective, n_trials=n_trials, timeout=timeout)

    logger.info(
        f"Tuning completed: best_trial={study.best_trial.number}, "
        f"best_value={study.best_value:.4f}"
    )
    logger.info(f"Best params: {study.best_params}")

    # base_params に最適パラメータをマージ
    best_params = dict(base_params)
    best_params.update(study.best_params)

    return {
        "best_params": best_params,
        "best_value": study.best_value,
        "best_trial_number": study.best_trial.number,
        "n_trials": len(study.trials),
    }


def save_best_params(params: dict, path: str) -> None:
    """
    最適パラメータをJSONファイルに保存する

    Args:
        params: パラメータ辞書
        path: 保存先パス
    """
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(params, ensure_ascii=False, indent=2))
    logger.info(f"Best params saved to {file_path}")
