"""
Optuna によるハイパーパラメータ調整モジュール

LightGBM 各モデルのハイパーパラメータを Optuna のベイズ最適化で探索する。
model_type 引数で対象モデルを切り替える。

  model_type="ranker"       : LGBMRanker（lambdarank、グループあり）
  model_type="ranker_multi" : LGBMRankerMulti（lambdarank、グループあり）
  model_type="regression"   : LGBMRegression（regression、グループなし、-RMSE最大化）
  model_type="classifier"   : LGBMClassifier（binary、グループなし、AUC最大化）
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

optuna.logging.set_verbosity(optuna.logging.WARNING)

# 共通探索範囲（全モデル型で使用）
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

_RANKER_TYPES = {"ranker", "ranker_multi"}


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
    X_valid: pd.DataFrame,
    y_valid: np.ndarray,
    base_params: dict,
    training_config: dict,
    search_space: dict,
    model_type: str = "ranker",
    groups_train: list[int] | None = None,
    groups_valid: list[int] | None = None,
    categorical_feature: list[str] | None = None,
):
    """
    Optuna の目的関数を生成する（クロージャ）

    Args:
        X_train: 学習用特徴量
        y_train: 学習用ラベル
        X_valid: 検証用特徴量
        y_valid: 検証用ラベル
        base_params: LightGBM の固定パラメータ
        training_config: 学習設定（num_boost_round, early_stopping_rounds）
        search_space: Optuna 探索範囲の辞書
        model_type: "ranker" | "ranker_multi" | "regression" | "classifier"
        groups_train: 学習用グループサイズ（ranker 系のみ必須）
        groups_valid: 検証用グループサイズ（ranker 系のみ必須）
        categorical_feature: カテゴリカル特徴量リスト

    Returns:
        objective 関数
    """
    if model_type in _RANKER_TYPES and (groups_train is None or groups_valid is None):
        raise ValueError(f"model_type='{model_type}' には groups_train / groups_valid が必要です")

    cat = categorical_feature or "auto"

    if model_type in _RANKER_TYPES:
        train_data = lgb.Dataset(
            X_train, label=y_train, group=groups_train,
            categorical_feature=cat, free_raw_data=False,
        )
        valid_data = lgb.Dataset(
            X_valid, label=y_valid, group=groups_valid,
            categorical_feature=cat, reference=train_data, free_raw_data=False,
        )
    else:
        train_data = lgb.Dataset(
            X_train, label=y_train,
            categorical_feature=cat, free_raw_data=False,
        )
        valid_data = lgb.Dataset(
            X_valid, label=y_valid,
            categorical_feature=cat, reference=train_data, free_raw_data=False,
        )

    def objective(trial: optuna.Trial) -> float:
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

        if model_type == "regression":
            y_pred = model.predict(X_valid)
            rmse = float(np.sqrt(np.mean((y_valid - y_pred) ** 2)))
            return -rmse  # 最大化 = RMSE 最小化

        # ranker / ranker_multi / classifier: AUC で評価
        y_pred = model.predict(X_valid)

        if model_type == "classifier":
            binary_labels = y_valid.astype(int)
        else:
            # ranker 系: y_valid は二値ラベル（3着以内=1）
            binary_labels = y_valid.astype(int)

        if len(np.unique(binary_labels)) < 2:
            return 0.0

        return roc_auc_score(binary_labels, y_pred)

    return objective


def run_tuning(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    X_valid: pd.DataFrame,
    y_valid: np.ndarray,
    config: dict,
    model_type: str = "ranker",
    groups_train: list[int] | None = None,
    groups_valid: list[int] | None = None,
    categorical_feature: list[str] | None = None,
) -> dict:
    """
    Optuna でハイパーパラメータ調整を実行する

    Args:
        X_train: 学習用特徴量
        y_train: 学習用ラベル
        X_valid: 検証用特徴量
        y_valid: 検証用ラベル
        config: 設定辞書（model / tuning セクションを含む）
        model_type: "ranker" | "ranker_multi" | "regression" | "classifier"
        groups_train: 学習用グループサイズ（ranker 系のみ）
        groups_valid: 検証用グループサイズ（ranker 系のみ）
        categorical_feature: カテゴリカル特徴量リスト

    Returns:
        {"best_params", "best_value", "best_trial_number", "n_trials"}
    """
    model_config = config["model"]

    # 汎用チューニング設定 + モデル種別オーバーライド
    generic_tuning = config.get("tuning", {})
    model_type_tuning = generic_tuning.get(model_type, {})
    tuning_config = {**generic_tuning, **model_type_tuning}

    n_trials = tuning_config.get("n_trials", 100)
    timeout = tuning_config.get("timeout", 3600)
    study_name = tuning_config.get("study_name", f"keiba_lgbm_{model_type}")
    storage = tuning_config.get("storage", None)
    search_space = tuning_config.get("search_space", DEFAULT_SEARCH_SPACE)

    # base_params: 探索対象外の固定パラメータ
    base_params = {
        k: v for k, v in model_config["params"].items()
        if k not in search_space
    }

    objective = create_objective(
        X_train=X_train,
        y_train=y_train,
        X_valid=X_valid,
        y_valid=y_valid,
        base_params=base_params,
        training_config=model_config["training"],
        search_space=search_space,
        model_type=model_type,
        groups_train=groups_train,
        groups_valid=groups_valid,
        categorical_feature=categorical_feature,
    )

    study = optuna.create_study(
        study_name=study_name,
        storage=storage,
        direction="maximize",
        load_if_exists=True,
    )

    logger.info(
        f"Tuning started: model_type={model_type}, n_trials={n_trials}, "
        f"timeout={timeout}s, search_space={list(search_space.keys())}"
    )

    study.optimize(objective, n_trials=n_trials, timeout=timeout)

    logger.info(
        f"Tuning completed: best_trial={study.best_trial.number}, "
        f"best_value={study.best_value:.4f}"
    )
    logger.info(f"Best params: {study.best_params}")

    best_params = dict(base_params)
    best_params.update(study.best_params)

    return {
        "best_params": best_params,
        "best_value": study.best_value,
        "best_trial_number": study.best_trial.number,
        "n_trials": len(study.trials),
    }


def save_best_params(params: dict, path: str) -> None:
    """最適パラメータを JSON ファイルに保存する"""
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(params, ensure_ascii=False, indent=2))
    logger.info(f"Best params saved to {file_path}")
