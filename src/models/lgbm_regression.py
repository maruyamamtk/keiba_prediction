"""
LightGBM 回帰モデル（着差Zスコア予測）

1着馬との走破タイム差をレース内σで正規化したZスコアを目的変数とする。
値が高い（0に近い）ほど強い馬と判断される。

グループ（レースID）を必要としない通常の回帰モデル。
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from src.models.lgbm_base import LGBMModelBase

logger = logging.getLogger(__name__)


@dataclass
class LGBMRegressionConfig:
    """LightGBM 回帰モデルの設定"""

    params: dict = field(default_factory=lambda: {
        "objective": "regression",
        "metric": "rmse",
        "boosting_type": "gbdt",
        "num_leaves": 31,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
        "seed": 42,
    })

    num_boost_round: int = 1000
    early_stopping_rounds: int = 50
    log_evaluation: int = 100


class LGBMRegression(LGBMModelBase):
    """LightGBM 回帰モデル（着差Zスコア予測）

    LGBMRanker と同等のインターフェースを提供するが、
    グループ（レースID）を必要としない通常の教師あり回帰。
    """

    def __init__(self, config: LGBMRegressionConfig | None = None):
        super().__init__()
        self.config = config or LGBMRegressionConfig()

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: np.ndarray,
        X_valid: pd.DataFrame,
        y_valid: np.ndarray,
        categorical_feature: list[str] | None = None,
    ) -> lgb.Booster:
        """
        モデルを学習する

        Args:
            X_train: 学習用特徴量
            y_train: 学習用ラベル（着差Zスコア、高いほど強い）
            X_valid: 検証用特徴量
            y_valid: 検証用ラベル
            categorical_feature: カテゴリカル特徴量名リスト

        Returns:
            学習済みBooster
        """
        self.feature_names = list(X_train.columns)

        train_data = lgb.Dataset(
            X_train,
            label=y_train,
            categorical_feature=categorical_feature or "auto",
            free_raw_data=False,
        )
        valid_data = lgb.Dataset(
            X_valid,
            label=y_valid,
            categorical_feature=categorical_feature or "auto",
            reference=train_data,
            free_raw_data=False,
        )

        logger.info(
            f"Regression training started: {X_train.shape[0]} rows, "
            f"{X_train.shape[1]} features"
        )

        self.model = lgb.train(
            self.config.params,
            train_data,
            num_boost_round=self.config.num_boost_round,
            valid_sets=[train_data, valid_data],
            valid_names=["train", "valid"],
            callbacks=[
                lgb.early_stopping(self.config.early_stopping_rounds),
                lgb.log_evaluation(self.config.log_evaluation),
            ],
        )

        if categorical_feature:
            self._categorical_feature_names = list(categorical_feature)
            self._build_categorical_dtypes()

        logger.info(f"Regression training completed: best_iteration={self.model.best_iteration}")
        return self.model

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        予測スコアを出力する

        Args:
            X: 特徴量DataFrame

        Returns:
            予測スコア（高いほど強い）
        """
        if self.model is None:
            raise RuntimeError("モデルが学習されていません。train()またはload()を先に実行してください。")

        X_pred = self._prepare_prediction_data(X)
        return self.model.predict(X_pred)

    def save(self, path: str, training_period: dict | None = None) -> None:
        """
        モデルをローカルに保存する

        Args:
            path: 保存先パス（.txtファイル）
            training_period: 学習・検証期間の辞書
        """
        if self.model is None:
            raise RuntimeError("保存するモデルがありません。")

        model_path = Path(path)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        self.model.save_model(str(model_path))

        meta_path = model_path.with_suffix(".meta.json")
        meta = {
            "model_type": "regression",
            "feature_names": self.feature_names,
            "best_iteration": self.model.best_iteration,
            "params": self.config.params,
        }
        if training_period:
            meta["training_period"] = training_period
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2))

        logger.info(f"Regression model saved to {model_path}")

    def load(self, path: str) -> None:
        """
        ローカルからモデルを読み込む

        Args:
            path: モデルファイルパス（.txtファイル）
        """
        model_path = Path(path)
        if not model_path.exists():
            raise FileNotFoundError(f"モデルファイルが見つかりません: {model_path}")

        self.model = lgb.Booster(model_file=str(model_path))

        meta_path = model_path.with_suffix(".meta.json")
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            self.feature_names = meta.get("feature_names")
            logger.info(
                f"Regression model loaded from {model_path} "
                f"(best_iteration={meta.get('best_iteration')})"
            )
        else:
            self.feature_names = self.model.feature_name()
            logger.info(f"Regression model loaded from {model_path} (no metadata)")

        self._categorical_feature_names = self._parse_categorical_feature_names(model_path)
        self._build_categorical_dtypes()
