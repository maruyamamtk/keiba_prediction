"""
LightGBM LambdaRank モデル

競馬レース内の着順予測を行うランク学習モデル。
レースIDをグループ単位として、各馬の相対的な順位を予測する。
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import lightgbm as lgb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class LGBMRankerConfig:
    """LightGBM LambdaRankの設定"""

    # LightGBMパラメータ
    params: dict = field(default_factory=lambda: {
        "objective": "lambdarank",
        "metric": "ndcg",
        "ndcg_eval_at": [3],
        "boosting_type": "gbdt",
        "num_leaves": 31,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
        "seed": 42,
    })

    # 学習設定
    num_boost_round: int = 1000
    early_stopping_rounds: int = 50
    log_evaluation: int = 100


class LGBMRanker:
    """LightGBM LambdaRankによるランク学習モデル"""

    def __init__(self, config: Optional[LGBMRankerConfig] = None):
        self.config = config or LGBMRankerConfig()
        self.model: Optional[lgb.Booster] = None
        self.feature_names: Optional[list[str]] = None

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: np.ndarray,
        groups_train: list[int],
        X_valid: pd.DataFrame,
        y_valid: np.ndarray,
        groups_valid: list[int],
        categorical_feature: Optional[list[str]] = None,
    ) -> lgb.Booster:
        """
        モデルを学習する

        Args:
            X_train: 学習用特徴量
            y_train: 学習用ラベル（着順の逆数等、高いほど良い値）
            groups_train: 学習用グループサイズ（各レースの馬数）
            X_valid: 検証用特徴量
            y_valid: 検証用ラベル
            groups_valid: 検証用グループサイズ
            categorical_feature: カテゴリカル特徴量名リスト

        Returns:
            学習済みBooster
        """
        self.feature_names = list(X_train.columns)

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

        logger.info(
            f"Training started: {X_train.shape[0]} rows, "
            f"{X_train.shape[1]} features, "
            f"{len(groups_train)} races (train), "
            f"{len(groups_valid)} races (valid)"
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

        logger.info(
            f"Training completed: best_iteration={self.model.best_iteration}"
        )

        return self.model

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        予測スコアを出力する

        Args:
            X: 特徴量DataFrame

        Returns:
            予測スコア（高いほど上位と予測）
        """
        if self.model is None:
            raise RuntimeError("モデルが学習されていません。train()またはload()を先に実行してください。")

        X_pred = self._prepare_prediction_data(X)
        return self.model.predict(X_pred)

    def _prepare_prediction_data(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        予測用にDataFrameをモデルの期待に合わせて整形する

        1. モデルの特徴量名でフィルタ（学習後に追加されたカラムを除外）
        2. object型カラムをcategory型に変換（LightGBMのpandas_categoricalと整合）

        Args:
            X: 特徴量DataFrame

        Returns:
            モデル入力用に整形されたDataFrame
        """
        model_feature_names = self.feature_names or self.model.feature_name()

        # モデルの特徴量名でフィルタ
        available = [c for c in model_feature_names if c in X.columns]
        missing = [c for c in model_feature_names if c not in X.columns]
        if missing:
            logger.warning(f"モデルの特徴量がデータに不足: {missing}")

        X_pred = X[available].copy()

        # object型（文字列）カラムをcategory型に変換
        # LightGBMはpandas_categoricalの数とcategory型カラム数が
        # 一致しないとエラーになるため、全object型をcategory型に変換する
        for col in X_pred.select_dtypes(include="object").columns:
            X_pred[col] = X_pred[col].astype("category")

        return X_pred

    def save(self, path: str) -> None:
        """
        モデルをローカルに保存する

        Args:
            path: 保存先パス（.txtファイル）
        """
        if self.model is None:
            raise RuntimeError("保存するモデルがありません。")

        model_path = Path(path)
        model_path.parent.mkdir(parents=True, exist_ok=True)

        self.model.save_model(str(model_path))

        # 特徴量名を別ファイルに保存
        meta_path = model_path.with_suffix(".meta.json")
        meta = {
            "feature_names": self.feature_names,
            "best_iteration": self.model.best_iteration,
            "params": self.config.params,
        }
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2))

        logger.info(f"Model saved to {model_path}")

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

        # メタデータを読み込み
        meta_path = model_path.with_suffix(".meta.json")
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            self.feature_names = meta.get("feature_names")
            logger.info(
                f"Model loaded from {model_path} "
                f"(best_iteration={meta.get('best_iteration')})"
            )
        else:
            self.feature_names = self.model.feature_name()
            logger.info(f"Model loaded from {model_path} (no metadata)")

    def feature_importance(self, importance_type: str = "gain") -> pd.DataFrame:
        """
        特徴量重要度を取得する

        Args:
            importance_type: "gain" or "split"

        Returns:
            特徴量重要度のDataFrame
        """
        if self.model is None:
            raise RuntimeError("モデルが学習されていません。")

        importance = self.model.feature_importance(importance_type=importance_type)
        names = self.feature_names or self.model.feature_name()

        df = pd.DataFrame({
            "feature": names,
            "importance": importance,
        })
        return df.sort_values("importance", ascending=False).reset_index(drop=True)
