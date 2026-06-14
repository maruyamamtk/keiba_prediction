"""
LightGBM 二値分類モデル（複勝率直接予測）

objective=binary による通常の二値分類モデル。
predict() が [0,1] の確率を直接返すため、期待値計算のキャリブレーション入力として使用する。
LGBMRanker（lambdarank）はランク学習として並存する。
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class LGBMClassifierConfig:
    """LightGBM 二値分類モデルの設定"""

    params: dict = field(default_factory=lambda: {
        "objective": "binary",
        "metric": "auc",
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


class LGBMClassifier:
    """LightGBM binary classifier（複勝率直接予測用）

    LGBMRanker と同等のインターフェースを提供するが、
    グループ（レースID）を必要とせず、predict() が [0,1] の確率を返す。
    """

    def __init__(self, config: LGBMClassifierConfig | None = None):
        self.config = config or LGBMClassifierConfig()
        self.model: lgb.Booster | None = None
        self.feature_names: list[str] | None = None
        self._categorical_feature_names: list[str] = []
        self._categorical_dtypes: dict[str, pd.CategoricalDtype] = {}

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
            y_train: 学習用ラベル（3着以内=1, それ以外=0）
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
            f"Classifier training started: {X_train.shape[0]} rows, "
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

        logger.info(f"Classifier training completed: best_iteration={self.model.best_iteration}")
        return self.model

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        複勝確率を出力する

        Args:
            X: 特徴量DataFrame

        Returns:
            複勝確率（0〜1 の範囲）
        """
        if self.model is None:
            raise RuntimeError("モデルが学習されていません。train()またはload()を先に実行してください。")

        X_pred = self._prepare_prediction_data(X)
        return self.model.predict(X_pred)

    def _prepare_prediction_data(self, X: pd.DataFrame) -> pd.DataFrame:
        """予測用にDataFrameをモデルの期待に合わせて整形する"""
        model_feature_names = self.feature_names or self.model.feature_name()

        missing = [c for c in model_feature_names if c not in X.columns]
        if missing:
            logger.warning(f"モデルの特徴量がデータに不足: {missing}")

        X_pred = X.reindex(columns=model_feature_names)

        for col, dtype in self._categorical_dtypes.items():
            if col in X_pred.columns:
                X_pred[col] = X_pred[col].astype(dtype)

        for col in X_pred.select_dtypes(include="object").columns:
            if not isinstance(X_pred[col].dtype, pd.CategoricalDtype):
                X_pred[col] = X_pred[col].astype("category")

        return X_pred

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
            "model_type": "classifier",
            "feature_names": self.feature_names,
            "best_iteration": self.model.best_iteration,
            "params": self.config.params,
        }
        if training_period:
            meta["training_period"] = training_period
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2))

        logger.info(f"Classifier model saved to {model_path}")

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
                f"Classifier model loaded from {model_path} "
                f"(best_iteration={meta.get('best_iteration')})"
            )
        else:
            self.feature_names = self.model.feature_name()
            logger.info(f"Classifier model loaded from {model_path} (no metadata)")

        self._categorical_feature_names = self._parse_categorical_feature_names(model_path)
        self._build_categorical_dtypes()

    def _parse_categorical_feature_names(self, model_path: Path) -> list[str]:
        """モデルファイルのcategorical_feature行からカテゴリカル特徴量名を返す。"""
        feature_names = self.feature_names or self.model.feature_name()
        with open(model_path) as f:
            for raw_line in f:
                stripped = raw_line.strip()
                if stripped.startswith("[categorical_feature:"):
                    content = stripped.split(":", 1)[1].strip().rstrip("]").strip()
                    if not content or content == "none":
                        return []
                    indices = [int(x.strip()) for x in content.split(",")]
                    return [feature_names[i] for i in indices]
                if stripped.startswith("[Tree"):
                    break
        return []

    def _build_categorical_dtypes(self) -> None:
        """カテゴリカル特徴量のdtypeキャッシュを構築する。"""
        model_cats = getattr(self.model, "pandas_categorical", [])
        self._categorical_dtypes = {
            col: pd.CategoricalDtype(categories=cats)
            for col, cats in zip(self._categorical_feature_names, model_cats)
        }

    def feature_importance(self, importance_type: str = "gain") -> pd.DataFrame:
        """特徴量重要度を取得する"""
        if self.model is None:
            raise RuntimeError("モデルが学習されていません。")

        importance = self.model.feature_importance(importance_type=importance_type)
        names = self.feature_names or self.model.feature_name()

        df = pd.DataFrame({"feature": names, "importance": importance})
        return df.sort_values("importance", ascending=False).reset_index(drop=True)
