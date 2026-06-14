"""
LightGBM モデル共通基底クラス

LGBMRanker / LGBMRegression / LGBMClassifier に重複していた以下のロジックを集約する。
- _prepare_prediction_data()
- _parse_categorical_feature_names()
- _build_categorical_dtypes()
- feature_importance()
"""

import logging
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class LGBMModelBase:
    """LightGBM モデルの共通ロジックを提供する基底クラス。

    サブクラスは train() / predict() / save() / load() を独自実装し、
    カテゴリカル特徴量処理と特徴量重要度取得はこのクラスから継承する。
    """

    def __init__(self) -> None:
        self.model: lgb.Booster | None = None
        self.feature_names: list[str] | None = None
        self._categorical_feature_names: list[str] = []
        self._categorical_dtypes: dict[str, pd.CategoricalDtype] = {}

    def _prepare_prediction_data(self, X: pd.DataFrame) -> pd.DataFrame:
        """予測用にDataFrameをモデルの期待に合わせて整形する。

        1. モデルの全特徴量を揃える（不足列はNaNで補完）
        2. カテゴリカル特徴量を学習時のカテゴリ情報で category 型に変換

        Args:
            X: 特徴量DataFrame

        Returns:
            モデル入力用に整形されたDataFrame
        """
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

    def _parse_categorical_feature_names(self, model_path: Path) -> list[str]:
        """モデルファイルの [categorical_feature: N,M,...] 行からカテゴリカル特徴量名を返す。"""
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
        """_categorical_feature_names と pandas_categorical から dtype キャッシュを構築する。"""
        model_cats = getattr(self.model, "pandas_categorical", [])
        self._categorical_dtypes = {
            col: pd.CategoricalDtype(categories=cats)
            for col, cats in zip(self._categorical_feature_names, model_cats)
        }

    def feature_importance(self, importance_type: str = "gain") -> pd.DataFrame:
        """特徴量重要度を取得する。

        Args:
            importance_type: "gain" or "split"

        Returns:
            特徴量重要度のDataFrame（importance 降順）
        """
        if self.model is None:
            raise RuntimeError("モデルが学習されていません。")

        importance = self.model.feature_importance(importance_type=importance_type)
        names = self.feature_names or self.model.feature_name()

        df = pd.DataFrame({"feature": names, "importance": importance})
        return df.sort_values("importance", ascending=False).reset_index(drop=True)
