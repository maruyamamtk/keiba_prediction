"""
LightGBM モデル共通基底クラスおよびLambdaRankモデル
"""

import json
import logging
from dataclasses import dataclass, field
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
        # win_place_prob キャリブレーション温度（Issue #414）。
        # None の場合、推論時は 1.0（未校正）にフォールバックする。
        self.calibration_temperature: float | None = None
        # win_place_prob アイソトニック校正器（Issue #416・本番の既定手法）。
        # {"method", "x_thresholds", "y_thresholds"} の dict。存在する場合は温度より優先。
        self.calibration_isotonic: dict | None = None

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
        if len(self._categorical_feature_names) != len(model_cats):
            logger.warning(
                f"カテゴリカル特徴量数が不一致: "
                f"names={len(self._categorical_feature_names)}, "
                f"pandas_categorical={len(model_cats)}"
            )
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


@dataclass
class LGBMRankerConfig:
    """LightGBM LambdaRankの設定"""

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

    num_boost_round: int = 1000
    early_stopping_rounds: int = 50
    log_evaluation: int = 100


class LGBMRanker(LGBMModelBase):
    """LightGBM LambdaRankによるランク学習モデル"""

    def __init__(self, config: LGBMRankerConfig | None = None):
        super().__init__()
        self.config = config or LGBMRankerConfig()

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: np.ndarray,
        groups_train: list[int],
        X_valid: pd.DataFrame,
        y_valid: np.ndarray,
        groups_valid: list[int],
        categorical_feature: list[str] | None = None,
    ) -> lgb.Booster:
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

        if categorical_feature:
            self._categorical_feature_names = list(categorical_feature)
            self._build_categorical_dtypes()

        logger.info(
            f"Training completed: best_iteration={self.model.best_iteration}"
        )

        return self.model

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("モデルが学習されていません。train()またはload()を先に実行してください。")

        X_pred = self._prepare_prediction_data(X)
        return self.model.predict(X_pred)

    def save(self, path: str, training_period: dict | None = None) -> None:
        if self.model is None:
            raise RuntimeError("保存するモデルがありません。")

        model_path = Path(path)
        model_path.parent.mkdir(parents=True, exist_ok=True)

        self.model.save_model(str(model_path))

        meta_path = model_path.with_suffix(".meta.json")
        meta = {
            "feature_names": self.feature_names,
            "best_iteration": self.model.best_iteration,
            "params": self.config.params,
        }
        if training_period:
            meta["training_period"] = training_period
        if self.calibration_temperature is not None:
            meta["calibration_temperature"] = self.calibration_temperature
        if self.calibration_isotonic is not None:
            meta["calibration_isotonic"] = self.calibration_isotonic
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2))

        logger.info(f"Model saved to {model_path}")

    def load(self, path: str) -> None:
        model_path = Path(path)
        if not model_path.exists():
            raise FileNotFoundError(f"モデルファイルが見つかりません: {model_path}")

        self.model = lgb.Booster(model_file=str(model_path))

        meta_path = model_path.with_suffix(".meta.json")
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            self.feature_names = meta.get("feature_names")
            self.calibration_temperature = meta.get("calibration_temperature")
            self.calibration_isotonic = meta.get("calibration_isotonic")
            logger.info(
                f"Model loaded from {model_path} "
                f"(best_iteration={meta.get('best_iteration')}, "
                f"calibration_temperature={self.calibration_temperature}, "
                f"calibration_isotonic={'有' if self.calibration_isotonic else '無'})"
            )
        else:
            self.feature_names = self.model.feature_name()
            logger.info(f"Model loaded from {model_path} (no metadata)")

        self._categorical_feature_names = self._parse_categorical_feature_names(model_path)
        self._build_categorical_dtypes()
