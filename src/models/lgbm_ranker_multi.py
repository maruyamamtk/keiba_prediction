"""
LightGBM LambdaRank 多値ラベルモデル

JRA賞金ウェイト（1着=120, 2着=90, ..., 10着=2, 11着以下=0）を
そのまま整数ラベルとして使うLambdaRankモデル。
label_gain[i]=i とすることでラベル値がそのままNDCGゲインになる。
"""

import json
from dataclasses import dataclass, field
from pathlib import Path

from src.models.lgbm_ranker import LGBMRanker, LGBMRankerConfig

# JRA賞金ウェイト（着順→ラベル値）。11着以下は辞書未登録 → 0 として扱う
JRA_PRIZE_WEIGHTS: dict[int, int] = {
    1: 120,
    2: 90,
    3: 70,
    4: 15,
    5: 10,
    6: 8,
    7: 7,
    8: 6,
    9: 4,
    10: 2,
}

# label_gain[i] = i とすることでラベル値がそのままNDCGゲインになる
# ラベル最大値=120 なので 0〜120 の121要素を用意する
LABEL_GAIN_MULTI: list[float] = list(range(121))


@dataclass
class LGBMRankerMultiConfig(LGBMRankerConfig):
    """JRA賞金ウェイト多値ラベル用 LambdaRank 設定"""

    params: dict = field(default_factory=lambda: {
        "objective": "lambdarank",
        "metric": "ndcg",
        "ndcg_eval_at": [3],
        "label_gain": LABEL_GAIN_MULTI,
        "boosting_type": "gbdt",
        "num_leaves": 31,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
        "seed": 42,
    })


class LGBMRankerMulti(LGBMRanker):
    """JRA賞金ウェイト多値ラベルを使った LambdaRank モデル

    ラベル: 着順1=120, 着順2=90, ..., 着順10=2, 11着以下=0
    label_gain[i]=i により、各ラベル値がそのままNDCGゲインとして使われる。
    """

    def __init__(self, config: LGBMRankerMultiConfig | None = None):
        super().__init__(config or LGBMRankerMultiConfig())

    def save(self, path: str, training_period: dict | None = None) -> None:
        """モデルをローカルに保存する（meta.jsonにmodel_typeを追記）"""
        super().save(path, training_period=training_period)

        meta_path = Path(path).with_suffix(".meta.json")
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            meta["model_type"] = "ranker_multi"
            meta["jra_prize_weights"] = JRA_PRIZE_WEIGHTS
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2))
