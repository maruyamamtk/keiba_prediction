"""
出走頭数 vs ジニ係数 散布図

2026-03-28〜29 のレースデータを対象に、
各レースの出走頭数とモデル予測スコアのジニ係数の関係を可視化する。

Usage:
    python3 scripts/plot_gini_vs_horses.py \
        --project-id keiba-prediction-1768734113 \
        --model-path ./src/models/lgbm_ranker_20260217.txt \
        [--output gini_vs_horses.png]
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

# プロジェクトルートを sys.path に追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.backtest.strategy import classify_race_pattern
from src.models.predict import predict_pipeline
from src.models.train import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TARGET_DATES = [
    datetime.date(2026, 3, 28),
    datetime.date(2026, 3, 29),
]

DATE_COLORS = {
    datetime.date(2026, 3, 28): "#4C72B0",
    datetime.date(2026, 3, 29): "#DD8452",
}
DATE_LABELS = {
    datetime.date(2026, 3, 28): "2026-03-28 (土)",
    datetime.date(2026, 3, 29): "2026-03-29 (日)",
}


def build_scatter_data(result_df: pd.DataFrame) -> pd.DataFrame:
    """
    predict_pipeline の出力から (race_id, n_horses, gini, race_date, pattern) を返す。
    """
    rows = []
    for race_id, grp in result_df.groupby("race_id"):
        probs = grp["win_place_prob"].tolist()
        n = len(probs)
        if n < 3:
            logger.warning(f"race_id={race_id}: {n}頭のためスキップ")
            continue
        rp = classify_race_pattern(probs, p1=0.3)
        race_date = pd.to_datetime(grp["race_date"].iloc[0]).date()
        rows.append(
            {
                "race_id": race_id,
                "n_horses": n,
                "gini": rp.gini_coefficient,
                "race_date": race_date,
                "pattern": rp.pattern,
            }
        )
    return pd.DataFrame(rows)


def plot_scatter(scatter_df: pd.DataFrame, output_path: str, p1: float = 0.3) -> None:
    """散布図を生成して保存する"""
    fig, ax = plt.subplots(figsize=(9, 6))

    # 日付ごとに色分けしてプロット
    for date in TARGET_DATES:
        sub = scatter_df[scatter_df["race_date"] == date]
        if sub.empty:
            continue
        ax.scatter(
            sub["n_horses"],
            sub["gini"],
            c=DATE_COLORS[date],
            label=DATE_LABELS[date],
            alpha=0.75,
            s=70,
            edgecolors="white",
            linewidths=0.5,
            zorder=3,
        )

    # p1 の境界線（突出型 / 標準型）
    ax.axhline(
        y=p1,
        color="crimson",
        linestyle="--",
        linewidth=1.2,
        alpha=0.8,
        label=f"p1 閾値 (Gini = {p1})",
        zorder=2,
    )

    # 領域ラベル
    x_max = scatter_df["n_horses"].max() + 1 if len(scatter_df) > 0 else 20
    ax.text(
        x_max, p1 + 0.01, "突出型 (one_dominant)",
        ha="right", va="bottom", color="crimson", fontsize=9,
    )
    ax.text(
        x_max, p1 - 0.01, "標準型 (standard)",
        ha="right", va="top", color="steelblue", fontsize=9,
    )

    # 統計サマリー（テキストボックス）
    total = len(scatter_df)
    dominant = (scatter_df["pattern"] == "one_dominant").sum()
    summary = (
        f"総レース数: {total}\n"
        f"突出型: {dominant} ({dominant / total * 100:.0f}%)" if total > 0
        else "データなし"
    )
    ax.text(
        0.02, 0.97, summary,
        transform=ax.transAxes,
        va="top", ha="left",
        fontsize=9,
        bbox=dict(boxstyle="round,pad=0.4", facecolor="lightyellow", alpha=0.8),
    )

    ax.set_xlabel("出走頭数", fontsize=12)
    ax.set_ylabel("ジニ係数 (複勝率分布)", fontsize=12)
    ax.set_title(
        "出走頭数 vs ジニ係数\n(2026-03-28〜29, モデル: LightGBM LambdaRank)",
        fontsize=13,
    )
    ax.legend(fontsize=10, loc="upper right")
    ax.set_xlim(left=max(0, scatter_df["n_horses"].min() - 1) if len(scatter_df) > 0 else 0)
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.2f}"))
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    logger.info(f"散布図を保存しました: {output_path}")
    plt.close(fig)


def main():
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="出走頭数 vs ジニ係数 散布図生成")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--output", default="results/gini_vs_horses.png")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    project_id = args.project_id
    if not project_id:
        logger.error("--project-id を指定してください")
        return 1

    config = load_config(args.config)

    logger.info(f"対象日: {TARGET_DATES}")
    result_df = predict_pipeline(
        project_id=project_id,
        execution_date=TARGET_DATES[-1],
        config=config,
        model_path=args.model_path,
        target_dates=TARGET_DATES,
    )

    if result_df.empty:
        logger.error("予測結果が空です")
        return 1

    logger.info(f"予測結果: {result_df['race_id'].nunique()} レース, {len(result_df)} 頭")

    scatter_df = build_scatter_data(result_df)
    logger.info(f"散布図データ: {len(scatter_df)} レース")

    # 簡易サマリーをログ出力
    for date in TARGET_DATES:
        sub = scatter_df[scatter_df["race_date"] == date]
        if sub.empty:
            continue
        dominant = (sub["pattern"] == "one_dominant").sum()
        logger.info(
            f"{date}: {len(sub)} レース, 突出型 {dominant} / 標準型 {len(sub)-dominant}"
            f", Gini mean={sub['gini'].mean():.3f} std={sub['gini'].std():.3f}"
        )

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    plot_scatter(scatter_df, args.output)
    print(f"\n散布図: {os.path.abspath(args.output)}")
    return 0


if __name__ == "__main__":
    exit(main())
