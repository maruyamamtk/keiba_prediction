"""
アンサンブルモジュール

多値ランク学習モデル（pred_score_multi）と着差回帰モデル（pred_score_regression）の
スコアをレース内Zスコア正規化してから加重平均し、最終ソートスコアを算出する。
"""

import numpy as np
import pandas as pd


def ensemble_rank_scores(
    df: pd.DataFrame,
    score_col_multi: str = "pred_score_multi",
    score_col_regression: str = "pred_score_regression",
    weight_multi: float = 0.7,
) -> pd.Series:
    """
    2つのランクスコアをレース内標準化してから加重平均する。

    Args:
        df: race_id, {score_col_multi}, {score_col_regression} を含む DataFrame
        score_col_multi: 多値ランク学習モデルのスコアカラム名
        score_col_regression: 着差回帰モデルのスコアカラム名（高いほど強い馬）
        weight_multi: 多値ランク学習スコアの重み（0〜1）。回帰スコアの重みは 1 - weight_multi
    Returns:
        final_score Series（高いほど上位予測）
    """
    if not 0.0 <= weight_multi <= 1.0:
        raise ValueError(f"weight_multi must be in [0, 1], got {weight_multi}")
    weight_regression = 1.0 - weight_multi

    def _zscore_within_race(series: pd.Series) -> pd.Series:
        valid = series.dropna()
        if len(valid) < 2:
            return series.where(series.isna(), 0.0)
        std = valid.std()
        if std < 1e-10:
            return series.where(series.isna(), 0.0)
        mean = valid.mean()
        return (series - mean) / std

    if "race_id" not in df.columns:
        raise ValueError("df must contain a 'race_id' column for within-race normalization")

    has_multi = score_col_multi in df.columns
    has_regression = score_col_regression in df.columns

    if has_multi:
        z_multi = df.groupby("race_id")[score_col_multi].transform(_zscore_within_race)
    else:
        z_multi = pd.Series(np.nan, index=df.index)

    if has_regression:
        z_regression = df.groupby("race_id")[score_col_regression].transform(_zscore_within_race)
    else:
        z_regression = pd.Series(np.nan, index=df.index)

    multi_nan = z_multi.isna()
    regression_nan = z_regression.isna()

    final_score = pd.Series(np.nan, index=df.index, dtype=float)

    both_available = ~multi_nan & ~regression_nan
    final_score[both_available] = (
        weight_multi * z_multi[both_available] + weight_regression * z_regression[both_available]
    )

    only_multi = ~multi_nan & regression_nan
    final_score[only_multi] = z_multi[only_multi]

    only_regression = multi_nan & ~regression_nan
    final_score[only_regression] = z_regression[only_regression]

    return final_score
