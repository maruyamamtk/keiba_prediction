"""
バックテスト評価指標

バックテストシミュレーション結果（賭け記録 DataFrame）から
各種パフォーマンス指標を計算する。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def recovery_rate(history_df: pd.DataFrame) -> float:
    """
    回収率 (%) = 合計払戻 / 合計賭け金 × 100

    Args:
        history_df: 賭け記録 DataFrame (bet_amount, return_amount カラムを持つ)

    Returns:
        回収率 (%)
    """
    total_bet = float(history_df["bet_amount"].sum())
    if total_bet == 0:
        return 0.0
    total_return = float(history_df["return_amount"].sum())
    return total_return / total_bet * 100.0


def hit_rate(history_df: pd.DataFrame) -> float:
    """
    的中率 (%) = 的中数 / 総賭け数 × 100

    Args:
        history_df: 賭け記録 DataFrame (is_hit カラムを持つ)

    Returns:
        的中率 (%)
    """
    n_bets = len(history_df)
    if n_bets == 0:
        return 0.0
    n_hits = int(history_df["is_hit"].sum())
    return n_hits / n_bets * 100.0


def max_drawdown(history_df: pd.DataFrame, initial_capital: float) -> float:
    """
    最大ドローダウン (%) = ピークからの最大下落幅 / ピーク資金 × 100

    資金曲線のピーク時点からの最大の下落割合を計算する。

    Args:
        history_df: 賭け記録 DataFrame (capital_after カラムを持つ)
        initial_capital: 初期資金

    Returns:
        最大ドローダウン (%)
    """
    if len(history_df) == 0:
        return 0.0

    capitals = np.concatenate(
        [[initial_capital], history_df["capital_after"].values]
    )
    peaks = np.maximum.accumulate(capitals)
    drawdowns = (peaks - capitals) / peaks * 100.0
    return float(drawdowns.max())


def sharpe_ratio(
    history_df: pd.DataFrame,
    risk_free_rate: float = 0.0,
    periods_per_year: int = 52,
) -> float:
    """
    シャープレシオ（週次リターンベース）

    = (平均週次リターン - リスクフリーレート) / 週次リターンの標準偏差 × √(年間週数)

    Args:
        history_df: 賭け記録 DataFrame (race_date, profit カラムを持つ)
        risk_free_rate: 年次リスクフリーレート (デフォルト: 0.0)
        periods_per_year: 年間の取引期間数 (デフォルト: 52 週)

    Returns:
        シャープレシオ
    """
    if len(history_df) == 0:
        return 0.0

    # 週次の利益を集計（競馬は週末開催）
    df = history_df.copy()
    df["race_date"] = pd.to_datetime(df["race_date"])
    df["week"] = df["race_date"].dt.to_period("W")
    weekly_pnl = df.groupby("week")["profit"].sum()

    if len(weekly_pnl) < 2:
        return 0.0

    weekly_rf = risk_free_rate / periods_per_year
    excess_returns = weekly_pnl - weekly_rf
    std = excess_returns.std()
    if std == 0 or np.isnan(std):
        return 0.0

    return float(excess_returns.mean() / std * np.sqrt(periods_per_year))


def compute_metrics(
    history_df: pd.DataFrame,
    initial_capital: float,
) -> dict:
    """
    すべての評価指標を一括計算する

    Args:
        history_df: バックテストの賭け記録 DataFrame
        initial_capital: 初期資金

    Returns:
        評価指標の辞書:
            recovery_rate: 回収率 (%)
            hit_rate: 的中率 (%)
            max_drawdown: 最大ドローダウン (%)
            sharpe_ratio: シャープレシオ
            total_bets: 総賭け数
            total_hits: 総的中数
            total_bet_amount: 総賭け金 (円)
            total_return_amount: 総払戻金 (円)
            profit: 損益 (円)
            final_capital: 最終資金 (円)
    """
    empty = {
        "recovery_rate": 0.0,
        "hit_rate": 0.0,
        "max_drawdown": 0.0,
        "sharpe_ratio": 0.0,
        "total_bets": 0,
        "total_hits": 0,
        "total_bet_amount": 0.0,
        "total_return_amount": 0.0,
        "profit": 0.0,
        "final_capital": initial_capital,
    }

    if len(history_df) == 0:
        return empty

    total_bet = float(history_df["bet_amount"].sum())
    total_return = float(history_df["return_amount"].sum())
    final_capital = float(history_df["capital_after"].iloc[-1])

    return {
        "recovery_rate": recovery_rate(history_df),
        "hit_rate": hit_rate(history_df),
        "max_drawdown": max_drawdown(history_df, initial_capital),
        "sharpe_ratio": sharpe_ratio(history_df),
        "total_bets": len(history_df),
        "total_hits": int(history_df["is_hit"].sum()),
        "total_bet_amount": total_bet,
        "total_return_amount": total_return,
        "profit": total_return - total_bet,
        "final_capital": final_capital,
    }
