"""
バックテストモジュール

過去データを使用した投資シミュレーションと評価指標を提供する。
"""

from src.backtest.metrics import compute_metrics
from src.backtest.simulator import BacktestSimulator, fractional_kelly, kelly_criterion

__all__ = [
    "BacktestSimulator",
    "kelly_criterion",
    "fractional_kelly",
    "compute_metrics",
]
