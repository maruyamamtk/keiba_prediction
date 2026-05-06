"""
バックテストモジュール

過去データを使用した投資シミュレーションと評価指標を提供する。
"""

from src.backtest.metrics import compute_metrics
from src.backtest.strategy import (
    select_bets_for_race,
    select_base_bets,
    _allocate_bets,
)
from src.backtest.strategy_optimizer import OptimizationResult, StrategyOptimizer

__all__ = [
    "compute_metrics",
    "select_bets_for_race",
    "select_base_bets",
    "_allocate_bets",
    "OptimizationResult",
    "StrategyOptimizer",
]
