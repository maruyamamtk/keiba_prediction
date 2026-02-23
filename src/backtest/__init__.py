"""
バックテストモジュール

過去データを使用した投資シミュレーションと評価指標を提供する。
"""

from src.backtest.metrics import compute_metrics
from src.backtest.simulator import BacktestSimulator, fractional_kelly, kelly_criterion
from src.backtest.strategy import (
    RacePattern,
    classify_race_pattern,
    select_bets_competitive,
    select_bets_for_race,
    select_bets_one_dominant,
    select_bets_standard,
)
from src.backtest.strategy_optimizer import OptimizationResult, StrategyOptimizer

__all__ = [
    "BacktestSimulator",
    "kelly_criterion",
    "fractional_kelly",
    "compute_metrics",
    "RacePattern",
    "classify_race_pattern",
    "select_bets_one_dominant",
    "select_bets_competitive",
    "select_bets_standard",
    "select_bets_for_race",
    "OptimizationResult",
    "StrategyOptimizer",
]
