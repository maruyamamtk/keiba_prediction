"""
投資戦略オプティマイザー

グリッドサーチにより投資戦略のパラメータを最適化する。

最適化対象パラメータ:
  - p1: 突出型の判定閾値
  - p2: 拮抗型の判定閾値
  - expected_return_threshold: 期待回収率フィルタ閾値
  - kelly_fraction: Fractional Kelly の係数
  - max_bet_ratio: 1レースあたりの最大賭け金比率
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from itertools import product

import numpy as np
import pandas as pd

from src.backtest.metrics import compute_metrics
from src.backtest.strategy import RacePattern, select_bets_for_race

logger = logging.getLogger(__name__)


@dataclass
class OptimizationResult:
    """
    グリッドサーチの1パラメータセットに対する最適化結果

    Attributes:
        params: 使用したパラメータの辞書
        recovery_rate: 回収率 (%)
        hit_rate: 的中率 (%)
        max_drawdown: 最大ドローダウン (%)
        sharpe_ratio: シャープレシオ
        total_bets: 総賭け数
        pattern_breakdown: パターン別の成績辞書
            各キーは 'one_dominant' | 'competitive' | 'standard'
            各値は {"bets", "hits", "bet_amount", "return_amount", "recovery_rate"}
    """

    params: dict
    recovery_rate: float
    hit_rate: float
    max_drawdown: float
    sharpe_ratio: float
    total_bets: int
    pattern_breakdown: dict = field(default_factory=dict)


class StrategyOptimizer:
    """
    投資戦略のグリッドサーチオプティマイザー

    predictions_df（予測結果）と payouts_df（払戻情報）を受け取り、
    パラメータグリッドで全組み合わせを試してバックテストを実行する。

    使用例::

        optimizer = StrategyOptimizer(predictions_df, payouts_df, initial_capital=100_000)
        results = optimizer.run_grid_search()
        best = optimizer.best_params(results, metric="recovery_rate")
        print(best.params, best.recovery_rate)
    """

    def __init__(
        self,
        predictions_df: pd.DataFrame,
        payouts_df: pd.DataFrame,
        initial_capital: float = 100_000.0,
    ):
        """
        初期化

        Args:
            predictions_df: 予測結果 DataFrame
                必須カラム:
                  - race_id, race_date, horse_id, horse_number
                  - win_place_prob (複勝率: 0〜1)
                  - finish_position (実際の着順)
                  - place_odds (複勝オッズ)
            payouts_df: 払戻情報 DataFrame
                カラム: race_id, horse_number_1, payout_amount, bet_type
                None でもよい（その場合は place_odds から推定）
            initial_capital: 初期資金 (円)
        """
        self.predictions_df = predictions_df.copy()
        self.payouts_df = payouts_df if payouts_df is not None else pd.DataFrame()
        self.initial_capital = initial_capital

        # 払戻マップを事前構築: (race_id, horse_number) -> payout_amount
        self._payout_map: dict[tuple, int] = {}
        if len(self.payouts_df) > 0 and "bet_type" in self.payouts_df.columns:
            place_df = self.payouts_df[self.payouts_df["bet_type"] == "place"]
            for _, row in place_df.iterrows():
                key = (str(row["race_id"]), int(row["horse_number_1"]))
                self._payout_map[key] = int(row["payout_amount"])

    def _run_simulation(
        self,
        p1: float,
        p2: float,
        expected_return_threshold: float,
        kelly_fraction: float,
        max_bet_ratio: float,
        min_bet_amount: float = 100.0,
    ) -> tuple[pd.DataFrame, dict[str, dict]]:
        """
        指定パラメータでシミュレーションを実行する（内部メソッド）

        Args:
            p1: 突出型判定閾値
            p2: 拮抗型判定閾値
            expected_return_threshold: 期待回収率閾値
            kelly_fraction: Fractional Kelly 係数
            max_bet_ratio: 1レースあたり最大賭け金比率
            min_bet_amount: 最低賭け金 (円)

        Returns:
            (history_df, pattern_stats) のタプル:
              - history_df: 賭け記録 DataFrame
              - pattern_stats: パターン別の集計辞書
        """
        # 日付・レースID でソート
        df = self.predictions_df.sort_values(
            ["race_date", "race_id", "horse_number"]
        ).copy()

        capital = float(self.initial_capital)
        records: list[dict] = []

        # パターン別集計用
        pattern_stats: dict[str, dict] = {
            "one_dominant": {"bets": 0, "hits": 0, "bet_amount": 0.0, "return_amount": 0.0},
            "competitive": {"bets": 0, "hits": 0, "bet_amount": 0.0, "return_amount": 0.0},
            "standard": {"bets": 0, "hits": 0, "bet_amount": 0.0, "return_amount": 0.0},
        }

        for race_id, race_group in df.groupby("race_id", sort=False):
            race_date = race_group["race_date"].iloc[0]

            # 着順の完全性チェック（有効な着順データが1件もなければスキップ）
            if "finish_position" in race_group.columns:
                valid_finish = pd.to_numeric(
                    race_group["finish_position"], errors="coerce"
                )
                if valid_finish.isna().all():
                    continue

            # NaN オッズを除外
            race_df = race_group.dropna(subset=["win_place_prob"]).copy()
            race_df = race_df[pd.to_numeric(race_df.get("place_odds", pd.Series(dtype=float)), errors="coerce") > 0]

            # place_odds カラムを odds として使用
            if "place_odds" in race_df.columns:
                race_df = race_df.rename(columns={"place_odds": "odds"})
            elif "odds" not in race_df.columns:
                continue

            race_df = race_df.dropna(subset=["odds"])
            race_df = race_df[pd.to_numeric(race_df["odds"], errors="coerce") > 0]

            if len(race_df) < 3:
                continue

            # パターン判定と賭け選定
            try:
                bets, race_pattern = select_bets_for_race(
                    race_df=race_df,
                    capital=capital,
                    p1=p1,
                    p2=p2,
                    expected_return_threshold=expected_return_threshold,
                    kelly_fraction=kelly_fraction,
                    max_bet_ratio=max_bet_ratio,
                    min_bet_amount=min_bet_amount,
                )
            except ValueError:
                continue

            if not bets:
                continue

            pattern_name = race_pattern.pattern

            # 各賭けの結果を記録
            for bet in bets:
                horse_number = bet["horse_number"]
                bet_amount = bet["bet_amount"]
                odds = bet["odds"]

                # 着順の確認
                horse_row = race_df[race_df["horse_number"] == horse_number]
                if len(horse_row) == 0:
                    continue

                finish_raw = horse_row["finish_position"].iloc[0]
                if pd.isna(finish_raw):
                    finish_position = None
                    is_hit = False
                elif int(finish_raw) == 0:
                    # 出走取消・競走中止 → スキップ
                    continue
                else:
                    finish_position = int(finish_raw)
                    is_hit = (1 <= finish_position <= 3)

                # 払戻金計算
                payout_key = (str(race_id), horse_number)
                payout_per_100 = self._payout_map.get(payout_key, None)

                if is_hit:
                    if payout_per_100 is not None:
                        return_amount = bet_amount * (payout_per_100 / 100.0)
                    else:
                        # 払戻データなし: オッズから推定
                        return_amount = bet_amount * odds
                else:
                    return_amount = 0.0

                profit = return_amount - bet_amount
                capital += profit

                # パターン別集計
                ps = pattern_stats[pattern_name]
                ps["bets"] += 1
                ps["hits"] += int(is_hit)
                ps["bet_amount"] += bet_amount
                ps["return_amount"] += return_amount

                records.append(
                    {
                        "race_id": str(race_id),
                        "race_date": race_date,
                        "horse_id": bet["horse_id"],
                        "horse_number": horse_number,
                        "win_place_prob": float(
                            horse_row["win_place_prob"].iloc[0]
                        ),
                        "odds": odds,
                        "expected_return": float(
                            horse_row["win_place_prob"].iloc[0]
                        ) * odds,
                        "bet_amount": bet_amount,
                        "finish_position": finish_position,
                        "is_hit": is_hit,
                        "payout_per_100": payout_per_100,
                        "return_amount": return_amount,
                        "profit": profit,
                        "capital_after": capital,
                        "pattern": pattern_name,
                    }
                )

        history_df = pd.DataFrame(records) if records else pd.DataFrame()

        # パターン別の回収率を計算
        for pname, ps in pattern_stats.items():
            ba = ps["bet_amount"]
            ps["recovery_rate"] = (
                ps["return_amount"] / ba * 100.0 if ba > 0 else 0.0
            )

        return history_df, pattern_stats

    def run_grid_search(
        self,
        p1_range: list[float] | None = None,
        p2_range: list[float] | None = None,
        threshold_range: list[float] | None = None,
        kelly_range: list[float] | None = None,
        max_bet_ratio_range: list[float] | None = None,
    ) -> list[OptimizationResult]:
        """
        グリッドサーチを実行し、全パラメータ組み合わせのバックテスト結果を返す

        デフォルト探索範囲:
          - p1: [0.1, 0.15, 0.2, 0.25, 0.3]
          - p2: [0.1, 0.15, 0.2]
          - threshold: [1.0, 1.1, 1.2, 1.3, 1.5]
          - kelly_fraction: [0.1, 0.25, 0.5]
          - max_bet_ratio: [0.03, 0.05]

        Args:
            p1_range: p1 の探索値リスト
            p2_range: p2 の探索値リスト
            threshold_range: 期待回収率閾値の探索値リスト
            kelly_range: kelly_fraction の探索値リスト
            max_bet_ratio_range: max_bet_ratio の探索値リスト

        Returns:
            OptimizationResult のリスト（全パラメータ組み合わせ分）
        """
        if p1_range is None:
            p1_range = [0.1, 0.15, 0.2, 0.25, 0.3]
        if p2_range is None:
            p2_range = [0.1, 0.15, 0.2]
        if threshold_range is None:
            threshold_range = [1.0, 1.1, 1.2, 1.3, 1.5]
        if kelly_range is None:
            kelly_range = [0.1, 0.25, 0.5]
        if max_bet_ratio_range is None:
            max_bet_ratio_range = [0.03, 0.05]

        # 全パラメータ組み合わせを生成
        param_grid = list(
            product(p1_range, p2_range, threshold_range, kelly_range, max_bet_ratio_range)
        )

        total = len(param_grid)
        logger.info(f"グリッドサーチ開始: {total} パラメータ組み合わせ")

        results: list[OptimizationResult] = []

        for i, (p1, p2, threshold, kelly, max_ratio) in enumerate(param_grid):
            params = {
                "p1": p1,
                "p2": p2,
                "expected_return_threshold": threshold,
                "kelly_fraction": kelly,
                "max_bet_ratio": max_ratio,
            }

            if (i + 1) % 50 == 0 or (i + 1) == total:
                logger.info(f"  [{i + 1}/{total}] p1={p1} p2={p2} threshold={threshold} kelly={kelly} max_ratio={max_ratio}")

            history_df, pattern_stats = self._run_simulation(
                p1=p1,
                p2=p2,
                expected_return_threshold=threshold,
                kelly_fraction=kelly,
                max_bet_ratio=max_ratio,
            )

            metrics = compute_metrics(history_df, self.initial_capital)

            results.append(
                OptimizationResult(
                    params=params,
                    recovery_rate=metrics["recovery_rate"],
                    hit_rate=metrics["hit_rate"],
                    max_drawdown=metrics["max_drawdown"],
                    sharpe_ratio=metrics["sharpe_ratio"],
                    total_bets=metrics["total_bets"],
                    pattern_breakdown=pattern_stats,
                )
            )

        logger.info(f"グリッドサーチ完了: {total} 件の結果")
        return results

    def best_params(
        self,
        results: list[OptimizationResult],
        metric: str = "recovery_rate",
    ) -> OptimizationResult:
        """
        指定した評価指標が最良のパラメータ設定を返す

        max_drawdown のみ「小さいほど良い」として扱う。
        その他の指標（recovery_rate, hit_rate, sharpe_ratio）は「大きいほど良い」。

        Args:
            results: run_grid_search の戻り値
            metric: 最適化対象の評価指標
                ('recovery_rate' | 'hit_rate' | 'sharpe_ratio' | 'max_drawdown')

        Returns:
            最良の OptimizationResult

        Raises:
            ValueError: results が空の場合
            AttributeError: 指定した metric が存在しない場合
        """
        if not results:
            raise ValueError("results が空です。run_grid_search を先に実行してください。")

        if not hasattr(results[0], metric):
            raise AttributeError(
                f"metric='{metric}' は OptimizationResult に存在しません。"
                f"利用可能: recovery_rate, hit_rate, max_drawdown, sharpe_ratio"
            )

        # max_drawdown は小さいほど良い
        reverse = (metric == "max_drawdown")
        sorted_results = sorted(
            results,
            key=lambda r: getattr(r, metric),
            reverse=not reverse,
        )

        best = sorted_results[0]
        logger.info(
            f"最良パラメータ ({metric}={getattr(best, metric):.4f}): {best.params}"
        )
        return best

    def filter_by_goals(
        self,
        results: list[OptimizationResult],
        min_recovery_rate: float = 100.0,
        max_max_drawdown: float = 30.0,
    ) -> list[OptimizationResult]:
        """
        回収率・最大ドローダウンの目標を達成するパラメータ設定のみを返す

        Args:
            results: run_grid_search の戻り値
            min_recovery_rate: 回収率の下限 (%) デフォルト: 100.0
            max_max_drawdown: 最大ドローダウンの上限 (%) デフォルト: 30.0

        Returns:
            条件を満たす OptimizationResult のリスト（回収率降順）
        """
        filtered = [
            r for r in results
            if r.recovery_rate >= min_recovery_rate
            and r.max_drawdown <= max_max_drawdown
        ]
        return sorted(filtered, key=lambda r: r.recovery_rate, reverse=True)

    def summary_by_pattern(
        self, results: list[OptimizationResult]
    ) -> pd.DataFrame:
        """
        全グリッドサーチ結果のパターン別成績サマリーを DataFrame で返す

        各パターン（one_dominant / competitive / standard）について、
        全パラメータ設定の平均・最大・最小の成績を集計する。

        Args:
            results: run_grid_search の戻り値

        Returns:
            パターン別の成績サマリー DataFrame
            インデックス: パターン名
            カラム: avg_recovery_rate, max_recovery_rate, min_recovery_rate,
                    avg_hit_rate, total_bets（全設定合計）
        """
        if not results:
            return pd.DataFrame()

        patterns = ["one_dominant", "competitive", "standard"]
        rows = []

        for pname in patterns:
            recovery_rates = []
            hit_rates = []
            total_bets_sum = 0

            for r in results:
                pb = r.pattern_breakdown.get(pname, {})
                if pb.get("bets", 0) > 0:
                    recovery_rates.append(pb.get("recovery_rate", 0.0))
                    ba = pb.get("bets", 0)
                    ha = pb.get("hits", 0)
                    hit_rates.append(ha / ba * 100.0 if ba > 0 else 0.0)
                    total_bets_sum += ba

            if recovery_rates:
                rows.append(
                    {
                        "pattern": pname,
                        "avg_recovery_rate": float(np.mean(recovery_rates)),
                        "max_recovery_rate": float(np.max(recovery_rates)),
                        "min_recovery_rate": float(np.min(recovery_rates)),
                        "avg_hit_rate": float(np.mean(hit_rates)),
                        "total_bets": total_bets_sum,
                    }
                )
            else:
                rows.append(
                    {
                        "pattern": pname,
                        "avg_recovery_rate": 0.0,
                        "max_recovery_rate": 0.0,
                        "min_recovery_rate": 0.0,
                        "avg_hit_rate": 0.0,
                        "total_bets": 0,
                    }
                )

        return pd.DataFrame(rows).set_index("pattern")
