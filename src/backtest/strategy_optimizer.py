"""
投資戦略オプティマイザー

グリッドサーチにより投資戦略のパラメータを最適化する。

最適化対象パラメータ:
  - p1: 突出型の判定閾値
  - expected_return_threshold: 期待回収率フィルタ閾値
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from itertools import product

import numpy as np
import pandas as pd

from .metrics import compute_metrics
from .strategy import RacePattern, select_bets_for_race

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
            各キーは 'one_dominant' | 'standard'
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
        combo_odds_df: pd.DataFrame | None = None,
        max_bet_ratio: float = 0.05,
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
                None でもよい
            initial_capital: 初期資金 (円)
            combo_odds_df: コンボオッズ DataFrame（None の場合は空 DataFrame）
            max_bet_ratio: 1レースあたりの最大賭け金比率
        """
        self.predictions_df = predictions_df.copy()
        self.payouts_df = payouts_df if payouts_df is not None else pd.DataFrame()
        self.initial_capital = initial_capital
        self.combo_odds_df = combo_odds_df if combo_odds_df is not None else pd.DataFrame()
        self.max_bet_ratio = max_bet_ratio

        # 払戻マップを事前構築: (race_id, bet_type, horse_numbers_tuple) -> payout_amount
        self._payout_map: dict[tuple, int] = {}
        if len(self.payouts_df) > 0 and "bet_type" in self.payouts_df.columns:
            for _, row in self.payouts_df.iterrows():
                bet_type = str(row.get("bet_type", "place"))
                race_id = str(row["race_id"])

                h1 = row.get("horse_number_1", None)
                h2 = row.get("horse_number_2", None)
                h3 = row.get("horse_number_3", None)

                if pd.isna(h1) if h1 is not None else True:
                    continue

                h1 = int(h1)

                if bet_type == "place":
                    key = (race_id, "place", (h1,))
                elif bet_type == "win":
                    key = (race_id, "win", (h1,))
                elif bet_type in ("wide", "umaren"):
                    if h2 is None or (isinstance(h2, float) and pd.isna(h2)):
                        continue
                    h2 = int(h2)
                    key = (race_id, bet_type, tuple(sorted([h1, h2])))
                elif bet_type == "sanrenpuku":
                    if h2 is None or h3 is None:
                        continue
                    if isinstance(h2, float) and pd.isna(h2):
                        continue
                    if isinstance(h3, float) and pd.isna(h3):
                        continue
                    h2 = int(h2)
                    h3 = int(h3)
                    key = (race_id, "sanrenpuku", tuple(sorted([h1, h2, h3])))
                else:
                    continue

                self._payout_map[key] = int(row["payout_amount"])

    def _run_simulation(
        self,
        p1: float,
        expected_return_threshold: float,
        min_bet_amount: float = 100.0,
        min_prob_threshold: float = 0.0,
        prob_weight_r: float = 1.0,
    ) -> tuple[pd.DataFrame, dict[str, dict]]:
        """
        指定パラメータでシミュレーションを実行する（内部メソッド）

        Args:
            p1: 突出型判定閾値
            expected_return_threshold: 期待回収率閾値
            min_bet_amount: 最低賭け金 (円)
            min_prob_threshold: 軸馬の最低複勝率
            prob_weight_r: 選定スコアの確率ウェイト係数

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

        # パターン別集計用（2パターン）
        pattern_stats: dict[str, dict] = {
            "one_dominant": {"bets": 0, "hits": 0, "bet_amount": 0.0, "return_amount": 0.0},
            "standard": {"bets": 0, "hits": 0, "bet_amount": 0.0, "return_amount": 0.0},
        }

        for race_id, race_group in df.groupby("race_id", sort=False):
            race_date = race_group["race_date"].iloc[0]

            # 着順の完全性チェック
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

            # このレースのcombo_odds_dfをフィルタ
            race_id_str = str(race_id)
            if len(self.combo_odds_df) > 0:
                race_combo_df = self.combo_odds_df[
                    self.combo_odds_df["race_id"] == race_id_str
                ]
            else:
                race_combo_df = pd.DataFrame()

            # パターン判定と賭け選定
            try:
                bets, race_pattern = select_bets_for_race(
                    race_df=race_df,
                    combo_odds_df=race_combo_df,
                    capital=capital,
                    p1=p1,
                    expected_return_threshold=expected_return_threshold,
                    max_bet_ratio=self.max_bet_ratio,
                    min_bet_amount=min_bet_amount,
                    min_prob_threshold=min_prob_threshold,
                    prob_weight_r=prob_weight_r,
                )
            except ValueError:
                continue

            if not bets:
                continue

            pattern_name = race_pattern.pattern

            # finish_position マップを作成
            finish_map = {}
            for _, row in race_df.iterrows():
                hn = int(row["horse_number"])
                fp_raw = row.get("finish_position", None)
                if fp_raw is not None and not pd.isna(fp_raw):
                    finish_map[hn] = int(fp_raw)

            # 各賭けの結果を記録
            for bet in bets:
                horse_numbers = bet["horse_numbers"]
                bet_amount = bet["bet_amount"]
                odds = bet["odds"]
                bet_type = bet["bet_type"]

                # 着順判定
                if bet_type == "win":
                    is_hit = finish_map.get(horse_numbers[0], 99) == 1
                else:
                    # place / wide / umaren / sanrenpuku: 全馬が3着以内
                    is_hit = all(
                        1 <= finish_map.get(h, 99) <= 3
                        for h in horse_numbers
                    )

                # 0着（取消等）チェック
                skip = False
                for h in horse_numbers:
                    fp = finish_map.get(h, None)
                    if fp is not None and fp == 0:
                        skip = True
                        break
                if skip:
                    continue

                # 払戻金計算
                payout_key = (race_id_str, bet_type, tuple(sorted(horse_numbers)))
                payout_per_100 = self._payout_map.get(payout_key, None)

                if is_hit:
                    if payout_per_100 is not None:
                        return_amount = bet_amount * (payout_per_100 / 100.0)
                    else:
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

                # horse_id: place/win なら bet から取得、それ以外はNone
                horse_id = bet.get("horse_id", None)
                # horse_number: 代表値（単一or複数）
                horse_number_repr = horse_numbers[0] if len(horse_numbers) == 1 else horse_numbers[0]

                # win_place_prob: place/win なら race_dfから取得
                win_place_prob_val = None
                if len(horse_numbers) == 1:
                    horse_rows = race_df[race_df["horse_number"] == horse_numbers[0]]
                    if len(horse_rows) > 0:
                        win_place_prob_val = float(horse_rows["win_place_prob"].iloc[0])

                # finish_position: 代表値
                finish_position = finish_map.get(horse_numbers[0], None)

                records.append(
                    {
                        "race_id": race_id_str,
                        "race_date": race_date,
                        "horse_id": horse_id,
                        "horse_number": horse_number_repr,
                        "horse_numbers": str(horse_numbers),
                        "bet_type": bet_type,
                        "win_place_prob": win_place_prob_val,
                        "odds": odds,
                        "expected_return": (win_place_prob_val * odds) if win_place_prob_val else None,
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
        threshold_range: list[float] | None = None,
        r_range: list[float] | None = None,
        min_prob_threshold: float = 0.0,
    ) -> list[OptimizationResult]:
        """
        グリッドサーチを実行し、全パラメータ組み合わせのバックテスト結果を返す

        デフォルト探索範囲:
          - p1: [0.1, 0.15, 0.2, 0.25, 0.3]
          - threshold: [1.0, 1.1, 1.2, 1.3, 1.5]
          - r: [0.5, 1.0, 1.5, 2.0]

        Args:
            p1_range: p1 の探索値リスト
            threshold_range: 期待回収率閾値の探索値リスト
            r_range: prob_weight_r の探索値リスト
            min_prob_threshold: 軸馬の最低複勝率（固定パラメータ。全組み合わせで共通適用）

        Returns:
            OptimizationResult のリスト（全パラメータ組み合わせ分）
        """
        if p1_range is None:
            p1_range = [0.1, 0.15, 0.2, 0.25, 0.3]
        if threshold_range is None:
            threshold_range = [1.0, 1.1, 1.2, 1.3, 1.5]
        if r_range is None:
            r_range = [0.5, 1.0, 1.5, 2.0]

        # 全パラメータ組み合わせを生成（5×5×4=100通り）
        param_grid = list(product(p1_range, threshold_range, r_range))

        total = len(param_grid)
        logger.info(f"グリッドサーチ開始: {total} パラメータ組み合わせ (min_prob_threshold={min_prob_threshold})")

        results: list[OptimizationResult] = []

        for i, (p1, threshold, r) in enumerate(param_grid):
            params = {
                "p1": p1,
                "expected_return_threshold": threshold,
                "prob_weight_r": r,
            }

            if (i + 1) % 10 == 0 or (i + 1) == total:
                logger.info(f"  [{i + 1}/{total}] p1={p1} threshold={threshold} r={r}")

            history_df, pattern_stats = self._run_simulation(
                p1=p1,
                expected_return_threshold=threshold,
                prob_weight_r=r,
                min_prob_threshold=min_prob_threshold,
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

        各パターン（one_dominant / standard）について、
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

        patterns = ["one_dominant", "standard"]
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
