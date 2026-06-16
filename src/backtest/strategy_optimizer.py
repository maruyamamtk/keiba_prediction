"""
投資戦略オプティマイザー

グリッドサーチまたはOptunaベイズ最適化により投資戦略のパラメータを最適化する。

最適化対象パラメータ:
  - expected_return_threshold: 期待回収率フィルタ閾値
  - top_n: 候補馬数
  - prob_weight_r: 選定スコアの確率ウェイト係数
  - min_prob_threshold: 最低複勝率
  - max_wide_odds: ワイドオッズ上限
  - temperature: win_place_prob ソフトマックス温度（Optuna探索時）
"""



import logging
import warnings
from dataclasses import dataclass, field
from itertools import product
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from .metrics import compute_metrics
from .strategy import select_bets_for_race

if TYPE_CHECKING:
    import optuna

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
        pattern_breakdown: 後方互換性のために残した空辞書
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
        budget_per_race: float = 3000.0,
        # 後方互換性のための旧パラメータ（無視される）
        max_bet_ratio: float | None = None,
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
                資金曲線・最大ドローダウン計算に使用。賭け金には影響しない
            combo_odds_df: コンボオッズ DataFrame（None の場合は空 DataFrame）
            budget_per_race: 1レースあたりの固定予算 (円)
        """
        self.predictions_df = predictions_df.copy()
        self.payouts_df = payouts_df if payouts_df is not None else pd.DataFrame()
        self.initial_capital = initial_capital
        self.combo_odds_df = combo_odds_df if combo_odds_df is not None else pd.DataFrame()
        self.budget_per_race = budget_per_race

        # 払戻マップを事前構築: (race_id, bet_type, horse_numbers_tuple) -> payout_amount
        self._payout_map: dict[tuple, int] = {}
        if len(self.payouts_df) > 0 and "bet_type" in self.payouts_df.columns:
            for _, row in self.payouts_df.iterrows():
                bet_type = str(row.get("bet_type", "place"))
                race_id = str(row["race_id"])

                h1 = row.get("horse_number_1", None)
                h2 = row.get("horse_number_2", None)
                h3 = row.get("horse_number_3", None)

                if h1 is None or pd.isna(h1):
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
        expected_return_threshold: float = 1.2,
        min_bet_amount: float = 100.0,
        min_prob_threshold: float = 0.0,
        prob_weight_r: float = 1.0,
        top_n: int = 5,
        max_wide_odds: float | None = None,
        temperature: float | None = None,
        # 後方互換性のための旧パラメータ（無視される）
        p1: float | None = None,
        prob_weight_r_dominant: float | None = None,
        prob_weight_r_standard: float | None = None,
        top_n_dominant: int | None = None,
        top_n_standard: int | None = None,
    ) -> tuple[pd.DataFrame, dict[str, dict]]:
        """
        指定パラメータでシミュレーションを実行する（内部メソッド）

        Args:
            expected_return_threshold: 期待回収率閾値
            min_bet_amount: 最低賭け金 (円)
            min_prob_threshold: 軸馬の最低複勝率
            prob_weight_r: 選定スコアの確率ウェイト係数
            top_n: 候補馬数
            max_wide_odds: ワイド購入の上限オッズ（None で無制限）
            temperature: ソフトマックス温度（None の場合は win_place_prob を再計算しない）
                pred_score カラムが存在する場合のみ有効

        Returns:
            (history_df, pattern_stats) のタプル:
              - history_df: 賭け記録 DataFrame
              - pattern_stats: 互換性のための空辞書
        """
        # temperature が指定され pred_score が存在する場合は win_place_prob を再計算
        if temperature is not None and "pred_score" in self.predictions_df.columns:
            from src.models.predict import normalize_win_place_prob
            df = normalize_win_place_prob(self.predictions_df, temperature=temperature)
            df = df.sort_values(["race_date", "race_id", "horse_number"]).copy()
        else:
            # 日付・レースID でソート
            df = self.predictions_df.sort_values(
                ["race_date", "race_id", "horse_number"]
            ).copy()

        capital = float(self.initial_capital)
        records: list[dict] = []

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

            # 賭け選定
            try:
                bets = select_bets_for_race(
                    race_df=race_df,
                    combo_odds_df=race_combo_df,
                    budget_per_race=self.budget_per_race,
                    expected_return_threshold=expected_return_threshold,
                    min_bet_amount=min_bet_amount,
                    min_prob_threshold=min_prob_threshold,
                    prob_weight_r=prob_weight_r,
                    top_n=top_n,
                    max_wide_odds=max_wide_odds,
                )
            except ValueError:
                continue

            if not bets:
                continue

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

                # horse_id: place/win なら bet から取得、それ以外はNone
                horse_id = bet.get("horse_id", None)
                horse_number_repr = horse_numbers[0]

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
                        "pattern": "unified",
                    }
                )

        history_df = pd.DataFrame(records) if records else pd.DataFrame()

        # 後方互換性のために空の pattern_stats を返す
        pattern_stats: dict[str, dict] = {}

        return history_df, pattern_stats

    def run_grid_search(
        self,
        threshold_range: list[float] | None = None,
        top_n_range: list[int] | None = None,
        r_range: list[float] | None = None,
        min_prob_threshold_range: list[float] | None = None,
        max_wide_odds_range: list[float | None] | None = None,
        # 後方互換性: min_prob_threshold_range が None の場合に使用する固定値
        min_prob_threshold: float = 0.0,
        # 後方互換性: max_wide_odds_range が None の場合に使用する固定値
        max_wide_odds: float | None = None,
        # 後方互換性のための旧パラメータ（無視される）
        p1_range: list[float] | None = None,
        top_n_dominant_range: list[int] | None = None,
        top_n_standard_range: list[int] | None = None,
        r_dominant_range: list[float] | None = None,
        r_standard_range: list[float] | None = None,
    ) -> list[OptimizationResult]:
        """
        グリッドサーチを実行し、全パラメータ組み合わせのバックテスト結果を返す

        デフォルト探索範囲（min_prob_threshold_range=None, max_wide_odds_range=None の場合）:
          - threshold:  [1.2, 1.35, 1.5, 1.75]  （期待回収率閾値）
          - top_n:      [2, 3, 4, 5]             （候補馬数）
          - r:          [0.6, 0.8, 1.0, 1.2, 1.5]（prob_weight_r）
        総組み合わせ数: 4×4×5 = 80通り

        max_wide_odds_range を指定した場合は追加次元のグリッドサーチになる:
          - threshold × top_n × r × min_prob × max_wide_odds 全組み合わせを探索

        Args:
            threshold_range: 期待回収率閾値の探索値リスト
            top_n_range: 候補馬数の探索値リスト
            r_range: prob_weight_r の探索値リスト
            min_prob_threshold_range: 全候補馬共通の最低複勝率の探索値リスト。
                None の場合は min_prob_threshold 固定値を全組み合わせに適用（後方互換）
            max_wide_odds_range: ワイドオッズ上限の探索値リスト（None を含む場合は無制限も探索）。
                None の場合は max_wide_odds 固定値を全組み合わせに適用
            min_prob_threshold: min_prob_threshold_range=None 時の固定値（後方互換）
            max_wide_odds: max_wide_odds_range=None 時の固定値（後方互換）
            p1_range: 廃止済み（無視される）
            top_n_dominant_range: 廃止済み（無視される）
            top_n_standard_range: 廃止済み（無視される）
            r_dominant_range: 廃止済み（r_range にリネームされた）
            r_standard_range: 廃止済み（無視される）

        Returns:
            OptimizationResult のリスト（全パラメータ組み合わせ分）
        """
        # 後方互換性: r_dominant_range が指定されていれば r_range として使用
        if r_range is None and r_dominant_range is not None:
            r_range = r_dominant_range

        if threshold_range is None:
            threshold_range = [1.2, 1.35, 1.5, 1.75]
        if top_n_range is None:
            top_n_range = [2, 3, 4, 5]
        if r_range is None:
            r_range = [0.6, 0.8, 1.0, 1.2, 1.5]

        # min_prob の探索値リストを確定
        mpt_values = min_prob_threshold_range if min_prob_threshold_range is not None else [min_prob_threshold]
        # max_wide_odds の探索値リストを確定
        mwo_values = max_wide_odds_range if max_wide_odds_range is not None else [max_wide_odds]

        param_grid = list(product(threshold_range, top_n_range, r_range, mpt_values, mwo_values))
        total = len(param_grid)

        dim_info = (
            f"threshold×top_n×r×min_prob×max_wide_odds = "
            f"{len(threshold_range)}×{len(top_n_range)}×{len(r_range)}×{len(mpt_values)}×{len(mwo_values)}"
        )
        warnings.warn(
            "run_grid_search() は非推奨です。代わりに run_optuna_search() を使用してください。",
            DeprecationWarning,
            stacklevel=2,
        )
        logger.info(f"グリッドサーチ開始: {total} パラメータ組み合わせ ({dim_info})")

        results: list[OptimizationResult] = []

        for i, (th, tn, r, mpt, mwo) in enumerate(param_grid):
            params = {
                "expected_return_threshold": th,
                "top_n": tn,
                "prob_weight_r": r,
                "min_prob_threshold": mpt,
                "max_wide_odds": mwo,
            }

            if (i + 1) % 20 == 0 or (i + 1) == total:
                logger.info(
                    f"  [{i + 1}/{total}] th={th} top_n={tn} r={r} min_prob={mpt} max_wide_odds={mwo}"
                )

            history_df, pattern_stats = self._run_simulation(
                expected_return_threshold=th,
                min_prob_threshold=mpt,
                prob_weight_r=r,
                top_n=tn,
                max_wide_odds=mwo,
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

    def run_optuna_search(
        self,
        n_trials: int = 200,
        timeout: int = 600,
        metric: str = "recovery_rate",
        min_recovery_rate: float = 100.0,
        max_max_drawdown: float = 30.0,
        min_total_bets: int = 30,
        sampler: "optuna.samplers.BaseSampler | None" = None,
    ) -> list[OptimizationResult]:
        """
        Optunaベイズ最適化で投資戦略パラメータを探索する

        探索パラメータ:
          - expected_return_threshold: [1.0, 2.5]（連続）
          - top_n: [1, 6]（整数）
          - prob_weight_r: [0.3, 2.0]（連続）
          - min_prob_threshold: [0.0, 0.3]（連続）
          - max_wide_odds: [5.0, 50.0] または None（条件付き連続）
          - temperature: [0.5, 3.0]（連続、pred_score カラムが存在する場合のみ）

        制約違反（回収率 < min_recovery_rate、最大DD > max_max_drawdown、
        賭け数 < min_total_bets）の場合はペナルティスコア（0.0）を返す。

        Args:
            n_trials: 試行回数（デフォルト: 200）
            timeout: タイムアウト秒数（デフォルト: 600）
            metric: 最大化する評価指標（デフォルト: "recovery_rate"）
            min_recovery_rate: 制約: 最低回収率 (%) （デフォルト: 100.0）
            max_max_drawdown: 制約: 最大ドローダウン上限 (%) （デフォルト: 30.0）
            min_total_bets: 制約: 最低賭け数（過学習防止、デフォルト: 30）
            sampler: Optunaサンプラー（None の場合は TPESampler）

        Returns:
            OptimizationResult のリスト（全試行分）

        Raises:
            ValueError: metric が有効値でない場合
        """
        import optuna

        _valid_metrics = {"recovery_rate", "hit_rate", "sharpe_ratio"}
        if metric not in _valid_metrics:
            raise ValueError(
                f"metric='{metric}' は無効です。有効値: {_valid_metrics}"
            )

        has_pred_score = "pred_score" in self.predictions_df.columns
        results: list[OptimizationResult] = []

        def objective(trial: optuna.Trial) -> float:
            expected_return_threshold = trial.suggest_float(
                "expected_return_threshold", 1.0, 2.5
            )
            top_n = trial.suggest_int("top_n", 1, 6)
            prob_weight_r = trial.suggest_float("prob_weight_r", 0.3, 2.0)
            min_prob_threshold = trial.suggest_float("min_prob_threshold", 0.0, 0.3)
            use_max_wide_odds = trial.suggest_categorical(
                "use_max_wide_odds", [True, False]
            )
            if use_max_wide_odds:
                max_wide_odds: float | None = trial.suggest_float(
                    "max_wide_odds", 5.0, 50.0
                )
            else:
                max_wide_odds = None
            temperature: float | None = (
                trial.suggest_float("temperature", 0.5, 3.0)
                if has_pred_score
                else None
            )

            history_df, _ = self._run_simulation(
                expected_return_threshold=expected_return_threshold,
                top_n=top_n,
                prob_weight_r=prob_weight_r,
                min_prob_threshold=min_prob_threshold,
                max_wide_odds=max_wide_odds,
                temperature=temperature,
            )
            metrics = compute_metrics(history_df, self.initial_capital)

            rr = metrics["recovery_rate"]
            dd = metrics["max_drawdown"]
            tb = metrics["total_bets"]

            params = {
                "expected_return_threshold": expected_return_threshold,
                "top_n": top_n,
                "prob_weight_r": prob_weight_r,
                "min_prob_threshold": min_prob_threshold,
                "max_wide_odds": max_wide_odds,
            }
            if has_pred_score:
                params["temperature"] = temperature

            results.append(
                OptimizationResult(
                    params=params,
                    recovery_rate=rr,
                    hit_rate=metrics["hit_rate"],
                    max_drawdown=dd,
                    sharpe_ratio=metrics["sharpe_ratio"],
                    total_bets=tb,
                )
            )

            # 制約違反時はペナルティ（回収率0%相当）
            if rr < min_recovery_rate or dd > max_max_drawdown or tb < min_total_bets:
                return 0.0

            return metrics[metric]

        if sampler is None:
            sampler = optuna.samplers.TPESampler()

        study = optuna.create_study(direction="maximize", sampler=sampler)
        study.optimize(objective, n_trials=n_trials, timeout=timeout)

        try:
            best_val = study.best_value
        except ValueError:
            best_val = float("nan")
        logger.info(
            f"Optuna最適化完了: {len(results)} 試行, "
            f"最良値={best_val:.4f} ({metric})"
        )
        return results

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
