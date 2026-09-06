#!/usr/bin/env python3
"""
投資戦略パラメータ最適化スクリプト（Optunaベイズ最適化）

グリッドサーチの代わりにOptunaベイズ最適化を使い、より効率的に最適パラメータを探索する。
最終的に最良パラメータを config/strategy_config.yaml に書き出す。

注: temperature は本番予測パス（predict.py）で適用されないため最適化対象外（Issue #399）。

OOS（アウトオブサンプル）評価の原則:
  --start-date / --end-date にはモデルの学習期間・検証期間に含まれない日付を指定すること。

Usage:
    .venv/bin/python scripts/optimize_strategy.py \\
        --project-id <PROJECT_ID> \\
        --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker_multi/20250106/lgbm_ranker_multi_20250106.txt \\
        --start-date 2025-01-11 \\
        --end-date 2025-12-31 \\
        --n-trials 200 \\
        --timeout 600 \\
        --metric recovery_rate

Issue #382: 投資意思決定パラメータをOptuna最適化問題として解く
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.run_backtest import (
    fetch_historical_features,
    fetch_historical_results,
    fetch_place_payouts as fetch_historical_payouts,
    fetch_place_odds,
    fetch_combo_odds,
    generate_predictions,
    _load_strategy_config,
)
from src.backtest.strategy_optimizer import StrategyOptimizer
from src.models.train import load_config

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

STRATEGY_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "strategy_config.yaml"


def _load_model_and_predict(
    model_path: str,
    features_df: pd.DataFrame,
    results_df: pd.DataFrame,
    project_id: str,
) -> pd.DataFrame:
    """モデルで予測を行い pred_score・win_place_prob・着順を含む DataFrame を返す"""
    if model_path.startswith("gs://"):
        import tempfile
        from google.cloud import storage
        bucket_name, blob_path = model_path[5:].split("/", 1)
        local_path = Path(tempfile.mkdtemp()) / Path(blob_path).name
        storage.Client(project=project_id).bucket(bucket_name).blob(blob_path).download_to_filename(
            str(local_path)
        )
        model_path = str(local_path)
        logger.info(f"GCSからモデルをダウンロード: {model_path}")

    config = load_config()
    predictions_df = generate_predictions(
        features_df=features_df,
        results_df=results_df,
        model_path=model_path,
        config=config,
    )
    if "place_odds" not in predictions_df.columns:
        predictions_df["place_odds"] = float("nan")
    return predictions_df


def save_best_params_to_yaml(
    best,
    start_date: datetime.date,
    end_date: datetime.date,
    metric: str,
    n_trials: int,
    use_harville: bool = False,
) -> None:
    """最良パラメータを config/strategy_config.yaml に上書き保存する"""
    with open(STRATEGY_CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    config["expected_return_threshold"] = best.params["expected_return_threshold"]
    config["top_n"] = best.params["top_n"]
    # prob_weight_r はアイソトニック校正（Issue #416）後は 1.0 固定・探索対象外（Issue #417）。
    config["prob_weight_r"] = 1.0
    config["min_prob_threshold"] = best.params["min_prob_threshold"]
    config["max_wide_odds"] = best.params.get("max_wide_odds", None)
    # use_harville: この最適化実行で実際に使ったモデルを記録する。本番のデフォルトは
    # 独立積（False）であり、Harvilleモデルへの切り替えは意図的に --use-harville を
    # 指定した場合のみ（実バックテストで独立積を上回る結果が未確認のため。詳細はPR参照）。
    config["use_harville"] = use_harville
    config["gamma"] = best.params.get("gamma", 1.0)
    # 廃止済みパラメータを削除（存在する場合）。temperature は本番予測パスで未適用のため
    # 最適化対象外であり、過去の最適化で残った値があれば削除する（Issue #399）。
    for key in ["temperature", "p1", "top_n_dominant", "top_n_standard", "prob_weight_r_dominant",
                "prob_weight_r_standard", "threshold_dominant", "threshold_standard"]:
        config.pop(key, None)
    config["optimization"] = {
        "last_run": datetime.datetime.now().isoformat(timespec="seconds"),
        "method": "optuna",
        "metric": metric,
        "n_trials": n_trials,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "recovery_rate": round(best.recovery_rate, 2),
        "hit_rate": round(best.hit_rate, 2),
        "max_drawdown": round(best.max_drawdown, 2),
        "total_bets": best.total_bets,
    }

    with open(STRATEGY_CONFIG_PATH, "w") as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    logger.info(f"最適パラメータを保存: {STRATEGY_CONFIG_PATH}")
    logger.info(
        f"  expected_return_threshold={best.params['expected_return_threshold']:.3f}, "
        f"top_n={best.params['top_n']}, "
        f"prob_weight_r={best.params['prob_weight_r']:.3f}, "
        f"min_prob_threshold={best.params['min_prob_threshold']:.3f}, "
        f"max_wide_odds={best.params.get('max_wide_odds')}, "
        f"gamma={best.params.get('gamma', 1.0):.3f}"
    )
    logger.info(
        f"  回収率={best.recovery_rate:.2f}%, 的中率={best.hit_rate:.2f}%, "
        f"最大ドローダウン={best.max_drawdown:.2f}%, 総賭け数={best.total_bets}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="投資戦略パラメータ最適化（Optunaベイズ最適化）"
    )
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--model-path", required=True, help="モデルパス（ローカルまたは gs://）")
    parser.add_argument("--start-date", required=True, help="最適化期間開始日（YYYY-MM-DD）")
    parser.add_argument("--end-date", required=True, help="最適化期間終了日（YYYY-MM-DD）")
    parser.add_argument(
        "--metric",
        default="recovery_rate",
        choices=["recovery_rate", "hit_rate", "sharpe_ratio"],
        help="最大化する評価指標（デフォルト: recovery_rate）",
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=200,
        help="Optuna試行回数（デフォルト: 200）",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="最適化タイムアウト秒数（デフォルト: 600）",
    )
    parser.add_argument(
        "--min-recovery-rate",
        type=float,
        default=100.0,
        help="制約: 最低回収率 %（デフォルト: 100.0）",
    )
    parser.add_argument(
        "--max-drawdown",
        type=float,
        default=30.0,
        help="制約: 最大ドローダウン上限 %（デフォルト: 30.0）",
    )
    parser.add_argument(
        "--min-total-bets",
        type=int,
        default=600,
        help="制約: 最低賭け数（デフォルト: 600）。約6ヶ月のバックテスト期間では600を標準とし、"
             "少数サンプルのまぐれ高回収率解を排除する（Issue #399）。",
    )
    parser.add_argument("--initial-capital", type=float, default=100_000.0)
    parser.add_argument(
        "--budget-per-race",
        type=float,
        default=None,
        help="1レースあたりの固定予算 (円)。デフォルト: strategy_config.yaml の値",
    )
    parser.add_argument("--output-csv", help="全試行結果のCSV保存先")
    parser.add_argument("--top-n", type=int, default=10, help="上位N件の結果を表示")
    parser.add_argument(
        "--use-harville",
        action="store_true",
        help="ワイド・三連複の同時確率をHarvilleモデルで計算する（デフォルト: 従来の独立積）",
    )
    parser.add_argument(
        "--search-gamma",
        action="store_true",
        help="Harvilleモデルのgamma（Henery補正指数、範囲[0.5, 1.5]）も探索対象に加える"
             "（--use-harville を暗黙に有効化する）",
    )
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID 環境変数を設定してください")

    use_harville = args.use_harville or args.search_gamma

    start_date = datetime.date.fromisoformat(args.start_date)
    end_date = datetime.date.fromisoformat(args.end_date)

    logger.info("=== 投資戦略パラメータ最適化（Optuna）===")
    logger.info(f"期間: {start_date} ~ {end_date}")
    logger.info(f"評価指標: {args.metric}, 試行回数: {args.n_trials}, タイムアウト: {args.timeout}s")

    features_df = fetch_historical_features(
        project_id=args.project_id,
        dataset="features",
        table="training_data",
        start_date=start_date,
        end_date=end_date,
    )
    results_df = fetch_historical_results(args.project_id, start_date, end_date)
    payouts_df = fetch_historical_payouts(args.project_id, start_date, end_date)

    if features_df.empty:
        logger.error("特徴量データが取得できませんでした。期間やテーブルを確認してください。")
        sys.exit(1)

    predictions_df = _load_model_and_predict(
        model_path=args.model_path,
        features_df=features_df,
        results_df=results_df,
        project_id=args.project_id,
    )

    race_ids = predictions_df["race_id"].unique().tolist()
    odds_df = fetch_place_odds(args.project_id, race_ids)
    predictions_df = predictions_df.drop(columns=["place_odds", "win_odds"], errors="ignore")

    if not odds_df.empty:
        merge_odds_cols = ["race_id", "horse_number", "place_odds"]
        if "win_odds" in odds_df.columns:
            merge_odds_cols.append("win_odds")
        predictions_df = predictions_df.merge(
            odds_df[merge_odds_cols], on=["race_id", "horse_number"], how="left"
        )
        n_with_odds = predictions_df["place_odds"].notna().sum()
        logger.info(f"place_oddsを付与: {n_with_odds}/{len(predictions_df)}件")
    else:
        logger.warning("place_odds が取得できませんでした。payouts_df から補完します。")
        predictions_df["place_odds"] = float("nan")

    if not payouts_df.empty and "bet_type" in payouts_df.columns:
        place_payouts = payouts_df[payouts_df["bet_type"] == "place"].copy()
        place_payouts["place_odds_payout"] = place_payouts["payout_amount"] / 100.0
        place_payouts = place_payouts.rename(columns={"horse_number_1": "horse_number"})
        predictions_df = predictions_df.merge(
            place_payouts[["race_id", "horse_number", "place_odds_payout"]],
            on=["race_id", "horse_number"],
            how="left",
        )
        mask = predictions_df["place_odds"].isna() & predictions_df["place_odds_payout"].notna()
        predictions_df.loc[mask, "place_odds"] = predictions_df.loc[mask, "place_odds_payout"]
        predictions_df = predictions_df.drop(columns=["place_odds_payout"])
        if mask.sum() > 0:
            logger.info(f"payouts_dfでplace_oddsを補完: {mask.sum()}件")

    race_ids = predictions_df["race_id"].unique().tolist()
    combo_odds_df = fetch_combo_odds(args.project_id, race_ids)

    _strategy_cfg = _load_strategy_config(str(STRATEGY_CONFIG_PATH))
    budget_per_race = args.budget_per_race if args.budget_per_race is not None else \
        float(_strategy_cfg.get("budget_per_race", 3000.0))
    logger.info(f"budget_per_race={budget_per_race}")

    enabled_bet_types = _strategy_cfg.get("enabled_bet_types")
    logger.info(f"enabled_bet_types={enabled_bet_types}")

    optimizer = StrategyOptimizer(
        predictions_df=predictions_df,
        payouts_df=payouts_df,
        initial_capital=args.initial_capital,
        combo_odds_df=combo_odds_df,
        budget_per_race=budget_per_race,
        enabled_bet_types=enabled_bet_types,
        use_harville=use_harville,
    )
    results = optimizer.run_optuna_search(
        n_trials=args.n_trials,
        timeout=args.timeout,
        metric=args.metric,
        min_recovery_rate=args.min_recovery_rate,
        max_max_drawdown=args.max_drawdown,
        min_total_bets=args.min_total_bets,
        search_gamma=args.search_gamma,
    )

    if not results:
        logger.error("最適化結果が空です。データを確認してください。")
        sys.exit(1)

    sorted_results = sorted(
        results,
        key=lambda r: getattr(r, args.metric),
        reverse=True,
    )
    logger.info(f"\n=== 上位{args.top_n}パラメータ（{args.metric} 順）===")
    for i, res in enumerate(sorted_results[: args.top_n]):
        logger.info(
            f"  #{i + 1}: th={res.params.get('expected_return_threshold', '?'):.3f} "
            f"top_n={res.params.get('top_n', '?')} "
            f"r={res.params.get('prob_weight_r', 1.0):.3f} "
            f"gamma={res.params.get('gamma', 1.0):.3f} "
            f"→ 回収率={res.recovery_rate:.1f}% 的中率={res.hit_rate:.1f}% "
            f"ドローダウン={res.max_drawdown:.1f}% 賭け数={res.total_bets}"
        )

    if args.output_csv:
        rows = [
            {
                **r.params,
                "recovery_rate": r.recovery_rate,
                "hit_rate": r.hit_rate,
                "max_drawdown": r.max_drawdown,
                "sharpe_ratio": r.sharpe_ratio,
                "total_bets": r.total_bets,
            }
            for r in results
        ]
        pd.DataFrame(rows).to_csv(args.output_csv, index=False)
        logger.info(f"全試行結果を保存: {args.output_csv}")

    # 制約を満たす結果から最良パラメータを選定
    valid_results = optimizer.filter_by_goals(
        results,
        min_recovery_rate=args.min_recovery_rate,
        max_max_drawdown=args.max_drawdown,
        min_total_bets=args.min_total_bets,
    )
    if valid_results:
        best = optimizer.best_params(valid_results, metric=args.metric)
        logger.info(
            f"\n制約を満たす試行数: {len(valid_results)} "
            f"(回収率>={args.min_recovery_rate}%, DD<={args.max_drawdown}%, 賭け数>={args.min_total_bets})"
        )
    else:
        logger.warning(
            "制約（賭け数下限含む）を満たすパラメータが見つかりません。"
            "最終選定でも賭け数下限を強制し、これを満たす中での最良を再探索します。"
        )
        # 賭け数下限のみは必ず守り、回収率・DDを緩めて最良を選ぶ
        bets_ok = [r for r in results if r.total_bets >= args.min_total_bets]
        if bets_ok:
            best = optimizer.best_params(bets_ok, metric=args.metric)
        else:
            logger.warning(
                f"賭け数>={args.min_total_bets} を満たす試行が皆無です。"
                "全試行から最良を選定します（下限未達のため要再最適化）。"
            )
            best = optimizer.best_params(results, metric=args.metric)

    save_best_params_to_yaml(best, start_date, end_date, args.metric, args.n_trials, use_harville=use_harville)

    logger.info("\n=== 完了 ===")
    logger.info("次回の POST /api/v1/strategy/daily から新しいパラメータが適用されます。")


if __name__ == "__main__":
    main()
