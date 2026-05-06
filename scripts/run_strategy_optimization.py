#!/usr/bin/env python3
"""
投資戦略パラメータ最適化スクリプト（手動実行）

グリッドサーチで最適な投資戦略パラメータを探索し、
config/strategy_config.yaml に保存する。

このスクリプトを手動実行することで、以下の日次自動実行が最適パラメータで動作する:
  - POST /api/v1/strategy/daily（Cloud Scheduler）
  - scripts/run_strategy.py（手動確認用）

OOS（アウトオブサンプル）評価の原則:
  --start-date / --end-date にはモデルの学習期間・検証期間に含まれない日付を指定すること。
  例: --execution-date 2025-01-06 で学習したモデル → 検証期間が ~2025-01-06 で終わるため
      --start-date 2025-01-11 以降を指定する。

実行時間の目安（データ期間: 約1年、レース数: 約3,200）:
  - デフォルト（r_dominant 4値 × r_standard 4値）: 1,296 組み合わせ → 約8時間
  - --r-dominant-range 1.0 --r-standard-range 1.0: 81 組み合わせ → 約30分（推奨）
  - --r-dominant-range 1.0 1.2 --r-standard-range 1.0 1.2: 324 組み合わせ → 約2時間

Usage:
    # 推奨: r 値を固定して高速実行（約30分）
    python scripts/run_strategy_optimization.py \\
        --project-id <PROJECT_ID> \\
        --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/20250106/lgbm_ranker_20250106.txt \\
        --start-date 2025-01-11 \\
        --end-date 2025-12-31 \\
        --r-dominant-range 1.0 \\
        --r-standard-range 1.0

    # フルグリッドサーチ（約8時間）
    python scripts/run_strategy_optimization.py \\
        --project-id <PROJECT_ID> \\
        --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/20250106/lgbm_ranker_20250106.txt \\
        --start-date 2025-01-11 \\
        --end-date 2025-12-31

    # 出力ファイルを指定
    python scripts/run_strategy_optimization.py \\
        --project-id <PROJECT_ID> \\
        --model-path <MODEL_PATH> \\
        --start-date 2025-01-11 \\
        --end-date 2025-12-31 \\
        --r-dominant-range 1.0 \\
        --r-standard-range 1.0 \\
        --output-csv results/optimization_oos_2025.csv \\
        --metric recovery_rate

Issue #105: 投資戦略モジュールをバックテストパイプラインに統合
Issue #258: OOSデータによる評価でのデータリーク修正
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
from google.cloud import bigquery

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
    """
    モデルで予測を行い、結果と着順をJOINした DataFrame を返す

    Args:
        model_path: モデルファイルパス（ローカルまたは gs:// URI）
        features_df: 特徴量 DataFrame
        results_df: 着順 DataFrame
        project_id: GCP プロジェクト ID

    Returns:
        race_id, race_date, horse_id, horse_number, win_place_prob, place_odds,
        finish_position を含む DataFrame
    """
    # GCS URI の場合はダウンロード
    if model_path.startswith("gs://"):
        import tempfile
        from google.cloud import storage
        bucket_name, blob_path = model_path[5:].split("/", 1)
        local_path = Path(tempfile.mkdtemp()) / Path(blob_path).name
        storage.Client(project=project_id).bucket(bucket_name).blob(blob_path).download_to_filename(str(local_path))
        model_path = str(local_path)
        logger.info(f"GCSからモデルをダウンロード: {model_path}")

    config = load_config()

    predictions_df = generate_predictions(
        features_df=features_df,
        results_df=results_df,
        model_path=model_path,
        config=config,
    )

    # place_odds カラムがなければ NaN で補完
    if "place_odds" not in predictions_df.columns:
        predictions_df["place_odds"] = float("nan")

    return predictions_df


def save_best_params_to_yaml(
    best,
    start_date: datetime.date,
    end_date: datetime.date,
    metric: str,
) -> None:
    """最良パラメータを config/strategy_config.yaml に上書き保存する"""
    with open(STRATEGY_CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    config["expected_return_threshold"] = best.params["expected_return_threshold"]
    config["top_n"] = best.params["top_n"]
    config["prob_weight_r"] = best.params["prob_weight_r"]
    config["min_prob_threshold"] = best.params["min_prob_threshold"]
    config["max_wide_odds"] = best.params.get("max_wide_odds", None)
    # 廃止済みパラメータを削除（存在する場合）
    for key in ["p1", "top_n_dominant", "top_n_standard", "prob_weight_r_dominant",
                "prob_weight_r_standard", "threshold_dominant", "threshold_standard"]:
        config.pop(key, None)
    config["optimization"] = {
        "last_run": datetime.datetime.now().isoformat(timespec="seconds"),
        "metric": metric,
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
        f"  expected_return_threshold={best.params['expected_return_threshold']}, "
        f"top_n={best.params['top_n']}, "
        f"prob_weight_r={best.params['prob_weight_r']}, "
        f"min_prob_threshold={best.params['min_prob_threshold']}, "
        f"max_wide_odds={best.params.get('max_wide_odds', None)}"
    )
    logger.info(f"  回収率={best.recovery_rate:.2f}%, 的中率={best.hit_rate:.2f}%, "
                f"最大ドローダウン={best.max_drawdown:.2f}%")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="投資戦略パラメータ最適化（グリッドサーチ）"
    )
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--model-path", required=True, help="モデルパス（ローカルまたは gs://）")
    parser.add_argument("--start-date", required=True, help="最適化期間開始日（YYYY-MM-DD）")
    parser.add_argument("--end-date", required=True, help="最適化期間終了日（YYYY-MM-DD）")
    parser.add_argument(
        "--metric",
        default="recovery_rate",
        choices=["recovery_rate", "hit_rate", "sharpe_ratio", "max_drawdown"],
        help="最適化の評価指標（デフォルト: recovery_rate）",
    )
    parser.add_argument("--initial-capital", type=float, default=100_000.0)
    parser.add_argument(
        "--budget-per-race",
        type=float,
        default=None,
        help="1レースあたりの固定予算 (円)。デフォルト: strategy_config.yaml の値",
    )
    parser.add_argument("--output-csv", help="全グリッドサーチ結果のCSV保存先")
    parser.add_argument("--top-n", type=int, default=10, help="上位N件の結果を表示")
    parser.add_argument(
        "--r-range",
        type=float,
        nargs="+",
        default=None,
        help="prob_weight_r の探索値（複数指定可、デフォルト: 0.6 0.8 1.0 1.2 1.5）",
    )
    parser.add_argument(
        "--threshold-range",
        type=float,
        nargs="+",
        default=None,
        help="expected_return_threshold の探索値（複数指定可、デフォルト: 1.2 1.35 1.5 1.75）",
    )
    parser.add_argument(
        "--top-n-range",
        type=int,
        nargs="+",
        default=None,
        help="top_n の探索値（複数指定可、デフォルト: 2 3 4 5）",
    )
    parser.add_argument(
        "--min-prob-threshold-range",
        type=float,
        nargs="+",
        default=None,
        help="min_prob_threshold の探索値（複数指定可）。指定時は4次元グリッドサーチ。未指定時は strategy_config.yaml の固定値を使用",
    )
    parser.add_argument(
        "--max-wide-odds-range",
        type=str,
        nargs="+",
        default=None,
        help="max_wide_odds の探索値（複数指定可、'None' で無制限を探索可）。例: --max-wide-odds-range 30 50 80 None",
    )
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID 環境変数を設定してください")

    start_date = datetime.date.fromisoformat(args.start_date)
    end_date = datetime.date.fromisoformat(args.end_date)

    logger.info(f"=== 投資戦略パラメータ最適化 ===")
    logger.info(f"期間: {start_date} ~ {end_date}")
    logger.info(f"評価指標: {args.metric}")

    # BigQuery からデータ取得
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

    # モデルで予測
    predictions_df = _load_model_and_predict(
        model_path=args.model_path,
        features_df=features_df,
        results_df=results_df,
        project_id=args.project_id,
    )

    # place_odds を取得（predictions.daily_odds 優先 → raw.odds フォールバック）
    race_ids = predictions_df["race_id"].unique().tolist()
    odds_df = fetch_place_odds(args.project_id, race_ids)

    predictions_df = predictions_df.drop(columns=["place_odds", "win_odds"], errors="ignore")

    if not odds_df.empty:
        merge_odds_cols = ["race_id", "horse_number", "place_odds"]
        if "win_odds" in odds_df.columns:
            merge_odds_cols.append("win_odds")
        predictions_df = predictions_df.merge(
            odds_df[merge_odds_cols],
            on=["race_id", "horse_number"],
            how="left",
        )
        n_with_odds = predictions_df["place_odds"].notna().sum()
        n_win_odds = predictions_df["win_odds"].notna().sum() if "win_odds" in predictions_df.columns else 0
        logger.info(f"place_oddsを付与: {n_with_odds}/{len(predictions_df)}件、win_odds付与: {n_win_odds}件")
    else:
        logger.warning("predictions.daily_odds と raw.odds が空のため、payouts_dfからplace_oddsを推算します")
        predictions_df["place_odds"] = float("nan")

    # raw.odds で取得できなかった馬は payouts_df（実際の払戻）で補完
    if not payouts_df.empty and "bet_type" in payouts_df.columns:
        place_payouts = payouts_df[payouts_df["bet_type"] == "place"].copy()
        place_payouts["place_odds_payout"] = place_payouts["payout_amount"] / 100.0
        place_payouts = place_payouts.rename(columns={"horse_number_1": "horse_number"})
        predictions_df = predictions_df.merge(
            place_payouts[["race_id", "horse_number", "place_odds_payout"]],
            on=["race_id", "horse_number"],
            how="left",
        )
        # raw.odds で取得できなかった行のみ payouts で補完
        mask = predictions_df["place_odds"].isna() & predictions_df["place_odds_payout"].notna()
        predictions_df.loc[mask, "place_odds"] = predictions_df.loc[mask, "place_odds_payout"]
        predictions_df = predictions_df.drop(columns=["place_odds_payout"])
        n_filled = mask.sum()
        if n_filled > 0:
            logger.info(f"payouts_dfでplace_oddsを補完: {n_filled}件")

    n_with_odds = predictions_df["place_odds"].notna().sum()
    logger.info(f"place_odds付与済み合計: {n_with_odds}/{len(predictions_df)}件")
    if n_with_odds == 0:
        logger.warning("place_oddsが全行NaNです。全ベットがスキップされます。")

    # コンボオッズ取得
    race_ids = predictions_df["race_id"].unique().tolist()
    combo_odds_df = fetch_combo_odds(args.project_id, race_ids)

    _strategy_cfg = _load_strategy_config(str(STRATEGY_CONFIG_PATH))
    min_prob_threshold = float(_strategy_cfg.get("min_prob_threshold", 0.10))
    budget_per_race = args.budget_per_race if args.budget_per_race is not None else \
        float(_strategy_cfg.get("budget_per_race", 3000.0))
    _yaml_mwo = _strategy_cfg.get("max_wide_odds", None)
    max_wide_odds_fixed = float(_yaml_mwo) if _yaml_mwo is not None else None

    # --max-wide-odds-range 引数を float | None のリストに変換
    max_wide_odds_range: list[float | None] | None = None
    if args.max_wide_odds_range is not None:
        max_wide_odds_range = [
            None if v.lower() == "none" else float(v)
            for v in args.max_wide_odds_range
        ]

    if args.min_prob_threshold_range is not None:
        logger.info(f"min_prob_threshold_range={args.min_prob_threshold_range} (探索パラメータ)")
    else:
        logger.info(f"min_prob_threshold={min_prob_threshold} (固定パラメータ)")
    if max_wide_odds_range is not None:
        logger.info(f"max_wide_odds_range={max_wide_odds_range} (探索パラメータ)")
    else:
        logger.info(f"max_wide_odds={max_wide_odds_fixed} (固定パラメータ)")
    logger.info(f"budget_per_race={budget_per_race} (固定パラメータ)")

    # グリッドサーチ最適化
    optimizer = StrategyOptimizer(
        predictions_df=predictions_df,
        payouts_df=payouts_df,
        initial_capital=args.initial_capital,
        combo_odds_df=combo_odds_df,
        budget_per_race=budget_per_race,
    )
    results = optimizer.run_grid_search(
        threshold_range=args.threshold_range,
        top_n_range=args.top_n_range,
        r_range=args.r_range,
        min_prob_threshold_range=args.min_prob_threshold_range,
        min_prob_threshold=min_prob_threshold,
        max_wide_odds_range=max_wide_odds_range,
        max_wide_odds=max_wide_odds_fixed,
    )

    if not results:
        logger.error("グリッドサーチ結果が空です。データを確認してください。")
        sys.exit(1)

    # 上位N件を表示
    sorted_results = sorted(
        results,
        key=lambda r: getattr(r, args.metric),
        reverse=(args.metric != "max_drawdown"),
    )
    logger.info(f"\n=== 上位{args.top_n}パラメータ（{args.metric} 順）===")
    for i, res in enumerate(sorted_results[: args.top_n]):
        logger.info(
            f"  #{i + 1}: th={res.params.get('expected_return_threshold', '?')} "
            f"top_n={res.params.get('top_n', '?')} "
            f"r={res.params.get('prob_weight_r', 1.0)} "
            f"→ 回収率={res.recovery_rate:.1f}% 的中率={res.hit_rate:.1f}% "
            f"ドローダウン={res.max_drawdown:.1f}%"
        )

    # CSV 出力（任意）
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
        logger.info(f"グリッドサーチ結果を保存: {args.output_csv}")

    # 最良パラメータを strategy_config.yaml に保存
    best = optimizer.best_params(results, metric=args.metric)
    save_best_params_to_yaml(best, start_date, end_date, args.metric)

    logger.info("\n=== 完了 ===")
    logger.info(f"strategy_config.yaml を更新しました。")
    logger.info("次回の POST /api/v1/strategy/daily から新しいパラメータが適用されます。")


if __name__ == "__main__":
    main()
