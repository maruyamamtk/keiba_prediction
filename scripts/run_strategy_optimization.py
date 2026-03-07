#!/usr/bin/env python3
"""
投資戦略パラメータ最適化スクリプト（手動実行）

グリッドサーチで最適な投資戦略パラメータを探索し、
config/strategy_config.yaml に保存する。

このスクリプトを手動実行することで、以下の日次自動実行が最適パラメータで動作する:
  - POST /api/v1/strategy/daily（Cloud Scheduler）
  - scripts/run_strategy.py（手動確認用）

Usage:
    python scripts/run_strategy_optimization.py \\
        --project-id <PROJECT_ID> \\
        --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/20260301/model.txt \\
        --start-date 2024-01-01 \\
        --end-date 2024-12-31

    # 出力ファイルを指定（デフォルト: config/strategy_config.yaml）
    python scripts/run_strategy_optimization.py \\
        --project-id <PROJECT_ID> \\
        --model-path <MODEL_PATH> \\
        --start-date 2024-01-01 \\
        --end-date 2024-12-31 \\
        --output-csv results/optimization_2024.csv \\
        --metric recovery_rate

Issue #105: 投資戦略モジュールをバックテストパイプラインに統合
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
    fetch_historical_payouts,
    fetch_historical_results,
)
from src.backtest.strategy_optimizer import StrategyOptimizer
from src.models.lgbm_ranker import LGBMRanker
from src.models.predict import _scores_to_place_prob
from src.models.train import build_feature_matrix, load_config

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

    model_config = load_config()
    feature_cols = model_config.get("feature_columns", [])

    ranker = LGBMRanker()
    ranker.load(model_path)

    X, groups, meta = build_feature_matrix(features_df, feature_cols)
    scores = ranker.predict(X)
    place_probs = _scores_to_place_prob(scores, groups)

    meta = meta.copy()
    meta["win_place_prob"] = place_probs

    # 着順をJOIN
    if len(results_df) > 0:
        meta = meta.merge(
            results_df[["race_id", "horse_id", "finish_position"]],
            on=["race_id", "horse_id"],
            how="left",
        )

    # place_odds カラムがなければ NaN で補完
    if "place_odds" not in meta.columns:
        meta["place_odds"] = float("nan")

    return meta


def save_best_params_to_yaml(
    best,
    start_date: datetime.date,
    end_date: datetime.date,
    metric: str,
) -> None:
    """最良パラメータを config/strategy_config.yaml に上書き保存する"""
    with open(STRATEGY_CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    config["p1"] = best.params["p1"]
    config["p2"] = best.params["p2"]
    config["expected_return_threshold"] = best.params["expected_return_threshold"]
    config["kelly_fraction"] = best.params["kelly_fraction"]
    config["max_bet_ratio"] = best.params["max_bet_ratio"]
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
    logger.info(f"  p1={best.params['p1']}, p2={best.params['p2']}, "
                f"threshold={best.params['expected_return_threshold']}, "
                f"kelly={best.params['kelly_fraction']}, "
                f"max_bet_ratio={best.params['max_bet_ratio']}")
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
    parser.add_argument("--output-csv", help="全グリッドサーチ結果のCSV保存先")
    parser.add_argument("--top-n", type=int, default=10, help="上位N件の結果を表示")
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

    # グリッドサーチ最適化
    optimizer = StrategyOptimizer(
        predictions_df=predictions_df,
        payouts_df=payouts_df,
        initial_capital=args.initial_capital,
    )
    results = optimizer.run_grid_search()

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
            f"  #{i + 1}: p1={res.params['p1']} p2={res.params['p2']} "
            f"threshold={res.params['expected_return_threshold']} "
            f"kelly={res.params['kelly_fraction']} "
            f"max_ratio={res.params['max_bet_ratio']} "
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
