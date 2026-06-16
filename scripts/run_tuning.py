"""
Optuna ハイパーパラメータ調整 CLI スクリプト

使用例:
  .venv/bin/python scripts/run_tuning.py --project-id <PROJECT_ID> --n-trials 100
"""

import argparse
import datetime
import json
import logging
import os
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models.train import (
    fetch_training_data,
    prepare_features_multi_label,
    split_train_valid_predict,
)
from src.models.tuning import run_tuning, save_best_params

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Optuna ハイパーパラメータ調整スクリプト",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--project-id", required=True, help="GCP プロジェクト ID")
    parser.add_argument(
        "--config",
        default="config/model_config.yaml",
        help="設定ファイルパス（default: config/model_config.yaml）",
    )
    parser.add_argument("--n-trials", type=int, default=None, help="Optuna 試行回数（config 値を上書き）")
    parser.add_argument("--timeout", type=int, default=None, help="タイムアウト秒数（config 値を上書き）")
    parser.add_argument(
        "--execution-date",
        default=None,
        help="実行日 YYYY-MM-DD（default: 今日）",
    )
    parser.add_argument(
        "--output-dir",
        default="output/tuning",
        help="最適パラメータ保存先ディレクトリ（default: output/tuning）",
    )
    args = parser.parse_args()

    project_id = args.project_id or os.environ.get("GCP_PROJECT_ID", "")
    execution_date = (
        datetime.date.fromisoformat(args.execution_date)
        if args.execution_date
        else datetime.date.today()
    )

    config = _load_config(args.config)
    data_config = config["data"]
    model_config = config["model"]

    logger.info(f"execution_date={execution_date}")

    # 1. データ取得
    logger.info("データ取得中...")
    df = fetch_training_data(
        project_id=project_id,
        dataset=data_config["dataset"],
        table=data_config["table"],
    )

    # 2. 時系列分割
    train_df, valid_df, _ = split_train_valid_predict(
        df=df,
        execution_date=execution_date,
        validation_months=model_config["training"]["validation_months"],
        date_column=data_config["date_column"],
    )

    if len(train_df) == 0:
        logger.error("学習データがありません")
        sys.exit(1)
    if len(valid_df) == 0:
        logger.error("検証データがありません")
        sys.exit(1)

    # 3. 特徴量準備（JRA賞金ウェイト多値ラベル）
    logger.info("特徴量準備中...")
    exclude_cols = data_config["exclude_columns"]
    cat_cols = data_config.get("categorical_columns", [])

    X_train, y_train, groups_train = prepare_features_multi_label(
        train_df, exclude_columns=exclude_cols, categorical_columns=cat_cols,
    )
    X_valid, y_valid, groups_valid = prepare_features_multi_label(
        valid_df, exclude_columns=exclude_cols, categorical_columns=cat_cols,
    )

    categorical_in_features = [c for c in cat_cols if c in X_train.columns]

    logger.info(
        f"学習データ: {len(X_train)}行 / 検証データ: {len(X_valid)}行 / 特徴量: {X_train.shape[1]}列"
    )

    # 4. チューニング設定の構築
    tuning_config = dict(config.get("tuning", {}))
    if args.n_trials is not None:
        tuning_config["n_trials"] = args.n_trials
    if args.timeout is not None:
        tuning_config["timeout"] = args.timeout

    base_params = model_config["params"]

    # 5. チューニング実行
    logger.info(
        f"チューニング開始: n_trials={tuning_config.get('n_trials')}, "
        f"timeout={tuning_config.get('timeout')}s"
    )
    result = run_tuning(
        X_train=X_train,
        y_train=y_train,
        X_valid=X_valid,
        y_valid=y_valid,
        config={"model": {**model_config, "params": base_params}, "tuning": tuning_config},
        model_type="ranker_multi",
        groups_train=groups_train,
        groups_valid=groups_valid,
        categorical_feature=categorical_in_features or None,
    )

    # 6. 結果表示・保存
    print("\n" + "=" * 60)
    print("チューニング完了: model_type=ranker_multi")
    print(f"  best_value : {result['best_value']:.4f}")
    print(f"  best_trial : {result['best_trial_number']}")
    print(f"  n_trials   : {result['n_trials']}")
    print("\n最適パラメータ:")
    print(json.dumps(result["best_params"], indent=2, ensure_ascii=False))
    print("=" * 60)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = execution_date.strftime("%Y%m%d")
    params_path = str(output_dir / f"best_params_ranker_multi_{date_str}.json")
    save_best_params(result["best_params"], params_path)
    logger.info(f"最適パラメータを保存しました: {params_path}")


if __name__ == "__main__":
    main()
