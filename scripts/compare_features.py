#!/usr/bin/env python3
"""
特徴量変更後のモデル精度比較スクリプト

新しい特徴量が feature_query_raw.sql に追加されるたびに実行し、
既存モデルとの精度を自動比較する。

ワークフロー:
  1. 特徴量パイプラインを実行（--skip-feature-pipeline で省略可）
  2. BigQuery から学習/検証データを取得
  3. ベースラインモデルの評価（GCS から読み込み or 最新モデルを自動取得）
  4. 新モデルの Optuna ハイパーパラメータ調整 + 学習
  5. 検証期間で両モデルを評価し Markdown レポートを生成

Usage:
  # 新特徴量追加後の標準ワークフロー（feature pipelineも実行）
  .venv/bin/python scripts/compare_features.py \\
      --project-id keiba-prediction-1768734113

  # feature pipeline 済みの場合はスキップ
  .venv/bin/python scripts/compare_features.py \\
      --project-id keiba-prediction-1768734113 \\
      --skip-feature-pipeline \\
      --baseline-model-gcs gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker/20260510/lgbm_ranker_20260510.txt

  # 全オプション
  .venv/bin/python scripts/compare_features.py \\
      --project-id keiba-prediction-1768734113 \\
      --start-date 2016-01-01 \\
      --end-date 2026-05-10 \\
      --execution-date 2026-05-10 \\
      --validation-months 6 \\
      --baseline-model-gcs gs://... \\
      --skip-feature-pipeline \\
      --n-trials 50 \\
      --timeout 1800 \\
      --output-report reports/comparison_20260510.md \\
      --verbose
"""

import argparse
import datetime
import json
import logging
import sys
import tempfile
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import yaml
from google.cloud import bigquery, storage

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ml.features.feature_pipeline import FeaturePipeline, FeaturePipelineConfig
from src.models.train import (
    fetch_training_data,
    split_train_valid_predict,
    prepare_features,
    evaluate_predictions,
    build_feature_matrix,
)
from src.models.lgbm_ranker import LGBMRanker
from src.models.tuning import run_tuning

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "model_config.yaml"


# ---------------------------------------------------------------------------
# 設定・ユーティリティ
# ---------------------------------------------------------------------------

def load_config(config_path: str | None = None) -> dict:
    path = Path(config_path) if config_path else CONFIG_PATH
    with open(path) as f:
        return yaml.safe_load(f)


def get_latest_model_gcs(project_id: str, bucket_suffix: str, model_prefix: str) -> str:
    """GCS から最新モデルの URI を自動取得する"""
    bucket_name = f"{project_id}-{bucket_suffix}"
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=f"{model_prefix}/"))
    txt_blobs = [b for b in blobs if b.name.endswith(".txt")]
    if not txt_blobs:
        raise FileNotFoundError(f"GCS にモデルが見つかりません: gs://{bucket_name}/{model_prefix}/")

    # 日付フォルダ名で降順ソートして最新を取得
    txt_blobs.sort(key=lambda b: b.name.split("/")[-2], reverse=True)
    latest = txt_blobs[0]
    uri = f"gs://{bucket_name}/{latest.name}"
    logger.info(f"最新モデルを自動取得: {uri}")
    return uri


def download_model_from_gcs(gcs_uri: str, project_id: str, tmpdir: Path) -> tuple[Path, dict]:
    """GCS からモデルファイルと meta.json をダウンロードする"""
    uri_without_prefix = gcs_uri.removeprefix("gs://")
    bucket_name, blob_path = uri_without_prefix.split("/", 1)

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    model_file = tmpdir / Path(blob_path).name
    meta_blob_path = str(Path(blob_path).parent / (Path(blob_path).stem + ".meta.json"))
    meta_file = tmpdir / (Path(blob_path).stem + ".meta.json")

    logger.info(f"モデルをダウンロード中: {gcs_uri}")
    bucket.blob(blob_path).download_to_filename(str(model_file))
    bucket.blob(meta_blob_path).download_to_filename(str(meta_file))

    with open(meta_file) as f:
        meta = json.load(f)

    return model_file, meta


# ---------------------------------------------------------------------------
# 特徴量パイプライン
# ---------------------------------------------------------------------------

def run_feature_pipeline(project_id: str, start_date: str, end_date: str) -> None:
    logger.info(f"特徴量パイプライン実行: {start_date} 〜 {end_date}")
    config = FeaturePipelineConfig()
    pipeline = FeaturePipeline(project_id, config)
    result = pipeline.run(start_date, end_date)
    logger.info(
        f"完了: 削除 {result['deleted_rows']}行, 挿入 {result['inserted_rows']}行, "
        f"{result['elapsed_time']:.1f}秒"
    )


# ---------------------------------------------------------------------------
# データ取得・分割
# ---------------------------------------------------------------------------

def fetch_and_split(
    project_id: str,
    dataset: str,
    table: str,
    execution_date: datetime.date,
    validation_months: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """BQ からデータ取得し学習/検証に分割する"""
    logger.info("BigQuery からデータ取得中...")
    df = fetch_training_data(project_id, dataset, table)

    train_df, valid_df, _ = split_train_valid_predict(
        df, execution_date, validation_months
    )

    period_info = {
        "train_from": str(train_df["race_date"].min()),
        "train_to": str(train_df["race_date"].max()),
        "train_rows": len(train_df),
        "train_races": train_df["race_id"].nunique(),
        "valid_from": str(valid_df["race_date"].min()),
        "valid_to": str(valid_df["race_date"].max()),
        "valid_rows": len(valid_df),
        "valid_races": valid_df["race_id"].nunique(),
    }
    logger.info(
        f"学習: {period_info['train_rows']}行/{period_info['train_races']}レース "
        f"({period_info['train_from']}〜{period_info['train_to']})"
    )
    logger.info(
        f"検証: {period_info['valid_rows']}行/{period_info['valid_races']}レース "
        f"({period_info['valid_from']}〜{period_info['valid_to']})"
    )
    return train_df, valid_df, period_info


# ---------------------------------------------------------------------------
# ベースラインモデル評価
# ---------------------------------------------------------------------------

def evaluate_baseline(
    model_file: Path,
    meta: dict,
    valid_df: pd.DataFrame,
    config: dict,
) -> dict:
    """GCS から読み込んだベースラインモデルを検証データで評価する"""
    baseline_features = meta.get("feature_names", [])
    logger.info(f"ベースライン特徴量数: {len(baseline_features)}")

    data_config = config["data"]
    exclude_cols = set(data_config["exclude_columns"])
    cat_cols = data_config.get("categorical_columns", [])

    # ベースラインモデルが使う特徴量のみ選択
    available = [c for c in baseline_features if c in valid_df.columns]
    missing = [c for c in baseline_features if c not in valid_df.columns]
    if missing:
        logger.warning(f"ベースラインモデルの特徴量が検証データにない: {missing}")

    X_valid = valid_df[available].copy()
    for col in cat_cols:
        if col in X_valid.columns:
            X_valid[col] = X_valid[col].astype("category")

    ranker = LGBMRanker()
    ranker.load(str(model_file))

    y_pred = ranker.predict(X_valid)
    y_true_positions = valid_df["finish_position"].fillna(0).values.astype(int)
    groups_valid = valid_df.groupby("race_id", sort=False).size().tolist()

    metrics = evaluate_predictions(y_true_positions, y_pred, groups_valid)
    metrics["best_iteration"] = meta.get("best_iteration", "N/A")
    metrics["feature_count"] = len(available)
    return metrics


# ---------------------------------------------------------------------------
# 新モデル: Optuna チューニング + 学習 + 評価
# ---------------------------------------------------------------------------

def train_and_evaluate_new_model(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    config: dict,
    n_trials: int,
    timeout: int,
) -> dict:
    """Optuna チューニング付きで新モデルを学習・評価する"""
    data_config = config["data"]
    exclude_cols = data_config["exclude_columns"]
    cat_cols = data_config.get("categorical_columns", [])

    X_train, y_train, groups_train = prepare_features(train_df, exclude_cols, cat_cols)
    X_valid, y_valid, groups_valid = prepare_features(valid_df, exclude_cols, cat_cols)

    new_feature_count = len(X_train.columns)
    logger.info(f"新モデル特徴量数: {new_feature_count}")

    # ハイパーパラメータ調整
    tune_config = dict(config)
    tune_config["tuning"] = dict(config.get("tuning", {}))
    tune_config["tuning"]["n_trials"] = n_trials
    tune_config["tuning"]["timeout"] = timeout

    logger.info(f"Optuna チューニング開始: n_trials={n_trials}, timeout={timeout}s")
    tuning_result = run_tuning(
        X_train, y_train, groups_train,
        X_valid, y_valid, groups_valid,
        tune_config,
    )
    best_params = tuning_result["best_params"]
    logger.info(f"チューニング完了: best_auc={tuning_result['best_value']:.4f}, params={best_params}")

    # 最適パラメータで本学習
    train_data = lgb.Dataset(X_train, label=y_train, group=groups_train, free_raw_data=False)
    valid_data = lgb.Dataset(
        X_valid, label=y_valid, group=groups_valid, reference=train_data, free_raw_data=False
    )
    training_cfg = config["model"]["training"]
    model = lgb.train(
        best_params,
        train_data,
        num_boost_round=training_cfg.get("num_boost_round", 1000),
        valid_sets=[valid_data],
        callbacks=[
            lgb.early_stopping(training_cfg.get("early_stopping_rounds", 50), verbose=False),
            lgb.log_evaluation(training_cfg.get("log_evaluation", 100)),
        ],
    )

    y_pred = model.predict(X_valid)
    y_true_positions = valid_df["finish_position"].fillna(0).values.astype(int)
    metrics = evaluate_predictions(y_true_positions, y_pred, groups_valid)
    metrics["best_iteration"] = model.best_iteration
    metrics["feature_count"] = new_feature_count
    metrics["best_params"] = best_params
    metrics["tuning_best_auc"] = tuning_result["best_value"]
    metrics["n_trials_done"] = tuning_result["n_trials"]

    return metrics


# ---------------------------------------------------------------------------
# レポート生成
# ---------------------------------------------------------------------------

def build_report(
    baseline_metrics: dict,
    new_metrics: dict,
    period_info: dict,
    baseline_gcs_uri: str,
    execution_date: datetime.date,
    n_trials: int,
    timeout: int,
    threshold: float = 0.005,
) -> str:
    def diff_str(new_val, base_val) -> str:
        d = new_val - base_val
        sign = "+" if d >= 0 else ""
        return f"{sign}{d:.4f}"

    def judge(new_val, base_val, threshold) -> str:
        d = new_val - base_val
        if d >= 0:
            return "✅ 改善"
        elif d >= -threshold:
            return f"✅ 同等 (±{threshold}以内)"
        else:
            return "❌ 悪化"

    ndcg_b, ndcg_n = baseline_metrics["ndcg@3"], new_metrics["ndcg@3"]
    auc_b, auc_n = baseline_metrics["auc"], new_metrics["auc"]
    rec_b, rec_n = baseline_metrics["recall@3"], new_metrics["recall@3"]

    passed = all([
        new_metrics["ndcg@3"] >= baseline_metrics["ndcg@3"] - threshold,
        new_metrics["auc"] >= baseline_metrics["auc"] - threshold,
        new_metrics["recall@3"] >= baseline_metrics["recall@3"] - threshold,
    ])
    verdict = "✅ **マージ可（全指標が合格基準を満たす）**" if passed else "❌ **要修正（一部指標が合格基準を下回る）**"

    report = f"""# 特徴量変更 精度比較レポート

生成日時: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
実行日: {execution_date}

## データ期間

| | 期間 | 行数 | レース数 |
|---|---|---|---|
| **学習** | {period_info['train_from']} 〜 {period_info['train_to']} | {period_info['train_rows']:,} | {period_info['train_races']:,} |
| **検証** | {period_info['valid_from']} 〜 {period_info['valid_to']} | {period_info['valid_rows']:,} | {period_info['valid_races']:,} |

## 精度比較

| 指標 | 既存モデル | 新モデル | 差分 | 判定 (±{threshold}) |
|---|---|---|---|---|
| **NDCG@3** | {ndcg_b:.4f} | {ndcg_n:.4f} | {diff_str(ndcg_n, ndcg_b)} | {judge(ndcg_n, ndcg_b, threshold)} |
| **AUC** | {auc_b:.4f} | {auc_n:.4f} | {diff_str(auc_n, auc_b)} | {judge(auc_n, auc_b, threshold)} |
| **Recall@3** | {rec_b:.4f} | {rec_n:.4f} | {diff_str(rec_n, rec_b)} | {judge(rec_n, rec_b, threshold)} |

## モデル詳細

| | 既存モデル | 新モデル |
|---|---|---|
| **特徴量数** | {baseline_metrics['feature_count']} | {new_metrics['feature_count']} (+{new_metrics['feature_count'] - baseline_metrics['feature_count']}) |
| **Best Iteration** | {baseline_metrics['best_iteration']} | {new_metrics['best_iteration']} |
| **ハイパーパラメータ** | チューニング済み（GCS） | Optuna {n_trials}試行 / {timeout}秒 (best AUC: {new_metrics['tuning_best_auc']:.4f}) |

## 新モデルの最適ハイパーパラメータ

```json
{json.dumps(new_metrics.get('best_params', {}), indent=2, ensure_ascii=False)}
```

## 総合判定

{verdict}

ベースラインモデル: `{baseline_gcs_uri}`
"""
    return report


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="特徴量変更後の精度比較スクリプト",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--project-id", required=True, help="GCP プロジェクト ID")
    parser.add_argument("--start-date", default="2016-01-01", help="特徴量生成の開始日 (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=str(datetime.date.today()), help="特徴量生成の終了日 (YYYY-MM-DD)")
    parser.add_argument(
        "--execution-date",
        default=str(datetime.date.today()),
        help="学習/検証分割の基準日 (YYYY-MM-DD)",
    )
    parser.add_argument("--validation-months", type=int, default=6, help="検証期間の月数")
    parser.add_argument("--dataset", default="features", help="BigQuery データセット名")
    parser.add_argument("--table", default="training_data", help="BigQuery テーブル名")
    parser.add_argument(
        "--baseline-model-gcs",
        help="ベースラインモデルの GCS URI (省略時は GCS から最新を自動取得)",
    )
    parser.add_argument("--skip-feature-pipeline", action="store_true", help="特徴量パイプラインをスキップ")
    parser.add_argument("--n-trials", type=int, default=50, help="Optuna トライアル数")
    parser.add_argument("--timeout", type=int, default=1800, help="Optuna タイムアウト秒数")
    parser.add_argument(
        "--output-report",
        default=f"reports/comparison_{datetime.date.today().strftime('%Y%m%d')}.md",
        help="レポート出力先パス",
    )
    parser.add_argument("--config", help="モデル設定ファイルパス")
    parser.add_argument("--threshold", type=float, default=0.005, help="同等とみなす差分の閾値")
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細ログを出力")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    optuna_logger = logging.getLogger("optuna")
    optuna_logger.setLevel(logging.WARNING)

    config = load_config(args.config)
    execution_date = datetime.date.fromisoformat(args.execution_date)

    # ── Step 1: 特徴量パイプライン ──────────────────────────────────────────
    if not args.skip_feature_pipeline:
        run_feature_pipeline(args.project_id, args.start_date, args.end_date)
    else:
        logger.info("特徴量パイプラインをスキップ（--skip-feature-pipeline）")

    # ── Step 2: データ取得・分割 ────────────────────────────────────────────
    train_df, valid_df, period_info = fetch_and_split(
        args.project_id, args.dataset, args.table,
        execution_date, args.validation_months,
    )

    # ── Step 3: ベースライン評価 ────────────────────────────────────────────
    gcs_cfg = config.get("gcs", {})
    baseline_gcs_uri = args.baseline_model_gcs or get_latest_model_gcs(
        args.project_id,
        gcs_cfg.get("bucket_suffix", "keiba-models"),
        gcs_cfg.get("model_prefix", "lgbm_ranker"),
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        model_file, meta = download_model_from_gcs(baseline_gcs_uri, args.project_id, Path(tmpdir))
        logger.info("ベースラインモデルを評価中...")
        baseline_metrics = evaluate_baseline(model_file, meta, valid_df, config)

    logger.info(
        f"ベースライン: NDCG@3={baseline_metrics['ndcg@3']:.4f}, "
        f"AUC={baseline_metrics['auc']:.4f}, "
        f"Recall@3={baseline_metrics['recall@3']:.4f}"
    )

    # ── Step 4: 新モデル（チューニング + 学習 + 評価）──────────────────────
    logger.info("新モデルのチューニング・学習・評価を開始...")
    new_metrics = train_and_evaluate_new_model(
        train_df, valid_df, config, args.n_trials, args.timeout
    )

    logger.info(
        f"新モデル: NDCG@3={new_metrics['ndcg@3']:.4f}, "
        f"AUC={new_metrics['auc']:.4f}, "
        f"Recall@3={new_metrics['recall@3']:.4f}"
    )

    # ── Step 5: レポート生成 ─────────────────────────────────────────────
    report = build_report(
        baseline_metrics, new_metrics, period_info,
        baseline_gcs_uri, execution_date,
        args.n_trials, args.timeout, args.threshold,
    )

    output_path = Path(args.output_report)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")

    print("\n" + "=" * 70)
    print(report)
    print("=" * 70)
    logger.info(f"レポートを保存しました: {output_path}")

    # 合格基準チェック（終了コードに反映）
    passed = all([
        new_metrics["ndcg@3"] >= baseline_metrics["ndcg@3"] - args.threshold,
        new_metrics["auc"] >= baseline_metrics["auc"] - args.threshold,
        new_metrics["recall@3"] >= baseline_metrics["recall@3"] - args.threshold,
    ])
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
