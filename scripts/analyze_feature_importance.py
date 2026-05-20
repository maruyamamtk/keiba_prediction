#!/usr/bin/env python3
"""
Feature Importance 分析スクリプト

GCSから最新モデル（または指定モデル）を読み込み、
LightGBMのgain/split importanceを可視化・CSV出力する。

Usage:
  # 最新モデルを自動取得
  .venv/bin/python scripts/analyze_feature_importance.py \\
      --project-id keiba-prediction-1768734113

  # モデルパスを直接指定（GCS URI or ローカルパス）
  .venv/bin/python scripts/analyze_feature_importance.py \\
      --project-id keiba-prediction-1768734113 \\
      --model-path gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker/20260516/lgbm_ranker_20260516.txt

  # 出力先・閾値を指定
  .venv/bin/python scripts/analyze_feature_importance.py \\
      --project-id keiba-prediction-1768734113 \\
      --output reports/feature_importance.csv \\
      --threshold 10
"""

import argparse
import logging
import sys
import tempfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models.lgbm_ranker import LGBMRanker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _get_latest_model_gcs_uri(project_id: str, prefix: str = "lgbm_ranker/") -> str:
    """GCSから最新モデルのURIを取得する。"""
    from google.cloud import storage

    bucket_name = f"{project_id}-keiba-models"
    logger.info(f"GCSから最新モデルを検索: gs://{bucket_name}/{prefix}")

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=prefix))
    model_blobs = [b for b in blobs if b.name.endswith(".txt")]

    if not model_blobs:
        raise FileNotFoundError(
            f"モデルファイルが見つかりません: gs://{bucket_name}/{prefix}"
        )

    latest_blob = max(model_blobs, key=lambda b: b.name.split("/")[-2])
    gcs_uri = f"gs://{bucket_name}/{latest_blob.name}"
    logger.info(f"最新モデルを取得: {gcs_uri}")
    return gcs_uri


def load_ranker(project_id: str, model_path: str | None) -> LGBMRanker:
    """モデルをロードして LGBMRanker を返す。"""
    from google.cloud import storage

    if model_path is None:
        model_path = _get_latest_model_gcs_uri(project_id)

    ranker = LGBMRanker()

    if model_path.startswith("gs://"):
        parts = model_path[len("gs://"):].split("/", 1)
        bucket_name, blob_name = parts[0], parts[1]
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_model = Path(tmp_dir) / Path(blob_name).name
            bucket.blob(blob_name).download_to_filename(str(local_model))
            logger.info(f"Downloaded {model_path} to {local_model}")

            meta_blob_name = blob_name.rsplit(".", 1)[0] + ".meta.json"
            local_meta = Path(tmp_dir) / Path(meta_blob_name).name
            meta_blob = bucket.blob(meta_blob_name)
            if meta_blob.exists():
                meta_blob.download_to_filename(str(local_meta))

            ranker.load(str(local_model))
    else:
        ranker.load(model_path)

    return ranker


def build_importance_df(ranker: LGBMRanker) -> pd.DataFrame:
    """gain/split の両方の importance を含む DataFrame を返す。"""
    gain_df = ranker.feature_importance("gain").rename(columns={"importance": "gain"})
    split_df = ranker.feature_importance("split").rename(columns={"importance": "split"})

    df = gain_df.merge(split_df, on="feature")
    df = df.sort_values("gain", ascending=False).reset_index(drop=True)
    df["gain_pct"] = df["gain"] / df["gain"].sum() * 100
    df["cumulative_gain_pct"] = df["gain_pct"].cumsum()
    df["rank"] = df.index + 1
    return df[["rank", "feature", "gain", "gain_pct", "cumulative_gain_pct", "split"]]


def analyze(
    project_id: str,
    model_path: str | None,
    output: str,
    threshold: int,
) -> None:
    ranker = load_ranker(project_id, model_path)

    importance_df = build_importance_df(ranker)
    n_features = len(importance_df)

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    importance_df.to_csv(output_path, index=False)
    logger.info(f"Feature importance を保存しました: {output_path} ({n_features} 特徴量)")

    zero_gain = importance_df[importance_df["gain"] == 0]
    bottom_n = int(n_features * threshold / 100)
    bottom_df = importance_df.tail(bottom_n)

    print("\n" + "=" * 60)
    print(f"モデル特徴量数: {n_features}")
    print(f"gain=0 の特徴量: {len(zero_gain)} 件")
    print(f"下位 {threshold}% の特徴量: {bottom_n} 件")
    print("=" * 60)

    if len(zero_gain) > 0:
        print(f"\n■ gain=0 の特徴量 ({len(zero_gain)} 件):")
        for _, row in zero_gain.iterrows():
            print(f"  [{int(row['rank']):3d}] {row['feature']}")

    print(f"\n■ 下位 {threshold}% の特徴量 ({bottom_n} 件):")
    for _, row in bottom_df.iterrows():
        print(
            f"  [{int(row['rank']):3d}] {row['feature']:<50s} "
            f"gain={row['gain']:8.1f} ({row['gain_pct']:.3f}%)"
        )

    print(f"\n■ 上位10件:")
    for _, row in importance_df.head(10).iterrows():
        print(
            f"  [{int(row['rank']):3d}] {row['feature']:<50s} "
            f"gain={row['gain']:10.1f} ({row['gain_pct']:.2f}%)"
        )

    print(f"\n出力ファイル: {output_path.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="LightGBM Feature Importance 分析")
    parser.add_argument("--project-id", required=True, help="GCPプロジェクトID")
    parser.add_argument(
        "--model-path",
        default=None,
        help="モデルファイルパス（GCS URI or ローカルパス）。省略時は最新モデルを自動取得",
    )
    parser.add_argument(
        "--output",
        default="feature_importance.csv",
        help="CSV出力パス（デフォルト: feature_importance.csv）",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=10,
        help="除去候補とするgainの下位パーセンタイル（デフォルト: 10）",
    )
    args = parser.parse_args()

    analyze(
        project_id=args.project_id,
        model_path=args.model_path,
        output=args.output,
        threshold=args.threshold,
    )


if __name__ == "__main__":
    main()
