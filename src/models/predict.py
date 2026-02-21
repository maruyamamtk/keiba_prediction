"""
LightGBM LambdaRank 推論スクリプト

学習済みモデルを使用して、今週の土曜・日曜のレースに対する
着順予測を行い、結果を出力する。

Usage:
    python src/models/predict.py --project-id <PROJECT_ID> --model-path <MODEL_PATH>
    python src/models/predict.py --project-id <PROJECT_ID> --model-path <MODEL_PATH> --execution-date 2026-02-15
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yaml
from google.cloud import bigquery, storage

from src.models.lgbm_ranker import LGBMRanker
from src.models.train import (
    CONFIG_PATH,
    build_feature_matrix,
    compute_week_boundaries,
    load_config,
)

logger = logging.getLogger(__name__)

VENUE_MAP = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
}


def fetch_prediction_data(
    project_id: str,
    dataset: str,
    table: str,
    target_dates: list[datetime.date],
) -> pd.DataFrame:
    """
    BigQueryから推論対象データを取得する

    Args:
        project_id: GCPプロジェクトID
        dataset: データセット名
        table: テーブル名
        target_dates: 推論対象日のリスト

    Returns:
        推論対象データのDataFrame
    """
    client = bigquery.Client(project=project_id)

    dates_str = ", ".join(f"'{d.isoformat()}'" for d in target_dates)
    query = f"""
    SELECT *
    FROM `{project_id}.{dataset}.{table}`
    WHERE race_date IN ({dates_str})
    ORDER BY race_date, race_id, horse_number
    """
    logger.info(f"Fetching prediction data for dates: {target_dates}")
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} rows")
    return df


def load_model_from_gcs(
    project_id: str,
    bucket_suffix: str,
    model_prefix: str,
    execution_date: datetime.date,
    local_dir: str,
) -> str:
    """
    GCSからモデルファイルをダウンロードする

    Args:
        project_id: GCPプロジェクトID
        bucket_suffix: バケット名のサフィックス
        model_prefix: GCS内のプレフィックス
        execution_date: 実行日
        local_dir: ダウンロード先ディレクトリ

    Returns:
        ローカルのモデルファイルパス
    """
    bucket_name = f"{project_id}-{bucket_suffix}"
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    date_str = execution_date.strftime("%Y%m%d")
    prefix = f"{model_prefix}/{date_str}/"

    local_path = Path(local_dir)
    local_path.mkdir(parents=True, exist_ok=True)

    model_file = None
    for blob in bucket.list_blobs(prefix=prefix):
        local_file = local_path / blob.name.split("/")[-1]
        blob.download_to_filename(str(local_file))
        logger.info(f"Downloaded {blob.name} to {local_file}")
        if local_file.suffix == ".txt":
            model_file = str(local_file)

    if model_file is None:
        raise FileNotFoundError(
            f"モデルファイルが見つかりません: gs://{bucket_name}/{prefix}"
        )

    return model_file


def _scores_to_place_prob(scores: np.ndarray, n_places: int = 3) -> np.ndarray:
    """
    スコアを複勝率に変換する（水充填アルゴリズム）

    softmax確率を元に、各馬の複勝率が0~1(0~100%)に収まり、
    合計がmin(n_places, 出走頭数)になるよう変換する。

    単純な softmax * n_places では1頭あたりの値が1を超える可能性があるため、
    上限1.0で超過分を未達馬に再配分する反復アルゴリズムを使用する。

    Args:
        scores: 各馬の予測スコア配列
        n_places: 複勝対象着順数（デフォルト3）

    Returns:
        各馬の複勝率配列（各要素0~1、合計=min(n_places, len(scores))）
    """
    n = len(scores)
    k = float(min(n_places, n))

    # softmax（数値安定性のためmaxを引く）
    shifted = scores - scores.max()
    exp_s = np.exp(shifted)
    p = exp_s / exp_s.sum()

    # 水充填アルゴリズム: k単位を各馬に分配（上限1.0）
    # 上限超過分を未上限馬にsoftmax確率比で再配分する
    probs = p * k
    for _ in range(n):  # 最大n回で必ず収束（毎回少なくとも1頭が確定）
        mask_over = probs > 1.0
        if not mask_over.any():
            break
        excess = (probs[mask_over] - 1.0).sum()
        probs[mask_over] = 1.0
        mask_under = probs < 1.0
        if not mask_under.any():
            break
        p_under_sum = p[mask_under].sum()
        if p_under_sum < 1e-12:
            probs[mask_under] += excess / mask_under.sum()
        else:
            probs[mask_under] += excess * p[mask_under] / p_under_sum

    return np.clip(probs, 0.0, 1.0)


def predict_pipeline(
    project_id: str,
    execution_date: datetime.date,
    config: dict,
    model_path: str,
) -> pd.DataFrame:
    """
    推論パイプラインを実行する

    Args:
        project_id: GCPプロジェクトID
        execution_date: 実行日
        config: 設定辞書
        model_path: モデルファイルパス

    Returns:
        予測結果のDataFrame
    """
    data_config = config["data"]

    # 1. モデル読み込み
    ranker = LGBMRanker()
    ranker.load(model_path)

    # 2. 推論対象データの取得
    saturday, sunday = compute_week_boundaries(execution_date)
    target_dates = [saturday, sunday]

    df = fetch_prediction_data(
        project_id=project_id,
        dataset=data_config["dataset"],
        table=data_config["table"],
        target_dates=target_dates,
    )

    if len(df) == 0:
        logger.warning("推論対象データがありません")
        return pd.DataFrame()

    # 3. 特徴量準備（train.pyと共通ロジックを使用）
    X = build_feature_matrix(
        df,
        exclude_columns=data_config["exclude_columns"],
        categorical_columns=data_config.get("categorical_columns", []),
    )

    # 4. 予測
    scores = ranker.predict(X)

    # 5. 結果の整形
    result_df = df[
        ["race_id", "race_date", "horse_id", "horse_number", "horse_name"]
    ].copy()
    if "venue_code" in df.columns:
        result_df["venue_code"] = df["venue_code"]
    if "race_number" in df.columns:
        result_df["race_number"] = df["race_number"]

    result_df["pred_score"] = scores
    # レースごとに複勝率を計算（水充填アルゴリズム）
    # 各馬の複勝率が0~1に収まり、合計がmin(3, 出走頭数)になるよう変換する
    for race_id, group in result_df.groupby("race_id"):
        probs = _scores_to_place_prob(group["pred_score"].values, n_places=3)
        result_df.loc[group.index, "win_place_prob"] = probs

    # レース内での予測順位を付与
    result_df["pred_rank"] = result_df.groupby("race_id")["pred_score"].rank(
        ascending=False, method="min"
    ).astype(int)

    # 着順情報がある場合は実際の順位も付与
    if "finish_position" in df.columns:
        result_df["finish_position"] = df["finish_position"]

    # オッズ情報がある場合
    for odds_col in ["odds_yesterday", "odds_today"]:
        if odds_col in df.columns:
            result_df[odds_col] = df[odds_col]

    # ソート
    result_df = result_df.sort_values(
        ["race_date", "race_id", "pred_rank"]
    ).reset_index(drop=True)

    return result_df


def format_predictions(result_df: pd.DataFrame) -> str:
    """予測結果を見やすい文字列に整形する"""
    if len(result_df) == 0:
        return "推論対象データがありません"

    lines = []
    for race_id, group in result_df.groupby("race_id", sort=False):
        race_date = group["race_date"].iloc[0]
        venue_code = group.get("venue_code", pd.Series(["?"])).iloc[0]
        venue_name = VENUE_MAP.get(str(venue_code), f"不明({venue_code})")
        race_num = group.get("race_number", pd.Series(["?"])).iloc[0]

        lines.append(f"\n{'='*60}")
        lines.append(f"Race: {venue_name} {race_num}R ({race_date})")
        lines.append(f"{'='*60}")
        lines.append(
            f"{'予測順':>6} {'馬番':>4} {'馬名':<10} {'スコア':>10} {'複勝率':>8} {'着順':>6}"
        )
        lines.append("-" * 55)

        for _, row in group.iterrows():
            finish_raw = row.get("finish_position", None)
            finish = str(int(finish_raw)) if pd.notna(finish_raw) else "-"
            horse_name = str(row.get("horse_name", "") or "")
            lines.append(
                f"{int(row['pred_rank']):>6} "
                f"{int(row['horse_number']):>4} "
                f"{horse_name:<10.10} "
                f"{row['pred_score']:>10.4f} "
                f"{row['win_place_prob']:>7.1%} "
                f"{finish:>6}"
            )

    return "\n".join(lines)


def main():
    """メイン関数（CLIから実行）"""
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="LightGBM LambdaRank 推論スクリプト")
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCPプロジェクトID",
    )
    parser.add_argument(
        "--model-path",
        required=True,
        help="モデルファイルパス（ローカル）",
    )
    parser.add_argument(
        "--execution-date",
        default=datetime.date.today().isoformat(),
        help="実行日 (YYYY-MM-DD, デフォルト: 今日)",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="設定ファイルパス",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="結果をCSV出力するパス",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細ログ")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if not args.project_id:
        logger.error("GCP_PROJECT_IDが設定されていません")
        return 1

    config = load_config(args.config)
    execution_date = datetime.date.fromisoformat(args.execution_date)

    result_df = predict_pipeline(
        project_id=args.project_id,
        execution_date=execution_date,
        config=config,
        model_path=args.model_path,
    )

    # 結果表示
    print(format_predictions(result_df))

    # CSV出力
    if args.output_csv and len(result_df) > 0:
        result_df.to_csv(args.output_csv, index=False)
        print(f"\n結果をCSVに保存しました: {args.output_csv}")

    # サマリー
    if len(result_df) > 0:
        print(f"\n合計: {result_df['race_id'].nunique()} レース, "
              f"{len(result_df)} 頭")

    return 0


if __name__ == "__main__":
    exit(main())
