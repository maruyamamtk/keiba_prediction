"""
LightGBM LambdaRank 学習スクリプト

BigQueryからfeatures.training_dataを取得し、
時系列分割で学習・検証を行い、モデルをローカルおよびGCSに保存する。

Usage:
    python src/models/train.py --project-id <PROJECT_ID>
    python src/models/train.py --project-id <PROJECT_ID> --execution-date 2026-02-15
"""

import argparse
import datetime
import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yaml
from google.cloud import bigquery, storage

from src.models.lgbm_ranker import LGBMRanker, LGBMRankerConfig

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "model_config.yaml"


def load_config(config_path: Optional[str] = None) -> dict:
    """設定ファイルを読み込む"""
    path = Path(config_path) if config_path else CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"設定ファイルが見つかりません: {path}")
    with open(path) as f:
        return yaml.safe_load(f)


def compute_week_boundaries(execution_date: datetime.date) -> tuple[datetime.date, datetime.date]:
    """
    実行日の週の土曜・日曜を推論対象日として返す

    Args:
        execution_date: 実行日

    Returns:
        (saturday, sunday) のタプル
    """
    weekday = execution_date.weekday()  # 月=0, 日=6
    # 今週の土曜日を計算
    days_to_saturday = (5 - weekday) % 7
    saturday = execution_date + datetime.timedelta(days=days_to_saturday)
    sunday = saturday + datetime.timedelta(days=1)
    return saturday, sunday


def fetch_training_data(
    project_id: str,
    dataset: str,
    table: str,
) -> pd.DataFrame:
    """
    BigQueryからtraining_dataを取得する

    Args:
        project_id: GCPプロジェクトID
        dataset: データセット名
        table: テーブル名

    Returns:
        全データのDataFrame
    """
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT *
    FROM `{project_id}.{dataset}.{table}`
    ORDER BY race_date, race_id, horse_number
    """
    logger.info(f"Fetching data from {project_id}.{dataset}.{table}...")
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} rows, {len(df.columns)} columns")
    return df


def split_train_valid_predict(
    df: pd.DataFrame,
    execution_date: datetime.date,
    validation_months: int,
    date_column: str = "race_date",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    時系列分割でデータを学習・検証・推論に分ける

    推論対象: 実行日の週の土曜・日曜
    検証: 推論対象日直前のvalidation_months分
    学習: それ以前の全データ

    Args:
        df: 全データ
        execution_date: 実行日
        validation_months: 検証期間（月数）
        date_column: 日付カラム名

    Returns:
        (train_df, valid_df, predict_df) のタプル
    """
    saturday, sunday = compute_week_boundaries(execution_date)

    # 推論対象: 今週の土日
    predict_mask = df[date_column].isin([saturday, sunday])
    predict_df = df[predict_mask].copy()

    # 推論対象以外のデータ
    remaining = df[~predict_mask].copy()

    # 検証期間の境界を計算
    valid_end = saturday - datetime.timedelta(days=1)
    valid_start = valid_end - pd.DateOffset(months=validation_months)

    valid_mask = (remaining[date_column] >= valid_start.date()) & (
        remaining[date_column] <= valid_end
    )
    valid_df = remaining[valid_mask].copy()
    train_df = remaining[~valid_mask & (remaining[date_column] < valid_start.date())].copy()

    logger.info(
        f"Data split: train={len(train_df)}, valid={len(valid_df)}, "
        f"predict={len(predict_df)}"
    )
    logger.info(
        f"Train period: {train_df[date_column].min()} ~ {train_df[date_column].max()}"
    )
    logger.info(
        f"Valid period: {valid_df[date_column].min()} ~ {valid_df[date_column].max()}"
    )
    if len(predict_df) > 0:
        logger.info(
            f"Predict dates: {predict_df[date_column].unique().tolist()}"
        )
    else:
        logger.info("No prediction target data found for this week's Saturday/Sunday")

    return train_df, valid_df, predict_df


def prepare_features(
    df: pd.DataFrame,
    exclude_columns: list[str],
    categorical_columns: list[str],
) -> tuple[pd.DataFrame, np.ndarray, list[int]]:
    """
    DataFrameから特徴量・ラベル・グループを準備する

    Args:
        df: 入力DataFrame
        exclude_columns: 除外カラムリスト
        categorical_columns: カテゴリカル特徴量リスト

    Returns:
        (X, y, groups) のタプル
        y: 着順ベースのrelevanceスコア（高いほど良い）
    """
    # 特徴量カラムの選定
    feature_cols = [
        c for c in df.columns
        if c not in exclude_columns
    ]

    X = df[feature_cols].copy()

    # カテゴリカル変数をcategory型に変換
    for col in categorical_columns:
        if col in X.columns:
            X[col] = X[col].astype("category")

    # ラベル: 着順を整数のrelevanceスコアに変換
    # LightGBM lambdarankは整数ラベルが必要
    # 1着=3, 2着=2, 3着=1, 4着以下=0
    positions = df["finish_position"].values.astype(int)
    y = np.where(positions == 1, 3,
         np.where(positions == 2, 2,
          np.where(positions == 3, 1, 0)))

    # グループサイズ（各レースの馬数）
    groups = df.groupby("race_id", sort=False).size().tolist()

    return X, y, groups


def evaluate_predictions(
    y_true_positions: np.ndarray,
    y_pred: np.ndarray,
    groups: list[int],
) -> dict:
    """
    予測結果を評価する

    Args:
        y_true_positions: 実際の着順（1, 2, 3, ...）
        y_pred: 予測スコア
        groups: グループサイズ

    Returns:
        評価指標の辞書
    """
    ndcg_scores = []
    recall_scores = []
    start = 0

    for group_size in groups:
        end = start + group_size
        true_pos = y_true_positions[start:end]
        pred = y_pred[start:end]

        # ランキング: 予測スコアの降順でtop3を取得
        top3_pred_idx = np.argsort(pred)[::-1][:3]

        # 実際の3着以内の馬のインデックス
        top3_true_idx = set(np.where(true_pos <= 3)[0])

        # Recall@3: 実際の3着以内の馬のうち、予測top3に含まれる割合
        if len(top3_true_idx) > 0:
            recall = len(set(top3_pred_idx) & top3_true_idx) / len(top3_true_idx)
            recall_scores.append(recall)

        # NDCG@3: relevanceスコアベースで計算
        relevance = np.where(true_pos <= 3, 1.0, 0.0)
        # 予測順でのrelevance
        pred_order = np.argsort(pred)[::-1]
        dcg = sum(
            relevance[pred_order[i]] / np.log2(i + 2)
            for i in range(min(3, group_size))
        )
        # 理想的な順序でのDCG
        ideal_order = np.argsort(relevance)[::-1]
        idcg = sum(
            relevance[ideal_order[i]] / np.log2(i + 2)
            for i in range(min(3, group_size))
        )
        if idcg > 0:
            ndcg_scores.append(dcg / idcg)

        start = end

    return {
        "ndcg@3": float(np.mean(ndcg_scores)) if ndcg_scores else 0.0,
        "recall@3": float(np.mean(recall_scores)) if recall_scores else 0.0,
        "num_races": len(groups),
    }


def upload_model_to_gcs(
    project_id: str,
    local_path: str,
    bucket_suffix: str,
    model_prefix: str,
    execution_date: datetime.date,
) -> str:
    """
    モデルファイルをGCSにアップロードする

    Args:
        project_id: GCPプロジェクトID
        local_path: ローカルのモデルファイルパス
        bucket_suffix: バケット名のサフィックス
        model_prefix: GCS内のプレフィックス
        execution_date: 実行日

    Returns:
        GCSのURI
    """
    bucket_name = f"{project_id}-{bucket_suffix}"
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    date_str = execution_date.strftime("%Y%m%d")
    model_path = Path(local_path)

    # モデルファイルと.meta.jsonの両方をアップロード
    uploaded = []
    for file_path in [model_path, model_path.with_suffix(".meta.json")]:
        if file_path.exists():
            blob_name = f"{model_prefix}/{date_str}/{file_path.name}"
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(str(file_path))
            gcs_uri = f"gs://{bucket_name}/{blob_name}"
            logger.info(f"Uploaded {file_path.name} to {gcs_uri}")
            uploaded.append(gcs_uri)

    return uploaded[0] if uploaded else ""


def train_pipeline(
    project_id: str,
    execution_date: datetime.date,
    config: dict,
    output_dir: Optional[str] = None,
    skip_gcs_upload: bool = False,
) -> dict:
    """
    学習パイプラインを実行する

    Args:
        project_id: GCPプロジェクトID
        execution_date: 実行日
        config: 設定辞書
        output_dir: モデル出力ディレクトリ（Noneの場合は一時ディレクトリ）
        skip_gcs_upload: GCSアップロードをスキップするか

    Returns:
        学習結果の辞書
    """
    data_config = config["data"]
    model_config = config["model"]
    gcs_config = config["gcs"]

    # 1. データ取得
    df = fetch_training_data(
        project_id=project_id,
        dataset=data_config["dataset"],
        table=data_config["table"],
    )

    # 2. データ分割
    train_df, valid_df, predict_df = split_train_valid_predict(
        df=df,
        execution_date=execution_date,
        validation_months=model_config["training"]["validation_months"],
        date_column=data_config["date_column"],
    )

    if len(train_df) == 0:
        raise ValueError("学習データがありません")
    if len(valid_df) == 0:
        raise ValueError("検証データがありません")

    # 3. 特徴量準備
    X_train, y_train, groups_train = prepare_features(
        train_df,
        exclude_columns=data_config["exclude_columns"],
        categorical_columns=data_config.get("categorical_columns", []),
    )
    X_valid, y_valid, groups_valid = prepare_features(
        valid_df,
        exclude_columns=data_config["exclude_columns"],
        categorical_columns=data_config.get("categorical_columns", []),
    )

    # 4. モデル学習
    ranker_config = LGBMRankerConfig(
        params=model_config["params"],
        num_boost_round=model_config["training"]["num_boost_round"],
        early_stopping_rounds=model_config["training"]["early_stopping_rounds"],
        log_evaluation=model_config["training"]["log_evaluation"],
    )
    ranker = LGBMRanker(config=ranker_config)

    categorical_in_features = [
        c for c in data_config.get("categorical_columns", [])
        if c in X_train.columns
    ]

    ranker.train(
        X_train=X_train,
        y_train=y_train,
        groups_train=groups_train,
        X_valid=X_valid,
        y_valid=y_valid,
        groups_valid=groups_valid,
        categorical_feature=categorical_in_features or None,
    )

    # 5. 検証データで評価
    valid_pred = ranker.predict(X_valid)
    metrics = evaluate_predictions(
        y_true_positions=valid_df["finish_position"].values,
        y_pred=valid_pred,
        groups=groups_valid,
    )
    logger.info(f"Validation metrics: {metrics}")

    # 6. モデル保存
    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="keiba_model_")

    date_str = execution_date.strftime("%Y%m%d")
    model_path = str(Path(output_dir) / f"lgbm_ranker_{date_str}.txt")
    ranker.save(model_path)

    # 7. GCSアップロード
    gcs_uri = ""
    if not skip_gcs_upload:
        gcs_uri = upload_model_to_gcs(
            project_id=project_id,
            local_path=model_path,
            bucket_suffix=gcs_config["bucket_suffix"],
            model_prefix=gcs_config["model_prefix"],
            execution_date=execution_date,
        )

    # 8. 特徴量重要度
    importance = ranker.feature_importance()
    logger.info(f"Top 10 features:\n{importance.head(10).to_string()}")

    result = {
        "execution_date": str(execution_date),
        "model_path": model_path,
        "gcs_uri": gcs_uri,
        "metrics": metrics,
        "best_iteration": ranker.model.best_iteration,
        "train_rows": len(train_df),
        "valid_rows": len(valid_df),
        "predict_rows": len(predict_df),
        "num_features": X_train.shape[1],
        "top_features": importance.head(10).to_dict(orient="records"),
    }

    return result


def main():
    """メイン関数（CLIから実行）"""
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="LightGBM LambdaRank 学習スクリプト")
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCPプロジェクトID",
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
        "--output-dir",
        default=None,
        help="モデル出力ディレクトリ",
    )
    parser.add_argument(
        "--skip-gcs-upload",
        action="store_true",
        help="GCSアップロードをスキップ",
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

    result = train_pipeline(
        project_id=args.project_id,
        execution_date=execution_date,
        config=config,
        output_dir=args.output_dir,
        skip_gcs_upload=args.skip_gcs_upload,
    )

    print("\n" + "=" * 60)
    print("学習結果")
    print("=" * 60)
    print(f"実行日: {result['execution_date']}")
    print(f"モデルパス: {result['model_path']}")
    if result['gcs_uri']:
        print(f"GCS URI: {result['gcs_uri']}")
    print(f"Best iteration: {result['best_iteration']}")
    print(f"学習データ: {result['train_rows']} rows")
    print(f"検証データ: {result['valid_rows']} rows")
    print(f"推論対象: {result['predict_rows']} rows")
    print(f"特徴量数: {result['num_features']}")
    print(f"\n評価指標:")
    print(f"  NDCG@3:   {result['metrics']['ndcg@3']:.4f}")
    print(f"  Recall@3: {result['metrics']['recall@3']:.4f}")
    print(f"  レース数: {result['metrics']['num_races']}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    exit(main())
