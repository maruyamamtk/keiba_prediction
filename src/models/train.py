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


import numpy as np
import pandas as pd
import yaml
from google.cloud import bigquery, storage
from sklearn.metrics import roc_auc_score

from src.models.lgbm_ranker import LGBMRanker, LGBMRankerConfig
from src.models.tuning import run_tuning, save_best_params

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "model_config.yaml"


def load_config(config_path: str | None = None) -> dict:
    """設定ファイルを読み込む"""
    path = Path(config_path) if config_path else CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"設定ファイルが見つかりません: {path}")
    with open(path) as f:
        return yaml.safe_load(f)


def compute_week_boundaries(execution_date: datetime.date) -> tuple:
    """
    実行日の週の土曜・日曜を推論対象日として返す

    月〜土に実行: 同じ週の土曜・日曜を返す
    日曜に実行: 前日の土曜・当日の日曜を返す
    （例: 2026/2/15(日)に実行 → 2/14(土), 2/15(日)が推論対象）

    Args:
        execution_date: 実行日

    Returns:
        (saturday, sunday) のタプル
    """
    weekday = execution_date.weekday()  # 月=0, 日=6

    if weekday == 6:  # 日曜日
        saturday = execution_date - datetime.timedelta(days=1)
        sunday = execution_date
    else:
        # 月〜土: 今週の土曜日を計算
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
    BigQueryからtraining_dataを取得し、raw.race_resultsからfinish_positionラベルをJOINして返す

    features.training_data の finish_position 列は全 NULL のため、
    学習ラベルは raw.race_results から直接取得する。
    BigQuery Storage API は文字列列を object 型で返すため、build_feature_matrix で
    数値型・categorical_columns 以外の列は自動除外される。

    Args:
        project_id: GCPプロジェクトID
        dataset: データセット名
        table: テーブル名

    Returns:
        finish_position ラベル付きの全データDataFrame
    """
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT
        t.* EXCEPT(finish_position),
        r_r.finish_position
    FROM `{project_id}.{dataset}.{table}` AS t
    LEFT JOIN `{project_id}.raw.race_results` AS r_r
        ON t.race_id = r_r.race_id
        AND t.horse_number = r_r.horse_number
    """
    logger.info(f"Fetching data from {project_id}.{dataset}.{table}...")
    df = client.query(query).to_dataframe()
    df = df.sort_values(["race_date", "race_id", "horse_number"]).reset_index(drop=True)
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

    # 検証期間の境界を計算（datetime.date型で統一）
    valid_end = saturday - datetime.timedelta(days=1)
    # validation_months分前の日付を計算
    valid_start_ts = pd.Timestamp(valid_end) - pd.DateOffset(months=validation_months)
    valid_start = valid_start_ts.date()

    # race_dateカラムの型に依存しないよう、pd.Timestamp経由で比較
    remaining_dates = pd.to_datetime(remaining[date_column])
    valid_mask = (remaining_dates >= pd.Timestamp(valid_start)) & (
        remaining_dates <= pd.Timestamp(valid_end)
    )
    train_mask = remaining_dates < pd.Timestamp(valid_start)
    valid_df = remaining[valid_mask].copy()
    train_df = remaining[train_mask].copy()

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


def build_feature_matrix(
    df: pd.DataFrame,
    exclude_columns: list,
    categorical_columns: list,
) -> pd.DataFrame:
    """
    DataFrameから特徴量行列を構築する（学習・推論共通）

    Args:
        df: 入力DataFrame
        exclude_columns: 除外カラムリスト
        categorical_columns: カテゴリカル特徴量リスト

    Returns:
        特徴量のDataFrame
    """
    feature_cols = [
        c for c in df.columns
        if c not in exclude_columns
        and (
            pd.api.types.is_numeric_dtype(df[c])
            or c in categorical_columns
        )
    ]

    X = df[feature_cols].copy()

    for col in categorical_columns:
        if col in X.columns:
            X[col] = X[col].astype("category")

    return X


def prepare_features(
    df: pd.DataFrame,
    exclude_columns: list,
    categorical_columns: list,
) -> tuple:
    """
    DataFrameから特徴量・ラベル・グループを準備する（学習用）

    Args:
        df: 入力DataFrame
        exclude_columns: 除外カラムリスト
        categorical_columns: カテゴリカル特徴量リスト

    Returns:
        (X, y, groups) のタプル
        y: 二値ラベル（3着以内=1, それ以外=0）
    """
    X = build_feature_matrix(df, exclude_columns, categorical_columns)

    # ラベル: 3着以内=1, それ以外=0 の二値ラベル
    # finish_position=0（出走取消等）またはNULL（欠損）は0として扱う
    # BigQuery Storageモジュール経由だとNullable Int64で返るためfillna(0)が必要
    positions = df["finish_position"].fillna(0).values.astype(int)
    y = np.where((positions >= 1) & (positions <= 3), 1, 0)

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

    # レース横断でのAUC（二値ラベル: 3着以内=1, それ以外=0）
    binary_labels = np.where(
        (y_true_positions >= 1) & (y_true_positions <= 3), 1, 0
    )
    if len(np.unique(binary_labels)) >= 2:
        auc = float(roc_auc_score(binary_labels, y_pred))
    else:
        auc = 0.0

    return {
        "ndcg@3": float(np.mean(ndcg_scores)) if ndcg_scores else 0.0,
        "recall@3": float(np.mean(recall_scores)) if recall_scores else 0.0,
        "auc": auc,
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
    output_dir: str | None = None,
    skip_gcs_upload: bool = False,
    tune: bool = False,
    n_trials: int | None = None,
    tune_timeout: int | None = None,
) -> dict:
    """
    学習パイプラインを実行する

    Args:
        project_id: GCPプロジェクトID
        execution_date: 実行日
        config: 設定辞書
        output_dir: モデル出力ディレクトリ（Noneの場合は一時ディレクトリ）
        skip_gcs_upload: GCSアップロードをスキップするか
        tune: ハイパーパラメータ調整を実行するか
        n_trials: Optuna の trial 数（Noneの場合は config から取得）
        tune_timeout: チューニングのタイムアウト秒数（Noneの場合は config から取得）

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

    categorical_in_features = [
        c for c in data_config.get("categorical_columns", [])
        if c in X_train.columns
    ]

    # 4. ハイパーパラメータ調整（--tune 指定時）
    tuning_result = None
    model_params = model_config["params"]

    if tune:
        # CLI引数でオーバーライド
        tuning_config = dict(config.get("tuning", {}))
        if n_trials is not None:
            tuning_config["n_trials"] = n_trials
        if tune_timeout is not None:
            tuning_config["timeout"] = tune_timeout

        config_for_tuning = {
            "model": model_config,
            "tuning": tuning_config,
        }

        tuning_result = run_tuning(
            X_train=X_train,
            y_train=y_train,
            groups_train=groups_train,
            X_valid=X_valid,
            y_valid=y_valid,
            groups_valid=groups_valid,
            config=config_for_tuning,
            categorical_feature=categorical_in_features or None,
        )
        model_params = tuning_result["best_params"]
        logger.info(f"Using tuned params: {model_params}")

    # 5. モデル学習（チューニング済みまたはデフォルトパラメータ）
    ranker_config = LGBMRankerConfig(
        params=model_params,
        num_boost_round=model_config["training"]["num_boost_round"],
        early_stopping_rounds=model_config["training"]["early_stopping_rounds"],
        log_evaluation=model_config["training"]["log_evaluation"],
    )
    ranker = LGBMRanker(config=ranker_config)

    ranker.train(
        X_train=X_train,
        y_train=y_train,
        groups_train=groups_train,
        X_valid=X_valid,
        y_valid=y_valid,
        groups_valid=groups_valid,
        categorical_feature=categorical_in_features or None,
    )

    # 6. 検証データで評価
    valid_pred = ranker.predict(X_valid)
    metrics = evaluate_predictions(
        y_true_positions=valid_df["finish_position"].fillna(0).values.astype(int),
        y_pred=valid_pred,
        groups=groups_valid,
    )
    logger.info(f"Validation metrics: {metrics}")

    # 7. モデル保存
    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="keiba_model_")

    date_str = execution_date.strftime("%Y%m%d")
    model_path = str(Path(output_dir) / f"lgbm_ranker_{date_str}.txt")
    ranker.save(model_path)

    # チューニング結果のパラメータも保存
    if tuning_result is not None:
        params_path = str(
            Path(output_dir) / f"best_params_{date_str}.json"
        )
        save_best_params(tuning_result["best_params"], params_path)

    # 8. GCSアップロード
    gcs_uri = ""
    if not skip_gcs_upload:
        gcs_uri = upload_model_to_gcs(
            project_id=project_id,
            local_path=model_path,
            bucket_suffix=gcs_config["bucket_suffix"],
            model_prefix=gcs_config["model_prefix"],
            execution_date=execution_date,
        )

    # 9. 特徴量重要度
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

    if tuning_result is not None:
        result["tuning"] = {
            "best_value": tuning_result["best_value"],
            "best_trial_number": tuning_result["best_trial_number"],
            "n_trials": tuning_result["n_trials"],
            "best_params": tuning_result["best_params"],
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
    parser.add_argument(
        "--tune",
        action="store_true",
        help="Optunaによるハイパーパラメータ調整を実行",
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=None,
        help="Optuna の trial 数（デフォルト: config から取得）",
    )
    parser.add_argument(
        "--tune-timeout",
        type=int,
        default=None,
        help="チューニングのタイムアウト秒数（デフォルト: config から取得）",
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
        tune=args.tune,
        n_trials=args.n_trials,
        tune_timeout=args.tune_timeout,
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
    print(f"  AUC:      {result['metrics']['auc']:.4f}")
    print(f"  レース数: {result['metrics']['num_races']}")
    if "tuning" in result:
        print(f"\nチューニング結果:")
        print(f"  Best AUC (tuning): {result['tuning']['best_value']:.4f}")
        print(f"  Trial数: {result['tuning']['n_trials']}")
        print(f"  Best trial: #{result['tuning']['best_trial_number']}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    exit(main())
