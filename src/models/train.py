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

from src.ml.features.feature_pipeline import FeaturePipeline
from src.models.lgbm_classifier import LGBMClassifier, LGBMClassifierConfig
from src.models.lgbm_ranker import LGBMRanker, LGBMRankerConfig
from src.models.lgbm_ranker_multi import JRA_PRIZE_WEIGHTS, LGBMRankerMulti, LGBMRankerMultiConfig
from src.models.lgbm_regression import LGBMRegression, LGBMRegressionConfig
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
    # training_data に finish_position 列が存在しない場合を考慮し、
    # raw.race_results から finish_position を直接 JOIN する
    query = f"""
    SELECT
        t.*,
        r_r.finish_position,
        r_r.finish_time
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


def fetch_training_data_from_sql(
    project_id: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    feature_query_raw.sql を直接実行して学習データを取得する（predict.py と同じ方式）

    features.training_data テーブルへの依存を排除し、SQL変更を即時反映したい場合に使用する。
    SQLと学習データの特徴量セット乖離（Issue #328）を防ぐ。

    Args:
        project_id: GCPプロジェクトID
        start_date: 取得開始日 (YYYY-MM-DD)
        end_date: 取得終了日 (YYYY-MM-DD)

    Returns:
        finish_position ラベル付きのDataFrame
    """
    pipeline = FeaturePipeline(project_id)
    feature_sql = pipeline.generate_query(start_date, end_date)

    client = bigquery.Client(project=project_id)
    logger.info(
        f"Fetching training data via feature_query_raw.sql ({start_date} ~ {end_date})..."
    )
    df = client.query(feature_sql).to_dataframe()
    df = df.sort_values(["race_date", "race_id", "horse_number"]).reset_index(drop=True)
    logger.info(f"Fetched {len(df)} rows, {len(df.columns)} columns from SQL")

    # finish_position / finish_time を raw.race_results から JOIN
    finish_query = f"""
    SELECT race_id, horse_number, finish_position, finish_time
    FROM `{project_id}.raw.race_results`
    WHERE race_date BETWEEN '{start_date}' AND '{end_date}'
    """
    finish_df = client.query(finish_query).to_dataframe()
    df = df.merge(finish_df, on=["race_id", "horse_number"], how="left")
    logger.info(
        f"finish_position JOIN: {df['finish_position'].notna().sum()}/{len(df)} rows with label"
    )
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


def prepare_features_multi_label(
    df: pd.DataFrame,
    exclude_columns: list,
    categorical_columns: list,
) -> tuple:
    """
    DataFrameから特徴量・多値ラベル・グループを準備する（多値ランク学習用）

    JRA賞金ウェイト値をそのまま整数ラベルとして使用する。
    ラベル値: 1着=120, 2着=90, 3着=70, 4着=15, 5着=10,
              6着=8, 7着=7, 8着=6, 9着=4, 10着=2, 11着以下/欠損=0

    LGBMRankerMultiConfig の label_gain[i]=i により、
    各ラベル値がそのままNDCGゲインとして機能する。

    Args:
        df: 入力DataFrame
        exclude_columns: 除外カラムリスト
        categorical_columns: カテゴリカル特徴量リスト

    Returns:
        (X, y, groups) のタプル
        y: 多値ラベル（整数, JRA賞金ウェイト値）
    """
    X = build_feature_matrix(df, exclude_columns, categorical_columns)

    # 着順をJRA賞金ウェイト整数値に直接変換
    # finish_position=0（出走取消等）またはNULL（欠損）および11着以下は0
    positions = df["finish_position"].fillna(0).values.astype(int)
    y = np.array([JRA_PRIZE_WEIGHTS.get(int(p), 0) for p in positions], dtype=int)

    # グループサイズ（各レースの馬数）
    groups = df.groupby("race_id", sort=False).size().tolist()

    return X, y, groups


def compute_time_diff_zscore(df: pd.DataFrame) -> pd.Series:
    """
    1着馬との走破タイム差をレース内σで正規化したZスコアを計算する

    変換ルール:
    - time_diff = finish_time - winner_finish_time（>= 0）
    - zscore = -time_diff / std（値が高いほど強い: 1着馬≒0, 大敗馬＜0）
    - finish_time が NULL の馬は NaN を返す
    - レース内 std == 0（全馬同タイム）のレースは NaN を返す
    - 1着馬が存在しない（finish_time が NULL）レースは NaN を返す

    Args:
        df: race_id / finish_position / finish_time カラムを持つ DataFrame

    Returns:
        各行のZスコア（NaN = 学習から除外すべき行）
    """
    result = pd.Series(np.nan, index=df.index)

    for race_id, group in df.groupby("race_id", sort=False):
        ft = group["finish_time"]
        pos = group["finish_position"].fillna(0).astype(int)

        # 1着馬のfinish_timeを取得
        winner_mask = (pos == 1) & ft.notna()
        if not winner_mask.any():
            continue
        winner_time = ft[winner_mask].iloc[0]

        # time_diff（>= 0）を計算（タイム不明の馬はNaN）
        time_diff = ft - winner_time

        # レース内の有効なtime_diffの標準偏差
        std = time_diff.dropna().std(ddof=0)
        if std == 0 or pd.isna(std):
            continue

        zscore = -time_diff / std
        result.loc[group.index] = zscore

    return result


def prepare_features_regression(
    df: pd.DataFrame,
    exclude_columns: list,
    categorical_columns: list,
) -> tuple:
    """
    DataFrameから特徴量・着差Zスコアラベルを準備する（回帰モデル用）

    finish_time が NULL のレースや std==0 のレースは学習から除外される（NaN行を除去）。
    グループ（レースID）は回帰モデルでは不要なため返さない。

    Args:
        df: finish_time カラムを含む DataFrame
        exclude_columns: 除外カラムリスト
        categorical_columns: カテゴリカル特徴量リスト

    Returns:
        (X, y) のタプル（NaN行除去済み）
    """
    if "finish_time" not in df.columns:
        raise ValueError("DataFrameに 'finish_time' カラムがありません")

    y_all = compute_time_diff_zscore(df)
    valid_mask = y_all.notna()
    df_valid = df[valid_mask].copy()
    y = y_all[valid_mask].values.astype(float)

    X = build_feature_matrix(df_valid, exclude_columns, categorical_columns)

    logger.info(
        f"Regression features: {len(df_valid)}/{len(df)} rows after NaN removal "
        f"({len(df) - len(df_valid)} excluded)"
    )
    return X, y


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
    use_feature_sql: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
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
        use_feature_sql: feature_query_raw.sql を直接実行して学習データを取得する（Issue #328対応）
                         True にすると features.training_data テーブルへの依存を排除できる
        start_date: use_feature_sql=True 時の取得開始日 (YYYY-MM-DD)
        end_date: use_feature_sql=True 時の取得終了日 (YYYY-MM-DD, デフォルト: 実行日)

    Returns:
        学習結果の辞書
    """
    data_config = config["data"]
    model_config = config["model"]
    gcs_config = config["gcs"]

    # 1. データ取得
    if use_feature_sql:
        sql_end = end_date or execution_date.isoformat()
        sql_start = start_date or "2016-01-01"
        df = fetch_training_data_from_sql(
            project_id=project_id,
            start_date=sql_start,
            end_date=sql_end,
        )
    else:
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
    training_period = {
        "train_from": str(train_df[data_config["date_column"]].min()),
        "train_to": str(train_df[data_config["date_column"]].max()),
        "train_rows": len(train_df),
        "train_races": train_df[data_config["group_column"]].nunique(),
        "valid_from": str(valid_df[data_config["date_column"]].min()),
        "valid_to": str(valid_df[data_config["date_column"]].max()),
        "valid_rows": len(valid_df),
        "valid_races": valid_df[data_config["group_column"]].nunique(),
    }
    ranker.save(model_path, training_period=training_period)

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


def train_pipeline_multi(
    project_id: str,
    execution_date: datetime.date,
    config: dict,
    output_dir: str | None = None,
    skip_gcs_upload: bool = False,
    use_feature_sql: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
    tune: bool = False,
    n_trials: int | None = None,
    tune_timeout: int | None = None,
) -> dict:
    """
    多値ランク学習パイプラインを実行する（JRA賞金ウェイト多値ラベル + LambdaRank）

    Args:
        project_id: GCPプロジェクトID
        execution_date: 実行日
        config: 設定辞書
        output_dir: モデル出力ディレクトリ（Noneの場合は一時ディレクトリ）
        skip_gcs_upload: GCSアップロードをスキップするか
        use_feature_sql: feature_query_raw.sql を直接実行して学習データを取得する
        start_date: use_feature_sql=True 時の取得開始日 (YYYY-MM-DD)
        end_date: use_feature_sql=True 時の取得終了日 (YYYY-MM-DD)
        tune: True のとき Optuna でハイパーパラメータ調整を実行
        n_trials: チューニング試行回数（None のとき config 値を使用）
        tune_timeout: チューニングタイムアウト秒数（None のとき config 値を使用）

    Returns:
        学習結果の辞書（model_type="ranker_multi" を含む）
    """
    data_config = config["data"]
    model_config = config["model"]
    gcs_config = config["gcs"]

    # 1. データ取得（train_pipeline と共通）
    if use_feature_sql:
        sql_end = end_date or execution_date.isoformat()
        sql_start = start_date or "2016-01-01"
        df = fetch_training_data_from_sql(
            project_id=project_id,
            start_date=sql_start,
            end_date=sql_end,
        )
    else:
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

    # 3. 特徴量準備（多値ラベル）
    X_train, y_train, groups_train = prepare_features_multi_label(
        train_df,
        exclude_columns=data_config["exclude_columns"],
        categorical_columns=data_config.get("categorical_columns", []),
    )
    X_valid, y_valid, groups_valid = prepare_features_multi_label(
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
        tuning_config = dict(config.get("tuning", {}))
        if n_trials is not None:
            tuning_config["n_trials"] = n_trials
        if tune_timeout is not None:
            tuning_config["timeout"] = tune_timeout

        tuning_result = run_tuning(
            X_train=X_train,
            y_train=y_train,
            X_valid=X_valid,
            y_valid=y_valid,
            config={"model": model_config, "tuning": tuning_config},
            model_type="ranker_multi",
            groups_train=groups_train,
            groups_valid=groups_valid,
            categorical_feature=categorical_in_features or None,
        )
        model_params = tuning_result["best_params"]
        logger.info(f"Using tuned params (ranker_multi): {model_params}")

    # 5. モデル学習（LGBMRankerMulti）
    ranker_config = LGBMRankerMultiConfig(
        params=model_params,
        num_boost_round=model_config["training"]["num_boost_round"],
        early_stopping_rounds=model_config["training"]["early_stopping_rounds"],
        log_evaluation=model_config["training"]["log_evaluation"],
    )
    ranker = LGBMRankerMulti(config=ranker_config)

    ranker.train(
        X_train=X_train,
        y_train=y_train,
        groups_train=groups_train,
        X_valid=X_valid,
        y_valid=y_valid,
        groups_valid=groups_valid,
        categorical_feature=categorical_in_features or None,
    )

    # 5. 検証データで評価（評価はbinary視点でも計算）
    valid_pred = ranker.predict(X_valid)
    metrics = evaluate_predictions(
        y_true_positions=valid_df["finish_position"].fillna(0).values.astype(int),
        y_pred=valid_pred,
        groups=groups_valid,
    )
    logger.info(f"Validation metrics (multi-label): {metrics}")

    # 6. モデル保存
    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="keiba_model_multi_")

    date_str = execution_date.strftime("%Y%m%d")
    model_path = str(Path(output_dir) / f"lgbm_ranker_multi_{date_str}.txt")
    training_period = {
        "train_from": str(train_df[data_config["date_column"]].min()),
        "train_to": str(train_df[data_config["date_column"]].max()),
        "train_rows": len(train_df),
        "train_races": train_df[data_config["group_column"]].nunique(),
        "valid_from": str(valid_df[data_config["date_column"]].min()),
        "valid_to": str(valid_df[data_config["date_column"]].max()),
        "valid_rows": len(valid_df),
        "valid_races": valid_df[data_config["group_column"]].nunique(),
    }
    ranker.save(model_path, training_period=training_period)

    if tuning_result is not None:
        params_path = str(Path(output_dir) / f"best_params_ranker_multi_{date_str}.json")
        save_best_params(tuning_result["best_params"], params_path)

    # 7. GCSアップロード（プレフィックスはranker_multiに固定）
    gcs_uri = ""
    if not skip_gcs_upload:
        gcs_uri = upload_model_to_gcs(
            project_id=project_id,
            local_path=model_path,
            bucket_suffix=gcs_config["bucket_suffix"],
            model_prefix="lgbm_ranker_multi",
            execution_date=execution_date,
        )

    # 8. 特徴量重要度
    importance = ranker.feature_importance()
    logger.info(f"Top 10 features:\n{importance.head(10).to_string()}")

    result = {
        "model_type": "ranker_multi",
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


def train_pipeline_regression(
    project_id: str,
    execution_date: datetime.date,
    config: dict,
    output_dir: str | None = None,
    skip_gcs_upload: bool = False,
    use_feature_sql: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
    tune: bool = False,
    n_trials: int | None = None,
    tune_timeout: int | None = None,
) -> dict:
    """
    着差回帰モデルの学習パイプラインを実行する

    目的変数: 1着馬との走破タイム差をレース内σで正規化したZスコア
    finish_time が NULL、またはレース内 std==0 の行は学習から除外される。

    Args:
        project_id: GCPプロジェクトID
        execution_date: 実行日
        config: 設定辞書
        output_dir: モデル出力ディレクトリ（Noneの場合は一時ディレクトリ）
        skip_gcs_upload: GCSアップロードをスキップするか
        use_feature_sql: feature_query_raw.sql を直接実行して学習データを取得する
        start_date: use_feature_sql=True 時の取得開始日 (YYYY-MM-DD)
        end_date: use_feature_sql=True 時の取得終了日 (YYYY-MM-DD)
        tune: True のとき Optuna でハイパーパラメータ調整を実行
        n_trials: チューニング試行回数（None のとき config 値を使用）
        tune_timeout: チューニングタイムアウト秒数（None のとき config 値を使用）

    Returns:
        学習結果の辞書（model_type="regression" を含む）
    """
    data_config = config["data"]
    model_config = config["model"]
    gcs_config = config["gcs"]

    # 1. データ取得（finish_time 付き）
    if use_feature_sql:
        sql_end = end_date or execution_date.isoformat()
        sql_start = start_date or "2016-01-01"
        df = fetch_training_data_from_sql(
            project_id=project_id,
            start_date=sql_start,
            end_date=sql_end,
        )
    else:
        df = fetch_training_data(
            project_id=project_id,
            dataset=data_config["dataset"],
            table=data_config["table"],
        )

    if "finish_time" not in df.columns:
        raise ValueError(
            "finish_time カラムが取得できませんでした。"
            "fetch_training_data() が finish_time を JOIN していることを確認してください。"
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

    # 3. 特徴量準備（NaN行は自動除外）
    X_train, y_train = prepare_features_regression(
        train_df,
        exclude_columns=data_config["exclude_columns"],
        categorical_columns=data_config.get("categorical_columns", []),
    )
    X_valid, y_valid = prepare_features_regression(
        valid_df,
        exclude_columns=data_config["exclude_columns"],
        categorical_columns=data_config.get("categorical_columns", []),
    )

    if len(X_train) == 0:
        raise ValueError("有効な学習データがありません（finish_time が全てNULL等）")
    if len(X_valid) == 0:
        raise ValueError("有効な検証データがありません")

    categorical_in_features = [
        c for c in data_config.get("categorical_columns", [])
        if c in X_train.columns
    ]

    # 4. ハイパーパラメータ調整（--tune 指定時）
    tuning_result = None
    _ranker_only_keys = {"objective", "metric", "ndcg_eval_at", "label_gain", "lambdarank_truncation_level"}
    _shared_params = {k: v for k, v in model_config.get("params", {}).items() if k not in _ranker_only_keys}
    reg_base_params = {**LGBMRegressionConfig().params, **_shared_params}

    if tune:
        tuning_config = dict(config.get("tuning", {}))
        if n_trials is not None:
            tuning_config["n_trials"] = n_trials
        if tune_timeout is not None:
            tuning_config["timeout"] = tune_timeout

        tuning_result = run_tuning(
            X_train=X_train,
            y_train=y_train,
            X_valid=X_valid,
            y_valid=y_valid,
            config={"model": {**model_config, "params": reg_base_params}, "tuning": tuning_config},
            model_type="regression",
            categorical_feature=categorical_in_features or None,
        )
        reg_base_params = tuning_result["best_params"]
        logger.info(f"Using tuned params (regression): {reg_base_params}")

    # 5. モデル学習
    reg_config = LGBMRegressionConfig(
        params=reg_base_params,
        num_boost_round=model_config["training"]["num_boost_round"],
        early_stopping_rounds=model_config["training"]["early_stopping_rounds"],
        log_evaluation=model_config["training"]["log_evaluation"],
    )
    regressor = LGBMRegression(config=reg_config)

    regressor.train(
        X_train=X_train,
        y_train=y_train,
        X_valid=X_valid,
        y_valid=y_valid,
        categorical_feature=categorical_in_features or None,
    )

    # 6. 検証データで RMSE 評価
    valid_pred = regressor.predict(X_valid)
    rmse = float(np.sqrt(np.mean((y_valid - valid_pred) ** 2)))
    logger.info(f"Regression validation RMSE: {rmse:.4f}")

    # 6. モデル保存
    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="keiba_model_regression_")

    date_str = execution_date.strftime("%Y%m%d")
    model_path = str(Path(output_dir) / f"lgbm_regression_{date_str}.txt")
    training_period = {
        "train_from": str(train_df[data_config["date_column"]].min()),
        "train_to": str(train_df[data_config["date_column"]].max()),
        "train_rows": len(X_train),
        "valid_from": str(valid_df[data_config["date_column"]].min()),
        "valid_to": str(valid_df[data_config["date_column"]].max()),
        "valid_rows": len(X_valid),
    }
    regressor.save(model_path, training_period=training_period)

    if tuning_result is not None:
        params_path = str(Path(output_dir) / f"best_params_regression_{date_str}.json")
        save_best_params(tuning_result["best_params"], params_path)

    # 7. GCSアップロード（プレフィックスは lgbm_regression に固定）
    gcs_uri = ""
    if not skip_gcs_upload:
        gcs_uri = upload_model_to_gcs(
            project_id=project_id,
            local_path=model_path,
            bucket_suffix=gcs_config["bucket_suffix"],
            model_prefix="lgbm_regression",
            execution_date=execution_date,
        )

    # 8. 特徴量重要度
    importance = regressor.feature_importance()
    logger.info(f"Top 10 features:\n{importance.head(10).to_string()}")

    result = {
        "model_type": "regression",
        "execution_date": str(execution_date),
        "model_path": model_path,
        "gcs_uri": gcs_uri,
        "metrics": {"rmse": rmse},
        "best_iteration": regressor.model.best_iteration,
        "train_rows": len(X_train),
        "valid_rows": len(X_valid),
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


def train_pipeline_classifier(
    project_id: str,
    execution_date: datetime.date,
    config: dict,
    output_dir: str | None = None,
    skip_gcs_upload: bool = False,
    use_feature_sql: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
    tune: bool = False,
    n_trials: int | None = None,
    tune_timeout: int | None = None,
) -> dict:
    """
    二値分類モデルの学習パイプラインを実行する

    objective=binary で複勝率を直接推定する。
    predict() が [0,1] の確率を返すため期待値計算のキャリブレーション入力として使用する。
    グループ（レースID）は不要。

    Args:
        project_id: GCPプロジェクトID
        execution_date: 実行日
        config: 設定辞書
        output_dir: モデル出力ディレクトリ（Noneの場合は一時ディレクトリ）
        skip_gcs_upload: GCSアップロードをスキップするか
        use_feature_sql: feature_query_raw.sql を直接実行して学習データを取得する
        start_date: use_feature_sql=True 時の取得開始日 (YYYY-MM-DD)
        end_date: use_feature_sql=True 時の取得終了日 (YYYY-MM-DD)
        tune: True のとき Optuna でハイパーパラメータ調整を実行
        n_trials: チューニング試行回数（None のとき config 値を使用）
        tune_timeout: チューニングタイムアウト秒数（None のとき config 値を使用）

    Returns:
        学習結果の辞書（model_type="classifier" を含む）
    """
    data_config = config["data"]
    model_config = config["model"]
    gcs_config = config["gcs"]

    # 1. データ取得
    if use_feature_sql:
        sql_end = end_date or execution_date.isoformat()
        sql_start = start_date or "2016-01-01"
        df = fetch_training_data_from_sql(
            project_id=project_id,
            start_date=sql_start,
            end_date=sql_end,
        )
    else:
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

    # 3. 特徴量準備（二値ラベル、グループなし）
    X_train, y_train, _ = prepare_features(
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

    # 4. classifier 固有の base params を構築（ranker 固有キーを除外して共通パラメータを引き継ぐ）
    _ranker_only_keys = {"objective", "metric", "ndcg_eval_at", "label_gain", "lambdarank_truncation_level"}
    _shared_params = {
        k: v for k, v in model_config.get("params", {}).items()
        if k not in _ranker_only_keys
    }
    clf_base_params = {**LGBMClassifierConfig().params, **_shared_params}

    # 4a. ハイパーパラメータ調整（--tune 指定時）
    tuning_result = None
    if tune:
        tuning_config = dict(config.get("tuning", {}))
        if n_trials is not None:
            tuning_config["n_trials"] = n_trials
        if tune_timeout is not None:
            tuning_config["timeout"] = tune_timeout

        tuning_result = run_tuning(
            X_train=X_train,
            y_train=y_train,
            X_valid=X_valid,
            y_valid=y_valid,
            config={"model": {**model_config, "params": clf_base_params}, "tuning": tuning_config},
            model_type="classifier",
            categorical_feature=categorical_in_features or None,
        )
        clf_base_params = tuning_result["best_params"]
        logger.info(f"Using tuned params (classifier): {clf_base_params}")

    # 4b. モデル学習
    clf_config = LGBMClassifierConfig(
        params=clf_base_params,
        num_boost_round=model_config["training"]["num_boost_round"],
        early_stopping_rounds=model_config["training"]["early_stopping_rounds"],
        log_evaluation=model_config["training"]["log_evaluation"],
    )
    classifier = LGBMClassifier(config=clf_config)

    classifier.train(
        X_train=X_train,
        y_train=y_train,
        X_valid=X_valid,
        y_valid=y_valid,
        categorical_feature=categorical_in_features or None,
    )

    # 5. 検証データで AUC 評価
    valid_pred = classifier.predict(X_valid)
    metrics = evaluate_predictions(
        y_true_positions=valid_df["finish_position"].fillna(0).values.astype(int),
        y_pred=valid_pred,
        groups=groups_valid,
    )
    logger.info(f"Classifier validation metrics: {metrics}")

    # 6. モデル保存
    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="keiba_model_classifier_")

    date_str = execution_date.strftime("%Y%m%d")
    model_path = str(Path(output_dir) / f"lgbm_classifier_{date_str}.txt")
    training_period = {
        "train_from": str(train_df[data_config["date_column"]].min()),
        "train_to": str(train_df[data_config["date_column"]].max()),
        "train_rows": len(train_df),
        "train_races": train_df[data_config["group_column"]].nunique(),
        "valid_from": str(valid_df[data_config["date_column"]].min()),
        "valid_to": str(valid_df[data_config["date_column"]].max()),
        "valid_rows": len(valid_df),
        "valid_races": valid_df[data_config["group_column"]].nunique(),
    }
    classifier.save(model_path, training_period=training_period)

    if tuning_result is not None:
        params_path = str(Path(output_dir) / f"best_params_classifier_{date_str}.json")
        save_best_params(tuning_result["best_params"], params_path)

    # 7. GCSアップロード（プレフィックスは lgbm_classifier に固定）
    gcs_uri = ""
    if not skip_gcs_upload:
        gcs_uri = upload_model_to_gcs(
            project_id=project_id,
            local_path=model_path,
            bucket_suffix=gcs_config["bucket_suffix"],
            model_prefix="lgbm_classifier",
            execution_date=execution_date,
        )

    # 8. 特徴量重要度
    importance = classifier.feature_importance()
    logger.info(f"Top 10 features:\n{importance.head(10).to_string()}")

    result = {
        "model_type": "classifier",
        "execution_date": str(execution_date),
        "model_path": model_path,
        "gcs_uri": gcs_uri,
        "metrics": metrics,
        "best_iteration": classifier.model.best_iteration,
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
    parser.add_argument(
        "--use-feature-sql",
        action="store_true",
        help=(
            "features.training_data テーブルの代わりに feature_query_raw.sql を直接実行して"
            "学習データを取得する（Issue #328: SQLと学習データの特徴量セット乖離を防ぐ）"
        ),
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="--use-feature-sql 時の取得開始日 (YYYY-MM-DD, デフォルト: 2016-01-01)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="--use-feature-sql 時の取得終了日 (YYYY-MM-DD, デフォルト: 実行日)",
    )
    parser.add_argument(
        "--model-type",
        default="ranker",
        choices=["ranker", "multi", "regression", "classifier"],
        help=(
            "学習するモデルの種類。"
            "ranker: 二値ラベル LambdaRank（デフォルト）, "
            "multi: JRA賞金ウェイト多値ラベル LambdaRank, "
            "regression: 着差Zスコア回帰, "
            "classifier: 二値分類（複勝率直接予測）"
        ),
    )

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

    if args.model_type == "multi":
        result = train_pipeline_multi(
            project_id=args.project_id,
            execution_date=execution_date,
            config=config,
            output_dir=args.output_dir,
            skip_gcs_upload=args.skip_gcs_upload,
            use_feature_sql=args.use_feature_sql,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    elif args.model_type == "regression":
        result = train_pipeline_regression(
            project_id=args.project_id,
            execution_date=execution_date,
            config=config,
            output_dir=args.output_dir,
            skip_gcs_upload=args.skip_gcs_upload,
            use_feature_sql=args.use_feature_sql,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    elif args.model_type == "classifier":
        result = train_pipeline_classifier(
            project_id=args.project_id,
            execution_date=execution_date,
            config=config,
            output_dir=args.output_dir,
            skip_gcs_upload=args.skip_gcs_upload,
            use_feature_sql=args.use_feature_sql,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    else:
        result = train_pipeline(
            project_id=args.project_id,
            execution_date=execution_date,
            config=config,
            output_dir=args.output_dir,
            skip_gcs_upload=args.skip_gcs_upload,
            tune=args.tune,
            n_trials=args.n_trials,
            tune_timeout=args.tune_timeout,
            use_feature_sql=args.use_feature_sql,
            start_date=args.start_date,
            end_date=args.end_date,
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
