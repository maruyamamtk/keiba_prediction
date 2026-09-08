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
from src.models.calibration import fit_calibration_isotonic, fit_calibration_temperature
from src.models.lgbm_ranker_multi import JRA_PRIZE_WEIGHTS, LGBMRankerMulti, LGBMRankerMultiConfig
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
    # raw.race_results から finish_position を直接 JOIN する。
    # finish_time（当該レース走破タイム）は回帰モデルの目的変数用に取得するが、
    # レース後にのみ確定するリーク特徴量のため exclude_columns で説明変数から除外する（Issue #402）。
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


def compute_validation_boundaries(
    execution_date: datetime.date,
    validation_months: int,
    test_days: int = 0,
) -> dict:
    """検証期間・test期間の境界日付を計算する（純粋な日付計算のみ、データ非依存）

    split_train_valid_predict/split_train_valid_test_predict の内部計算と、
    monthly_retrain.py の --dry-run 表示が同じロジックを共有するための
    唯一の実装（Issue #430）。

    3分割の考え方: train（validより前の全期間）→ valid（Early Stopping・
    ハイパーパラメータ選定用）→ test（train+validどちらにも一切使わない、
    真に未見のデータ。モデルの汎化性能評価と戦略パラメータ最適化の両方に使う）
    → predict（今週末の予測対象）。test_days=0 の場合は従来通りvalidが
    実行日直前まで伸びる2分割（train/valid）になる。

    Args:
        execution_date: 実行日
        validation_months: 検証期間（月数）
        test_days: test期間の日数。検証期間の終端を実行日からこの日数分
            手前にずらすことで、その後ろにtest期間を確保する

    Returns:
        dict: valid_start, valid_end を必ず含む。test_days > 0 の場合は
              test_start, test_end も含む（すべて datetime.date）
    """
    saturday, _ = compute_week_boundaries(execution_date)
    valid_end = saturday - datetime.timedelta(days=1 + test_days)
    valid_start = (
        pd.Timestamp(valid_end) - pd.DateOffset(months=validation_months)
    ).date()
    result = {"valid_start": valid_start, "valid_end": valid_end}
    if test_days > 0:
        result["test_start"] = valid_end + datetime.timedelta(days=1)
        result["test_end"] = saturday - datetime.timedelta(days=1)
    return result


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

    3分割（train/valid/test）が必要な場合は split_train_valid_test_predict を使うこと。

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

    boundaries = compute_validation_boundaries(execution_date, validation_months)
    valid_end = boundaries["valid_end"]
    valid_start = boundaries["valid_start"]

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


def split_train_valid_test_predict(
    df: pd.DataFrame,
    execution_date: datetime.date,
    validation_months: int,
    test_days: int,
    date_column: str = "race_date",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    時系列3分割（train/valid/test）+ 推論対象データに分ける（Issue #430）

    train（それ以前の全期間）→ valid（Early Stopping・ハイパーパラメータ選定用）
    → test（train・validのどちらにも一切使われない、真に未見のデータ。
    train+valid結合データで再学習した最終モデルの汎化性能評価と、戦略パラメータ
    最適化の両方に安全に使い回せる）→ predict（今週末の予測対象）の順に並ぶ。

    Args:
        df: 全データ
        execution_date: 実行日
        validation_months: 検証期間（月数）
        test_days: test期間の日数（1以上を指定すること）
        date_column: 日付カラム名

    Returns:
        (train_df, valid_df, test_df, predict_df) のタプル
    """
    if test_days <= 0:
        raise ValueError("test_days は1以上を指定してください（0の場合は split_train_valid_predict を使うこと）")

    saturday, sunday = compute_week_boundaries(execution_date)

    predict_mask = df[date_column].isin([saturday, sunday])
    predict_df = df[predict_mask].copy()
    remaining = df[~predict_mask].copy()

    boundaries = compute_validation_boundaries(execution_date, validation_months, test_days)
    valid_start, valid_end = boundaries["valid_start"], boundaries["valid_end"]
    test_start, test_end = boundaries["test_start"], boundaries["test_end"]

    remaining_dates = pd.to_datetime(remaining[date_column])
    train_mask = remaining_dates < pd.Timestamp(valid_start)
    valid_mask = (remaining_dates >= pd.Timestamp(valid_start)) & (
        remaining_dates <= pd.Timestamp(valid_end)
    )
    test_mask = (remaining_dates >= pd.Timestamp(test_start)) & (
        remaining_dates <= pd.Timestamp(test_end)
    )

    train_df = remaining[train_mask].copy()
    valid_df = remaining[valid_mask].copy()
    test_df = remaining[test_mask].copy()

    logger.info(
        f"Data split (train/valid/test): train={len(train_df)}, valid={len(valid_df)}, "
        f"test={len(test_df)}, predict={len(predict_df)}"
    )
    if len(train_df) > 0:
        logger.info(f"Train period: {train_df[date_column].min()} ~ {train_df[date_column].max()}")
    if len(valid_df) > 0:
        logger.info(f"Valid period: {valid_df[date_column].min()} ~ {valid_df[date_column].max()}")
    if len(test_df) > 0:
        logger.info(f"Test period: {test_df[date_column].min()} ~ {test_df[date_column].max()}")
    if len(predict_df) > 0:
        logger.info(f"Predict dates: {predict_df[date_column].unique().tolist()}")
    else:
        logger.info("No prediction target data found for this week's Saturday/Sunday")

    return train_df, valid_df, test_df, predict_df


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
    use_feature_sql: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
    tune: bool = False,
    n_trials: int | None = None,
    tune_timeout: int | None = None,
    test_days: int = 0,
) -> dict:
    """
    多値ランク学習パイプラインを実行する（JRA賞金ウェイト多値ラベル + LambdaRank）

    test_days=0（デフォルト）の場合は従来通りtrain/valid/predictの2分割。
    train+validでハイパーパラメータ調整・Early Stoppingを行い、validの成績を
    そのままモデル品質指標として報告する。

    test_days>0の場合はIssue #430の3分割ワークフローになる:
      1. train/valid/testに3分割する（test_days日ぶんをtestとして確保）
      2. train+validでハイパーパラメータ調整・Early Stopping（従来通り）
      3. train+validを結合し、Early Stoppingで決まったラウンド数固定でリフィット
         （test期間には一切触れない）
      4. リフィットしたモデルをtest（真に未見データ）で評価し、これを
         報告するモデル品質指標・キャリブレーション基準とする
    こうすることで、モデル品質指標（NDCG@3等）とキャリブレーション自体も
    ハイパーパラメータ選定に使ったのと同じデータで評価するバイアスを避けられる。
    さらにtest期間はモデルの学習・選定に一切使われていないため、
    戦略パラメータ最適化・ホールドアウト検証にそのまま安全に使い回せる。

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
        test_days: test期間の日数。0（デフォルト）ならtrain/valid2分割。
            1以上ならtrain/valid/test3分割＋リフィットワークフローを実行し、
            戻り値の training_period に test_from/test_to を含める。

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
    test_df: pd.DataFrame | None = None
    if test_days > 0:
        train_df, valid_df, test_df, predict_df = split_train_valid_test_predict(
            df=df,
            execution_date=execution_date,
            validation_months=model_config["training"]["validation_months"],
            test_days=test_days,
            date_column=data_config["date_column"],
        )
    else:
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
    if test_days > 0 and (test_df is None or len(test_df) == 0):
        raise ValueError("test期間のデータがありません")

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
    # LGBMRankerMultiConfig の label_gain（0〜120）を YAML params にマージ
    # model_config.yaml には label_gain がないため、チューニング時に渡らず LightGBM がデフォルト31要素で動いてしまう問題を防ぐ
    multi_default_params = LGBMRankerMultiConfig().params
    merged_model_config = dict(model_config)
    merged_model_config["params"] = {**multi_default_params, **model_config["params"]}
    model_params = merged_model_config["params"]

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
            config={"model": merged_model_config, "tuning": tuning_config},
            model_type="ranker_multi",
            groups_train=groups_train,
            groups_valid=groups_valid,
            categorical_feature=categorical_in_features or None,
        )
        model_params = tuning_result["best_params"]
        logger.info(f"Using tuned params (ranker_multi): {model_params}")

    # 5. モデル学習（LGBMRankerMulti、train+validでEarly Stopping・ハイパラ調整）
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

    if test_days > 0:
        # 5b. リフィット（Issue #430）: train+validを結合し、上記Early Stoppingで
        # 決まったラウンド数に固定して再学習する。test期間には一切触れないため、
        # 最終モデルはtrain+validの全データを活用しつつ、testでの評価は完全にクリーンなまま。
        best_num_boost_round = max(int(ranker.model.best_iteration), 1)
        logger.info(
            f"リフィット開始: train+valid結合={len(train_df) + len(valid_df)}行, "
            f"num_boost_round={best_num_boost_round}固定（Early Stoppingなし）"
        )
        combined_df = pd.concat([train_df, valid_df], ignore_index=True)
        X_combined, y_combined, groups_combined = prepare_features_multi_label(
            combined_df,
            exclude_columns=data_config["exclude_columns"],
            categorical_columns=data_config.get("categorical_columns", []),
        )
        refit_config = LGBMRankerMultiConfig(
            params=model_params,
            num_boost_round=best_num_boost_round,
            early_stopping_rounds=model_config["training"]["early_stopping_rounds"],
            log_evaluation=model_config["training"]["log_evaluation"],
        )
        final_ranker = LGBMRankerMulti(config=refit_config)
        final_ranker.train(
            X_train=X_combined,
            y_train=y_combined,
            groups_train=groups_combined,
            categorical_feature=categorical_in_features or None,
        )

        # 5c. test（train・validのどちらにも一切使われていない真に未見データ）で評価する。
        # これが報告するモデル品質指標・キャリブレーション基準になる。
        X_eval, y_eval, groups_eval = prepare_features_multi_label(
            test_df,
            exclude_columns=data_config["exclude_columns"],
            categorical_columns=data_config.get("categorical_columns", []),
        )
        eval_pred = final_ranker.predict(X_eval)
        eval_df = test_df
        ranker = final_ranker  # 保存・デプロイするのはリフィット後のモデル
    else:
        eval_pred = ranker.predict(X_valid)
        eval_df = valid_df
        groups_eval = groups_valid

    metrics = evaluate_predictions(
        y_true_positions=eval_df["finish_position"].fillna(0).values.astype(int),
        y_pred=eval_pred,
        groups=groups_eval,
    )
    logger.info(f"{'Test' if test_days > 0 else 'Validation'} metrics (multi-label): {metrics}")

    # 5d. キャリブレーション温度・アイソトニック校正器のフィット（Issue #414/#416）
    # test_days>0時はtest（真に未見）、そうでなければvalid（従来通り）上でフィットする。
    # 出走取消・結果なし馬（finish_position<=0）は実績が無いため除外し、
    # scripts/evaluate_calibration.py の評価方法（raced horses のみ）と一致させる。
    eval_positions = eval_df["finish_position"].fillna(0).values.astype(int)
    calib_df = pd.DataFrame(
        {
            "race_id": eval_df[data_config["group_column"]].values,
            "pred_score": eval_pred,
            "finish_position": eval_positions,
        }
    )
    calib_df = calib_df[calib_df["finish_position"] > 0].copy()
    calib_df["is_place"] = (calib_df["finish_position"] <= 3).astype(int)
    calibration_isotonic = fit_calibration_isotonic(calib_df)
    calibration_temperature = fit_calibration_temperature(calib_df)
    logger.info(
        f"Calibration temperature: {calibration_temperature:.4f}, "
        f"isotonic thresholds: {len(calibration_isotonic['x_thresholds'])}"
    )

    # 6. モデル保存
    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="keiba_model_multi_")

    date_str = execution_date.strftime("%Y%m%d")
    model_path = str(Path(output_dir) / f"lgbm_ranker_multi_{date_str}.txt")
    training_period = {
        "train_from": pd.Timestamp(train_df[data_config["date_column"]].min()).date().isoformat(),
        "train_to": pd.Timestamp(train_df[data_config["date_column"]].max()).date().isoformat(),
        "train_rows": len(train_df),
        "train_races": train_df[data_config["group_column"]].nunique(),
        "valid_from": pd.Timestamp(valid_df[data_config["date_column"]].min()).date().isoformat(),
        "valid_to": pd.Timestamp(valid_df[data_config["date_column"]].max()).date().isoformat(),
        "valid_rows": len(valid_df),
        "valid_races": valid_df[data_config["group_column"]].nunique(),
    }
    if test_days > 0:
        # train・validのどちらにも一切使われていない真に未見の期間（Issue #430）。
        # モデルの汎化性能評価・戦略パラメータ最適化・ホールドアウト検証に使う。
        training_period["test_from"] = pd.Timestamp(
            test_df[data_config["date_column"]].min()
        ).date().isoformat()
        training_period["test_to"] = pd.Timestamp(
            test_df[data_config["date_column"]].max()
        ).date().isoformat()
        training_period["test_rows"] = len(test_df)
        training_period["test_races"] = test_df[data_config["group_column"]].nunique()
    ranker.calibration_temperature = calibration_temperature
    ranker.calibration_isotonic = calibration_isotonic
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
        "training_period": training_period,
        "calibration_temperature": calibration_temperature,
        "calibration_isotonic_points": len(calibration_isotonic["x_thresholds"]),
        # リフィットモデル（test_days>0）はEarly Stoppingを使わないためbest_iteration=0に
        # なる（意味を持たない）。実際に使われたラウンド数はnum_trees()で取得する。
        "best_iteration": (
            ranker.model.num_trees() if test_days > 0 else ranker.model.best_iteration
        ),
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
    print(f"校正温度: {result['calibration_temperature']:.4f}")
    print(f"アイソトニック校正: {result['calibration_isotonic_points']}点")
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
