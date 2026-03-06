"""
LightGBM LambdaRank 推論スクリプト

学習済みモデルを使用して、今週の土曜・日曜のレースに対する
着順予測を行い、結果を出力する。

Usage:
    python src/models/predict.py --project-id <PROJECT_ID> --model-path <MODEL_PATH>
    python src/models/predict.py --project-id <PROJECT_ID> --model-path <MODEL_PATH> --execution-date 2026-02-15
    python src/models/predict.py --project-id <PROJECT_ID> --model-path <MODEL_PATH> --save-to-bq
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


def fetch_race_results(
    project_id: str,
    target_dates: list[datetime.date],
) -> pd.DataFrame:
    """
    raw.race_results から対象日の着順データを取得する

    レース終了後の実際の着順確認に使用する。
    レースが未実施の場合（未来のレース）は空のDataFrameを返す。

    Args:
        project_id: GCPプロジェクトID
        target_dates: 対象日のリスト

    Returns:
        着順データのDataFrame（race_id, horse_id, finish_position）
    """
    client = bigquery.Client(project=project_id)
    dates_str = ", ".join(f"'{d.isoformat()}'" for d in target_dates)
    query = f"""
    SELECT race_id, horse_id, finish_position
    FROM `{project_id}.raw.race_results`
    WHERE race_date IN ({dates_str})
    """
    logger.info(f"Fetching race results for dates: {target_dates}")
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} race result rows")
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
    target_dates: Optional[list[datetime.date]] = None,
) -> pd.DataFrame:
    """
    推論パイプラインを実行する

    Args:
        project_id: GCPプロジェクトID
        execution_date: 実行日（target_dates未指定時に週の土日を算出する基準日）
        config: 設定辞書
        model_path: モデルファイルパス
        target_dates: 推論対象日のリスト。指定した場合はその日付のみ対象とする。
                      未指定の場合は execution_date の週の土曜・日曜を使用する。

    Returns:
        予測結果のDataFrame
    """
    data_config = config["data"]

    # 1. モデル読み込み
    ranker = LGBMRanker()
    ranker.load(model_path)

    # 2. 推論対象日の決定
    if target_dates is None:
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

    # 着順情報: raw.race_resultsから直接取得する
    # features.training_dataのfinish_positionは不正な値（0/1等）が格納されている
    # 場合があるため、信頼性の高いデータソースを使用する
    race_results_df = fetch_race_results(project_id=project_id, target_dates=target_dates)
    if len(race_results_df) > 0:
        result_df = result_df.merge(
            race_results_df[["race_id", "horse_id", "finish_position"]],
            on=["race_id", "horse_id"],
            how="left",
        )
    else:
        result_df["finish_position"] = np.nan

    # オッズ情報がある場合
    for odds_col in ["odds_yesterday", "odds_today"]:
        if odds_col in df.columns:
            result_df[odds_col] = df[odds_col]

    # ソート
    result_df = result_df.sort_values(
        ["race_date", "race_id", "pred_rank"]
    ).reset_index(drop=True)

    return result_df


def save_predictions_to_bq(
    result_df: pd.DataFrame,
    project_id: str,
    dataset: str = "predictions",
    table: str = "daily_predictions",
) -> int:
    """
    予測結果をBigQueryのpredictions.daily_predictionsテーブルにUPSERTする

    保存するカラム: race_id, race_date, horse_id, horse_number, horse_name,
                   venue_code, race_number, win_place_prob, pred_score,
                   rank_in_race, place_odds, created_at

    一意キー: (race_id, horse_id) の組み合わせでUPSERT（MERGE）を実行する。

    Args:
        result_df: 予測結果のDataFrame（predict_pipelineの出力）
        project_id: GCPプロジェクトID
        dataset: 保存先データセット名（デフォルト: "predictions"）
        table: 保存先テーブル名（デフォルト: "daily_predictions"）

    Returns:
        保存した行数

    Raises:
        ValueError: 必須カラムが不足している場合
    """
    if len(result_df) == 0:
        logger.warning("保存するデータがありません")
        return 0

    required_columns = ["race_id", "horse_id", "win_place_prob", "pred_score"]
    missing = [c for c in required_columns if c not in result_df.columns]
    if missing:
        raise ValueError(f"必須カラムが不足しています: {missing}")

    # 保存対象カラムを選択（存在するもののみ）
    save_columns = [
        "race_id", "race_date", "horse_id", "horse_number", "horse_name",
        "venue_code", "race_number", "win_place_prob", "pred_score",
    ]
    # place_oddsカラムがあれば含める
    for col in ["place_odds", "odds_yesterday", "odds_today"]:
        if col in result_df.columns:
            save_columns.append(col)
            break

    available_columns = [c for c in save_columns if c in result_df.columns]
    save_df = result_df[available_columns].copy()

    # pred_rank を rank_in_race としてBQスキーマに合わせてリネーム
    if "pred_rank" in result_df.columns:
        save_df["rank_in_race"] = result_df["pred_rank"].astype("Int64")

    # created_atを追加
    save_df["created_at"] = pd.Timestamp.now(tz="UTC")

    # 型の整合
    if "horse_number" in save_df.columns:
        save_df["horse_number"] = save_df["horse_number"].astype("Int64")
    if "race_number" in save_df.columns:
        save_df["race_number"] = save_df["race_number"].astype("Int64")

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"
    temp_table_ref = (
        f"{project_id}.{dataset}._temp_{table}_"
        f"{pd.Timestamp.now(tz='UTC').strftime('%Y%m%d%H%M%S')}"
    )

    logger.info(f"BigQuery保存開始: {table_ref} ({len(save_df)}行)")

    try:
        # 一時テーブルを作成してデータをロード（スキーマはautodetect）
        # 1時間後に自動削除される有効期限を設定し、プロセス強制終了時の残留を防ぐ
        _temp_table_obj = bigquery.Table(temp_table_ref)
        _temp_table_obj.expires = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        client.create_table(_temp_table_obj)
        logger.debug(f"一時テーブルを作成しました: {temp_table_ref}")

        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True,
            )
            job = client.load_table_from_dataframe(save_df, temp_table_ref, job_config=job_config)
            job.result()
            logger.debug(f"一時テーブルに {len(save_df)} 行をロードしました")

            # MERGE文を構築（一意キー: race_id + horse_id）
            unique_keys = ["race_id", "horse_id"]
            columns = list(save_df.columns)
            join_conditions = " AND ".join(
                [f"T.{key} = S.{key}" for key in unique_keys]
            )
            update_columns = [col for col in columns if col not in unique_keys]
            update_set = ", ".join([f"T.{col} = S.{col}" for col in update_columns])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"S.{col}" for col in columns])

            merge_query = f"""
            MERGE `{table_ref}` T
            USING `{temp_table_ref}` S
            ON {join_conditions}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
            """

            logger.debug("MERGEクエリを実行中...")
            query_job = client.query(merge_query)
            query_job.result()
            logger.info(f"BigQuery保存完了: {len(save_df)}行を {table_ref} にUPSERTしました")

        finally:
            client.delete_table(temp_table_ref, not_found_ok=True)
            logger.debug(f"一時テーブルを削除しました: {temp_table_ref}")

        return len(save_df)

    except Exception as e:
        logger.error(f"BigQuery保存エラー: {e}")
        raise


def save_predictions_to_gcs(
    result_df: pd.DataFrame,
    project_id: str,
    race_date: Optional[datetime.date] = None,
    bucket_suffix: str = "keiba-predictions",
) -> str:
    """
    予測結果をGCSにCSVとして保存する

    保存パス: gs://{project_id}-{bucket_suffix}/{YYYY-MM-DD}/predictions.csv

    Args:
        result_df: 予測結果のDataFrame（predict_pipelineの出力）
        project_id: GCPプロジェクトID
        race_date: 保存先のサブディレクトリに使う日付。
                   未指定時は result_df の race_date の最小値を使用。
        bucket_suffix: バケット名のサフィックス（デフォルト: "keiba-predictions"）

    Returns:
        保存先のGCS URI（例: gs://my-project-keiba-predictions/2026-03-01/predictions.csv）

    Raises:
        ValueError: result_df が空の場合
    """
    if len(result_df) == 0:
        raise ValueError("保存するデータがありません")

    if race_date is None:
        race_date = pd.to_datetime(result_df["race_date"]).min().date()

    date_str = race_date.strftime("%Y-%m-%d")
    bucket_name = f"{project_id}-{bucket_suffix}"
    blob_name = f"{date_str}/predictions.csv"

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    csv_content = result_df.to_csv(index=False)
    blob.upload_from_string(csv_content, content_type="text/csv")

    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    logger.info(f"予測結果をGCSに保存しました: {gcs_uri} ({len(result_df)}行)")
    return gcs_uri


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
            if finish_raw is None or pd.isna(finish_raw):
                finish = "-"
            else:
                pos = int(finish_raw)
                finish = str(pos) if pos > 0 else "-"
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
        "--target-dates",
        nargs="+",
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "推論対象日 (YYYY-MM-DD 形式, 複数指定可). "
            "指定した場合はその日付のみ対象とする。"
            "未指定の場合は --execution-date の週の土曜・日曜を使用する。"
            " 例: --target-dates 2026-02-14 2026-02-15"
        ),
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
    parser.add_argument(
        "--save-to-bq",
        action="store_true",
        default=False,
        help="予測結果をBigQuery（predictions.daily_predictions）に保存する",
    )

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    project_id = args.project_id
    if not project_id:
        try:
            import google.auth
            _, project_id = google.auth.default()
        except Exception:
            pass
    if not project_id:
        logger.error(
            "GCPプロジェクトIDを特定できません。"
            "--project-id オプションまたは GCP_PROJECT_ID 環境変数を設定してください。"
        )
        return 1
    logger.info(f"使用するGCPプロジェクトID: {project_id}")

    config = load_config(args.config)
    execution_date = datetime.date.fromisoformat(args.execution_date)

    parsed_target_dates = None
    if args.target_dates:
        try:
            parsed_target_dates = [
                datetime.date.fromisoformat(d) for d in args.target_dates
            ]
        except ValueError as e:
            logger.error(f"--target-dates のフォーマットが不正です: {e}")
            return 1

    result_df = predict_pipeline(
        project_id=project_id,
        execution_date=execution_date,
        config=config,
        model_path=args.model_path,
        target_dates=parsed_target_dates,
    )

    # 結果表示
    print(format_predictions(result_df))

    # CSV出力
    if args.output_csv and len(result_df) > 0:
        result_df.to_csv(args.output_csv, index=False)
        print(f"\n結果をCSVに保存しました: {args.output_csv}")

    # BigQuery保存
    if args.save_to_bq and len(result_df) > 0:
        saved_rows = save_predictions_to_bq(
            result_df=result_df,
            project_id=project_id,
        )
        print(f"\n{saved_rows}行をBigQuery（predictions.daily_predictions）に保存しました")

    # サマリー
    if len(result_df) > 0:
        print(f"\n合計: {result_df['race_id'].nunique()} レース, "
              f"{len(result_df)} 頭")

    return 0


if __name__ == "__main__":
    exit(main())
