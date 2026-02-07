#!/usr/bin/env python3
"""
特徴量パイプライン

Phase 1特徴量を統合し、学習用データを生成する。
BigQueryのfeatures.training_dataテーブルに出力する。

機能:
- バッチ処理による特徴量生成
- 並列処理による高速化
- リトライ処理によるエラー耐性
- 進捗管理とログ出力
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable
import logging
import threading
import time
import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions

from src.ml.features.past_performance import PastPerformanceFeatures, PastPerformanceConfig
from src.ml.features.condition_features import ConditionFeatures, ConditionConfig

# ロガー設定
logger = logging.getLogger(__name__)


# リトライ対象の例外
RETRYABLE_EXCEPTIONS = (
    google_exceptions.ServiceUnavailable,
    google_exceptions.InternalServerError,
    google_exceptions.DeadlineExceeded,
    google_exceptions.TooManyRequests,
    ConnectionError,
    TimeoutError,
)


def retry_with_backoff(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple = RETRYABLE_EXCEPTIONS,
):
    """
    指数バックオフ付きでリトライを行うデコレータ

    Args:
        func: 実行する関数
        max_retries: 最大リトライ回数
        base_delay: 初期待機時間（秒）
        max_delay: 最大待機時間（秒）
        exceptions: リトライ対象の例外

    Returns:
        関数の戻り値
    """
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < max_retries:
                delay = min(base_delay * (2**attempt), max_delay)
                logger.warning(
                    f"Retry attempt {attempt + 1}/{max_retries} after error: {e}. "
                    f"Waiting {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries} retries failed: {e}")
    raise last_exception


class ProgressTracker:
    """進捗管理クラス（スレッドセーフ）"""

    def __init__(self, total_items: int):
        self.total_items = total_items
        self.processed_items = 0
        self.successful_items = 0
        self.failed_items = 0
        self.start_time: Optional[float] = None
        self._lock = threading.Lock()

    def start(self):
        """進捗トラッキング開始"""
        self.start_time = time.time()

    def update(self, success: bool = True):
        """進捗を更新（スレッドセーフ）"""
        with self._lock:
            self.processed_items += 1
            if success:
                self.successful_items += 1
            else:
                self.failed_items += 1

    @property
    def elapsed_time(self) -> float:
        """経過時間（秒）"""
        if self.start_time is None:
            return 0.0
        return time.time() - self.start_time

    @property
    def progress_percent(self) -> float:
        """進捗率（%）"""
        if self.total_items == 0:
            return 100.0
        return (self.processed_items / self.total_items) * 100

    @property
    def estimated_remaining_time(self) -> Optional[float]:
        """推定残り時間（秒）"""
        if self.processed_items == 0 or self.start_time is None:
            return None
        avg_time = self.elapsed_time / self.processed_items
        remaining_items = self.total_items - self.processed_items
        return avg_time * remaining_items

    def get_status_message(self) -> str:
        """進捗状況のメッセージを取得"""
        remaining = self.estimated_remaining_time
        remaining_str = (
            f"{remaining / 60:.1f}分" if remaining is not None else "計算中..."
        )
        return (
            f"[{self.processed_items}/{self.total_items}] "
            f"{self.progress_percent:.1f}% 完了 | "
            f"成功: {self.successful_items} | "
            f"失敗: {self.failed_items} | "
            f"残り時間: {remaining_str}"
        )


@dataclass
class FeaturePipelineConfig:
    """特徴量パイプラインの設定"""

    # 出力先
    output_dataset: str = "features"
    output_table: str = "training_data"

    # 過去走特徴量設定
    past_performance_config: Optional[PastPerformanceConfig] = None

    # 条件適性特徴量設定
    condition_config: Optional[ConditionConfig] = None

    # 並列処理設定
    max_workers: int = 4

    # リトライ設定
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0

    # 進捗ログ出力間隔（レース数）
    progress_log_interval: int = 10


class FeaturePipeline:
    """特徴量パイプラインを実行するクラス"""

    def __init__(
        self,
        project_id: str,
        config: Optional[FeaturePipelineConfig] = None,
    ):
        """
        Args:
            project_id: GCPプロジェクトID
            config: パイプライン設定
        """
        self.project_id = project_id
        self.config = config or FeaturePipelineConfig()
        self.client = bigquery.Client(project=project_id)

        # 特徴量生成クラスの初期化
        self.past_perf = PastPerformanceFeatures(
            project_id, self.config.past_performance_config
        )
        self.condition = ConditionFeatures(project_id, self.config.condition_config)

    def run(
        self,
        start_date: str,
        end_date: str,
        batch_size: int = 100,
        parallel: bool = True,
    ) -> dict:
        """
        指定期間のレースに対して特徴量を生成

        Args:
            start_date: 開始日 (YYYY-MM-DD)
            end_date: 終了日 (YYYY-MM-DD)
            batch_size: 一度に処理するレース数（BigQueryへの保存単位）
            parallel: 並列処理を有効にするか

        Returns:
            処理結果の統計
        """
        logger.info(f"Feature pipeline started: {start_date} to {end_date}")
        logger.info(
            f"Settings: batch_size={batch_size}, parallel={parallel}, "
            f"max_workers={self.config.max_workers}"
        )

        # 対象レースを取得
        races = self._fetch_target_races(start_date, end_date)
        total_races = len(races)

        if total_races == 0:
            logger.warning("No races found in the specified period")
            return {
                "total_races": 0,
                "processed_races": 0,
                "errors": 0,
                "elapsed_time": 0.0,
            }

        logger.info(f"Found {total_races} races to process")

        # 進捗トラッカー初期化
        progress = ProgressTracker(total_items=total_races)
        progress.start()

        all_features = []

        # バッチ処理
        for i in range(0, total_races, batch_size):
            batch_races = races.iloc[i : i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_races + batch_size - 1) // batch_size
            logger.info(f"Processing batch {batch_num}/{total_batches}")

            if parallel and len(batch_races) > 1:
                batch_features = self._process_batch_parallel(
                    batch_races, progress
                )
            else:
                batch_features = self._process_batch_sequential(
                    batch_races, progress
                )

            all_features.extend(batch_features)

            # バッチ終了時の進捗ログ
            logger.info(f"Batch {batch_num} completed. {progress.get_status_message()}")

            # バッチ単位でBigQueryに保存（メモリ効率化）
            if batch_features:
                combined_batch = pd.concat(batch_features, ignore_index=True)
                self._save_to_bigquery_with_retry(combined_batch)
                logger.info(f"Saved {len(combined_batch)} rows from batch {batch_num}")

        result = {
            "total_races": total_races,
            "processed_races": progress.successful_items,
            "errors": progress.failed_items,
            "elapsed_time": progress.elapsed_time,
        }

        logger.info(f"Feature pipeline completed: {result}")
        return result

    def _process_batch_parallel(
        self,
        races: pd.DataFrame,
        progress: ProgressTracker,
    ) -> list[pd.DataFrame]:
        """バッチを並列処理"""
        batch_features = []

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {
                executor.submit(self._process_race_with_retry, race): race["race_id"]
                for _, race in races.iterrows()
            }

            for future in as_completed(futures):
                race_id = futures[future]
                try:
                    race_features = future.result()
                    if race_features is not None and not race_features.empty:
                        batch_features.append(race_features)
                        progress.update(success=True)
                    else:
                        progress.update(success=True)  # 空結果も成功として扱う
                except Exception as e:
                    logger.error(f"Error processing race {race_id}: {e}")
                    progress.update(success=False)

                # 進捗ログ出力
                if progress.processed_items % self.config.progress_log_interval == 0:
                    logger.info(progress.get_status_message())

        return batch_features

    def _process_batch_sequential(
        self,
        races: pd.DataFrame,
        progress: ProgressTracker,
    ) -> list[pd.DataFrame]:
        """バッチを順次処理"""
        batch_features = []

        for _, race in races.iterrows():
            try:
                race_features = self._process_race_with_retry(race)
                if race_features is not None and not race_features.empty:
                    batch_features.append(race_features)
                    progress.update(success=True)
                else:
                    progress.update(success=True)  # 空結果も成功として扱う
            except Exception as e:
                logger.error(f"Error processing race {race['race_id']}: {e}")
                progress.update(success=False)

            # 進捗ログ出力
            if progress.processed_items % self.config.progress_log_interval == 0:
                logger.info(progress.get_status_message())

        return batch_features

    def _process_race_with_retry(self, race: pd.Series) -> Optional[pd.DataFrame]:
        """リトライ付きでレースを処理"""
        return retry_with_backoff(
            func=lambda: self._process_race(race),
            max_retries=self.config.max_retries,
            base_delay=self.config.retry_base_delay,
            max_delay=self.config.retry_max_delay,
        )

    def _save_to_bigquery_with_retry(self, features: pd.DataFrame) -> None:
        """リトライ付きでBigQueryに保存"""
        retry_with_backoff(
            func=lambda: self._save_to_bigquery(features),
            max_retries=self.config.max_retries,
            base_delay=self.config.retry_base_delay,
            max_delay=self.config.retry_max_delay,
        )

    def _fetch_target_races(
        self,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """対象レースを取得

        race_resultsテーブルに存在するレースのみを取得する。
        race_infoテーブルとLEFT JOINして、レース条件情報を補完する。
        """
        query = f"""
        SELECT DISTINCT
            rr.race_id,
            rr.race_date,
            COALESCE(ri.venue_code, SUBSTR(rr.race_id, 1, 2)) as venue_code,
            COALESCE(ri.course_type, rr.course_type) as course_type,
            COALESCE(ri.distance, rr.distance) as distance,
            COALESCE(ri.track_condition, rr.track_condition) as track_condition
        FROM `{self.project_id}.raw.race_results` rr
        LEFT JOIN `{self.project_id}.raw.race_info` ri
            ON rr.race_id = ri.race_id
        WHERE rr.race_date >= '{start_date}'
            AND rr.race_date <= '{end_date}'
        ORDER BY rr.race_date, rr.race_id
        """
        return self.client.query(query).to_dataframe()

    def _process_race(self, race: pd.Series) -> Optional[pd.DataFrame]:
        """1レースの特徴量を生成"""
        race_id = race["race_id"]
        race_date = str(race["race_date"])

        logger.debug(f"Processing race: {race_id}")

        # 出走馬と成績情報を取得
        horses_df = self._fetch_race_horses(race_id)
        if horses_df.empty:
            logger.debug(f"No horses found for race {race_id} (race_date: {race_date})")
            return None

        logger.debug(f"Found {len(horses_df)} horses for race {race_id}")

        horse_ids = horses_df["horse_id"].tolist()

        # 過去走特徴量
        past_features = self.past_perf.generate_features(race_date, horse_ids)

        # 条件適性特徴量
        condition_features = self.condition.generate_features(race_date, horse_ids)

        # 距離変更特徴量
        distance_features = self.past_perf.generate_distance_change_features(
            race_date, int(race["distance"]), horse_ids
        )

        # 脚質特徴量
        running_style_features = self.condition.get_running_style_features(
            race_date, horse_ids
        )

        # 全特徴量をマージ
        features = horses_df.copy()

        if not past_features.empty:
            features = features.merge(past_features, on="horse_id", how="left")

        if not condition_features.empty:
            features = features.merge(
                condition_features.drop(columns=["feature_date"], errors="ignore"),
                on="horse_id",
                how="left",
            )

        if not distance_features.empty:
            features = features.merge(distance_features, on="horse_id", how="left")

        if not running_style_features.empty:
            features = features.merge(running_style_features, on="horse_id", how="left")

        # レース条件に対する適性スコアを計算
        features = self._add_race_aptitude_scores(features, race)

        # 目的変数を追加
        features["target_place"] = features["finish_position"].apply(
            lambda x: True if pd.notna(x) and x <= 3 else False
        )

        # メタ情報を追加
        features["race_id"] = race_id
        features["race_date"] = race_date
        features["created_at"] = datetime.utcnow()

        return features

    def _fetch_race_horses(self, race_id: str) -> pd.DataFrame:
        """レース出走馬と成績を取得

        raw.race_results（レース成績データ）をベースに、
        raw.horse_results（前日予想データ）からbracket_numberを取得し、
        raw.race_info（レース情報）からvenue_codeとrace_numberを取得する。
        """
        query = f"""
        SELECT
            rr.horse_id,
            rr.horse_number,
            hr.bracket_number,
            rr.weight_carried as weight,
            rr.jockey_code as jockey_id,
            rr.trainer_code as trainer_id,
            rr.finish_position,
            rr.finish_time,
            rr.last_3f_time,
            ri.venue_code,
            rr.course_type,
            rr.distance,
            rr.track_condition,
            rr.num_horses,
            ri.race_number
        FROM `{self.project_id}.raw.race_results` rr
        LEFT JOIN `{self.project_id}.raw.horse_results` hr
            ON rr.race_id = hr.race_id
            AND rr.horse_id = hr.horse_id
        LEFT JOIN `{self.project_id}.raw.race_info` ri
            ON rr.race_id = ri.race_id
        WHERE rr.race_id = '{race_id}'
            AND (rr.abnormal_code IS NULL OR rr.abnormal_code = 0)
        """
        return self.client.query(query).to_dataframe()

    def _add_race_aptitude_scores(
        self, features: pd.DataFrame, race: pd.Series
    ) -> pd.DataFrame:
        """今回のレース条件に対する適性スコアを計算して追加"""
        # コース適性
        course_type = race.get("course_type", "")
        course_map = {"芝": "turf", "ダート": "dirt", "障害": "jump"}
        safe_course = course_map.get(course_type, "unknown")

        col_name = f"{safe_course}_place_rate"
        if col_name in features.columns:
            features["current_course_aptitude"] = features[col_name]
        else:
            features["current_course_aptitude"] = None

        # 距離適性
        distance = race.get("distance", 0)
        if distance < 1400:
            dist_cat = "sprint"
        elif distance < 1800:
            dist_cat = "mile"
        elif distance < 2200:
            dist_cat = "intermediate"
        else:
            dist_cat = "long"

        col_name = f"dist_{dist_cat}_place_rate"
        if col_name in features.columns:
            features["current_distance_aptitude"] = features[col_name]
        else:
            features["current_distance_aptitude"] = None

        # 馬場適性
        track_condition = race.get("track_condition", "")
        if track_condition in ["稍重", "重", "不良"]:
            track_cat = "heavy"
        else:
            track_cat = "good"

        col_name = f"track_{track_cat}_place_rate"
        if col_name in features.columns:
            features["current_track_aptitude"] = features[col_name]
        else:
            features["current_track_aptitude"] = None

        # 競馬場適性
        venue_code = race.get("venue_code", "")
        col_name = f"venue_{venue_code}_place_rate"
        if col_name in features.columns:
            features["current_venue_aptitude"] = features[col_name]
        else:
            features["current_venue_aptitude"] = None

        return features

    def _save_to_bigquery(self, features: pd.DataFrame) -> None:
        """特徴量をBigQueryに保存"""
        table_id = (
            f"{self.project_id}.{self.config.output_dataset}.{self.config.output_table}"
        )

        # テーブルスキーマを取得
        table = self.client.get_table(table_id)
        schema_columns = {field.name for field in table.schema}

        # カラムを整理 (training_dataスキーマに合わせる)
        output_columns = self._get_output_columns(features)
        # スキーマに存在するカラムのみに絞る
        output_columns = [c for c in output_columns if c in schema_columns]
        output_df = features[output_columns].copy()

        # データ型を調整
        output_df = self._convert_column_types(output_df)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        job = self.client.load_table_from_dataframe(output_df, table_id, job_config)
        job.result()

        logger.info(f"Saved {len(output_df)} rows to {table_id}")

    def _convert_column_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """BigQueryスキーマに合わせてカラムの型を変換"""
        import numpy as np

        # 日付型
        if "race_date" in df.columns:
            df["race_date"] = pd.to_datetime(df["race_date"]).dt.date

        # 文字列型として保存すべきカラム
        string_columns = [
            "race_id",
            "horse_id",
            "venue_code",
            "course_type",
            "track_condition",
            "jockey_id",
            "trainer_id",
        ]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).replace("nan", None).replace("None", None)

        # 整数型として保存すべきカラム
        int_columns = [
            "race_number",
            "num_horses",
            "bracket_number",
            "horse_number",
            "finish_position",
            "career_races",
            "career_wins",
            "career_places",
            "days_since_last_race",
            "distance",
            "distance_change",
        ]
        for col in int_columns:
            if col in df.columns:
                # NaNを含む可能性があるため、Int64を使用
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # 浮動小数点型として保存すべきカラム
        float_columns = [
            "weight",
            "finish_time",
            "last_3f_time",
            "past_3_avg_position",
            "past_5_avg_position",
            "past_10_avg_position",
            "past_3_avg_last3f",
            "past_5_avg_last3f",
            "past_3_win_rate",
            "past_5_win_rate",
            "past_3_place_rate",
            "past_5_place_rate",
            "position_trend_3",
            "distance_change_ratio",
            "turf_win_rate",
            "turf_place_rate",
            "dirt_win_rate",
            "dirt_place_rate",
            "dist_sprint_place_rate",
            "dist_mile_place_rate",
            "dist_intermediate_place_rate",
            "dist_long_place_rate",
            "track_good_place_rate",
            "track_heavy_place_rate",
            "current_course_aptitude",
            "current_distance_aptitude",
            "current_track_aptitude",
            "current_venue_aptitude",
            "avg_corner4_position",
            "avg_corner4_ratio",
            "front_rate",
            "closer_rate",
        ]
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

        # ブール型
        if "target_place" in df.columns:
            df["target_place"] = df["target_place"].astype(bool)

        # distance_category_change は文字列
        if "distance_category_change" in df.columns:
            df["distance_category_change"] = (
                df["distance_category_change"]
                .astype(str)
                .replace("nan", None)
                .replace("None", None)
            )

        return df

    def _get_output_columns(self, features: pd.DataFrame) -> list[str]:
        """出力するカラムを決定"""
        # 必須カラム
        required_columns = ["race_id", "horse_id", "race_date"]

        # 利用可能なカラムを確認
        available = set(features.columns)

        # 出力カラム (存在するもののみ)
        optional_columns = [
            "target_place",
            "finish_position",
            "venue_code",
            "race_number",
            "course_type",
            "distance",
            "track_condition",
            "num_horses",
            "bracket_number",
            "horse_number",
            "weight",
            # 過去走特徴量
            "past_3_avg_position",
            "past_5_avg_position",
            "past_10_avg_position",
            "past_3_avg_last3f",
            "past_5_avg_last3f",
            "past_3_win_rate",
            "past_5_win_rate",
            "past_3_place_rate",
            "past_5_place_rate",
            "days_since_last_race",
            "career_races",
            "career_wins",
            "career_places",
            "position_trend_3",
            "distance_change",
            "distance_change_ratio",
            "distance_category_change",
            # 条件適性特徴量
            "turf_win_rate",
            "turf_place_rate",
            "dirt_win_rate",
            "dirt_place_rate",
            "dist_sprint_place_rate",
            "dist_mile_place_rate",
            "dist_intermediate_place_rate",
            "dist_long_place_rate",
            "track_good_place_rate",
            "track_heavy_place_rate",
            "current_course_aptitude",
            "current_distance_aptitude",
            "current_track_aptitude",
            "current_venue_aptitude",
            # 脚質特徴量
            "avg_corner4_position",
            "avg_corner4_ratio",
            "front_rate",
            "closer_rate",
            # 人的要素
            "jockey_id",
            "trainer_id",
            # メタ情報
            "created_at",
        ]

        output = required_columns + [c for c in optional_columns if c in available]

        return output

    def generate_for_race(
        self,
        race_id: str,
        race_date: str,
    ) -> pd.DataFrame:
        """
        特定のレースに対して特徴量を生成（予測時に使用）

        Args:
            race_id: レースID
            race_date: レース開催日

        Returns:
            特徴量DataFrame (BigQueryには保存しない)
        """
        # レース情報を取得
        query = f"""
        SELECT
            race_id,
            race_date,
            venue_code,
            course_type,
            distance,
            track_condition
        FROM `{self.project_id}.raw.race_info`
        WHERE race_id = '{race_id}'
        """
        race_df = self.client.query(query).to_dataframe()

        if race_df.empty:
            logger.warning(f"Race not found: {race_id}")
            return pd.DataFrame()

        race = race_df.iloc[0]
        return self._process_race(race)


def main():
    """メイン関数（CLIから実行）"""
    import argparse
    import os
    from dotenv import load_dotenv

    # .envファイルから環境変数を読み込み
    load_dotenv()

    parser = argparse.ArgumentParser(description="特徴量パイプラインを実行")
    parser.add_argument("--start-date", required=True, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="終了日 (YYYY-MM-DD)")
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCPプロジェクトID",
    )
    parser.add_argument("--batch-size", type=int, default=100, help="バッチサイズ")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="並列処理のワーカー数",
    )
    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="並列処理を無効化",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="最大リトライ回数",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細ログを出力")

    args = parser.parse_args()

    # ロギング設定
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if not args.project_id:
        logger.error("GCP_PROJECT_ID is not set")
        return 1

    # 設定
    config = FeaturePipelineConfig(
        max_workers=args.max_workers,
        max_retries=args.max_retries,
    )

    # パイプライン実行
    pipeline = FeaturePipeline(args.project_id, config)
    result = pipeline.run(
        args.start_date,
        args.end_date,
        args.batch_size,
        parallel=not args.no_parallel,
    )

    # 結果出力
    print("\n" + "=" * 60)
    print("特徴量パイプライン実行結果")
    print("=" * 60)
    print(f"対象レース数: {result['total_races']}")
    print(f"処理成功: {result['processed_races']}")
    print(f"エラー: {result['errors']}")
    print(f"処理時間: {result['elapsed_time']:.1f}秒")
    print("=" * 60)

    return 0 if result["errors"] == 0 else 1


if __name__ == "__main__":
    exit(main())
