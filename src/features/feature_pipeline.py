#!/usr/bin/env python3
"""
特徴量パイプライン

Phase 1特徴量を統合し、学習用データを生成する。
BigQueryのfeatures.training_dataテーブルに出力する。
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import logging
import pandas as pd
from google.cloud import bigquery

from src.features.past_performance import PastPerformanceFeatures, PastPerformanceConfig
from src.features.condition_features import ConditionFeatures, ConditionConfig

# ロガー設定
logger = logging.getLogger(__name__)


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
    ) -> dict:
        """
        指定期間のレースに対して特徴量を生成

        Args:
            start_date: 開始日 (YYYY-MM-DD)
            end_date: 終了日 (YYYY-MM-DD)
            batch_size: 一度に処理するレース数

        Returns:
            処理結果の統計
        """
        logger.info(f"Feature pipeline started: {start_date} to {end_date}")

        # 対象レースを取得
        races = self._fetch_target_races(start_date, end_date)
        total_races = len(races)

        if total_races == 0:
            logger.warning("No races found in the specified period")
            return {"total_races": 0, "processed_races": 0, "errors": 0}

        logger.info(f"Found {total_races} races to process")

        processed = 0
        errors = 0
        all_features = []

        # バッチ処理
        for i in range(0, total_races, batch_size):
            batch_races = races.iloc[i : i + batch_size]
            logger.info(
                f"Processing batch {i // batch_size + 1}/{(total_races + batch_size - 1) // batch_size}"
            )

            for _, race in batch_races.iterrows():
                try:
                    race_features = self._process_race(race)
                    if race_features is not None and not race_features.empty:
                        all_features.append(race_features)
                        processed += 1
                except Exception as e:
                    logger.error(f"Error processing race {race['race_id']}: {e}")
                    errors += 1

        # 特徴量を結合
        if all_features:
            combined_features = pd.concat(all_features, ignore_index=True)
            logger.info(f"Generated {len(combined_features)} feature rows")

            # BigQueryに保存
            self._save_to_bigquery(combined_features)
        else:
            logger.warning("No features generated")

        result = {
            "total_races": total_races,
            "processed_races": processed,
            "errors": errors,
        }

        logger.info(f"Feature pipeline completed: {result}")
        return result

    def _fetch_target_races(
        self,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """対象レースを取得"""
        query = f"""
        SELECT
            race_id,
            race_date,
            venue_code,
            course_type,
            distance,
            track_condition
        FROM `{self.project_id}.raw.race_info`
        WHERE race_date >= '{start_date}'
            AND race_date <= '{end_date}'
        ORDER BY race_date, race_id
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
            return None

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
        """レース出走馬と成績を取得"""
        query = f"""
        SELECT
            hr.horse_id,
            hr.horse_number,
            hr.bracket_number,
            hr.weight_carried as weight,
            hr.jockey_code as jockey_id,
            hr.trainer_code as trainer_id,
            rr.finish_position,
            rr.finish_time,
            rr.last_3f_time,
            ri.venue_code,
            ri.course_type,
            ri.distance,
            ri.track_condition,
            ri.num_horses,
            ri.race_number
        FROM `{self.project_id}.raw.horse_results` hr
        LEFT JOIN `{self.project_id}.raw.race_results` rr
            ON hr.race_id = rr.race_id
            AND hr.horse_id = rr.horse_id
        LEFT JOIN `{self.project_id}.raw.race_info` ri
            ON hr.race_id = ri.race_id
        WHERE hr.race_id = '{race_id}'
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

        # カラムを整理 (training_dataスキーマに合わせる)
        output_columns = self._get_output_columns(features)
        output_df = features[output_columns].copy()

        # データ型を調整
        if "race_date" in output_df.columns:
            output_df["race_date"] = pd.to_datetime(output_df["race_date"]).dt.date

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        job = self.client.load_table_from_dataframe(output_df, table_id, job_config)
        job.result()

        logger.info(f"Saved {len(output_df)} rows to {table_id}")

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

    parser = argparse.ArgumentParser(description="特徴量パイプラインを実行")
    parser.add_argument("--start-date", required=True, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="終了日 (YYYY-MM-DD)")
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCPプロジェクトID",
    )
    parser.add_argument("--batch-size", type=int, default=100, help="バッチサイズ")
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

    # パイプライン実行
    pipeline = FeaturePipeline(args.project_id)
    result = pipeline.run(args.start_date, args.end_date, args.batch_size)

    print("\n" + "=" * 60)
    print("特徴量パイプライン実行結果")
    print("=" * 60)
    print(f"対象レース数: {result['total_races']}")
    print(f"処理成功: {result['processed_races']}")
    print(f"エラー: {result['errors']}")
    print("=" * 60)

    return 0 if result["errors"] == 0 else 1


if __name__ == "__main__":
    exit(main())
