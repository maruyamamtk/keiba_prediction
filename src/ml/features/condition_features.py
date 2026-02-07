#!/usr/bin/env python3
"""
条件適性特徴量

Phase 1特徴量の一部として、馬の条件別適性を計算する。
- 芝/ダート適性
- 距離帯別適性
- 競馬場別適性
- 馬場状態別適性
- 季節適性
"""

from dataclasses import dataclass
from typing import Optional
import pandas as pd
from google.cloud import bigquery


@dataclass
class ConditionConfig:
    """条件適性特徴量の設定"""

    # 平滑化パラメータ
    smoothing_factor: float = 10.0
    # 最低サンプル数
    min_samples: int = 3
    # 季節の定義 (月のリスト)
    seasons: dict = None  # type: ignore

    def __post_init__(self):
        if self.seasons is None:
            self.seasons = {
                "spring": [3, 4, 5],
                "summer": [6, 7, 8],
                "autumn": [9, 10, 11],
                "winter": [12, 1, 2],
            }


class ConditionFeatures:
    """条件適性特徴量を生成するクラス"""

    # 距離カテゴリの定義
    DISTANCE_CATEGORIES = {
        "sprint": (0, 1400),  # スプリント
        "mile": (1400, 1800),  # マイル
        "intermediate": (1800, 2200),  # 中距離
        "long": (2200, 9999),  # 長距離
    }

    # 馬場状態カテゴリ
    TRACK_CONDITIONS = {
        "good": ["良"],
        "heavy": ["稍重", "重", "不良"],
    }

    def __init__(
        self,
        project_id: str,
        config: Optional[ConditionConfig] = None,
    ):
        """
        Args:
            project_id: GCPプロジェクトID
            config: 条件適性特徴量の設定
        """
        self.project_id = project_id
        self.config = config or ConditionConfig()
        self.client = bigquery.Client(project=project_id)

    def generate_features(
        self,
        target_date: str,
        horse_ids: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        指定日時点での条件適性特徴量を生成

        Args:
            target_date: 特徴量を生成する基準日 (YYYY-MM-DD形式)
            horse_ids: 対象馬のIDリスト (Noneの場合は全馬)

        Returns:
            条件適性特徴量のDataFrame
        """
        # 過去走データを取得
        past_results = self._fetch_past_results(target_date, horse_ids)

        if past_results.empty:
            return pd.DataFrame()

        # 全体の勝率・複勝率を計算（平滑化用）
        global_stats = self._calculate_global_stats(past_results)

        # 各特徴量を生成
        features = self._calculate_features(past_results, global_stats)
        features["feature_date"] = target_date

        return features

    def _fetch_past_results(
        self,
        target_date: str,
        horse_ids: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """過去走データをBigQueryから取得"""
        horse_filter = ""
        if horse_ids:
            horse_ids_str = ", ".join([f"'{h}'" for h in horse_ids])
            horse_filter = f"AND rr.horse_id IN ({horse_ids_str})"

        query = f"""
        SELECT
            rr.horse_id,
            rr.race_id,
            rr.race_date,
            rr.finish_position,
            rr.distance,
            rr.course_type,
            rr.track_condition,
            ri.venue_code,
            EXTRACT(MONTH FROM rr.race_date) as race_month
        FROM `{self.project_id}.raw.race_results` rr
        LEFT JOIN `{self.project_id}.raw.race_info` ri
            ON rr.race_id = ri.race_id
        WHERE rr.race_date < '{target_date}'
            AND rr.finish_position IS NOT NULL
            AND rr.abnormal_code = 0
            {horse_filter}
        """

        df = self.client.query(query).to_dataframe()

        # 距離カテゴリを追加
        df["distance_category"] = df["distance"].apply(self._get_distance_category)

        # 馬場状態カテゴリを追加
        df["track_category"] = df["track_condition"].apply(self._get_track_category)

        # 季節を追加
        df["season"] = df["race_month"].apply(self._get_season)

        return df

    def _get_distance_category(self, distance: int) -> str:
        """距離から距離カテゴリを取得"""
        if pd.isna(distance):
            return "unknown"
        for category, (min_dist, max_dist) in self.DISTANCE_CATEGORIES.items():
            if min_dist <= distance < max_dist:
                return category
        return "unknown"

    def _get_track_category(self, condition: str) -> str:
        """馬場状態からカテゴリを取得"""
        if pd.isna(condition):
            return "unknown"
        for category, conditions in self.TRACK_CONDITIONS.items():
            if condition in conditions:
                return category
        return "unknown"

    def _get_season(self, month: int) -> str:
        """月から季節を取得"""
        if pd.isna(month):
            return "unknown"
        for season, months in self.config.seasons.items():
            if month in months:
                return season
        return "unknown"

    def _calculate_global_stats(self, past_results: pd.DataFrame) -> dict:
        """全体の統計値を計算（平滑化用）"""
        return {
            "global_win_rate": (past_results["finish_position"] == 1).mean(),
            "global_place_rate": (past_results["finish_position"] <= 3).mean(),
        }

    def _calculate_features(
        self,
        past_results: pd.DataFrame,
        global_stats: dict,
    ) -> pd.DataFrame:
        """条件適性特徴量を計算"""
        features_list = []

        for horse_id, horse_data in past_results.groupby("horse_id"):
            feature_row = {"horse_id": horse_id}

            # コース適性 (芝/ダート)
            for course_type in ["芝", "ダート", "障害"]:
                course_data = horse_data[horse_data["course_type"] == course_type]
                win_rate, place_rate = self._calculate_smoothed_rates(
                    course_data, global_stats
                )
                safe_name = self._safe_column_name(course_type)
                feature_row[f"{safe_name}_win_rate"] = win_rate
                feature_row[f"{safe_name}_place_rate"] = place_rate
                feature_row[f"{safe_name}_races"] = len(course_data)

            # 距離カテゴリ適性
            for category in self.DISTANCE_CATEGORIES.keys():
                cat_data = horse_data[horse_data["distance_category"] == category]
                win_rate, place_rate = self._calculate_smoothed_rates(
                    cat_data, global_stats
                )
                feature_row[f"dist_{category}_win_rate"] = win_rate
                feature_row[f"dist_{category}_place_rate"] = place_rate
                feature_row[f"dist_{category}_races"] = len(cat_data)

            # 馬場状態適性
            for track_cat in ["good", "heavy"]:
                track_data = horse_data[horse_data["track_category"] == track_cat]
                win_rate, place_rate = self._calculate_smoothed_rates(
                    track_data, global_stats
                )
                feature_row[f"track_{track_cat}_win_rate"] = win_rate
                feature_row[f"track_{track_cat}_place_rate"] = place_rate
                feature_row[f"track_{track_cat}_races"] = len(track_data)

            # 競馬場別適性（主要競馬場のみ）
            major_venues = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]
            for venue in major_venues:
                venue_data = horse_data[horse_data["venue_code"] == venue]
                win_rate, place_rate = self._calculate_smoothed_rates(
                    venue_data, global_stats
                )
                feature_row[f"venue_{venue}_win_rate"] = win_rate
                feature_row[f"venue_{venue}_place_rate"] = place_rate
                feature_row[f"venue_{venue}_races"] = len(venue_data)

            # 季節適性
            for season in self.config.seasons.keys():
                season_data = horse_data[horse_data["season"] == season]
                win_rate, place_rate = self._calculate_smoothed_rates(
                    season_data, global_stats
                )
                feature_row[f"season_{season}_win_rate"] = win_rate
                feature_row[f"season_{season}_place_rate"] = place_rate
                feature_row[f"season_{season}_races"] = len(season_data)

            features_list.append(feature_row)

        return pd.DataFrame(features_list)

    def _calculate_smoothed_rates(
        self,
        data: pd.DataFrame,
        global_stats: dict,
    ) -> tuple[Optional[float], Optional[float]]:
        """平滑化した勝率・複勝率を計算"""
        n = len(data)

        if n == 0:
            return None, None

        # 平滑化 (Bayesian averaging)
        # smoothed = (n * sample_rate + m * global_rate) / (n + m)
        m = self.config.smoothing_factor

        wins = (data["finish_position"] == 1).sum()
        places = (data["finish_position"] <= 3).sum()

        smoothed_win_rate = (wins + m * global_stats["global_win_rate"]) / (n + m)
        smoothed_place_rate = (places + m * global_stats["global_place_rate"]) / (n + m)

        return smoothed_win_rate, smoothed_place_rate

    def _safe_column_name(self, name: str) -> str:
        """カラム名に安全な文字列に変換"""
        mapping = {
            "芝": "turf",
            "ダート": "dirt",
            "障害": "jump",
        }
        return mapping.get(name, name)

    def generate_race_features(
        self,
        race_id: str,
        race_date: str,
        race_conditions: dict,
    ) -> pd.DataFrame:
        """
        特定レースに出走する全馬の条件適性特徴量を生成し、
        今回のレース条件に対する適性スコアを計算

        Args:
            race_id: レースID
            race_date: レース開催日
            race_conditions: レース条件 (course_type, distance, track_condition, venue_code)

        Returns:
            レース出走馬の条件適性特徴量DataFrame
        """
        # レース出走馬を取得
        query = f"""
        SELECT DISTINCT horse_id
        FROM `{self.project_id}.raw.horse_results`
        WHERE race_id = '{race_id}'
        """
        horse_ids_df = self.client.query(query).to_dataframe()
        horse_ids = horse_ids_df["horse_id"].tolist()

        if not horse_ids:
            return pd.DataFrame()

        # 条件適性特徴量を生成
        features = self.generate_features(race_date, horse_ids)

        if features.empty:
            return pd.DataFrame()

        # 今回のレース条件に対する適性スコアを計算
        features = self._add_current_race_aptitude(features, race_conditions)
        features["race_id"] = race_id

        return features

    def _add_current_race_aptitude(
        self,
        features: pd.DataFrame,
        race_conditions: dict,
    ) -> pd.DataFrame:
        """今回のレース条件に対する適性スコアを追加"""
        # コース適性
        course_type = race_conditions.get("course_type", "")
        safe_course = self._safe_column_name(course_type)
        if f"{safe_course}_place_rate" in features.columns:
            features["current_course_aptitude"] = features[f"{safe_course}_place_rate"]
        else:
            features["current_course_aptitude"] = None

        # 距離適性
        distance = race_conditions.get("distance", 0)
        distance_category = self._get_distance_category(distance)
        if f"dist_{distance_category}_place_rate" in features.columns:
            features["current_distance_aptitude"] = features[
                f"dist_{distance_category}_place_rate"
            ]
        else:
            features["current_distance_aptitude"] = None

        # 馬場適性
        track_condition = race_conditions.get("track_condition", "")
        track_category = self._get_track_category(track_condition)
        if f"track_{track_category}_place_rate" in features.columns:
            features["current_track_aptitude"] = features[
                f"track_{track_category}_place_rate"
            ]
        else:
            features["current_track_aptitude"] = None

        # 競馬場適性
        venue_code = race_conditions.get("venue_code", "")
        if f"venue_{venue_code}_place_rate" in features.columns:
            features["current_venue_aptitude"] = features[
                f"venue_{venue_code}_place_rate"
            ]
        else:
            features["current_venue_aptitude"] = None

        return features

    def get_running_style_features(
        self,
        target_date: str,
        horse_ids: list[str],
    ) -> pd.DataFrame:
        """
        脚質に関する特徴量を生成

        Args:
            target_date: 基準日
            horse_ids: 対象馬のIDリスト

        Returns:
            脚質特徴量のDataFrame
        """
        horse_ids_str = ", ".join([f"'{h}'" for h in horse_ids])

        query = f"""
        SELECT
            horse_id,
            AVG(corner_position_4) as avg_corner4_position,
            AVG(CASE WHEN corner_position_4 IS NOT NULL THEN
                corner_position_4 * 1.0 / num_horses
            END) as avg_corner4_ratio,
            SUM(CASE WHEN corner_position_4 <= 3 THEN 1 ELSE 0 END) as front_runner_count,
            COUNT(*) as total_races,
            SUM(CASE WHEN corner_position_4 <= 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as front_rate,
            SUM(CASE WHEN corner_position_4 > num_horses * 0.6 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as closer_rate
        FROM `{self.project_id}.raw.race_results`
        WHERE race_date < '{target_date}'
            AND horse_id IN ({horse_ids_str})
            AND abnormal_code = 0
            AND corner_position_4 IS NOT NULL
        GROUP BY horse_id
        """

        return self.client.query(query).to_dataframe()
