#!/usr/bin/env python3
"""
過去走集計特徴量

Phase 1特徴量の一部として、馬の過去走データから特徴量を生成する。
- 過去N走の基本統計 (着順、上がり3F、走破タイム)
- 休養日数
- 距離変更
- トレンド指標
"""

from dataclasses import dataclass
from typing import Optional
import pandas as pd
import numpy as np
from google.cloud import bigquery


@dataclass
class PastPerformanceConfig:
    """過去走特徴量の設定"""

    # 集計する過去走数
    n_past_races: list[int] = None  # type: ignore
    # 平滑化パラメータ (サンプル数が少ない場合に全体平均へ寄せる)
    smoothing_min_samples: int = 3

    def __post_init__(self):
        if self.n_past_races is None:
            self.n_past_races = [3, 5, 10]


class PastPerformanceFeatures:
    """過去走集計特徴量を生成するクラス"""

    def __init__(
        self,
        project_id: str,
        config: Optional[PastPerformanceConfig] = None,
    ):
        """
        Args:
            project_id: GCPプロジェクトID
            config: 過去走特徴量の設定
        """
        self.project_id = project_id
        self.config = config or PastPerformanceConfig()
        self.client = bigquery.Client(project=project_id)

    def generate_features(
        self,
        target_date: str,
        horse_ids: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        指定日時点での過去走特徴量を生成

        Args:
            target_date: 特徴量を生成する基準日 (YYYY-MM-DD形式)
            horse_ids: 対象馬のIDリスト (Noneの場合は全馬)

        Returns:
            過去走特徴量のDataFrame
        """
        # 過去走データを取得
        past_results = self._fetch_past_results(target_date, horse_ids)

        if past_results.empty:
            return pd.DataFrame()

        # 各特徴量を生成
        features = self._calculate_features(past_results, target_date)

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
            horse_filter = f"AND horse_id IN ({horse_ids_str})"

        query = f"""
        SELECT
            horse_id,
            race_id,
            race_date,
            finish_position,
            finish_time,
            last_3f_time,
            distance,
            course_type,
            track_condition,
            weight_carried,
            corner_position_4,
            idm,
            agari_index,
            ten_index
        FROM `{self.project_id}.raw.race_results`
        WHERE race_date < '{target_date}'
            AND finish_position IS NOT NULL
            AND abnormal_code = 0
            {horse_filter}
        ORDER BY horse_id, race_date DESC
        """

        df = self.client.query(query).to_dataframe()
        return df

    def _calculate_features(
        self,
        past_results: pd.DataFrame,
        target_date: str,
    ) -> pd.DataFrame:
        """過去走データから特徴量を計算"""
        features_list = []

        for horse_id, horse_data in past_results.groupby("horse_id"):
            # 日付順にソート（新しい順）
            horse_data = horse_data.sort_values("race_date", ascending=False)

            feature_row = {"horse_id": horse_id, "feature_date": target_date}

            # 過去N走の集計特徴量
            for n in self.config.n_past_races:
                recent_n = horse_data.head(n)
                prefix = f"past_{n}"

                if len(recent_n) >= self.config.smoothing_min_samples:
                    # 着順統計
                    feature_row[f"{prefix}_avg_position"] = recent_n[
                        "finish_position"
                    ].mean()
                    feature_row[f"{prefix}_min_position"] = recent_n[
                        "finish_position"
                    ].min()
                    feature_row[f"{prefix}_max_position"] = recent_n[
                        "finish_position"
                    ].max()
                    feature_row[f"{prefix}_std_position"] = recent_n[
                        "finish_position"
                    ].std()

                    # 上がり3F統計
                    valid_last3f = recent_n["last_3f_time"].dropna()
                    if len(valid_last3f) > 0:
                        feature_row[f"{prefix}_avg_last3f"] = valid_last3f.mean()
                        feature_row[f"{prefix}_min_last3f"] = valid_last3f.min()
                    else:
                        feature_row[f"{prefix}_avg_last3f"] = None
                        feature_row[f"{prefix}_min_last3f"] = None

                    # 走破タイム統計 (距離正規化)
                    valid_time = recent_n[["finish_time", "distance"]].dropna()
                    if len(valid_time) > 0:
                        # 1000mあたりのタイムに正規化
                        normalized_time = (
                            valid_time["finish_time"] / valid_time["distance"] * 1000
                        )
                        feature_row[f"{prefix}_avg_normalized_time"] = (
                            normalized_time.mean()
                        )
                    else:
                        feature_row[f"{prefix}_avg_normalized_time"] = None

                    # 勝率・複勝率
                    feature_row[f"{prefix}_win_rate"] = (
                        recent_n["finish_position"] == 1
                    ).mean()
                    feature_row[f"{prefix}_place_rate"] = (
                        recent_n["finish_position"] <= 3
                    ).mean()

                    # IDM平均
                    valid_idm = recent_n["idm"].dropna()
                    if len(valid_idm) > 0:
                        feature_row[f"{prefix}_avg_idm"] = valid_idm.mean()
                    else:
                        feature_row[f"{prefix}_avg_idm"] = None

                else:
                    # サンプル数不足の場合はNone
                    for col in [
                        "avg_position",
                        "min_position",
                        "max_position",
                        "std_position",
                        "avg_last3f",
                        "min_last3f",
                        "avg_normalized_time",
                        "win_rate",
                        "place_rate",
                        "avg_idm",
                    ]:
                        feature_row[f"{prefix}_{col}"] = None

            # 直近1走の情報
            if len(horse_data) >= 1:
                latest = horse_data.iloc[0]
                feature_row["last_finish_position"] = latest["finish_position"]
                feature_row["last_finish_time"] = latest["finish_time"]
                feature_row["last_last3f"] = latest["last_3f_time"]
                feature_row["last_distance"] = latest["distance"]
                feature_row["last_course_type"] = latest["course_type"]
                feature_row["last_track_condition"] = latest["track_condition"]
                feature_row["last_idm"] = latest["idm"]

                # 休養日数
                last_race_date = pd.to_datetime(latest["race_date"])
                target_dt = pd.to_datetime(target_date)
                feature_row["days_since_last_race"] = (target_dt - last_race_date).days

                # 4角通過順位
                feature_row["last_corner4_position"] = latest["corner_position_4"]
            else:
                feature_row["last_finish_position"] = None
                feature_row["last_finish_time"] = None
                feature_row["last_last3f"] = None
                feature_row["last_distance"] = None
                feature_row["last_course_type"] = None
                feature_row["last_track_condition"] = None
                feature_row["last_idm"] = None
                feature_row["days_since_last_race"] = None
                feature_row["last_corner4_position"] = None

            # トレンド指標 (直近3走の着順トレンド)
            if len(horse_data) >= 3:
                recent_3 = horse_data.head(3)["finish_position"].values
                # 改善傾向: 負の傾きは改善 (着順が下がっている)
                x = np.array([0, 1, 2])
                slope, _ = np.polyfit(x, recent_3, 1)
                feature_row["position_trend_3"] = slope
            else:
                feature_row["position_trend_3"] = None

            # キャリア情報
            feature_row["career_races"] = len(horse_data)
            feature_row["career_wins"] = (horse_data["finish_position"] == 1).sum()
            feature_row["career_places"] = (horse_data["finish_position"] <= 3).sum()

            features_list.append(feature_row)

        return pd.DataFrame(features_list)

    def generate_race_features(
        self,
        race_id: str,
        race_date: str,
    ) -> pd.DataFrame:
        """
        特定レースに出走する全馬の過去走特徴量を生成

        Args:
            race_id: レースID
            race_date: レース開催日

        Returns:
            レース出走馬の過去走特徴量DataFrame
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

        # 過去走特徴量を生成
        features = self.generate_features(race_date, horse_ids)

        if features.empty:
            return pd.DataFrame()

        # race_idを追加
        features["race_id"] = race_id

        return features

    def generate_distance_change_features(
        self,
        target_date: str,
        current_distance: int,
        horse_ids: list[str],
    ) -> pd.DataFrame:
        """
        距離変更に関する特徴量を生成

        Args:
            target_date: 基準日
            current_distance: 今回の距離
            horse_ids: 対象馬のIDリスト

        Returns:
            距離変更特徴量のDataFrame
        """
        # 前走データを取得
        horse_ids_str = ", ".join([f"'{h}'" for h in horse_ids])
        query = f"""
        WITH ranked AS (
            SELECT
                horse_id,
                distance,
                ROW_NUMBER() OVER (PARTITION BY horse_id ORDER BY race_date DESC) as rn
            FROM `{self.project_id}.raw.race_results`
            WHERE race_date < '{target_date}'
                AND horse_id IN ({horse_ids_str})
                AND abnormal_code = 0
        )
        SELECT horse_id, distance as last_distance
        FROM ranked
        WHERE rn = 1
        """

        last_distances = self.client.query(query).to_dataframe()

        if last_distances.empty:
            return pd.DataFrame()

        # 距離変更特徴量を計算
        last_distances["current_distance"] = current_distance
        last_distances["distance_change"] = (
            current_distance - last_distances["last_distance"]
        )
        last_distances["distance_change_ratio"] = (
            last_distances["distance_change"] / last_distances["last_distance"]
        )

        # 距離カテゴリ (短距離/マイル/中距離/長距離)
        def get_distance_category(d):
            if d < 1400:
                return "sprint"
            elif d < 1800:
                return "mile"
            elif d < 2200:
                return "intermediate"
            else:
                return "long"

        last_distances["last_distance_category"] = last_distances["last_distance"].apply(
            get_distance_category
        )
        last_distances["current_distance_category"] = get_distance_category(
            current_distance
        )
        last_distances["distance_category_change"] = (
            last_distances["last_distance_category"]
            != last_distances["current_distance_category"]
        ).astype(int)

        return last_distances[
            [
                "horse_id",
                "distance_change",
                "distance_change_ratio",
                "distance_category_change",
            ]
        ]
