#!/usr/bin/env python3
"""
特徴量モジュールのテスト
"""

import pytest
import pandas as pd
import numpy as np
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import date

from src.features.past_performance import (
    PastPerformanceFeatures,
    PastPerformanceConfig,
)
from src.features.condition_features import (
    ConditionFeatures,
    ConditionConfig,
)
from src.features.feature_pipeline import (
    FeaturePipeline,
    FeaturePipelineConfig,
    ProgressTracker,
    retry_with_backoff,
)


class TestPastPerformanceConfig:
    """PastPerformanceConfigのテスト"""

    def test_default_config(self):
        """デフォルト設定のテスト"""
        config = PastPerformanceConfig()
        assert config.n_past_races == [3, 5, 10]
        assert config.smoothing_min_samples == 3

    def test_custom_config(self):
        """カスタム設定のテスト"""
        config = PastPerformanceConfig(
            n_past_races=[3, 5],
            smoothing_min_samples=5,
        )
        assert config.n_past_races == [3, 5]
        assert config.smoothing_min_samples == 5


class TestPastPerformanceFeatures:
    """PastPerformanceFeaturesのテスト"""

    @pytest.fixture
    def mock_client(self):
        """BigQueryクライアントのモック"""
        with patch("src.features.past_performance.bigquery.Client") as mock:
            yield mock

    @pytest.fixture
    def sample_past_results(self):
        """サンプル過去走データ"""
        return pd.DataFrame(
            {
                "horse_id": ["H001"] * 5 + ["H002"] * 3,
                "race_id": [f"R{i}" for i in range(5)] + [f"R{i}" for i in range(3)],
                "race_date": pd.to_datetime(
                    [
                        "2024-01-10",
                        "2024-01-05",
                        "2024-01-01",
                        "2023-12-25",
                        "2023-12-20",
                        "2024-01-08",
                        "2024-01-03",
                        "2023-12-28",
                    ]
                ),
                "finish_position": [1, 3, 2, 5, 4, 2, 1, 3],
                "finish_time": [120.0, 121.5, 120.5, 122.0, 121.0, 119.5, 120.0, 121.0],
                "last_3f_time": [34.0, 35.0, 34.5, 36.0, 35.5, 33.5, 34.0, 34.5],
                "distance": [1600] * 8,
                "course_type": ["芝"] * 8,
                "track_condition": ["良"] * 8,
                "weight_carried": [56.0] * 8,
                "corner_position_4": [3, 5, 4, 8, 7, 2, 3, 4],
                "idm": [65.0, 62.0, 64.0, 58.0, 60.0, 68.0, 66.0, 64.0],
                "agari_index": [50.0, 48.0, 49.0, 45.0, 46.0, 52.0, 51.0, 50.0],
                "ten_index": [48.0, 47.0, 48.0, 44.0, 45.0, 50.0, 49.0, 48.0],
            }
        )

    def test_calculate_features(self, mock_client, sample_past_results):
        """特徴量計算のテスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        mock_instance.query.return_value.to_dataframe.return_value = sample_past_results

        features = PastPerformanceFeatures("test-project")
        result = features._calculate_features(sample_past_results, "2024-01-15")

        # H001の特徴量を確認
        h001 = result[result["horse_id"] == "H001"].iloc[0]
        assert h001["past_3_avg_position"] == pytest.approx(2.0, rel=0.01)
        assert h001["past_5_avg_position"] == pytest.approx(3.0, rel=0.01)
        assert h001["career_races"] == 5
        assert h001["career_wins"] == 1
        assert h001["career_places"] == 3
        assert h001["last_finish_position"] == 1

        # H002の特徴量を確認
        h002 = result[result["horse_id"] == "H002"].iloc[0]
        assert h002["past_3_avg_position"] == pytest.approx(2.0, rel=0.01)
        assert h002["career_races"] == 3

    def test_position_trend(self, mock_client, sample_past_results):
        """着順トレンドのテスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        mock_instance.query.return_value.to_dataframe.return_value = sample_past_results

        features = PastPerformanceFeatures("test-project")
        result = features._calculate_features(sample_past_results, "2024-01-15")

        h001 = result[result["horse_id"] == "H001"].iloc[0]
        # 直近3走: 1, 3, 2 → 改善傾向 (1が最新)
        # 回帰係数を計算: x=[0,1,2], y=[1,3,2]
        assert h001["position_trend_3"] is not None

    def test_days_since_last_race(self, mock_client, sample_past_results):
        """休養日数のテスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        mock_instance.query.return_value.to_dataframe.return_value = sample_past_results

        features = PastPerformanceFeatures("test-project")
        result = features._calculate_features(sample_past_results, "2024-01-15")

        h001 = result[result["horse_id"] == "H001"].iloc[0]
        # 2024-01-15 - 2024-01-10 = 5日
        assert h001["days_since_last_race"] == 5


class TestConditionConfig:
    """ConditionConfigのテスト"""

    def test_default_config(self):
        """デフォルト設定のテスト"""
        config = ConditionConfig()
        assert config.smoothing_factor == 10.0
        assert config.min_samples == 3
        assert "spring" in config.seasons
        assert config.seasons["spring"] == [3, 4, 5]

    def test_custom_config(self):
        """カスタム設定のテスト"""
        config = ConditionConfig(
            smoothing_factor=5.0,
            min_samples=5,
        )
        assert config.smoothing_factor == 5.0
        assert config.min_samples == 5


class TestConditionFeatures:
    """ConditionFeaturesのテスト"""

    @pytest.fixture
    def mock_client(self):
        """BigQueryクライアントのモック"""
        with patch("src.features.condition_features.bigquery.Client") as mock:
            yield mock

    def test_get_distance_category(self, mock_client):
        """距離カテゴリのテスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        features = ConditionFeatures("test-project")

        assert features._get_distance_category(1200) == "sprint"
        assert features._get_distance_category(1600) == "mile"
        assert features._get_distance_category(2000) == "intermediate"
        assert features._get_distance_category(2400) == "long"
        assert features._get_distance_category(None) == "unknown"

    def test_get_track_category(self, mock_client):
        """馬場カテゴリのテスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        features = ConditionFeatures("test-project")

        assert features._get_track_category("良") == "good"
        assert features._get_track_category("稍重") == "heavy"
        assert features._get_track_category("重") == "heavy"
        assert features._get_track_category("不良") == "heavy"
        assert features._get_track_category(None) == "unknown"

    def test_get_season(self, mock_client):
        """季節のテスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        features = ConditionFeatures("test-project")

        assert features._get_season(3) == "spring"
        assert features._get_season(6) == "summer"
        assert features._get_season(9) == "autumn"
        assert features._get_season(12) == "winter"
        assert features._get_season(1) == "winter"

    def test_safe_column_name(self, mock_client):
        """カラム名変換のテスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        features = ConditionFeatures("test-project")

        assert features._safe_column_name("芝") == "turf"
        assert features._safe_column_name("ダート") == "dirt"
        assert features._safe_column_name("障害") == "jump"
        assert features._safe_column_name("unknown") == "unknown"

    def test_calculate_smoothed_rates(self, mock_client):
        """平滑化された勝率計算のテスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        features = ConditionFeatures("test-project")
        global_stats = {
            "global_win_rate": 0.1,
            "global_place_rate": 0.3,
        }

        # テストデータ: 10レース中、勝利2回、複勝5回
        data = pd.DataFrame(
            {
                "finish_position": [1, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            }
        )

        win_rate, place_rate = features._calculate_smoothed_rates(data, global_stats)

        # 平滑化: (n * sample_rate + m * global_rate) / (n + m)
        # win: (2 + 10 * 0.1) / (10 + 10) = 3 / 20 = 0.15
        # place: (4 + 10 * 0.3) / (10 + 10) = 7 / 20 = 0.35
        assert win_rate == pytest.approx(0.15, rel=0.01)
        assert place_rate == pytest.approx(0.35, rel=0.01)

    def test_calculate_smoothed_rates_empty(self, mock_client):
        """空データの平滑化率テスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        features = ConditionFeatures("test-project")
        global_stats = {
            "global_win_rate": 0.1,
            "global_place_rate": 0.3,
        }

        data = pd.DataFrame({"finish_position": []})
        win_rate, place_rate = features._calculate_smoothed_rates(data, global_stats)

        assert win_rate is None
        assert place_rate is None


class TestFeaturePipelineConfig:
    """FeaturePipelineConfigのテスト"""

    def test_default_config(self):
        """デフォルト設定のテスト"""
        config = FeaturePipelineConfig()
        assert config.output_dataset == "features"
        assert config.output_table == "training_data"


class TestFeaturePipeline:
    """FeaturePipelineのテスト"""

    @pytest.fixture
    def mock_clients(self):
        """BigQueryクライアントのモック"""
        with patch("src.features.feature_pipeline.bigquery.Client") as mock_pipeline, patch(
            "src.features.past_performance.bigquery.Client"
        ) as mock_past, patch(
            "src.features.condition_features.bigquery.Client"
        ) as mock_cond:
            yield mock_pipeline, mock_past, mock_cond

    def test_get_output_columns(self, mock_clients):
        """出力カラム選択のテスト"""
        mock_pipeline, mock_past, mock_cond = mock_clients

        pipeline = FeaturePipeline("test-project")

        # 必須カラムと一部のオプショナルカラムを含むDataFrame
        features = pd.DataFrame(
            {
                "race_id": ["R001"],
                "horse_id": ["H001"],
                "race_date": ["2024-01-15"],
                "finish_position": [1],
                "past_3_avg_position": [2.5],
                "turf_win_rate": [0.2],
                "extra_column": ["should_not_be_included"],
            }
        )

        output_cols = pipeline._get_output_columns(features)

        # 必須カラムが含まれる
        assert "race_id" in output_cols
        assert "horse_id" in output_cols
        assert "race_date" in output_cols

        # オプショナルカラムが含まれる
        assert "finish_position" in output_cols
        assert "past_3_avg_position" in output_cols
        assert "turf_win_rate" in output_cols

        # 未定義カラムは含まれない
        assert "extra_column" not in output_cols


class TestDistanceChangeFeatures:
    """距離変更特徴量のテスト"""

    @pytest.fixture
    def mock_client(self):
        """BigQueryクライアントのモック"""
        with patch("src.features.past_performance.bigquery.Client") as mock:
            yield mock

    def test_distance_change_calculation(self, mock_client):
        """距離変更計算のテスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        # モックデータ
        last_distances = pd.DataFrame(
            {
                "horse_id": ["H001", "H002"],
                "last_distance": [1600, 2000],
            }
        )
        mock_instance.query.return_value.to_dataframe.return_value = last_distances

        features = PastPerformanceFeatures("test-project")
        result = features.generate_distance_change_features(
            "2024-01-15",
            1800,  # 今回の距離
            ["H001", "H002"],
        )

        # H001: 1600 -> 1800 (延長)
        h001 = result[result["horse_id"] == "H001"].iloc[0]
        assert h001["distance_change"] == 200
        assert h001["distance_change_ratio"] == pytest.approx(0.125, rel=0.01)

        # H002: 2000 -> 1800 (短縮)
        h002 = result[result["horse_id"] == "H002"].iloc[0]
        assert h002["distance_change"] == -200
        assert h002["distance_change_ratio"] == pytest.approx(-0.1, rel=0.01)


class TestRunningStyleFeatures:
    """脚質特徴量のテスト"""

    @pytest.fixture
    def mock_client(self):
        """BigQueryクライアントのモック"""
        with patch("src.features.condition_features.bigquery.Client") as mock:
            yield mock

    def test_running_style_query_called(self, mock_client):
        """脚質クエリが呼ばれることのテスト"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        mock_instance.query.return_value.to_dataframe.return_value = pd.DataFrame()

        features = ConditionFeatures("test-project")
        features.get_running_style_features("2024-01-15", ["H001", "H002"])

        # クエリが呼ばれたことを確認
        mock_instance.query.assert_called_once()
        call_args = mock_instance.query.call_args[0][0]
        assert "corner_position_4" in call_args
        assert "H001" in call_args
        assert "H002" in call_args


class TestProgressTracker:
    """ProgressTrackerのテスト"""

    def test_initial_state(self):
        """初期状態のテスト"""
        tracker = ProgressTracker(total_items=100)

        assert tracker.total_items == 100
        assert tracker.processed_items == 0
        assert tracker.successful_items == 0
        assert tracker.failed_items == 0
        assert tracker.progress_percent == 0.0

    def test_update_success(self):
        """成功時の更新テスト"""
        tracker = ProgressTracker(total_items=100)
        tracker.start()

        tracker.update(success=True)

        assert tracker.processed_items == 1
        assert tracker.successful_items == 1
        assert tracker.failed_items == 0
        assert tracker.progress_percent == 1.0

    def test_update_failure(self):
        """失敗時の更新テスト"""
        tracker = ProgressTracker(total_items=100)
        tracker.start()

        tracker.update(success=False)

        assert tracker.processed_items == 1
        assert tracker.successful_items == 0
        assert tracker.failed_items == 1

    def test_progress_percent(self):
        """進捗率のテスト"""
        tracker = ProgressTracker(total_items=10)

        for _ in range(5):
            tracker.update(success=True)

        assert tracker.progress_percent == 50.0

    def test_progress_percent_zero_total(self):
        """総数0の場合の進捗率テスト"""
        tracker = ProgressTracker(total_items=0)
        assert tracker.progress_percent == 100.0

    def test_elapsed_time(self):
        """経過時間のテスト"""
        tracker = ProgressTracker(total_items=10)
        tracker.start()

        time.sleep(0.1)
        elapsed = tracker.elapsed_time

        assert elapsed >= 0.1

    def test_elapsed_time_before_start(self):
        """開始前の経過時間テスト"""
        tracker = ProgressTracker(total_items=10)
        assert tracker.elapsed_time == 0.0

    def test_estimated_remaining_time(self):
        """推定残り時間のテスト"""
        tracker = ProgressTracker(total_items=10)
        tracker.start()

        # 5件処理後
        for _ in range(5):
            tracker.update(success=True)

        remaining = tracker.estimated_remaining_time

        # 残り5件なので、推定時間は経過時間と同程度
        assert remaining is not None
        assert remaining >= 0

    def test_estimated_remaining_time_no_processed(self):
        """処理前の推定残り時間テスト"""
        tracker = ProgressTracker(total_items=10)
        tracker.start()

        assert tracker.estimated_remaining_time is None

    def test_get_status_message(self):
        """ステータスメッセージのテスト"""
        tracker = ProgressTracker(total_items=10)
        tracker.start()

        for _ in range(3):
            tracker.update(success=True)
        tracker.update(success=False)

        message = tracker.get_status_message()

        assert "[4/10]" in message
        assert "40.0%" in message
        assert "成功: 3" in message
        assert "失敗: 1" in message


class TestRetryWithBackoff:
    """retry_with_backoffのテスト"""

    def test_success_first_try(self):
        """初回成功のテスト"""
        call_count = 0

        def success_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = retry_with_backoff(success_func, max_retries=3)

        assert result == "success"
        assert call_count == 1

    def test_success_after_retry(self):
        """リトライ後成功のテスト"""
        call_count = 0

        def fail_then_success():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Connection failed")
            return "success"

        result = retry_with_backoff(
            fail_then_success,
            max_retries=3,
            base_delay=0.01,  # テスト用に短い待機時間
        )

        assert result == "success"
        assert call_count == 3

    def test_all_retries_fail(self):
        """全リトライ失敗のテスト"""
        call_count = 0

        def always_fail():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Connection failed")

        with pytest.raises(ConnectionError):
            retry_with_backoff(
                always_fail,
                max_retries=2,
                base_delay=0.01,
            )

        assert call_count == 3  # 初回 + 2回リトライ

    def test_non_retryable_exception(self):
        """リトライ対象外例外のテスト"""
        call_count = 0

        def raise_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not retryable")

        with pytest.raises(ValueError):
            retry_with_backoff(
                raise_value_error,
                max_retries=3,
                exceptions=(ConnectionError,),
            )

        assert call_count == 1  # リトライせず1回のみ


class TestFeaturePipelineConfigEnhanced:
    """拡張されたFeaturePipelineConfigのテスト"""

    def test_default_parallel_settings(self):
        """デフォルトの並列処理設定テスト"""
        config = FeaturePipelineConfig()

        assert config.max_workers == 4
        assert config.max_retries == 3
        assert config.retry_base_delay == 1.0
        assert config.retry_max_delay == 60.0
        assert config.progress_log_interval == 10

    def test_custom_parallel_settings(self):
        """カスタム並列処理設定テスト"""
        config = FeaturePipelineConfig(
            max_workers=8,
            max_retries=5,
            retry_base_delay=2.0,
            progress_log_interval=20,
        )

        assert config.max_workers == 8
        assert config.max_retries == 5
        assert config.retry_base_delay == 2.0
        assert config.progress_log_interval == 20


class TestIntegration:
    """統合テスト"""

    @pytest.fixture
    def mock_all_clients(self):
        """全クライアントのモック"""
        with patch("src.features.feature_pipeline.bigquery.Client") as mock1, patch(
            "src.features.past_performance.bigquery.Client"
        ) as mock2, patch(
            "src.features.condition_features.bigquery.Client"
        ) as mock3:
            for mock in [mock1, mock2, mock3]:
                mock_instance = MagicMock()
                mock.return_value = mock_instance
                mock_instance.query.return_value.to_dataframe.return_value = (
                    pd.DataFrame()
                )
            yield mock1, mock2, mock3

    def test_pipeline_initialization(self, mock_all_clients):
        """パイプライン初期化のテスト"""
        pipeline = FeaturePipeline("test-project")

        assert pipeline.project_id == "test-project"
        assert pipeline.past_perf is not None
        assert pipeline.condition is not None

    def test_pipeline_empty_races(self, mock_all_clients):
        """レースが存在しない場合のテスト"""
        mock_pipeline, _, _ = mock_all_clients
        mock_instance = MagicMock()
        mock_pipeline.return_value = mock_instance
        mock_instance.query.return_value.to_dataframe.return_value = pd.DataFrame()

        pipeline = FeaturePipeline("test-project")
        result = pipeline.run("2024-01-01", "2024-01-31")

        assert result["total_races"] == 0
        assert result["processed_races"] == 0
        assert result["errors"] == 0
        assert "elapsed_time" in result

    def test_pipeline_parallel_option(self, mock_all_clients):
        """並列/順次処理オプションのテスト"""
        mock_pipeline, _, _ = mock_all_clients
        mock_instance = MagicMock()
        mock_pipeline.return_value = mock_instance
        mock_instance.query.return_value.to_dataframe.return_value = pd.DataFrame()

        pipeline = FeaturePipeline("test-project")

        # 並列処理
        result_parallel = pipeline.run("2024-01-01", "2024-01-31", parallel=True)
        assert result_parallel["total_races"] == 0

        # 順次処理
        result_sequential = pipeline.run("2024-01-01", "2024-01-31", parallel=False)
        assert result_sequential["total_races"] == 0
