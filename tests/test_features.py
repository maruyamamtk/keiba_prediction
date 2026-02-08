#!/usr/bin/env python3
"""
特徴量モジュールのテスト（SQL駆動パイプライン）
"""

import pytest
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, PropertyMock

from google.api_core import exceptions as google_exceptions

from src.ml.features.feature_pipeline import (
    FeaturePipeline,
    FeaturePipelineConfig,
    retry_with_backoff,
    SQL_TEMPLATE_PATH,
)


class TestFeaturePipelineConfig:
    """FeaturePipelineConfigのテスト"""

    def test_default_config(self):
        """デフォルト設定のテスト"""
        config = FeaturePipelineConfig()
        assert config.output_dataset == "features"
        assert config.output_table == "training_data"
        assert config.max_retries == 3
        assert config.retry_base_delay == 1.0
        assert config.retry_max_delay == 60.0

    def test_custom_config(self):
        """カスタム設定のテスト"""
        config = FeaturePipelineConfig(
            output_dataset="custom_features",
            output_table="custom_training",
            max_retries=5,
            retry_base_delay=2.0,
            retry_max_delay=120.0,
        )
        assert config.output_dataset == "custom_features"
        assert config.output_table == "custom_training"
        assert config.max_retries == 5
        assert config.retry_base_delay == 2.0
        assert config.retry_max_delay == 120.0


class TestSQLTemplate:
    """SQLテンプレートファイルのテスト"""

    def test_sql_template_file_exists(self):
        """SQLテンプレートファイルが存在すること"""
        assert SQL_TEMPLATE_PATH.exists(), (
            f"SQLテンプレートが見つかりません: {SQL_TEMPLATE_PATH}"
        )

    def test_sql_template_has_placeholders(self):
        """SQLテンプレートに必要なプレースホルダが含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "{project_id}" in content
        assert "{start_date}" in content
        assert "{end_date}" in content

    def test_sql_template_has_required_ctes(self):
        """SQLテンプレートに必要なCTEが含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_base_race_entries" in content
        assert "temp_past_race_features" in content
        assert "temp_past_race_features2" in content
        assert "temp_horse_master_feature" in content
        assert "temp_horse_master_feature2" in content

    def test_sql_template_no_duplicate_column_names(self):
        """SQLテンプレートに重複カラム名がないこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # sire_surface_place_rateは1回のみ（修正済み: 2つ目はsire_surface_place_ratio）
        assert content.count("as sire_surface_place_rate") == 1
        assert "sire_surface_place_ratio" in content

    def test_sql_template_references_required_tables(self):
        """SQLテンプレートが必要なテーブルを参照していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "raw.horse_results" in content
        assert "raw.race_info" in content
        assert "raw.horse_master" in content
        assert "raw.race_results" in content
        assert "raw.horse_extended" in content
        assert "raw.venue_info" in content

    def test_sql_template_no_hardcoded_dates(self):
        """SQLテンプレートにハードコードされた日付がないこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # パラメタライズされていない日付リテラルがないか確認
        # BETWEEN '{start_date}' AND '{end_date}' のみ許容
        import re
        date_literals = re.findall(r"= '\d{4}-\d{2}-\d{2}'", content)
        assert len(date_literals) == 0, (
            f"ハードコードされた日付が見つかりました: {date_literals}"
        )


class TestFeaturePipeline:
    """FeaturePipelineのテスト"""

    @pytest.fixture
    def mock_bq_client(self):
        """BigQueryクライアントのモック"""
        with patch("src.ml.features.feature_pipeline.bigquery.Client") as mock:
            yield mock

    def test_initialization(self, mock_bq_client):
        """パイプライン初期化のテスト"""
        pipeline = FeaturePipeline("test-project")

        assert pipeline.project_id == "test-project"
        assert pipeline.sql_template is not None
        assert len(pipeline.sql_template) > 0
        mock_bq_client.assert_called_once_with(project="test-project")

    def test_initialization_with_config(self, mock_bq_client):
        """カスタム設定での初期化テスト"""
        config = FeaturePipelineConfig(
            output_dataset="my_features",
            max_retries=5,
        )
        pipeline = FeaturePipeline("test-project", config)

        assert pipeline.config.output_dataset == "my_features"
        assert pipeline.config.max_retries == 5

    def test_build_query_substitutes_parameters(self, mock_bq_client):
        """クエリ生成時にパラメータが正しく置換されること"""
        pipeline = FeaturePipeline("my-gcp-project")
        query = pipeline._build_query("2025-01-01", "2025-01-31")

        assert "my-gcp-project" in query
        assert "2025-01-01" in query
        assert "2025-01-31" in query
        assert "{project_id}" not in query
        assert "{start_date}" not in query
        assert "{end_date}" not in query

    def test_build_query_date_in_between_clause(self, mock_bq_client):
        """日付がBETWEEN句に正しく埋め込まれること"""
        pipeline = FeaturePipeline("test-project")
        query = pipeline._build_query("2025-06-01", "2025-06-30")

        assert "BETWEEN '2025-06-01' AND '2025-06-30'" in query

    def test_generate_query_same_as_build_query(self, mock_bq_client):
        """generate_queryが_build_queryと同じ結果を返すこと"""
        pipeline = FeaturePipeline("test-project")
        q1 = pipeline._build_query("2025-01-01", "2025-01-31")
        q2 = pipeline.generate_query("2025-01-01", "2025-01-31")

        assert q1 == q2

    def test_run_delete_then_insert(self, mock_bq_client):
        """runメソッドがDELETE→INSERT順で実行されること"""
        mock_instance = MagicMock()
        mock_bq_client.return_value = mock_instance

        # get_tableが成功する（テーブルが存在する）
        mock_instance.get_table.return_value = MagicMock()

        # DELETE用のモック
        mock_delete_job = MagicMock()
        mock_delete_job.num_dml_affected_rows = 10
        mock_delete_job.result.return_value = None

        # INSERT用のモック
        mock_insert_result = MagicMock()
        mock_insert_result.total_rows = 150

        mock_insert_job = MagicMock()
        mock_insert_job.result.return_value = mock_insert_result

        # queryの呼び出し順を制御
        mock_instance.query.side_effect = [mock_delete_job, mock_insert_job]

        pipeline = FeaturePipeline("test-project")
        result = pipeline.run("2025-01-01", "2025-01-31")

        assert result["deleted_rows"] == 10
        assert result["inserted_rows"] == 150
        assert result["start_date"] == "2025-01-01"
        assert result["end_date"] == "2025-01-31"
        assert result["elapsed_time"] >= 0

        # queryが2回呼ばれる（DELETE + INSERT）
        assert mock_instance.query.call_count == 2

        # 1回目はDELETEクエリ
        delete_call = mock_instance.query.call_args_list[0]
        assert "DELETE FROM" in delete_call[0][0]

    def test_run_table_not_exists_skip_delete(self, mock_bq_client):
        """テーブルが存在しない場合、DELETEをスキップすること"""
        mock_instance = MagicMock()
        mock_bq_client.return_value = mock_instance

        # get_tableがNotFoundを返す
        mock_instance.get_table.side_effect = google_exceptions.NotFound("not found")

        # INSERT用のモック
        mock_insert_result = MagicMock()
        mock_insert_result.total_rows = 100

        mock_insert_job = MagicMock()
        mock_insert_job.result.return_value = mock_insert_result

        mock_instance.query.return_value = mock_insert_job

        pipeline = FeaturePipeline("test-project")
        result = pipeline.run("2025-01-01", "2025-01-31")

        assert result["deleted_rows"] == 0
        assert result["inserted_rows"] == 100

        # queryは1回のみ（INSERTのみ、DELETEはスキップ）
        assert mock_instance.query.call_count == 1

    def test_run_sets_time_partitioning(self, mock_bq_client):
        """runメソッドでrace_dateによるパーティション設定がされること"""
        mock_instance = MagicMock()
        mock_bq_client.return_value = mock_instance
        mock_instance.get_table.side_effect = google_exceptions.NotFound("not found")

        mock_insert_result = MagicMock()
        mock_insert_result.total_rows = 50
        mock_insert_job = MagicMock()
        mock_insert_job.result.return_value = mock_insert_result
        mock_instance.query.return_value = mock_insert_job

        pipeline = FeaturePipeline("test-project")
        pipeline.run("2025-01-01", "2025-01-31")

        # query呼び出しのjob_configを検証
        call_kwargs = mock_instance.query.call_args
        job_config = call_kwargs[1]["job_config"]
        assert job_config.time_partitioning.field == "race_date"

    def test_delete_existing_data_query_format(self, mock_bq_client):
        """DELETE文のフォーマットが正しいこと"""
        mock_instance = MagicMock()
        mock_bq_client.return_value = mock_instance
        mock_instance.get_table.return_value = MagicMock()

        mock_job = MagicMock()
        mock_job.num_dml_affected_rows = 5
        mock_job.result.return_value = None
        mock_instance.query.return_value = mock_job

        pipeline = FeaturePipeline("my-project")
        deleted = pipeline._delete_existing_data_if_table_exists(
            "my-project.features.training_data", "2025-01-01", "2025-01-31"
        )

        assert deleted == 5
        delete_query = mock_instance.query.call_args[0][0]
        assert "DELETE FROM" in delete_query
        assert "BETWEEN '2025-01-01' AND '2025-01-31'" in delete_query


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
            base_delay=0.01,
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

    def test_exponential_backoff_delay(self):
        """指数バックオフの遅延が正しいこと"""
        call_times = []

        def fail_func():
            call_times.append(time.time())
            raise ConnectionError("fail")

        with pytest.raises(ConnectionError):
            retry_with_backoff(
                fail_func,
                max_retries=2,
                base_delay=0.05,
                max_delay=10.0,
            )

        # 3回呼ばれるはず
        assert len(call_times) == 3
        # 1回目→2回目: 約0.05秒
        delay1 = call_times[1] - call_times[0]
        assert delay1 >= 0.04
        # 2回目→3回目: 約0.10秒 (2^1 * 0.05)
        delay2 = call_times[2] - call_times[1]
        assert delay2 >= 0.08
