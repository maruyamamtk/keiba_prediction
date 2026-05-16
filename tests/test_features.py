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

    def test_sql_template_has_rotation_features(self):
        """SQLテンプレートにローテーション特徴量が含まれること（Issue #268）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "is_fresh" in content, "is_fresh が見つかりません"
        assert "is_renso" in content, "is_renso が見つかりません"
        assert "idm_trend_3" in content, "idm_trend_3 が見つかりません"
        assert "finish_position_trend_3" in content, "finish_position_trend_3 が見つかりません"
        assert "weight_carried_diff" in content, "weight_carried_diff が見つかりません"
        assert "continuous_run_count" in content, "continuous_run_count が見つかりません"

    def test_sql_rotation_features_threshold_values(self):
        """ローテーション特徴量の閾値が正しいこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # is_fresh の閾値が12週であること
        assert "race_date_diff_1 >= 12" in content, "is_fresh の閾値(12週)が見つかりません"
        # is_renso の閾値が1週以下であること
        assert "race_date_diff_1 <= 1" in content, "is_renso の閾値(1週)が見つかりません"

    def test_sql_continuous_run_count_uses_5_race_diffs(self):
        """continuous_run_count が5走前まで参照していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "race_date_diff_3" in content
        assert "race_date_diff_4" in content
        assert "race_date_diff_5" in content

    def test_sql_rotation_features_no_future_data_leak(self):
        """ローテーション特徴量に未来データ漏洩がないこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # 全ての過去走JOIN（r_r_1〜r_r_5）に race_date < t_b_r_e.race_date 条件があること
        import re
        future_guards = re.findall(
            r"r_r_\d\.race_date < t_b_r_e\.race_date", content
        )
        assert len(future_guards) == 5, (
            f"時系列境界ガードが5つあるべき所: {len(future_guards)}つしかありません"
        )

    def test_sql_weight_carried_diff_uses_past_race(self):
        """weight_carried_diff が r_r_1（過去走）の斤量を参照していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "r_r_1.weight_carried" in content, (
            "weight_carried_diff が r_r_1.weight_carried を参照していません"
        )

    def test_sql_has_te_ctes(self):
        """Target Encoding用のCTEが存在すること（Issue #270）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_global_mean_te" in content, "グローバル平均CTE が見つかりません"
        assert "temp_te_history_base" in content, "TE履歴ベースCTE が見つかりません"
        assert "temp_jockey_te" in content, "騎手TE CTE が見つかりません"
        assert "temp_trainer_te" in content, "調教師TE CTE が見つかりません"
        assert "temp_sire_te" in content, "種牡馬TE CTE が見つかりません"

    def test_sql_te_columns_present(self):
        """TE特徴量カラムが最終SELECTに含まれること（Issue #270）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        expected_columns = [
            "jockey_te",
            "jockey_course_type_te",
            "jockey_venue_te",
            "jockey_distance_te",
            "jockey_direction_te",
            "jockey_course_type_venue_te",
            "jockey_course_type_distance_te",
            "jockey_course_type_distance_venue_te",
            "trainer_te",
            "trainer_course_type_te",
            "trainer_venue_te",
            "trainer_distance_te",
            "trainer_direction_te",
            "trainer_course_type_venue_te",
            "trainer_course_type_distance_te",
            "trainer_course_type_distance_venue_te",
            "sire_te",
            "sire_course_type_te",
            "sire_venue_te",
            "sire_distance_te",
            "sire_direction_te",
            "sire_course_type_venue_te",
            "sire_course_type_distance_te",
            "sire_course_type_distance_venue_te",
        ]
        for col in expected_columns:
            assert col in content, f"TE特徴量カラム '{col}' が見つかりません"

    def test_sql_te_window_uses_range_for_same_day_exclusion(self):
        """TE Window関数がRANGEを使って同日レースを除外すること（Issue #270）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "range between unbounded preceding and 1 preceding" in content, (
            "TE Window関数で同日除外（RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING）が見つかりません"
        )

    def test_sql_te_window_uses_unix_date(self):
        """TE Window関数がunix_dateで整数変換していること（同日を整数1単位として除外）（Issue #270）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "unix_date(race_date)" in content, (
            "TE Window関数で unix_date(race_date) が見つかりません"
        )

    def test_sql_te_history_base_excludes_non_finishers(self):
        """TE履歴ベースが取消・除外馬を除外していること（Issue #270）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_base_section = content[
            content.find("temp_te_history_base"): content.find("temp_jockey_te")
        ]
        assert "finish_position > 0" in te_base_section, (
            "temp_te_history_base で finish_position > 0 フィルタが見つかりません"
        )

    def test_sql_te_history_base_joins_required_tables(self):
        """TE履歴ベースが必要なテーブルをJOINしていること（Issue #270）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_base_section = content[
            content.find("temp_te_history_base"): content.find("temp_jockey_te")
        ]
        assert "raw.race_results" in te_base_section
        assert "raw.race_info" in te_base_section
        assert "raw.horse_results" in te_base_section
        assert "raw.horse_master" in te_base_section

    def test_sql_te_final_join_uses_race_id_and_horse_number(self):
        """最終SELECTでTEテーブルをrace_id+horse_numberでJOINしていること（Issue #270）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "left join temp_jockey_te as t_j_te" in content
        assert "left join temp_trainer_te as t_tr_te" in content
        assert "left join temp_sire_te as t_s_te" in content
        # JOINキーの確認
        assert "t_j_te.race_id" in content
        assert "t_j_te.horse_number" in content

    def test_sql_te_global_mean_from_race_results(self):
        """グローバル平均がrace_resultsから計算されること（Issue #270）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        global_mean_section = content[
            content.find("temp_global_mean_te"): content.find("temp_te_history_base")
        ]
        assert "raw.race_results" in global_mean_section, (
            "temp_global_mean_te で raw.race_results が参照されていません"
        )
        assert "finish_position between 1 and 3" in global_mean_section, (
            "グローバル平均で 3着以内判定 (finish_position between 1 and 3) が見つかりません"
        )

    def test_sql_has_distance_band_in_base_entries(self):
        """temp_base_race_entries に distance_band が含まれること（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        base_section = content[
            content.find("temp_base_race_entries"): content.find("temp_past_race_features")
        ]
        assert "distance_band" in base_section, "distance_band が temp_base_race_entries に見つかりません"
        assert "'sprint'" in base_section, "距離帯 sprint の定義が見つかりません"
        assert "'mile'" in base_section, "距離帯 mile の定義が見つかりません"
        assert "'intermediate'" in base_section, "距離帯 intermediate の定義が見つかりません"
        assert "'long'" in base_section, "距離帯 long の定義が見つかりません"

    def test_sql_distance_band_thresholds_correct(self):
        """distance_band の閾値が仕様通りであること（sprint<1400, mile<1800, intermediate<2200）（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        base_section = content[
            content.find("temp_base_race_entries"): content.find("temp_past_race_features")
        ]
        assert "distance < 1400" in base_section, "sprint の閾値 <1400 が見つかりません"
        assert "distance < 1800" in base_section, "mile の閾値 <1800 が見つかりません"
        assert "distance < 2200" in base_section, "intermediate の閾値 <2200 が見つかりません"

    def test_sql_has_distance_change_features(self):
        """temp_past_race_features に distance_change と distance_change_flag が含まれること（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        past_section = content[
            content.find("temp_past_race_features"): content.find("temp_past_race_features2")
        ]
        assert "distance_change" in past_section, "distance_change が見つかりません"
        assert "distance_change_flag" in past_section, "distance_change_flag が見つかりません"

    def test_sql_distance_change_uses_past_race_distance(self):
        """distance_change が当日距離と1走前距離の差分を使っていること（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "t_b_r_e.distance - r_r_1.distance" in content, (
            "distance_change の計算式 (t_b_r_e.distance - r_r_1.distance) が見つかりません"
        )

    def test_sql_distance_change_flag_handles_null(self):
        """distance_change_flag が NULL（初出走）を正しく扱うこと（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "r_r_1.distance is null then null" in content, (
            "distance_change_flag の NULL ハンドリングが見つかりません"
        )

    def test_sql_has_horse_distance_ctes(self):
        """距離帯別・距離別 TE 用 CTE が存在すること（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_horse_distance_base" in content, "temp_horse_distance_base CTE が見つかりません"
        assert "temp_horse_distance_band_te" in content, "temp_horse_distance_band_te CTE が見つかりません"
        assert "temp_horse_distance_te" in content, "temp_horse_distance_te CTE が見つかりません"

    def test_sql_horse_distance_band_te_columns_present(self):
        """距離帯別特徴量が最終 SELECT に含まれること（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        expected = [
            "distance_band_top3_finish_rate",
            "distance_band_top1_finish_rate",
            "distance_band_rate_diff",
            "new_distance_band_flag",
        ]
        for col in expected:
            assert col in content, f"距離帯別特徴量 '{col}' が見つかりません"

    def test_sql_horse_distance_te_columns_present(self):
        """距離別特徴量が最終 SELECT に含まれること（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        expected = [
            "distance_top3_finish_rate",
            "distance_top1_finish_rate",
            "distance_rate_diff",
            "new_distance_flag",
        ]
        for col in expected:
            assert col in content, f"距離別特徴量 '{col}' が見つかりません"

    def test_sql_horse_distance_base_excludes_non_finishers(self):
        """temp_horse_distance_base が取消・除外馬を除外していること（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        base_section = content[
            content.find("temp_horse_distance_base"): content.find("temp_horse_distance_band_te")
        ]
        assert "finish_position > 0" in base_section, (
            "temp_horse_distance_base で finish_position > 0 フィルタが見つかりません"
        )

    def test_sql_horse_distance_te_uses_range_window(self):
        """距離 TE Window 関数が RANGE で同日除外していること（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        dist_te_section = content[content.find("temp_horse_distance_band_te"):]
        assert "range between unbounded preceding and 1 preceding" in dist_te_section, (
            "距離 TE Window 関数で同日除外 (RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) が見つかりません"
        )

    def test_sql_horse_distance_te_partitions_by_horse_id(self):
        """距離 TE が horse_id で正しくパーティションされていること（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        dist_section = content[content.find("temp_horse_distance_band_te"):]
        assert "partition by horse_id, distance_band" in dist_section, (
            "距離帯 TE で 'partition by horse_id, distance_band' が見つかりません"
        )
        assert "partition by horse_id, distance" in dist_section, (
            "距離 TE で 'partition by horse_id, distance' が見つかりません"
        )

    def test_sql_horse_distance_te_final_joins(self):
        """距離帯別・距離別 TE が最終 SELECT で JOIN されていること（Issue #271）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "left join temp_horse_distance_band_te as t_h_db_te" in content, (
            "temp_horse_distance_band_te の JOIN が見つかりません"
        )
        assert "left join temp_horse_distance_te as t_h_d_te" in content, (
            "temp_horse_distance_te の JOIN が見つかりません"
        )

    def test_sql_normalized_features_have_time_boundary(self):
        """finish_time_normalized と last_3f_normalized が時系列ガードを持つこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # ORDER BY race_date + RANGE が両方の正規化に適用されていることを確認
        assert content.count("range between unbounded preceding and current row") >= 2, (
            "finish_time_normalized または last_3f_normalized の時系列ガードが不足しています"
        )
        # 全体集計（ORDER BY なし）が残っていないことを確認
        import re
        bad_windows = re.findall(
            r"partition by t_p_r_f\.venue_code_prev_1.*?(?:\n.*?){0,2}(?<!\border by\b)",
            content,
        )
        # finish_time_normalized と last_3f_normalized の partition に order by が付いていること
        finish_idx = content.find("finish_time_normalized")
        last3f_idx = content.find("last_3f_normalized")
        finish_section = content[max(0, finish_idx - 300): finish_idx + 50]
        last3f_section = content[max(0, last3f_idx - 300): last3f_idx + 50]
        assert "order by t_p_r_f.race_date" in finish_section, (
            "finish_time_normalized のウィンドウに ORDER BY race_date がありません"
        )
        assert "order by t_p_r_f.race_date" in last3f_section, (
            "last_3f_normalized のウィンドウに ORDER BY race_date がありません"
        )

    def test_sql_has_intra_race_relative_features(self):
        """レース内相対指標が SQL に含まれること（Issue #269）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        expected = [
            "weight_carried_rank",
            "weight_carried_diff_from_mean",
            "running_style_front_count",
            "running_style_front_ratio",
            "is_sole_leader",
            "race_idm_std",
            "race_idm_cv",
            "horse_number_ratio",
        ]
        for col in expected:
            assert col in content, f"レース内相対指標 '{col}' が見つかりません"

    def test_sql_intra_race_features_use_race_id_partition(self):
        """レース内相対指標が race_id でパーティションされていること（Issue #269）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # weight_carried_rank の Window 関数が race_id でパーティションされていること
        rank_idx = content.find("weight_carried_rank")
        rank_section = content[max(0, rank_idx - 200): rank_idx + 10]
        assert "partition by t_p_r_f.race_id" in rank_section, (
            "weight_carried_rank が race_id でパーティションされていません"
        )

    def test_sql_intra_race_features_no_time_window(self):
        """レース内相対指標に time-series window がないこと（事前情報のみ使用）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # race_idm_std の周辺に ORDER BY race_date が存在しないことを確認
        std_idx = content.find("race_idm_std")
        std_section = content[max(0, std_idx - 100): std_idx + 200]
        assert "order by" not in std_section, (
            "race_idm_std のウィンドウに不要な ORDER BY が含まれています（事前情報のみ使用すべき）"
        )

    def test_sql_is_sole_leader_checks_running_style_1(self):
        """is_sole_leader が running_style = 1（逃げ）のみを対象にしていること（Issue #269）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        sole_idx = content.find("is_sole_leader")
        sole_section = content[max(0, sole_idx - 300): sole_idx + 10]
        assert "running_style = 1" in sole_section, (
            "is_sole_leader が running_style = 1 を参照していません"
        )

    def test_sql_horse_number_ratio_uses_num_horses(self):
        """horse_number_ratio が num_horses で除算していること（Issue #269）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        ratio_idx = content.find("horse_number_ratio")
        ratio_section = content[max(0, ratio_idx - 100): ratio_idx + 10]
        assert "num_horses" in ratio_section, (
            "horse_number_ratio が num_horses を使用していません"
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

    def test_build_query_rejects_invalid_start_date(self, mock_bq_client):
        """不正な開始日が拒否されること"""
        pipeline = FeaturePipeline("test-project")
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            pipeline._build_query("2025/01/01", "2025-01-31")

    def test_build_query_rejects_invalid_end_date(self, mock_bq_client):
        """不正な終了日が拒否されること"""
        pipeline = FeaturePipeline("test-project")
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            pipeline._build_query("2025-01-01", "20250131")

    def test_build_query_rejects_sql_injection(self, mock_bq_client):
        """SQLインジェクション的な入力が拒否されること"""
        pipeline = FeaturePipeline("test-project")
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            pipeline._build_query("2025-01-01'; DROP TABLE --", "2025-01-31")

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
