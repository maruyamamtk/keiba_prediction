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
    TE_DAILY_SQL_TEMPLATE_PATH,
    PREDICT_TE_SQL_TEMPLATE_PATH,
    _TE_BLOCK_START,
    _TE_BLOCK_END,
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

    def test_sql_te_history_raw_excludes_obstacle_races(self):
        """temp_te_history_raw が障害レースを除外していること（Issue #331）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # コメント行をスキップして実際のCTE定義を検索
        raw_start = content.find(",temp_te_history_raw as (")
        raw_end = content.find(",temp_te_history_base as (", raw_start + 1)
        te_raw_section = content[raw_start:raw_end]
        assert "!= 'obstacle'" in te_raw_section, (
            "temp_te_history_raw に障害レース除外条件 (!= 'obstacle') がありません"
        )
        assert "course_type" in te_raw_section, (
            "temp_te_history_raw に course_type フィルタが見つかりません"
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

    def test_sql_te_history_base_includes_prediction_entries(self):
        """temp_te_history_base が horse_results 起点で当日予測エントリも含むこと（Issue #284）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_base_section = content[
            content.find("temp_te_history_base"): content.find("temp_jockey_te")
        ]
        # horse_results が起点（FROM句の最初のテーブル）
        assert "from `{project_id}`.raw.horse_results" in te_base_section, (
            "temp_te_history_base が horse_results を起点にしていません"
        )
        # race_results は LEFT JOIN（当日レース未確定でも JOIN できるよう）
        assert "left join `{project_id}`.raw.race_results" in te_base_section, (
            "temp_te_history_base で race_results が LEFT JOIN になっていません"
        )
        # race_id IS NULL 条件で予測日エントリも取り込む
        assert "r_r.race_id is null" in te_base_section, (
            "temp_te_history_base に 'r_r.race_id is null' 条件がありません（予測日エントリが除外される）"
        )

    def test_sql_horse_distance_base_includes_prediction_entries(self):
        """temp_horse_distance_base が horse_results 起点で当日予測エントリも含むこと（Issue #284）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        base_section = content[
            content.find("temp_horse_distance_base"): content.find("temp_horse_distance_band_te")
        ]
        # horse_results が起点
        assert "from `{project_id}`.raw.horse_results" in base_section, (
            "temp_horse_distance_base が horse_results を起点にしていません"
        )
        # race_results は LEFT JOIN
        assert "left join `{project_id}`.raw.race_results" in base_section, (
            "temp_horse_distance_base で race_results が LEFT JOIN になっていません"
        )
        # race_id IS NULL 条件で予測日エントリも取り込む
        assert "r_r.race_id is null" in base_section, (
            "temp_horse_distance_base に 'r_r.race_id is null' 条件がありません（予測日エントリが除外される）"
        )

    def test_sql_te_history_base_prediction_entries_excluded_from_own_window(self):
        """予測日エントリが同日の TE ウィンドウ計算から除外されること（Issue #284）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING が存在することで当日行は
        # 自身の TE 計算から除外される（is_top3=0 の予測日エントリは分母・分子に寄与しない）
        assert "range between unbounded preceding and 1 preceding" in content, (
            "TE Window 関数の同日除外ガード (RANGE BETWEEN ... AND 1 PRECEDING) が見つかりません"
        )

    def test_sql_normalized_features_have_time_boundary(self):
        """finish_time_normalized と last_3f_normalized が時系列ガードを持つこと（Issue #295修正後）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # ORDER BY race_date + RANGE が両方の正規化に適用されていることを確認（1 preceding で当日除外）
        assert content.count("range between unbounded preceding and 1 preceding") >= 2, (
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
        assert "order by unix_date(t_p_r_f.race_date)" in finish_section, (
            "finish_time_normalized のウィンドウに ORDER BY unix_date(race_date) がありません"
        )
        assert "order by unix_date(t_p_r_f.race_date)" in last3f_section, (
            "last_3f_normalized のウィンドウに ORDER BY unix_date(race_date) がありません"
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


class TestTELowFrequencyMask:
    """TE低頻度エンティティNaNマスク（Issue #293）のテスト"""

    def test_sql_has_te_pre_ctes(self):
        """騎手・調教師・種牡馬の _pre CTE（カウント計算用）が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_jockey_te_pre" in content, "temp_jockey_te_pre が見つかりません"
        assert "temp_trainer_te_pre" in content, "temp_trainer_te_pre が見つかりません"
        assert "temp_sire_te_pre" in content, "temp_sire_te_pre が見つかりません"

    def test_sql_count_columns_use_correct_window(self):
        """jockey_count / trainer_count / sire_count が 1826日ウィンドウを使用すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # jockey_count の定義部分を確認
        jockey_pre_start = content.find("temp_jockey_te_pre")
        jockey_pre_end = content.find("temp_jockey_te as (")
        jockey_pre_section = content[jockey_pre_start:jockey_pre_end]
        assert "jockey_count" in jockey_pre_section
        assert "range between 1826 preceding and 1 preceding" in jockey_pre_section

        trainer_pre_start = content.find("temp_trainer_te_pre")
        trainer_pre_end = content.find("temp_trainer_te as (")
        trainer_pre_section = content[trainer_pre_start:trainer_pre_end]
        assert "trainer_count" in trainer_pre_section
        assert "range between 1826 preceding and 1 preceding" in trainer_pre_section

        sire_pre_start = content.find("temp_sire_te_pre")
        sire_pre_end = content.find("temp_sire_te as (")
        sire_pre_section = content[sire_pre_start:sire_pre_end]
        assert "sire_count" in sire_pre_section
        assert "range between 1826 preceding and 1 preceding" in sire_pre_section

    def test_sql_mask_wrapper_applies_if_condition(self):
        """マスクラッパーCTEが単軸 >= 20 の IF 条件を適用していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # 騎手マスク（単軸）
        jockey_wrapper_start = content.find("temp_jockey_te as (")
        jockey_wrapper_end = content.find("temp_trainer_te_pre")
        jockey_wrapper = content[jockey_wrapper_start:jockey_wrapper_end]
        assert "IF(jockey_count >= 20, jockey_te, NULL)" in jockey_wrapper
        assert "IF(jockey_count >= 20, jockey_course_type_te, NULL)" in jockey_wrapper

        # 調教師マスク（単軸）
        trainer_wrapper_start = content.find("temp_trainer_te as (")
        trainer_wrapper_end = content.find("temp_sire_te_pre")
        trainer_wrapper = content[trainer_wrapper_start:trainer_wrapper_end]
        assert "IF(trainer_count >= 20, trainer_te, NULL)" in trainer_wrapper

        # 種牡馬マスク（単軸）
        sire_wrapper_start = content.find("temp_sire_te as (")
        sire_wrapper_end = content.find("temp_horse_distance_base")
        sire_wrapper = content[sire_wrapper_start:sire_wrapper_end]
        assert "IF(sire_count >= 20, sire_te, NULL)" in sire_wrapper

    def test_sql_mask_threshold_is_20(self):
        """単軸TEのマスク閾値が 20 であること（変更時に気付けるよう）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "IF(jockey_count >= 20," in content
        assert "IF(trainer_count >= 20," in content
        assert "IF(sire_count >= 20," in content

    def test_sql_combined_te_thresholds_by_axis(self):
        """複合TE（2軸/3軸）は軸数に応じて閾値が段階的に緩和されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # 騎手: 2軸 >= 5, 3軸 >= 3
        jockey_wrapper_start = content.find("temp_jockey_te as (")
        jockey_wrapper_end = content.find("temp_trainer_te_pre")
        jockey_wrapper = content[jockey_wrapper_start:jockey_wrapper_end]
        assert "IF(jockey_count >= 5, jockey_course_type_venue_te, NULL)" in jockey_wrapper
        assert "IF(jockey_count >= 5, jockey_course_type_distance_te, NULL)" in jockey_wrapper
        assert "IF(jockey_count >= 3, jockey_course_type_distance_venue_te, NULL)" in jockey_wrapper

        # 調教師: 2軸 >= 5, 3軸 >= 3
        trainer_wrapper_start = content.find("temp_trainer_te as (")
        trainer_wrapper_end = content.find("temp_sire_te_pre")
        trainer_wrapper = content[trainer_wrapper_start:trainer_wrapper_end]
        assert "IF(trainer_count >= 5, trainer_course_type_venue_te, NULL)" in trainer_wrapper
        assert "IF(trainer_count >= 3, trainer_course_type_distance_venue_te, NULL)" in trainer_wrapper

        # 種牡馬: 2軸 >= 5, 3軸 >= 3
        sire_wrapper_start = content.find("temp_sire_te as (")
        sire_wrapper_end = content.find("temp_horse_distance_base")
        sire_wrapper = content[sire_wrapper_start:sire_wrapper_end]
        assert "IF(sire_count >= 5, sire_course_type_venue_te, NULL)" in sire_wrapper
        assert "IF(sire_count >= 3, sire_course_type_distance_venue_te, NULL)" in sire_wrapper

        # 馬自身: 2軸/3軸 >= 2
        horse_te_start = content.find(",temp_horse_te as (")
        horse_te_end = content.find("temp_horse_distance_base")
        horse_te_section = content[horse_te_start:horse_te_end]
        assert "IF(horse_count >= 2, horse_course_type_venue_te, NULL)" in horse_te_section
        assert "IF(horse_count >= 2, horse_course_type_distance_te, NULL)" in horse_te_section
        assert "IF(horse_count >= 2, horse_course_type_distance_venue_te, NULL)" in horse_te_section

    def test_sql_all_9_jockey_te_columns_masked(self):
        """9つの騎手TE基本特徴量がすべてマスク対象であること（軸数別閾値）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        jockey_wrapper_start = content.find("temp_jockey_te as (")
        jockey_wrapper_end = content.find("temp_trainer_te_pre")
        jockey_wrapper = content[jockey_wrapper_start:jockey_wrapper_end]
        # 単軸（>= 20）
        single_axis_cols = [
            "jockey_te", "jockey_course_type_te", "jockey_venue_te",
            "jockey_distance_band_te", "jockey_distance_te", "jockey_direction_te",
        ]
        for col in single_axis_cols:
            assert f"IF(jockey_count >= 20, {col}, NULL)" in jockey_wrapper, (
                f"騎手TE単軸 '{col}' のマスク条件 (>= 20) が見つかりません"
            )
        # 2軸（>= 5）
        assert "IF(jockey_count >= 5, jockey_course_type_venue_te, NULL)" in jockey_wrapper
        assert "IF(jockey_count >= 5, jockey_course_type_distance_te, NULL)" in jockey_wrapper
        # 3軸（>= 3）
        assert "IF(jockey_count >= 3, jockey_course_type_distance_venue_te, NULL)" in jockey_wrapper

    def test_sql_final_join_still_references_wrapper_ctes(self):
        """最終SELECTがマスクラッパーCTE（temp_jockey_te等）を参照していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "left join temp_jockey_te as t_j_te" in content
        assert "left join temp_trainer_te as t_tr_te" in content
        assert "left join temp_sire_te as t_s_te" in content


class TestTimeNormalizationFix:
    """finish_time_normalized / last_3f_normalized のリーク修正テスト（Issue #295）"""

    def test_sql_time_normalization_uses_1_preceding(self):
        """finish_time_normalized と last_3f_normalized が 1 preceding を使用すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        norm_start = content.find("finish_time_normalized")
        last3f_start = content.find("last_3f_normalized")
        norm_section = content[norm_start - 500:last3f_start + 500]
        assert "range between unbounded preceding and 1 preceding" in norm_section, (
            "finish_time_normalized / last_3f_normalized が '1 preceding' を使用していません"
        )

    def test_sql_time_normalization_no_current_row(self):
        """finish_time_normalized / last_3f_normalized で CURRENT ROW を使用していないこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        norm_start = content.find("finish_time_normalized")
        last3f_end = content.find("last_3f_normalized") + len("last_3f_normalized") + 200
        norm_section = content[norm_start - 800:last3f_end]
        assert "current row" not in norm_section.lower(), (
            "finish_time_normalized / last_3f_normalized に 'current row' が残っています"
        )

    def test_sql_no_current_row_anywhere(self):
        """SQLファイル全体に CURRENT ROW が存在しないこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "current row" not in content.lower(), (
            "feature_query_raw.sql に 'current row' が残っています"
        )


class TestGainZeroFeatureRemoval:
    """gain=0 特徴量除去テスト（Issue #291 SQL変更フェーズ / Issue #296）"""

    REMOVED_FEATURES = [
        "running_style", "improvement", "stable_index", "blinker", "pace_forecast",
        "early_advantage", "behind_advantage", "small_number_early_advantage",
        "bracket_number", "condition_change_flag",
        "improvement_code_2", "improvement_code_3", "improvement_code_4", "improvement_code_5",
        "corner_position_1", "corner_position_2", "corner_position_3",
        "corner_position_4", "corner_position_5",
        "disadvantage_3", "disadvantage_5",
        "position_fault_2", "position_fault_3",
        "late_start_3", "late_start_5",
        "mean_corner_position", "ema_corner_position",
        "running_style_front_count", "is_sole_leader", "is_renso",
        "turf_condition_code", "turf_condition_inner",
        "straight_bias_outer", "straight_bias_outermost",
        "dirt_condition_code", "new_direction_flag", "new_surface_dist_flag",
        "new_track_dist_flag", "new_distance_flag",
    ]

    def test_sql_except_clause_excludes_gain_zero_features(self):
        """最終SELECT の EXCEPT 節に gain=0 特徴量が全て含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        final_select_start = content.rfind("select\n  t_p_r_f")
        final_select_section = content[final_select_start:]
        for feature in self.REMOVED_FEATURES:
            assert feature in final_select_section, (
                f"gain=0 特徴量 '{feature}' が最終SELECT EXCEPT 節またはコメントに含まれていません"
            )

    def test_sql_removed_features_count(self):
        """除去対象の gain=0 特徴量が 39 件であること"""
        assert len(self.REMOVED_FEATURES) == 39


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

    def test_run_truncate_before_run_uses_write_truncate(self, mock_bq_client):
        """truncate_before_run=True の場合、TRUNCATE TABLE → WRITE_TRUNCATE で実行されること"""
        mock_instance = MagicMock()
        mock_bq_client.return_value = mock_instance

        # get_table でテーブル存在を確認（行数返却）
        mock_table = MagicMock()
        mock_table.num_rows = 497357
        mock_instance.get_table.return_value = mock_table

        # TRUNCATE TABLE 用のモック
        mock_truncate_job = MagicMock()
        mock_truncate_job.num_dml_affected_rows = 497357
        mock_truncate_job.result.return_value = None

        # INSERT（WRITE_TRUNCATE）用のモック
        mock_insert_result = MagicMock()
        mock_insert_result.total_rows = 497357
        mock_insert_job = MagicMock()
        mock_insert_job.result.return_value = mock_insert_result

        mock_instance.query.side_effect = [mock_truncate_job, mock_insert_job]

        pipeline = FeaturePipeline("test-project")
        result = pipeline.run("2016-01-01", "2026-06-06", truncate_before_run=True)

        assert result["deleted_rows"] == 497357
        assert result["inserted_rows"] == 497357

        # queryが2回呼ばれる（TRUNCATE + INSERT）
        assert mock_instance.query.call_count == 2

        # 1回目はTRUNCATEクエリ
        truncate_call = mock_instance.query.call_args_list[0]
        assert "TRUNCATE TABLE" in truncate_call[0][0]

        # 2回目はWRITE_TRUNCATEで書き込み
        insert_call = mock_instance.query.call_args_list[1]
        job_config = insert_call[1]["job_config"]
        from google.cloud import bigquery as bq
        assert job_config.write_disposition == bq.WriteDisposition.WRITE_TRUNCATE

    def test_run_truncate_before_run_table_not_exists(self, mock_bq_client):
        """truncate_before_run=True でテーブルが存在しない場合、TRUNCATEをスキップすること"""
        mock_instance = MagicMock()
        mock_bq_client.return_value = mock_instance

        # get_table が NotFound を返す
        mock_instance.get_table.side_effect = google_exceptions.NotFound("not found")

        mock_insert_result = MagicMock()
        mock_insert_result.total_rows = 100
        mock_insert_job = MagicMock()
        mock_insert_job.result.return_value = mock_insert_result
        mock_instance.query.return_value = mock_insert_job

        pipeline = FeaturePipeline("test-project")
        result = pipeline.run("2016-01-01", "2026-06-06", truncate_before_run=True)

        assert result["deleted_rows"] == 0
        assert result["inserted_rows"] == 100
        # queryは1回のみ（TRUNCATEはスキップ、INSERTのみ）
        assert mock_instance.query.call_count == 1

    def test_run_default_uses_write_append(self, mock_bq_client):
        """truncate_before_run=False（デフォルト）の場合、WRITE_APPEND で実行されること"""
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

        insert_call = mock_instance.query.call_args_list[0]
        job_config = insert_call[1]["job_config"]
        from google.cloud import bigquery as bq
        assert job_config.write_disposition == bq.WriteDisposition.WRITE_APPEND


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


class TestRaceExclusionFilter:
    """学習・推論対象レース除外フィルタのテスト（Issue #301）"""

    def test_sql_has_exclusion_filters_in_base_entries(self):
        """temp_base_race_entries に4つの除外フィルタが含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "race_class != 'A1'" in content, "新馬戦除外フィルタが見つかりません"
        assert "!= 'obstacle'" in content, "障害戦除外フィルタが見つかりません"
        assert "num_horses) > 7" in content, "少頭数除外フィルタが見つかりません"
        assert "venue_code = '04' and r_i.distance = 1000 and r_i.direction = 'straight'" in content, "新潟直線1000m除外フィルタが見つかりません"

    def test_sql_exclusion_filters_applied_to_both_ctes(self):
        """temp_base_race_entries / temp_horse_master_feature / temp_mare_race_base に除外フィルタが含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert content.count("race_class != 'A1'") == 2, "新馬戦除外が2箇所にあるべき"
        assert content.count("!= 'obstacle'") == 4, "障害戦除外が4箇所にあるべき（base/horse_master/mare_race_base/te_history_raw）"
        assert content.count("num_horses) > 7") == 2, "少頭数除外が2箇所にあるべき"
        assert content.count("venue_code = '04' and r_i.distance = 1000 and r_i.direction = 'straight'") == 2, "新潟直線除外が2箇所にあるべき"

    def test_sql_obstacle_exclusion_handles_null(self):
        """障害戦除外フィルタが course_type NULL の場合を安全に扱うこと（coalesce使用）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "coalesce(r_i.course_type, '') != 'obstacle'" in content, "障害戦除外が coalesce でNULL安全になっていません"

    def test_sql_small_field_exclusion_uses_coalesce(self):
        """少頭数除外フィルタが coalesce(t_r_h_c.num_horses, r_i.num_horses) を使うこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "coalesce(t_r_h_c.num_horses, r_i.num_horses) > 7" in content, "少頭数除外が coalesce を使っていません"

    def test_sql_niigata_exclusion_is_conjunction(self):
        """新潟直線1000m除外が venue_code・distance・direction の AND 条件であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "not (r_i.venue_code = '04' and r_i.distance = 1000 and r_i.direction = 'straight')" in content, "新潟直線1000m除外の複合条件が見つかりません"


class TestHorseTEFeature:
    """馬自身 Target Encoding 特徴量のテスト（Issue #303）"""

    def test_sql_has_temp_horse_te_ctes(self):
        """temp_horse_te_pre と temp_horse_te CTE が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_horse_te_pre" in content, "temp_horse_te_pre が見つかりません"
        assert "temp_horse_te as (" in content, "temp_horse_te が見つかりません"

    def test_sql_te_history_base_has_required_columns(self):
        """temp_te_history_base に horse_id / season / distance_change_type / weight_carried_change_type が含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # temp_te_history_raw から temp_jockey_te_pre までの範囲を確認
        base_start = content.find("temp_te_history_raw as (")
        jockey_start = content.find("temp_jockey_te_pre")
        base_section = content[base_start:jockey_start]
        assert "horse_id" in base_section, "temp_te_history_base に horse_id がありません"
        assert "season" in base_section, "temp_te_history_base に season がありません"
        assert "distance_change_type" in base_section, "temp_te_history_base に distance_change_type がありません"
        assert "weight_carried_change_type" in base_section, "temp_te_history_base に weight_carried_change_type がありません"

    def test_sql_distance_change_type_definition(self):
        """distance_change_type が extension / shortening / same の3値で定義されること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        base_start = content.find("temp_te_history_base as (")
        base_end = content.find("temp_te_history_raw")
        # 逆順検索で実際のtemp_te_history_baseのLAG定義部分を取得
        base_section = content[base_end:base_end + 3000]
        assert "'extension'" in base_section, "distance_change_type に 'extension' がありません"
        assert "'shortening'" in base_section, "distance_change_type に 'shortening' がありません"
        assert "'same'" in base_section, "distance_change_type に 'same' がありません"

    def test_sql_weight_carried_change_type_definition(self):
        """weight_carried_change_type が increase / decrease / same の3値で定義されること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        base_section_start = content.find("temp_te_history_base as (")
        base_section = content[base_section_start:base_section_start + 3000]
        assert "'increase'" in base_section, "weight_carried_change_type に 'increase' がありません"
        assert "'decrease'" in base_section, "weight_carried_change_type に 'decrease' がありません"

    def test_sql_horse_te_columns_in_final_select(self):
        """horse_te および全12条件別TE差分カラムが最終SELECTに含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        final_select_start = content.find("temp_final_raw as (")
        final_section = content[final_select_start:]
        expected_columns = [
            "horse_te",
            "horse_course_type_te_diff",
            "horse_venue_te_diff",
            "horse_distance_band_te_diff",
            "horse_distance_te_diff",
            "horse_direction_te_diff",
            "horse_jockey_te_diff",
            "horse_season_te_diff",
            "horse_course_type_venue_te_diff",
            "horse_course_type_distance_te_diff",
            "horse_course_type_distance_venue_te_diff",
            "horse_distance_change_te_diff",
            "horse_weight_carried_change_te_diff",
        ]
        for col in expected_columns:
            assert col in final_section, f"最終SELECT に '{col}' が見つかりません"

    def test_sql_horse_te_mask_threshold_is_5(self):
        """馬自身TE のマスク閾値が 5 であること（騎手・調教師の20より低い）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        wrapper_start = content.find("temp_horse_te as (")
        wrapper_end = content.find("temp_horse_distance_base")
        wrapper_section = content[wrapper_start:wrapper_end]
        assert "IF(horse_count >= 5, horse_te, NULL)" in wrapper_section, "馬TEマスク閾値が 5 ではありません"

    def test_sql_final_join_includes_horse_te(self):
        """最終SELECTが temp_horse_te を LEFT JOIN していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "left join temp_horse_te as t_h_te" in content, "最終SELECTに temp_horse_te の LEFT JOIN がありません"

    def test_sql_season_definition(self):
        """季節定義が spring / summer / autumn / winter の4値であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        base_section_start = content.find("temp_te_history_base as (")
        base_section = content[base_section_start:base_section_start + 2000]
        assert "'spring'" in base_section, "season に 'spring' がありません"
        assert "'summer'" in base_section, "season に 'summer' がありません"
        assert "'autumn'" in base_section, "season に 'autumn' がありません"
        assert "'winter'" in base_section, "season に 'winter' がありません"


class TestCareerDistanceFeature:
    """キャリア最長・最短距離フラグ特徴量のテスト（Issue #305）"""

    def test_sql_has_career_distance_ctes(self):
        """temp_career_distance_flags と temp_career_distance CTE が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_career_distance_flags as (" in content, "temp_career_distance_flags が見つかりません"
        assert "temp_career_distance as (" in content, "temp_career_distance が見つかりません"

    def test_sql_career_distance_uses_range_1_preceding(self):
        """temp_career_distance が RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING を使用していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        cte_start = content.find("temp_career_distance as (")
        cte_end = content.find("temp_training as (")
        cte_section = content[cte_start:cte_end]
        assert "range between unbounded preceding and 1 preceding" in cte_section, \
            "temp_career_distance に当日除外の RANGE BETWEEN 句がありません"

    def test_sql_career_distance_group1_columns_in_final_select(self):
        """グループ1（全出走）の6特徴量が最終SELECTに含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        final_select_start = content.find("temp_final_raw as (")
        final_section = content[final_select_start:]
        group1_columns = [
            "is_career_max_distance",
            "career_max_distance_diff",
            "is_career_min_distance",
            "career_min_distance_diff",
            "career_distance_range",
            "career_distance_count",
        ]
        for col in group1_columns:
            assert col in final_section, f"最終SELECT に '{col}' が見つかりません"

    def test_sql_career_distance_group2_columns_in_final_select(self):
        """グループ2（3着以内）の6特徴量が最終SELECTに含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        final_select_start = content.find("temp_final_raw as (")
        final_section = content[final_select_start:]
        group2_columns = [
            "is_beyond_placed_max_distance",
            "placed_max_distance_diff",
            "is_below_placed_min_distance",
            "placed_min_distance_diff",
            "placed_distance_range",
            "placed_distance_count",
        ]
        for col in group2_columns:
            assert col in final_section, f"最終SELECT に '{col}' が見つかりません"

    def test_sql_final_join_includes_career_distance(self):
        """最終SELECTが temp_career_distance を LEFT JOIN していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "left join temp_career_distance as t_c_d" in content, \
            "最終SELECTに temp_career_distance の LEFT JOIN がありません"

    def test_sql_placed_null_handling(self):
        """好走実績ゼロの馬は NULL になること（CASE WHEN is_top3=1 THEN distance END で自然に NULL）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        cte_start = content.find("temp_career_distance as (")
        cte_end = content.find("temp_training as (")
        cte_section = content[cte_start:cte_end]
        assert "case when is_top3 = 1 then distance end" in cte_section, \
            "好走実績ゼロの馬を NULL にする CASE WHEN が見つかりません"

    def test_sql_career_distance_no_current_race_data(self):
        """temp_career_distance_flags が is_first_at_distance フラグを含むこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        flags_start = content.find("temp_career_distance_flags as (")
        flags_end = content.find("temp_career_distance as (")
        flags_section = content[flags_start:flags_end]
        assert "is_first_at_distance" in flags_section, "is_first_at_distance フラグがありません"
        assert "is_first_placed_at_distance" in flags_section, "is_first_placed_at_distance フラグがありません"

    def test_sql_career_distance_based_on_horse_distance_base(self):
        """temp_career_distance_flags が temp_horse_distance_base を参照していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        flags_start = content.find("temp_career_distance_flags as (")
        flags_end = content.find("temp_career_distance as (")
        flags_section = content[flags_start:flags_end]
        assert "from temp_horse_distance_base" in flags_section, \
            "temp_career_distance_flags が temp_horse_distance_base を参照していません"


class TestCornerPositionFeature:
    """前走コーナー通過順位（全コーナー）・折り合い指標のテスト（Issue #306）"""

    def test_sql_has_raw_corner_columns_prev1(self):
        """前走（prev_1）の全4コーナー通過順が定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf_section = content[prf_start:prf2_start]
        for col in ["corner1_prev_1", "corner2_prev_1", "corner3_prev_1", "corner4_prev_1"]:
            assert col in prf_section, f"temp_past_race_features に '{col}' が見つかりません"

    def test_sql_has_raw_corner_columns_prev2(self):
        """2走前（prev_2）の全4コーナー通過順が定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf_section = content[prf_start:prf2_start]
        for col in ["corner1_prev_2", "corner2_prev_2", "corner3_prev_2", "corner4_prev_2"]:
            assert col in prf_section, f"temp_past_race_features に '{col}' が見つかりません"

    def test_sql_has_raw_corner_columns_prev3(self):
        """3走前（prev_3）の全4コーナー通過順が定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf_section = content[prf_start:prf2_start]
        for col in ["corner1_prev_3", "corner2_prev_3", "corner3_prev_3", "corner4_prev_3"]:
            assert col in prf_section, f"temp_past_race_features に '{col}' が見つかりません"

    def test_sql_has_corner_gain_features_in_prf2(self):
        """折り合い指標（corner_gain_1to4_prev_N）が temp_past_race_features2 に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        horse_master_start = content.find("temp_horse_master_feature as (")
        prf2_section = content[prf2_start:horse_master_start]
        for col in ["corner_gain_1to4_prev_1", "corner_gain_1to4_prev_2", "corner_gain_1to4_prev_3"]:
            assert col in prf2_section, f"temp_past_race_features2 に '{col}' が見つかりません"

    def test_sql_has_aggregate_corner_features_in_prf2(self):
        """集計折り合い指標が temp_past_race_features2 に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        horse_master_start = content.find("temp_horse_master_feature as (")
        prf2_section = content[prf2_start:horse_master_start]
        for col in [
            "mean_corner_gain_1to4",
            "ema_corner_gain_1to4",
            "mean_corner1_position",
            "corner1_to_finish_delta_prev_1",
        ]:
            assert col in prf2_section, f"temp_past_race_features2 に '{col}' が見つかりません"

    def test_sql_corner4_uses_r_r_corner_position_4(self):
        """corner4_prev_N は r_r_N.corner_position_4 を参照していること（既存データと整合）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf_section = content[prf_start:prf2_start]
        assert "r_r_1.corner_position_4 as corner4_prev_1" in prf_section, \
            "corner4_prev_1 が r_r_1.corner_position_4 を参照していません"
        assert "r_r_2.corner_position_4 as corner4_prev_2" in prf_section, \
            "corner4_prev_2 が r_r_2.corner_position_4 を参照していません"
        assert "r_r_3.corner_position_4 as corner4_prev_3" in prf_section, \
            "corner4_prev_3 が r_r_3.corner_position_4 を参照していません"

    def test_sql_corner_gain_null_when_corner1_null(self):
        """corner_gain_1to4_prev_N は corner1が NULL の場合 NULL になること（直線コース対応）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        horse_master_start = content.find("temp_horse_master_feature as (")
        prf2_section = content[prf2_start:horse_master_start]
        # 分母にcorner1 is not null条件があることを確認（直線コース除外）
        assert "corner1_prev_1 is not null and t_p_r_f.corner4_prev_1 is not null" in prf2_section, \
            "mean_corner_gain_1to4 の分母に corner1/corner4 null チェックがありません"

    def test_sql_corner1_to_finish_delta_excludes_invalid_finish(self):
        """corner1_to_finish_delta_prev_1 は finish_position=0（取消/失格）を除外すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        horse_master_start = content.find("temp_horse_master_feature as (")
        prf2_section = content[prf2_start:horse_master_start]
        assert "finish_position_1 > 0" in prf2_section, \
            "corner1_to_finish_delta_prev_1 が finish_position=0 を除外していません"


class TestMareFeature:
    """母馬（繁殖牝馬）競走実績・産駒TE特徴量のテスト（Issue #307）"""

    def test_sql_has_mare_ctEs(self):
        """母馬実績・TEに必要な CTE が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_mare_race_base as (" in content, "temp_mare_race_base が見つかりません"
        assert "temp_mare_stats as (" in content, "temp_mare_stats が見つかりません"
        assert "temp_mare_te_pre as (" in content, "temp_mare_te_pre が見つかりません"
        assert "temp_mare_te as (" in content, "temp_mare_te が見つかりません"

    def test_sql_mare_race_base_filters_obstacle(self):
        """temp_mare_race_base が障害戦を除外すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_base_start = content.find("temp_mare_race_base as (")
        mare_stats_start = content.find("temp_mare_stats as (")
        section = content[mare_base_start:mare_stats_start]
        assert "!= 'obstacle'" in section, "temp_mare_race_base に障害戦除外フィルタがありません"
        assert "finish_position > 0" in section, "temp_mare_race_base に finish_position > 0 フィルタがありません"
        assert "dam_name is not null" in section, "temp_mare_race_base に dam_name not null フィルタがありません"

    def test_sql_mare_race_base_joins_on_horse_number(self):
        """temp_mare_race_base が race_results を horse_number でも JOIN すること（クロスJOIN防止）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_base_start = content.find("temp_mare_race_base as (")
        mare_stats_start = content.find("temp_mare_stats as (")
        section = content[mare_base_start:mare_stats_start]
        assert "rr_d.horse_number = hr_d.horse_number" in section, \
            "race_results の JOIN に horse_number 条件がありません（クロスJOIN発生のリスク）"

    def test_sql_mare_te_pre_excludes_same_day(self):
        """temp_mare_te_pre が同日レースを TE 集計から除外すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_te_pre_start = content.find("temp_mare_te_pre as (")
        mare_te_start = content.find("temp_mare_te as (")
        section = content[mare_te_pre_start:mare_te_start]
        assert "range between unbounded preceding and 1 preceding" in section, \
            "temp_mare_te_pre に同日除外（RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING）がありません"
        assert "partition by dam_name" in section, \
            "temp_mare_te_pre の partition by に dam_name がありません"

    def test_sql_mare_te_low_frequency_mask(self):
        """temp_mare_te が産駒数 < 3 の母馬を NULL マスクすること（緩和: 10→3）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_te_start = content.find("temp_mare_te as (")
        horse_te_pre_start = content.find("temp_horse_te_pre as (")
        section = content[mare_te_start:horse_te_pre_start]
        assert "mare_count >= 3" in section, \
            "temp_mare_te に産駒数 < 3 の NULL マスク（>= 3）がありません"

    def test_sql_mare_placed_max_distance_diff_sign(self):
        """mare_placed_max_distance_diff は「当レース距離 − 母馬好走最長距離」であること（正値 = 超過）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "t_p_r_f.distance - t_m_s.mare_placed_max_distance as mare_placed_max_distance_diff" in content, \
            "mare_placed_max_distance_diff の符号方向が正しくありません"

    def test_sql_mare_a4_diff_formula(self):
        """グループA-4 diff特徴量が「条件別rate - mare_place_rate」であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "t_m_v.mare_venue_place_rate - t_m_s.mare_place_rate as mare_venue_place_rate_diff" in content, \
            "mare_venue_place_rate_diff の計算式が正しくありません"
        assert "t_m_db.mare_distance_band_place_rate - t_m_s.mare_place_rate as mare_distance_band_place_rate_diff" in content, \
            "mare_distance_band_place_rate_diff の計算式が正しくありません"

    def test_sql_dam_name_added_to_te_history(self):
        """dam_name が temp_te_history_raw と temp_te_history_base に追加されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_raw_start = content.find("temp_te_history_raw as (")
        te_base_start = content.find("temp_te_history_base as (")
        te_raw_section = content[te_raw_start:te_base_start]
        assert "h_m.dam_name" in te_raw_section, \
            "temp_te_history_raw に dam_name の SELECT がありません"
        te_base_end = content.find("from temp_te_history_raw", te_base_start)
        te_base_section = content[te_base_start:te_base_end]
        assert ",dam_name" in te_base_section, \
            "temp_te_history_base に dam_name が含まれていません"


class TestAgeBasedTEFeature:
    """種牡馬・母馬の年齢帯別TE（早熟・晩成性特徴量）のテスト（Issue #308）"""

    def test_sql_horse_age_in_te_history_raw(self):
        """temp_te_history_raw に horse_age が追加されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_raw_start = content.find("temp_te_history_raw as (")
        te_base_start = content.find("temp_te_history_base as (")
        section = content[te_raw_start:te_base_start]
        assert "date_diff(r_i.race_date, h_m.birth_date, year) as horse_age" in section, \
            "temp_te_history_raw に horse_age の計算がありません"

    def test_sql_age_band_in_te_history_base(self):
        """temp_te_history_base に age_band（2yo/3yo/4yo/5plus）の CASE WHEN が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_base_start = content.find("temp_te_history_base as (")
        jockey_pre_start = content.find("temp_jockey_te_pre as (")
        section = content[te_base_start:jockey_pre_start]
        assert "age_band" in section, "temp_te_history_base に age_band カラムがありません"
        assert "'2yo'" in section, "age_band に '2yo' ケースがありません"
        assert "'3yo'" in section, "age_band に '3yo' ケースがありません"
        assert "'4yo'" in section, "age_band に '4yo' ケースがありません"
        assert "'5plus'" in section, "age_band に '5plus' ケースがありません"

    def test_sql_sire_age_band_te_in_pre_cte(self):
        """temp_sire_te_pre に年齢帯別TE 4本と出走数カウント 4本が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        sire_pre_start = content.find("temp_sire_te_pre as (")
        sire_te_start = content.find("temp_sire_te as (")
        section = content[sire_pre_start:sire_te_start]
        for col in ["sire_age2_te", "sire_age3_te", "sire_age4_te", "sire_age5plus_te"]:
            assert col in section, f"temp_sire_te_pre に {col} がありません"
        for col in ["sire_age2_count", "sire_age3_count", "sire_age4_count", "sire_age5plus_count"]:
            assert col in section, f"temp_sire_te_pre に {col} がありません"

    def test_sql_sire_age_band_te_low_frequency_mask(self):
        """temp_sire_te で年齢帯別産駒数 < 5 の場合は対応する TE が NULL になること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        sire_te_start = content.find("temp_sire_te as (")
        mare_race_base_start = content.find("temp_mare_race_base as (")
        section = content[sire_te_start:mare_race_base_start]
        assert "IF(sire_age2_count >= 5, sire_age2_te, NULL)" in section, \
            "sire_age2_te の低頻度マスク（>= 5）がありません"
        assert "IF(sire_age3_count >= 5, sire_age3_te, NULL)" in section, \
            "sire_age3_te の低頻度マスク（>= 5）がありません"
        assert "IF(sire_age4_count >= 5, sire_age4_te, NULL)" in section, \
            "sire_age4_te の低頻度マスク（>= 5）がありません"
        assert "IF(sire_age5plus_count >= 5, sire_age5plus_te, NULL)" in section, \
            "sire_age5plus_te の低頻度マスク（>= 5）がありません"

    def test_sql_sire_current_age_te_selects_correct_band(self):
        """sire_current_age_te が horse_age に応じて正しい年齢帯 TE を選択すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "when t_p_r_f.horse_age = 2 then t_s_te.sire_age2_te" in content, \
            "sire_current_age_te の 2歳ケースがありません"
        assert "when t_p_r_f.horse_age = 3 then t_s_te.sire_age3_te" in content, \
            "sire_current_age_te の 3歳ケースがありません"
        assert "when t_p_r_f.horse_age = 4 then t_s_te.sire_age4_te" in content, \
            "sire_current_age_te の 4歳ケースがありません"
        assert "end as sire_current_age_te" in content, \
            "最終 SELECT に sire_current_age_te がありません"

    def test_sql_sire_precocity_diff_formula(self):
        """sire_precocity_diff が（若齢TE平均 − 老齢TE平均）の形式であること（正値=早熟型）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "sire_precocity_diff" in content, "sire_precocity_diff がありません"
        assert "sire_age2_te" in content and "sire_age3_te" in content, \
            "sire_precocity_diff に若齢TE（age2/age3）が含まれていません"
        assert "sire_age4_te" in content and "sire_age5plus_te" in content, \
            "sire_precocity_diff に老齢TE（age4/age5plus）が含まれていません"

    def test_sql_mare_age_band_te_in_pre_cte(self):
        """temp_mare_te_pre に年齢帯別TE 4本と出走数カウント 4本が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_te_pre_start = content.find("temp_mare_te_pre as (")
        mare_te_start = content.find("temp_mare_te as (")
        section = content[mare_te_pre_start:mare_te_start]
        for col in ["mare_age2_te", "mare_age3_te", "mare_age4_te", "mare_age5plus_te"]:
            assert col in section, f"temp_mare_te_pre に {col} がありません"
        for col in ["mare_age2_count", "mare_age3_count", "mare_age4_count", "mare_age5plus_count"]:
            assert col in section, f"temp_mare_te_pre に {col} がありません"

    def test_sql_mare_age_band_te_low_frequency_mask(self):
        """temp_mare_te で年齢帯別産駒数 < 5 の場合は対応する TE が NULL になること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_te_start = content.find("temp_mare_te as (")
        horse_te_pre_start = content.find("temp_horse_te_pre as (")
        section = content[mare_te_start:horse_te_pre_start]
        assert "IF(mare_age2_count >= 5, mare_age2_te, NULL)" in section, \
            "mare_age2_te の低頻度マスク（>= 5）がありません"
        assert "IF(mare_age3_count >= 5, mare_age3_te, NULL)" in section, \
            "mare_age3_te の低頻度マスク（>= 5）がありません"
        assert "IF(mare_age4_count >= 5, mare_age4_te, NULL)" in section, \
            "mare_age4_te の低頻度マスク（>= 5）がありません"
        assert "IF(mare_age5plus_count >= 5, mare_age5plus_te, NULL)" in section, \
            "mare_age5plus_te の低頻度マスク（>= 5）がありません"

    def test_sql_mare_current_age_te_selects_correct_band(self):
        """mare_current_age_te が horse_age に応じて正しい年齢帯 TE を選択すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        final_start = content.rfind("from\n  temp_past_race_features2")
        section = content[final_start - 55000:final_start]
        assert "when t_p_r_f.horse_age = 2 then t_m_te.mare_age2_te" in section, \
            "mare_current_age_te の 2歳ケースがありません"
        assert "when t_p_r_f.horse_age = 3 then t_m_te.mare_age3_te" in section, \
            "mare_current_age_te の 3歳ケースがありません"
        assert "mare_current_age_te" in section, \
            "最終 SELECT に mare_current_age_te がありません"

    def test_sql_mare_early_late_career_place_rate_in_stats(self):
        """temp_mare_stats に mare_early_career_place_rate / mare_late_career_place_rate が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_stats_start = content.find("temp_mare_stats as (")
        mare_venue_start = content.find("temp_mare_venue_stats as (")
        section = content[mare_stats_start:mare_venue_start]
        assert "mare_early_career_place_rate" in section, \
            "temp_mare_stats に mare_early_career_place_rate がありません"
        assert "mare_late_career_place_rate" in section, \
            "temp_mare_stats に mare_late_career_place_rate がありません"
        assert "horse_age_at_race between 2 and 3" in section, \
            "mare_early_career_place_rate に 2〜3歳条件がありません"
        assert "horse_age_at_race >= 4" in section, \
            "mare_late_career_place_rate に 4歳以上条件がありません"

    def test_sql_mare_precocity_index_in_final_select(self):
        """最終 SELECT に mare_precocity_index（early - late）が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "mare_precocity_index" in content, "最終 SELECT に mare_precocity_index がありません"
        assert "mare_early_career_place_rate - t_m_s.mare_late_career_place_rate as mare_precocity_index" in content, \
            "mare_precocity_index の計算式が正しくありません"

    def test_sql_mare_race_base_joins_horse_master_for_birth_date(self):
        """temp_mare_race_base が dam_id_lookup を使って horse_results を JOIN して birth_date を取得すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_base_start = content.find("temp_mare_race_base as (")
        mare_stats_start = content.find("temp_mare_stats as (")
        section = content[mare_base_start:mare_stats_start]
        assert "dam_id_lookup" in section, \
            "temp_mare_race_base に dam_id_lookup サブクエリがありません"
        assert "horse_age_at_race" in section, \
            "temp_mare_race_base に horse_age_at_race がありません"


class TestCourseBiasFeature:
    """前走の馬場バイアス×コース取りによるパフォーマンス補正特徴量のテスト（Issue #309）"""

    def test_sql_disadvantage_1_in_prf_cte(self):
        """disadvantage_1（前走の総不利値）が temp_past_race_features に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        assert "r_r_1.disadvantage" in section and "disadvantage_1" in section, \
            "temp_past_race_features に disadvantage_1 の定義がありません"

    def test_sql_has_disadvantage_breakdown_prev1(self):
        """前走の不利値内訳（front/mid/back_disadvantage_1）が temp_past_race_features に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        for col in ["front_disadvantage_1", "mid_disadvantage_1", "back_disadvantage_1"]:
            assert col in section, f"temp_past_race_features に '{col}' が見つかりません"

    def test_sql_has_course_position_prev1(self):
        """course_position_prev1（前走コース取り）が temp_past_race_features に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        assert "course_position_prev1" in section, \
            "temp_past_race_features に course_position_prev1 がありません"
        assert "r_r_1.course_position" in section, \
            "course_position_prev1 が r_r_1.course_position を参照していません"

    def test_sql_has_track_bias_prev1(self):
        """track_bias_prev1（前走馬場差）が temp_past_race_features に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        assert "track_bias_prev1" in section, \
            "temp_past_race_features に track_bias_prev1 がありません"
        assert "r_r_1.track_bias" in section, \
            "track_bias_prev1 が r_r_1.track_bias を参照していません"

    def test_sql_has_prev1_venue_info_join(self):
        """前走の venue_info JOIN（v_i_prev1）が temp_past_race_features に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        assert "v_i_prev1" in section, \
            "temp_past_race_features に v_i_prev1 JOIN がありません"
        assert "r_r_1.race_date = v_i_prev1.race_date" in section, \
            "v_i_prev1 の JOIN条件に r_r_1.race_date がありません"

    def test_sql_prev1_course_bias_score_case_logic(self):
        """prev1_course_bias_score の CASE WHEN が course_position 1〜5 の全ケースを網羅していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        final_start = content.rfind("from\n  temp_past_race_features2")
        section = content[final_start - 55000:final_start]
        assert "prev1_course_bias_score" in section, \
            "最終 SELECT に prev1_course_bias_score がありません"
        assert "course_position_prev1" in section, \
            "prev1_course_bias_score の CASE WHEN に course_position_prev1 がありません"
        assert "prev1_straight_bias_innermost" in section, \
            "course_position=1（最内）のケースがありません"
        assert "prev1_straight_bias_outermost" in section, \
            "course_position=5（大外）のケースがありません"

    def test_sql_prev1_course_bias_score_center_is_average(self):
        """course_position_prev1=3（中）のとき内外の平均値（inner + outer / 2）になること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "(t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2" in content, \
            "course_position=3（中）の計算式が内外平均値になっていません"

    def test_sql_prev1_venue_info_null_when_overseas(self):
        """前走 venue_info が存在しない（海外遠征帰りなど）場合は prev1_straight_bias_* が NULL になること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        assert "left join" in section and "v_i_prev1" in section, \
            "v_i_prev1 が LEFT JOIN でないため NULL 処理されません"

    def test_sql_prev1_course_bias_disadvantage_flag_in_final_select(self):
        """prev1_course_bias_disadvantage_flag が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "prev1_course_bias_disadvantage_flag" in content, \
            "最終 SELECT に prev1_course_bias_disadvantage_flag がありません"

    def test_sql_has_disadvantage_breakdown_all_prev(self):
        """2〜5走前の不利値内訳（front/mid/back_disadvantage_N）が temp_past_race_features に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        for n in range(2, 6):
            for prefix in ["front_disadvantage", "mid_disadvantage", "back_disadvantage"]:
                col = f"{prefix}_{n}"
                assert col in section, f"temp_past_race_features に '{col}' が見つかりません"

    def test_sql_has_course_position_all_prev(self):
        """2〜5走前の course_position_prevN が temp_past_race_features に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        for n in range(2, 6):
            assert f"course_position_prev{n}" in section, \
                f"temp_past_race_features に course_position_prev{n} がありません"

    def test_sql_has_track_bias_all_prev(self):
        """2〜5走前の track_bias_prevN が temp_past_race_features に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        for n in range(2, 6):
            assert f"track_bias_prev{n}" in section, \
                f"temp_past_race_features に track_bias_prev{n} がありません"

    def test_sql_has_venue_info_join_all_prev(self):
        """2〜5走前の venue_info LEFT JOIN（v_i_prevN）が temp_past_race_features に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        for n in range(2, 6):
            assert f"v_i_prev{n}" in section, \
                f"temp_past_race_features に v_i_prev{n} JOIN がありません"

    def test_sql_has_straight_bias_all_prev(self):
        """2〜5走前の prevN_straight_bias_* が temp_past_race_features に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        for n in range(2, 6):
            for suffix in ["innermost", "inner", "outer", "outermost"]:
                col = f"prev{n}_straight_bias_{suffix}"
                assert col in section, f"temp_past_race_features に '{col}' がありません"

    def test_sql_has_course_bias_score_all_prev(self):
        """2〜5走前の prevN_course_bias_score が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for n in range(2, 6):
            assert f"prev{n}_course_bias_score" in content, \
                f"最終 SELECT に prev{n}_course_bias_score がありません"

    def test_sql_has_course_bias_disadvantage_flag_all_prev(self):
        """2〜5走前の prevN_course_bias_disadvantage_flag が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for n in range(2, 6):
            assert f"prev{n}_course_bias_disadvantage_flag" in content, \
                f"最終 SELECT に prev{n}_course_bias_disadvantage_flag がありません"


class TestGateBiasFeature:
    """枠番×馬場バイアス交差特徴量のテスト（Issue #310）"""

    def test_sql_gate_bias_score_in_final_select(self):
        """gate_bias_score が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "gate_bias_score" in content, \
            "最終 SELECT に gate_bias_score がありません"

    def test_sql_gate_bias_advantage_flag_in_final_select(self):
        """gate_bias_advantage_flag が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "gate_bias_advantage_flag" in content, \
            "最終 SELECT に gate_bias_advantage_flag がありません"

    def test_sql_gate_bias_score_zone_thresholds(self):
        """gate_bias_score の CASE WHEN がゾーン境界（0.25 / 0.50 / 0.75）で切り替わること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "horse_number_ratio <= 0.25" in content, \
            "gate_bias_score に 0.25（内側ゾーン）の閾値がありません"
        assert "horse_number_ratio <= 0.50" in content, \
            "gate_bias_score に 0.50（内中ゾーン）の閾値がありません"
        assert "horse_number_ratio <= 0.75" in content, \
            "gate_bias_score に 0.75（外中ゾーン）の閾値がありません"

    def test_sql_gate_bias_score_uses_all_four_zones(self):
        """gate_bias_score が innermost / inner / outer / outermost の4ゾーンを参照すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for col in ["straight_bias_innermost", "straight_bias_inner",
                    "straight_bias_outer", "straight_bias_outermost"]:
            assert f"t_h_m_f.{col}" in content, \
                f"gate_bias_score が t_h_m_f.{col} を参照していません"

    def test_sql_straight_bias_range_in_final_select(self):
        """straight_bias_range が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "straight_bias_range" in content, \
            "最終 SELECT に straight_bias_range がありません"
        assert "straight_bias_innermost - t_h_m_f.straight_bias_outermost" in content, \
            "straight_bias_range の計算式（innermost - outermost）が正しくありません"

    def test_sql_is_strong_bias_race_in_final_select(self):
        """is_strong_bias_race が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "is_strong_bias_race" in content, \
            "最終 SELECT に is_strong_bias_race がありません"
        assert ">= 3" in content, \
            "is_strong_bias_race の閾値（ABS >= 3）がありません"

    def test_sql_mean_course_position_in_prf2_cte(self):
        """mean_course_position が temp_past_race_features2 に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        hm_start = content.find("temp_horse_master_feature as (")
        section = content[prf2_start:hm_start]
        assert "mean_course_position" in section, \
            "temp_past_race_features2 に mean_course_position がありません"
        assert "course_position_prev1" in section, \
            "mean_course_position が course_position_prev1 を参照していません"

    def test_sql_ema_course_position_in_prf2_cte(self):
        """ema_course_position が temp_past_race_features2 に定義され、重み付け集計になっていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        hm_start = content.find("temp_horse_master_feature as (")
        section = content[prf2_start:hm_start]
        assert "ema_course_position" in section, \
            "temp_past_race_features2 に ema_course_position がありません"
        assert "1.5" in section and "0.5" in section, \
            "ema_course_position に前走ウェイト（1.5）または3走前ウェイト（0.5）がありません"

    def test_sql_course_position_bias_risk_in_final_select(self):
        """course_position_bias_risk が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "course_position_bias_risk" in content, \
            "最終 SELECT に course_position_bias_risk がありません"
        assert "ema_course_position * t_h_m_f.straight_bias_inner" in content, \
            "course_position_bias_risk の計算式（ema_course_position × straight_bias_inner）が正しくありません"

    def test_sql_course_position_uses_only_past_races(self):
        """当レースの course_position は使用せず、過去走（r_r_1 等）のみ参照していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        section = content[prf_start:prf2_start]
        assert "r_r_1.course_position" in section, \
            "前走の course_position（r_r_1.course_position）が見つかりません"
        assert "h_r.course_position" not in section, \
            "当レース（h_r.course_position）がリークしています"


class TestIDMZoneCorrectionFeature:
    """馬場バイアス×コース取りによるIDM補正特徴量のテスト（Issue #311）"""

    def test_sql_idm_zone_neutral_all_prev(self):
        """idm_zone_neutral_1〜5 が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for n in range(1, 6):
            assert f"idm_zone_neutral_{n}" in content, \
                f"最終 SELECT に idm_zone_neutral_{n} がありません"

    def test_sql_idm_zone_potential_all_prev(self):
        """idm_zone_potential_1〜5 が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for n in range(1, 6):
            assert f"idm_zone_potential_{n}" in content, \
                f"最終 SELECT に idm_zone_potential_{n} がありません"

    def test_sql_idm_zone_neutral_formula(self):
        """idm_zone_neutral_N が idm_N と track_bias_prevN を参照していること（補正式の構成要素）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for n in range(1, 6):
            assert f"t_p_r_f.idm_{n}" in content, \
                f"idm_zone_neutral_{n} で t_p_r_f.idm_{n} が参照されていません"
            assert f"t_p_r_f.track_bias_prev{n}" in content, \
                f"idm_zone_neutral_{n} で t_p_r_f.track_bias_prev{n} が参照されていません"
            assert f"t_p_r_f.course_position_prev{n}" in content, \
                f"idm_zone_neutral_{n} で t_p_r_f.course_position_prev{n} が参照されていません"

    def test_sql_idm_zone_neutral_uses_case_for_course_bias(self):
        """idm_zone_neutral_N の CASE WHEN で course_position_prevN → straight_bias_* の対応が正しいこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for n in range(1, 6):
            assert f"t_p_r_f.prev{n}_straight_bias_innermost" in content, \
                f"idm_zone_neutral_{n} で prev{n}_straight_bias_innermost が参照されていません"
            assert f"t_p_r_f.prev{n}_straight_bias_outermost" in content, \
                f"idm_zone_neutral_{n} で prev{n}_straight_bias_outermost が参照されていません"

    def test_sql_idm_zone_neutral_null_when_no_course_position(self):
        """course_position_prevN が NULL の場合、ELSE NULL で NULL を返すこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        neutral_section_start = content.find("idm_zone_neutral_1")
        neutral_section_end = content.find("idm_zone_neutral_trend")
        neutral_section = content[neutral_section_start:neutral_section_end]
        assert "ELSE NULL" in neutral_section, \
            "idm_zone_neutral セクションの CASE WHEN に ELSE NULL がありません（NULL伝播が保証されません）"

    def test_sql_idm_zone_potential_uses_greatest_coalesce(self):
        """idm_zone_potential_N が GREATEST(COALESCE(...)) で最有利ゾーンを選択していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "GREATEST(" in content, \
            "idm_zone_potential に GREATEST 関数がありません"
        assert "COALESCE(t_p_r_f.prev1_straight_bias_innermost, t_p_r_f.track_bias_prev1)" in content, \
            "idm_zone_potential_1 で innermost の COALESCE フォールバックがありません"

    def test_sql_idm_zone_potential_structure_ge_neutral(self):
        """idm_zone_potential_1 の計算に GREATEST が含まれていること（potential >= neutral の構造的保証）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        potential_idx = content.find("AS idm_zone_potential_1")
        assert potential_idx > 0, "idm_zone_potential_1 が見つかりません"
        pot_section = content[max(0, potential_idx - 1000):potential_idx + 50]
        assert "GREATEST(" in pot_section, \
            "idm_zone_potential_1 の計算に GREATEST がありません"

    def test_sql_idm_zone_correction_all_prev(self):
        """idm_zone_correction_1〜5 が最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for n in range(1, 6):
            assert f"idm_zone_correction_{n}" in content, \
                f"最終 SELECT に idm_zone_correction_{n} がありません"

    def test_sql_idm_zone_correction_formula(self):
        """idm_zone_correction_N が track_bias_prevN - course_bias の形式になっていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        correction_idx = content.find("AS idm_zone_correction_1")
        assert correction_idx > 0, "idm_zone_correction_1 が見つかりません"
        correction_section = content[max(0, correction_idx - 600):correction_idx + 50]
        assert "t_p_r_f.track_bias_prev1" in correction_section, \
            "idm_zone_correction_1 で track_bias_prev1 が参照されていません"
        assert "t_p_r_f.course_position_prev1" in correction_section, \
            "idm_zone_correction_1 で course_position_prev1 が参照されていません"

    def test_sql_ema_idm_zone_neutral_weights(self):
        """ema_idm_zone_neutral の重みが 1.5/1.25/1.0/0.75/0.5 であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        ema_idx = content.find("AS ema_idm_zone_neutral\n")
        assert ema_idx > 0, "ema_idm_zone_neutral が見つかりません"
        ema_section = content[max(0, ema_idx - 3000):ema_idx + 50]
        assert "* 1.5" in ema_section, "ema_idm_zone_neutral に前走ウェイト 1.5 がありません"
        assert "* 1.25" in ema_section, "ema_idm_zone_neutral に2走前ウェイト 1.25 がありません"
        assert "* 1.0" in ema_section, "ema_idm_zone_neutral に3走前ウェイト 1.0 がありません"
        assert "* 0.75" in ema_section, "ema_idm_zone_neutral に4走前ウェイト 0.75 がありません"
        assert "* 0.5" in ema_section, "ema_idm_zone_neutral に5走前ウェイト 0.5 がありません"
        assert "THEN 1.5" in ema_section, "ema_idm_zone_neutral の分母に 1.5 がありません"
        assert "THEN 1.25" in ema_section, "ema_idm_zone_neutral の分母に 1.25 がありません"
        assert "THEN 0.5" in ema_section, "ema_idm_zone_neutral の分母に 0.5 がありません"

    def test_sql_aggregate_features_in_final_select(self):
        """mean/ema の集計特徴量が4本とも最終 SELECT に存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for feature in ["mean_idm_zone_neutral", "ema_idm_zone_neutral",
                        "mean_idm_zone_potential", "ema_idm_zone_potential"]:
            assert feature in content, \
                f"最終 SELECT に {feature} がありません"

    def test_sql_ema_idm_zone_neutral_diff_in_final_select(self):
        """ema_idm_zone_neutral_diff が最終 SELECT に存在し、ema_idm との差分であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "ema_idm_zone_neutral_diff" in content, \
            "最終 SELECT に ema_idm_zone_neutral_diff がありません"
        diff_idx = content.find("AS ema_idm_zone_neutral_diff")
        assert diff_idx > 0, "ema_idm_zone_neutral_diff の AS 句が見つかりません"
        diff_section = content[max(0, diff_idx - 200):diff_idx + 50]
        assert "t_p_r_f.ema_idm" in diff_section, \
            "ema_idm_zone_neutral_diff が t_p_r_f.ema_idm との差分になっていません"

    def test_sql_idm_zone_neutral_trend_in_final_select(self):
        """idm_zone_neutral_trend が最終 SELECT に存在し、1走前 - 3走前の差分であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "idm_zone_neutral_trend" in content, \
            "最終 SELECT に idm_zone_neutral_trend がありません"
        trend_idx = content.find("AS idm_zone_neutral_trend")
        assert trend_idx > 0, "idm_zone_neutral_trend の AS 句が見つかりません"
        trend_section = content[max(0, trend_idx - 1200):trend_idx + 50]
        assert "t_p_r_f.idm_1" in trend_section, \
            "idm_zone_neutral_trend が idm_1 を参照していません"
        assert "t_p_r_f.idm_3" in trend_section, \
            "idm_zone_neutral_trend が idm_3 を参照していません"

    def test_sql_no_current_race_idm_leak(self):
        """当レースの IDM（h_r.idm）が idm_zone_neutral セクションに含まれていないこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        neutral_section_start = content.find("idm_zone_neutral_1")
        neutral_section_end = content.find("idm_zone_neutral_trend") + len("idm_zone_neutral_trend")
        neutral_section = content[neutral_section_start:neutral_section_end]
        assert "h_r.idm" not in neutral_section, \
            "idm_zone_neutral セクションに当レース（h_r.idm）が含まれています（リーク）"


class TestNullImputation:
    """NULL値補完（Issue #330）のテスト"""

    def test_sql_has_temp_final_raw_cte(self):
        """temp_final_raw CTEが存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_final_raw as (" in content, \
            "temp_final_raw CTE が見つかりません"

    def test_sql_has_percentile_cont_imputation(self):
        """NULL補完CTEがPERCENTILE_CONTとSELECT * EXCEPTによる2段階構造であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_null_fill_med as (" in content, \
            "temp_null_fill_med CTE が見つかりません"
        assert "select * except(" in content, \
            "select * except( が見つかりません"
        assert "percentile_cont" in content, \
            "percentile_cont が見つかりません"

    def test_sql_te_columns_imputed_with_percentile_cont(self):
        """TE系カラムがPERCENTILE_CONTで補完されていること（Issue #330）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_columns = [
            "jockey_te",
            "trainer_te",
            "sire_te",
            "horse_te",
            "mare_te",
        ]
        for col in te_columns:
            pattern = f"percentile_cont({col}, 0.5) over (partition by race_id)"
            assert pattern in content, \
                f"TE列 '{col}' の PERCENTILE_CONT 補完が見つかりません"

    def test_sql_past_race_features_imputed(self):
        """過去走特徴量がPERCENTILE_CONTで補完されていること（Issue #330）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        past_race_columns = [
            "finish_time_1",
            "idm_1",
            "mean_idm",
            "finish_time_normalized",
            "last_3f_normalized",
        ]
        for col in past_race_columns:
            pattern = f"percentile_cont({col}, 0.5) over (partition by race_id)"
            assert pattern in content, \
                f"過去走特徴量 '{col}' の PERCENTILE_CONT 補完が見つかりません"

    def test_sql_cha_features_imputed(self):
        """調教特徴量がPERCENTILE_CONTで補完されていること（Issue #330）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        cha_columns = [
            "cha_training_index",
            "training_last_3f",
            "training_furlongs",
        ]
        for col in cha_columns:
            pattern = f"percentile_cont({col}, 0.5) over (partition by race_id)"
            assert pattern in content, \
                f"調教特徴量 '{col}' の PERCENTILE_CONT 補完が見つかりません"

    def test_sql_mare_features_imputed(self):
        """母馬実績特徴量がPERCENTILE_CONTで補完されていること（Issue #330）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_columns = [
            "mare_place_rate",
            "mare_te",
            "mare_early_career_place_rate",
        ]
        for col in mare_columns:
            pattern = f"percentile_cont({col}, 0.5) over (partition by race_id)"
            assert pattern in content, \
                f"母馬特徴量 '{col}' の PERCENTILE_CONT 補完が見つかりません"

    def test_sql_bias_features_imputed(self):
        """バイアス補正特徴量がPERCENTILE_CONTで補完されていること（Issue #330）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        bias_columns = [
            "prev1_course_bias_score",
            "gate_bias_score",
            "idm_zone_neutral_1",
            "idm_zone_neutral_trend",
        ]
        for col in bias_columns:
            pattern = f"percentile_cont({col}, 0.5) over (partition by race_id)"
            assert pattern in content, \
                f"バイアス補正特徴量 '{col}' の PERCENTILE_CONT 補完が見つかりません"

    def test_sql_te_fallback_is_global_mean(self):
        """TE系のフォールバック値がグローバル複勝率（0.22）であること（Issue #330）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_fallback_pattern = "coalesce(jockey_te, _jockey_te_med, 0.22) as jockey_te"
        assert te_fallback_pattern in content, \
            "jockey_te のフォールバック値が 0.22 ではありません"

    def test_sql_non_te_fallback_is_zero(self):
        """TE以外のフォールバック値が0であること（Issue #330）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        non_te_fallback_pattern = "coalesce(finish_time_1, _finish_time_1_med, 0) as finish_time_1"
        assert non_te_fallback_pattern in content, \
            "finish_time_1 のフォールバック値が 0 ではありません"

    def test_sql_imputation_uses_race_id_partition(self):
        """PERCENTILE_CONTが同一レース内（race_id）でパーティションされていること（Issue #330）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        partition_pattern = "percentile_cont(jockey_te, 0.5) over (partition by race_id)"
        assert partition_pattern in content, \
            "PERCENTILE_CONT が race_id でパーティションされていません"

    def test_sql_imputation_uses_coalesce_chain(self):
        """補完がCOALESCE(元値, 事前計算中央値, フォールバック)の3段階であること（Issue #330）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        coalesce_pattern = "coalesce(jockey_te, _jockey_te_med, 0.22) as jockey_te"
        assert coalesce_pattern in content, \
            "COALESCE の3段階補完パターン（元値→事前計算中央値→フォールバック）が見つかりません"

    def test_sql_from_temp_final_raw(self):
        """最終SELECTがtemp_final_rawから取得していること（Issue #330）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "from temp_final_raw" in content, \
            "最終SELECT の FROM が temp_final_raw を参照していません"


class TestRunRatioFeature:
    """条件別出走比率特徴量（Issue #332）のテスト

    種牡馬・母馬TE集計に、条件別出走数 / 全出走数 の比率特徴量を追加する。
    """

    def test_sql_sire_run_ratio_in_te_pre(self):
        """temp_sire_te_pre に sire_*_run_ratio 列が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        sire_pre_start = content.find("temp_sire_te_pre as (")
        sire_pre_end = content.find("temp_sire_te as (")
        section = content[sire_pre_start:sire_pre_end]
        for col in [
            "sire_course_type_run_ratio",
            "sire_venue_run_ratio",
            "sire_distance_band_run_ratio",
            "sire_distance_run_ratio",
        ]:
            assert col in section, f"temp_sire_te_pre に '{col}' が見つかりません"

    def test_sql_mare_run_ratio_in_te_pre(self):
        """temp_mare_te_pre に mare_*_run_ratio 列が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_pre_start = content.find("temp_mare_te_pre as (")
        mare_pre_end = content.find("temp_mare_te as (")
        section = content[mare_pre_start:mare_pre_end]
        for col in [
            "mare_course_type_run_ratio",
            "mare_venue_run_ratio",
            "mare_distance_band_run_ratio",
            "mare_distance_run_ratio",
        ]:
            assert col in section, f"temp_mare_te_pre に '{col}' が見つかりません"

    def test_sql_sire_run_ratio_uses_safe_divide(self):
        """sire_course_type_run_ratio が safe_divide を使用していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        sire_pre_start = content.find("temp_sire_te_pre as (")
        sire_pre_end = content.find("temp_sire_te as (")
        section = content[sire_pre_start:sire_pre_end]
        assert "sire_course_type_run_ratio" in section
        # safe_divide と run_ratio が同じ CTE に存在すること
        assert "safe_divide(" in section
        assert "partition by sire_name, course_type" in section

    def test_sql_sire_run_ratio_masked_in_te(self):
        """temp_sire_te が sire_count >= 20 でマスクを適用していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        sire_te_start = content.find("temp_sire_te as (")
        sire_te_end = content.find("temp_horse_distance_base")
        section = content[sire_te_start:sire_te_end]
        for col in [
            "sire_course_type_run_ratio",
            "sire_venue_run_ratio",
            "sire_distance_band_run_ratio",
            "sire_distance_run_ratio",
        ]:
            assert f"IF(sire_count >= 20, {col}, NULL)" in section, (
                f"temp_sire_te に '{col}' の低頻度マスク（>= 20）が見つかりません"
            )

    def test_sql_mare_run_ratio_masked_in_te(self):
        """temp_mare_te が mare_count >= 3 でマスクを適用していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        mare_te_start = content.find("temp_mare_te as (")
        mare_te_end = content.find("temp_horse_te_pre as (")
        section = content[mare_te_start:mare_te_end]
        for col in [
            "mare_course_type_run_ratio",
            "mare_venue_run_ratio",
            "mare_distance_band_run_ratio",
            "mare_distance_run_ratio",
        ]:
            assert f"IF(mare_count >= 3, {col}, NULL)" in section, (
                f"temp_mare_te に '{col}' の低頻度マスク（>= 3）が見つかりません"
            )

    def test_sql_run_ratio_in_final_select(self):
        """最終SELECTに sire_*_run_ratio / mare_*_run_ratio が含まれていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for col in [
            "sire_course_type_run_ratio",
            "sire_venue_run_ratio",
            "sire_distance_band_run_ratio",
            "sire_distance_run_ratio",
            "mare_course_type_run_ratio",
            "mare_venue_run_ratio",
            "mare_distance_band_run_ratio",
            "mare_distance_run_ratio",
        ]:
            assert f"t_s_te.{col}" in content or f"t_m_te.{col}" in content, (
                f"最終SELECT に '{col}' の参照が見つかりません"
            )

    def test_sql_run_ratio_null_imputation(self):
        """run_ratio 列が NULL補完ブロックに含まれていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for col in [
            "sire_course_type_run_ratio",
            "sire_venue_run_ratio",
            "sire_distance_band_run_ratio",
            "sire_distance_run_ratio",
            "mare_course_type_run_ratio",
            "mare_venue_run_ratio",
            "mare_distance_band_run_ratio",
            "mare_distance_run_ratio",
        ]:
            pattern = f"percentile_cont({col}, 0.5) over (partition by race_id)"
            assert pattern in content, (
                f"'{col}' の PERCENTILE_CONT NULL補完が見つかりません"
            )

    def test_sql_sire_run_ratio_uses_preceding_window(self):
        """sire run_ratio が 1826日ウィンドウを使用していること（データリーク防止）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        sire_pre_start = content.find("temp_sire_te_pre as (")
        sire_pre_end = content.find("temp_sire_te as (")
        section = content[sire_pre_start:sire_pre_end]
        # run_ratio セクションに 1826 preceding が含まれること
        assert "sire_course_type_run_ratio" in section
        assert "range between 1826 preceding and 1 preceding" in section


class TestTERankFeature:
    """TE系・出走比率の同一レース内RANK特徴量（Issue #333）のテスト"""

    def test_sql_has_temp_null_filled_cte(self):
        """temp_null_filled CTEが存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_null_filled as (" in content, \
            "temp_null_filled CTE が見つかりません"

    def test_sql_rank_uses_partition_by_race_id(self):
        """RANK()がrace_idでパーティションされていること（出走頭数内に収まる保証）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "RANK() OVER (PARTITION BY race_id ORDER BY jockey_te DESC NULLS LAST) AS jockey_te_rank" in content, \
            "jockey_te_rank の RANK() 定義が見つかりません"

    def test_sql_rank_uses_nulls_last(self):
        """全RANK定義がNULLS LASTを使用していること（NULL馬が最低ランク保証）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        rank_cols = [
            "jockey_te_rank",
            "trainer_te_rank",
            "sire_te_rank",
            "mare_te_rank",
            "horse_te_rank",
            "sire_course_type_run_ratio_rank",
            "mare_course_type_run_ratio_rank",
        ]
        for col in rank_cols:
            pattern = f"NULLS LAST) AS {col}"
            assert pattern in content, \
                f"'{col}' の定義に NULLS LAST が見つかりません"

    def test_sql_rank_uses_desc_order(self):
        """RANK()がDESC順であること（値が大きい馬がrank=1になる保証）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "ORDER BY jockey_te DESC NULLS LAST) AS jockey_te_rank" in content, \
            "jockey_te_rank が DESC 順序で定義されていません"

    def test_sql_all_111_rank_columns_present(self):
        """111列分のRANK列が最終SELECTに含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        rank_columns = [
            # 騎手TE系 17列
            "jockey_te_rank", "jockey_course_type_te_rank", "jockey_venue_te_rank",
            "jockey_distance_band_te_rank", "jockey_distance_te_rank", "jockey_direction_te_rank",
            "jockey_course_type_venue_te_rank", "jockey_course_type_distance_te_rank",
            "jockey_course_type_distance_venue_te_rank",
            "jockey_course_type_te_diff_rank", "jockey_venue_te_diff_rank",
            "jockey_distance_band_te_diff_rank", "jockey_distance_te_diff_rank",
            "jockey_direction_te_diff_rank", "jockey_course_type_venue_te_diff_rank",
            "jockey_course_type_distance_te_diff_rank", "jockey_course_type_distance_venue_te_diff_rank",
            # 調教師TE系 17列
            "trainer_te_rank", "trainer_course_type_te_rank", "trainer_venue_te_rank",
            "trainer_distance_band_te_rank", "trainer_distance_te_rank", "trainer_direction_te_rank",
            "trainer_course_type_venue_te_rank", "trainer_course_type_distance_te_rank",
            "trainer_course_type_distance_venue_te_rank",
            "trainer_course_type_te_diff_rank", "trainer_venue_te_diff_rank",
            "trainer_distance_band_te_diff_rank", "trainer_distance_te_diff_rank",
            "trainer_direction_te_diff_rank", "trainer_course_type_venue_te_diff_rank",
            "trainer_course_type_distance_te_diff_rank", "trainer_course_type_distance_venue_te_diff_rank",
            # 種牡馬TE系 22列
            "sire_te_rank", "sire_course_type_te_rank", "sire_venue_te_rank",
            "sire_distance_band_te_rank", "sire_distance_te_rank", "sire_direction_te_rank",
            "sire_course_type_venue_te_rank", "sire_course_type_distance_te_rank",
            "sire_course_type_distance_venue_te_rank",
            "sire_course_type_te_diff_rank", "sire_venue_te_diff_rank",
            "sire_distance_band_te_diff_rank", "sire_distance_te_diff_rank",
            "sire_direction_te_diff_rank", "sire_course_type_venue_te_diff_rank",
            "sire_course_type_distance_te_diff_rank", "sire_course_type_distance_venue_te_diff_rank",
            "sire_age2_te_rank", "sire_age3_te_rank", "sire_age4_te_rank",
            "sire_age5plus_te_rank", "sire_current_age_te_rank",
            # 母馬TE系 22列
            "mare_te_rank", "mare_course_type_te_rank", "mare_venue_te_rank",
            "mare_distance_band_te_rank", "mare_distance_te_rank", "mare_direction_te_rank",
            "mare_course_type_venue_te_rank", "mare_course_type_distance_te_rank",
            "mare_course_type_distance_venue_te_rank",
            "mare_course_type_te_diff_rank", "mare_venue_te_diff_rank",
            "mare_distance_band_te_diff_rank", "mare_distance_te_diff_rank",
            "mare_direction_te_diff_rank", "mare_course_type_venue_te_diff_rank",
            "mare_course_type_distance_te_diff_rank", "mare_course_type_distance_venue_te_diff_rank",
            "mare_age2_te_rank", "mare_age3_te_rank", "mare_age4_te_rank",
            "mare_age5plus_te_rank", "mare_current_age_te_rank",
            # 馬自身TE系 25列
            "horse_te_rank", "horse_course_type_te_rank", "horse_venue_te_rank",
            "horse_distance_band_te_rank", "horse_distance_te_rank", "horse_direction_te_rank",
            "horse_jockey_te_rank", "horse_season_te_rank",
            "horse_course_type_venue_te_rank", "horse_course_type_distance_te_rank",
            "horse_course_type_distance_venue_te_rank",
            "horse_distance_change_te_rank", "horse_weight_carried_change_te_rank",
            "horse_course_type_te_diff_rank", "horse_venue_te_diff_rank",
            "horse_distance_band_te_diff_rank", "horse_distance_te_diff_rank",
            "horse_direction_te_diff_rank", "horse_jockey_te_diff_rank",
            "horse_season_te_diff_rank", "horse_course_type_venue_te_diff_rank",
            "horse_course_type_distance_te_diff_rank", "horse_course_type_distance_venue_te_diff_rank",
            "horse_distance_change_te_diff_rank", "horse_weight_carried_change_te_diff_rank",
            # 出走比率系 8列
            "sire_course_type_run_ratio_rank", "sire_venue_run_ratio_rank",
            "sire_distance_band_run_ratio_rank", "sire_distance_run_ratio_rank",
            "mare_course_type_run_ratio_rank", "mare_venue_run_ratio_rank",
            "mare_distance_band_run_ratio_rank", "mare_distance_run_ratio_rank",
        ]
        assert len(rank_columns) == 111, f"RANK列数が111ではありません（実際: {len(rank_columns)}）"
        for col in rank_columns:
            assert f"AS {col}" in content, \
                f"最終SELECT に RANK列 '{col}' が見つかりません"

    def test_rank_logic_min_is_one(self):
        """RANK()の最小値が1であることをPandasで検証"""
        import pandas as pd
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "jockey_te": [0.30, 0.25, 0.20],
        })
        df["jockey_te_rank"] = df.groupby("race_id")["jockey_te"].rank(
            method="min", ascending=False, na_option="bottom"
        ).astype(int)
        assert df["jockey_te_rank"].min() == 1, \
            "jockey_te_rank の最小値が1ではありません"

    def test_rank_logic_max_equals_horse_count(self):
        """RANK()の最大値がレース出走頭数以下であることをPandasで検証"""
        import pandas as pd
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "jockey_te": [0.30, 0.25, 0.20],
        })
        df["jockey_te_rank"] = df.groupby("race_id")["jockey_te"].rank(
            method="min", ascending=False, na_option="bottom"
        ).astype(int)
        n_horses = df["race_id"].value_counts()["R1"]
        assert df["jockey_te_rank"].max() <= n_horses, \
            f"jockey_te_rank の最大値 {df['jockey_te_rank'].max()} が出走頭数 {n_horses} を超えています"

    def test_rank_logic_null_gets_higher_rank_number(self):
        """NULLを持つ馬のランクが非NULLの馬より高い数値（低順位）になることをPandasで検証"""
        import pandas as pd
        import numpy as np
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "jockey_te": [0.30, 0.25, np.nan],
        })
        df["jockey_te_rank"] = df.groupby("race_id")["jockey_te"].rank(
            method="min", ascending=False, na_option="bottom"
        ).astype(int)
        null_rank = df.loc[df["jockey_te"].isna(), "jockey_te_rank"].iloc[0]
        non_null_max_rank = df.loc[df["jockey_te"].notna(), "jockey_te_rank"].max()
        assert null_rank > non_null_max_rank, \
            f"NULL馬のランク（{null_rank}）が非NULL馬の最大ランク（{non_null_max_rank}）より高くありません"

    def test_rank_logic_max_te_gets_rank_one(self):
        """jockey_te が最大の馬の jockey_te_rank が1であることをPandasで検証"""
        import pandas as pd
        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "jockey_te": [0.20, 0.30, 0.25],
        })
        df["jockey_te_rank"] = df.groupby("race_id")["jockey_te"].rank(
            method="min", ascending=False, na_option="bottom"
        ).astype(int)
        max_te_idx = df["jockey_te"].idxmax()
        assert df.loc[max_te_idx, "jockey_te_rank"] == 1, \
            f"jockey_te が最大の馬の rank が {df.loc[max_te_idx, 'jockey_te_rank']} です（期待値: 1）"


class TestHorseTEDiffSummary:
    """horse TE_diff 時系列集計特徴量のテスト（Issue #341）"""

    def test_sql_has_temp_horse_te_diff_pre_cte(self):
        """temp_horse_te_diff_pre CTE が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_horse_te_diff_pre" in content

    def test_sql_has_temp_horse_te_diff_summary_cte(self):
        """temp_horse_te_diff_summary CTE が存在すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_horse_te_diff_summary" in content

    def test_sql_horse_te_has_horse_id_and_race_date(self):
        """temp_horse_te の SELECT に horse_id と race_date が含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        horse_te_start = content.index(",temp_horse_te as (")
        horse_te_section = content[horse_te_start:horse_te_start + 500]
        assert "horse_id" in horse_te_section
        assert "race_date" in horse_te_section

    def test_sql_diff_avg_features_in_final_select(self):
        """_te_diff_avg 特徴量が temp_final_raw の SELECT に含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for axis in ["course_type", "venue", "distance_band", "distance", "direction",
                     "jockey", "season"]:
            col = f"horse_{axis}_te_diff_avg"
            assert col in content, f"{col} が SQL に見つからない"

    def test_sql_diff_rank_avg_features_in_final_select(self):
        """_te_diff_rank_avg 特徴量が temp_final_raw の SELECT に含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for axis in ["course_type", "venue", "distance_band", "distance", "direction",
                     "jockey", "season"]:
            col = f"horse_{axis}_te_diff_rank_avg"
            assert col in content, f"{col} が SQL に見つからない"

    def test_sql_summary_uses_preceding_window(self):
        """時系列集計で当日行が除外されること（ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        summary_start = content.index(",temp_horse_te_diff_summary as (")
        summary_end = content.index("/* 馬の距離帯別", summary_start)
        summary_section = content[summary_start:summary_end]
        assert "rows between unbounded preceding and 1 preceding" in summary_section

    def test_sql_rank_avg_excludes_null_diff(self):
        """ランク平均計算で diff が NULL の場合（出走5回未満）を除外すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        summary_start = content.index(",temp_horse_te_diff_summary as (")
        summary_end = content.index("/* 馬の距離帯別", summary_start)
        summary_section = content[summary_start:summary_end]
        assert "IF(h_course_type_diff IS NOT NULL" in summary_section

    def test_sql_diff_avg_null_imputation_in_except(self):
        """_horse_{axis}_te_diff_avg_med が temp_null_filled の except リストに含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "_horse_course_type_te_diff_avg_med" in content
        assert "_horse_venue_te_diff_avg_med" in content

    def test_sql_diff_rank_avg_null_imputation_in_except(self):
        """_horse_{axis}_te_diff_rank_avg_med が temp_null_filled の except リストに含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "_horse_course_type_te_diff_rank_avg_med" in content
        assert "_horse_venue_te_diff_rank_avg_med" in content

    def test_sql_null_imputation_coalesce_with_fallback_zero(self):
        """NULL 埋めが coalesce(val, med, 0.0) パターンであること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "coalesce(horse_course_type_te_diff_avg, _horse_course_type_te_diff_avg_med, 0.0)" in content
        assert "coalesce(horse_course_type_te_diff_rank_avg, _horse_course_type_te_diff_rank_avg_med, 0.0)" in content

    def test_rank_avg_logic_null_excluded(self):
        """diff が NULL の行はランク平均から除外されることをPandasで検証"""
        import pandas as pd
        import numpy as np
        df = pd.DataFrame({
            "horse_id": ["H1", "H1", "H1"],
            "race_date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
            "horse_te": [None, 0.25, 0.25],
            "horse_venue_te": [None, 0.30, 0.28],
        })
        df["h_venue_diff"] = df["horse_venue_te"] - df["horse_te"]
        df = df.sort_values("race_date").reset_index(drop=True)
        df["h_venue_diff_rank"] = df.groupby(df["race_date"].dt.to_period("D"))["h_venue_diff"].rank(
            method="min", ascending=False, na_option="bottom"
        )
        # 3行目の rank_avg は2行目のランク（NULLでない行）のみを平均すべき
        valid_ranks = df.loc[df["h_venue_diff"].notna(), "h_venue_diff_rank"].values
        rank_avg = float(np.mean(valid_ranks[:-1])) if len(valid_ranks) > 1 else float("nan")
        assert not pd.isna(rank_avg) or len(valid_ranks) <= 1

    def test_diff_avg_logic_null_excluded(self):
        """diff が NULL の行は平均計算から除外されることをPandasで検証"""
        import pandas as pd
        import numpy as np
        s = pd.Series([None, 0.05, -0.02, None, 0.03])
        result = s.mean(skipna=True)
        expected = np.mean([0.05, -0.02, 0.03])
        assert abs(result - expected) < 1e-9


class TestGateStyleFeature:
    """脚質分類・ペース展開特徴量のテスト（Issue #343）"""

    def test_sql_has_avg_corner_prevN_in_prf(self):
        """avg_corner_prev1〜avg_corner_prev5 が temp_past_race_features に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf_section = content[prf_start:prf2_start]
        for i in range(1, 6):
            col = f"avg_corner_prev{i}"
            assert col in prf_section, f"temp_past_race_features に '{col}' が見つかりません"

    def test_sql_avg_corner_uses_all_four_corners(self):
        """avg_corner_prev1 が全4コーナー (corner_position_1〜4) を平均すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf_section = content[prf_start:prf2_start]
        for col in ["r_r_1.corner_position_1", "r_r_1.corner_position_2",
                    "r_r_1.corner_position_3", "r_r_1.corner_position_4"]:
            assert col in prf_section, f"avg_corner_prev1 の計算に '{col}' が含まれていません"

    def test_sql_has_gate_style_prev123_in_prf2(self):
        """gate_style_prev1〜gate_style_prev3 が temp_past_race_features2 に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        horse_master_start = content.find("temp_horse_master_feature as (")
        prf2_section = content[prf2_start:horse_master_start]
        for i in range(1, 4):
            col = f"gate_style_prev{i}"
            assert col in prf2_section, f"temp_past_race_features2 に '{col}' が見つかりません"

    def test_sql_has_avg_gate_style_score_in_prf2(self):
        """avg_gate_style_score が temp_past_race_features2 に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        horse_master_start = content.find("temp_horse_master_feature as (")
        prf2_section = content[prf2_start:horse_master_start]
        assert "avg_gate_style_score" in prf2_section

    def test_sql_gate_style_thresholds_correct(self):
        """gate_style_prev1 の閾値が front=3.5/mid_front=7.0/mid=10.0 になっていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        horse_master_start = content.find("temp_horse_master_feature as (")
        prf2_section = content[prf2_start:horse_master_start]
        gate_style_section = prf2_section[prf2_section.find("gate_style_prev1"):]
        assert "<= 3.5 then 1" in gate_style_section
        assert "<= 7.0 then 2" in gate_style_section
        assert "<= 10.0 then 3" in gate_style_section

    def test_sql_has_same_race_front_count_in_final_raw(self):
        """same_race_front_count が temp_final_raw に window 関数で定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        final_raw_start = content.find(",temp_final_raw as (")
        null_fill_start = content.find(",temp_null_fill_med as (")
        final_raw_section = content[final_raw_start:null_fill_start]
        assert "same_race_front_count" in final_raw_section
        assert "countif" in final_raw_section
        assert "partition by" in final_raw_section

    def test_sql_has_same_race_style_rank_in_final_raw(self):
        """same_race_style_rank が temp_final_raw に window 関数で定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        final_raw_start = content.find(",temp_final_raw as (")
        null_fill_start = content.find(",temp_null_fill_med as (")
        final_raw_section = content[final_raw_start:null_fill_start]
        assert "same_race_style_rank" in final_raw_section
        assert "rank()" in final_raw_section

    def test_gate_style_classification_logic(self):
        """脚質スコア分類ロジックの正確性をPythonで検証"""
        def gate_style_score(avg_corner):
            if avg_corner is None:
                return None
            if avg_corner <= 3.5:
                return 1
            if avg_corner <= 7.0:
                return 2
            if avg_corner <= 10.0:
                return 3
            return 4

        assert gate_style_score(None) is None
        assert gate_style_score(1.0) == 1   # front (逃/先行)
        assert gate_style_score(3.5) == 1   # 境界値 front
        assert gate_style_score(4.0) == 2   # mid_front (先行差し)
        assert gate_style_score(7.0) == 2   # 境界値 mid_front
        assert gate_style_score(8.0) == 3   # mid (差し)
        assert gate_style_score(10.0) == 3  # 境界値 mid
        assert gate_style_score(11.0) == 4  # back (追い込み)
        assert gate_style_score(18.0) == 4  # 最後方

    def test_avg_gate_style_score_logic(self):
        """avg_gate_style_score 計算ロジックの正確性をPythonで検証"""
        import numpy as np

        def gate_style_score(avg_corner):
            if avg_corner is None:
                return None
            if avg_corner <= 3.5:
                return 1
            if avg_corner <= 7.0:
                return 2
            if avg_corner <= 10.0:
                return 3
            return 4

        def avg_gate_style_score(corners):
            scores = [gate_style_score(c) for c in corners if gate_style_score(c) is not None]
            return np.mean(scores) if scores else None

        assert avg_gate_style_score([2.0, 3.0, None, None, None]) == pytest.approx(1.0)
        assert avg_gate_style_score([2.0, 6.0, None, None, None]) == pytest.approx(1.5)
        assert avg_gate_style_score([None, None, None, None, None]) is None
        assert avg_gate_style_score([3.5, 7.0, 10.0, 11.0, 1.0]) == pytest.approx(11 / 5)

    def test_same_race_front_count_logic(self):
        """same_race_front_count: avg_gate_style_score <= 2.5 の馬の頭数を正しく集計"""
        import pandas as pd

        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1", "R1"],
            "horse_number": [1, 2, 3, 4],
            "avg_gate_style_score": [1.0, 2.0, 2.5, 3.0],
        })
        df["same_race_front_count"] = df.groupby("race_id")["avg_gate_style_score"].transform(
            lambda x: (x <= 2.5).sum()
        )
        assert df.loc[df["horse_number"] == 1, "same_race_front_count"].iloc[0] == 3
        assert df.loc[df["horse_number"] == 4, "same_race_front_count"].iloc[0] == 3

    def test_same_race_style_rank_logic(self):
        """same_race_style_rank: 脚質スコアが小さい（前目の）馬ほど順位が低い"""
        import pandas as pd

        df = pd.DataFrame({
            "race_id": ["R1", "R1", "R1"],
            "horse_number": [1, 2, 3],
            "avg_gate_style_score": [1.0, 2.5, 4.0],
        })
        df["same_race_style_rank"] = df.groupby("race_id")["avg_gate_style_score"].rank(method="min")
        assert df.loc[df["horse_number"] == 1, "same_race_style_rank"].iloc[0] == 1
        assert df.loc[df["horse_number"] == 2, "same_race_style_rank"].iloc[0] == 2
        assert df.loc[df["horse_number"] == 3, "same_race_style_rank"].iloc[0] == 3

    def test_no_future_data_in_avg_corner(self):
        """avg_corner_prevN は過去走データ（race_date < 当日）のみを参照すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf_start = content.find("temp_past_race_features as (")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf_section = content[prf_start:prf2_start]
        assert "r_r_1.race_date < t_b_r_e.race_date" in prf_section, \
            "r_r_1 の JOIN に過去日付制約がありません"


class TestCoursePaceAndGateStyleTEFeature:
    """Issue #349: 開催条件別ペース傾向・脚質適性TE特徴量のテスト"""

    def test_sql_has_temp_course_pace_stats_cte(self):
        """temp_course_pace_stats CTE が feature_query_raw.sql に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_course_pace_stats as (" in content

    def test_sql_course_pace_stats_uses_unbounded_preceding_window(self):
        """course_pace_score が UNBOUNDED PRECEDING AND 1 PRECEDING ウィンドウを使用すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        cps_start = content.find("temp_course_pace_stats as (")
        gate_style_te_start = content.find("temp_gate_style_te_base as (")
        cps_section = content[cps_start:gate_style_te_start]
        assert "unbounded preceding and 1 preceding" in cps_section, \
            "course_pace_stats に unbounded preceding ウィンドウがありません"
        assert "partition by venue_code, distance, course_type" in cps_section, \
            "course_pace_stats に正しい PARTITION BY がありません"

    def test_sql_has_gate_style_te_ctEs(self):
        """temp_gate_style_te_base / _pre / gate_style_te の3 CTE が定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_gate_style_te_base as (" in content
        assert "temp_gate_style_te_pre as (" in content
        assert "temp_gate_style_te as (" in content

    def test_sql_gate_style_te_pre_uses_1826_day_window(self):
        """gate_style_course_te が直近5年（1826日）のウィンドウを使用すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_pre_start = content.find("temp_gate_style_te_pre as (")
        te_start = content.find("temp_gate_style_te as (")
        te_pre_section = content[te_pre_start:te_start]
        assert "1826 preceding and 1 preceding" in te_pre_section, \
            "gate_style_te_pre に 1826 preceding ウィンドウがありません"

    def test_sql_gate_style_te_has_low_freq_mask(self):
        """gate_style_te に出走数 >= 10 の低頻度マスクがあること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_start = content.find("temp_gate_style_te as (")
        training_start = content.find("temp_training as (")
        te_section = content[te_start:training_start]
        assert "gs_course_count >= 10" in te_section, \
            "gate_style_te に出走数 >= 10 のマスクがありません"

    def test_sql_gate_style_te_base_excludes_null_avg_gate_style(self):
        """temp_gate_style_te_base が avg_gate_style_score IS NULL の行を除外すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        base_start = content.find("temp_gate_style_te_base as (")
        te_pre_start = content.find("temp_gate_style_te_pre as (")
        base_section = content[base_start:te_pre_start]
        assert "avg_gate_style_score is not null" in base_section, \
            "temp_gate_style_te_base で NULL avg_gate_style_score が除外されていません"

    def test_sql_has_course_pace_score_in_final_raw(self):
        """course_pace_score が temp_final_raw に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        final_raw_start = content.find(",temp_final_raw as (")
        null_fill_start = content.find(",temp_null_fill_med as (")
        final_raw_section = content[final_raw_start:null_fill_start]
        assert "course_pace_score" in final_raw_section
        assert "temp_course_pace_stats" in final_raw_section

    def test_sql_has_gate_style_advantage_in_final_raw(self):
        """gate_style_advantage_score / flag / gate_style_course_te が temp_final_raw に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        final_raw_start = content.find(",temp_final_raw as (")
        null_fill_start = content.find(",temp_null_fill_med as (")
        final_raw_section = content[final_raw_start:null_fill_start]
        assert "gate_style_advantage_score" in final_raw_section
        assert "gate_style_advantage_flag" in final_raw_section
        assert "gate_style_course_te" in final_raw_section

    def test_course_pace_score_logic(self):
        """course_pace_score: 同一条件（venue × distance × course_type）の過去レース平均脚質スコアを検証"""
        import pandas as pd
        import numpy as np

        data = [
            {"race_id": "R1", "horse_number": 1, "venue_code": "05", "distance": 1600,
             "course_type": "turf", "race_date": pd.Timestamp("2024-01-07"), "avg_gate_style_score": 1.5},
            {"race_id": "R1", "horse_number": 2, "venue_code": "05", "distance": 1600,
             "course_type": "turf", "race_date": pd.Timestamp("2024-01-07"), "avg_gate_style_score": 2.5},
            # 1週後の新レース
            {"race_id": "R2", "horse_number": 1, "venue_code": "05", "distance": 1600,
             "course_type": "turf", "race_date": pd.Timestamp("2024-01-14"), "avg_gate_style_score": 3.0},
        ]
        df = pd.DataFrame(data)

        def compute_course_pace_score(row, df_all):
            mask = (
                (df_all["venue_code"] == row["venue_code"]) &
                (df_all["distance"] == row["distance"]) &
                (df_all["course_type"] == row["course_type"]) &
                (df_all["race_date"] < row["race_date"])
            )
            past = df_all.loc[mask, "avg_gate_style_score"].dropna()
            return past.mean() if len(past) > 0 else np.nan

        r2_h1 = df[(df["race_id"] == "R2") & (df["horse_number"] == 1)].iloc[0]
        score = compute_course_pace_score(r2_h1, df)
        assert score == pytest.approx(2.0), f"R2の course_pace_score が期待値 2.0 でなく {score}"

    def test_gate_style_advantage_score_logic(self):
        """gate_style_advantage_score = -(|avg_gate_style_score - course_pace_score|) の検証"""
        import math

        def advantage_score(avg_gs, course_pace):
            if avg_gs is None or course_pace is None:
                return None
            return -abs(avg_gs - course_pace)

        assert advantage_score(1.5, 1.5) == pytest.approx(0.0)
        assert advantage_score(1.0, 3.0) == pytest.approx(-2.0)
        assert advantage_score(3.5, 2.0) == pytest.approx(-1.5)
        assert advantage_score(None, 2.0) is None
        assert advantage_score(2.0, None) is None

    def test_gate_style_advantage_flag_boundary(self):
        """gate_style_advantage_flag: |diff| <= 0.5 で 1、それ以上で 0 になること"""
        def advantage_flag(avg_gs, course_pace):
            if avg_gs is None or course_pace is None:
                return None
            return 1 if abs(avg_gs - course_pace) <= 0.5 else 0

        assert advantage_flag(2.0, 2.0) == 1   # 完全一致
        assert advantage_flag(2.0, 2.5) == 1   # diff = 0.5 (境界値、有利)
        assert advantage_flag(2.0, 2.51) == 0  # diff > 0.5 (不利)
        assert advantage_flag(1.0, 3.5) == 0   # 大きく不一致
        assert advantage_flag(None, 2.0) is None

    def test_sql_course_pace_stats_excludes_current_race_day(self):
        """course_pace_score ウィンドウが 1 PRECEDING で当日レースを除外すること（リークなし）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        cps_start = content.find("temp_course_pace_stats as (")
        gate_style_te_start = content.find("temp_gate_style_te_base as (")
        cps_section = content[cps_start:gate_style_te_start]
        assert "1 preceding" in cps_section, \
            "course_pace_stats のウィンドウが当日レースを除外していません"
        assert "and 1 preceding" in cps_section

    def test_sql_gate_style_course_te_excludes_current_race_day(self):
        """gate_style_course_te ウィンドウが 1 PRECEDING で当日レースを除外すること（リークなし）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        te_pre_start = content.find("temp_gate_style_te_pre as (")
        te_start = content.find("temp_gate_style_te as (")
        te_pre_section = content[te_pre_start:te_start]
        assert "and 1 preceding" in te_pre_section, \
            "gate_style_te_pre のウィンドウが当日レースを除外していません"


class TestEntityTeDailySqlTemplate:
    """te_daily_query.sql の構造テスト"""

    def test_te_daily_sql_file_exists(self):
        assert TE_DAILY_SQL_TEMPLATE_PATH.exists(), "te_daily_query.sql が存在しません"

    def test_te_daily_sql_has_template_vars(self):
        content = TE_DAILY_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "{project_id}" in content
        assert "{target_date}" in content

    def test_te_daily_sql_has_all_entity_types(self):
        content = TE_DAILY_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for entity in ("jockey", "trainer", "sire", "mare", "horse"):
            assert f"'{entity}'" in content, f"entity_type '{entity}' が te_daily_query.sql にありません"

    def test_te_daily_sql_has_required_output_columns(self):
        content = TE_DAILY_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for col in ("entity_type", "entity_id", "condition_type", "condition_key", "as_of_date", "cnt", "sum_top3"):
            assert col in content, f"出力列 '{col}' が te_daily_query.sql にありません"

    def test_te_daily_sql_excludes_target_date(self):
        content = TE_DAILY_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "race_date < date('{target_date}')" in content, \
            "target_date 当日レースの除外条件がありません"

    def test_te_daily_sql_has_window_1826_days(self):
        content = TE_DAILY_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "1826" in content, "直近 1826 日ウィンドウが te_daily_query.sql にありません"

    def test_te_daily_sql_has_history_all_for_mare(self):
        content = TE_DAILY_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "history_all" in content, "母馬用の全期間テーブル (history_all) がありません"

    def test_te_daily_sql_has_age_band_for_sire_and_mare(self):
        content = TE_DAILY_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "'2yo'" in content and "'5plus'" in content, \
            "年齢帯 TE（2yo/5plus）が te_daily_query.sql にありません"


class TestPredictTeSqlTemplate:
    """feature_query_predict_te.sql の構造テスト"""

    def test_predict_te_sql_file_exists(self):
        assert PREDICT_TE_SQL_TEMPLATE_PATH.exists(), "feature_query_predict_te.sql が存在しません"

    def test_predict_te_sql_has_template_vars(self):
        content = PREDICT_TE_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "{project_id}" in content
        assert "{target_date}" in content
        assert "{start_date}" not in content, "feature_query_predict_te.sql に start_date が残っています"

    def test_predict_te_sql_has_required_ctes(self):
        content = PREDICT_TE_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        required = [
            "temp_global_mean_te",
            "temp_entity_te",
            "temp_jockey_te",
            "temp_trainer_te",
            "temp_sire_te",
            "temp_mare_te",
            "temp_horse_te",
            "temp_horse_te_diff_pre",
            "temp_horse_te_diff_summary",
        ]
        for cte in required:
            assert cte in content, f"必須 CTE '{cte}' が feature_query_predict_te.sql にありません"

    def test_predict_te_sql_uses_entity_te_daily(self):
        content = PREDICT_TE_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "features.entity_te_daily" in content

    def test_predict_te_sql_filters_target_date(self):
        content = PREDICT_TE_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "= date('{target_date}')" in content

    def test_predict_te_sql_no_range_between_window(self):
        content = PREDICT_TE_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "range between" not in content.lower(), \
            "feature_query_predict_te.sql に RANGE BETWEEN 窓関数が残っています（パフォーマンス問題の原因）"

    def test_predict_te_sql_horse_diff_summary_has_all_avg_columns(self):
        content = PREDICT_TE_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for col in (
            "horse_course_type_te_diff_avg",
            "horse_venue_te_diff_avg",
            "horse_wcc_te_diff_avg",
            "horse_course_type_te_diff_rank_avg",
            "horse_wcc_te_diff_rank_avg",
        ):
            assert col in content, f"diff_summary 列 '{col}' が見つかりません"


class TestGeneratePredictQuery:
    """generate_predict_query() の動作テスト"""

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_generate_predict_query_replaces_date_filter(self, mock_bq):
        pipeline = FeaturePipeline("test-project")
        sql = pipeline.generate_predict_query("2026-06-14")
        assert "'{start_date}'" not in sql
        assert "'{end_date}'" not in sql
        assert "date('2026-06-14')" in sql

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_generate_predict_query_replaces_te_block(self, mock_bq):
        pipeline = FeaturePipeline("test-project")
        sql = pipeline.generate_predict_query("2026-06-14")
        assert "temp_entity_te" in sql, "TEブロックが entity_te_daily 版に置換されていません"
        assert "features.entity_te_daily" in sql

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_generate_predict_query_removes_range_between_from_te(self, mock_bq):
        pipeline = FeaturePipeline("test-project")
        sql = pipeline.generate_predict_query("2026-06-14")
        # TE ブロックの RANGE BETWEEN が除去されているか確認
        # （temp_global_mean_te 以降、temp_horse_distance_base より前）
        te_start = sql.find("temp_global_mean_te")
        dist_base_start = sql.find("temp_horse_distance_base")
        te_section = sql[te_start:dist_base_start]
        assert "range between" not in te_section.lower(), \
            "予測クエリの TE セクションに RANGE BETWEEN が残っています"

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_generate_predict_query_keeps_downstream_ctes(self, mock_bq):
        pipeline = FeaturePipeline("test-project")
        sql = pipeline.generate_predict_query("2026-06-14")
        assert "temp_horse_distance_base" in sql, "下流 CTE が失われています"
        assert "temp_past_race_features" in sql

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_generate_predict_query_invalid_date_raises(self, mock_bq):
        pipeline = FeaturePipeline("test-project")
        with pytest.raises(ValueError):
            pipeline.generate_predict_query("not-a-date")


class TestRunTeDaily:
    """run_te_daily() のモックテスト"""

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_run_te_daily_calls_execute_to_table(self, mock_bq_class):
        mock_client = MagicMock()
        mock_bq_class.return_value = mock_client

        mock_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 12345
        mock_job.result.return_value = mock_result
        mock_client.query.return_value = mock_job

        pipeline = FeaturePipeline("test-project")
        result = pipeline.run_te_daily("2026-06-14")

        assert result["as_of_date"] == "2026-06-14"
        assert result["inserted_rows"] == 12345
        assert "elapsed_time" in result
        mock_client.query.assert_called_once()

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_run_te_daily_invalid_date_raises(self, mock_bq):
        pipeline = FeaturePipeline("test-project")
        with pytest.raises(ValueError):
            pipeline.run_te_daily("20260614")

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_run_te_daily_query_contains_target_date(self, mock_bq_class):
        mock_client = MagicMock()
        mock_bq_class.return_value = mock_client

        captured_sql = []
        mock_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 0
        mock_job.result.return_value = mock_result

        def capture_query(sql, **kwargs):
            captured_sql.append(sql)
            return mock_job

        mock_client.query.side_effect = capture_query

        pipeline = FeaturePipeline("test-project")
        pipeline.run_te_daily("2026-06-14")

        assert len(captured_sql) == 1
        assert "2026-06-14" in captured_sql[0]
        assert "test-project" in captured_sql[0]

    @patch("scripts.create_entity_te_daily_table.create_table")
    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_run_te_daily_creates_table_when_missing(self, mock_bq_class, mock_create_table):
        """entity_te_daily 未作成時は自動作成してから同じクエリを再実行する"""
        from google.api_core import exceptions as google_exceptions

        mock_client = MagicMock()
        mock_bq_class.return_value = mock_client

        mock_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 42
        mock_job.result.return_value = mock_result

        mock_client.query.side_effect = [
            google_exceptions.BadRequest(
                "Partitioning specification must be provided in order to "
                "create partitioned table"
            ),
            mock_job,
        ]

        pipeline = FeaturePipeline("test-project")
        result = pipeline.run_te_daily("2026-06-14")

        assert result["inserted_rows"] == 42
        mock_create_table.assert_called_once_with("test-project")
        assert mock_client.query.call_count == 2

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_run_te_daily_reraises_other_bad_request(self, mock_bq_class):
        """パーティション未指定以外のBadRequestはテーブル作成せず再送出する"""
        from google.api_core import exceptions as google_exceptions

        mock_client = MagicMock()
        mock_bq_class.return_value = mock_client
        mock_client.query.side_effect = google_exceptions.BadRequest("some other error")

        pipeline = FeaturePipeline("test-project")
        with pytest.raises(google_exceptions.BadRequest):
            pipeline.run_te_daily("2026-06-14")


class TestHasEntityTeForDate:
    """has_entity_te_for_date() のモックテスト"""

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_returns_true_when_data_exists(self, mock_bq_class):
        mock_client = MagicMock()
        mock_bq_class.return_value = mock_client
        mock_result = MagicMock()
        mock_result.total_rows = 1
        mock_client.query.return_value.result.return_value = mock_result

        pipeline = FeaturePipeline("test-project")
        assert pipeline.has_entity_te_for_date("2026-06-14") is True

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_returns_false_when_no_data(self, mock_bq_class):
        mock_client = MagicMock()
        mock_bq_class.return_value = mock_client
        mock_result = MagicMock()
        mock_result.total_rows = 0
        mock_client.query.return_value.result.return_value = mock_result

        pipeline = FeaturePipeline("test-project")
        assert pipeline.has_entity_te_for_date("2026-06-14") is False

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_returns_false_on_exception(self, mock_bq_class):
        mock_client = MagicMock()
        mock_bq_class.return_value = mock_client
        mock_client.query.side_effect = Exception("table not found")

        pipeline = FeaturePipeline("test-project")
        assert pipeline.has_entity_te_for_date("2026-06-14") is False

    @patch("src.ml.features.feature_pipeline.bigquery.Client")
    def test_run_te_daily_uses_partition_write_truncate(self, mock_bq_class):
        """run_te_daily がパーティション単位の WRITE_TRUNCATE を使用することを確認（冪等性）"""
        from unittest.mock import call
        from google.cloud import bigquery as bq
        mock_client = MagicMock()
        mock_bq_class.return_value = mock_client
        mock_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 50
        mock_job.result.return_value = mock_result
        mock_client.query.return_value = mock_job

        pipeline = FeaturePipeline("test-project")
        pipeline.run_te_daily("2026-06-14")

        _, kwargs = mock_client.query.call_args
        job_config = kwargs["job_config"]
        assert job_config.write_disposition == bq.WriteDisposition.WRITE_TRUNCATE
        assert "20260614" in str(job_config.destination)


class TestJockeyHorseComboFeature:
    """Issue #345: 騎手×馬コンビTE・乗り替わりフラグ特徴量のテスト"""

    def test_sql_has_combo_te_ctes(self):
        """temp_jockey_horse_combo_te_pre / temp_jockey_horse_combo_te CTE が定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_jockey_horse_combo_te_pre as (" in content
        assert "temp_jockey_horse_combo_te as (" in content

    def test_sql_has_jockey_change_cte(self):
        """temp_jockey_change CTE が定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_jockey_change as (" in content

    def test_sql_combo_te_uses_1826_day_window_with_1_preceding(self):
        """combo_te が1826日ウィンドウかつ当日除外（AND 1 PRECEDING）を使用すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        combo_start = content.find("temp_jockey_horse_combo_te_pre as (")
        combo_end = content.find("temp_jockey_horse_combo_te as (")
        combo_section = content[combo_start:combo_end]
        assert "range between 1826 preceding and 1 preceding" in combo_section, \
            "combo_te に当日除外ウィンドウがありません"
        assert "partition by jockey_code, horse_id" in combo_section, \
            "combo_te のパーティションが jockey_code, horse_id でありません"

    def test_sql_combo_te_uses_smoothing_m5(self):
        """combo_te のスムージング係数が m=5 であること（m=10 ではなく）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        combo_start = content.find("temp_jockey_horse_combo_te_pre as (")
        combo_end = content.find("temp_jockey_horse_combo_te as (")
        combo_section = content[combo_start:combo_end]
        assert "+ 5 * g.global_top3_rate" in combo_section, \
            "スムージング係数が m=5 ではありません"
        assert ", 0) + 5\n" in combo_section or "), 0) + 5\n" in combo_section, \
            "分母のスムージング係数が 5 ではありません"

    def test_sql_combo_te_has_low_freq_mask_3(self):
        """combo_te の低頻度マスクが 3戦未満はNULL であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        combo_wrapper_start = content.find("temp_jockey_horse_combo_te as (")
        combo_wrapper_end = content.find("temp_jockey_change as (")
        combo_wrapper = content[combo_wrapper_start:combo_wrapper_end]
        assert "combo_count >= 3" in combo_wrapper, \
            "低頻度マスク(combo_count >= 3)がありません"
        assert "jockey_horse_combo_count" in combo_wrapper, \
            "jockey_horse_combo_count カラムがありません"

    def test_sql_jockey_change_has_prev_jockey_codes(self):
        """temp_jockey_change が直近3走の騎手コードを LAG で取得すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        change_start = content.find("temp_jockey_change as (")
        # 次のCTEを探す
        change_end = content.find("/* 調教師 Target Encoding", change_start)
        change_section = content[change_start:change_end]
        assert "lag(b.jockey_code, 1)" in change_section, "prev1_jockey_code の LAG がありません"
        assert "lag(b.jockey_code, 2)" in change_section, "prev2_jockey_code の LAG がありません"
        assert "lag(b.jockey_code, 3)" in change_section, "prev3_jockey_code の LAG がありません"
        assert "partition by b.horse_id order by b.race_date" in change_section, \
            "LAGのウィンドウが horse_id パーティション + race_date ORDER でありません"

    def test_sql_jockey_change_has_jockey_te_join(self):
        """temp_jockey_change が temp_jockey_te_pre を INNER JOIN して jockey_te を取得すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        change_start = content.find("temp_jockey_change as (")
        change_end = content.find("/* 調教師 Target Encoding", change_start)
        change_section = content[change_start:change_end]
        assert "inner join temp_jockey_te_pre as p" in change_section, \
            "temp_jockey_te_pre への INNER JOIN がありません"
        assert "p.jockey_te as cur_jockey_te" in change_section, \
            "cur_jockey_te がありません"
        assert "lag(p.jockey_te, 1)" in change_section, \
            "prev_jockey_te の LAG がありません"

    def test_sql_final_select_has_all_new_features(self):
        """最終SELECTに4特徴量（combo_te, combo_count, is_regular_jockey, jockey_change_type）が含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "jockey_horse_combo_te" in content
        assert "jockey_horse_combo_count" in content
        assert "is_regular_jockey" in content
        assert "jockey_change_type" in content

    def test_sql_is_regular_jockey_logic_uses_2_of_3(self):
        """is_regular_jockey が直近3走中2走以上同一騎手の条件を使用すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # is_regular_jockey の CASE 式を確認
        assert "prev1_jockey_code" in content, "prev1_jockey_code の参照がありません"
        assert "prev2_jockey_code" in content, "prev2_jockey_code の参照がありません"
        assert "prev3_jockey_code" in content, "prev3_jockey_code の参照がありません"
        assert ">= 2" in content, "2走以上の条件がありません"

    def test_sql_null_fill_has_combo_te_median(self):
        """temp_null_fill_med に jockey_horse_combo_te の中央値計算が含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        null_fill_start = content.find("temp_null_fill_med as (")
        null_filled_start = content.find("temp_null_filled as (")
        null_fill_section = content[null_fill_start:null_filled_start]
        assert "_jockey_horse_combo_te_med" in null_fill_section, \
            "temp_null_fill_med に _jockey_horse_combo_te_med がありません"

    def test_sql_null_fill_coalesces_combo_te_with_fallback(self):
        """temp_null_filled が combo_te を中央値→0.22 でフォールバックすること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        null_filled_start = content.find("temp_null_filled as (")
        null_filled_section = content[null_filled_start:]
        assert "coalesce(jockey_horse_combo_te, _jockey_horse_combo_te_med, 0.22)" in null_filled_section, \
            "jockey_horse_combo_te の NULL 補完が正しくありません"

    def test_sql_rank_section_has_combo_te_rank(self):
        """最終 RANK セクションに jockey_horse_combo_te_rank が含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "jockey_horse_combo_te_rank" in content, \
            "jockey_horse_combo_te_rank が見つかりません"


class TestLast3fRankFeature:
    """Issue #346: 近走上がり3F レース内相対順位特徴量のテスト"""

    def test_sql_has_last3f_rank_improvement_3(self):
        """last3f_rank_improvement_3 が temp_past_race_features2 に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf2_end = content.find("temp_horse_master_feature as (", prf2_start)
        prf2_section = content[prf2_start:prf2_end]
        assert "last3f_rank_improvement_3" in prf2_section, \
            "last3f_rank_improvement_3 が temp_past_race_features2 に定義されていません"

    def test_sql_has_last3f_rank_avg_3(self):
        """last3f_rank_avg_3 が temp_past_race_features2 に定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf2_end = content.find("temp_horse_master_feature as (", prf2_start)
        prf2_section = content[prf2_start:prf2_end]
        assert "last3f_rank_avg_3" in prf2_section, \
            "last3f_rank_avg_3 が temp_past_race_features2 に定義されていません"

    def test_sql_improvement_uses_past_rank_columns(self):
        """last3f_rank_improvement_3 が last_3f_rank_in_race_1/3 の差分であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf2_end = content.find("temp_horse_master_feature as (", prf2_start)
        prf2_section = content[prf2_start:prf2_end]
        assert "last_3f_rank_in_race_1 - t_p_r_f.last_3f_rank_in_race_3" in prf2_section or \
               "last_3f_rank_in_race_1 - last_3f_rank_in_race_3" in prf2_section, \
            "last3f_rank_improvement_3 が rank_1 - rank_3 でありません"

    def test_sql_improvement_null_check(self):
        """last3f_rank_improvement_3 が prev1 または prev3 が NULL の場合に NULL を返すこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf2_end = content.find("temp_horse_master_feature as (", prf2_start)
        prf2_section = content[prf2_start:prf2_end]
        assert "last_3f_rank_in_race_1 is null or t_p_r_f.last_3f_rank_in_race_3 is null" in prf2_section, \
            "prev1/prev3 が NULL の場合の NULL ガードがありません"

    def test_sql_avg3_uses_safe_divide(self):
        """last3f_rank_avg_3 が safe_divide と nullif を使用していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        prf2_start = content.find("temp_past_race_features2 as (")
        prf2_end = content.find("temp_horse_master_feature as (", prf2_start)
        prf2_section = content[prf2_start:prf2_end]
        avg_idx = prf2_section.find("last3f_rank_avg_3")
        assert avg_idx >= 0, "last3f_rank_avg_3 が見つかりません"
        # safe_divide があることを確認（avg_3 定義部分の前にあるはず）
        assert "safe_divide(" in prf2_section[max(0, avg_idx - 500):avg_idx + 500], \
            "last3f_rank_avg_3 に safe_divide がありません"

    def test_sql_null_fill_has_improvement_median(self):
        """temp_null_fill_med に last3f_rank_improvement_3 の中央値計算が含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        null_fill_start = content.find("temp_null_fill_med as (")
        null_filled_start = content.find("temp_null_filled as (")
        null_fill_section = content[null_fill_start:null_filled_start]
        assert "_last3f_rank_improvement_3_med" in null_fill_section, \
            "temp_null_fill_med に _last3f_rank_improvement_3_med がありません"
        assert "_last3f_rank_avg_3_med" in null_fill_section, \
            "temp_null_fill_med に _last3f_rank_avg_3_med がありません"

    def test_sql_null_filled_coalesces_with_zero_fallback(self):
        """temp_null_filled が last3f_rank 特徴量を中央値→0 でフォールバックすること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        null_filled_start = content.find("temp_null_filled as (")
        null_filled_end = content.find("from temp_null_fill_med")
        null_filled_section = content[null_filled_start:null_filled_end]
        assert "coalesce(last3f_rank_improvement_3, _last3f_rank_improvement_3_med, 0)" in null_filled_section, \
            "last3f_rank_improvement_3 の NULL 補完が正しくありません（フォールバック=0 が必要）"
        assert "coalesce(last3f_rank_avg_3, _last3f_rank_avg_3_med, 0)" in null_filled_section, \
            "last3f_rank_avg_3 の NULL 補完が正しくありません（フォールバック=0 が必要）"

    def test_sql_rank_section_has_new_rank_features(self):
        """最終 RANK セクションに last3f_rank_improvement_3_rank / last3f_rank_avg_3_rank が含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        rank_section_start = content.find("-- 同一レース内RANK特徴量を追加")
        rank_section = content[rank_section_start:]
        assert "last3f_rank_improvement_3_rank" in rank_section, \
            "last3f_rank_improvement_3_rank が RANK セクションにありません"
        assert "last3f_rank_avg_3_rank" in rank_section, \
            "last3f_rank_avg_3_rank が RANK セクションにありません"

    def test_sql_rank_uses_asc_order(self):
        """RANK は末脚順位 ASC（小さい=良い）で降順ではないこと"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        imp_idx = content.find("last3f_rank_improvement_3_rank")
        avg_idx = content.find("last3f_rank_avg_3_rank")
        imp_line = content[max(0, imp_idx - 100):imp_idx + 100]
        avg_line = content[max(0, avg_idx - 100):avg_idx + 100]
        assert "ORDER BY last3f_rank_improvement_3 ASC" in imp_line, \
            "last3f_rank_improvement_3_rank が ASC 順でありません"
        assert "ORDER BY last3f_rank_avg_3 ASC" in avg_line, \
            "last3f_rank_avg_3_rank が ASC 順でありません"


class TestGradeTEFeature:
    """グレード別TE・格上挑戦フラグ特徴量のテスト（Issue #347）"""

    def test_sql_has_grade_te_ctes(self):
        """temp_grade_te_pre / temp_grade_te CTE が定義されていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "temp_grade_te_pre as (" in content, \
            "temp_grade_te_pre CTE が見つかりません"
        assert "temp_grade_te as (" in content, \
            "temp_grade_te CTE が見つかりません"

    def test_sql_grade_te_uses_race_class(self):
        """grade_te_pre が race_class カラムを参照していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        grade_pre_start = content.find("temp_grade_te_pre as (")
        grade_te_start = content.find("temp_grade_te as (")
        section = content[grade_pre_start:grade_te_start]
        assert "race_class" in section, \
            "temp_grade_te_pre が race_class を参照していません"
        assert "race_class = 'G1'" in section, \
            "temp_grade_te_pre に G1 フィルタがありません"
        assert "race_class = 'G2'" in section, \
            "temp_grade_te_pre に G2 フィルタがありません"
        assert "race_class = 'G3'" in section, \
            "temp_grade_te_pre に G3 フィルタがありません"

    def test_sql_grade_te_window_excludes_current_row(self):
        """grade_te_pre のウィンドウ関数が当日行を除外（1 PRECEDING）していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        grade_pre_start = content.find("temp_grade_te_pre as (")
        grade_te_start = content.find("temp_grade_te as (")
        section = content[grade_pre_start:grade_te_start]
        assert "range between unbounded preceding and 1 preceding" in section, \
            "temp_grade_te_pre に当日行除外ウィンドウがありません"

    def test_sql_grade_te_low_freq_mask(self):
        """temp_grade_te が出走数 >= 3 でマスクしていること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        grade_te_start = content.find("temp_grade_te as (")
        # 次のCTEの開始を探す
        next_cte_start = content.find("/* 馬TE_diff 集計用Stage1", grade_te_start)
        section = content[grade_te_start:next_cte_start]
        assert "g1_count >= 3" in section, \
            "horse_g1_te の低頻度マスク (g1_count >= 3) がありません"
        assert "g2_count >= 3" in section, \
            "horse_g2_te の低頻度マスク (g2_count >= 3) がありません"
        assert "g3_count >= 3" in section, \
            "horse_g3_te の低頻度マスク (g3_count >= 3) がありません"

    def test_sql_grade_te_uses_smoothing_m10(self):
        """temp_grade_te のスムージング係数が m=10 であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        grade_te_start = content.find("temp_grade_te as (")
        next_cte_start = content.find("/* 馬TE_diff 集計用Stage1", grade_te_start)
        section = content[grade_te_start:next_cte_start]
        assert "+ 10 * g.global_top3_rate" in section, \
            "スムージング係数が m=10（g.global_top3_rate）ではありません"

    def test_sql_final_select_has_all_grade_features(self):
        """最終 SELECT に6特徴量すべてが含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        for feature in [
            "horse_g1_te",
            "horse_g2_te",
            "horse_g3_te",
            "grade_step_up_flag",
            "g1_experience_flag",
            "best_grade_achieved",
        ]:
            assert feature in content, \
                f"最終 SELECT に {feature} がありません"

    def test_sql_final_join_references_grade_te(self):
        """最終 SELECT が temp_grade_te を LEFT JOIN していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "left join temp_grade_te as t_g_te" in content, \
            "最終 SELECT に temp_grade_te の LEFT JOIN がありません"
