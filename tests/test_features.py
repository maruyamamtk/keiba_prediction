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
        """jockey_count / trainer_count / sire_count が 1 preceding ウィンドウを使用すること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # jockey_count の定義部分を確認
        jockey_pre_start = content.find("temp_jockey_te_pre")
        jockey_pre_end = content.find("temp_jockey_te as (")
        jockey_pre_section = content[jockey_pre_start:jockey_pre_end]
        assert "jockey_count" in jockey_pre_section
        assert "range between unbounded preceding and 1 preceding" in jockey_pre_section

        trainer_pre_start = content.find("temp_trainer_te_pre")
        trainer_pre_end = content.find("temp_trainer_te as (")
        trainer_pre_section = content[trainer_pre_start:trainer_pre_end]
        assert "trainer_count" in trainer_pre_section
        assert "range between unbounded preceding and 1 preceding" in trainer_pre_section

        sire_pre_start = content.find("temp_sire_te_pre")
        sire_pre_end = content.find("temp_sire_te as (")
        sire_pre_section = content[sire_pre_start:sire_pre_end]
        assert "sire_count" in sire_pre_section
        assert "range between unbounded preceding and 1 preceding" in sire_pre_section

    def test_sql_mask_wrapper_applies_if_condition(self):
        """マスクラッパーCTEが IF(count >= 20, ..., NULL) を適用していること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        # 騎手マスク
        jockey_wrapper_start = content.find("temp_jockey_te as (")
        jockey_wrapper_end = content.find("temp_trainer_te_pre")
        jockey_wrapper = content[jockey_wrapper_start:jockey_wrapper_end]
        assert "IF(jockey_count >= 20, jockey_te, NULL)" in jockey_wrapper
        assert "IF(jockey_count >= 20, jockey_course_type_te, NULL)" in jockey_wrapper

        # 調教師マスク
        trainer_wrapper_start = content.find("temp_trainer_te as (")
        trainer_wrapper_end = content.find("temp_sire_te_pre")
        trainer_wrapper = content[trainer_wrapper_start:trainer_wrapper_end]
        assert "IF(trainer_count >= 20, trainer_te, NULL)" in trainer_wrapper

        # 種牡馬マスク
        sire_wrapper_start = content.find("temp_sire_te as (")
        sire_wrapper_end = content.find("temp_horse_distance_base")
        sire_wrapper = content[sire_wrapper_start:sire_wrapper_end]
        assert "IF(sire_count >= 20, sire_te, NULL)" in sire_wrapper

    def test_sql_mask_threshold_is_20(self):
        """マスク閾値が 20 であること（変更時に気付けるよう）"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert "IF(jockey_count >= 20," in content
        assert "IF(trainer_count >= 20," in content
        assert "IF(sire_count >= 20," in content

    def test_sql_all_9_jockey_te_columns_masked(self):
        """9つの騎手TE基本特徴量がすべてマスク対象であること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        jockey_wrapper_start = content.find("temp_jockey_te as (")
        jockey_wrapper_end = content.find("temp_trainer_te_pre")
        jockey_wrapper = content[jockey_wrapper_start:jockey_wrapper_end]
        expected_columns = [
            "jockey_te",
            "jockey_course_type_te",
            "jockey_venue_te",
            "jockey_distance_band_te",
            "jockey_distance_te",
            "jockey_direction_te",
            "jockey_course_type_venue_te",
            "jockey_course_type_distance_te",
            "jockey_course_type_distance_venue_te",
        ]
        for col in expected_columns:
            assert f"IF(jockey_count >= 20, {col}, NULL)" in jockey_wrapper, (
                f"騎手TE '{col}' のマスク条件が見つかりません"
            )

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
        """temp_base_race_entries と temp_horse_master_feature の両方に除外フィルタが含まれること"""
        content = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
        assert content.count("race_class != 'A1'") == 2, "新馬戦除外が2箇所にあるべき"
        assert content.count("!= 'obstacle'") == 2, "障害戦除外が2箇所にあるべき"
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
        final_select_start = content.rfind("select")
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
        final_select_start = content.rfind("select")
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
        final_select_start = content.rfind("select")
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
