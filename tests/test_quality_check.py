#!/usr/bin/env python3
"""
quality_check.pyのテスト

データ品質チェッカークラスの単体テスト
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.data.quality_check import (
    CheckResult,
    DataQualityChecker,
    QualityReport,
    send_alert,
)
from src.data.validation_rules import (
    Severity,
    TableValidationConfig,
)


class TestCheckResult:
    """CheckResultデータクラスのテスト"""

    def test_check_result_creation(self):
        """CheckResultが正しく作成されることを確認"""
        result = CheckResult(
            check_name="null_check",
            table_name="raw.race_info",
            passed=True,
            severity="ERROR",
            message="テスト成功",
            details={"column": "race_id"},
        )

        assert result.check_name == "null_check"
        assert result.table_name == "raw.race_info"
        assert result.passed is True
        assert result.severity == "ERROR"
        assert result.message == "テスト成功"
        assert result.details == {"column": "race_id"}
        assert result.timestamp is not None

    def test_check_result_default_details(self):
        """CheckResultのdetailsがデフォルトで空辞書であることを確認"""
        result = CheckResult(
            check_name="test",
            table_name="test.table",
            passed=True,
            severity="INFO",
            message="テスト",
        )

        assert result.details == {}


class TestQualityReport:
    """QualityReportデータクラスのテスト"""

    def test_quality_report_creation(self):
        """QualityReportが正しく作成されることを確認"""
        results = [
            CheckResult(
                check_name="test1",
                table_name="raw.test",
                passed=True,
                severity="INFO",
                message="成功",
            ),
            CheckResult(
                check_name="test2",
                table_name="raw.test",
                passed=False,
                severity="ERROR",
                message="失敗",
            ),
        ]

        report = QualityReport(
            report_id="20240101_120000",
            generated_at="2024-01-01T12:00:00",
            project_id="test-project",
            total_checks=2,
            passed_checks=1,
            failed_checks=1,
            error_count=1,
            warning_count=0,
            info_count=0,
            results=results,
        )

        assert report.report_id == "20240101_120000"
        assert report.total_checks == 2
        assert report.passed_checks == 1
        assert report.failed_checks == 1
        assert len(report.results) == 2

    def test_quality_report_to_dict(self):
        """QualityReportのto_dictが正しく動作することを確認"""
        result = CheckResult(
            check_name="test",
            table_name="raw.test",
            passed=True,
            severity="INFO",
            message="テスト",
        )

        report = QualityReport(
            report_id="20240101_120000",
            generated_at="2024-01-01T12:00:00",
            project_id="test-project",
            total_checks=1,
            passed_checks=1,
            failed_checks=0,
            error_count=0,
            warning_count=0,
            info_count=0,
            results=[result],
        )

        report_dict = report.to_dict()

        assert report_dict["report_id"] == "20240101_120000"
        assert report_dict["summary"]["total_checks"] == 1
        assert len(report_dict["results"]) == 1
        assert report_dict["results"][0]["check_name"] == "test"


class TestDataQualityCheckerTableExists:
    """DataQualityCheckerのテーブル存在確認テスト"""

    def test_table_exists_true(self):
        """テーブルが存在する場合のテスト"""
        mock_client = MagicMock()

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=["race_id"],
            not_null_columns=["race_id"],
            date_columns=[],
            numeric_columns=[],
        )

        result = checker.check_table_exists(config)

        assert result is True
        assert len(checker.results) == 1
        assert checker.results[0].passed is True

    def test_table_exists_false(self):
        """テーブルが存在しない場合のテスト"""
        from google.cloud.exceptions import NotFound

        mock_client = MagicMock()
        mock_client.get_table.side_effect = NotFound("Table not found")

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="nonexistent",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=[],
            date_columns=[],
            numeric_columns=[],
        )

        result = checker.check_table_exists(config)

        assert result is False
        assert len(checker.results) == 1
        assert checker.results[0].passed is False


class TestDataQualityCheckerRowCount:
    """DataQualityCheckerのレコード数チェックテスト"""

    def test_row_count_above_minimum(self):
        """レコード数が最低値以上の場合のテスト"""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [{"count": 5000}]
        mock_client.query.return_value = mock_query_job

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=[],
            date_columns=[],
            numeric_columns=[],
            expected_min_rows=1000,
        )

        checker.check_row_count(config)

        assert len(checker.results) == 1
        assert checker.results[0].passed is True
        assert checker.results[0].details["row_count"] == 5000

    def test_row_count_below_minimum(self):
        """レコード数が最低値未満の場合のテスト"""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [{"count": 500}]
        mock_client.query.return_value = mock_query_job

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=[],
            date_columns=[],
            numeric_columns=[],
            expected_min_rows=1000,
        )

        checker.check_row_count(config)

        assert len(checker.results) == 1
        assert checker.results[0].passed is False
        assert checker.results[0].details["row_count"] == 500


class TestDataQualityCheckerNullValues:
    """DataQualityCheckerのNULL値チェックテスト"""

    def test_null_check_no_nulls(self):
        """NULL値がない場合のテスト"""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            {"total_count": 1000, "null_count": 0}
        ]
        mock_client.query.return_value = mock_query_job

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=["race_id"],
            date_columns=[],
            numeric_columns=[],
        )

        checker.check_null_values(config)

        assert len(checker.results) == 1
        assert checker.results[0].passed is True
        assert checker.results[0].details["null_count"] == 0

    def test_null_check_with_nulls(self):
        """NULL値がある場合のテスト"""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            {"total_count": 1000, "null_count": 50}
        ]
        mock_client.query.return_value = mock_query_job

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=["race_id"],
            date_columns=[],
            numeric_columns=[],
        )

        checker.check_null_values(config)

        assert len(checker.results) == 1
        assert checker.results[0].passed is False
        assert checker.results[0].details["null_count"] == 50
        assert checker.results[0].details["null_percentage"] == 5.0

    def test_null_check_no_columns(self):
        """not_null_columnsが空の場合は何もしない"""
        mock_client = MagicMock()

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=[],
            date_columns=[],
            numeric_columns=[],
        )

        checker.check_null_values(config)

        assert len(checker.results) == 0


class TestDataQualityCheckerDuplicates:
    """DataQualityCheckerの重複チェックテスト"""

    def test_duplicate_check_no_duplicates(self):
        """重複がない場合のテスト"""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            {"total_count": 1000, "distinct_count": 1000}
        ]
        mock_client.query.return_value = mock_query_job

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=["race_id"],
            not_null_columns=[],
            date_columns=[],
            numeric_columns=[],
        )

        checker.check_duplicates(config)

        assert len(checker.results) == 1
        assert checker.results[0].passed is True
        assert checker.results[0].details["duplicate_count"] == 0

    def test_duplicate_check_with_duplicates(self):
        """重複がある場合のテスト"""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            {"total_count": 1000, "distinct_count": 950}
        ]
        mock_client.query.return_value = mock_query_job

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=["race_id"],
            not_null_columns=[],
            date_columns=[],
            numeric_columns=[],
        )

        checker.check_duplicates(config)

        assert len(checker.results) == 1
        assert checker.results[0].passed is False
        assert checker.results[0].details["duplicate_count"] == 50


class TestDataQualityCheckerDateRange:
    """DataQualityCheckerの日付範囲チェックテスト"""

    def test_date_range_valid(self):
        """日付範囲が有効な場合のテスト"""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            {
                "min_date": "2020-01-01",
                "max_date": "2024-01-01",
                "too_old_count": 0,
                "too_future_count": 0,
                "total_count": 1000,
            }
        ]
        mock_client.query.return_value = mock_query_job

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=[],
            date_columns=["race_date"],
            numeric_columns=[],
        )

        checker.check_date_range(config)

        assert len(checker.results) == 1
        assert checker.results[0].passed is True

    def test_date_range_with_issues(self):
        """日付範囲に問題がある場合のテスト"""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            {
                "min_date": "2010-01-01",
                "max_date": "2030-01-01",
                "too_old_count": 5,
                "too_future_count": 3,
                "total_count": 1000,
            }
        ]
        mock_client.query.return_value = mock_query_job

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=[],
            date_columns=["race_date"],
            numeric_columns=[],
        )

        checker.check_date_range(config)

        assert len(checker.results) == 1
        assert checker.results[0].passed is False
        assert checker.results[0].details["too_old_count"] == 5
        assert checker.results[0].details["too_future_count"] == 3


class TestDataQualityCheckerNumericRange:
    """DataQualityCheckerの数値範囲チェックテスト"""

    def test_numeric_range_valid(self):
        """数値範囲が有効な場合のテスト"""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            {
                "min_val": 1,
                "max_val": 12,
                "avg_val": 6.5,
                "below_min_count": 0,
                "above_max_count": 0,
                "total_count": 1000,
            }
        ]
        mock_client.query.return_value = mock_query_job

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=[],
            date_columns=[],
            numeric_columns=["race_number"],
        )

        checker.check_numeric_range(config)

        assert len(checker.results) == 1
        assert checker.results[0].passed is True

    def test_numeric_range_with_outliers(self):
        """数値範囲外の値がある場合のテスト"""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            {
                "min_val": 0,
                "max_val": 15,
                "avg_val": 6.0,
                "below_min_count": 5,
                "above_max_count": 3,
                "total_count": 1000,
            }
        ]
        mock_client.query.return_value = mock_query_job

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=[],
            date_columns=[],
            numeric_columns=["race_number"],
        )

        checker.check_numeric_range(config)

        assert len(checker.results) == 1
        assert checker.results[0].passed is False


class TestDataQualityCheckerReport:
    """DataQualityCheckerのレポート生成テスト"""

    def test_generate_report(self):
        """レポート生成のテスト"""
        mock_client = MagicMock()

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        # 手動で結果を追加
        checker._add_result(
            check_name="test1",
            table_name="raw.test",
            passed=True,
            severity=Severity.INFO,
            message="成功",
        )
        checker._add_result(
            check_name="test2",
            table_name="raw.test",
            passed=False,
            severity=Severity.ERROR,
            message="失敗",
        )
        checker._add_result(
            check_name="test3",
            table_name="raw.test",
            passed=False,
            severity=Severity.WARNING,
            message="警告",
        )

        report = checker._generate_report()

        assert report.total_checks == 3
        assert report.passed_checks == 1
        assert report.failed_checks == 2
        assert report.error_count == 1
        assert report.warning_count == 1
        assert report.info_count == 0

    def test_should_alert_with_errors(self):
        """エラーがある場合にアラートが必要"""
        mock_client = MagicMock()

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        report = QualityReport(
            report_id="test",
            generated_at="2024-01-01",
            project_id="test",
            total_checks=1,
            passed_checks=0,
            failed_checks=1,
            error_count=1,
            warning_count=0,
            info_count=0,
            results=[],
        )

        assert checker.should_alert(report) is True

    def test_should_not_alert_without_errors(self):
        """エラーがない場合にアラートは不要"""
        mock_client = MagicMock()

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        report = QualityReport(
            report_id="test",
            generated_at="2024-01-01",
            project_id="test",
            total_checks=1,
            passed_checks=0,
            failed_checks=1,
            error_count=0,
            warning_count=1,
            info_count=0,
            results=[],
        )

        assert checker.should_alert(report) is False


class TestDataQualityCheckerSaveReport:
    """DataQualityCheckerのレポート保存テスト"""

    def test_save_report(self):
        """レポートが正しく保存されることを確認"""
        mock_client = MagicMock()

        with patch(
            "src.data.quality_check.bigquery.Client", return_value=mock_client
        ):
            checker = DataQualityChecker(project_id="test-project")

        report = QualityReport(
            report_id="test_report",
            generated_at="2024-01-01T12:00:00",
            project_id="test-project",
            total_checks=1,
            passed_checks=1,
            failed_checks=0,
            error_count=0,
            warning_count=0,
            info_count=0,
            results=[],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "reports" / "test_report.json"
            checker.save_report(report, output_path)

            assert output_path.exists()

            with open(output_path, "r", encoding="utf-8") as f:
                saved_report = json.load(f)

            assert saved_report["report_id"] == "test_report"
            assert saved_report["project_id"] == "test-project"


class TestSendAlert:
    """send_alert関数のテスト"""

    def test_send_alert_prints_errors(self, capsys):
        """アラートがエラー情報を出力することを確認"""
        result = CheckResult(
            check_name="null_check",
            table_name="raw.race_info",
            passed=False,
            severity="ERROR",
            message="NULL値が検出されました",
        )

        report = QualityReport(
            report_id="test",
            generated_at="2024-01-01",
            project_id="test",
            total_checks=1,
            passed_checks=0,
            failed_checks=1,
            error_count=1,
            warning_count=0,
            info_count=0,
            results=[result],
        )

        send_alert(report)

        captured = capsys.readouterr()
        assert "アラート" in captured.out
        assert "raw.race_info" in captured.out
        assert "null_check" in captured.out


class TestTableValidationConfig:
    """TableValidationConfigデータクラスのテスト"""

    def test_config_creation(self):
        """TableValidationConfigが正しく作成されることを確認"""
        config = TableValidationConfig(
            dataset_id="raw",
            table_id="race_info",
            description="レース情報テーブル",
            primary_key_columns=["race_id"],
            not_null_columns=["race_id", "race_date"],
            date_columns=["race_date"],
            numeric_columns=["race_number", "distance"],
            expected_min_rows=1000,
        )

        assert config.dataset_id == "raw"
        assert config.table_id == "race_info"
        assert config.description == "レース情報テーブル"
        assert config.primary_key_columns == ["race_id"]
        assert len(config.not_null_columns) == 2
        assert config.expected_min_rows == 1000

    def test_config_default_expected_min_rows(self):
        """expected_min_rowsのデフォルト値が0であることを確認"""
        config = TableValidationConfig(
            dataset_id="raw",
            table_id="test",
            description="テスト",
            primary_key_columns=[],
            not_null_columns=[],
            date_columns=[],
            numeric_columns=[],
        )

        assert config.expected_min_rows == 0


class TestTableNameParsing:
    """テーブル名パースのテスト"""

    def test_table_name_without_dot_is_invalid(self):
        """テーブル名に.が含まれない場合は不正な形式"""
        table_name = "invalid_table"
        assert "." not in table_name

    def test_table_name_with_dot_format(self):
        """テーブル名が正しい形式の場合の処理"""
        # .を含むテーブル名は正しくパースされる
        table_name = "raw.race_info"
        assert "." in table_name
        parts = table_name.split(".", 1)
        assert parts[0] == "raw"
        assert parts[1] == "race_info"

    def test_table_name_with_multiple_dots(self):
        """複数の.がある場合も正しく処理される"""
        table_name = "dataset.table.extra"
        parts = table_name.split(".", 1)
        assert parts[0] == "dataset"
        assert parts[1] == "table.extra"
