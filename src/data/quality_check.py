#!/usr/bin/env python3
"""
BigQueryデータ品質チェックスクリプト

Issue #8: データ品質チェックスクリプトの実装

機能:
- NULL値のチェック
- 重複レコードのチェック
- 日付範囲の整合性チェック
- 数値範囲の検証
- レコード数の確認
- レポート生成
- 異常検知時のアラート
"""

import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.data.validation_rules import (
    DATE_RANGE_CONFIG,
    NUMERIC_RANGE_CONFIG,
    TABLE_VALIDATION_CONFIGS,
    Severity,
    TableValidationConfig,
)


@dataclass
class CheckResult:
    """チェック結果を格納するデータクラス"""

    check_name: str
    table_name: str
    passed: bool
    severity: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class QualityReport:
    """品質チェックレポートを格納するデータクラス"""

    report_id: str
    generated_at: str
    project_id: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    error_count: int
    warning_count: int
    info_count: int
    results: List[CheckResult]

    def to_dict(self) -> Dict[str, Any]:
        """辞書形式に変換"""
        return {
            "report_id": self.report_id,
            "generated_at": self.generated_at,
            "project_id": self.project_id,
            "summary": {
                "total_checks": self.total_checks,
                "passed_checks": self.passed_checks,
                "failed_checks": self.failed_checks,
                "error_count": self.error_count,
                "warning_count": self.warning_count,
                "info_count": self.info_count,
            },
            "results": [asdict(r) for r in self.results],
        }


class DataQualityChecker:
    """データ品質チェッカークラス"""

    def __init__(self, project_id: str):
        """
        初期化

        Args:
            project_id: GCPプロジェクトID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.results: List[CheckResult] = []

    def _table_exists(self, dataset_id: str, table_id: str) -> bool:
        """テーブルが存在するか確認"""
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def _run_query(self, query: str) -> List[Dict[str, Any]]:
        """クエリを実行して結果を返す"""
        query_job = self.client.query(query)
        results = query_job.result()
        return [dict(row) for row in results]

    def _add_result(
        self,
        check_name: str,
        table_name: str,
        passed: bool,
        severity: Severity,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """チェック結果を追加"""
        self.results.append(
            CheckResult(
                check_name=check_name,
                table_name=table_name,
                passed=passed,
                severity=severity.value,
                message=message,
                details=details or {},
            )
        )

    def check_table_exists(self, config: TableValidationConfig) -> bool:
        """テーブルの存在確認"""
        table_name = f"{config.dataset_id}.{config.table_id}"
        exists = self._table_exists(config.dataset_id, config.table_id)

        self._add_result(
            check_name="table_exists",
            table_name=table_name,
            passed=exists,
            severity=Severity.ERROR,
            message=f"テーブル {table_name} {'が存在します' if exists else 'が存在しません'}",
        )

        return exists

    def check_row_count(self, config: TableValidationConfig) -> None:
        """レコード数のチェック"""
        table_name = f"{config.dataset_id}.{config.table_id}"
        table_ref = f"{self.project_id}.{table_name}"

        query = f"SELECT COUNT(*) as count FROM `{table_ref}`"
        result = self._run_query(query)
        row_count = result[0]["count"] if result else 0

        passed = row_count >= config.expected_min_rows

        self._add_result(
            check_name="row_count_check",
            table_name=table_name,
            passed=passed,
            severity=Severity.WARNING,
            message=(
                f"レコード数: {row_count:,} "
                f"(最低期待値: {config.expected_min_rows:,})"
            ),
            details={"row_count": row_count, "expected_min": config.expected_min_rows},
        )

    def check_null_values(self, config: TableValidationConfig) -> None:
        """NULL値のチェック"""
        if not config.not_null_columns:
            return

        table_name = f"{config.dataset_id}.{config.table_id}"
        table_ref = f"{self.project_id}.{table_name}"

        for column in config.not_null_columns:
            query = f"""
            SELECT
                COUNT(*) as total_count,
                COUNTIF({column} IS NULL) as null_count
            FROM `{table_ref}`
            """
            result = self._run_query(query)

            if result:
                total_count = result[0]["total_count"]
                null_count = result[0]["null_count"]
                null_percentage = (
                    (null_count / total_count * 100) if total_count > 0 else 0
                )

                passed = null_count == 0

                self._add_result(
                    check_name="null_check",
                    table_name=table_name,
                    passed=passed,
                    severity=Severity.ERROR,
                    message=(
                        f"カラム '{column}': "
                        f"NULL件数 {null_count:,} / {total_count:,} "
                        f"({null_percentage:.2f}%)"
                    ),
                    details={
                        "column": column,
                        "null_count": null_count,
                        "total_count": total_count,
                        "null_percentage": null_percentage,
                    },
                )

    def check_duplicates(self, config: TableValidationConfig) -> None:
        """重複レコードのチェック"""
        if not config.primary_key_columns:
            return

        table_name = f"{config.dataset_id}.{config.table_id}"
        table_ref = f"{self.project_id}.{table_name}"

        key_columns = ", ".join(config.primary_key_columns)

        query = f"""
        SELECT
            COUNT(*) as total_count,
            COUNT(DISTINCT CONCAT({', '.join([f'CAST({col} AS STRING)' for col in config.primary_key_columns])})) as distinct_count
        FROM `{table_ref}`
        """
        result = self._run_query(query)

        if result:
            total_count = result[0]["total_count"]
            distinct_count = result[0]["distinct_count"]
            duplicate_count = total_count - distinct_count

            passed = duplicate_count == 0

            self._add_result(
                check_name="duplicate_check",
                table_name=table_name,
                passed=passed,
                severity=Severity.ERROR,
                message=(
                    f"主キー ({key_columns}): "
                    f"重複件数 {duplicate_count:,} / {total_count:,}"
                ),
                details={
                    "key_columns": config.primary_key_columns,
                    "duplicate_count": duplicate_count,
                    "total_count": total_count,
                },
            )

    def check_date_range(self, config: TableValidationConfig) -> None:
        """日付範囲のチェック"""
        if not config.date_columns:
            return

        table_name = f"{config.dataset_id}.{config.table_id}"
        table_ref = f"{self.project_id}.{table_name}"

        min_date = DATE_RANGE_CONFIG["min_date"]
        max_date = (
            datetime.now() + timedelta(days=DATE_RANGE_CONFIG["max_future_days"])
        ).strftime("%Y-%m-%d")

        for column in config.date_columns:
            query = f"""
            SELECT
                MIN({column}) as min_date,
                MAX({column}) as max_date,
                COUNTIF({column} < '{min_date}') as too_old_count,
                COUNTIF({column} > '{max_date}') as too_future_count,
                COUNT(*) as total_count
            FROM `{table_ref}`
            WHERE {column} IS NOT NULL
            """
            result = self._run_query(query)

            if result:
                data = result[0]
                issues = []

                if data["too_old_count"] > 0:
                    issues.append(f"古すぎる日付: {data['too_old_count']}件")

                if data["too_future_count"] > 0:
                    issues.append(f"未来すぎる日付: {data['too_future_count']}件")

                passed = len(issues) == 0

                self._add_result(
                    check_name="date_range_check",
                    table_name=table_name,
                    passed=passed,
                    severity=Severity.WARNING,
                    message=(
                        f"カラム '{column}': "
                        f"範囲 {data['min_date']} ~ {data['max_date']} "
                        f"{', '.join(issues) if issues else '正常'}"
                    ),
                    details={
                        "column": column,
                        "min_date": str(data["min_date"]),
                        "max_date": str(data["max_date"]),
                        "too_old_count": data["too_old_count"],
                        "too_future_count": data["too_future_count"],
                        "expected_min": min_date,
                        "expected_max": max_date,
                    },
                )

    def check_numeric_range(self, config: TableValidationConfig) -> None:
        """数値範囲のチェック"""
        if not config.numeric_columns:
            return

        table_name = f"{config.dataset_id}.{config.table_id}"
        table_ref = f"{self.project_id}.{table_name}"

        for column in config.numeric_columns:
            range_config = NUMERIC_RANGE_CONFIG.get(column)
            if not range_config:
                continue

            min_val = range_config["min"]
            max_val = range_config["max"]

            query = f"""
            SELECT
                MIN({column}) as min_val,
                MAX({column}) as max_val,
                AVG({column}) as avg_val,
                COUNTIF({column} < {min_val}) as below_min_count,
                COUNTIF({column} > {max_val}) as above_max_count,
                COUNT(*) as total_count
            FROM `{table_ref}`
            WHERE {column} IS NOT NULL
            """
            result = self._run_query(query)

            if result:
                data = result[0]
                issues = []

                if data["below_min_count"] > 0:
                    issues.append(f"最小値未満: {data['below_min_count']}件")

                if data["above_max_count"] > 0:
                    issues.append(f"最大値超過: {data['above_max_count']}件")

                passed = len(issues) == 0

                self._add_result(
                    check_name="numeric_range_check",
                    table_name=table_name,
                    passed=passed,
                    severity=Severity.INFO,
                    message=(
                        f"カラム '{column}': "
                        f"範囲 {data['min_val']} ~ {data['max_val']} "
                        f"(期待値: {min_val} ~ {max_val}) "
                        f"{', '.join(issues) if issues else '正常'}"
                    ),
                    details={
                        "column": column,
                        "min_val": data["min_val"],
                        "max_val": data["max_val"],
                        "avg_val": float(data["avg_val"]) if data["avg_val"] else None,
                        "expected_min": min_val,
                        "expected_max": max_val,
                        "below_min_count": data["below_min_count"],
                        "above_max_count": data["above_max_count"],
                    },
                )

    def run_all_checks(
        self, configs: Optional[List[TableValidationConfig]] = None
    ) -> QualityReport:
        """
        すべてのチェックを実行

        Args:
            configs: チェック対象のテーブル設定リスト。Noneの場合はデフォルト設定を使用。

        Returns:
            品質チェックレポート
        """
        self.results = []

        if configs is None:
            configs = TABLE_VALIDATION_CONFIGS

        print("=" * 60)
        print("データ品質チェックを開始します")
        print("=" * 60)

        for config in configs:
            table_name = f"{config.dataset_id}.{config.table_id}"
            print(f"\n[{config.description}] {table_name}")
            print("-" * 40)

            # テーブル存在確認
            if not self.check_table_exists(config):
                print(f"  テーブルが存在しないためスキップします")
                continue

            # 各種チェックを実行
            self.check_row_count(config)
            self.check_null_values(config)
            self.check_duplicates(config)
            self.check_date_range(config)
            self.check_numeric_range(config)

            # 結果の概要を表示
            table_results = [r for r in self.results if r.table_name == table_name]
            passed = sum(1 for r in table_results if r.passed)
            failed = len(table_results) - passed
            print(f"  チェック完了: {passed} passed, {failed} failed")

        # レポート生成
        report = self._generate_report()

        return report

    def _generate_report(self) -> QualityReport:
        """品質チェックレポートを生成"""
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r.passed)
        failed_checks = total_checks - passed_checks

        error_count = sum(
            1 for r in self.results if not r.passed and r.severity == "ERROR"
        )
        warning_count = sum(
            1 for r in self.results if not r.passed and r.severity == "WARNING"
        )
        info_count = sum(
            1 for r in self.results if not r.passed and r.severity == "INFO"
        )

        report = QualityReport(
            report_id=datetime.now().strftime("%Y%m%d_%H%M%S"),
            generated_at=datetime.now().isoformat(),
            project_id=self.project_id,
            total_checks=total_checks,
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            error_count=error_count,
            warning_count=warning_count,
            info_count=info_count,
            results=self.results,
        )

        return report

    def print_report(self, report: QualityReport) -> None:
        """レポートをコンソールに出力"""
        print("\n" + "=" * 60)
        print("データ品質チェックレポート")
        print("=" * 60)

        print(f"\nレポートID: {report.report_id}")
        print(f"生成日時: {report.generated_at}")
        print(f"プロジェクト: {report.project_id}")

        print("\n--- サマリー ---")
        print(f"総チェック数: {report.total_checks}")
        print(f"成功: {report.passed_checks}")
        print(f"失敗: {report.failed_checks}")
        print(f"  - ERROR: {report.error_count}")
        print(f"  - WARNING: {report.warning_count}")
        print(f"  - INFO: {report.info_count}")

        # 失敗したチェックの詳細を表示
        failed_results = [r for r in report.results if not r.passed]
        if failed_results:
            print("\n--- 失敗したチェック ---")
            for result in failed_results:
                severity_mark = {
                    "ERROR": "[ERROR]",
                    "WARNING": "[WARN]",
                    "INFO": "[INFO]",
                }.get(result.severity, "[?]")
                print(f"\n{severity_mark} {result.table_name}")
                print(f"  チェック: {result.check_name}")
                print(f"  詳細: {result.message}")

        # 全体のステータス
        print("\n" + "=" * 60)
        if report.error_count > 0:
            print("ステータス: FAILED (ERROR検出)")
        elif report.warning_count > 0:
            print("ステータス: WARNING")
        else:
            print("ステータス: PASSED")
        print("=" * 60)

    def save_report(self, report: QualityReport, output_path: Path) -> None:
        """レポートをJSONファイルに保存"""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)

        print(f"\nレポートを保存しました: {output_path}")

    def should_alert(self, report: QualityReport) -> bool:
        """アラートが必要かどうかを判定"""
        return report.error_count > 0


def send_alert(report: QualityReport) -> None:
    """
    アラートを送信

    将来的にはメール/LINE通知を実装予定。
    現在はコンソール出力のみ。
    """
    print("\n" + "!" * 60)
    print("アラート: データ品質に問題が検出されました")
    print("!" * 60)

    error_results = [
        r for r in report.results if not r.passed and r.severity == "ERROR"
    ]

    for result in error_results:
        print(f"\n[ERROR] {result.table_name}")
        print(f"  チェック: {result.check_name}")
        print(f"  詳細: {result.message}")


def main():
    """メイン処理"""
    import argparse

    parser = argparse.ArgumentParser(description="データ品質チェックスクリプト")
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="レポート出力先パス（JSON形式）",
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="特定のテーブルのみチェック（例: raw.race_info）",
    )
    parser.add_argument(
        "--no-alert",
        action="store_true",
        help="アラートを無効化",
    )
    args = parser.parse_args()

    # .envファイルを読み込み
    load_dotenv()

    # 環境変数からプロジェクトIDを取得
    project_id = os.getenv("GCP_PROJECT_ID")

    if not project_id:
        print("エラー: GCP_PROJECT_ID環境変数が設定されていません。")
        print(".envファイルを作成し、GCP_PROJECT_IDを設定してください。")
        sys.exit(1)

    print(f"GCPプロジェクトID: {project_id}\n")

    # チェッカーを作成
    checker = DataQualityChecker(project_id)

    # チェック対象を絞り込み
    configs = TABLE_VALIDATION_CONFIGS
    if args.table:
        dataset_id, table_id = args.table.split(".")
        configs = [
            c
            for c in configs
            if c.dataset_id == dataset_id and c.table_id == table_id
        ]
        if not configs:
            print(f"エラー: テーブル {args.table} の設定が見つかりません。")
            sys.exit(1)

    # チェック実行
    report = checker.run_all_checks(configs)

    # レポート出力
    checker.print_report(report)

    # レポート保存
    if args.output:
        output_path = Path(args.output)
    else:
        # デフォルトの出力先
        output_dir = Path(__file__).parent.parent.parent / "reports"
        output_path = output_dir / f"quality_report_{report.report_id}.json"

    checker.save_report(report, output_path)

    # アラート送信
    if not args.no_alert and checker.should_alert(report):
        send_alert(report)
        sys.exit(1)

    if report.error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
