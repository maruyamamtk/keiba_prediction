"""
scripts/backfill_horse_weight_diff.py のユニットテスト（Issue #450）

SEC から対象期間の horse_weight_diff が修正後の値で抽出され、UPDATE/突合SQLが horse_weight_diff だけを扱うことを検証する。
"""

import sys
from datetime import date
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from scripts.backfill_horse_weight_diff import build_update_sql, build_verify_sql, extract_rows  # noqa: E402
from tests.test_jrdb_parser import _SEC_LINE_1000M, _SEC_LINE_1800M  # noqa: E402


class TestExtractRows:
    def test_negative_single_digit_is_extracted(self):
        """「- 2」「- 4」の実データ行が負の値で抽出され、キー列と対象列だけを持つこと"""
        rows = extract_rows(_SEC_LINE_1800M + "\n" + _SEC_LINE_1000M, date(2017, 1, 1), date(2025, 12, 31))

        assert rows == [
            {"race_id": "08251301", "horse_number": 1, "horse_weight_diff": -2},
            {"race_id": "01171301", "horse_number": 1, "horse_weight_diff": -4},
        ]

    def test_out_of_period_rows_are_skipped(self):
        """対象期間外のレースは抽出されないこと"""
        rows = extract_rows(_SEC_LINE_1800M + "\n" + _SEC_LINE_1000M, date(2025, 1, 1), date(2025, 12, 31))

        assert [r["race_id"] for r in rows] == ["08251301"]


class TestSql:
    def test_update_sets_only_target_column(self):
        sql = build_update_sql("p.raw.race_results", "p.raw._staging")
        set_clause = sql.split("SET", 1)[1].split("FROM", 1)[0]
        assert set_clause.strip() == "horse_weight_diff = s.horse_weight_diff"
        assert "race_date BETWEEN @start_date AND @end_date" in sql

    def test_verify_excludes_target_and_checks_overwrite(self):
        sql = build_verify_sql("p.raw.race_results", "p.raw.bak")
        assert sql.count("EXCEPT(horse_weight_diff)") == 2
        assert "AS n_overwritten" in sql
        assert "BETWEEN -9 AND -1" in sql
