"""
scripts/backfill_kyf_columns.py のユニットテスト（Issue #452）

対象列が修正後パーサーの値で抽出され、UPDATE/突合SQLが対象列だけを扱うことを検証する。
"""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from scripts.backfill_kyf_columns import (  # noqa: E402
    TARGET_COLUMNS,
    build_update_sql,
    build_verify_sql,
    collect_day_rows,
)
from src.automation.data.jrdb_parser import JRDBParser  # noqa: E402
from tests.test_jrdb_parser import _KYF_LINE_REAL, _KYF_LINE_REAL_2017  # noqa: E402


def _line(race_id: str, horse_number: int, base: str = _KYF_LINE_REAL) -> str:
    return race_id + f"{horse_number:02d}" + base[10:]


class TestTargetColumns:
    def test_columns_are_parser_outputs(self):
        """対象列がすべて parse_kyf_line の出力に含まれ、重複がないこと"""
        result = JRDBParser.parse_kyf_line(_KYF_LINE_REAL)
        assert set(TARGET_COLUMNS) <= set(result)
        assert len(set(TARGET_COLUMNS)) == len(TARGET_COLUMNS)

    def test_unchanged_fields_excluded(self):
        """位置を変えていない列（騎手名・コード・ブリンカー等）は対象外であること"""
        for col in ("race_id", "horse_number", "idm", "blinker", "jockey_name", "jockey_code", "prev_race_key_1"):
            assert col not in TARGET_COLUMNS


class TestCollectDayRows:
    def test_rows_have_fixed_values(self):
        """抽出行がキー列 + 対象列だけを持ち、修正後の位置の値であること"""
        content = "\n".join([_line("06251501", 1), _line("06251501", 10, _KYF_LINE_REAL_2017)])
        rows, remaining = collect_day_rows([(content, "KYF", "kyf")], {"06251501"})

        assert remaining == set()
        assert set(rows[0]) == {"race_id", "horse_number", *TARGET_COLUMNS}
        assert [(r["horse_number"], r["bracket_number"], r["sex_code"]) for r in rows] == [(1, 1, 2), (10, 6, 1)]

    def test_partial_kyf_is_completed_by_kyg(self):
        """KYF にないレースは次のソース（KYG）から補われ、どこにもないレースは未網羅として返ること"""
        sources = [(_line("06251511", 1), "KYF", "kyf"), (_line("06251501", 1), "KYG", "kyg")]
        rows, remaining = collect_day_rows(sources, {"06251511", "06251501", "06251502"})

        assert sorted(r["race_id"] for r in rows) == ["06251501", "06251511"]
        assert remaining == {"06251502"}


class TestSql:
    def test_update_sets_only_target_columns(self):
        sql = build_update_sql("p.raw.horse_results", "p.raw._staging")
        set_clause = sql.split("SET", 1)[1].split("FROM", 1)[0]
        assert [a.split("=")[0].strip() for a in set_clause.split(",")] == list(TARGET_COLUMNS)

    def test_verify_excludes_target_columns_and_counts_each(self):
        sql = build_verify_sql("p.raw.horse_results", "p.raw.bak")
        assert f"EXCEPT({', '.join(TARGET_COLUMNS)})" in sql
        for col in TARGET_COLUMNS:
            assert f"AS changed_{col}" in sql
