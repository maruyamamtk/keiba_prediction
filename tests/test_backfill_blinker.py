"""
scripts/backfill_blinker.py のユニットテスト（Issue #441）

部分的な KYF（メインレースのみ）しかない日でも、KYG から残りのレースを補い、
どのソースにもないレースを未網羅として返すことを検証する。
"""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from scripts.backfill_blinker import collect_day_rows  # noqa: E402
from tests.test_jrdb_parser import _KYF_LINE_REAL, _with_blinker  # noqa: E402


def _line(race_id: str, horse_number: int, blinker: str) -> str:
    return race_id + f"{horse_number:02d}" + _with_blinker(_KYF_LINE_REAL, blinker)[10:]


class TestCollectDayRows:
    def test_partial_kyf_is_completed_by_kyg(self):
        """KYF にないレースは次のソース（KYG）から補われ、先に見つかったレースは上書きされないこと"""
        kyf = _line("06260111", 1, "1")
        kyg = "\n".join([_line("06260111", 1, "3"), _line("06260101", 1, "2"), _line("06260101", 2, " ")])
        sources = [(kyf, "KYF", "kyf"), (kyg, "KYG", "kyg")]

        rows, remaining = collect_day_rows(sources, {"06260111", "06260101"})

        assert remaining == set()
        assert sorted((r["race_id"], r["horse_number"], r["blinker"]) for r in rows) == [
            ("06260101", 1, "2"),
            ("06260101", 2, None),
            ("06260111", 1, "1"),
        ]

    def test_missing_race_is_reported(self):
        """どのソースにもないレースが未網羅として返ること"""
        rows, remaining = collect_day_rows([(_line("06260101", 1, " "), "KYF", "kyf")], {"06260101", "06260102"})

        assert len(rows) == 1
        assert remaining == {"06260102"}
