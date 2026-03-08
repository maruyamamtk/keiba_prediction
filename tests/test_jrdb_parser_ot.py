"""OT（三連複基準オッズ）パーサーのテスト"""
import pytest
from itertools import combinations
from typing import Dict, Optional

from src.automation.data.jrdb_parser import JRDBParser


def _build_ot_line(
    race_key: str = "05261101",
    registered_count: int = 16,
    sanrenpuku_odds: Optional[Dict] = None,
) -> str:
    """
    OT行を構築するヘルパー関数。
    sanrenpuku_odds: {(h1, h2, h3): odds_value} のdict
    未指定の組み合わせは 9999.9 (取消) として埋める。
    """
    parts = [race_key, f"{registered_count:02d}"]
    all_triples = list(combinations(range(1, 19), 3))
    for h1, h2, h3 in all_triples:
        val = (sanrenpuku_odds or {}).get((h1, h2, h3), 9999.9)
        if val >= 9999.0:
            parts.append("9999.9")
        else:
            parts.append(f"{val:6.1f}")
    line = "".join(parts)
    # パディング (4912バイト相当)
    line = line.ljust(4910)
    line += "\r\n"
    return line


class TestParseOtLine:
    def test_basic_parse_returns_dict(self):
        line = _build_ot_line(sanrenpuku_odds={(1, 2, 3): 150.0})
        result = JRDBParser.parse_ot_line(line)
        assert result is not None
        assert result["race_id"] == "05261101"
        assert result["registered_count"] == 16

    def test_sanrenpuku_odds_count(self):
        line = _build_ot_line()
        result = JRDBParser.parse_ot_line(line)
        assert result is not None
        assert len(result["sanrenpuku_odds_list"]) == 816  # C(18,3)

    def test_specific_odds_value(self):
        line = _build_ot_line(sanrenpuku_odds={(1, 2, 3): 150.0})
        result = JRDBParser.parse_ot_line(line)
        assert result is not None
        found = [t for t in result["sanrenpuku_odds_list"] if t[0] == 1 and t[1] == 2 and t[2] == 3]
        assert len(found) == 1
        assert abs(found[0][3] - 150.0) < 0.1

    def test_cancelled_odds_is_none(self):
        """取消時 (9999.9) は None になること"""
        line = _build_ot_line(sanrenpuku_odds={(1, 2, 3): 9999.9})
        result = JRDBParser.parse_ot_line(line)
        assert result is not None
        found = [t for t in result["sanrenpuku_odds_list"] if t[0] == 1 and t[1] == 2 and t[2] == 3]
        assert found[0][3] is None

    def test_line_too_short_returns_none(self):
        result = JRDBParser.parse_ot_line("short")
        assert result is None

    def test_empty_race_key_returns_none(self):
        result = JRDBParser.parse_ot_line("        16" + "9999.9" * 816)
        assert result is None

    def test_combination_order(self):
        """組み合わせ順が (1,2,3), (1,2,4), ..., (16,17,18) であること"""
        line = _build_ot_line()
        result = JRDBParser.parse_ot_line(line)
        triples = [(t[0], t[1], t[2]) for t in result["sanrenpuku_odds_list"]]
        expected = list(combinations(range(1, 19), 3))
        assert triples == expected

    def test_created_at_present(self):
        line = _build_ot_line(sanrenpuku_odds={(1, 2, 3): 100.0})
        result = JRDBParser.parse_ot_line(line)
        assert result is not None
        assert "created_at" in result


class TestExpandOtToComboRows:
    def _parse_and_expand(self, sanrenpuku_odds: Optional[Dict]) -> list:
        line = _build_ot_line(sanrenpuku_odds=sanrenpuku_odds)
        record = JRDBParser.parse_ot_line(line)
        assert record is not None
        return JRDBParser.expand_ot_to_combo_rows(record, file_date="2026-01-31")

    def test_basic_expansion(self):
        rows = self._parse_and_expand({(1, 2, 3): 150.0, (1, 2, 4): 200.0})
        assert len(rows) == 2

    def test_bet_type_is_sanrenpuku(self):
        rows = self._parse_and_expand({(1, 2, 3): 100.0})
        assert rows[0]["bet_type"] == "sanrenpuku"

    def test_schema_fields(self):
        rows = self._parse_and_expand({(1, 2, 3): 100.0})
        row = rows[0]
        assert "race_id" in row
        assert "race_date" in row
        assert "bet_type" in row
        assert "horse_number_1" in row
        assert "horse_number_2" in row
        assert "horse_number_3" in row
        assert "odds_value" in row
        assert "odds_timestamp" in row

    def test_horse_number_3_is_set(self):
        rows = self._parse_and_expand({(2, 5, 9): 300.0})
        assert rows[0]["horse_number_3"] == 9

    def test_cancelled_odds_skipped(self):
        """9999.9 (取消) はスキップされること"""
        rows = self._parse_and_expand({(1, 2, 3): 9999.9})
        assert len(rows) == 0

    def test_all_cancelled_returns_empty(self):
        rows = self._parse_and_expand({})  # 全部9999.9
        assert rows == []

    def test_odds_timestamp_format(self):
        rows = self._parse_and_expand({(1, 2, 3): 100.0})
        assert "2026-01-31T19:00:00" in rows[0]["odds_timestamp"]

    def test_horse_numbers_correct(self):
        rows = self._parse_and_expand({(3, 7, 12): 500.0})
        assert rows[0]["horse_number_1"] == 3
        assert rows[0]["horse_number_2"] == 7
        assert rows[0]["horse_number_3"] == 12

    def test_race_id_propagated(self):
        line = _build_ot_line(race_key="09261205", sanrenpuku_odds={(1, 2, 3): 100.0})
        record = JRDBParser.parse_ot_line(line)
        assert record is not None
        rows = JRDBParser.expand_ot_to_combo_rows(record, file_date="2026-01-31")
        assert all(r["race_id"] == "09261205" for r in rows)
