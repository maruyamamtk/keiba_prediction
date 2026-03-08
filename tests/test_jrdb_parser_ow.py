"""OW（ワイド基準オッズ）パーサーのテスト"""
import pytest
from itertools import combinations
from typing import Dict, Optional

from src.automation.data.jrdb_parser import JRDBParser


def _build_ow_line(
    race_key: str = "05261101",
    registered_count: int = 16,
    wide_odds: Optional[Dict] = None,
) -> str:
    """
    OW行を構築するヘルパー関数。
    wide_odds: {(h1, h2): odds_value} のdict (未指定の組み合わせは空白=未発売)
    """
    parts = [race_key, f"{registered_count:02d}"]
    all_pairs = list(combinations(range(1, 19), 2))
    for h1, h2 in all_pairs:
        val = (wide_odds or {}).get((h1, h2), 0.0)
        if val is not None and val > 0:
            parts.append(f"{val:5.1f}")
        else:
            parts.append("     ")  # 0は空白(未発売)
    line = "".join(parts)
    # パディング (780バイト相当)
    line = line.ljust(778)
    line += "\r\n"
    return line


class TestParseOwLine:
    def test_basic_parse_returns_dict(self):
        line = _build_ow_line(wide_odds={(1, 2): 28.2, (1, 3): 39.6})
        result = JRDBParser.parse_ow_line(line)
        assert result is not None
        assert result["race_id"] == "05261101"
        assert result["registered_count"] == 16

    def test_wide_odds_count(self):
        line = _build_ow_line()
        result = JRDBParser.parse_ow_line(line)
        assert result is not None
        assert len(result["wide_odds_list"]) == 153  # C(18,2)

    def test_specific_odds_value(self):
        line = _build_ow_line(wide_odds={(1, 2): 28.2})
        result = JRDBParser.parse_ow_line(line)
        assert result is not None
        found = [(h1, h2, v) for h1, h2, v in result["wide_odds_list"] if h1 == 1 and h2 == 2]
        assert len(found) == 1
        assert abs(found[0][2] - 28.2) < 0.05

    def test_zero_odds_parsed_as_none_or_zero(self):
        line = _build_ow_line(wide_odds={(5, 6): 0.0})
        result = JRDBParser.parse_ow_line(line)
        assert result is not None
        found = [(h1, h2, v) for h1, h2, v in result["wide_odds_list"] if h1 == 5 and h2 == 6]
        assert found[0][2] is None or found[0][2] == 0.0

    def test_line_too_short_returns_none(self):
        result = JRDBParser.parse_ow_line("12345")
        assert result is None

    def test_empty_race_key_returns_none(self):
        result = JRDBParser.parse_ow_line("        16" + "     " * 153)
        assert result is None

    def test_combination_order(self):
        """組み合わせ順が (1,2), (1,3), ..., (17,18) であること"""
        line = _build_ow_line()
        result = JRDBParser.parse_ow_line(line)
        pairs = [(h1, h2) for h1, h2, _ in result["wide_odds_list"]]
        expected = list(combinations(range(1, 19), 2))
        assert pairs == expected

    def test_created_at_present(self):
        line = _build_ow_line(wide_odds={(1, 2): 10.0})
        result = JRDBParser.parse_ow_line(line)
        assert result is not None
        assert "created_at" in result


class TestExpandOwToComboRows:
    def _parse_and_expand(self, wide_odds: Optional[Dict]) -> list:
        line = _build_ow_line(wide_odds=wide_odds)
        record = JRDBParser.parse_ow_line(line)
        assert record is not None
        return JRDBParser.expand_ow_to_combo_rows(record, file_date="2026-01-31")

    def test_basic_expansion(self):
        rows = self._parse_and_expand({(1, 2): 28.2, (1, 3): 39.6})
        assert len(rows) == 2

    def test_bet_type_is_wide(self):
        rows = self._parse_and_expand({(1, 2): 10.0})
        assert rows[0]["bet_type"] == "wide"

    def test_schema_fields(self):
        rows = self._parse_and_expand({(2, 5): 15.3})
        row = rows[0]
        assert "race_id" in row
        assert "race_date" in row
        assert "bet_type" in row
        assert "horse_number_1" in row
        assert "horse_number_2" in row
        assert "horse_number_3" in row
        assert "odds_value" in row
        assert "odds_timestamp" in row

    def test_horse_number_3_is_none(self):
        rows = self._parse_and_expand({(1, 2): 10.0})
        assert rows[0]["horse_number_3"] is None

    def test_zero_odds_skipped(self):
        rows = self._parse_and_expand({(1, 2): 0.0})
        assert len(rows) == 0

    def test_empty_odds_returns_empty_list(self):
        rows = self._parse_and_expand({})
        assert rows == []

    def test_odds_timestamp_format(self):
        rows = self._parse_and_expand({(1, 2): 10.0})
        assert "2026-01-31T19:00:00" in rows[0]["odds_timestamp"]

    def test_horse_number_values(self):
        rows = self._parse_and_expand({(3, 7): 20.5})
        assert rows[0]["horse_number_1"] == 3
        assert rows[0]["horse_number_2"] == 7

    def test_race_id_propagated(self):
        line = _build_ow_line(race_key="09261205", wide_odds={(1, 2): 10.0})
        record = JRDBParser.parse_ow_line(line)
        assert record is not None
        rows = JRDBParser.expand_ow_to_combo_rows(record, file_date="2026-01-31")
        assert all(r["race_id"] == "09261205" for r in rows)
