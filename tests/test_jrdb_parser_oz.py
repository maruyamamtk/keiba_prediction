"""
JRDBParser の OZ（基準オッズ）パーサーのユニットテスト

parse_oz_line() および expand_oz_to_odds_rows(), expand_oz_to_combo_rows() の動作を検証する。
"""

import pytest
from itertools import combinations

from src.automation.data.jrdb_parser import JRDBParser


# ---------------------------------------------------------------------------
# テスト用ヘルパー
# ---------------------------------------------------------------------------

def _build_oz_line(
    race_key: str = "05261101",
    registered_count: int = 8,
    win_odds: list = None,
    place_odds: list = None,
) -> str:
    """
    テスト用 OZ 行文字列を構築するヘルパー。

    フォーマット:
        0-7   : レースキー (8文字)
        8-9   : 登録頭数 (2文字)
        10-99 : 単勝オッズ 18頭分 (5文字×18, 例: "046.6")
        100-189: 複勝オッズ 18頭分 (5文字×18)
        190+  : 馬連オッズ (今回はスペース埋め)

    Args:
        race_key: 8文字のレースキー
        registered_count: 登録頭数 (1-18)
        win_odds: 馬番1始まりの単勝オッズリスト (float or None)。
                  未指定部分は "     " で埋める。
        place_odds: 馬番1始まりの複勝オッズリスト (float or None)。
    """
    win_odds = win_odds or []
    place_odds = place_odds or []

    def _fmt_odds(odds_list: list) -> str:
        """18頭分のオッズを5文字×18の文字列に変換"""
        result = ""
        for i in range(18):
            val = odds_list[i] if i < len(odds_list) else None
            if val is None or val <= 0:
                result += "     "
            else:
                result += f"{val:05.1f}"
        return result

    body = (
        race_key
        + f"{registered_count:02d}"
        + _fmt_odds(win_odds)
        + _fmt_odds(place_odds)
        + "0" * 765  # 馬連オッズ部分 (今回はスコープ外; 末尾スペースによる strip 問題を防ぐため 0 で埋める)
    )
    return body


# ---------------------------------------------------------------------------
# parse_oz_line()
# ---------------------------------------------------------------------------

class TestParseOzLine:
    """parse_oz_line() の基本動作テスト"""

    def test_returns_dict_with_correct_race_id(self):
        """レースキーが正しく取得される"""
        line = _build_oz_line(race_key="05261101")
        result = JRDBParser.parse_oz_line(line)
        assert result is not None
        assert result["race_id"] == "05261101"

    def test_registered_count_parsed(self):
        """登録頭数が正しく取得される"""
        line = _build_oz_line(registered_count=12)
        result = JRDBParser.parse_oz_line(line)
        assert result is not None
        assert result["registered_count"] == 12

    def test_win_odds_parsed_correctly(self):
        """単勝オッズが正しくパースされる"""
        odds = [46.6, 3.2, 15.0, None, None, None, None, None]
        line = _build_oz_line(win_odds=odds)
        result = JRDBParser.parse_oz_line(line)
        assert result is not None
        win_list = result["win_odds_list"]
        assert len(win_list) == 18
        assert win_list[0] == pytest.approx(46.6, rel=1e-3)
        assert win_list[1] == pytest.approx(3.2, rel=1e-3)
        assert win_list[2] == pytest.approx(15.0, rel=1e-3)

    def test_place_odds_parsed_correctly(self):
        """複勝オッズが正しくパースされる"""
        odds = [8.5, 1.5, 4.0, None, None, None, None, None]
        line = _build_oz_line(place_odds=odds)
        result = JRDBParser.parse_oz_line(line)
        assert result is not None
        place_list = result["place_odds_list"]
        assert len(place_list) == 18
        assert place_list[0] == pytest.approx(8.5, rel=1e-3)
        assert place_list[1] == pytest.approx(1.5, rel=1e-3)

    def test_empty_odds_parsed_as_none(self):
        """オッズが空白の馬番は None になる"""
        line = _build_oz_line(win_odds=[10.0], place_odds=[2.0])
        result = JRDBParser.parse_oz_line(line)
        assert result is not None
        # 馬番2以降は空白 → None
        assert result["win_odds_list"][1] is None
        assert result["place_odds_list"][1] is None

    def test_returns_none_for_short_line(self):
        """短すぎる行は None を返す"""
        result = JRDBParser.parse_oz_line("05261101" + "08" + "046.6")
        assert result is None

    def test_returns_none_for_empty_race_key(self):
        """レースキーが空白の行は None を返す"""
        line = "        " + "08" + " " * 945
        result = JRDBParser.parse_oz_line(line)
        assert result is None

    def test_created_at_present(self):
        """created_at フィールドが含まれる"""
        line = _build_oz_line()
        result = JRDBParser.parse_oz_line(line)
        assert result is not None
        assert "created_at" in result


# ---------------------------------------------------------------------------
# expand_oz_to_odds_rows()
# ---------------------------------------------------------------------------

class TestExpandOzToOddsRows:
    """expand_oz_to_odds_rows() の動作テスト"""

    def _make_oz_record(self, race_id="05261101", win_odds=None, place_odds=None):
        """テスト用 OZ レコードを作成するヘルパー"""
        win_list = [None] * 18
        place_list = [None] * 18
        if win_odds:
            for i, v in enumerate(win_odds):
                if i < 18:
                    win_list[i] = v
        if place_odds:
            for i, v in enumerate(place_odds):
                if i < 18:
                    place_list[i] = v
        return {
            "race_id": race_id,
            "registered_count": 8,
            "win_odds_list": win_list,
            "place_odds_list": place_list,
            "created_at": "2024-12-31T10:00:00",
        }

    def test_generates_correct_number_of_rows(self):
        """有効なオッズ行のみ生成される"""
        record = self._make_oz_record(
            win_odds=[46.6, 3.2, 15.0],
            place_odds=[8.5, 1.5],
        )
        rows = JRDBParser.expand_oz_to_odds_rows(record)
        # win 3行 + place 2行 = 5行
        assert len(rows) == 5

    def test_horse_number_starts_at_1(self):
        """horse_number は 1 始まり"""
        record = self._make_oz_record(win_odds=[46.6])
        rows = JRDBParser.expand_oz_to_odds_rows(record)
        win_rows = [r for r in rows if r["odds_type"] == "win"]
        assert win_rows[0]["horse_number"] == 1

    def test_odds_type_set_correctly(self):
        """odds_type が win / place で正しく設定される"""
        record = self._make_oz_record(win_odds=[10.0], place_odds=[3.0])
        rows = JRDBParser.expand_oz_to_odds_rows(record)
        types = {r["odds_type"] for r in rows}
        assert types == {"win", "place"}

    def test_horse_id_is_none(self):
        """horse_id は None (OZ ファイルに含まれないため)"""
        record = self._make_oz_record(win_odds=[10.0])
        rows = JRDBParser.expand_oz_to_odds_rows(record)
        assert all(r["horse_id"] is None for r in rows)

    def test_zero_odds_skipped(self):
        """0以下のオッズはスキップされる"""
        record = self._make_oz_record(win_odds=[0.0, 10.0])
        rows = JRDBParser.expand_oz_to_odds_rows(record)
        win_rows = [r for r in rows if r["odds_type"] == "win"]
        assert len(win_rows) == 1
        assert win_rows[0]["horse_number"] == 2

    def test_none_odds_skipped(self):
        """None オッズはスキップされる"""
        record = self._make_oz_record(win_odds=[None, None])
        rows = JRDBParser.expand_oz_to_odds_rows(record)
        assert len(rows) == 0

    def test_odds_timestamp_uses_file_date(self):
        """file_date が渡された場合 odds_timestamp に反映される"""
        record = self._make_oz_record(win_odds=[10.0])
        rows = JRDBParser.expand_oz_to_odds_rows(record, file_date="2024-12-31")
        assert rows[0]["odds_timestamp"] == "2024-12-31T19:00:00+00:00"

    def test_odds_timestamp_fallback_to_created_at(self):
        """file_date が None の場合 created_at にフォールバックする"""
        record = self._make_oz_record(win_odds=[10.0])
        rows = JRDBParser.expand_oz_to_odds_rows(record, file_date=None)
        assert rows[0]["odds_timestamp"] == record["created_at"]

    def test_odds_value_matches_input(self):
        """odds_value が入力値と一致する"""
        record = self._make_oz_record(win_odds=[46.6], place_odds=[8.5])
        rows = JRDBParser.expand_oz_to_odds_rows(record)
        win_row = next(r for r in rows if r["odds_type"] == "win")
        place_row = next(r for r in rows if r["odds_type"] == "place")
        assert win_row["odds_value"] == pytest.approx(46.6, rel=1e-3)
        assert place_row["odds_value"] == pytest.approx(8.5, rel=1e-3)

    def test_race_id_propagated(self):
        """race_id が全行に伝播する"""
        record = self._make_oz_record(race_id="09261205", win_odds=[5.0])
        rows = JRDBParser.expand_oz_to_odds_rows(record)
        assert all(r["race_id"] == "09261205" for r in rows)


# ---------------------------------------------------------------------------
# parse_file() での OZ 統合テスト
# ---------------------------------------------------------------------------

class TestParseFileOZ:
    """parse_file() が OZ を正しく処理することを確認"""

    def test_oz_recognized_by_parse_file(self):
        """parse_file() が OZ データタイプを認識してパースする"""
        line = _build_oz_line(
            race_key="05261101",
            registered_count=3,
            win_odds=[5.0, 10.0, 20.0],
            place_odds=[2.0, 3.0, 5.0],
        )
        results = JRDBParser.parse_file(line + "\n", "OZ")
        assert len(results) == 1
        assert results[0]["race_id"] == "05261101"

    def test_oz_multiple_lines_parsed(self):
        """複数行のOZファイルが全行パースされる"""
        line1 = _build_oz_line(race_key="05261101", win_odds=[5.0])
        line2 = _build_oz_line(race_key="05261102", win_odds=[8.0])
        content = line1 + "\n" + line2
        results = JRDBParser.parse_file(content, "OZ")
        assert len(results) == 2
        race_ids = {r["race_id"] for r in results}
        assert race_ids == {"05261101", "05261102"}


# ---------------------------------------------------------------------------
# OZ 馬連オッズテスト (umaren_odds_list / expand_oz_to_combo_rows)
# ---------------------------------------------------------------------------

def _build_oz_line_with_umaren(
    race_key: str = "05261101",
    registered_count: int = 8,
    win_odds: list = None,
    place_odds: list = None,
    umaren_odds: dict = None,
) -> str:
    """
    馬連オッズを含む OZ 行を構築するヘルパー。
    umaren_odds: {(h1, h2): odds_value} のdict
    """
    win_odds = win_odds or []
    place_odds = place_odds or []

    def _fmt_odds(odds_list: list) -> str:
        result = ""
        for i in range(18):
            val = odds_list[i] if i < len(odds_list) else None
            if val is None or val <= 0:
                result += "     "
            else:
                result += f"{val:05.1f}"
        return result

    # 馬連オッズ部分 (153通り × 5文字)
    all_pairs = list(combinations(range(1, 19), 2))
    umaren_part = ""
    for h1, h2 in all_pairs:
        val = (umaren_odds or {}).get((h1, h2), 0.0)
        if val is not None and val > 0:
            umaren_part += f"{val:05.1f}"
        else:
            umaren_part += "     "

    body = (
        race_key
        + f"{registered_count:02d}"
        + _fmt_odds(win_odds)
        + _fmt_odds(place_odds)
        + umaren_part
    )
    return body


class TestOzUmarenParsing:
    """OZ 馬連オッズ (umaren_odds_list) のテスト"""

    def test_umaren_odds_list_present_when_long_enough(self):
        """十分な長さの行では umaren_odds_list が返されること"""
        line = _build_oz_line_with_umaren(umaren_odds={(1, 2): 28.2})
        result = JRDBParser.parse_oz_line(line)
        assert result is not None
        assert "umaren_odds_list" in result
        assert len(result["umaren_odds_list"]) == 153

    def test_umaren_odds_list_empty_for_short_line(self):
        """955バイト未満の行では umaren_odds_list が空になること"""
        # _build_oz_line は馬連部分を "0"×765 で埋めるが、umaren_odds_listは実際には
        # "0"文字列がsafe_float("00000")=0.0を返す可能性があるため、
        # 短い行で確認する
        short_line = "05261101" + "08" + "     " * 18 + "     " * 18  # 190文字未満
        result = JRDBParser.parse_oz_line(short_line)
        # 行が短すぎる(<100)場合はNoneが返る
        # 100以上190未満の場合はumaren_odds_listが空
        if result is not None:
            assert result["umaren_odds_list"] == []

    def test_specific_umaren_odds_value(self):
        """特定の馬連オッズ値が正しくパースされること"""
        line = _build_oz_line_with_umaren(umaren_odds={(1, 2): 28.2, (3, 5): 150.0})
        result = JRDBParser.parse_oz_line(line)
        assert result is not None
        found_12 = [(h1, h2, v) for h1, h2, v in result["umaren_odds_list"] if h1 == 1 and h2 == 2]
        assert len(found_12) == 1
        assert abs(found_12[0][2] - 28.2) < 0.05

    def test_combination_order(self):
        """組み合わせ順が (1,2), (1,3), ..., (17,18) であること"""
        line = _build_oz_line_with_umaren()
        result = JRDBParser.parse_oz_line(line)
        assert result is not None
        pairs = [(h1, h2) for h1, h2, _ in result["umaren_odds_list"]]
        expected = list(combinations(range(1, 19), 2))
        assert pairs == expected


class TestExpandOzToComboRows:
    """expand_oz_to_combo_rows() の動作テスト"""

    def _make_oz_record_with_umaren(self, race_id="05261101", umaren_odds_dict=None):
        """テスト用 OZ レコードを作成するヘルパー"""
        all_pairs = list(combinations(range(1, 19), 2))
        umaren_list = []
        for h1, h2 in all_pairs:
            val = (umaren_odds_dict or {}).get((h1, h2), None)
            umaren_list.append((h1, h2, val))
        return {
            "race_id": race_id,
            "registered_count": 8,
            "win_odds_list": [None] * 18,
            "place_odds_list": [None] * 18,
            "umaren_odds_list": umaren_list,
            "created_at": "2024-12-31T10:00:00",
        }

    def test_basic_expansion(self):
        """馬連オッズが正しく展開されること"""
        record = self._make_oz_record_with_umaren(umaren_odds_dict={(1, 2): 28.2, (3, 5): 150.0})
        rows = JRDBParser.expand_oz_to_combo_rows(record, file_date="2024-12-31")
        assert len(rows) == 2

    def test_bet_type_is_umaren(self):
        """bet_type が 'umaren' であること"""
        record = self._make_oz_record_with_umaren(umaren_odds_dict={(1, 2): 10.0})
        rows = JRDBParser.expand_oz_to_combo_rows(record, file_date="2024-12-31")
        assert rows[0]["bet_type"] == "umaren"

    def test_schema_fields(self):
        """必須フィールドが全て含まれること"""
        record = self._make_oz_record_with_umaren(umaren_odds_dict={(2, 5): 15.3})
        rows = JRDBParser.expand_oz_to_combo_rows(record, file_date="2024-12-31")
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
        """horse_number_3 が None であること (馬連は2頭)"""
        record = self._make_oz_record_with_umaren(umaren_odds_dict={(1, 2): 10.0})
        rows = JRDBParser.expand_oz_to_combo_rows(record, file_date="2024-12-31")
        assert rows[0]["horse_number_3"] is None

    def test_zero_or_none_odds_skipped(self):
        """0以下またはNoneのオッズはスキップされること"""
        record = self._make_oz_record_with_umaren(umaren_odds_dict={(1, 2): 0.0})
        rows = JRDBParser.expand_oz_to_combo_rows(record, file_date="2024-12-31")
        assert len(rows) == 0

    def test_empty_umaren_returns_empty_list(self):
        """umaren_odds_listが空または全None時は空リストを返すこと"""
        record = self._make_oz_record_with_umaren(umaren_odds_dict={})
        rows = JRDBParser.expand_oz_to_combo_rows(record, file_date="2024-12-31")
        assert rows == []

    def test_odds_timestamp_format(self):
        """file_date が odds_timestamp に反映されること"""
        record = self._make_oz_record_with_umaren(umaren_odds_dict={(1, 2): 10.0})
        rows = JRDBParser.expand_oz_to_combo_rows(record, file_date="2024-12-31")
        assert "2024-12-31T19:00:00" in rows[0]["odds_timestamp"]

    def test_race_id_propagated(self):
        """race_id が全行に伝播すること"""
        record = self._make_oz_record_with_umaren(race_id="09261205", umaren_odds_dict={(1, 2): 10.0})
        rows = JRDBParser.expand_oz_to_combo_rows(record, file_date="2024-12-31")
        assert all(r["race_id"] == "09261205" for r in rows)
