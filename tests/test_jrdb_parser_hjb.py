"""
JRDBParser の HJB/HJC 払戻情報パーサーのユニットテスト

parse_hjb_line() および expand_hjb_to_payout_rows() の動作を検証する。
"""

import pytest

from src.automation.data.jrdb_parser import JRDBParser


def _build_hjb_line(
    race_key: str = "08251301",
    win: list = None,
    place: list = None,
    wakuren: list = None,
    umaren: list = None,
    wide: list = None,
    umatan: list = None,
    sanrenpuku: list = None,
    sanrentan: list = None,
) -> str:
    """
    テスト用 HJB/HJC 行文字列を構築するヘルパー。

    各引数はエントリのリスト:
        win/place/wakuren: [(horse_str2, payout_str7), ...]  (最大3/5/3件)
        umaren/wide/umatan: [(horse_str4, payout_str8), ...]  (最大3/7/6件)
        sanrenpuku: [(horse_str6, payout_str8), ...]  (最大3件)
        sanrentan:  [(horse_str6, payout_str9), ...]  (最大6件, HJC のみ)

    指定がない部分はスペースで埋める。
    """
    def _pad_entries(entries, horse_w, payout_w, max_n):
        result = ""
        for i in range(max_n):
            if entries and i < len(entries):
                h, p = entries[i]
                result += h.ljust(horse_w)[:horse_w] + p.rjust(payout_w)[:payout_w]
            else:
                result += " " * (horse_w + payout_w)
        return result

    # 単勝(3件×9) + 複勝(5件×9) + 枠連(3件×9) + 馬連(3件×12) +
    # ワイド(7件×12) + 馬単(6件×12) + 三連複(3件×14)
    body = (
        race_key
        + _pad_entries(win, 2, 7, 3)
        + _pad_entries(place, 2, 7, 5)
        + _pad_entries(wakuren, 2, 7, 3)
        + _pad_entries(umaren, 4, 8, 3)
        + _pad_entries(wide, 4, 8, 7)
        + _pad_entries(umatan, 4, 8, 6)
        + _pad_entries(sanrenpuku, 6, 8, 3)
    )
    # 三連単(6件×15, HJCのみ)
    if sanrentan is not None:
        body += _pad_entries(sanrentan, 6, 9, 6)
    return body


class TestParseRaceIdHex:
    """parse_race_id() の16進数フィールド対応テスト"""

    def test_day_decimal(self):
        """日フィールドが通常の数字(1-9)の場合は正常にパースされる"""
        result = JRDBParser.parse_race_id("08251301")
        assert result["day"] == 3

    def test_day_hex_a(self):
        """日フィールドが 'a' (=10) のレースキーを正しくパースする"""
        result = JRDBParser.parse_race_id("08251a01")
        assert result["day"] == 10

    def test_day_hex_b(self):
        """日フィールドが 'b' (=11) のレースキーを正しくパースする（実際のバグケース）"""
        result = JRDBParser.parse_race_id("08221b01")
        assert result["day"] == 11

    def test_day_hex_c(self):
        """日フィールドが 'c' (=12) のレースキーを正しくパースする"""
        result = JRDBParser.parse_race_id("08221c01")
        assert result["day"] == 12

    def test_kai_hex_a(self):
        """回フィールドが 'a' (=10) の場合も正しくパースされる"""
        result = JRDBParser.parse_race_id("0822a101")
        assert result["kai"] == 10

    def test_hjb_line_with_hex_day_parses_successfully(self):
        """16進数の日フィールドを含むHJB行全体が正常にパースされる（Issue #95）"""
        line = _build_hjb_line(
            race_key="08221b01",  # day='b'=11
            win=[("03", "0000170")],
        )
        result = JRDBParser.parse_hjb_line(line)
        assert result is not None
        assert result["race_id"] == "08221b01"
        assert len(result["win_payouts"]) == 1


class TestParseHjbLine:
    """parse_hjb_line() のテスト"""

    def test_parses_win_payout(self):
        """単勝払戻が正しくパースされる"""
        line = _build_hjb_line(
            race_key="08251301",
            win=[("03", "0000170")],
        )
        result = JRDBParser.parse_hjb_line(line)
        assert result is not None
        assert result["race_id"] == "08251301"
        assert len(result["win_payouts"]) == 1
        horse, payout = result["win_payouts"][0]
        assert horse == 3
        assert payout == 170

    def test_parses_place_payouts(self):
        """複勝払戻(複数馬)が正しくパースされる"""
        line = _build_hjb_line(
            place=[
                ("03", "0000110"),
                ("09", "0000110"),
                ("08", "0000120"),
            ],
        )
        result = JRDBParser.parse_hjb_line(line)
        assert result is not None
        assert len(result["place_payouts"]) == 3
        horses = [p[0] for p in result["place_payouts"]]
        assert horses == [3, 9, 8]

    def test_parses_umaren_payout(self):
        """馬連払戻(4桁組合せ)が正しくパースされる"""
        line = _build_hjb_line(
            umaren=[("0309", "00001540")],
        )
        result = JRDBParser.parse_hjb_line(line)
        assert result is not None
        assert len(result["umaren_payouts"]) == 1
        combo, payout = result["umaren_payouts"][0]
        # combo は safe_int("0309") = 309
        assert combo == 309
        assert payout == 1540

    def test_parses_wide_payouts(self):
        """ワイド払戻(複数)が正しくパースされる"""
        line = _build_hjb_line(
            wide=[
                ("0309", "00000370"),
                ("0308", "00000380"),
                ("0908", "00000410"),
            ],
        )
        result = JRDBParser.parse_hjb_line(line)
        assert result is not None
        assert len(result["wide_payouts"]) == 3

    def test_parses_sanrenpuku_payout(self):
        """三連複払戻(6桁組合せ)が正しくパースされる"""
        line = _build_hjb_line(
            sanrenpuku=[("030809", "00001050")],
        )
        result = JRDBParser.parse_hjb_line(line)
        assert result is not None
        assert len(result["sanrenpuku_payouts"]) == 1
        combo, payout = result["sanrenpuku_payouts"][0]
        assert combo == 30809
        assert payout == 1050

    def test_parses_sanrentan_payout_hjc(self):
        """三連単払戻(HJCのみ, 9桁払戻金)が正しくパースされる"""
        line = _build_hjb_line(
            sanrentan=[("030908", "000005670")],
        )
        result = JRDBParser.parse_hjb_line(line)
        assert result is not None
        assert len(result["sanrentan_payouts"]) == 1
        combo, payout = result["sanrentan_payouts"][0]
        assert combo == 30908
        assert payout == 5670

    def test_empty_entries_are_ignored(self):
        """空エントリ(スペース埋め)は含まれない"""
        line = _build_hjb_line()  # 全エントリ空
        result = JRDBParser.parse_hjb_line(line)
        assert result is not None
        assert result["win_payouts"] == []
        assert result["place_payouts"] == []
        assert result["umaren_payouts"] == []

    def test_returns_none_for_short_line(self):
        """行が短すぎる場合 None を返す"""
        result = JRDBParser.parse_hjb_line("08251301   ")
        assert result is None

    def test_race_id_preserved(self):
        """race_id がレースキーそのままで返る"""
        line = _build_hjb_line(race_key="05261204")
        result = JRDBParser.parse_hjb_line(line)
        assert result is not None
        assert result["race_id"] == "05261204"

    def test_sanrentan_absent_in_hjb(self):
        """HJB形式(三連単なし)では sanrentan_payouts が空"""
        # sanrentan 引数を渡さない → 三連単セクションなし
        line = _build_hjb_line(
            win=[("03", "0000170")],
        )
        assert len(line) < 431
        result = JRDBParser.parse_hjb_line(line)
        assert result is not None
        assert result["sanrentan_payouts"] == []


class TestExpandHjbToPayoutRows:
    """expand_hjb_to_payout_rows() のテスト"""

    def _make_record(self, **payouts):
        """テスト用の最小レコードを返す"""
        base = {
            "race_id": "08251301",
            "venue_code": "08",
            "year": 25,
            "race_number": 1,
            "win_payouts": [],
            "place_payouts": [],
            "wakuren_payouts": [],
            "umaren_payouts": [],
            "wide_payouts": [],
            "umatan_payouts": [],
            "sanrenpuku_payouts": [],
            "sanrentan_payouts": [],
            "created_at": "2025-01-01T00:00:00",
            "updated_at": "2025-01-01T00:00:00",
        }
        base.update(payouts)
        return base

    def test_win_row_has_correct_fields(self):
        """単勝: horse_number_1のみ設定され、2/3はNone"""
        record = self._make_record(win_payouts=[(3, 170)])
        rows = JRDBParser.expand_hjb_to_payout_rows(record)
        assert len(rows) == 1
        row = rows[0]
        assert row["bet_type"] == "win"
        assert row["horse_number_1"] == 3
        assert row["horse_number_2"] is None
        assert row["horse_number_3"] is None
        assert row["payout_amount"] == 170
        assert row["rank"] == 1

    def test_place_multiple_entries_rank(self):
        """複勝: 複数エントリのrank連番"""
        record = self._make_record(
            place_payouts=[(3, 110), (9, 110), (8, 120)]
        )
        rows = JRDBParser.expand_hjb_to_payout_rows(record)
        place_rows = [r for r in rows if r["bet_type"] == "place"]
        assert len(place_rows) == 3
        assert [r["rank"] for r in place_rows] == [1, 2, 3]
        assert [r["horse_number_1"] for r in place_rows] == [3, 9, 8]

    def test_umaren_splits_4digit_combo_correctly(self):
        """馬連: 4桁組合せ(0309)→horse_number_1=3, horse_number_2=9"""
        record = self._make_record(umaren_payouts=[(309, 1540)])
        rows = JRDBParser.expand_hjb_to_payout_rows(record)
        umaren_rows = [r for r in rows if r["bet_type"] == "umaren"]
        assert len(umaren_rows) == 1
        assert umaren_rows[0]["horse_number_1"] == 3
        assert umaren_rows[0]["horse_number_2"] == 9
        assert umaren_rows[0]["horse_number_3"] is None
        assert umaren_rows[0]["payout_amount"] == 1540

    def test_wide_splits_4digit_combo_with_leading_zero_horse(self):
        """ワイド: 0103 → horse1=1, horse2=3 (先頭ゼロが消えた整数103を正しく分割)"""
        record = self._make_record(wide_payouts=[(103, 500)])
        rows = JRDBParser.expand_hjb_to_payout_rows(record)
        wide_rows = [r for r in rows if r["bet_type"] == "wide"]
        assert len(wide_rows) == 1
        assert wide_rows[0]["horse_number_1"] == 1
        assert wide_rows[0]["horse_number_2"] == 3

    def test_umatan_splits_4digit_combo(self):
        """馬単: 4桁組合せが正しく分割される"""
        record = self._make_record(umatan_payouts=[(309, 2780)])
        rows = JRDBParser.expand_hjb_to_payout_rows(record)
        umatan_rows = [r for r in rows if r["bet_type"] == "umatan"]
        assert len(umatan_rows) == 1
        assert umatan_rows[0]["horse_number_1"] == 3
        assert umatan_rows[0]["horse_number_2"] == 9

    def test_sanrenpuku_splits_6digit_combo(self):
        """三連複: 6桁組合せ(030809)→horse1=3, horse2=8, horse3=9"""
        record = self._make_record(sanrenpuku_payouts=[(30809, 1050)])
        rows = JRDBParser.expand_hjb_to_payout_rows(record)
        sr_rows = [r for r in rows if r["bet_type"] == "sanrenpuku"]
        assert len(sr_rows) == 1
        assert sr_rows[0]["horse_number_1"] == 3
        assert sr_rows[0]["horse_number_2"] == 8
        assert sr_rows[0]["horse_number_3"] == 9
        assert sr_rows[0]["payout_amount"] == 1050

    def test_sanrentan_splits_6digit_combo(self):
        """三連単: 6桁組合せが正しく分割される"""
        record = self._make_record(sanrentan_payouts=[(30908, 5670)])
        rows = JRDBParser.expand_hjb_to_payout_rows(record)
        st_rows = [r for r in rows if r["bet_type"] == "sanrentan"]
        assert len(st_rows) == 1
        assert st_rows[0]["horse_number_1"] == 3
        assert st_rows[0]["horse_number_2"] == 9
        assert st_rows[0]["horse_number_3"] == 8

    def test_wakuren_splits_frame_numbers(self):
        """枠連: 2桁(38)→frame1=3, frame2=8"""
        record = self._make_record(wakuren_payouts=[(38, 820)])
        rows = JRDBParser.expand_hjb_to_payout_rows(record)
        wk_rows = [r for r in rows if r["bet_type"] == "wakuren"]
        assert len(wk_rows) == 1
        assert wk_rows[0]["horse_number_1"] == 3
        assert wk_rows[0]["horse_number_2"] == 8

    def test_empty_record_returns_empty_list(self):
        """空のレコード(全払戻なし)は空のリストを返す"""
        record = self._make_record()
        rows = JRDBParser.expand_hjb_to_payout_rows(record)
        assert rows == []

    def test_race_id_propagated(self):
        """全行に race_id が伝播される"""
        record = self._make_record(
            win_payouts=[(3, 170)],
            place_payouts=[(3, 110), (9, 110)],
        )
        rows = JRDBParser.expand_hjb_to_payout_rows(record)
        for row in rows:
            assert row["race_id"] == "08251301"

    def test_full_hjb_line_round_trip(self):
        """parse_hjb_line → expand_hjb_to_payout_rows のラウンドトリップ"""
        line = _build_hjb_line(
            race_key="08251301",
            win=[("03", "0000170")],
            place=[("03", "0000110"), ("09", "0000110"), ("08", "0000120")],
            umaren=[("0309", "00001540")],
            sanrenpuku=[("030809", "00001050")],
        )
        parsed = JRDBParser.parse_hjb_line(line)
        assert parsed is not None
        rows = JRDBParser.expand_hjb_to_payout_rows(parsed)
        bet_types = {r["bet_type"] for r in rows}
        assert "win" in bet_types
        assert "place" in bet_types
        assert "umaren" in bet_types
        assert "sanrenpuku" in bet_types

        win_rows = [r for r in rows if r["bet_type"] == "win"]
        assert win_rows[0]["horse_number_1"] == 3
        assert win_rows[0]["payout_amount"] == 170

        umaren_rows = [r for r in rows if r["bet_type"] == "umaren"]
        assert umaren_rows[0]["horse_number_1"] == 3
        assert umaren_rows[0]["horse_number_2"] == 9

        sr_rows = [r for r in rows if r["bet_type"] == "sanrenpuku"]
        assert sr_rows[0]["horse_number_1"] == 3
        assert sr_rows[0]["horse_number_2"] == 8
        assert sr_rows[0]["horse_number_3"] == 9
