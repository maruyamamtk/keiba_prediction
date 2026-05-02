"""
netkeibaスクレイパーのテスト

対象モジュール: src.automation.data.netkeiba_scraper
- parse_netkeiba_race_id: race_idのパース
- _parse_race_list_html: レース一覧HTMLのパース
- _parse_win_place_odds_html: 単複オッズHTMLのパース
- netkeiba_to_jrdb_race_id: BQ照合（モック）
- save_odds_to_bq: BQ保存（モック）
- scrape_today_odds: 全体フロー（モック）

Note: Playwrightを使う get_today_race_list / get_win_place_odds は
      結合テストのため、ここでは _parse_* 関数のみをテストする。

Issue #131: netkeibaリアルタイムオッズスクレイパーの実装
"""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.automation.data.netkeiba_scraper import (
    BLOCKED_RESOURCE_TYPES,
    _create_page_with_route_block,
    _parse_combo_odds_html,
    _parse_race_list_html,
    _parse_sanrenpuku_ninki_html,
    _parse_win_place_odds_html,
    get_combo_odds,
    get_today_race_list,
    get_win_place_odds,
    jrdb_race_id_to_netkeiba,
    netkeiba_to_jrdb_race_id,
    parse_netkeiba_race_id,
    save_combo_odds_to_bq,
    save_odds_to_bq,
    scrape_odds_for_race,
    scrape_today_odds,
)


# ---------------------------------------------------------------------------
# parse_netkeiba_race_id
# ---------------------------------------------------------------------------


class TestParseNetkeibaRaceId:
    def test_normal(self):
        result = parse_netkeiba_race_id("202405021211")
        assert result["year"] == "2024"
        assert result["venue_code"] == "05"
        assert result["kai"] == "02"
        assert result["nichi"] == "12"
        assert result["race_number"] == 11

    def test_sapporo(self):
        result = parse_netkeiba_race_id("202401020208")
        assert result["venue_code"] == "01"
        assert result["race_number"] == 8

    def test_invalid_length(self):
        with pytest.raises(ValueError, match="12桁"):
            parse_netkeiba_race_id("20240502121")  # 11桁

    def test_non_digit(self):
        with pytest.raises(ValueError, match="12桁"):
            parse_netkeiba_race_id("2024050212AB")


# ---------------------------------------------------------------------------
# _parse_race_list_html
# ---------------------------------------------------------------------------


class TestParseRaceListHtml:
    def _make_html(self, race_ids: list[str]) -> str:
        links = "".join(
            f'<a href="/race/result.html?race_id={rid}&ref=top">レース</a>'
            for rid in race_ids
        )
        return f"<html><body>{links}</body></html>"

    def test_single_race(self):
        html = self._make_html(["202405021201"])
        result = _parse_race_list_html(html)
        assert len(result) == 1
        assert result[0]["netkeiba_race_id"] == "202405021201"
        assert result[0]["venue_code"] == "05"
        assert result[0]["race_number"] == 1

    def test_multiple_races_deduplication(self):
        # 同一race_idが複数のリンクに出現 → 重複除去
        html = self._make_html(["202405021201", "202405021201", "202405021202"])
        result = _parse_race_list_html(html)
        assert len(result) == 2

    def test_multiple_venues(self):
        html = self._make_html([
            "202405021201",  # 東京
            "202406030801",  # 中山
        ])
        result = _parse_race_list_html(html)
        assert len(result) == 2
        venue_codes = {r["venue_code"] for r in result}
        assert "05" in venue_codes
        assert "06" in venue_codes

    def test_sorted_by_race_id(self):
        html = self._make_html(["202405021212", "202405021201"])
        result = _parse_race_list_html(html)
        assert result[0]["netkeiba_race_id"] == "202405021201"
        assert result[1]["netkeiba_race_id"] == "202405021212"

    def test_no_race_links(self):
        html = "<html><body><p>レースなし</p></body></html>"
        result = _parse_race_list_html(html)
        assert result == []

    def test_ignores_invalid_race_ids(self):
        html = (
            '<a href="/race/result.html?race_id=INVALID">x</a>'
            '<a href="/race/result.html?race_id=202405021201">ok</a>'
        )
        result = _parse_race_list_html(html)
        assert len(result) == 1
        assert result[0]["netkeiba_race_id"] == "202405021201"


# ---------------------------------------------------------------------------
# _parse_win_place_odds_html
# ---------------------------------------------------------------------------


def _make_odds_html(
    win_rows: list[tuple[int, float]],
    place_rows: list[tuple[int, float, float]],
) -> str:
    """単複オッズテーブルを含むHTMLを生成する（netkeibaの実際のDOM構造を模倣）

    実際の構造:
      <div id="odds_tan_block">
        <table class="RaceOdds_HorseList_Table">
          <tr><th>枠</th><th>馬番</th><th>印</th><th>選択</th><th>馬名</th><th>オッズ</th></tr>
          <tr><td>枠番</td><td>馬番</td><td></td><td></td><td>馬名</td><td>オッズ</td></tr>
        </table>
      </div>
    複勝オッズ区切り: "1.3 - 1.8" （スペース+ハイフン）
    """
    header = "<tr><th>枠</th><th>馬番</th><th>印</th><th>選択</th><th>馬名</th><th>オッズ</th></tr>"
    win_rows_html = "".join(
        f"<tr><td>1</td><td>{h}</td><td></td><td></td><td>馬名{h}</td><td>{o}</td></tr>"
        for h, o in win_rows
    )
    place_rows_html = "".join(
        f"<tr><td>1</td><td>{h}</td><td></td><td></td><td>馬名{h}</td><td>{lo} - {hi}</td></tr>"
        for h, lo, hi in place_rows
    )
    return (
        "<html><body>"
        '<div id="odds_tan_block">'
        '<table class="RaceOdds_HorseList_Table">'
        f"<thead>{header}</thead>"
        f"<tbody>{win_rows_html}</tbody>"
        "</table></div>"
        '<div id="odds_fuku_block">'
        '<table class="RaceOdds_HorseList_Table">'
        f"<thead>{header}</thead>"
        f"<tbody>{place_rows_html}</tbody>"
        "</table></div>"
        "</body></html>"
    )


class TestParseWinPlaceOddsHtml:
    def test_normal(self):
        html = _make_odds_html(
            win_rows=[(1, 2.5), (2, 4.0), (3, 8.0)],
            place_rows=[(1, 1.3, 1.8), (2, 1.9, 2.5), (3, 3.1, 4.2)],
        )
        df = _parse_win_place_odds_html(html)
        assert not df.empty
        assert len(df) == 3

    def test_win_odds_extracted(self):
        html = _make_odds_html(
            win_rows=[(1, 2.5)],
            place_rows=[(1, 1.3, 1.8)],
        )
        df = _parse_win_place_odds_html(html)
        horse1 = df[df["horse_number"] == 1]
        assert not horse1.empty
        assert horse1.iloc[0]["win_odds"] == pytest.approx(2.5, abs=0.01)

    def test_place_odds_range(self):
        html = _make_odds_html(
            win_rows=[(1, 2.5)],
            place_rows=[(1, 1.3, 1.8)],
        )
        df = _parse_win_place_odds_html(html)
        horse1 = df[df["horse_number"] == 1]
        assert not horse1.empty
        assert horse1.iloc[0]["place_odds_min"] == pytest.approx(1.3, abs=0.01)
        assert horse1.iloc[0]["place_odds_max"] == pytest.approx(1.8, abs=0.01)

    def test_empty_html(self):
        df = _parse_win_place_odds_html("<html><body></body></html>")
        assert df.empty
        assert list(df.columns) == [
            "horse_number", "win_odds", "place_odds_min", "place_odds_max"
        ]

    def test_columns_present(self):
        html = _make_odds_html(
            win_rows=[(1, 2.5)],
            place_rows=[(1, 1.3, 1.8)],
        )
        df = _parse_win_place_odds_html(html)
        for col in ["horse_number", "win_odds", "place_odds_min", "place_odds_max"]:
            assert col in df.columns

    def test_sorted_by_horse_number(self):
        html = _make_odds_html(
            win_rows=[(3, 8.0), (1, 2.5)],
            place_rows=[(3, 3.1, 4.2), (1, 1.3, 1.8)],
        )
        df = _parse_win_place_odds_html(html)
        assert list(df["horse_number"]) == [1, 3]

    def test_no_place_odds_table(self):
        """複勝テーブルがない場合でも単勝は取得できる"""
        html = (
            "<html><body>"
            '<div id="odds_tan_block">'
            '<table class="RaceOdds_HorseList_Table">'
            "<thead><tr><th>枠</th><th>馬番</th><th>印</th><th>選択</th><th>馬名</th><th>オッズ</th></tr></thead>"
            "<tbody><tr><td>1</td><td>1</td><td></td><td></td><td>馬名1</td><td>2.5</td></tr></tbody>"
            "</table></div>"
            "</body></html>"
        )
        df = _parse_win_place_odds_html(html)
        assert not df.empty
        assert df.iloc[0]["win_odds"] == pytest.approx(2.5, abs=0.01)


# ---------------------------------------------------------------------------
# _parse_combo_odds_html
# ---------------------------------------------------------------------------


def _make_combo_html(ticket_type: str, combos: list[tuple]) -> str:
    """組み合わせオッズ span を含むHTMLを生成する

    Args:
        ticket_type: 'b4'(馬連) / 'b5'(ワイド) / 'b6'(馬単) / 'b7'(三連複)
        combos: 2頭の場合 [(h1, h2, odds), ...]、3頭の場合 [(h1, h2, h3, odds), ...]
    """
    type_code = {"b4": "4", "b5": "5", "b7": "7", "b6": "6"}[ticket_type]
    is_trio = ticket_type == "b7"
    spans = ""
    for combo in combos:
        if is_trio:
            h1, h2, h3, o = combo
            span_id = f"odds-{type_code}-{h1:02d}{h2:02d}{h3:02d}"
        else:
            h1, h2, o = combo
            span_id = f"odds-{type_code}-{h1:02d}{h2:02d}"
        spans += f'<span id="{span_id}">{o}</span>'
    return f"<html><body>{spans}</body></html>"


class TestParseComboOddsHtml:
    def test_umaren_b4(self):
        html = _make_combo_html("b4", [(1, 2, 5.5), (1, 3, 8.0)])
        df = _parse_combo_odds_html(html, "b4")
        assert len(df) == 2
        assert set(df["ticket_type"]) == {"umaren"}
        row = df[(df["horse_number_1"] == 1) & (df["horse_number_2"] == 2)].iloc[0]
        assert row["odds"] == pytest.approx(5.5)
        assert row["horse_number_3"] is None

    def test_wide_b5(self):
        """b5 はワイド（Issue #167 修正: 旧 umatan → 正しくは wide）"""
        html = _make_combo_html("b5", [(1, 2, 6.0)])
        df = _parse_combo_odds_html(html, "b5")
        assert len(df) == 1
        assert df.iloc[0]["ticket_type"] == "wide"

    def test_umatan_b6(self):
        """b6 は馬単・順序あり（Issue #167 修正: 旧 sanrenpuku → 正しくは umatan）"""
        html = _make_combo_html("b6", [(1, 2, 8.0)])
        df = _parse_combo_odds_html(html, "b6")
        assert len(df) == 1
        assert df.iloc[0]["ticket_type"] == "umatan"
        assert df.iloc[0]["horse_number_1"] == 1
        assert df.iloc[0]["horse_number_2"] == 2
        assert df.iloc[0]["horse_number_3"] is None

    def test_sanrenpuku_b7_returns_empty(self):
        """b7（三連複）は _parse_combo_odds_html では処理しない（_parse_sanrenpuku_ninki_html を使用）"""
        html = _make_combo_html("b7", [(1, 2, 3, 12.5)])
        df = _parse_combo_odds_html(html, "b7")
        assert df.empty

    def test_empty_html(self):
        df = _parse_combo_odds_html("<html><body></body></html>", "b4")
        assert df.empty
        assert list(df.columns) == [
            "ticket_type", "horse_number_1", "horse_number_2", "horse_number_3", "odds"
        ]

    def test_invalid_ticket_type(self):
        df = _parse_combo_odds_html("<html><body></body></html>", "b9")
        assert df.empty

    def test_filters_odds_lte_1(self):
        """オッズが1.0以下の組み合わせは除外される"""
        html = _make_combo_html("b4", [(1, 2, 1.0), (1, 3, 5.5)])
        df = _parse_combo_odds_html(html, "b4")
        assert len(df) == 1
        assert df.iloc[0]["horse_number_2"] == 3

    def test_skips_non_numeric_odds(self):
        """数値でないオッズはスキップされる"""
        type_code = "4"
        html = (
            f'<span id="odds-{type_code}-0102">---</span>'
            f'<span id="odds-{type_code}-0103">5.5</span>'
        )
        df = _parse_combo_odds_html(html, "b4")
        assert len(df) == 1
        assert df.iloc[0]["odds"] == pytest.approx(5.5)

    def test_comma_in_odds(self):
        """カンマ区切りのオッズ（例: 1,234.5）を正しくパースできる"""
        type_code = "4"
        html = f'<span id="odds-{type_code}-0102">1,234.5</span>'
        df = _parse_combo_odds_html(html, "b4")
        assert len(df) == 1
        assert df.iloc[0]["odds"] == pytest.approx(1234.5)


# ---------------------------------------------------------------------------
# save_combo_odds_to_bq (BQモック)
# ---------------------------------------------------------------------------
# _parse_sanrenpuku_ninki_html
# ---------------------------------------------------------------------------


def _make_sanrenpuku_ninki_html(combos: list[tuple]) -> str:
    """ninki-data_N 形式の三連複HTMLを生成するヘルパー

    Args:
        combos: [(h1, h2, h3, odds), ...] の形式
    """
    rows = ""
    for i, (h1, h2, h3, odds) in enumerate(combos, start=1):
        rows += (
            f'<tr id="ninki-data_{i}">'
            f'<td class="Ninki">{i}</td>'
            f'<td class="Check_Odd"></td>'
            f'<td>{h1}{h2}{h3}</td>'
            f'<td class="Name_Odds">{odds}</td>'
            f'<td>{h1}</td>'
            f'<td class="Horse_Name Txt_L">馬名{h1}</td>'
            f'<td>{h2}</td>'
            f'<td class="Horse_Name Txt_L">馬名{h2}</td>'
            f'<td>{h3}</td>'
            f'<td class="Horse_Name Txt_L">馬名{h3}</td>'
            f'</tr>'
        )
    return f"<html><body><table>{rows}</table></body></html>"


class TestParseSanrenpukuNinkiHtml:
    def test_basic(self):
        """基本的な三連複パースが正常に動作する"""
        html = _make_sanrenpuku_ninki_html([(1, 5, 6, 14.1), (5, 6, 10, 11.5)])
        df = _parse_sanrenpuku_ninki_html(html)
        assert len(df) == 2
        assert set(df["ticket_type"]) == {"sanrenpuku"}

    def test_horse_numbers_sorted_ascending(self):
        """馬番は昇順に整列されて保存される"""
        html = _make_sanrenpuku_ninki_html([(6, 1, 5, 14.1)])
        df = _parse_sanrenpuku_ninki_html(html)
        assert df.iloc[0]["horse_number_1"] == 1
        assert df.iloc[0]["horse_number_2"] == 5
        assert df.iloc[0]["horse_number_3"] == 6

    def test_odds_value(self):
        """オッズが正しく取得される"""
        html = _make_sanrenpuku_ninki_html([(5, 6, 10, 11.5)])
        df = _parse_sanrenpuku_ninki_html(html)
        assert df.iloc[0]["odds"] == pytest.approx(11.5)

    def test_filters_odds_lte_1(self):
        """オッズが1.0以下の行は除外される"""
        html = _make_sanrenpuku_ninki_html([(1, 2, 3, 1.0), (1, 5, 6, 14.1)])
        df = _parse_sanrenpuku_ninki_html(html)
        assert len(df) == 1
        assert df.iloc[0]["odds"] == pytest.approx(14.1)

    def test_comma_in_odds(self):
        """カンマ区切りのオッズ（例: 1,234.5）を正しくパースできる"""
        html = _make_sanrenpuku_ninki_html([(1, 2, 3, "1,234.5")])
        df = _parse_sanrenpuku_ninki_html(html)
        assert len(df) == 1
        assert df.iloc[0]["odds"] == pytest.approx(1234.5)

    def test_empty_html(self):
        """ninki-data_N が存在しないHTMLは空のDFを返す"""
        df = _parse_sanrenpuku_ninki_html("<html><body></body></html>")
        assert df.empty
        assert list(df.columns) == [
            "ticket_type", "horse_number_1", "horse_number_2", "horse_number_3", "odds"
        ]

    def test_multiple_rows(self):
        """複数行が全件パースされる"""
        combos = [(1, 2, 3, 10.0), (1, 2, 4, 15.0), (1, 3, 4, 20.0)]
        html = _make_sanrenpuku_ninki_html(combos)
        df = _parse_sanrenpuku_ninki_html(html)
        assert len(df) == 3


# ---------------------------------------------------------------------------


class TestSaveComboOddsToBq:
    def _make_sample_df(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                "ticket_type": "umaren",
                "horse_number_1": 1,
                "horse_number_2": 2,
                "horse_number_3": None,
                "odds": 5.5,
            },
            {
                "ticket_type": "sanrenpuku",
                "horse_number_1": 1,
                "horse_number_2": 2,
                "horse_number_3": 3,
                "odds": 12.5,
            },
        ])

    def test_raises_on_empty_df(self):
        with pytest.raises(ValueError, match="空"):
            save_combo_odds_to_bq(
                pd.DataFrame(),
                race_id="RACE001",
                race_date=datetime.date(2024, 5, 26),
                scraped_at=datetime.datetime(2024, 5, 26, 8, 0, 0),
                project_id="test-project",
            )

    def test_calls_bq_load_and_merge(self):
        df = self._make_sample_df()
        mock_client = MagicMock()
        mock_load_job = MagicMock()
        mock_merge_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_load_job
        mock_client.query.return_value = mock_merge_job

        with patch(
            "src.automation.data.netkeiba_scraper.bigquery.Client",
            return_value=mock_client,
        ):
            result = save_combo_odds_to_bq(
                df,
                race_id="RACE001",
                race_date=datetime.date(2024, 5, 26),
                scraped_at=datetime.datetime(2024, 5, 26, 8, 0, 0),
                project_id="test-project",
            )

        assert result == 2
        mock_client.load_table_from_dataframe.assert_called_once()
        mock_load_job.result.assert_called_once()
        mock_client.query.assert_called()


# ---------------------------------------------------------------------------
# netkeiba_to_jrdb_race_id (BQモック)
# ---------------------------------------------------------------------------


class TestNetkeibaToJrdbRaceId:
    def _make_bq_client(self, race_id: str | None):
        mock_client = MagicMock()
        if race_id:
            mock_row = MagicMock()
            mock_row.race_id = race_id
            mock_client.query.return_value.result.return_value = [mock_row]
        else:
            mock_client.query.return_value.result.return_value = []
        return mock_client

    def test_found(self):
        client = self._make_bq_client("2024050212110001")
        result = netkeiba_to_jrdb_race_id(
            race_date=datetime.date(2024, 5, 26),
            venue_code="05",
            race_number=11,
            bq_client=client,
            project_id="test-project",
        )
        assert result == "2024050212110001"

    def test_not_found(self):
        client = self._make_bq_client(None)
        result = netkeiba_to_jrdb_race_id(
            race_date=datetime.date(2024, 5, 26),
            venue_code="05",
            race_number=11,
            bq_client=client,
            project_id="test-project",
        )
        assert result is None

    def test_bq_error_returns_none(self):
        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("BQエラー")
        result = netkeiba_to_jrdb_race_id(
            race_date=datetime.date(2024, 5, 26),
            venue_code="05",
            race_number=11,
            bq_client=mock_client,
            project_id="test-project",
        )
        assert result is None


# ---------------------------------------------------------------------------
# scrape_today_odds (Playwright + BQをモック)
# ---------------------------------------------------------------------------


def _make_sample_odds_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"horse_number": 1, "win_odds": 2.5, "place_odds_min": 1.3, "place_odds_max": 1.8},
        {"horse_number": 2, "win_odds": 4.0, "place_odds_min": 1.9, "place_odds_max": 2.5},
        {"horse_number": 3, "win_odds": 8.0, "place_odds_min": 3.1, "place_odds_max": 4.2},
    ])


class TestScrapeTodayOdds:
    def test_returns_dataframe_with_expected_columns(self):
        """正常系: モックで全工程をバイパスしてDataFrameの構造を確認"""
        mock_races = [
            {"netkeiba_race_id": "202405021201", "venue_code": "05", "race_number": 1},
        ]
        mock_odds = _make_sample_odds_df()

        with patch(
            "src.automation.data.netkeiba_scraper.get_today_race_list",
            return_value=mock_races,
        ):
            with patch(
                "src.automation.data.netkeiba_scraper.get_win_place_odds",
                return_value=mock_odds,
            ):
                with patch(
                    "src.automation.data.netkeiba_scraper.netkeiba_to_jrdb_race_id",
                    return_value="RACE001",
                ):
                    with patch(
                        "src.automation.data.netkeiba_scraper.bigquery.Client"
                    ):
                        df = scrape_today_odds(
                            date=datetime.date(2024, 5, 26),
                            project_id="test-project",
                            sleep_sec=0,
                        )

        assert not df.empty
        for col in [
            "race_id", "race_date", "horse_number", "win_odds",
            "place_odds_min", "place_odds_max", "scraped_at",
        ]:
            assert col in df.columns, f"列 '{col}' が見つかりません"

        assert df["race_id"].iloc[0] == "RACE001"
        assert len(df) == 3

    def test_skips_race_when_jrdb_id_not_found(self):
        """JRDBのrace_idが見つからないレースはスキップされる"""
        mock_races = [
            {"netkeiba_race_id": "202405021201", "venue_code": "05", "race_number": 1},
        ]

        with patch(
            "src.automation.data.netkeiba_scraper.get_today_race_list",
            return_value=mock_races,
        ):
            with patch(
                "src.automation.data.netkeiba_scraper.netkeiba_to_jrdb_race_id",
                return_value=None,
            ):
                with patch(
                    "src.automation.data.netkeiba_scraper.bigquery.Client"
                ):
                    df = scrape_today_odds(
                        date=datetime.date(2024, 5, 26),
                        project_id="test-project",
                        sleep_sec=0,
                    )

        assert df.empty

    def test_empty_when_no_races(self):
        """レース一覧が空の場合は空のDataFrameを返す"""
        with patch(
            "src.automation.data.netkeiba_scraper.get_today_race_list",
            return_value=[],
        ):
            with patch(
                "src.automation.data.netkeiba_scraper.bigquery.Client"
            ):
                df = scrape_today_odds(
                    date=datetime.date(2024, 5, 26),
                    project_id="test-project",
                    sleep_sec=0,
                )
        assert df.empty


# ---------------------------------------------------------------------------
# save_odds_to_bq (BQモック)
# ---------------------------------------------------------------------------


class TestSaveOddsToBq:
    def _make_sample_df(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                "race_id": "RACE001",
                "race_date": datetime.date(2024, 5, 26),
                "horse_number": 1,
                "win_odds": 2.5,
                "place_odds_min": 1.3,
                "place_odds_max": 1.8,
                "scraped_at": datetime.datetime(2024, 5, 26, 8, 0, 0),
            },
        ])

    def test_raises_on_empty_df(self):
        with pytest.raises(ValueError, match="空"):
            save_odds_to_bq(pd.DataFrame(), project_id="test-project")

    def test_calls_bq_load_and_merge(self):
        df = self._make_sample_df()
        mock_client = MagicMock()
        mock_load_job = MagicMock()
        mock_merge_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_load_job
        mock_client.query.return_value = mock_merge_job

        with patch(
            "src.automation.data.netkeiba_scraper.bigquery.Client",
            return_value=mock_client,
        ):
            result = save_odds_to_bq(df, project_id="test-project")

        assert result == 1
        mock_client.load_table_from_dataframe.assert_called_once()
        mock_load_job.result.assert_called_once()
        mock_client.query.assert_called()


# ---------------------------------------------------------------------------
# Issue #184: Playwright タイムアウト修正 (_create_page_with_route_block / domcontentloaded)
# ---------------------------------------------------------------------------


class TestCreatePageWithRouteBlock:
    """_create_page_with_route_block のルートブロック動作を検証する"""

    def _make_route(self, resource_type: str):
        """指定 resource_type を返すモックルートを生成する"""
        route = MagicMock()
        route.request.resource_type = resource_type
        return route

    def test_route_aborts_image_resource(self):
        """画像リソースは abort() されること"""
        mock_browser = MagicMock()
        mock_page = MagicMock()
        mock_browser.new_page.return_value = mock_page

        captured_handler = []

        def capture_route(pattern, handler):
            captured_handler.append(handler)

        mock_page.route.side_effect = capture_route

        _create_page_with_route_block(mock_browser)

        assert len(captured_handler) == 1
        route = self._make_route("image")
        captured_handler[0](route)
        route.abort.assert_called_once()
        route.continue_.assert_not_called()

    def test_route_continues_document_resource(self):
        """ドキュメントリソースは continue_() されること"""
        mock_browser = MagicMock()
        mock_page = MagicMock()
        mock_browser.new_page.return_value = mock_page

        captured_handler = []

        def capture_route(pattern, handler):
            captured_handler.append(handler)

        mock_page.route.side_effect = capture_route

        _create_page_with_route_block(mock_browser)

        route = self._make_route("document")
        captured_handler[0](route)
        route.continue_.assert_called_once()
        route.abort.assert_not_called()

    def test_blocked_resource_types_contains_expected(self):
        """BLOCKED_RESOURCE_TYPES に image/font/media/stylesheet が含まれること"""
        assert "image" in BLOCKED_RESOURCE_TYPES
        assert "font" in BLOCKED_RESOURCE_TYPES
        assert "media" in BLOCKED_RESOURCE_TYPES
        assert "stylesheet" in BLOCKED_RESOURCE_TYPES


class TestPlaywrightDomcontentloaded:
    """各 get_* 関数が wait_until="domcontentloaded" を使用することを検証する"""

    def _make_playwright_mock(self, html=""):
        """sync_playwright コンテキストマネージャのモックを構築する"""
        mock_page = MagicMock()
        mock_page.content.return_value = html
        mock_browser = MagicMock()
        mock_browser.new_page.return_value = mock_page
        mock_p = MagicMock()
        mock_p.chromium.launch.return_value = mock_browser

        mock_playwright_cm = MagicMock()
        mock_playwright_cm.__enter__ = MagicMock(return_value=mock_p)
        mock_playwright_cm.__exit__ = MagicMock(return_value=False)
        return mock_playwright_cm, mock_browser, mock_page

    def test_uses_domcontentloaded_in_race_list(self):
        """get_today_race_list が wait_until="domcontentloaded" で page.goto を呼ぶこと"""
        mock_cm, mock_browser, mock_page = self._make_playwright_mock()

        with patch("playwright.sync_api.sync_playwright", return_value=mock_cm):
            get_today_race_list(datetime.date(2024, 5, 26), sleep_sec=0)

        mock_page.goto.assert_called_once()
        _, kwargs = mock_page.goto.call_args
        assert kwargs.get("wait_until") == "domcontentloaded"

    def test_uses_domcontentloaded_in_win_place_odds(self):
        """get_win_place_odds が wait_until="domcontentloaded" で page.goto を呼ぶこと"""
        mock_cm, mock_browser, mock_page = self._make_playwright_mock()

        with patch("playwright.sync_api.sync_playwright", return_value=mock_cm):
            get_win_place_odds("202405021211", sleep_sec=0)

        mock_page.goto.assert_called_once()
        _, kwargs = mock_page.goto.call_args
        assert kwargs.get("wait_until") == "domcontentloaded"

    def test_uses_domcontentloaded_in_combo_odds(self):
        """get_combo_odds が wait_until="domcontentloaded" で page.goto を呼ぶこと"""
        mock_cm, mock_browser, mock_page = self._make_playwright_mock()

        with patch("playwright.sync_api.sync_playwright", return_value=mock_cm):
            get_combo_odds("202405021211", ticket_types=["b4"], sleep_sec=0)

        mock_page.goto.assert_called()
        _, kwargs = mock_page.goto.call_args
        assert kwargs.get("wait_until") == "domcontentloaded"

    def test_sanrenpuku_uses_housiki_c99_url(self):
        """三連複（b7）は housiki=c99 パラメータ付きURLを使うこと"""
        mock_cm, mock_browser, mock_page = self._make_playwright_mock()
        visited_urls: list[str] = []

        def capture_goto(url, **kwargs):
            visited_urls.append(url)

        mock_page.goto.side_effect = capture_goto

        with patch("playwright.sync_api.sync_playwright", return_value=mock_cm):
            get_combo_odds("202405021211", ticket_types=["b7"], sleep_sec=0)

        assert len(visited_urls) == 1
        assert "housiki=c99" in visited_urls[0]
        assert "type=b7" in visited_urls[0]

    def test_umaren_wide_do_not_use_housiki(self):
        """馬連（b4）・ワイド（b5）は housiki=c99 を使わないこと"""
        mock_cm, mock_browser, mock_page = self._make_playwright_mock()
        visited_urls: list[str] = []

        def capture_goto(url, **kwargs):
            visited_urls.append(url)

        mock_page.goto.side_effect = capture_goto

        with patch("playwright.sync_api.sync_playwright", return_value=mock_cm):
            get_combo_odds("202405021211", ticket_types=["b4", "b5"], sleep_sec=0)

        for url in visited_urls:
            assert "housiki" not in url


# ---------------------------------------------------------------------------
# jrdb_race_id_to_netkeiba
# ---------------------------------------------------------------------------


class TestJrdbRaceIdToNetkeiba:
    """JRDB race_id → netkeiba race_id 変換の検証"""

    def test_basic_conversion(self):
        """東京1回4日11R（2026年）のJRDB race_idをnetkeiba形式に変換できること"""
        # JRDB: venue=06, year=26, kai=1, day=4, race=11 → "06261411"
        result = jrdb_race_id_to_netkeiba("06261411")
        assert result == "202606010411"

    def test_hex_kai_and_day(self):
        """回次・日次が10以上（16進数）の場合も正しく変換できること"""
        # venue=05, year=25→2025, kai='a'=10, day='b'=11, race=01
        # 期待値: "2025" + "05" + "10" + "11" + "01" = "202505101101"
        result = jrdb_race_id_to_netkeiba("0525ab01")
        assert result == "202505101101"

    def test_hex_conversion_correct(self):
        """16進数の回次・日次が正しく10進数2桁ゼロ埋めになること"""
        # kai=a=10, day=b=11
        result = jrdb_race_id_to_netkeiba("0525ab01")
        assert result[4:6] == "05"   # venue_code
        assert result[6:8] == "10"   # kai=10
        assert result[8:10] == "11"  # day=11
        assert result[10:12] == "01" # race_number

    def test_invalid_length_raises(self):
        """8文字でないJRDB race_idはValueErrorを送出すること"""
        with pytest.raises(ValueError):
            jrdb_race_id_to_netkeiba("0626141")  # 7文字

    def test_round_trip_components(self):
        """変換後のnetkeiba race_idをparse_netkeiba_race_idで逆解析すると元の情報に一致すること"""
        jrdb_id = "06261411"
        netkeiba_id = jrdb_race_id_to_netkeiba(jrdb_id)
        parsed = parse_netkeiba_race_id(netkeiba_id)

        assert parsed["year"] == "2026"
        assert parsed["venue_code"] == "06"
        assert parsed["kai"] == "01"
        assert parsed["nichi"] == "04"
        assert parsed["race_number"] == 11

    def test_output_is_12_digits(self):
        """変換結果は必ず12桁の文字列であること"""
        result = jrdb_race_id_to_netkeiba("06261411")
        assert len(result) == 12
        assert result.isdigit()


# ---------------------------------------------------------------------------
# scrape_odds_for_race
# ---------------------------------------------------------------------------


class TestScrapeOddsForRace:
    """scrape_odds_for_race() の動作検証"""

    RACE_DATE = datetime.date(2026, 4, 12)
    JRDB_RACE_ID = "06261411"
    PROJECT_ID = "test-project"

    def _make_win_place_df(self) -> pd.DataFrame:
        return pd.DataFrame([
            {"horse_number": 1, "win_odds": 3.5, "place_odds_min": 1.5, "place_odds_max": 2.0},
            {"horse_number": 2, "win_odds": 5.0, "place_odds_min": 2.0, "place_odds_max": 3.0},
        ])

    def _make_combo_df(self) -> pd.DataFrame:
        return pd.DataFrame([
            {"ticket_type": "umaren", "horse_number_1": 1, "horse_number_2": 2, "horse_number_3": None, "odds": 10.0},
        ])

    def test_returns_true_on_success(self):
        """単複・組み合わせオッズの取得・保存が成功した場合 True を返すこと"""
        with (
            patch(
                "src.automation.data.netkeiba_scraper.get_win_place_odds",
                return_value=self._make_win_place_df(),
            ),
            patch(
                "src.automation.data.netkeiba_scraper.save_odds_to_bq",
                return_value=2,
            ),
            patch(
                "src.automation.data.netkeiba_scraper.get_combo_odds",
                return_value=self._make_combo_df(),
            ),
            patch(
                "src.automation.data.netkeiba_scraper.save_combo_odds_to_bq",
                return_value=1,
            ),
        ):
            result = scrape_odds_for_race(
                self.JRDB_RACE_ID, self.RACE_DATE, self.PROJECT_ID, sleep_sec=0
            )

        assert result is True

    def test_returns_false_when_win_place_empty(self):
        """単複オッズが空の場合 False を返すこと"""
        with patch(
            "src.automation.data.netkeiba_scraper.get_win_place_odds",
            return_value=pd.DataFrame(),
        ):
            result = scrape_odds_for_race(
                self.JRDB_RACE_ID, self.RACE_DATE, self.PROJECT_ID, sleep_sec=0
            )

        assert result is False

    def test_returns_false_when_bq_save_fails(self):
        """単複オッズのBQ保存が失敗した場合 False を返すこと"""
        with (
            patch(
                "src.automation.data.netkeiba_scraper.get_win_place_odds",
                return_value=self._make_win_place_df(),
            ),
            patch(
                "src.automation.data.netkeiba_scraper.save_odds_to_bq",
                side_effect=RuntimeError("BQ error"),
            ),
        ):
            result = scrape_odds_for_race(
                self.JRDB_RACE_ID, self.RACE_DATE, self.PROJECT_ID, sleep_sec=0
            )

        assert result is False

    def test_combo_failure_does_not_affect_return(self):
        """組み合わせオッズの取得失敗は True を返す（フォールバック）こと"""
        with (
            patch(
                "src.automation.data.netkeiba_scraper.get_win_place_odds",
                return_value=self._make_win_place_df(),
            ),
            patch(
                "src.automation.data.netkeiba_scraper.save_odds_to_bq",
                return_value=2,
            ),
            patch(
                "src.automation.data.netkeiba_scraper.get_combo_odds",
                side_effect=RuntimeError("combo error"),
            ),
        ):
            result = scrape_odds_for_race(
                self.JRDB_RACE_ID, self.RACE_DATE, self.PROJECT_ID, sleep_sec=0
            )

        assert result is True

    def test_default_ticket_types_are_b4_b5_b7(self):
        """ticket_types を省略した場合 b4/b5/b7 が使われること"""
        called_with: list[list] = []

        def fake_combo(netkeiba_race_id, ticket_types=None, sleep_sec=1.5):
            called_with.append(ticket_types)
            return pd.DataFrame()

        with (
            patch(
                "src.automation.data.netkeiba_scraper.get_win_place_odds",
                return_value=self._make_win_place_df(),
            ),
            patch(
                "src.automation.data.netkeiba_scraper.save_odds_to_bq",
                return_value=2,
            ),
            patch(
                "src.automation.data.netkeiba_scraper.get_combo_odds",
                side_effect=fake_combo,
            ),
        ):
            scrape_odds_for_race(
                self.JRDB_RACE_ID, self.RACE_DATE, self.PROJECT_ID, sleep_sec=0
            )

        assert called_with == [["b4", "b5", "b7"]]
