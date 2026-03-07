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
    _parse_race_list_html,
    _parse_win_place_odds_html,
    netkeiba_to_jrdb_race_id,
    parse_netkeiba_race_id,
    save_odds_to_bq,
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
    """単複オッズテーブルを含むHTMLを生成する（netkeibaのDOM構造を模倣）"""
    win_rows_html = "".join(
        f"<tr><td>{h}</td><td>馬名{h}</td><td>{o}</td></tr>"
        for h, o in win_rows
    )
    place_rows_html = "".join(
        f"<tr><td>{h}</td><td>馬名{h}</td><td>{lo}〜{hi}</td></tr>"
        for h, lo, hi in place_rows
    )
    return (
        "<html><body>"
        '<table id="odds_tan_block">'
        "<thead><tr><th>馬番</th><th>馬名</th><th>単勝オッズ</th></tr></thead>"
        f"<tbody>{win_rows_html}</tbody>"
        "</table>"
        '<table id="odds_fuku_block">'
        "<thead><tr><th>馬番</th><th>馬名</th><th>複勝オッズ</th></tr></thead>"
        f"<tbody>{place_rows_html}</tbody>"
        "</table>"
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
            '<table id="odds_tan_block">'
            "<thead><tr><th>馬番</th><th>馬名</th><th>単勝オッズ</th></tr></thead>"
            "<tbody><tr><td>1</td><td>馬名1</td><td>2.5</td></tr></tbody>"
            "</table>"
            "</body></html>"
        )
        df = _parse_win_place_odds_html(html)
        assert not df.empty
        assert df.iloc[0]["win_odds"] == pytest.approx(2.5, abs=0.01)


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
