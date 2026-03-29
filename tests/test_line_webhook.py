"""
LINE Webhook ハンドラーのユニットテスト

テスト対象:
  - verify_line_signature: 署名検証
  - parse_race_query: メッセージパース
  - _format_prediction_table: 予測テーブルフォーマット
  - _format_bet_recommendations: 推奨馬券フォーマット
  - handle_race_query: BQ データ取得 + 返答生成（BigQuery をモック）
  - process_webhook_events: Webhook イベント処理（LINE API をモック）
"""

from __future__ import annotations

import hashlib
import hmac
import json
from base64 import b64encode
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.automation.api.line_webhook import (
    VENUE_NAME_TO_CODE,
    _format_bet_recommendations,
    _format_prediction_table,
    _format_single_bet_line,
    format_push_notification,
    handle_race_query,
    parse_race_query,
    process_webhook_events,
    verify_line_signature,
)


# ---------------------------------------------------------------------------
# フィクスチャ
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_merged_df() -> pd.DataFrame:
    return pd.DataFrame({
        "horse_number": [12, 13, 11, 1, 10],
        "horse_name": ["ニューオーリンズ", "チュウワチーフ", "ペイドラロワール", "ストーンズ", "リメンバーヒム"],
        "win_place_prob": [0.520, 0.512, 0.400, 0.360, 0.327],
        "pred_score": [0.6762, 0.6613, 0.4155, 0.3089, 0.2145],
        "rank_in_race": [1, 2, 3, 4, 5],
        "place_odds": [2.5, 3.0, 4.2, 5.1, 6.0],
        "odds": [2.5, 3.0, 4.2, 5.1, 6.0],
        "horse_id": ["h12", "h13", "h11", "h01", "h10"],
    })


@pytest.fixture
def sample_bets() -> list[dict]:
    return [
        {
            "bet_type": "place",
            "horse_numbers": [12],
            "horse_id": "h12",
            "odds": 2.5,
            "bet_amount": 2000.0,
        },
        {
            "bet_type": "place",
            "horse_numbers": [13],
            "horse_id": "h13",
            "odds": 3.0,
            "bet_amount": 1500.0,
        },
    ]


# ---------------------------------------------------------------------------
# verify_line_signature
# ---------------------------------------------------------------------------

class TestVerifyLineSignature:
    def test_valid_signature(self):
        secret = "test_secret"
        body = b'{"events":[]}'
        hash_val = hmac.new(secret.encode(), body, hashlib.sha256).digest()
        signature = b64encode(hash_val).decode()
        assert verify_line_signature(secret, body, signature) is True

    def test_invalid_signature(self):
        secret = "test_secret"
        body = b'{"events":[]}'
        assert verify_line_signature(secret, body, "invalid_sig") is False

    def test_empty_body(self):
        secret = "test_secret"
        body = b""
        hash_val = hmac.new(secret.encode(), body, hashlib.sha256).digest()
        signature = b64encode(hash_val).decode()
        assert verify_line_signature(secret, body, signature) is True

    def test_wrong_secret(self):
        body = b'{"events":[]}'
        hash_val = hmac.new(b"correct", body, hashlib.sha256).digest()
        signature = b64encode(hash_val).decode()
        assert verify_line_signature("wrong", body, signature) is False


# ---------------------------------------------------------------------------
# parse_race_query
# ---------------------------------------------------------------------------

class TestParseRaceQuery:
    def test_full_date_format(self):
        result = parse_race_query("2026-03-08 阪神 12R")
        assert result is not None
        assert result["race_date"] == date(2026, 3, 8)
        assert result["venue_code"] == "09"
        assert result["venue_name"] == "阪神"
        assert result["race_number"] == 12

    def test_slash_date_format(self):
        result = parse_race_query("2026/03/08 東京 11R")
        assert result is not None
        assert result["race_date"] == date(2026, 3, 8)
        assert result["venue_code"] == "05"
        assert result["race_number"] == 11

    def test_short_date_format(self):
        result = parse_race_query("03/08 阪神 12R")
        assert result is not None
        assert result["race_date"].month == 3
        assert result["race_date"].day == 8

    def test_no_r_suffix(self):
        result = parse_race_query("2026-03-08 中山 5")
        assert result is not None
        assert result["race_number"] == 5

    def test_all_venues(self):
        venues = ["札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉"]
        for venue in venues:
            result = parse_race_query(f"2026-03-08 {venue} 1R")
            assert result is not None, f"{venue} のパースに失敗"
            assert result["venue_name"] == venue

    def test_no_venue_returns_none(self):
        assert parse_race_query("2026-03-08 1R") is None

    def test_no_race_number_returns_none(self):
        assert parse_race_query("2026-03-08 阪神") is None

    def test_no_date_returns_none(self):
        assert parse_race_query("阪神 12R") is None

    def test_free_text_returns_none(self):
        assert parse_race_query("今日の推奨を教えて") is None

    def test_help_text_returns_none(self):
        assert parse_race_query("ヘルプ") is None


# ---------------------------------------------------------------------------
# _format_prediction_table
# ---------------------------------------------------------------------------

class TestFormatPredictionTable:
    def test_contains_header(self, sample_merged_df):
        result = _format_prediction_table(date(2026, 3, 8), "阪神", 12, sample_merged_df)
        assert "阪神 12R" in result
        assert "2026-03-08" in result

    def test_contains_columns_header(self, sample_merged_df):
        result = _format_prediction_table(date(2026, 3, 8), "阪神", 12, sample_merged_df)
        assert "複勝率" in result
        assert "オッズ" in result
        assert "期待値" in result

    def test_contains_horse_names(self, sample_merged_df):
        result = _format_prediction_table(date(2026, 3, 8), "阪神", 12, sample_merged_df)
        assert "ニューオーリンズ" in result
        assert "チュウワチーフ" in result

    def test_sorted_by_rank(self, sample_merged_df):
        result = _format_prediction_table(date(2026, 3, 8), "阪神", 12, sample_merged_df)
        lines = result.split("\n")
        data_lines = [l for l in lines if l.strip() and l.strip()[0].isdigit()]
        ranks = [int(l.split()[0]) for l in data_lines if l.split()[0].isdigit()]
        assert ranks == sorted(ranks)

    def test_missing_odds_shows_dash(self, sample_merged_df):
        df = sample_merged_df.copy()
        df["place_odds"] = None
        result = _format_prediction_table(date(2026, 3, 8), "阪神", 12, df)
        assert "-" in result


# ---------------------------------------------------------------------------
# _format_bet_recommendations
# ---------------------------------------------------------------------------

class TestFormatBetRecommendations:
    def test_contains_title(self, sample_merged_df, sample_bets):
        result = _format_bet_recommendations(
            date(2026, 3, 8), "阪神", 12, sample_bets, "standard", sample_merged_df
        )
        assert "阪神 12R" in result
        assert "推奨馬券" in result

    def test_contains_pattern_label(self, sample_merged_df, sample_bets):
        result = _format_bet_recommendations(
            date(2026, 3, 8), "阪神", 12, sample_bets, "one_dominant", sample_merged_df
        )
        assert "突出型" in result

    def test_contains_bet_type_label(self, sample_merged_df, sample_bets):
        result = _format_bet_recommendations(
            date(2026, 3, 8), "阪神", 12, sample_bets, "standard", sample_merged_df
        )
        assert "複勝" in result

    def test_contains_total_bet(self, sample_merged_df, sample_bets):
        result = _format_bet_recommendations(
            date(2026, 3, 8), "阪神", 12, sample_bets, "standard", sample_merged_df
        )
        assert "合計投資額" in result
        assert "3,500" in result  # 2000 + 1500

    def test_empty_bets(self, sample_merged_df):
        result = _format_bet_recommendations(
            date(2026, 3, 8), "阪神", 12, [], "standard", sample_merged_df
        )
        assert "推奨馬券なし" in result

    def test_horse_name_in_bet(self, sample_merged_df, sample_bets):
        result = _format_bet_recommendations(
            date(2026, 3, 8), "阪神", 12, sample_bets, "standard", sample_merged_df
        )
        assert "ニューオーリンズ" in result


# ---------------------------------------------------------------------------
# handle_race_query (BigQuery をモック)
# ---------------------------------------------------------------------------

class TestHandleRaceQuery:
    def _make_pred_df(self):
        return pd.DataFrame({
            "horse_number": [12, 13, 11, 1, 10],
            "horse_name": ["ニューオーリンズ", "チュウワチーフ", "ペイドラロワール", "ストーンズ", "リメンバーヒム"],
            "win_place_prob": [0.520, 0.512, 0.400, 0.360, 0.327],
            "pred_score": [0.6762, 0.6613, 0.4155, 0.3089, 0.2145],
            "rank_in_race": [1, 2, 3, 4, 5],
        })

    def _make_odds_df(self):
        return pd.DataFrame({
            "horse_number": [12, 13, 11, 1, 10],
            "win_odds": [3.5, 4.0, 7.0, 9.0, 12.0],
            "place_odds": [2.5, 3.0, 4.2, 5.1, 6.0],
        })

    @patch("src.automation.api.line_webhook._fetch_race_predictions")
    @patch("src.automation.api.line_webhook._fetch_race_odds")
    @patch("src.automation.api.line_webhook._fetch_race_combo_odds")
    def test_returns_two_messages(self, mock_combo, mock_odds, mock_pred):
        mock_pred.return_value = self._make_pred_df()
        mock_odds.return_value = self._make_odds_df()
        mock_combo.return_value = pd.DataFrame()

        result = handle_race_query("2026-03-08 阪神 12R", "test_project")

        assert result is not None
        assert len(result) == 2
        assert result[0]["type"] == "text"
        assert result[1]["type"] == "text"

    @patch("src.automation.api.line_webhook._fetch_race_predictions")
    @patch("src.automation.api.line_webhook._fetch_race_odds")
    @patch("src.automation.api.line_webhook._fetch_race_combo_odds")
    def test_message1_contains_odds_and_ev(self, mock_combo, mock_odds, mock_pred):
        mock_pred.return_value = self._make_pred_df()
        mock_odds.return_value = self._make_odds_df()
        mock_combo.return_value = pd.DataFrame()

        result = handle_race_query("2026-03-08 阪神 12R", "test_project")

        msg1 = result[0]["text"]
        assert "オッズ" in msg1
        assert "期待値" in msg1

    @patch("src.automation.api.line_webhook._fetch_race_predictions")
    @patch("src.automation.api.line_webhook._fetch_race_odds")
    @patch("src.automation.api.line_webhook._fetch_race_combo_odds")
    def test_message2_contains_recommendation(self, mock_combo, mock_odds, mock_pred):
        mock_pred.return_value = self._make_pred_df()
        mock_odds.return_value = self._make_odds_df()
        mock_combo.return_value = pd.DataFrame()

        result = handle_race_query("2026-03-08 阪神 12R", "test_project")

        msg2 = result[1]["text"]
        assert "推奨馬券" in msg2
        assert "合計投資額" in msg2

    @patch("src.automation.api.line_webhook._fetch_race_predictions")
    @patch("src.automation.api.line_webhook._fetch_race_odds")
    @patch("src.automation.api.line_webhook._fetch_race_combo_odds")
    def test_empty_predictions_returns_error_message(self, mock_combo, mock_odds, mock_pred):
        mock_pred.return_value = pd.DataFrame()
        mock_odds.return_value = pd.DataFrame()
        mock_combo.return_value = pd.DataFrame()

        result = handle_race_query("2026-03-08 阪神 12R", "test_project")

        assert result is not None
        assert len(result) == 1
        assert "見つかりません" in result[0]["text"]

    def test_non_race_query_returns_none(self):
        result = handle_race_query("今日の推奨を教えて", "test_project")
        assert result is None

    def test_free_text_returns_none(self):
        result = handle_race_query("ヘルプ", "test_project")
        assert result is None


# ---------------------------------------------------------------------------
# process_webhook_events (LINE API をモック)
# ---------------------------------------------------------------------------

class TestProcessWebhookEvents:
    def _make_event(self, text: str) -> dict:
        return {
            "type": "message",
            "replyToken": "test_reply_token",
            "message": {"type": "text", "text": text},
        }

    @patch("src.automation.api.line_webhook._fetch_race_predictions")
    @patch("src.automation.api.line_webhook._fetch_race_odds")
    @patch("src.automation.api.line_webhook._fetch_race_combo_odds")
    @patch("src.utils.line_notify.requests.post")
    def test_race_query_triggers_reply(self, mock_post, mock_combo, mock_odds, mock_pred):
        mock_pred.return_value = pd.DataFrame({
            "horse_number": [1, 2, 3],
            "horse_name": ["A", "B", "C"],
            "win_place_prob": [0.5, 0.4, 0.3],
            "pred_score": [0.5, 0.4, 0.3],
            "rank_in_race": [1, 2, 3],
        })
        mock_odds.return_value = pd.DataFrame({
            "horse_number": [1, 2, 3],
            "win_odds": [2.0, 3.0, 5.0],
            "place_odds": [1.5, 2.0, 3.0],
        })
        mock_combo.return_value = pd.DataFrame()
        mock_post.return_value = MagicMock(status_code=200)

        events = [self._make_event("2026-03-08 阪神 12R")]
        process_webhook_events(events, "test_project", "test_token")

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        payload = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs[0][1]
        assert payload["replyToken"] == "test_reply_token"
        assert len(payload["messages"]) == 2

    @patch("src.utils.line_notify.requests.post")
    def test_non_race_message_no_reply(self, mock_post):
        events = [self._make_event("こんにちは")]
        process_webhook_events(events, "test_project", "test_token")
        mock_post.assert_not_called()

    @patch("src.utils.line_notify.requests.post")
    def test_non_message_event_ignored(self, mock_post):
        events = [{"type": "follow", "replyToken": "token"}]
        process_webhook_events(events, "test_project", "test_token")
        mock_post.assert_not_called()

    @patch("src.utils.line_notify.requests.post")
    def test_non_text_message_ignored(self, mock_post):
        events = [{
            "type": "message",
            "replyToken": "token",
            "message": {"type": "image", "id": "123"},
        }]
        process_webhook_events(events, "test_project", "test_token")
        mock_post.assert_not_called()


# ---------------------------------------------------------------------------
# _format_single_bet_line
# ---------------------------------------------------------------------------

class TestFormatSingleBetLine:
    def _make_place_bet(self) -> dict:
        return {
            "bet_type": "place",
            "horse_numbers": "3",
            "horse_names": "テストホース",
            "win_place_prob": 0.25,
            "place_odds": 4.5,
            "expected_return": 1.125,
            "bet_amount": 1000.0,
        }

    def _make_win_bet(self) -> dict:
        return {
            "bet_type": "win",
            "horse_numbers": "6",
            "horse_names": "オールザワールド",
            "win_place_prob": 0.733,
            "place_odds": 5.8,
            "expected_return": None,  # 単勝は expected_return なし
            "bet_amount": 1000.0,
        }

    def _make_wide_bet(self) -> dict:
        return {
            "bet_type": "wide",
            "horse_numbers": "1,3",
            "horse_names": "ホースA,テストホース",
            "win_place_prob": None,
            "place_odds": 8.2,
            "expected_return": None,
            "bet_amount": 500.0,
        }

    def _make_sanrenpuku_bet(self) -> dict:
        return {
            "bet_type": "sanrenpuku",
            "horse_numbers": "1,3,7",
            "horse_names": "ホースA,テストホース,ホースC",
            "win_place_prob": None,
            "place_odds": 25.0,
            "expected_return": None,
            "bet_amount": 300.0,
        }

    def test_place_shows_prob_and_ev(self):
        result = _format_single_bet_line(self._make_place_bet())
        assert "複勝率" in result
        assert "25.0%" in result
        assert "期待値" in result
        assert "1.12" in result  # 1.125 → 1.12 (:.2f truncates)

    def test_place_shows_horse_name(self):
        result = _format_single_bet_line(self._make_place_bet())
        assert "テストホース" in result
        assert "馬番3" in result

    def test_wide_no_prob_no_ev(self):
        result = _format_single_bet_line(self._make_wide_bet())
        assert "複勝率" not in result
        assert "期待値" not in result

    def test_wide_shows_both_horses(self):
        result = _format_single_bet_line(self._make_wide_bet())
        assert "馬番1" in result
        assert "馬番3" in result
        assert "ホースA" in result
        assert "テストホース" in result

    def test_sanrenpuku_shows_three_horses(self):
        result = _format_single_bet_line(self._make_sanrenpuku_bet())
        assert "馬番1" in result
        assert "馬番3" in result
        assert "馬番7" in result

    def test_place_shows_bet_amount(self):
        result = _format_single_bet_line(self._make_place_bet())
        assert "1,000" in result

    def test_wide_shows_bet_amount(self):
        result = _format_single_bet_line(self._make_wide_bet())
        assert "500" in result

    def test_win_shows_place_prob_as_reference(self):
        """単勝は「複勝率(参考)」ラベルで確率を表示する"""
        result = _format_single_bet_line(self._make_win_bet())
        assert "複勝率(参考)" in result
        assert "73.3%" in result

    def test_win_does_not_show_expected_return(self):
        """単勝は期待値を表示しない（win_probがないため計算不可）"""
        result = _format_single_bet_line(self._make_win_bet())
        assert "期待値" not in result

    def test_win_shows_odds_and_amount(self):
        """単勝はオッズと推奨額を表示する"""
        result = _format_single_bet_line(self._make_win_bet())
        assert "5.8倍" in result
        assert "1,000" in result

    def test_place_still_shows_expected_return(self):
        """複勝は引き続き期待値を表示する（回帰テスト）"""
        result = _format_single_bet_line(self._make_place_bet())
        assert "期待値" in result
        assert "複勝率(参考)" not in result  # 複勝は "(参考)" なし
        assert "複勝率:" in result


# ---------------------------------------------------------------------------
# format_push_notification
# ---------------------------------------------------------------------------

class TestFormatPushNotification:
    def _make_decisions(self) -> list[dict]:
        return [
            {
                "venue_code": "09",
                "race_number": 5,
                "race_pattern": "standard",
                "horse_numbers": "3",
                "horse_names": "テストホース",
                "bet_type": "place",
                "bet_amount": 1000.0,
                "win_place_prob": 0.25,
                "place_odds": 4.5,
                "expected_return": 1.125,
                            },
            {
                "venue_code": "09",
                "race_number": 5,
                "race_pattern": "standard",
                "horse_numbers": "1,3",
                "horse_names": "ホースA,テストホース",
                "bet_type": "wide",
                "bet_amount": 500.0,
                "win_place_prob": None,
                "place_odds": 8.2,
                "expected_return": None,
                            },
            {
                "venue_code": "06",
                "race_number": 3,
                "race_pattern": "one_dominant",
                "horse_numbers": "7",
                "horse_names": "エースホース",
                "bet_type": "win",
                "bet_amount": 2000.0,
                "win_place_prob": 0.40,
                "place_odds": 3.0,
                "expected_return": 1.20,
                            },
        ]

    def test_returns_at_least_two_messages(self):
        result = format_push_notification(date(2026, 3, 16), self._make_decisions())
        assert len(result) >= 2

    def test_header_contains_date(self):
        result = format_push_notification(date(2026, 3, 16), self._make_decisions())
        assert "2026-03-16" in result[0]["text"]

    def test_header_contains_total_bet(self):
        result = format_push_notification(date(2026, 3, 16), self._make_decisions())
        # 1000 + 500 + 2000 = 3500
        assert "3,500" in result[0]["text"]

    def test_contains_race_label(self):
        full_text = "\n".join(m["text"] for m in format_push_notification(date(2026, 3, 16), self._make_decisions()))
        assert "阪神 5R" in full_text
        assert "中山 3R" in full_text

    def test_place_bet_shows_prob(self):
        full_text = "\n".join(m["text"] for m in format_push_notification(date(2026, 3, 16), self._make_decisions()))
        assert "複勝率" in full_text
        assert "25.0%" in full_text

    def test_wide_bet_no_prob(self):
        full_text = "\n".join(m["text"] for m in format_push_notification(date(2026, 3, 16), self._make_decisions()))
        # ワイドのブロックには複勝率行が1つしかない（複勝のもの）
        assert "ワイド" in full_text
        # ワイドの行には「複勝率」が含まれないはず
        for msg in format_push_notification(date(2026, 3, 16), self._make_decisions()):
            for line in msg["text"].split("\n"):
                if "ワイド" in line:
                    assert "複勝率" not in line

    def test_empty_decisions_returns_no_recommendation(self):
        result = format_push_notification(date(2026, 3, 16), [])
        assert len(result) == 1
        assert "推奨馬券はありません" in result[0]["text"]

    def test_pattern_labels_shown(self):
        full_text = "\n".join(m["text"] for m in format_push_notification(date(2026, 3, 16), self._make_decisions()))
        assert "標準型" in full_text
        assert "突出型" in full_text

    def test_races_sorted_by_venue_then_race_number(self):
        """競馬場コード順 → レース番号順でソートされる"""
        decisions = [
            {"venue_code": "09", "race_number": 10, "race_pattern": "standard",
             "horse_numbers": "1", "horse_names": "A", "bet_type": "place",
             "bet_amount": 100.0, "win_place_prob": 0.3, "place_odds": 3.0, "expected_return": 0.9},
            {"venue_code": "06", "race_number": 5, "race_pattern": "standard",
             "horse_numbers": "2", "horse_names": "B", "bet_type": "place",
             "bet_amount": 100.0, "win_place_prob": 0.3, "place_odds": 3.0, "expected_return": 0.9},
            {"venue_code": "06", "race_number": 2, "race_pattern": "standard",
             "horse_numbers": "3", "horse_names": "C", "bet_type": "place",
             "bet_amount": 100.0, "win_place_prob": 0.3, "place_odds": 3.0, "expected_return": 0.9},
        ]
        full_text = "\n".join(m["text"] for m in format_push_notification(date(2026, 3, 16), decisions))
        # 06（中山）2R → 06 5R → 09（阪神）10R の順
        pos_06_2 = full_text.index("中山 2R")
        pos_06_5 = full_text.index("中山 5R")
        pos_09_10 = full_text.index("阪神 10R")
        assert pos_06_2 < pos_06_5 < pos_09_10

    def _make_many_decisions(self, n_races: int) -> list[dict]:
        """n_races レース分のテストデータを生成する"""
        decisions = []
        for i in range(1, n_races + 1):
            decisions.append({
                "venue_code": "09",
                "race_number": i,
                "race_pattern": "standard",
                "horse_numbers": "1",
                "horse_names": "テストホース",
                "bet_type": "place",
                "bet_amount": 1000.0,
                "win_place_prob": 0.3,
                "place_odds": 3.5,
                "expected_return": 1.05,
            })
        return decisions

    def test_10_races_fit_in_one_content_message(self):
        """10レースは1通のコンテンツメッセージに収まる（ヘッダー含め2件）"""
        result = format_push_notification(date(2026, 3, 16), self._make_many_decisions(10))
        assert len(result) == 2  # header + 1 content

    def test_11_races_split_into_two_content_messages(self):
        """11レースは2通のコンテンツメッセージに分割される（ヘッダー含め3件）"""
        result = format_push_notification(date(2026, 3, 16), self._make_many_decisions(11))
        assert len(result) == 3  # header + 2 content

    def test_all_races_included_no_cutoff(self):
        """15レースが全て送信される（10レース打ち切りなし）"""
        result = format_push_notification(date(2026, 3, 16), self._make_many_decisions(15))
        full_text = "\n".join(m["text"] for m in result)
        # 15Rまで全て含まれる
        for i in range(1, 16):
            assert f"阪神 {i}R" in full_text

    def test_header_shows_correct_race_count(self):
        """ヘッダーに正確なレース数が表示される"""
        result = format_push_notification(date(2026, 3, 16), self._make_many_decisions(15))
        assert "15" in result[0]["text"]  # レース数: 15

