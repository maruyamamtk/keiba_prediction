"""
LINE 日次プッシュ通知のユニットテスト

テスト対象:
  - is_weekend_in_jst: 土日判定
  - format_push_notification: 通知メッセージフォーマット
  - send_daily_push_notification: BQ取得 + 送信（モック）
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.automation.api.line_webhook import (
    format_push_notification,
    is_weekend_in_jst,
    send_daily_push_notification,
)


# ---------------------------------------------------------------------------
# is_weekend_in_jst
# ---------------------------------------------------------------------------

class TestIsWeekendInJst:
    def test_saturday_is_weekend(self):
        # 2026-03-07 は土曜
        assert is_weekend_in_jst(date(2026, 3, 7)) is True

    def test_sunday_is_weekend(self):
        # 2026-03-08 は日曜
        assert is_weekend_in_jst(date(2026, 3, 8)) is True

    def test_monday_is_weekday(self):
        assert is_weekend_in_jst(date(2026, 3, 9)) is False

    def test_friday_is_weekday(self):
        assert is_weekend_in_jst(date(2026, 3, 13)) is False

    def test_all_weekdays(self):
        # 2026-03-09(月) 〜 2026-03-13(金)
        for day in range(9, 14):
            assert is_weekend_in_jst(date(2026, 3, day)) is False


# ---------------------------------------------------------------------------
# format_push_notification
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_decisions():
    return [
        {
            "venue_code": "09",
            "race_number": 12,
            "race_pattern": "standard",
            "horse_number": 12,
            "horse_name": "ニューオーリンズ",
            "bet_type": "place",
            "bet_amount": 2000.0,
            "win_place_prob": 0.52,
            "place_odds": 2.5,
            "expected_return": 1.30,
        },
        {
            "venue_code": "09",
            "race_number": 12,
            "race_pattern": "standard",
            "horse_number": 13,
            "horse_name": "チュウワチーフ",
            "bet_type": "place",
            "bet_amount": 1500.0,
            "win_place_prob": 0.51,
            "place_odds": 3.0,
            "expected_return": 1.53,
        },
        {
            "venue_code": "05",
            "race_number": 11,
            "race_pattern": "one_dominant",
            "horse_number": 5,
            "horse_name": "トーキョーホース",
            "bet_type": "win",
            "bet_amount": 1000.0,
            "win_place_prob": 0.60,
            "place_odds": 2.0,
            "expected_return": 1.20,
        },
    ]


class TestFormatPushNotification:
    def test_returns_list_of_messages(self, sample_decisions):
        messages = format_push_notification(date(2026, 3, 8), sample_decisions)
        assert isinstance(messages, list)
        assert len(messages) >= 1
        for msg in messages:
            assert msg["type"] == "text"

    def test_header_contains_date(self, sample_decisions):
        messages = format_push_notification(date(2026, 3, 8), sample_decisions)
        header = messages[0]["text"]
        assert "2026-03-08" in header

    def test_header_contains_total_bet(self, sample_decisions):
        messages = format_push_notification(date(2026, 3, 8), sample_decisions)
        header = messages[0]["text"]
        assert "4,500" in header  # 2000 + 1500 + 1000

    def test_venue_name_in_body(self, sample_decisions):
        messages = format_push_notification(date(2026, 3, 8), sample_decisions)
        body = " ".join(m["text"] for m in messages)
        assert "阪神" in body
        assert "東京" in body

    def test_bet_type_label_in_body(self, sample_decisions):
        messages = format_push_notification(date(2026, 3, 8), sample_decisions)
        body = " ".join(m["text"] for m in messages)
        assert "複勝" in body
        assert "単勝" in body

    def test_horse_name_in_body(self, sample_decisions):
        messages = format_push_notification(date(2026, 3, 8), sample_decisions)
        body = " ".join(m["text"] for m in messages)
        assert "ニューオーリンズ" in body

    def test_pattern_label_in_body(self, sample_decisions):
        messages = format_push_notification(date(2026, 3, 8), sample_decisions)
        body = " ".join(m["text"] for m in messages)
        assert "標準型" in body
        assert "突出型" in body

    def test_empty_decisions_returns_no_data_message(self):
        messages = format_push_notification(date(2026, 3, 8), [])
        assert len(messages) == 1
        assert "ありません" in messages[0]["text"]

    def test_max_5_messages(self):
        # 上位10レースに絞られた後も、LINEメッセージは5件以下に収まること
        # （上位10レースの選定はSQL側で行うため、ここでは10レース分のデータを渡す）
        decisions = [
            {
                "venue_code": "05",
                "race_number": i,
                "race_pattern": "standard",
                "horse_number": 1,
                "horse_name": f"ウマ{i}",
                "bet_type": "place",
                "bet_amount": 1000.0,
                "win_place_prob": 0.5,
                "place_odds": 2.0,
                "expected_return": 1.0,
                "max_expected_return": 1.0,
            }
            for i in range(1, 11)  # 上位10レース分
        ]
        messages = format_push_notification(date(2026, 3, 8), decisions)
        assert len(messages) <= 5


# ---------------------------------------------------------------------------
# send_daily_push_notification
# ---------------------------------------------------------------------------

class TestSendDailyPushNotification:
    @patch("src.automation.api.line_webhook._fetch_investment_decisions")
    @patch("src.utils.line_notify.requests.post")
    def test_weekend_sends_notification(self, mock_post, mock_fetch):
        mock_fetch.return_value = [
            {
                "venue_code": "09", "race_number": 12, "race_pattern": "standard",
                "horse_number": 1, "horse_name": "テスト馬", "bet_type": "place",
                "bet_amount": 1000.0, "win_place_prob": 0.5,
                "place_odds": 2.0, "expected_return": 1.0,
            }
        ]
        mock_post.return_value = MagicMock(status_code=200)

        result = send_daily_push_notification(
            project_id="test_project",
            channel_access_token="test_token",
            user_id="test_user",
            target_date=date(2026, 3, 7),  # 土曜
        )

        assert result["status"] == "sent"
        assert result["messages_sent"] >= 1
        mock_post.assert_called_once()

    @patch("src.utils.line_notify.requests.post")
    def test_weekday_skips(self, mock_post):
        result = send_daily_push_notification(
            project_id="test_project",
            channel_access_token="test_token",
            user_id="test_user",
            target_date=date(2026, 3, 9),  # 月曜
        )

        assert result["status"] == "skipped"
        assert result["messages_sent"] == 0
        mock_post.assert_not_called()

    @patch("src.automation.api.line_webhook._fetch_investment_decisions")
    @patch("src.utils.line_notify.requests.post")
    def test_no_decisions_returns_no_decisions(self, mock_post, mock_fetch):
        mock_fetch.return_value = []
        mock_post.return_value = MagicMock(status_code=200)

        result = send_daily_push_notification(
            project_id="test_project",
            channel_access_token="test_token",
            user_id="test_user",
            target_date=date(2026, 3, 8),  # 日曜
        )

        assert result["status"] == "no_decisions"
        assert result["messages_sent"] == 0
        mock_post.assert_not_called()

    @patch("src.automation.api.line_webhook._fetch_investment_decisions")
    @patch("src.utils.line_notify.requests.post")
    def test_sunday_sends_notification(self, mock_post, mock_fetch):
        mock_fetch.return_value = [
            {
                "venue_code": "05", "race_number": 11, "race_pattern": "one_dominant",
                "horse_number": 3, "horse_name": "日曜馬", "bet_type": "place",
                "bet_amount": 2000.0, "win_place_prob": 0.6,
                "place_odds": 2.5, "expected_return": 1.5,
            }
        ]
        mock_post.return_value = MagicMock(status_code=200)

        result = send_daily_push_notification(
            project_id="test_project",
            channel_access_token="test_token",
            user_id="test_user",
            target_date=date(2026, 3, 8),  # 日曜
        )

        assert result["status"] == "sent"
        mock_post.assert_called_once()

    @patch("src.automation.api.line_webhook._fetch_investment_decisions")
    @patch("src.utils.line_notify.requests.post")
    def test_fetch_called_with_max_races_10(self, mock_post, mock_fetch):
        """_fetch_investment_decisions が max_races=10 で呼ばれることを確認"""
        mock_fetch.return_value = []
        mock_post.return_value = MagicMock(status_code=200)

        send_daily_push_notification(
            project_id="test_project",
            channel_access_token="test_token",
            user_id="test_user",
            target_date=date(2026, 3, 7),  # 土曜
        )

        mock_fetch.assert_called_once_with("test_project", date(2026, 3, 7), max_races=10)
