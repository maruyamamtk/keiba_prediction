"""
LINE通知ユーティリティ（send_notification）のテスト（Issue #437）
"""

from unittest.mock import patch

from src.utils.line_notify import send_notification


class TestSendNotification:
    """send_notification のテスト"""

    @patch("src.utils.line_notify.push_messages")
    def test_sends_message_when_credentials_present(self, mock_push):
        """トークン・ユーザーIDが揃っていればpush_messagesが呼ばれること"""
        send_notification("token", "user-1", "テストメッセージ")

        mock_push.assert_called_once_with(
            "token", "user-1", [{"type": "text", "text": "テストメッセージ"}]
        )

    @patch("src.utils.line_notify.push_messages")
    def test_skips_when_token_missing(self, mock_push):
        """トークンが空文字の場合は何もしないこと"""
        send_notification("", "user-1", "テストメッセージ")
        mock_push.assert_not_called()

    @patch("src.utils.line_notify.push_messages")
    def test_skips_when_user_id_missing(self, mock_push):
        """ユーザーIDが空文字の場合は何もしないこと"""
        send_notification("token", "", "テストメッセージ")
        mock_push.assert_not_called()

    @patch("src.utils.line_notify.push_messages", side_effect=Exception("LINE API error"))
    def test_failure_does_not_raise(self, mock_push):
        """push_messagesが例外を送出しても呼び出し元に伝播しないこと"""
        send_notification("token", "user-1", "テストメッセージ")  # 例外が発生しないこと
