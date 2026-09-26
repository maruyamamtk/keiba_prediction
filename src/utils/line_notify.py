"""
LINE Messaging API 通知ユーティリティ

プッシュ通知・リプライ送信のラッパーモジュール。

環境変数:
  LINE_CHANNEL_ACCESS_TOKEN: Messaging API チャネルアクセストークン
"""



import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"
LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

# LINE Messaging API の1回の呼び出しで送信できる最大メッセージ数
_MAX_MESSAGES_PER_CALL = 5


def _headers(channel_access_token: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {channel_access_token}",
    }


def reply_messages(
    channel_access_token: str,
    reply_token: str,
    messages: list[dict[str, Any]],
) -> None:
    """
    LINE リプライ API でメッセージを送信する

    Args:
        channel_access_token: チャネルアクセストークン
        reply_token: Webhook イベントの replyToken
        messages: 送信するメッセージオブジェクトのリスト（最大5件）
    """
    payload: dict[str, Any] = {
        "replyToken": reply_token,
        "messages": messages[:_MAX_MESSAGES_PER_CALL],
    }
    resp = requests.post(
        LINE_REPLY_URL,
        headers=_headers(channel_access_token),
        json=payload,
        timeout=10,
    )
    if resp.status_code != 200:
        logger.error(
            f"LINE reply failed: status={resp.status_code}, body={resp.text}"
        )
        resp.raise_for_status()
    logger.info(f"LINE reply sent: {len(messages)} message(s)")


def push_messages(
    channel_access_token: str,
    to: str,
    messages: list[dict[str, Any]],
) -> None:
    """
    LINE プッシュ API でメッセージを送信する

    Args:
        channel_access_token: チャネルアクセストークン
        to: 送信先ユーザーID またはグループID
        messages: 送信するメッセージオブジェクトのリスト（最大5件）
    """
    payload: dict[str, Any] = {
        "to": to,
        "messages": messages[:_MAX_MESSAGES_PER_CALL],
    }
    resp = requests.post(
        LINE_PUSH_URL,
        headers=_headers(channel_access_token),
        json=payload,
        timeout=10,
    )
    if resp.status_code != 200:
        logger.error(
            f"LINE push failed: status={resp.status_code}, body={resp.text}"
        )
        resp.raise_for_status()
    logger.info(f"LINE push sent to {to}: {len(messages)} message(s)")


def text_message(text: str) -> dict[str, str]:
    """テキストメッセージオブジェクトを生成する"""
    return {"type": "text", "text": text}


def send_notification(
    channel_access_token: str,
    line_user_id: str,
    message: str,
) -> None:
    """
    運用者向けのLINE通知を送信する（失敗しても例外を送出せず警告ログのみ出す）。

    channel_access_token または line_user_id が未設定の場合は何もしない。
    呼び出し元の主処理（購入・予測等）を通知失敗で止めたくない箇所向けのヘルパー。

    Args:
        channel_access_token: チャネルアクセストークン
        line_user_id: 送信先ユーザーID
        message: 送信するテキストメッセージ
    """
    if not channel_access_token or not line_user_id:
        return
    try:
        push_messages(channel_access_token, line_user_id, [text_message(message)])
    except Exception as e:
        logger.warning(f"LINE通知失敗（無視します）: {e}")
