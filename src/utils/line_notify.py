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
