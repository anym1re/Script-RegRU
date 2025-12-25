import html
import json
import logging
import os
import urllib.request
from typing import Optional, Tuple


def resolve_telegram_config() -> Tuple[str, str]:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    return token, chat_id


def fetch_last_chat_id(token: str, logger: logging.Logger) -> Optional[str]:
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning("Telegram getUpdates failed: %s", e)
        return None

    if not payload.get("ok"):
        logger.warning("Telegram getUpdates not ok: %s", payload)
        return None

    updates = payload.get("result", [])
    if not updates:
        return None

    for update in reversed(updates):
        msg = (
            update.get("message")
            or update.get("edited_message")
            or update.get("channel_post")
            or update.get("edited_channel_post")
        )
        if msg and "chat" in msg and "id" in msg["chat"]:
            return str(msg["chat"]["id"])

    return None


def format_pre(text: str) -> str:
    return f"<pre>{html.escape(text)}</pre>"


def send_telegram_message(
    logger: logging.Logger,
    text: str,
    *,
    parse_mode: Optional[str] = None,
) -> bool:
    token, chat_id = resolve_telegram_config()
    if not token:
        logger.info("Telegram token not configured; skip notification.")
        return False

    if not chat_id:
        chat_id = fetch_last_chat_id(token, logger)
        if not chat_id:
            logger.info("Telegram chat_id not configured; skip notification.")
            return False

    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    data = json.dumps(payload).encode("utf-8")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                logger.warning("Telegram notify failed: HTTP %s", resp.status)
                return False
    except Exception as e:
        logger.warning("Telegram notify failed: %s", e)
        return False

    return True
