from __future__ import annotations

import re
from typing import List

from pyrogram import filters
from pyrogram.handlers import MessageHandler, EditedMessageHandler

from ..core.mltb_client import TgClient
from ..helper.ext_utils.terabox_queue import db_track_leech_post


# Configure the specific channel here (ID or username). Use negative ID for channels.
TRACK_CHANNEL_IDS: List[int] = []  # e.g., [-1001234567890]

# Command trigger to detect in caption
TRIGGER_PHRASE = "/leech terabox"

LINK_REGEX = re.compile(r"https?://\S+", re.IGNORECASE)


def _extract_links(text: str | None) -> List[str]:
    if not text:
        return []
    seen = set()
    links = []
    for m in LINK_REGEX.findall(text):
        if m not in seen:
            seen.add(m)
            links.append(m)
    return links


async def _handle_terabox_leech(_, message):
    if message.chat and message.chat.id in TRACK_CHANNEL_IDS and message.caption:
        if TRIGGER_PHRASE in message.caption:
            # Extract thumbnail file_id (photo or video thumb)
            thumb_id = ""
            if message.photo:
                # Highest res photo is last
                thumb_id = message.photo[-1].file_id
            elif message.video and message.video.thumbs:
                thumb_id = message.video.thumbs[-1].file_id

            links = _extract_links(message.caption)
            await db_track_leech_post(message.chat.id, message.id, thumb_id, links)
    # Do not consume; allow other handlers to continue normal flow
    return


def register_leech_terabox_tracker() -> None:
    if not TRACK_CHANNEL_IDS:
        return
    chat_filter = filters.chat(TRACK_CHANNEL_IDS) & filters.channel
    TgClient.bot.add_handler(MessageHandler(_handle_terabox_leech, chat_filter))
    TgClient.bot.add_handler(EditedMessageHandler(_handle_terabox_leech, chat_filter))


