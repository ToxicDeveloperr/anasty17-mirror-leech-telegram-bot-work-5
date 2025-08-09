from __future__ import annotations

import asyncio
import os
import re
import tempfile
from contextlib import asynccontextmanager
from typing import Iterable, List, Tuple

import httpx
from pyrogram import filters
from pyrogram.handlers import MessageHandler

from ..core.mltb_client import TgClient
from ..helper.mirror_leech_utils.download_utils.direct_link_generator import (
    terabox as terabox_to_direct,
)


# ========================= User configurable variables =========================
# Define source channels to monitor (IDs). Example: [-1001234567890]
SOURCE_CHANNEL_IDS: List[int] = [-1002487065354]

# Define destination channel to upload files to (ID)
DESTINATION_CHANNEL_ID: int = -1002176533426

# Define details channel to post the summary (ID)
DETAILS_CHANNEL_ID: int = -1002271035070

# Max number of links to process from a single post (0 = no limit)
MAX_LINKS_PER_POST: int = 0

# How many links to process concurrently per post
MAX_CONCURRENT_LINKS: int = 2


# =============================== Implementation ===============================
TERABOX_DOMAINS = [
    "terabox.com",
    "nephobox.com",
    "4funbox.com",
    "mirrobox.com",
    "momerybox.com",
    "teraboxapp.com",
    "1024tera.com",
    "terabox.app",
    "gibibox.com",
    "goaibox.com",
    "terasharelink.com",
    "teraboxlink.com",
    "freeterabox.com",
    "1024terabox.com",
    "teraboxshare.com",
    "terafileshare.com",
    "terabox.club",
]

TERABOX_REGEX = re.compile(
    r"https?://(?:www\.)?(?:" + "|".join(re.escape(d) for d in TERABOX_DOMAINS) + r")[^\s]+",
    re.IGNORECASE,
)


def _limit_links(links: List[str]) -> List[str]:
    if MAX_LINKS_PER_POST and MAX_LINKS_PER_POST > 0:
        return links[:MAX_LINKS_PER_POST]
    return links


def _extract_links_from_text(text: str | None) -> List[str]:
    if not text:
        return []
    found = TERABOX_REGEX.findall(text)
    # Keep order, remove duplicates preserving first occurrence
    unique: List[str] = []
    seen = set()
    for link in found:
        if link not in seen:
            seen.add(link)
            unique.append(link)
    return _limit_links(unique)


async def _resolve_terabox_link(share_url: str) -> Tuple[str, str]:
    """Return (direct_url, suggested_name). suggested_name may be empty."""
    direct = terabox_to_direct(share_url)
    if isinstance(direct, dict):
        contents = direct.get("contents") or []
        if not contents:
            raise ValueError("No downloadable content from Terabox response")
        item = contents[0]
        return item.get("url", share_url), item.get("filename", "")
    return direct, ""


@asynccontextmanager
async def _httpx_client():
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=20)
    timeout = httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0)
    async with httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True) as client:
        yield client


async def _guess_filename_from_headers(headers, fallback: str) -> str:
    disp = headers.get("Content-Disposition") or headers.get("content-disposition")
    if disp:
        match = re.search(r"filename\*=UTF-8''([^;]+)|filename\s*=\s*\"?([^;\"]+)\"?", disp, re.IGNORECASE)
        if match:
            name = match.group(1) or match.group(2)
            return os.path.basename(name)
    return os.path.basename(fallback) if fallback else "file.bin"


async def _download_file(url: str, suggested_name: str) -> str:
    """Download URL to a temp file; return file path."""
    os.makedirs("temp_relay", exist_ok=True)
    async with _httpx_client() as client:
        resp = await client.get(url)
        resp.raise_for_status()
        filename = await _guess_filename_from_headers(resp.headers, suggested_name or url)
        fd, path = tempfile.mkstemp(prefix="relay_", suffix="_" + filename, dir="temp_relay")
        os.close(fd)
        async with aiofiles_open(path, mode="wb") as f:
            async for chunk in resp.aiter_bytes(chunk_size=1024 * 512):
                if chunk:
                    await f.write(chunk)
    return path


async def _upload_to_destination(file_path: str, caption: str | None = None) -> Tuple[str, int]:
    """Upload to destination channel. Return (file_id, message_id)."""
    msg = await TgClient.bot.send_document(
        chat_id=DESTINATION_CHANNEL_ID,
        document=file_path,
        caption=caption or "",
        disable_notification=True,
    )
    file_id = None
    if msg.document:
        file_id = msg.document.file_id
    elif msg.video:
        file_id = msg.video.file_id
    elif msg.audio:
        file_id = msg.audio.file_id
    elif msg.photo:
        file_id = msg.photo.file_id
    return file_id or "", msg.id


async def _process_single_link(link: str, original_caption: str | None) -> Tuple[str, int, str]:
    """Return (file_id, dest_msg_id, link)."""
    # Resolve direct link using existing converter (runs sync-style; call in thread)
    direct_url, suggested_name = await asyncio.to_thread(_resolve_terabox_link, link)
    file_path = await _download_file(direct_url, suggested_name)
    try:
        file_id, msg_id = await _upload_to_destination(file_path, caption=original_caption)
        return file_id, msg_id, link
    finally:
        try:
            os.remove(file_path)
        except Exception:
            pass


async def _send_details_message(original_message, links: Iterable[str], file_ids: Iterable[str]) -> None:
    # Copy original message (with media/thumbnail) to details channel
    try:
        copied = await TgClient.bot.copy_message(
            chat_id=DETAILS_CHANNEL_ID,
            from_chat_id=original_message.chat.id,
            message_id=original_message.id,
        )
        header_msg_id = copied.id
    except Exception:
        header_msg_id = None

    # Send file IDs as a follow-up message
    text_parts = ["Terabox links processed:", *[f"- {u}" for u in links], "", "Telegram file_ids:"]
    text_parts.extend(f"- {fid}" for fid in file_ids if fid)
    text = "\n".join(text_parts)

    await TgClient.bot.send_message(
        chat_id=DETAILS_CHANNEL_ID,
        text=text,
        reply_to_message_id=header_msg_id,
        disable_notification=True,
    )


async def _handle_source_post(_, message):
    # Extract links from caption/text
    links = _extract_links_from_text(message.caption or message.text)
    if not links:
        return

    sem = asyncio.Semaphore(MAX_CONCURRENT_LINKS if MAX_CONCURRENT_LINKS > 0 else 1)
    results: List[Tuple[str, int, str]] = []

    async def worker(link: str):
        async with sem:
            try:
                res = await _process_single_link(link, message.caption)
                results.append(res)
            except Exception:
                # Skip on error for this link
                pass

    await asyncio.gather(*(worker(l) for l in links))

    if results:
        file_ids = [r[0] for r in results if r and r[0]]
        await _send_details_message(message, links, file_ids)


def register_handlers() -> None:
    if not SOURCE_CHANNEL_IDS or DESTINATION_CHANNEL_ID == 0 or DETAILS_CHANNEL_ID == 0:
        # Not configured; skip registration
        return
    chat_filter = filters.chat(SOURCE_CHANNEL_IDS)
    TgClient.bot.add_handler(MessageHandler(_handle_source_post, chat_filter))


# -------- aiofiles minimal import (lazy) --------
try:
    from aiofiles import open as aiofiles_open  # type: ignore
except Exception:  # pragma: no cover
    async def aiofiles_open(path, mode="r"):
        # Fallback to thread-based file IO if aiofiles is not available
        loop = asyncio.get_event_loop()

        class _AsyncFile:
            def __init__(self, _path, _mode):
                self._path = _path
                self._mode = _mode
                self._f = None

            async def __aenter__(self):
                self._f = await loop.run_in_executor(None, lambda: open(self._path, self._mode))
                return self

            async def __aexit__(self, exc_type, exc, tb):
                if self._f:
                    await loop.run_in_executor(None, self._f.close)

            async def write(self, data: bytes):
                await loop.run_in_executor(None, self._f.write, data)

        return _AsyncFile(path, mode)


