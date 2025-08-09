from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Dict, Optional

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection


# Separate Mongo instance for Terabox relay tracking ONLY
# Replace with your own if needed; do not mix with existing DBs
MONGO_URI = (
    "mongodb+srv://tejaschavan1110:15HNqpSmaq40eQzX@cluster0.aoldz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)

_client: Optional[MongoClient] = None
_db = None
_posts: Optional[Collection] = None
_links: Optional[Collection] = None

_queue: asyncio.Queue[Dict] = asyncio.Queue()
_worker_task: Optional[asyncio.Task] = None
_consumer_coro: Optional[Callable[[int, int], Awaitable[None]]] = None


def _ensure_db():
    global _client, _db, _posts, _links
    if _client is None:
        _client = MongoClient(MONGO_URI)
        _db = _client.get_database("terabox_relay")
        _posts = _db.get_collection("posts")
        _links = _db.get_collection("links")
        _posts.create_index([("chat_id", ASCENDING), ("message_id", ASCENDING)], unique=True)
        _links.create_index([("chat_id", ASCENDING), ("message_id", ASCENDING), ("link", ASCENDING)], unique=True)


async def db_upsert_post(chat_id: int, message_id: int, status: str, links: Optional[list] = None):
    _ensure_db()
    def _op():
        _posts.update_one(
            {"chat_id": chat_id, "message_id": message_id},
            {"$set": {"status": status}, "$setOnInsert": {"links": links or []}},
            upsert=True,
        )
    await asyncio.to_thread(_op)


async def db_insert_link(chat_id: int, message_id: int, link: str, status: str, file_id: str = "", dest_msg_id: int = 0):
    _ensure_db()
    def _op():
        _links.update_one(
            {"chat_id": chat_id, "message_id": message_id, "link": link},
            {"$set": {"status": status, "file_id": file_id, "dest_msg_id": dest_msg_id}},
            upsert=True,
        )
    await asyncio.to_thread(_op)


async def db_get_post_status(chat_id: int, message_id: int) -> Optional[str]:
    _ensure_db()
    def _op():
        doc = _posts.find_one({"chat_id": chat_id, "message_id": message_id}, {"status": 1})
        return doc.get("status") if doc else None
    return await asyncio.to_thread(_op)


def set_consumer(consumer: Callable[[int, int], Awaitable[None]]):
    global _consumer_coro
    _consumer_coro = consumer


def enqueue_post(chat_id: int, message_id: int):
    _queue.put_nowait({"chat_id": chat_id, "message_id": message_id})


async def _worker():
    while True:
        item = await _queue.get()
        try:
            if _consumer_coro is not None:
                await _consumer_coro(item["chat_id"], item["message_id"])  # one-by-one
        except Exception:
            pass
        finally:
            _queue.task_done()


def init_queue_worker():
    global _worker_task
    if _worker_task is None or _worker_task.done():
        loop = asyncio.get_event_loop()
        _worker_task = loop.create_task(_worker())


