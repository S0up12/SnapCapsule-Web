from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.services.media_processor import (
    MediaProcessor,
    build_thumbnail_cache_path,
    build_web_video_cache_path,
)
from routers.chats import router as chats_router
from routers.memories import router as memories_router


class MemoriesStubDB:
    def __init__(self, rows: list[object]):
        self.rows = rows

    def get_assets(self, limit: int = 50, offset: int = 0):
        return self.rows

    def count_assets(self) -> int:
        return len(self.rows)


class ChatsStubDB:
    def __init__(
        self,
        conversations: list[dict],
        messages: list[object],
        media_map: dict[int, list[dict[str, str | None]]] | None = None,
    ):
        self._conversations = conversations
        self._messages = messages
        self._media_map = media_map or {}

    def get_conversations(self) -> list[dict]:
        return self._conversations

    def get_messages_paginated(self, username: str, limit: int = 50, offset: int = 0):
        return self._messages

    def count_messages_for_conversation(self, username: str) -> int:
        return len(self._messages)

    def get_message_media_map(self, message_ids: list[int]) -> dict[int, list[dict[str, str | None]]]:
        return {
            message_id: self._media_map.get(message_id, [])
            for message_id in message_ids
        }


def _build_client(db) -> TestClient:
    app = FastAPI()
    app.state.db = db
    app.include_router(memories_router)
    app.include_router(chats_router)
    return TestClient(app)


def test_memories_route_skips_bad_rows_and_never_generates_media(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    data_root = tmp_path / "data"
    raw_dir = data_root / "raw_media"
    cache_dir = data_root / "cache"
    raw_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SNAPCAPSULE_RAW_MEDIA_DIR", str(raw_dir))
    monkeypatch.setenv("SNAPCAPSULE_CACHE_DIR", str(cache_dir))

    media_path = raw_dir / "memories" / "2026-03-06_ABC-main.jpg"
    media_path.parent.mkdir(parents=True, exist_ok=True)
    media_path.write_bytes(b"image")

    def _fail(*args, **kwargs):
        raise AssertionError("blocking media generation should not be called")

    monkeypatch.setattr(MediaProcessor, "get_thumbnail_sync", _fail)
    monkeypatch.setattr(MediaProcessor, "get_web_media_sync", _fail)

    db = MemoriesStubDB(
        [
            ("asset-1", str(media_path), "image", None, "2026", 1),
            ("asset-2", None, "image", None, "2026", 0),
            ("bad-row",),
        ]
    )
    client = _build_client(db)

    caplog.set_level(logging.ERROR)
    response = client.get("/api/memories/")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert len(payload["items"]) == 2
    assert payload["items"][0]["media_url"] == "/media/raw/memories/2026-03-06_ABC-main.jpg"
    assert payload["items"][0]["thumbnail_url"] == "/media/raw/memories/2026-03-06_ABC-main.jpg"
    assert payload["items"][1]["media_url"] is None
    assert payload["items"][1]["thumbnail_url"] is None
    assert payload["items"][1]["overlay_url"] is None
    assert "Failed to parse memory row" in caplog.text


def test_chat_messages_route_uses_predicted_urls_and_skips_bad_messages(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    data_root = tmp_path / "data"
    raw_dir = data_root / "raw_media"
    cache_dir = data_root / "cache"
    raw_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SNAPCAPSULE_RAW_MEDIA_DIR", str(raw_dir))
    monkeypatch.setenv("SNAPCAPSULE_CACHE_DIR", str(cache_dir))

    video_path = raw_dir / "chat_media" / "2026-03-06_ABC-main.mp4"
    video_path.parent.mkdir(parents=True, exist_ok=True)
    video_path.write_bytes(b"video")

    thumbnail_path = build_thumbnail_cache_path(cache_dir, video_path, (400, 400), False, None)
    thumbnail_path.parent.mkdir(parents=True, exist_ok=True)
    thumbnail_path.write_bytes(b"thumb")

    web_path = build_web_video_cache_path(cache_dir, video_path)
    web_path.parent.mkdir(parents=True, exist_ok=True)
    web_path.write_bytes(b"web")

    def _fail(*args, **kwargs):
        raise AssertionError("blocking media generation should not be called")

    monkeypatch.setattr(MediaProcessor, "get_thumbnail_sync", _fail)
    monkeypatch.setattr(MediaProcessor, "get_web_media_sync", _fail)

    valid_message = SimpleNamespace(
        id=101,
        sender="friend.snap",
        content="Look at this",
        timestamp="2026-03-06T22:58:48",
        msg_type="VIDEO",
        source="chat",
        media_refs=[None, str(video_path)],
    )
    invalid_message = object()
    db = ChatsStubDB(
        [{"username": "friend.snap", "display_name": "Friend", "message_count": 2}],
        [valid_message, invalid_message],
        media_map={
            101: [
                {
                    "file_path": str(video_path),
                    "overlay_path": None,
                    "file_type": "video",
                }
            ]
        },
    )
    client = _build_client(db)

    caplog.set_level(logging.ERROR)
    response = client.get("/api/chats/friend.snap/messages")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert len(payload["items"]) == 1
    media = payload["items"][0]["media"]
    assert len(media) == 1
    assert media[0]["media_url"] == f"/media/cache/web/{web_path.name}"
    assert media[0]["thumbnail_url"] == f"/media/cache/{thumbnail_path.name}"
    assert media[0]["overlay_url"] is None
    assert "Failed to parse chat message" in caplog.text
