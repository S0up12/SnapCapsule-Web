from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from core.database.schema import DatabaseManager
from core.services.media_processor import MediaProcessor
from routers.dependencies import get_database, get_media_processor
from routers.media import build_media_url, ensure_thumbnail_url, resolve_overlay_path

router = APIRouter(prefix="/api/chats", tags=["chats"])


@router.get("/")
def list_conversations(
    db: DatabaseManager = Depends(get_database),
) -> dict[str, list[dict] | int]:
    items = db.get_conversations()
    return {
        "items": items,
        "total": len(items),
    }


@router.get("/{account_id}/messages")
def list_conversation_messages(
    account_id: str,
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: DatabaseManager = Depends(get_database),
    processor: MediaProcessor = Depends(get_media_processor),
) -> dict[str, int | str | list[dict]]:
    conversations = {item["username"] for item in db.get_conversations()}
    if account_id not in conversations:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Conversation '{account_id}' was not found.",
        )

    messages = db.get_messages_paginated(account_id, limit=limit, offset=skip)
    total = db.count_messages_for_conversation(account_id)

    items: list[dict] = []
    for message in messages:
        media_items: list[dict] = []
        for media_ref in message.media_refs:
            overlay = resolve_overlay_path(media_ref, None)
            web_media_path = processor.get_web_media_sync(Path(media_ref), timeout=60)
            media_source = web_media_path or Path(media_ref)
            media_url = build_media_url(request, media_source)
            thumbnail_url = ensure_thumbnail_url(
                request,
                processor,
                media_source,
                overlay_path=overlay,
            )
            media_items.append(
                {
                    "media_url": media_url,
                    "thumbnail_url": thumbnail_url or media_url,
                    "overlay_url": build_media_url(request, overlay),
                }
            )

        items.append(
            {
                "id": message.id,
                "sender": message.sender,
                "content": message.content,
                "timestamp": message.timestamp.isoformat(),
                "msg_type": message.msg_type,
                "source": message.source,
                "media": media_items,
            }
        )

    return {
        "account_id": account_id,
        "items": items,
        "skip": skip,
        "limit": limit,
        "total": total,
    }
