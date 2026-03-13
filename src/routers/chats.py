from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, status

from core.database.schema import DatabaseManager
from core.services.media_processor import MediaProcessor
from core.utils.logger import get_logger
from routers.dependencies import get_database, get_media_processor
from routers.media import (
    queue_missing_video_derivatives,
    resolve_media_url,
    resolve_overlay_path,
    resolve_preview_url,
)

router = APIRouter(prefix="/api/chats", tags=["chats"])
logger = get_logger("ChatsRouter")


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
    background_tasks: BackgroundTasks,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: DatabaseManager = Depends(get_database),
    processor: MediaProcessor = Depends(get_media_processor),
) -> dict[str, int | str | list[dict]]:
    try:
        conversations = {item["username"] for item in db.get_conversations()}
        if account_id not in conversations:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversation '{account_id}' was not found.",
            )

        messages = db.get_messages_paginated(account_id, limit=limit, offset=skip)
        total = db.count_messages_for_conversation(account_id)
        media_map = db.get_message_media_map(
            [message.id for message in messages if getattr(message, "id", None) is not None]
        )

        items: list[dict] = []
        for message in messages:
            try:
                media_items: list[dict] = []
                message_media = media_map.get(getattr(message, "id", None), [])
                if not message_media and message.media_refs:
                    message_media = [
                        {
                            "file_path": media_ref,
                            "overlay_path": None,
                            "file_type": None,
                        }
                        for media_ref in message.media_refs
                    ]

                for media_entry in message_media:
                    try:
                        file_path = media_entry.get("file_path")
                        file_type = media_entry.get("file_type")
                        overlay = resolve_overlay_path(file_path, media_entry.get("overlay_path"))
                        media_items.append(
                            {
                                "media_url": resolve_media_url(request, file_path),
                                "thumbnail_url": resolve_preview_url(
                                    request,
                                    file_path,
                                    file_type=file_type,
                                    overlay_path=overlay,
                                ),
                                "overlay_url": resolve_media_url(request, overlay),
                            }
                        )
                        queue_missing_video_derivatives(
                            background_tasks,
                            processor,
                            file_path,
                            file_type=file_type,
                            overlay_path=overlay,
                        )
                    except Exception as exc:
                        logger.error(
                            "Failed to parse chat media for message '%s': %s",
                            getattr(message, "id", "unknown"),
                            exc,
                            exc_info=True,
                        )
                        continue

                timestamp = getattr(message, "timestamp", None)
                timestamp_value = (
                    timestamp.isoformat()
                    if hasattr(timestamp, "isoformat")
                    else (str(timestamp) if timestamp is not None else None)
                )

                items.append(
                    {
                        "id": message.id,
                        "sender": message.sender,
                        "content": message.content,
                        "timestamp": timestamp_value,
                        "msg_type": message.msg_type,
                        "source": message.source,
                        "media": media_items,
                    }
                )
            except Exception as exc:
                logger.error(
                    "Failed to parse chat message '%s' in conversation '%s': %s",
                    getattr(message, "id", "unknown"),
                    account_id,
                    exc,
                    exc_info=True,
                )
                continue

        return {
            "account_id": account_id,
            "items": items,
            "skip": skip,
            "limit": limit,
            "total": total,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e
