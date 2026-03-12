from __future__ import annotations

from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from core.database.schema import DatabaseManager
from core.services.media_processor import MediaProcessor
from core.utils.logger import get_logger
from routers.dependencies import get_database, get_media_processor
from routers.media import build_media_url, ensure_thumbnail_url, resolve_overlay_path

router = APIRouter(prefix="/api/memories", tags=["memories"])
logger = get_logger("MemoriesRouter")


@router.get("/")
def list_memories(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: DatabaseManager = Depends(get_database),
    processor: MediaProcessor = Depends(get_media_processor),
) -> dict[str, int | list[dict]]:
    try:
        rows = db.get_assets(limit=limit, offset=skip)
        total = db.count_assets()

        items: list[dict] = []
        for asset_id, file_path, file_type, overlay_path, year, is_favorite in rows:
            media_path = Path(file_path) if file_path else None
            overlay = resolve_overlay_path(file_path, overlay_path)
            web_media_path = (
                processor.get_web_media_sync(media_path, timeout=60)
                if media_path and media_path.exists()
                else None
            )
            media_source = web_media_path or media_path
            media_url = build_media_url(request, media_source)
            thumbnail_url = ensure_thumbnail_url(
                request,
                processor,
                media_source,
                overlay_path=overlay,
            ) if media_source else None

            items.append(
                {
                    "asset_id": asset_id,
                    "file_type": file_type,
                    "year": year,
                    "is_favorite": bool(is_favorite),
                    "media_url": media_url,
                    "thumbnail_url": thumbnail_url or media_url,
                    "overlay_url": build_media_url(request, overlay),
                }
            )

        return {
            "items": items,
            "skip": skip,
            "limit": limit,
            "total": total,
        }
    except Exception as e:
        logger.error(f"Error fetching data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e
