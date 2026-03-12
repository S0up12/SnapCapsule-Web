from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from core.database.schema import DatabaseManager
from core.utils.logger import get_logger
from routers.dependencies import get_database
from routers.media import resolve_media_url, resolve_overlay_path, resolve_preview_url

router = APIRouter(prefix="/api/memories", tags=["memories"])
logger = get_logger("MemoriesRouter")


@router.get("/")
def list_memories(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: DatabaseManager = Depends(get_database),
) -> dict[str, int | list[dict]]:
    try:
        rows = db.get_assets(limit=limit, offset=skip)
        total = db.count_assets()

        items: list[dict] = []
        for row in rows:
            try:
                asset_id, file_path, file_type, overlay_path, year, is_favorite = row
                overlay = resolve_overlay_path(file_path, overlay_path)
                media_url = resolve_media_url(request, file_path)
                thumbnail_url = resolve_preview_url(
                    request,
                    file_path,
                    file_type=file_type,
                    overlay_path=overlay,
                )

                items.append(
                    {
                        "asset_id": asset_id,
                        "file_type": file_type,
                        "year": year,
                        "is_favorite": bool(is_favorite),
                        "media_url": media_url,
                        "thumbnail_url": thumbnail_url,
                        "overlay_url": resolve_media_url(request, overlay),
                    }
                )
            except Exception as exc:
                logger.error(
                    "Failed to parse memory row '%s': %s",
                    row,
                    exc,
                    exc_info=True,
                )
                continue

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
