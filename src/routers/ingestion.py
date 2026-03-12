from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.concurrency import run_in_threadpool

from core.database.schema import DatabaseManager
from core.services.ingestion import IngestionService
from core.utils.paths import get_raw_media_dir
from routers.dependencies import (
    get_database,
    get_extracted_imports_directory,
    get_failed_imports_directory,
    get_imports_directory,
    get_ingestion_lock,
    get_ingestion_service,
    get_pending_imports_directory,
    get_processed_imports_directory,
)

router = APIRouter(prefix="/api/ingest", tags=["ingestion"])


@router.post("/")
async def ingest_pending_exports(
    ingestor: IngestionService = Depends(get_ingestion_service),
    lock: Any = Depends(get_ingestion_lock),
    pending_dir: Path = Depends(get_pending_imports_directory),
    extracted_dir: Path = Depends(get_extracted_imports_directory),
    processed_dir: Path = Depends(get_processed_imports_directory),
    failed_dir: Path = Depends(get_failed_imports_directory),
) -> dict[str, Any]:
    archives = ingestor.list_pending_archives(pending_dir)
    if not archives:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No valid Snapchat export archives found in {pending_dir}.",
        )

    if not lock.acquire(blocking=False):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An ingestion job is already running.",
        )

    try:
        summary = await run_in_threadpool(
            ingestor.process_pending_queue,
            pending_dir,
            extracted_dir,
            processed_dir,
            failed_dir,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process pending imports: {exc}",
        ) from exc
    finally:
        lock.release()

    if summary.get("cancelled"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ingestion cancelled.",
        )

    return summary


@router.post("/cancel")
def cancel_ingestion(
    ingestor: IngestionService = Depends(get_ingestion_service),
    lock: Any = Depends(get_ingestion_lock),
) -> dict[str, str | bool]:
    ingestor.request_cancel()
    return {
        "success": True,
        "ingestion_running": lock.locked(),
        "detail": "Cancellation requested. Waiting for the current import to stop.",
    }


@router.get("/status")
def get_ingestion_status(
    db: DatabaseManager = Depends(get_database),
    ingestor: IngestionService = Depends(get_ingestion_service),
    lock: Any = Depends(get_ingestion_lock),
    imports_dir: Path = Depends(get_imports_directory),
    pending_dir: Path = Depends(get_pending_imports_directory),
) -> dict[str, Any]:
    stats = db.get_archive_stats()
    status_payload = ingestor.get_status_snapshot(pending_dir)

    return {
        **stats,
        **status_payload,
        "has_data": (stats["memories_count"] + stats["messages_count"]) > 0,
        "ingestion_running": lock.locked(),
        "imports_dir": str(imports_dir),
        "pending_dir": str(pending_dir),
        "raw_media_dir": str(get_raw_media_dir()),
        "latest_import": status_payload.get("current_archive") or status_payload.get("latest_zip") or "",
        "latest_import_kind": "archive" if status_payload.get("latest_zip") else "",
    }
