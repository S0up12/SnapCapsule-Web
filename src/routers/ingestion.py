from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.concurrency import run_in_threadpool

from core.database.schema import DatabaseManager
from core.services.ingestion import IngestionService
from routers.dependencies import (
    get_database,
    get_imports_directory,
    get_ingestion_lock,
    get_ingestion_service,
)

router = APIRouter(prefix="/api/ingest", tags=["ingestion"])

_ZIP_MARKERS = (
    "json/chat_history.json",
    "json/memories_history.json",
    "chat_history.json",
    "memories_history.json",
)


def _normalize_member_name(name: str) -> str:
    return name.replace("\\", "/").lstrip("./")


def _is_snapchat_export(zip_path: Path) -> bool:
    try:
        with zipfile.ZipFile(zip_path) as archive:
            names = [_normalize_member_name(name) for name in archive.namelist()]
    except zipfile.BadZipFile:
        return False

    has_json_markers = any(
        name.endswith(marker)
        for name in names
        for marker in _ZIP_MARKERS
    )
    has_html_export = any(name.startswith("html/chat_history/") for name in names)
    return has_json_markers or has_html_export


def _find_latest_snapchat_export(imports_dir: Path) -> Path | None:
    candidates = sorted(
        imports_dir.glob("*.zip"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for candidate in candidates:
        if _is_snapchat_export(candidate):
            return candidate
    return None


def _noop_progress(_progress: float, _message: str) -> None:
    return None


@router.post("/")
async def ingest_latest_export(
    ingestor: IngestionService = Depends(get_ingestion_service),
    lock: Any = Depends(get_ingestion_lock),
    imports_dir: Path = Depends(get_imports_directory),
) -> dict[str, str | bool]:
    zip_path = _find_latest_snapchat_export(imports_dir)
    if zip_path is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No valid Snapchat export ZIP found in {imports_dir}.",
        )

    if not lock.acquire(blocking=False):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An ingestion job is already running.",
        )

    extract_dir = imports_dir / "extracted" / zip_path.stem

    try:
        success = await run_in_threadpool(
            ingestor.process_zip,
            zip_path,
            extract_dir,
            _noop_progress,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to ingest {zip_path.name}: {exc}",
        ) from exc
    finally:
        lock.release()

    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ingestion failed for {zip_path.name}.",
        )

    return {
        "success": True,
        "zip_file": zip_path.name,
        "extract_dir": str(extract_dir),
    }


@router.get("/status")
def get_ingestion_status(
    db: DatabaseManager = Depends(get_database),
    lock: Any = Depends(get_ingestion_lock),
    imports_dir: Path = Depends(get_imports_directory),
) -> dict[str, int | bool | str]:
    stats = db.get_archive_stats()
    latest_zip = _find_latest_snapchat_export(imports_dir)

    return {
        **stats,
        "has_data": (stats["memories_count"] + stats["messages_count"]) > 0,
        "ingestion_running": lock.locked(),
        "imports_dir": str(imports_dir),
        "latest_zip": latest_zip.name if latest_zip else "",
    }
