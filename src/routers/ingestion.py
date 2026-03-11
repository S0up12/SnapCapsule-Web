from __future__ import annotations

from collections.abc import Iterable
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

_ARCHIVE_MARKERS = (
    "json/chat_history.json",
    "json/memories_history.json",
    "json/snap_history.json",
    "chat_history.json",
    "memories_history.json",
    "snap_history.json",
)
_ARCHIVE_SUFFIXES = (".zip", ".rar", ".7z")


def _normalize_member_name(name: str) -> str:
    return name.replace("\\", "/").lstrip("./")


def _has_export_markers(names: Iterable[str]) -> bool:
    normalized_names = [_normalize_member_name(name) for name in names]
    has_json_markers = any(
        name.endswith(marker)
        for name in normalized_names
        for marker in _ARCHIVE_MARKERS
    )
    has_html_export = any(
        name.endswith("html/chat_history") or "html/chat_history/" in name
        for name in normalized_names
    )
    return has_json_markers or has_html_export


def _is_snapchat_export(archive_path: Path, ingestor: IngestionService) -> bool:
    try:
        names = ingestor.list_archive_members(archive_path)
    except Exception:
        return False

    return _has_export_markers(names)


def _find_latest_snapchat_archive(imports_dir: Path, ingestor: IngestionService) -> Path | None:
    candidates = sorted(
        (
            candidate
            for suffix in _ARCHIVE_SUFFIXES
            for candidate in imports_dir.glob(f"*{suffix}")
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for candidate in candidates:
        if _is_snapchat_export(candidate, ingestor):
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
    pre_extracted_root = ingestor.find_pre_extracted_root(imports_dir)
    archive_path = None if pre_extracted_root else _find_latest_snapchat_archive(imports_dir, ingestor)

    if pre_extracted_root is None and archive_path is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No valid Snapchat export archive or extracted folder found in {imports_dir}.",
        )

    if not lock.acquire(blocking=False):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An ingestion job is already running.",
        )

    extract_dir = pre_extracted_root or (imports_dir / "extracted" / archive_path.stem)

    try:
        if pre_extracted_root is not None:
            success = await run_in_threadpool(
                ingestor.process_folder,
                extract_dir,
                _noop_progress,
                True,
            )
        else:
            success = await run_in_threadpool(
                ingestor.process_archive,
                archive_path,
                extract_dir,
                _noop_progress,
            )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to ingest {(archive_path.name if archive_path else extract_dir.name)}: {exc}",
        ) from exc
    finally:
        lock.release()

    if not success:
        if ingestor.was_cancelled():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Ingestion cancelled.",
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ingestion failed for {(archive_path.name if archive_path else extract_dir.name)}.",
        )

    return {
        "success": True,
        "zip_file": archive_path.name if archive_path else extract_dir.name,
        "extract_dir": str(extract_dir),
        "source_type": "folder" if pre_extracted_root else "archive",
    }


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
) -> dict[str, int | bool | str]:
    stats = db.get_archive_stats()
    pre_extracted_root = ingestor.find_pre_extracted_root(imports_dir)
    latest_archive = _find_latest_snapchat_archive(imports_dir, ingestor)
    latest_import = pre_extracted_root.name if pre_extracted_root else (latest_archive.name if latest_archive else "")
    latest_import_kind = "folder" if pre_extracted_root else ("archive" if latest_archive else "")

    return {
        **stats,
        "has_data": (stats["memories_count"] + stats["messages_count"]) > 0,
        "ingestion_running": lock.locked(),
        "imports_dir": str(imports_dir),
        "latest_zip": latest_archive.name if latest_archive else "",
        "latest_import": latest_import,
        "latest_import_kind": latest_import_kind,
    }
