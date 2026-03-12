from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from core.database.schema import DatabaseManager
from core.services.ingestion import IngestionService
from core.services.media_processor import MediaProcessor
from core.services.settings import SettingsManager
from core.utils.logger import get_logger
from core.utils.paths import (
    get_app_data_dir,
    get_cache_dir,
    get_database_path,
    get_extracted_imports_dir,
    get_failed_imports_dir,
    get_imports_dir,
    get_pending_imports_dir,
    get_processed_imports_dir,
    get_raw_media_dir,
)
from core.version import APP_VERSION
from routers.chats import router as chats_router
from routers.ingestion import router as ingestion_router
from routers.memories import router as memories_router
from routers.settings import router as settings_router

logger = get_logger("Main")
_CACHE_DIR = get_cache_dir()
_RAW_MEDIA_DIR = get_raw_media_dir()


def _cors_origins() -> list[str]:
    return ["*"]


async def _auto_import_worker(app: FastAPI) -> None:
    while True:
        try:
            settings: SettingsManager = app.state.settings
            ingestor: IngestionService = app.state.ingestor
            lock: threading.Lock = app.state.ingestion_lock
            pending_dir = get_pending_imports_dir()

            if settings.get_auto_import_enabled() and not lock.locked():
                archives = ingestor.list_pending_archives(pending_dir)
                if archives and lock.acquire(blocking=False):
                    try:
                        logger.info("Auto-import watchdog processing %s archive(s).", len(archives))
                        await run_in_threadpool(
                            ingestor.process_pending_queue,
                            pending_dir,
                            get_extracted_imports_dir(),
                            get_processed_imports_dir(),
                            get_failed_imports_dir(),
                        )
                    finally:
                        lock.release()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Auto-import watchdog error: %s", exc)

        await asyncio.sleep(300)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = DatabaseManager(get_database_path())
    processor = MediaProcessor(cache_dir=_CACHE_DIR)
    ingestor = IngestionService(db)
    settings = SettingsManager(db)

    app.state.db = db
    app.state.processor = processor
    app.state.ingestor = ingestor
    app.state.settings = settings
    app.state.ingestion_lock = threading.Lock()
    watchdog_task = asyncio.create_task(_auto_import_worker(app))

    try:
        yield
    finally:
        watchdog_task.cancel()
        try:
            await watchdog_task
        except asyncio.CancelledError:
            pass
        processor.executor.shutdown(wait=False, cancel_futures=True)


app = FastAPI(
    title="SnapCapsule",
    version=APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount(
    "/media/cache",
    StaticFiles(directory=str(_CACHE_DIR)),
    name="media-cache",
)
app.mount(
    "/media/raw",
    StaticFiles(directory=str(_RAW_MEDIA_DIR)),
    name="media-raw",
)

app.include_router(ingestion_router)
app.include_router(settings_router)
app.include_router(memories_router)
app.include_router(chats_router)


@app.get("/")
async def health_check() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "SnapCapsule",
        "version": APP_VERSION,
        "paths": {
            "database": str(get_database_path()),
            "app_data": str(get_app_data_dir()),
            "cache": str(_CACHE_DIR),
            "imports": str(get_imports_dir()),
            "pending": str(get_pending_imports_dir()),
            "extracted": str(get_extracted_imports_dir()),
            "processed": str(get_processed_imports_dir()),
            "failed": str(get_failed_imports_dir()),
            "raw_media": str(_RAW_MEDIA_DIR),
        },
    }
