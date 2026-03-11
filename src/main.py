from contextlib import asynccontextmanager
import threading
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from core.database.schema import DatabaseManager
from core.services.ingestion import IngestionService
from core.services.media_processor import MediaProcessor
from core.utils.paths import get_app_data_dir, get_cache_dir, get_database_path, get_imports_dir, get_raw_media_dir
from core.version import APP_VERSION
from routers.chats import router as chats_router
from routers.ingestion import router as ingestion_router
from routers.memories import router as memories_router


def _cors_origins() -> list[str]:
    return ["*"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = DatabaseManager(get_database_path())
    processor = MediaProcessor(cache_dir=get_cache_dir())
    ingestor = IngestionService(db)

    app.state.db = db
    app.state.processor = processor
    app.state.ingestor = ingestor
    app.state.ingestion_lock = threading.Lock()

    try:
        yield
    finally:
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
    StaticFiles(directory=str(get_cache_dir())),
    name="media-cache",
)
app.mount(
    "/media/raw",
    StaticFiles(directory=str(get_raw_media_dir())),
    name="media-raw",
)

app.include_router(ingestion_router)
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
            "cache": str(get_cache_dir()),
            "imports": str(get_imports_dir()),
        },
    }
