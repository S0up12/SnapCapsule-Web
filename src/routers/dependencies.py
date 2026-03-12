from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import Request

from core.database.schema import DatabaseManager
from core.services.ingestion import IngestionService
from core.services.media_processor import MediaProcessor
from core.services.settings import SettingsManager
from core.utils.paths import (
    get_extracted_imports_dir,
    get_failed_imports_dir,
    get_imports_dir,
    get_pending_imports_dir,
    get_processed_imports_dir,
)


def get_database(request: Request) -> DatabaseManager:
    return request.app.state.db


def get_ingestion_service(request: Request) -> IngestionService:
    return request.app.state.ingestor


def get_media_processor(request: Request) -> MediaProcessor:
    return request.app.state.processor


def get_settings_manager(request: Request) -> SettingsManager:
    return request.app.state.settings


def get_ingestion_lock(request: Request) -> Any:
    return request.app.state.ingestion_lock


def get_imports_directory() -> Path:
    return get_imports_dir()


def get_pending_imports_directory() -> Path:
    return get_pending_imports_dir()


def get_extracted_imports_directory() -> Path:
    return get_extracted_imports_dir()


def get_processed_imports_directory() -> Path:
    return get_processed_imports_dir()


def get_failed_imports_directory() -> Path:
    return get_failed_imports_dir()
