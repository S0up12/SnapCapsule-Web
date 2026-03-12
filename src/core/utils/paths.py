from __future__ import annotations

import os
import sys
from pathlib import Path

DEFAULT_DATA_ROOT = Path("/data")
DEFAULT_DATABASE_DIR = DEFAULT_DATA_ROOT / "database"
DEFAULT_CACHE_DIR = DEFAULT_DATA_ROOT / "cache"
DEFAULT_IMPORTS_DIR = DEFAULT_DATA_ROOT / "imports"
DEFAULT_PENDING_IMPORTS_DIR = DEFAULT_IMPORTS_DIR / "pending"
DEFAULT_EXTRACTED_IMPORTS_DIR = DEFAULT_IMPORTS_DIR / "extracted"
DEFAULT_PROCESSED_IMPORTS_DIR = DEFAULT_IMPORTS_DIR / "processed"
DEFAULT_FAILED_IMPORTS_DIR = DEFAULT_IMPORTS_DIR / "failed"
DEFAULT_RAW_MEDIA_DIR = DEFAULT_DATA_ROOT / "raw_media"


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_source_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_resource_root() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)  # type: ignore[attr-defined]
    return get_source_root()


def get_resource_path(*parts: str) -> Path:
    return get_resource_root().joinpath(*parts)


def get_data_root() -> Path:
    return _ensure_dir(
        Path(os.getenv("SNAPCAPSULE_DATA_DIR", str(Path.home() / ".snapcapsule")))
    )


def get_app_data_dir() -> Path:
    return _ensure_dir(
        Path(os.getenv("SNAPCAPSULE_DATABASE_DIR", str(get_data_root() / "database")))
    )


def get_database_path(filename: str = "app_state.db") -> Path:
    return get_app_data_dir() / filename


def get_cache_dir() -> Path:
    return _ensure_dir(
        Path(os.getenv("SNAPCAPSULE_CACHE_DIR", str(get_data_root() / "cache")))
    )


def get_imports_dir() -> Path:
    return _ensure_dir(
        Path(os.getenv("SNAPCAPSULE_IMPORTS_DIR", str(get_data_root() / "imports")))
    )


def get_pending_imports_dir() -> Path:
    return _ensure_dir(get_imports_dir() / "pending")


def get_extracted_imports_dir() -> Path:
    return _ensure_dir(get_imports_dir() / "extracted")


def get_processed_imports_dir() -> Path:
    return _ensure_dir(get_imports_dir() / "processed")


def get_failed_imports_dir() -> Path:
    return _ensure_dir(get_imports_dir() / "failed")


def get_raw_media_dir() -> Path:
    return _ensure_dir(
        Path(os.getenv("SNAPCAPSULE_RAW_MEDIA_DIR", str(get_data_root() / "raw_media")))
    )
