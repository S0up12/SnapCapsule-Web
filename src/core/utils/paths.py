from __future__ import annotations

import os
import sys
from pathlib import Path


def get_source_root() -> Path:
    """Return the source root directory (the folder that contains assets/)."""
    return Path(__file__).resolve().parents[2]


def get_resource_root() -> Path:
    """Return the resource root for dev or frozen (PyInstaller) builds."""
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)  # type: ignore[attr-defined]
    return get_source_root()


def get_resource_path(*parts: str) -> Path:
    return get_resource_root().joinpath(*parts)


def get_app_data_dir() -> Path:
    """
    Return the persistent application directory used by the backend.

    For the containerized service we keep database-adjacent files together,
    while allowing each storage class to be overridden independently through
    environment variables.
    """
    path = Path(os.getenv("SNAPCAPSULE_DATABASE_DIR", str(get_data_root() / "database")))
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_data_root() -> Path:
    path = Path(os.getenv("SNAPCAPSULE_DATA_DIR", str(Path.home() / ".snapcapsule")))
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_database_path(filename: str = "app_state.db") -> Path:
    return get_app_data_dir() / filename


def get_cache_dir() -> Path:
    path = Path(os.getenv("SNAPCAPSULE_CACHE_DIR", str(get_data_root() / "cache")))
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_imports_dir() -> Path:
    path = Path(os.getenv("SNAPCAPSULE_IMPORTS_DIR", str(get_data_root() / "imports")))
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_raw_media_dir() -> Path:
    path = get_imports_dir() / "extracted"
    path.mkdir(parents=True, exist_ok=True)
    return path
