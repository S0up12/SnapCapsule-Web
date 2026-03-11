from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import Request

from core.services.media_processor import MediaProcessor
from core.utils.media_paths import find_caption_overlay, resolve_preferred_media_path
from core.utils.paths import get_cache_dir, get_raw_media_dir


def _normalize_path_value(value: str | Path) -> str:
    return str(value).replace("\\", "/")


def _safe_relative_path(file_path: str | Path, root: Path) -> Optional[str]:
    normalized_path = _normalize_path_value(file_path).strip()
    normalized_root = _normalize_path_value(root.resolve()).rstrip("/")
    if not normalized_path:
        return None

    root_prefix = f"{normalized_root}/"
    if normalized_path == normalized_root:
        return ""
    if normalized_path.startswith(root_prefix):
        return normalized_path[len(root_prefix):].lstrip("/")

    try:
        relative = Path(normalized_path).resolve().relative_to(root.resolve())
    except ValueError:
        root_name = root.name.strip("/\\")
        marker = f"/{root_name}/"
        if marker in normalized_path:
            return normalized_path.split(marker, 1)[1].lstrip("/")

        candidate = normalized_path.lstrip("./").lstrip("/")
        if candidate and not Path(normalized_path).is_absolute():
            return candidate.replace("\\", "/")
        return None

    return relative.as_posix()


def _build_static_url(mount_path: str, relative_path: str) -> str:
    clean_relative = relative_path.replace("\\", "/").lstrip("/")
    return f"{mount_path.rstrip('/')}/{clean_relative}"


def build_media_url(request: Request, file_path: str | Path | None) -> str | None:
    if not file_path:
        return None

    cache_relative = _safe_relative_path(file_path, get_cache_dir())
    if cache_relative is not None:
        return _build_static_url("/media/cache", cache_relative)

    raw_relative = _safe_relative_path(file_path, get_raw_media_dir())
    if raw_relative is not None:
        return _build_static_url("/media/raw", raw_relative)

    return None


def ensure_thumbnail_url(
    request: Request,
    processor: MediaProcessor,
    file_path: str | Path,
    overlay_path: str | Path | None = None,
    size: tuple[int, int] = (400, 400),
) -> str | None:
    target = Path(resolve_preferred_media_path(str(file_path)))
    overlay = Path(overlay_path) if overlay_path else None

    thumbnail_path = processor.get_thumbnail_sync(
        target,
        size=size,
        crop=False,
        overlay_path=overlay,
        timeout=10,
    )
    return build_media_url(request, thumbnail_path)


def resolve_overlay_path(file_path: str | Path, overlay_path: str | Path | None) -> str | None:
    if overlay_path:
        overlay_candidate = Path(str(overlay_path))
        if overlay_candidate.exists():
            return _normalize_path_value(overlay_candidate)

    preferred_media_path = resolve_preferred_media_path(str(file_path))
    derived_overlay = find_caption_overlay(preferred_media_path)
    return _normalize_path_value(derived_overlay) if derived_overlay else None
