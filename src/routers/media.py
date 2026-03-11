from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import Request

from core.services.media_processor import MediaProcessor
from core.utils.media_paths import find_caption_overlay, resolve_preferred_media_path
from core.utils.paths import get_cache_dir, get_raw_media_dir


def _safe_relative_path(file_path: Path, root: Path) -> Optional[str]:
    try:
        relative = file_path.resolve().relative_to(root.resolve())
    except ValueError:
        return None
    return relative.as_posix()


def build_media_url(request: Request, file_path: str | Path | None) -> str | None:
    if not file_path:
        return None

    path = Path(file_path)
    cache_relative = _safe_relative_path(path, get_cache_dir())
    if cache_relative is not None:
        return str(request.url_for("media-cache", path=cache_relative))

    raw_relative = _safe_relative_path(path, get_raw_media_dir())
    if raw_relative is not None:
        return str(request.url_for("media-raw", path=raw_relative))

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
        return str(Path(overlay_path))
    derived_overlay = find_caption_overlay(str(file_path))
    return derived_overlay
