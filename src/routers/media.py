from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from fastapi import Request

from core.services.media_processor import (
    VIDEO_EXTENSIONS,
    build_thumbnail_cache_path,
    build_web_video_cache_path,
)
from core.utils.paths import get_cache_dir, get_raw_media_dir


def _normalize_path_value(value: str | Path) -> str:
    return str(value).replace("\\", "/")


def _safe_relative_path(file_path: str | Path, root: Path) -> Optional[str]:
    if file_path is None:
        return None
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


def _is_video_target(file_path: str | Path | None, file_type: str | None = None) -> bool:
    if file_type and str(file_type).strip().lower() == "video":
        return True
    if not file_path:
        return False
    return Path(str(file_path)).suffix.lower() in VIDEO_EXTENSIONS


def _video_placeholder_data_url() -> str:
    svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 400 400'>"
        "<rect width='400' height='400' fill='#111827'/>"
        "<circle cx='200' cy='200' r='64' fill='#1f2937' stroke='#334155' stroke-width='8'/>"
        "<path d='M182 160 L248 200 L182 240 Z' fill='#cbd5e1'/>"
        "<text x='200' y='306' text-anchor='middle' fill='#94a3b8' "
        "font-family='Arial, sans-serif' font-size='24' letter-spacing='2'>"
        "VIDEO"
        "</text>"
        "</svg>"
    )
    return f"data:image/svg+xml;utf8,{svg}"


def build_media_url(request: Request, file_path: str | Path | None) -> str | None:
    if file_path is None:
        return None
    if isinstance(file_path, str) and not file_path.strip():
        return None

    cache_relative = _safe_relative_path(file_path, get_cache_dir())
    if cache_relative is not None:
        return _build_static_url("/media/cache", cache_relative)

    raw_relative = _safe_relative_path(file_path, get_raw_media_dir())
    if raw_relative is not None:
        return _build_static_url("/media/raw", raw_relative)

    return None


def resolve_media_url(request: Request, file_path: str | Path | None) -> str | None:
    if file_path is None:
        return None
    if isinstance(file_path, str) and not file_path.strip():
        return None

    target = Path(str(file_path))
    if not os.path.exists(target):
        return None

    if target.suffix.lower() in VIDEO_EXTENSIONS:
        web_path = build_web_video_cache_path(get_cache_dir(), target)
        if os.path.exists(web_path):
            return build_media_url(request, web_path)

    return build_media_url(request, target)


def predict_thumbnail_url(
    request: Request,
    file_path: str | Path | None,
    overlay_path: str | Path | None = None,
    size: tuple[int, int] = (400, 400),
) -> str | None:
    if not file_path:
        return None
    target = Path(str(file_path))
    if not os.path.exists(target):
        return None
    overlay = Path(str(overlay_path)) if overlay_path else None
    overlay_for_hash = overlay if overlay and os.path.exists(overlay) else None
    thumbnail_path = build_thumbnail_cache_path(
        get_cache_dir(),
        target,
        size=size,
        crop=False,
        overlay_path=overlay_for_hash,
    )
    if not os.path.exists(thumbnail_path):
        return None
    return build_media_url(request, thumbnail_path)


def resolve_preview_url(
    request: Request,
    file_path: str | Path | None,
    *,
    file_type: str | None = None,
    overlay_path: str | Path | None = None,
    size: tuple[int, int] = (400, 400),
) -> str | None:
    thumbnail_url = predict_thumbnail_url(
        request,
        file_path,
        overlay_path=overlay_path,
        size=size,
    )
    if thumbnail_url:
        return thumbnail_url

    media_url = resolve_media_url(request, file_path)
    if not _is_video_target(file_path, file_type=file_type):
        return media_url

    return _video_placeholder_data_url()


def resolve_overlay_path(file_path: str | Path | None, overlay_path: str | Path | None) -> str | None:
    if overlay_path:
        overlay_candidate = Path(str(overlay_path))
        if os.path.exists(overlay_candidate):
            return _normalize_path_value(overlay_candidate)
    return None
