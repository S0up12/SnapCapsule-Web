from pathlib import Path
from typing import Optional

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".heic"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".webm", ".mkv", ".m4v"}
MEDIA_SUFFIXES = ["_overlay", "_caption", "_image", "_video", "_media", "_main"]


def _strip_media_suffix(stem: str) -> str:
    for suffix in MEDIA_SUFFIXES:
        if stem.endswith(suffix):
            return stem[:-len(suffix)]
    return stem


def resolve_preferred_image_path(file_path: str) -> str:
    try:
        path = Path(file_path)
    except Exception:
        return file_path
    if path.suffix.lower() not in IMAGE_EXTENSIONS:
        return file_path
    if path.stem.endswith("_image"):
        return file_path
    candidate = path.with_name(f"{path.stem}_image{path.suffix}")
    if candidate.exists():
        return str(candidate)
    return file_path


def resolve_preferred_media_path(file_path: str) -> str:
    """
    Prefer the best available sibling for media files.
    - Images: prefer *_image
    - Videos: prefer *_video, then *_main, then *_media
    """
    try:
        path = Path(file_path)
    except Exception:
        return file_path

    suffix = path.suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        return resolve_preferred_image_path(file_path)

    if suffix in VIDEO_EXTENSIONS:
        stem = path.stem
        for suf in MEDIA_SUFFIXES:
            if stem.endswith(suf):
                stem = stem[:-len(suf)]
                break

        for preferred in ("_video", "_main", "_media", ""):
            candidate = path.with_name(f"{stem}{preferred}{suffix}")
            if candidate.exists():
                return str(candidate)

    return file_path


def find_caption_overlay(file_path: str) -> Optional[str]:
    try:
        path = Path(file_path)
    except Exception:
        return None

    stem = _strip_media_suffix(path.stem)
    candidates = [
        path.with_name(f"{stem}_caption.png"),
        path.with_name(f"{stem}_overlay.png"),
    ]

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    return None
