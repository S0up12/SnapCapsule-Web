from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Optional

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".heic"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".webm", ".mkv", ".m4v"}
MEDIA_VARIANTS = ("overlay", "caption", "image", "video", "media", "main")
MEDIA_SUFFIXES = tuple(
    f"{separator}{variant}"
    for separator in ("-", "_")
    for variant in MEDIA_VARIANTS
)
INLINE_VARIANT_RE = re.compile(
    r"^(?P<prefix>.+?)_(?P<variant>overlay|caption|image|video|media)~(?P<rest>.+)$",
    re.IGNORECASE,
)


def normalize_media_stem(stem: str) -> str:
    stem = str(stem)
    match = INLINE_VARIANT_RE.match(stem)
    if match:
        return f"{match.group('prefix')}_{match.group('rest')}"

    lowered = stem.lower()
    for suffix in MEDIA_SUFFIXES:
        if lowered.endswith(suffix):
            return stem[: -len(suffix)]
    return stem


def is_overlay_variant(name: str | Path) -> bool:
    stem = name.stem if isinstance(name, Path) else str(name)
    match = INLINE_VARIANT_RE.match(stem)
    if match:
        return match.group("variant").lower() in {"overlay", "caption"}
    lowered = stem.lower()
    return lowered.endswith(("-overlay", "_overlay", "-caption", "_caption"))


def _iter_related_media_files(path: Path) -> Iterable[Path]:
    base_stem = normalize_media_stem(path.stem)
    if not path.parent.exists():
        return []
    return (
        candidate
        for candidate in path.parent.iterdir()
        if candidate.is_file() and normalize_media_stem(candidate.stem) == base_stem
    )


def _variant_rank(path: Path) -> int:
    stem = path.stem
    suffix = path.suffix.lower()

    if is_overlay_variant(stem):
        return 99

    match = INLINE_VARIANT_RE.match(stem)
    if match:
        variant = match.group("variant").lower()
        if variant == "media":
            return 0
        if variant == "video" and suffix in VIDEO_EXTENSIONS:
            return 0
        if variant == "image" and suffix in IMAGE_EXTENSIONS:
            return 0
        if variant == "main":
            return 1
        return 2

    lowered = stem.lower()
    if suffix in IMAGE_EXTENSIONS and lowered.endswith(("-image", "_image")):
        return 0
    if suffix in VIDEO_EXTENSIONS and lowered.endswith(("-video", "_video")):
        return 0
    if lowered.endswith(("-main", "_main")):
        return 1
    if lowered.endswith(("-media", "_media")):
        return 2
    return 3


def resolve_preferred_image_path(file_path: str) -> str:
    return resolve_preferred_media_path(file_path)


def resolve_preferred_media_path(file_path: str) -> str:
    try:
        path = Path(file_path)
    except Exception:
        return file_path

    suffix = path.suffix.lower()
    if suffix not in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS:
        return file_path

    related = list(_iter_related_media_files(path))
    if not related:
        return file_path

    if suffix in IMAGE_EXTENSIONS:
        candidates = [
            candidate
            for candidate in related
            if candidate.suffix.lower() in IMAGE_EXTENSIONS and not is_overlay_variant(candidate)
        ]
    else:
        candidates = [
            candidate
            for candidate in related
            if candidate.suffix.lower() in VIDEO_EXTENSIONS and not is_overlay_variant(candidate)
        ]

    if not candidates:
        return file_path

    best = min(candidates, key=lambda candidate: (_variant_rank(candidate), candidate.name.lower()))
    return str(best)


def find_caption_overlay(file_path: str) -> Optional[str]:
    try:
        path = Path(file_path)
    except Exception:
        return None

    related = [
        candidate
        for candidate in _iter_related_media_files(path)
        if candidate.suffix.lower() in IMAGE_EXTENSIONS and is_overlay_variant(candidate)
    ]
    if not related:
        return None

    best = min(
        related,
        key=lambda candidate: (
            0 if "overlay" in candidate.stem.lower() else 1,
            candidate.name.lower(),
        ),
    )
    return str(best)
