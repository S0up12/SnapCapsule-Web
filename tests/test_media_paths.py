from __future__ import annotations

from pathlib import Path

from core.utils.media_paths import (
    find_caption_overlay,
    normalize_media_stem,
    resolve_preferred_media_path,
)


def test_normalize_media_stem_handles_suffix_and_inline_variants() -> None:
    assert normalize_media_stem("2025-04-10_FOO-main") == "2025-04-10_FOO"
    assert normalize_media_stem("2025-04-10_FOO_overlay") == "2025-04-10_FOO"
    assert normalize_media_stem("2026-01-23_media~ABC123") == "2026-01-23_ABC123"
    assert normalize_media_stem("2026-01-23_overlay~ABC123") == "2026-01-23_ABC123"


def test_media_resolution_prefers_main_and_overlay_pairs(tmp_path: Path) -> None:
    video = tmp_path / "2026-01-23_media~ABC123.mp4"
    overlay = tmp_path / "2026-01-23_overlay~ABC123.png"
    alternate = tmp_path / "2026-01-23_ABC123.mp4"
    video.write_bytes(b"video")
    overlay.write_bytes(b"overlay")
    alternate.write_bytes(b"alt")

    assert resolve_preferred_media_path(str(alternate)) == str(video)
    assert find_caption_overlay(str(video)) == str(overlay)
