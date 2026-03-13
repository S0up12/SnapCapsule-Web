from __future__ import annotations

import subprocess
from pathlib import Path

import core.services.media_processor as media_processor_module
from core.services.media_processor import MediaProcessor


def test_zero_byte_media_is_quarantined_before_thumbnail_work(tmp_path: Path) -> None:
    processor = MediaProcessor(cache_dir=tmp_path / "cache")
    empty_image = tmp_path / "empty.jpg"
    empty_image.write_bytes(b"")

    assert processor.get_thumbnail_sync(empty_image) is None
    assert processor.is_quarantined(empty_image) is True


def test_web_transcode_timeout_quarantines_media(tmp_path: Path, monkeypatch) -> None:
    processor = MediaProcessor(cache_dir=tmp_path / "cache")
    broken_video = tmp_path / "broken.mp4"
    broken_video.write_bytes(b"not-a-real-video")
    output_path = tmp_path / "cache" / "web" / "broken_web.mp4"

    def _timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="ffmpeg", timeout=30)

    monkeypatch.setattr(media_processor_module.subprocess, "run", _timeout)

    assert processor._transcode_video_for_web(str(broken_video), str(output_path)) is False
    assert processor.is_quarantined(broken_video) is True
