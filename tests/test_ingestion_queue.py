from __future__ import annotations

import json
import os
import threading
import zipfile
from pathlib import Path

from core.database.schema import DatabaseManager
from core.services.ingestion import IngestionService
from core.services.media_processor import MediaProcessor


def _write_zip(path: Path, files: dict[str, bytes | str]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)


def test_pending_archives_are_sorted_by_family_then_part(tmp_path: Path) -> None:
    pending = tmp_path / "pending"
    pending.mkdir()

    base = pending / "export-100.zip"
    part = pending / "export-100-2.zip"
    newer = pending / "export-200.zip"

    _write_zip(base, {"json/chat_history.json": "{}"})
    _write_zip(part, {"memories/2026-03-06_ABC-main.mp4": b"video"})
    _write_zip(newer, {"json/chat_history.json": "{}"})

    os.utime(base, (1_000, 1_000))
    os.utime(part, (1_001, 1_001))
    os.utime(newer, (2_000, 2_000))

    service = IngestionService(
        DatabaseManager(tmp_path / "app_state.db"),
        MediaProcessor(cache_dir=tmp_path / "cache"),
    )
    ordered = [path.name for path in service.list_pending_archives(pending)]
    assert ordered == ["export-100.zip", "export-100-2.zip", "export-200.zip"]


def test_memory_download_name_uses_mid_and_date() -> None:
    item = {
        "Date": "2026-03-06 22:58:48 UTC",
        "Media Type": "Video",
        "Media Download Url": "https://example.com/download?mid=34074738-6349-4C1E-88DD-84B615FAC1B9",
    }

    filename = IngestionService.build_memory_download_name(item, "video/mp4")
    assert filename == "2026-03-06_34074738-6349-4C1E-88DD-84B615FAC1B9-main.mp4"


def test_copy_to_raw_queues_precompute_with_overlay_pairs(tmp_path: Path, monkeypatch) -> None:
    raw_dir = tmp_path / "raw_media"
    extracted_root = tmp_path / "extracted"
    monkeypatch.setenv("SNAPCAPSULE_RAW_MEDIA_DIR", str(raw_dir))

    memory_dir = extracted_root / "memories"
    chat_dir = extracted_root / "chat_media"
    memory_dir.mkdir(parents=True, exist_ok=True)
    chat_dir.mkdir(parents=True, exist_ok=True)

    main_image = memory_dir / "2026-03-06_media~ABC123.jpg"
    overlay_image = memory_dir / "2026-03-06_overlay~ABC123.png"
    chat_video = chat_dir / "2026-03-06_ABC999-main.mp4"
    main_image.write_bytes(b"image")
    overlay_image.write_bytes(b"overlay")
    chat_video.write_bytes(b"video")

    class RecordingProcessor:
        def __init__(self) -> None:
            self.calls: list[tuple[Path, Path | None, bool, bool | None]] = []

        def queue_precompute(
            self,
            file_path,
            overlay_path=None,
            size=(400, 400),
            crop=False,
            resolve_variants=True,
        ) -> None:
            media = Path(file_path)
            overlay = Path(overlay_path) if overlay_path else None
            self.calls.append(
                (
                    media,
                    overlay,
                    media.exists(),
                    overlay.exists() if overlay else None,
                )
            )

    processor = RecordingProcessor()
    service = IngestionService(DatabaseManager(tmp_path / "app_state.db"), processor)

    service._copy_extracted_media_to_raw(extracted_root, None, 0.0, 1.0)

    copied_main = raw_dir / "memories" / main_image.name
    copied_overlay = raw_dir / "memories" / overlay_image.name
    copied_video = raw_dir / "chat_media" / chat_video.name
    assert copied_main.exists()
    assert copied_overlay.exists()
    assert copied_video.exists()

    assert processor.calls == [
        (copied_main, copied_overlay, True, True),
        (copied_video, None, True, None),
    ]


def test_copy_to_raw_does_not_wait_for_background_media_work(tmp_path: Path, monkeypatch) -> None:
    raw_dir = tmp_path / "raw_media"
    extracted_root = tmp_path / "extracted"
    monkeypatch.setenv("SNAPCAPSULE_RAW_MEDIA_DIR", str(raw_dir))

    memory_dir = extracted_root / "memories"
    memory_dir.mkdir(parents=True, exist_ok=True)
    main_image = memory_dir / "2026-03-06_ABC-main.jpg"
    main_image.write_bytes(b"image")

    release = threading.Event()
    started = threading.Event()
    worker_threads: list[threading.Thread] = []

    class DeferredProcessor:
        def queue_precompute(
            self,
            file_path,
            overlay_path=None,
            size=(400, 400),
            crop=False,
            resolve_variants=True,
        ) -> None:
            def _run() -> None:
                started.set()
                release.wait(timeout=5)

            thread = threading.Thread(target=_run)
            thread.start()
            worker_threads.append(thread)

    service = IngestionService(DatabaseManager(tmp_path / "app_state.db"), DeferredProcessor())

    service._copy_extracted_media_to_raw(extracted_root, None, 0.0, 1.0)

    assert started.wait(timeout=1)
    assert any(thread.is_alive() for thread in worker_threads)

    release.set()
    for thread in worker_threads:
        thread.join(timeout=1)


def test_process_pending_queue_stages_split_archives_and_finalizes_once(tmp_path: Path) -> None:
    pending = tmp_path / "pending"
    extracted = tmp_path / "extracted"
    processed = tmp_path / "processed"
    failed = tmp_path / "failed"
    pending.mkdir()
    extracted.mkdir()
    processed.mkdir()
    failed.mkdir()

    archives = [
        pending / "export-100.zip",
        pending / "export-100-2.zip",
        pending / "export-100-3.zip",
    ]
    for archive_path in archives:
        _write_zip(archive_path, {"json/chat_history.json": "{}"})

    service = IngestionService(
        DatabaseManager(tmp_path / "app_state.db"),
        MediaProcessor(cache_dir=tmp_path / "cache"),
    )

    calls: list[tuple[str, bool]] = []

    def fake_process_archive(archive_path, extract_to, progress_cb, finalize=True):
        calls.append((Path(archive_path).name, finalize))
        progress_cb(1.0, "ok")
        return True

    service.process_archive = fake_process_archive  # type: ignore[method-assign]

    result = service.process_pending_queue(pending, extracted, processed, failed)

    assert result["success"] is True
    assert calls == [
        ("export-100.zip", False),
        ("export-100-2.zip", False),
        ("export-100-3.zip", True),
    ]
