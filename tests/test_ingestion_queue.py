from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path

from core.database.schema import DatabaseManager
from core.services.ingestion import IngestionService


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

    service = IngestionService(DatabaseManager(tmp_path / "app_state.db"))
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
