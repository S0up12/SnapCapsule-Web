from __future__ import annotations

import threading
import zipfile
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.database.schema import DatabaseManager
from core.services.ingestion import IngestionService
from core.services.settings import SettingsManager
from routers.ingestion import router as ingestion_router
from routers.settings import router as settings_router


def _make_archive(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("json/chat_history.json", "{}")
        archive.writestr("json/memories_history.json", '{"Saved Media": []}')
        archive.writestr("json/snap_history.json", "{}")


def _build_test_client(tmp_path: Path, monkeypatch) -> TestClient:
    data_root = tmp_path / "data"
    database_dir = data_root / "database"
    imports_dir = data_root / "imports"
    raw_media_dir = data_root / "raw_media"

    monkeypatch.setenv("SNAPCAPSULE_DATA_DIR", str(data_root))
    monkeypatch.setenv("SNAPCAPSULE_DATABASE_DIR", str(database_dir))
    monkeypatch.setenv("SNAPCAPSULE_IMPORTS_DIR", str(imports_dir))
    monkeypatch.setenv("SNAPCAPSULE_RAW_MEDIA_DIR", str(raw_media_dir))

    app = FastAPI()
    database_dir.mkdir(parents=True, exist_ok=True)
    imports_dir.mkdir(parents=True, exist_ok=True)
    raw_media_dir.mkdir(parents=True, exist_ok=True)
    db = DatabaseManager(database_dir / "app_state.db")
    ingestor = IngestionService(db)
    settings = SettingsManager(db)

    app.state.db = db
    app.state.ingestor = ingestor
    app.state.settings = settings
    app.state.ingestion_lock = threading.Lock()
    app.include_router(ingestion_router)
    app.include_router(settings_router)
    return TestClient(app)


def test_settings_routes_round_trip(tmp_path: Path, monkeypatch) -> None:
    client = _build_test_client(tmp_path, monkeypatch)

    assert client.get("/api/settings/").json() == {"auto_import_enabled": False}

    response = client.post("/api/settings/", json={"auto_import_enabled": True})
    assert response.status_code == 200
    assert response.json() == {"auto_import_enabled": True}
    assert client.get("/api/settings/").json() == {"auto_import_enabled": True}


def test_ingestion_status_and_batch_endpoint(tmp_path: Path, monkeypatch) -> None:
    client = _build_test_client(tmp_path, monkeypatch)
    pending_dir = tmp_path / "data" / "imports" / "pending"
    processed_dir = tmp_path / "data" / "imports" / "processed"
    pending_dir.mkdir(parents=True, exist_ok=True)

    archive_path = pending_dir / "export-100.zip"
    _make_archive(archive_path)

    status_response = client.get("/api/ingest/status")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["queue_pending"] == 1
    assert status_payload["download_total"] == 0
    assert status_payload["current_step"] == "Idle"

    ingest_response = client.post("/api/ingest/")
    assert ingest_response.status_code == 200
    ingest_payload = ingest_response.json()
    assert ingest_payload["total_archives"] == 1
    assert ingest_payload["processed_archives"] == 1
    assert ingest_payload["failed_archives"] == 0
    assert (processed_dir / "export-100.zip").exists()


def test_ingestion_returns_conflict_when_lock_is_held(tmp_path: Path, monkeypatch) -> None:
    client = _build_test_client(tmp_path, monkeypatch)
    pending_dir = tmp_path / "data" / "imports" / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    _make_archive(pending_dir / "export-100.zip")

    lock = client.app.state.ingestion_lock
    lock.acquire()
    try:
        response = client.post("/api/ingest/")
    finally:
        lock.release()

    assert response.status_code == 409
