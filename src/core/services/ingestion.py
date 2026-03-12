import json
import mimetypes
import re
import shutil
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import httpx
import py7zr
import rarfile

from core.database.schema import DatabaseManager
from core.models import Message, MediaAsset
from core.services.media_processor import MediaProcessor
from core.utils.logger import get_logger
from core.utils.media_paths import (
    IMAGE_EXTENSIONS,
    VIDEO_EXTENSIONS,
    is_overlay_variant,
    normalize_media_stem,
)
from core.utils.paths import get_app_data_dir, get_raw_media_dir
from bs4 import BeautifulSoup

logger = get_logger("IngestionService")

ProgressCallback = Callable[[float, str], None]
_MEMORY_CONTENT_TYPE_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "video/mp4": ".mp4",
    "video/quicktime": ".mov",
    "video/webm": ".webm",
}
_ARCHIVE_PART_RE = re.compile(r"^(?P<family>.+?)(?:-(?P<part>\d+))?$")

class IngestionCancelled(Exception):
    pass

@dataclass
class IngestionJobStatus:
    running: bool = False
    current_step: str = "Idle"
    overall_progress: float = 0.0
    current_archive: str = ""
    current_archive_index: int = 0
    current_archive_total: int = 0
    download_total: int = 0
    download_completed: int = 0
    download_skipped: int = 0
    download_failed: int = 0

class IngestionService:
    EXPORT_MARKER_FILES = (
        "chat_history.json",
        "memories_history.json",
        "snap_history.json",
        "account.json",
        "user_profile.json",
    )
    ARCHIVE_SUFFIXES = (".zip", ".rar", ".7z")

    def __init__(self, db: DatabaseManager, processor: MediaProcessor):
        self.db = db
        self.processor = processor
        self.chunk_size = 100
        self.message_chunk_size = 500
        self.current_root: Optional[Path] = None
        self.media_buckets: Dict[str, List[Dict]] = {} 
        self.media_id_map: Dict[str, str] = {}
        self.media_match_mode: str = "strict"
        self._cancel_requested = threading.Event()
        self._last_run_cancelled = False
        self._status_lock = threading.Lock()
        self._job_status = IngestionJobStatus()

    def request_cancel(self) -> None:
        self._cancel_requested.set()

    def was_cancelled(self) -> bool:
        return self._last_run_cancelled

    def get_job_status(self) -> dict[str, Any]:
        with self._status_lock:
            return asdict(self._job_status)

    def _reset_job_status(self) -> None:
        with self._status_lock:
            self._job_status = IngestionJobStatus()

    def _update_job_status(self, **updates: Any) -> None:
        with self._status_lock:
            for key, value in updates.items():
                setattr(self._job_status, key, value)

    @staticmethod
    def _normalize_member_name(name: str) -> str:
        return name.replace("\\", "/").lstrip("./")

    def _members_have_media_roots(self, names: List[str]) -> bool:
        normalized = [self._normalize_member_name(name) for name in names]
        return any(
            name.startswith("memories/") or name.startswith("chat_media/")
            for name in normalized
        )

    def contains_archive_data(self, archive_path: Path) -> bool:
        try:
            names = self.list_archive_members(archive_path)
        except Exception:
            return False

        normalized = [self._normalize_member_name(name) for name in names]
        has_markers = any(
            name.endswith(marker)
            for name in normalized
            for marker in (
                "json/chat_history.json",
                "json/memories_history.json",
                "json/snap_history.json",
                "json/account.json",
                "json/user_profile.json",
                "chat_history.json",
                "memories_history.json",
                "snap_history.json",
                "account.json",
                "user_profile.json",
            )
        )
        has_html = any(
            name.endswith("html/chat_history") or "html/chat_history/" in name
            for name in normalized
        )
        return has_markers or has_html or self._members_have_media_roots(normalized)

    def _archive_family_key(
        self,
        archive_path: Path,
        known_stems: Optional[set[str]] = None,
    ) -> tuple[str, int]:
        stem = archive_path.stem
        match = re.match(r"^(?P<family>.+)-(?P<part>\d+)$", stem)
        if not match:
            return stem, 0

        family = match.group("family")
        if known_stems is not None and family not in known_stems:
            return stem, 0

        return family, int(match.group("part"))

    def list_pending_archives(self, pending_dir: Path) -> list[Path]:
        pending_dir = Path(pending_dir)
        archives = [
            candidate
            for suffix in self.ARCHIVE_SUFFIXES
            for candidate in pending_dir.glob(f"*{suffix}")
            if candidate.is_file()
        ]
        valid_archives = [candidate for candidate in archives if self.contains_archive_data(candidate)]
        if not valid_archives:
            return []

        known_stems = {archive_path.stem for archive_path in valid_archives}
        family_times: dict[str, float] = {}
        for archive_path in valid_archives:
            family, _ = self._archive_family_key(archive_path, known_stems)
            family_times.setdefault(family, archive_path.stat().st_mtime)
            family_times[family] = min(family_times[family], archive_path.stat().st_mtime)

        return sorted(
            valid_archives,
            key=lambda candidate: (
                family_times[self._archive_family_key(candidate, known_stems)[0]],
                self._archive_family_key(candidate, known_stems)[0].lower(),
                self._archive_family_key(candidate, known_stems)[1],
                candidate.name.lower(),
            ),
        )

    def get_status_snapshot(self, pending_dir: Path) -> dict[str, Any]:
        pending_archives = self.list_pending_archives(pending_dir)
        state = self.get_job_status()
        return {
            **state,
            "queue_pending": len(pending_archives),
            "queue_total": state["current_archive_total"] if state["running"] else len(pending_archives),
            "latest_zip": pending_archives[0].name if pending_archives else "",
        }

    def list_archive_members(self, archive_path: Path) -> List[str]:
        archive_path = Path(archive_path)
        suffix = archive_path.suffix.lower()

        if suffix == ".zip":
            with zipfile.ZipFile(archive_path, "r") as archive:
                return archive.namelist()
        if suffix == ".rar":
            with rarfile.RarFile(archive_path, "r") as archive:
                return archive.namelist()
        if suffix == ".7z":
            with py7zr.SevenZipFile(archive_path, "r") as archive:
                return archive.getnames()

        raise ValueError(f"Unsupported archive format: {archive_path.suffix}")

    def contains_export_markers(self, path: Path) -> bool:
        path = Path(path)
        json_dir = path / "json"

        for marker in self.EXPORT_MARKER_FILES:
            if (path / marker).exists() or (json_dir / marker).exists():
                return True

        return (path / "html" / "chat_history").exists()

    def find_pre_extracted_root(self, search_root: Path) -> Optional[Path]:
        search_root = Path(search_root)
        candidate_bases = [
            search_root / "extracted",
            search_root / "raw",
            search_root,
        ]

        candidates: set[Path] = set()

        for base in candidate_bases:
            if not base.exists() or not base.is_dir():
                continue

            if self.contains_export_markers(base):
                candidates.add(self._find_snap_root(base))

            for marker in self.EXPORT_MARKER_FILES:
                for marker_path in base.rglob(marker):
                    candidate = marker_path.parent
                    if candidate.name == "json":
                        candidate = candidate.parent
                    candidate = self._find_snap_root(candidate)
                    if self.contains_export_markers(candidate):
                        candidates.add(candidate)

            for chat_dir in base.rglob("chat_history"):
                if chat_dir.is_dir() and chat_dir.parent.name == "html":
                    candidate = self._find_snap_root(chat_dir.parent.parent)
                    if self.contains_export_markers(candidate):
                        candidates.add(candidate)

        if not candidates:
            return None

        return max(candidates, key=lambda candidate: candidate.stat().st_mtime)

    def _reset_runtime_state(self):
        self.current_root = None
        self.media_buckets = {}
        self.media_id_map = {}
        self.media_match_mode = "strict"

    def _reset_job_flags(self):
        self._cancel_requested.clear()
        self._last_run_cancelled = False

    def _raise_if_cancelled(self):
        if self._cancel_requested.is_set():
            raise IngestionCancelled("Ingestion cancelled by user.")

    def _handle_cancelled_run(
        self,
        progress_cb: Optional[ProgressCallback] = None,
        extract_to: Optional[Path] = None,
        remove_extract_dir: bool = False,
    ) -> bool:
        self._last_run_cancelled = True
        logger.info("Ingestion cancelled by user.")

        if remove_extract_dir and extract_to and extract_to.exists():
            try:
                shutil.rmtree(extract_to)
            except Exception as exc:
                logger.debug(f"Failed to clean partial extract directory {extract_to}: {exc}")

        self._update_job_status(running=False, current_step="Import cancelled.")
        if progress_cb:
            progress_cb(0.0, "Import cancelled.")

        return False

    def _maybe_emit_progress(
        self,
        progress_cb: Optional[ProgressCallback],
        start: float,
        end: float,
        processed: int,
        total: int,
        msg: str,
        last_emit: float
    ) -> float:
        if not progress_cb or total <= 0:
            return last_emit
        ratio = processed / total
        val = start + (end - start) * ratio
        if val > end:
            val = end
        if processed == total or (val - last_emit) >= 0.002:
            progress_cb(val, msg)
            return val
        return last_emit

    def _get_staged_dir(self, create: bool = False) -> Path:
        staged_cfg = None
        try:
            staged_cfg = self.db.get_config("staged_path")
        except Exception:
            staged_cfg = None
        if staged_cfg:
            staged_dir = Path(staged_cfg)
        else:
            staged_dir = get_app_data_dir() / "staged_data"
        if create:
            staged_dir.parent.mkdir(parents=True, exist_ok=True)
            staged_dir.mkdir(parents=True, exist_ok=True)
            try:
                self.db.set_config("staged_path", str(staged_dir))
            except Exception:
                pass
        return staged_dir

    def _is_safe_zip_member(self, name: str) -> bool:
        if not name:
            return False
        cleaned = name.replace("\\", "/")
        posix = PurePosixPath(cleaned)
        if posix.is_absolute() or ".." in posix.parts:
            return False
        win = PureWindowsPath(cleaned)
        if win.is_absolute() or win.drive:
            return False
        return True

    def _prepare_clean_directory(self, path: Path) -> None:
        path = Path(path)
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)

    def _move_to_bucket(self, source: Path, target_dir: Path) -> Path:
        target_dir.mkdir(parents=True, exist_ok=True)
        destination = target_dir / source.name
        if not destination.exists():
            return source.replace(destination)

        counter = 1
        while True:
            candidate = target_dir / f"{source.stem}-{counter}{source.suffix}"
            if not candidate.exists():
                return source.replace(candidate)
            counter += 1

    def _group_archives_by_family(self, archives: list[Path]) -> list[list[Path]]:
        if not archives:
            return []

        known_stems = {archive_path.stem for archive_path in archives}
        groups: list[list[Path]] = []
        current_group: list[Path] = []
        current_family: str | None = None

        for archive_path in archives:
            family, _ = self._archive_family_key(archive_path, known_stems)
            if current_family != family:
                if current_group:
                    groups.append(current_group)
                current_group = [archive_path]
                current_family = family
            else:
                current_group.append(archive_path)

        if current_group:
            groups.append(current_group)

        return groups

    @staticmethod
    def _batched(items: list[Any], size: int) -> list[list[Any]]:
        if size <= 0:
            return [items]
        return [items[index : index + size] for index in range(0, len(items), size)]

    def process_pending_queue(
        self,
        pending_dir: Path,
        extracted_dir: Path,
        processed_dir: Path,
        failed_dir: Path,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> dict[str, Any]:
        pending_dir = Path(pending_dir)
        extracted_dir = Path(extracted_dir)
        processed_dir = Path(processed_dir)
        failed_dir = Path(failed_dir)

        self._reset_job_flags()
        self._reset_job_status()
        archives = self.list_pending_archives(pending_dir)
        total_archives = len(archives)
        if total_archives == 0:
            return {
                "success": True,
                "cancelled": False,
                "total_archives": 0,
                "processed_archives": 0,
                "failed_archives": 0,
                "results": [],
            }

        processed_count = 0
        failed_count = 0
        results: list[dict[str, Any]] = []
        self._update_job_status(
            running=True,
            current_step="Preparing batch...",
            current_archive_total=total_archives,
        )

        try:
            archive_groups = self._group_archives_by_family(archives)
            flat_index = 0
            for group in archive_groups:
                for position, archive_path in enumerate(group, start=1):
                    flat_index += 1
                    self._raise_if_cancelled()
                    self._prepare_clean_directory(extracted_dir)
                    self._update_job_status(
                        current_archive=archive_path.name,
                        current_archive_index=flat_index,
                        current_archive_total=total_archives,
                        current_step=f"Processing archive {flat_index} of {total_archives}",
                        overall_progress=(flat_index - 1) / total_archives,
                        download_total=0,
                        download_completed=0,
                        download_skipped=0,
                        download_failed=0,
                    )

                    finalize_family = position == len(group)

                    def archive_progress(progress: float, message: str) -> None:
                        overall = ((flat_index - 1) + progress) / total_archives
                        step_message = f"Processing archive {flat_index} of {total_archives}: {message}"
                        self._update_job_status(
                            current_archive=archive_path.name,
                            current_archive_index=flat_index,
                            current_archive_total=total_archives,
                            current_step=step_message,
                            overall_progress=min(overall, 1.0),
                        )
                        if progress_cb:
                            progress_cb(min(overall, 1.0), step_message)

                    success = self.process_archive(
                        archive_path,
                        extracted_dir,
                        archive_progress,
                        finalize=finalize_family,
                    )
                    moved_to = self._move_to_bucket(
                        archive_path,
                        processed_dir if success else failed_dir,
                    )
                    results.append(
                        {
                            "archive": archive_path.name,
                            "success": bool(success),
                            "destination": str(moved_to),
                        }
                    )
                    if success:
                        processed_count += 1
                    else:
                        failed_count += 1
                    self._prepare_clean_directory(extracted_dir)
                    if self.was_cancelled() or not success:
                        break
                if self.was_cancelled() or (results and not results[-1]["success"]):
                    break
        finally:
            self._prepare_clean_directory(extracted_dir)
            self._update_job_status(
                running=False,
                overall_progress=0.0 if self.was_cancelled() else (1.0 if total_archives else 0.0),
                current_step="Import cancelled." if self.was_cancelled() else "Idle",
            )

        return {
            "success": failed_count == 0 and not self.was_cancelled(),
            "cancelled": self.was_cancelled(),
            "total_archives": total_archives,
            "processed_archives": processed_count,
            "failed_archives": failed_count,
            "results": results,
        }

    def process_archive(
        self,
        archive_path: Path,
        extract_to: Path,
        progress_cb: ProgressCallback,
        finalize: bool = True,
    ):
        archive_path = Path(archive_path)
        extract_to = Path(extract_to)
        self._reset_runtime_state()

        try:
            self._raise_if_cancelled()
            progress_cb(0.02, "Checking disk space...")
            if not self._has_enough_space(archive_path, extract_to):
                logger.error("Insufficient disk space.")
                progress_cb(0.0, "Error: Not enough disk space!")
                return False

            progress_cb(0.08, "Extracting archive...")
            self._smart_extract(archive_path, extract_to, progress_cb, start=0.08, end=0.20)

            progress_cb(0.20, "Inspecting memories metadata...")
            self._download_memories_from_history(extract_to, progress_cb, start=0.20, end=0.32)

            progress_cb(0.32, "Staging physical media...")
            self._copy_extracted_media_to_raw(extract_to, progress_cb, start=0.32, end=0.38)

            if not finalize:
                self.current_root = self._find_snap_root(extract_to)
                if self.contains_export_markers(self.current_root):
                    self.db.set_config("root_path", str(self.current_root.absolute()))
                    progress_cb(0.70, "Staging metadata...")
                    self._create_staging_environment()
                elif not self._has_staged_metadata():
                    raise ValueError("No staged metadata found. Import a base archive with JSON first.")
                else:
                    progress_cb(0.70, "Reusing staged metadata...")

                progress_cb(1.0, "Archive staged.")
                return True

            return self.process_folder(
                extract_to,
                progress_cb,
                skip_extract=True,
                reset_cancel_state=False,
            )
        except IngestionCancelled:
            return self._handle_cancelled_run(
                progress_cb,
                extract_to=extract_to,
                remove_extract_dir=True,
            )
        except Exception as e:
            logger.error(f"Archive ingestion failed: {e}")
            progress_cb(0.0, f"Error: {str(e)}")
            return False
        finally:
            self._reset_runtime_state()

    def process_zip(self, zip_path: Path, extract_to: Path, progress_cb: Callable[[float, str], None]):
        return self.process_archive(zip_path, extract_to, progress_cb)

    def _get_archive_size(self, archive_path: Path) -> int:
        archive_path = Path(archive_path)
        suffix = archive_path.suffix.lower()

        if suffix == ".zip":
            with zipfile.ZipFile(archive_path, "r") as archive:
                return sum(member.file_size for member in archive.infolist())
        if suffix == ".rar":
            with rarfile.RarFile(archive_path, "r") as archive:
                return sum(member.file_size for member in archive.infolist())
        if suffix == ".7z":
            with py7zr.SevenZipFile(archive_path, "r") as archive:
                return sum(
                    int(getattr(member, "uncompressed", 0) or getattr(member, "size", 0) or 0)
                    for member in archive.list()
                )

        raise ValueError(f"Unsupported archive format: {archive_path.suffix}")

    def _has_enough_space(self, archive_path: Path, dest_path: Path) -> bool:
        try:
            total_uncompressed_size = self._get_archive_size(archive_path)
            check_path = dest_path if dest_path.exists() else dest_path.parent
            if not check_path.exists(): check_path = get_app_data_dir()
            total, used, free = shutil.disk_usage(check_path)
            required = total_uncompressed_size + (500 * 1024 * 1024)
            return free >= required
        except Exception:
            return True 

    def _smart_extract(
        self,
        archive_path: Path,
        dest_path: Path,
        progress_cb: Optional[Callable[[float, str], None]] = None,
        start: float = 0.0,
        end: float = 1.0
    ):
        archive_path = Path(archive_path)
        dest_path.mkdir(parents=True, exist_ok=True)

        suffix = archive_path.suffix.lower()
        if suffix == ".zip":
            self._smart_extract_zip(archive_path, dest_path, progress_cb, start, end)
            return
        if suffix == ".rar":
            self._smart_extract_rar(archive_path, dest_path, progress_cb, start, end)
            return
        if suffix == ".7z":
            self._smart_extract_7z(archive_path, dest_path, progress_cb, start, end)
            return

        raise ValueError(f"Unsupported archive format: {archive_path.suffix}")

    def _smart_extract_zip(
        self,
        archive_path: Path,
        dest_path: Path,
        progress_cb: Optional[Callable[[float, str], None]],
        start: float,
        end: float,
    ):
        with zipfile.ZipFile(archive_path, "r") as archive:
            members = archive.infolist()
            self._extract_members(
                members,
                dest_path,
                progress_cb,
                start,
                end,
                get_name=lambda member: member.filename,
                is_dir=lambda member: member.is_dir(),
                open_member=lambda member: archive.open(member),
            )

    def _smart_extract_rar(
        self,
        archive_path: Path,
        dest_path: Path,
        progress_cb: Optional[Callable[[float, str], None]],
        start: float,
        end: float,
    ):
        with rarfile.RarFile(archive_path, "r") as archive:
            members = archive.infolist()
            self._extract_members(
                members,
                dest_path,
                progress_cb,
                start,
                end,
                get_name=lambda member: member.filename,
                is_dir=lambda member: member.is_dir(),
                open_member=lambda member: archive.open(member),
            )

    def _smart_extract_7z(
        self,
        archive_path: Path,
        dest_path: Path,
        progress_cb: Optional[Callable[[float, str], None]],
        start: float,
        end: float,
    ):
        with py7zr.SevenZipFile(archive_path, "r") as archive:
            members = archive.getnames()

        total_members = len(members)
        processed = 0
        last_emit = start
        if progress_cb:
            progress_cb(start, "Extracting & Merging...")

        for member_name in members:
            self._raise_if_cancelled()
            normalized_name = member_name.replace("\\", "/").rstrip("/")
            is_dir = member_name.endswith("/")

            if normalized_name and not self._is_safe_zip_member(normalized_name):
                logger.warning(f"Skipping unsafe archive member: {member_name}")
                processed += 1
                last_emit = self._maybe_emit_progress(
                    progress_cb, start, end, processed, total_members, "Extracting & Merging...", last_emit
                )
                continue

            target = dest_path / normalized_name if normalized_name else dest_path
            if is_dir:
                target.mkdir(parents=True, exist_ok=True)
            elif self._should_extract_member(target):
                target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with py7zr.SevenZipFile(archive_path, "r") as archive:
                        archive.extract(path=dest_path, targets=[member_name])
                except Exception as exc:
                    logger.error(f"Failed to extract {member_name}: {exc}")

            processed += 1
            last_emit = self._maybe_emit_progress(
                progress_cb, start, end, processed, total_members, "Extracting & Merging...", last_emit
            )

        if progress_cb:
            progress_cb(end, "Extracting & Merging...")

    def _should_extract_member(self, target: Path) -> bool:
        return not target.exists()

    def _extract_members(
        self,
        members: List[Any],
        dest_path: Path,
        progress_cb: Optional[Callable[[float, str], None]],
        start: float,
        end: float,
        get_name: Callable[[Any], str],
        is_dir: Callable[[Any], bool],
        open_member: Callable[[Any], Any],
    ):
        total_members = len(members)
        processed = 0
        last_emit = start
        if progress_cb:
            progress_cb(start, "Extracting & Merging...")

        for member in members:
            self._raise_if_cancelled()
            member_name = get_name(member)

            if not self._is_safe_zip_member(member_name):
                logger.warning(f"Skipping unsafe archive member: {member_name}")
                processed += 1
                last_emit = self._maybe_emit_progress(
                    progress_cb, start, end, processed, total_members, "Extracting & Merging...", last_emit
                )
                continue

            target = dest_path / member_name
            if is_dir(member):
                target.mkdir(parents=True, exist_ok=True)
            elif self._should_extract_member(target):
                target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with open_member(member) as src, open(target, "wb") as dst:
                        shutil.copyfileobj(src, dst)
                except Exception as exc:
                    logger.error(f"Failed to extract {member_name}: {exc}")

            processed += 1
            last_emit = self._maybe_emit_progress(
                progress_cb, start, end, processed, total_members, "Extracting & Merging...", last_emit
            )

        if progress_cb:
            progress_cb(end, "Extracting & Merging...")

    def _memory_json_path(self, root: Path) -> Optional[Path]:
        for candidate in (root / "json" / "memories_history.json", root / "memories_history.json"):
            if candidate.exists():
                return candidate
        return None

    @staticmethod
    def _extract_mid_from_url(url: str) -> Optional[str]:
        if not url:
            return None
        try:
            params = parse_qs(urlparse(url).query)
        except Exception:
            return None
        values = params.get("mid")
        if not values:
            return None
        candidate = values[0].strip()
        return candidate or None

    @staticmethod
    def _safe_iso_date(raw_date: str) -> Optional[str]:
        if not raw_date:
            return None
        try:
            return datetime.fromisoformat(raw_date.replace(" UTC", "")).strftime("%Y-%m-%d")
        except Exception:
            return raw_date[:10] if len(raw_date) >= 10 else None

    @classmethod
    def build_memory_download_name(
        cls,
        item: dict[str, Any],
        content_type: Optional[str] = None,
    ) -> Optional[str]:
        url = str(item.get("Media Download Url") or item.get("Download Link") or "").strip()
        mid = cls._extract_mid_from_url(url)
        date_part = cls._safe_iso_date(str(item.get("Date") or "").strip())
        if not mid or not date_part:
            return None

        extension = None
        if content_type:
            extension = _MEMORY_CONTENT_TYPE_EXTENSIONS.get(
                content_type.split(";", 1)[0].strip().lower()
            )
        if not extension:
            media_type = str(item.get("Media Type") or "").strip().lower()
            if media_type == "video":
                extension = ".mp4"
            elif media_type == "image":
                extension = ".jpg"
        if not extension:
            extension = mimetypes.guess_extension(content_type or "") if content_type else None
        if not extension:
            extension = ".bin"
        return f"{date_part}_{mid}-main{extension}"

    def _download_memories_from_history(
        self,
        extracted_root: Path,
        progress_cb: Optional[ProgressCallback],
        start: float,
        end: float,
    ) -> None:
        json_path = self._memory_json_path(extracted_root)
        if not json_path or not json_path.exists():
            self._update_job_status(
                download_total=0,
                download_completed=0,
                download_skipped=0,
                download_failed=0,
            )
            if progress_cb:
                progress_cb(end, "No memories download metadata found.")
            return

        try:
            payload = self._load_json_file(json_path)
        except Exception as exc:
            logger.debug(f"Failed to read memories download metadata from {json_path}: {exc}")
            if progress_cb:
                progress_cb(end, "Skipping memories download metadata.")
            return

        saved_media = payload.get("Saved Media", []) if isinstance(payload, dict) else []
        if not isinstance(saved_media, list):
            if progress_cb:
                progress_cb(end, "Skipping malformed memories metadata.")
            return

        extracted_memories_dir = extracted_root / "memories"
        raw_memories_dir = get_raw_media_dir() / "memories"
        extracted_memories_dir.mkdir(parents=True, exist_ok=True)
        raw_memories_dir.mkdir(parents=True, exist_ok=True)

        download_jobs: list[tuple[str, Path, dict[str, Any]]] = []
        skipped = 0
        total_candidates = 0
        for item in saved_media:
            self._raise_if_cancelled()
            if not isinstance(item, dict):
                continue
            url = str(item.get("Media Download Url") or item.get("Download Link") or "").strip()
            if not url:
                continue
            filename = self.build_memory_download_name(item)
            if not filename:
                continue
            total_candidates += 1
            raw_target = raw_memories_dir / filename
            extract_target = extracted_memories_dir / filename
            if raw_target.exists() or extract_target.exists():
                skipped += 1
                continue
            download_jobs.append((url, extract_target, item))

        self._update_job_status(
            download_total=total_candidates,
            download_completed=0,
            download_skipped=skipped,
            download_failed=0,
        )
        if total_candidates == 0:
            if progress_cb:
                progress_cb(end, "No memories downloads needed.")
            return

        completed = 0
        failed = 0
        processed = skipped
        last_emit = start
        if progress_cb:
            progress_cb(start, "Downloading memories...")

        def worker(url: str, destination: Path, item: dict[str, Any]) -> tuple[bool, Path]:
            timeout = httpx.Timeout(30.0, connect=10.0)
            try:
                with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                    response = client.get(url)
                if response.status_code in {403, 404}:
                    return False, destination
                response.raise_for_status()
                resolved_name = self.build_memory_download_name(
                    item,
                    content_type=response.headers.get("content-type"),
                )
                if resolved_name:
                    destination = destination.with_name(resolved_name)
                destination.parent.mkdir(parents=True, exist_ok=True)
                with open(destination, "wb") as handle:
                    handle.write(response.content)
                return True, destination
            except (httpx.HTTPError, OSError) as exc:
                logger.debug(f"Failed to download memory {url}: {exc}")
                return False, destination

        if download_jobs:
            with ThreadPoolExecutor(max_workers=min(8, max(len(download_jobs), 1))) as executor:
                futures = [
                    executor.submit(worker, url, destination, item)
                    for url, destination, item in download_jobs
                ]
                for future in as_completed(futures):
                    self._raise_if_cancelled()
                    success, _ = future.result()
                    processed += 1
                    if success:
                        completed += 1
                    else:
                        failed += 1
                    self._update_job_status(
                        download_total=total_candidates,
                        download_completed=completed,
                        download_skipped=skipped,
                        download_failed=failed,
                    )
                    last_emit = self._maybe_emit_progress(
                        progress_cb,
                        start,
                        end,
                        processed,
                        total_candidates,
                        "Downloading memories...",
                        last_emit,
                    )

        if progress_cb:
            progress_cb(end, "Downloading memories...")

    def _copy_extracted_media_to_raw(
        self,
        extracted_root: Path,
        progress_cb: Optional[ProgressCallback],
        start: float,
        end: float,
    ) -> None:
        raw_root = get_raw_media_dir()
        candidates: list[Path] = []
        staged_files: list[Path] = []
        copy_end = start + ((end - start) * 0.75)
        for folder_name in ("chat_media", "memories"):
            source_dir = extracted_root / folder_name
            if source_dir.exists():
                candidates.extend(path for path in source_dir.rglob("*") if path.is_file())

        total = len(candidates)
        if total == 0:
            if progress_cb:
                progress_cb(end, "No physical media to stage.")
            return

        processed = 0
        last_emit = start
        if progress_cb:
            progress_cb(start, "Staging physical media...")
        for source_file in candidates:
            self._raise_if_cancelled()
            relative = source_file.relative_to(extracted_root)
            destination = raw_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not destination.exists():
                try:
                    shutil.copy2(source_file, destination)
                except Exception as exc:
                    logger.debug(f"Failed to copy media {source_file} to {destination}: {exc}")
                    processed += 1
                    last_emit = self._maybe_emit_progress(
                        progress_cb,
                        start,
                        copy_end,
                        processed,
                        total,
                        "Staging physical media...",
                        last_emit,
                    )
                    continue
            if destination.exists():
                staged_files.append(destination)
            processed += 1
            last_emit = self._maybe_emit_progress(
                progress_cb,
                start,
                copy_end,
                processed,
                total,
                "Staging physical media...",
                last_emit,
            )

        if progress_cb:
            progress_cb(copy_end, "Staging physical media...")

        self._dispatch_media_precompute(
            staged_files,
            progress_cb=progress_cb,
            start=copy_end,
            end=end,
        )

    @staticmethod
    def _media_variant_rank(path: Path) -> tuple[int, str]:
        stem = path.stem.lower()
        suffix = path.suffix.lower()

        if "_media~" in stem:
            return (0, path.name.lower())
        if suffix in VIDEO_EXTENSIONS and stem.endswith(("_video", "-video")):
            return (0, path.name.lower())
        if suffix in IMAGE_EXTENSIONS and stem.endswith(("_image", "-image")):
            return (0, path.name.lower())
        if stem.endswith(("_main", "-main")):
            return (1, path.name.lower())
        if stem.endswith(("_media", "-media")):
            return (2, path.name.lower())
        return (3, path.name.lower())

    def _dispatch_media_precompute(
        self,
        staged_files: list[Path],
        progress_cb: Optional[ProgressCallback] = None,
        start: float = 0.0,
        end: float = 1.0,
    ) -> None:
        if not staged_files:
            if progress_cb:
                progress_cb(end, "Queueing media precompute...")
            return

        groups: dict[tuple[str, str], dict[str, list[Path] | Path | None]] = {}
        for candidate in staged_files:
            self._raise_if_cancelled()
            if not candidate.exists():
                continue

            group_key = (str(candidate.parent.resolve()), normalize_media_stem(candidate.stem))
            group = groups.setdefault(group_key, {"main": [], "overlay": None})
            if is_overlay_variant(candidate):
                current_overlay = group.get("overlay")
                if current_overlay is None or candidate.name.lower() < Path(current_overlay).name.lower():
                    group["overlay"] = candidate
                continue

            group["main"].append(candidate)

        queue_targets: list[tuple[Path, Path | None]] = []
        queued_paths: set[str] = set()
        for group in groups.values():
            main_files = [
                path for path in group["main"]
                if isinstance(path, Path) and path.exists()
            ]
            if not main_files:
                continue

            overlay = group["overlay"] if isinstance(group["overlay"], Path) else None
            image_candidates = [path for path in main_files if path.suffix.lower() in IMAGE_EXTENSIONS]
            video_candidates = [path for path in main_files if path.suffix.lower() in VIDEO_EXTENSIONS]

            for candidate_set in (image_candidates, video_candidates):
                if not candidate_set:
                    continue
                preferred = min(candidate_set, key=self._media_variant_rank)
                queue_key = str(preferred.resolve())
                if queue_key in queued_paths:
                    continue
                queued_paths.add(queue_key)
                queue_targets.append((preferred, overlay))

        total_targets = len(queue_targets)
        processed_targets = 0
        last_emit = start
        if progress_cb:
            progress_cb(start, "Queueing media precompute...")

        for preferred, overlay in queue_targets:
            self._raise_if_cancelled()
            try:
                self.processor.queue_precompute(
                    preferred,
                    overlay_path=overlay,
                    resolve_variants=False,
                )
            except Exception as exc:
                logger.error(
                    "Failed to queue media precompute for '%s': %s",
                    preferred,
                    exc,
                    exc_info=True,
                )
            processed_targets += 1
            last_emit = self._maybe_emit_progress(
                progress_cb,
                start,
                end,
                processed_targets,
                total_targets,
                "Queueing media precompute...",
                last_emit,
            )

        if progress_cb:
            progress_cb(end, "Queueing media precompute...")

    def _has_staged_metadata(self) -> bool:
        staged_dir = self._get_staged_dir()
        return any(
            (staged_dir / filename).exists()
            for filename in ("chat_history.json", "snap_history.json", "memories_history.json")
        )

    def process_folder(
        self,
        folder_path: Path,
        progress_cb: ProgressCallback,
        skip_extract: bool = False,
        reset_cancel_state: bool = True,
    ):
        if reset_cancel_state:
            self._reset_job_flags()
        self._reset_runtime_state()

        try:
            folder_path = Path(folder_path)
            self._raise_if_cancelled()
            if not skip_extract:
                progress_cb(0.02, "Verifying folder...")
            self.current_root = self._find_snap_root(folder_path)
            self.media_match_mode = (self.db.get_config("media_match_mode") or "strict").lower()
            has_new_metadata = self.contains_export_markers(self.current_root)

            if has_new_metadata:
                logger.info(f"Root detected at: {self.current_root}")
                self.db.set_config("root_path", str(self.current_root.absolute()))
                progress_cb(0.40, "Staging metadata...")
                self._create_staging_environment()
            elif not self._has_staged_metadata():
                raise ValueError("No staged metadata found. Import a base archive with JSON first.")
            else:
                progress_cb(0.40, "Reusing staged metadata...")

            progress_cb(0.48, "Indexing media...")
            self._index_and_bucket_media(get_raw_media_dir(), progress_cb, start=0.48, end=0.78)

            progress_cb(0.80, "Identifying user...")
            self._parse_account_info()

            progress_cb(0.82, "Parsing chats...")
            self._parse_chats(progress_cb, start=0.82, end=0.92)
            self._parse_snap_history(progress_cb, start=0.92, end=0.99)

            progress_cb(1.0, "Complete!")
            self.db.set_config("last_ingested_at", datetime.now(UTC).isoformat())
            return True
        except IngestionCancelled:
            return self._handle_cancelled_run(progress_cb)
        except Exception as e:
            logger.error(f"Folder ingestion failed: {e}")
            progress_cb(0.0, f"Error: {e}")
            return False
        finally:
            self._reset_runtime_state()
            if reset_cancel_state:
                self._cancel_requested.clear()

    def rebuild_chat_media_links(
        self,
        progress_cb: Optional[ProgressCallback] = None
    ) -> bool:
        try:
            if not self._has_staged_metadata():
                if progress_cb:
                    progress_cb(0.0, "Error: No staged metadata found.")
                return False

            self.media_match_mode = (self.db.get_config("media_match_mode") or "strict").lower()

            if progress_cb:
                progress_cb(0.20, "Indexing media...")
            self._index_and_bucket_media(get_raw_media_dir(), progress_cb, start=0.20, end=0.70)

            self.db.clear_messages()
            self._parse_chats(progress_cb, start=0.72, end=0.88)
            self._parse_snap_history(progress_cb, start=0.88, end=1.0)

            if progress_cb:
                progress_cb(1.0, "Rebuild Complete!")
            return True
        except Exception as e:
            logger.error(f"Rebuild failed: {e}")
            if progress_cb:
                progress_cb(0.0, f"Error: {e}")
            return False

    def _find_snap_root(self, path: Path) -> Path:
        if not path.exists() or not path.is_dir():
            return path
        if (path / "json").exists() or (path / "html").exists(): return path
        for p in path.iterdir():
            if p.is_dir() and ((p / "json").exists() or (p / "html").exists()): return p
        return path

    def _create_staging_environment(self):
        self._raise_if_cancelled()
        staged_dir = self._get_staged_dir(create=True)
        self._merge_chats_to_stage(staged_dir)
        self._merge_memories_to_stage(staged_dir)
        
        candidates = set()
        json_dir = self.current_root / "json"
        if json_dir.exists():
            for p in json_dir.glob("*.json"): candidates.add(p.name)
        for p in self.current_root.glob("*.json"): candidates.add(p.name)
        
        special_files = {"chat_history.json", "memories_history.json"}
        for fname in candidates:
            self._raise_if_cancelled()
            if fname in special_files: continue
            src = None
            if json_dir.exists() and (json_dir / fname).exists(): src = json_dir / fname
            elif (self.current_root / fname).exists(): src = self.current_root / fname
            if src and src.exists():
                try: shutil.copy2(src, staged_dir / fname)
                except Exception as e: logger.error(f"Failed to stage {fname}: {e}")

    @staticmethod
    def _chat_message_signature(message: dict[str, Any]) -> str:
        sender = str(message.get("From") or "")
        created = str(message.get("Created") or "")
        msg_type = str(message.get("Media Type") or "")
        content = "" if message.get("Content") is None else str(message.get("Content"))
        media_ids = str(message.get("Media IDs") or "")
        return "|".join([sender, created, msg_type, content, media_ids])

    def _merge_chats_to_stage(self, staged_dir: Path):
        self._raise_if_cancelled()
        staged_file = staged_dir / "chat_history.json"
        master_chats = {}
        if staged_file.exists():
            try:
                with open(staged_file, "r", encoding="utf-8") as f: master_chats = json.load(f)
            except Exception as exc:
                logger.debug(f"Failed to read staged chat history: {exc}")
        new_chats = {}
        json_src = self.current_root / "json" / "chat_history.json"
        html_src = self.current_root / "html" / "chat_history"
        if json_src.exists():
            try:
                with open(json_src, "r", encoding="utf-8") as f: new_chats = json.load(f)
            except Exception as exc:
                logger.debug(f"Failed to read chat_history.json: {exc}")
        elif html_src.exists():
            new_chats = self._parse_html_directory(html_src)
        for user_key, messages in new_chats.items():
            self._raise_if_cancelled()
            if not isinstance(messages, list):
                continue
            master_chats.setdefault(user_key, [])
            existing_sigs = {
                self._chat_message_signature(msg)
                for msg in master_chats[user_key]
                if isinstance(msg, dict)
            }
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                sig = self._chat_message_signature(msg)
                if sig not in existing_sigs:
                    master_chats[user_key].append(msg)
                    existing_sigs.add(sig)
        with open(staged_file, "w", encoding="utf-8") as f:
            json.dump(master_chats, f, indent=4)

    def _memory_entry_signature(self, item: dict[str, Any]) -> str:
        url = str(item.get("Media Download Url") or item.get("Download Link") or "").strip()
        mid = self._extract_mid_from_url(url)
        if mid:
            return f"mid:{mid}"
        return f"fallback:{item.get('Date', '')}|{url}"

    def _merge_memories_to_stage(self, staged_dir: Path):
        self._raise_if_cancelled()
        staged_file = staged_dir / "memories_history.json"
        master_mems = {"Saved Media": []}
        if staged_file.exists():
            try:
                with open(staged_file, "r", encoding="utf-8") as f: master_mems = json.load(f)
            except Exception as exc:
                logger.debug(f"Failed to read staged memories history: {exc}")
        if not isinstance(master_mems, dict):
            master_mems = {"Saved Media": []}
        if not isinstance(master_mems.get("Saved Media"), list):
            master_mems["Saved Media"] = []

        json_src = self._memory_json_path(self.current_root)
        if json_src and json_src.exists():
            try:
                with open(json_src, "r", encoding="utf-8") as f:
                    new_data = json.load(f)
                existing_entries = {
                    self._memory_entry_signature(item)
                    for item in master_mems.get("Saved Media", [])
                    if isinstance(item, dict)
                }
                for item in new_data.get("Saved Media", []):
                    self._raise_if_cancelled()
                    if not isinstance(item, dict):
                        continue
                    signature = self._memory_entry_signature(item)
                    if signature not in existing_entries:
                        master_mems["Saved Media"].append(item)
                        existing_entries.add(signature)
                with open(staged_file, "w", encoding="utf-8") as f:
                    json.dump(master_mems, f, indent=4)
            except Exception as exc:
                logger.debug(f"Failed to merge memories history: {exc}")

    def _parse_html_directory(self, html_dir: Path) -> Dict:
        chats = {}
        for html_file in html_dir.glob("*.html"):
            self._raise_if_cancelled()
            try:
                with open(html_file, "r", encoding="utf-8") as f:
                    soup = BeautifulSoup(f, "html.parser")
                title = soup.find("title")
                friend_name = title.text.replace("Snapchat - ", "").strip() if title else html_file.stem
                messages = []
                rows = soup.find_all("tr")
                for row in rows:
                    self._raise_if_cancelled()
                    cols = row.find_all("td")
                    if len(cols) >= 3:
                        sender = cols[0].text.strip()
                        msg_type = cols[1].text.strip()
                        timestamp = cols[2].text.strip().replace(" UTC", "")
                        content = cols[3].text.strip() if len(cols) > 3 else ""
                        messages.append({
                            "From": sender,
                            "Content": content,
                            "Created": timestamp + " UTC",
                            "Media Type": msg_type if msg_type in ["MEDIA", "IMAGE", "VIDEO"] else "TEXT"
                        })
                if messages:
                    chats[friend_name] = messages
            except Exception as exc:
                logger.debug(f"Failed to parse html chat file {html_file}: {exc}")
        return chats

    def _index_and_bucket_media(
        self,
        root: Path,
        progress_cb: Optional[ProgressCallback] = None,
        start: float = 0.0,
        end: float = 1.0
    ):
        self.media_buckets = {}
        self.media_id_map = {}
        media_folders = [root / "chat_media", root / "memories"]
        date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")
        video_exts = {".mp4", ".mov", ".avi", ".webm", ".mkv", ".m4v"}

        def extract_media_id(stem_id: str) -> Optional[str]:
            parts = stem_id.split("_", 1)
            if len(parts) != 2:
                return None
            date_part, id_part = parts
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_part):
                return None
            if not id_part.startswith("b~"):
                return None
            return id_part
        
        def get_stem_id(path: Path) -> str:
            return normalize_media_stem(path.stem)

        def is_video(path: Path) -> bool:
            return path.suffix.lower() in video_exts

        def main_rank(path: Path) -> int:
            stem = path.stem.lower()
            if "_media~" in stem.lower():
                return 0
            if is_video(path):
                if stem.endswith(("_video", "-video")):
                    return 0
            else:
                if stem.endswith(("_image", "-image")):
                    return 0
            if stem.endswith(("_main", "-main")):
                return 1
            if stem.endswith(("_media", "-media")):
                return 2
            return 3

        files_by_folder: dict[Path, list[Path]] = {}
        for folder in media_folders:
            if not folder.exists():
                continue
            files_by_folder[folder] = [
                file for file in folder.rglob("*")
                if file.is_file() and not file.name.startswith(".")
            ]
        total_files = sum(len(files) for files in files_by_folder.values())

        processed_files = 0
        last_emit = start
        if progress_cb:
            progress_cb(start, "Indexing Media...")

        for folder in media_folders:
            files = files_by_folder.get(folder)
            if not files:
                continue
            is_chat_media = (folder.name == "chat_media")
            
            groups = {}
            batch = []
            for file in files:
                self._raise_if_cancelled()
                processed_files += 1
                last_emit = self._maybe_emit_progress(
                    progress_cb, start, end, processed_files, total_files, "Indexing Media...", last_emit
                )
                stem_id = get_stem_id(file)
                if stem_id not in groups:
                    groups[stem_id] = {"main": None, "overlay": None, "ts": None}
                
                is_overlay_file = is_overlay_variant(file)
                
                if is_overlay_file:
                    groups[stem_id]["overlay"] = file
                else:
                    main = groups[stem_id]["main"]
                    if not main:
                        groups[stem_id]["main"] = file
                        groups[stem_id]["ts"] = self._get_best_timestamp(file, date_pattern)
                    else:
                        if is_video(file) and not is_video(main):
                            groups[stem_id]["main"] = file
                            groups[stem_id]["ts"] = self._get_best_timestamp(file, date_pattern)
                        elif is_video(file) == is_video(main) and main_rank(file) < main_rank(main):
                            groups[stem_id]["main"] = file
                            groups[stem_id]["ts"] = self._get_best_timestamp(file, date_pattern)

            for stem_id, data in groups.items():
                self._raise_if_cancelled()
                main_file = data.get("main")
                if not main_file: continue
                
                best_ts = data["ts"]
                ftype = "video" if main_file.suffix.lower() in video_exts else "image"
                
                overlay_path = str(data["overlay"].absolute()) if data.get("overlay") else None

                if is_chat_media:
                    media_id = extract_media_id(stem_id)
                    if media_id:
                        if media_id not in self.media_id_map:
                            self.media_id_map[media_id] = str(main_file.absolute())
                        else:
                            logger.debug(f"Duplicate media id detected: {media_id}")
                
                try:
                    file_size = main_file.stat().st_size
                except Exception as exc:
                    logger.debug(f"Failed to stat media file {main_file}: {exc}")
                    continue

                batch.append(MediaAsset(
                    asset_id=f"{folder.name}:{stem_id}",
                    file_path=str(main_file.absolute()),
                    file_type=ftype,
                    file_size=file_size,
                    created_at=best_ts,
                    overlay_path=overlay_path
                ))

                if is_chat_media and best_ts:
                    date_key = best_ts.strftime("%Y-%m-%d")
                    if date_key not in self.media_buckets:
                        self.media_buckets[date_key] = []
                    
                    self.media_buckets[date_key].append({
                        "path": str(main_file.absolute()),
                        "ts": best_ts,
                        "claimed": False,
                        "overlay": overlay_path
                    })

                if len(batch) >= self.chunk_size:
                    self.db.add_assets_batch(batch)
                    batch = []
        
        if batch: self.db.add_assets_batch(batch)
        
        for date_key in self.media_buckets:
            self.media_buckets[date_key].sort(key=lambda x: x["ts"])

        if progress_cb:
            progress_cb(end, "Indexing Media...")

    def _get_best_timestamp(self, file: Path, pattern) -> Optional[datetime]:
        match = pattern.search(file.name)
        if not match: return None
        try:
            filename_date = datetime.strptime(match.group(1), "%Y-%m-%d")
        except: return None

        try:
            stats = file.stat()
            meta_ts = datetime.fromtimestamp(stats.st_mtime)
            if meta_ts.year == filename_date.year and meta_ts.month == filename_date.month:
                return meta_ts
        except: pass
        return filename_date

    def _parse_account_info(self):
        staged_dir = self._get_staged_dir()
        potential_files = ["account.json", "user_profile.json"]
        username, display_name = None, None
        
        for fname in potential_files:
            fpath = staged_dir / fname
            if fpath.exists():
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        basic = data.get("Basic Information", data)
                        if not username: username = basic.get("Username")
                        if not display_name: display_name = basic.get("Name")
                except Exception as exc:
                    logger.debug(f"Failed to parse account info from {fpath}: {exc}")
        
        if username:
            self.db.set_config("owner_username", username)
            self.db.upsert_user(username, display_name)
            if display_name: self.db.set_config("owner_display_name", display_name)

    def _collect_conversation_ids(self, payload: Any) -> set[str]:
        conversation_ids: set[str] = set()
        if not isinstance(payload, dict):
            return conversation_ids

        for key, content in payload.items():
            self._raise_if_cancelled()
            if isinstance(content, list):
                conversation_ids.add(str(key))
            elif isinstance(content, dict):
                for nested_key, nested_content in content.items():
                    self._raise_if_cancelled()
                    if isinstance(nested_content, list):
                        conversation_ids.add(str(nested_key))

        return conversation_ids

    def _load_json_file(self, path: Path) -> Any:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _reset_messages_for_import(self):
        staged_dir = self._get_staged_dir()
        conversation_ids: set[str] = set()

        for filename in ("chat_history.json", "snap_history.json"):
            self._raise_if_cancelled()
            staged_path = staged_dir / filename
            if not staged_path.exists():
                continue

            try:
                payload = self._load_json_file(staged_path)
            except Exception as exc:
                logger.debug(f"Failed to inspect staged file {staged_path}: {exc}")
                continue

            conversation_ids.update(self._collect_conversation_ids(payload))

        if conversation_ids:
            self.db.clear_messages_for_conversations(sorted(conversation_ids))

    def _parse_chats(
        self,
        progress_cb: Optional[Callable[[float, str], None]] = None,
        start: float = 0.0,
        end: float = 1.0
    ):
        staged_chat_path = self._get_staged_dir() / "chat_history.json"
        if staged_chat_path.exists():
            self._parse_json_chats(staged_chat_path, progress_cb, start=start, end=end)

    def _parse_snap_history(
        self,
        progress_cb: Optional[Callable[[float, str], None]] = None,
        start: float = 0.0,
        end: float = 1.0
    ):
        staged_snap_path = self._get_staged_dir() / "snap_history.json"
        if not staged_snap_path.exists():
            return
        try:
            with open(staged_snap_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Snap history parse error: {e}")
            return

        snap_entries = []
        total = 0

        for conversation_id, messages in (data or {}).items():
            self._raise_if_cancelled()
            if not isinstance(messages, list):
                continue

            found_title = None
            for entry in messages:
                self._raise_if_cancelled()
                if t := entry.get("Conversation Title"):
                    found_title = t
                    break
            if found_title:
                self.db.update_conversation_title(conversation_id, found_title)

            for entry in messages:
                self._raise_if_cancelled()
                try:
                    media_type = (entry.get("Media Type") or "").strip()
                    if not media_type or media_type == "TEXT":
                        continue
                    ts_raw = entry.get("Created", "")
                    clean_ts = ts_raw.replace(" UTC", "")
                    ts = datetime.fromisoformat(clean_ts)
                    snap_entries.append({
                        "conversation_id": conversation_id,
                        "sender": entry.get("From", "Unknown"),
                        "ts": ts,
                        "type": media_type
                    })
                except Exception:
                    continue

        total = len(snap_entries)
        if total == 0:
            return

        # Group by date and assign unclaimed media deterministically
        by_date = {}
        for entry in snap_entries:
            date_key = entry["ts"].strftime("%Y-%m-%d")
            by_date.setdefault(date_key, []).append(entry)

        for date_key, entries in by_date.items():
            bucket = self.media_buckets.get(date_key, [])
            candidates = [
                b for b in bucket
                if not b["claimed"]
                and "overlay" not in b["path"]
                and "thumbnail" not in b["path"]
            ]
            candidates.sort(key=lambda x: x["path"])
            entries.sort(key=lambda x: x["ts"])
            for entry, media in zip(entries, candidates):
                entry["media"] = media["path"]
                media["claimed"] = True

        messages_by_conversation: dict[str, list[Message]] = {}
        for entry in snap_entries:
            media_path = entry.get("media")
            if not media_path:
                continue
            messages_by_conversation.setdefault(entry["conversation_id"], []).append(
                Message(
                    sender=entry["sender"],
                    content="",
                    timestamp=entry["ts"],
                    msg_type=entry["type"],
                    media_refs=[media_path],
                    source="snap",
                )
            )

        processed = 0
        last_emit = start
        if progress_cb:
            progress_cb(start, "Parsing Snap History...")

        for conversation_id, conversation_messages in messages_by_conversation.items():
            for chunk in self._batched(conversation_messages, self.message_chunk_size):
                self._raise_if_cancelled()
                self.db.add_messages_batch(conversation_id, chunk)
                processed += len(chunk)
                last_emit = self._maybe_emit_progress(
                    progress_cb, start, end, processed, total, "Parsing Snap History...", last_emit
                )

        if processed < total:
            last_emit = self._maybe_emit_progress(
                progress_cb, start, end, total, total, "Parsing Snap History...", last_emit
            )

    def _count_messages(self, data: Any) -> int:
        total = 0
        if isinstance(data, dict):
            for _, content in data.items():
                if isinstance(content, list):
                    total += len(content)
                elif isinstance(content, dict):
                    for _, msgs in content.items():
                        if isinstance(msgs, list):
                            total += len(msgs)
        return total

    def _parse_json_chats(
        self,
        json_path: Path,
        progress_cb: Optional[Callable[[float, str], None]] = None,
        start: float = 0.0,
        end: float = 1.0
    ):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            total_messages = self._count_messages(data)
            progress_state = {
                "processed": 0,
                "total": total_messages,
                "last": start
            }
            if progress_cb:
                progress_cb(start, "Parsing Chats...")
            if isinstance(data, dict):
                for key, content in data.items():
                    self._raise_if_cancelled()
                    if isinstance(content, list):
                        self._process_json_message_list(
                            key, content, progress_cb, progress_state, start=start, end=end
                        )
                    elif isinstance(content, dict):
                        for user, msgs in content.items():
                            self._raise_if_cancelled()
                            if isinstance(msgs, list):
                                self._process_json_message_list(
                                    user, msgs, progress_cb, progress_state, start=start, end=end
                                )
            if progress_cb:
                progress_cb(end, "Parsing Chats...")
        except Exception as e:
            logger.error(f"JSON Parsing error: {e}")

    def _process_json_message_list(
        self,
        conversation_id: str,
        messages: List[Dict[str, Any]],
        progress_cb: Optional[Callable[[float, str], None]] = None,
        progress_state: Optional[Dict[str, Any]] = None,
        start: float = 0.0,
        end: float = 1.0
    ):
        # NEW: Check for Conversation Title (Group Name)
        found_title = None
        for entry in messages:
            self._raise_if_cancelled()
            if t := entry.get("Conversation Title"):
                found_title = t
                break
        
        if found_title:
            self.db.update_conversation_title(conversation_id, found_title)

        message_models: list[Message] = []
        for entry in messages:
            self._raise_if_cancelled()
            try:
                sender = entry.get("From", "Unknown")
                content = entry.get("Content", "")
                ts_raw = entry.get("Created", "")
                media_type = entry.get("Media Type", "TEXT")
                
                clean_ts = ts_raw.replace(" UTC", "")
                ts = datetime.fromisoformat(clean_ts)
                
                linked_media = self._find_media_for_message(
                    ts,
                    self._parse_media_ids(entry.get("Media IDs", "")),
                    media_type,
                )
                message_models.append(
                    Message(
                        sender=sender,
                        content=content if content else "",
                        timestamp=ts,
                        msg_type=media_type,
                        media_refs=linked_media,
                    )
                )
            except: continue

        message_models.sort(key=lambda item: item.timestamp)

        for chunk in self._batched(message_models, self.message_chunk_size):
            self._raise_if_cancelled()
            self.db.add_messages_batch(
                conversation_id,
                chunk,
                display_name=found_title,
            )
            if progress_state is not None:
                progress_state["processed"] += len(chunk)
                progress_state["last"] = self._maybe_emit_progress(
                    progress_cb,
                    start,
                    end,
                    progress_state["processed"],
                    progress_state["total"],
                    "Parsing Chats...",
                    progress_state["last"]
                )

    def _parse_media_ids(self, raw: Any) -> List[str]:
        if not raw:
            return []
        if not isinstance(raw, str):
            raw = str(raw)
        parts = re.split(r"[|,]", raw)
        return [p.strip() for p in parts if p.strip()]

    def _mark_bucket_claimed(self, timestamp: Optional[datetime], paths: List[str]):
        if not timestamp or not paths:
            return
        date_key = timestamp.strftime("%Y-%m-%d")
        bucket = self.media_buckets.get(date_key)
        if not bucket:
            return
        path_set = set(paths)
        for entry in bucket:
            if entry["path"] in path_set:
                entry["claimed"] = True

    def _find_media_for_message(self, timestamp: datetime, media_ids: List[str], msg_type: str) -> List[str]:
        # 1) Primary: explicit Media IDs
        if media_ids and self.media_id_map:
            paths = []
            for mid in media_ids:
                path = self.media_id_map.get(mid)
                if path:
                    paths.append(path)
            if paths:
                self._mark_bucket_claimed(timestamp, paths)
                return paths

        # 2) Strict mode: no fallback
        if (self.media_match_mode or "strict").lower() != "soft":
            return []

        # 3) Soft fallback only for non-text messages
        if (msg_type or "").upper() == "TEXT":
            return []

        date_key = timestamp.strftime("%Y-%m-%d")
        bucket = self.media_buckets.get(date_key)
        if not bucket:
            return []

        # Try a tight timestamp match first (only when entry has a real time component)
        for entry in bucket:
            if entry["claimed"]:
                continue
            if "overlay" in entry["path"] or "thumbnail" in entry["path"]:
                continue
            ts = entry.get("ts")
            if not ts:
                continue
            if ts.hour != 0 or ts.minute != 0 or ts.second != 0:
                delta = abs((ts - timestamp).total_seconds())
                if delta < 5:
                    entry["claimed"] = True
                    return [entry["path"]]

        # Conservative fallback: only if exactly one candidate remains
        candidates = [
            entry for entry in bucket
            if not entry["claimed"]
            and "overlay" not in entry["path"]
            and "thumbnail" not in entry["path"]
        ]
        if len(candidates) == 1:
            candidates[0]["claimed"] = True
            return [candidates[0]["path"]]
        return []
