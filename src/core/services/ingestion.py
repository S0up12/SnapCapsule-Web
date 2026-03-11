import json
import re
import shutil
import threading
import zipfile
from datetime import datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Callable, Dict, List, Optional

import py7zr
import rarfile

from core.database.schema import DatabaseManager
from core.models import Message, MediaAsset
from core.utils.logger import get_logger
from core.utils.paths import get_app_data_dir
from bs4 import BeautifulSoup

logger = get_logger("IngestionService")

class IngestionCancelled(Exception):
    pass

class IngestionService:
    EXPORT_MARKER_FILES = (
        "chat_history.json",
        "memories_history.json",
        "snap_history.json",
        "account.json",
        "user_profile.json",
    )
    ARCHIVE_SUFFIXES = (".zip", ".rar", ".7z")

    def __init__(self, db: DatabaseManager):
        self.db = db
        self.chunk_size = 100
        self.current_root: Optional[Path] = None
        self.media_buckets: Dict[str, List[Dict]] = {} 
        self.media_id_map: Dict[str, str] = {}
        self.media_match_mode: str = "strict"
        self._cancel_requested = threading.Event()
        self._last_run_cancelled = False

    def request_cancel(self) -> None:
        self._cancel_requested.set()

    def was_cancelled(self) -> bool:
        return self._last_run_cancelled

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
        progress_cb: Optional[Callable[[float, str], None]] = None,
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

        if progress_cb:
            progress_cb(0.0, "Import cancelled.")

        return False

    def _maybe_emit_progress(
        self,
        progress_cb: Optional[Callable[[float, str], None]],
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

    def process_archive(
        self,
        archive_path: Path,
        extract_to: Path,
        progress_cb: Callable[[float, str], None],
    ):
        archive_path = Path(archive_path)
        extract_to = Path(extract_to)
        created_extract_dir = not extract_to.exists()
        self._reset_job_flags()
        self._reset_runtime_state()

        try:
            self._raise_if_cancelled()
            progress_cb(0.0, "Checking disk space...")
            if not self._has_enough_space(archive_path, extract_to):
                logger.error("Insufficient disk space.")
                progress_cb(0.0, "Error: Not enough disk space!")
                return False

            progress_cb(0.1, "Extracting & Merging...")
            self._smart_extract(archive_path, extract_to, progress_cb, start=0.1, end=0.2)

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
                remove_extract_dir=created_extract_dir,
            )
        except Exception as e:
            logger.error(f"Archive ingestion failed: {e}")
            progress_cb(0.0, f"Error: {str(e)}")
            return False
        finally:
            self._reset_runtime_state()
            self._cancel_requested.clear()

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
        is_media = target.suffix.lower() in ['.jpg', '.jpeg', '.png', '.mp4', '.mov', '.avi', '.webm', '.m4a']
        if is_media and target.exists():
            return False
        return True

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

    def process_folder(
        self,
        folder_path: Path,
        progress_cb: Callable[[float, str], None],
        skip_extract: bool = False,
        reset_cancel_state: bool = True,
    ):
        if reset_cancel_state:
            self._reset_job_flags()
        self._reset_runtime_state()

        try:
            folder_path = Path(folder_path)
            self._raise_if_cancelled()
            if not skip_extract: progress_cb(0.1, "Verifying folder...")
            self.current_root = self._find_snap_root(folder_path)
            if not self.contains_export_markers(self.current_root):
                raise ValueError(f"No Snapchat export markers found in {self.current_root}.")
            logger.info(f"Root detected at: {self.current_root}")
            self.db.set_config("root_path", str(self.current_root.absolute()))
            self.media_match_mode = (self.db.get_config("media_match_mode") or "strict").lower()

            progress_cb(0.2, "Staging Data...")
            self._create_staging_environment()

            progress_cb(0.25, "Indexing Media...")
            self._index_and_bucket_media(self.current_root, progress_cb, start=0.25, end=0.6)

            progress_cb(0.62, "Identifying User...")
            self._parse_account_info()

            progress_cb(0.64, "Refreshing Messages...")
            self._reset_messages_for_import()

            progress_cb(0.65, "Parsing Chats...")
            self._parse_chats(progress_cb, start=0.65, end=0.85)
            self._parse_snap_history(progress_cb, start=0.85, end=0.95)

            progress_cb(1.0, "Complete!")
            return True
        except IngestionCancelled:
            return self._handle_cancelled_run(progress_cb)
        except Exception as e:
            logger.error(f"Folder ingestion failed: {e}")
            return False
        finally:
            self._reset_runtime_state()
            if reset_cancel_state:
                self._cancel_requested.clear()

    def rebuild_chat_media_links(
        self,
        progress_cb: Optional[Callable[[float, str], None]] = None
    ) -> bool:
        try:
            root_path = self.db.get_config("root_path")
            if not root_path:
                if progress_cb:
                    progress_cb(0.0, "Error: No root folder configured.")
                return False

            self.current_root = Path(root_path)
            if not self.current_root.exists():
                if progress_cb:
                    progress_cb(0.0, "Error: Root folder missing.")
                return False

            self.media_match_mode = (self.db.get_config("media_match_mode") or "strict").lower()

            if progress_cb:
                progress_cb(0.05, "Preparing rebuild...")

            self._create_staging_environment()

            if progress_cb:
                progress_cb(0.2, "Indexing Media...")
            self._index_and_bucket_media(self.current_root, progress_cb, start=0.2, end=0.6)

            if progress_cb:
                progress_cb(0.62, "Rebuilding chat links...")

            self.db.clear_messages()
            self._parse_chats(progress_cb, start=0.65, end=0.85)
            self._parse_snap_history(progress_cb, start=0.85, end=0.98)

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
            if user_key not in master_chats:
                master_chats[user_key] = messages
            else:
                existing_sigs = {f"{m.get('Created')}_{m.get('Content')}" for m in master_chats[user_key]}
                for msg in messages:
                    sig = f"{msg.get('Created')}_{msg.get('Content')}"
                    if sig not in existing_sigs:
                        master_chats[user_key].append(msg)
                        existing_sigs.add(sig)
        with open(staged_file, "w", encoding="utf-8") as f:
            json.dump(master_chats, f, indent=4)

    def _merge_memories_to_stage(self, staged_dir: Path):
        self._raise_if_cancelled()
        staged_file = staged_dir / "memories_history.json"
        master_mems = {"Saved Media": []}
        if staged_file.exists():
            try:
                with open(staged_file, "r", encoding="utf-8") as f: master_mems = json.load(f)
            except Exception as exc:
                logger.debug(f"Failed to read staged memories history: {exc}")
        json_src = self.current_root / "json" / "memories_history.json"
        if not json_src.exists(): json_src = self.current_root / "memories_history.json"
        if json_src.exists():
            try:
                with open(json_src, "r", encoding="utf-8") as f:
                    new_data = json.load(f)
                existing_dates = {m.get("Date") for m in master_mems.get("Saved Media", [])}
                for item in new_data.get("Saved Media", []):
                    self._raise_if_cancelled()
                    if item.get("Date") not in existing_dates:
                        master_mems["Saved Media"].append(item)
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
        progress_cb: Optional[Callable[[float, str], None]] = None,
        start: float = 0.0,
        end: float = 1.0
    ):
        self.media_buckets = {}
        self.media_id_map = {}
        media_folders = [root / "chat_media", root / "memories"]
        date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")
        video_exts = {".mp4", ".mov", ".avi", ".webm"}

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
            name = path.stem
            suffixes = ["_overlay", "_caption", "_image", "_video", "_media", "_main"]
            for suffix in suffixes:
                if name.endswith(suffix):
                    return name[:-len(suffix)]
            return name

        def is_video(path: Path) -> bool:
            return path.suffix.lower() in video_exts

        def main_rank(path: Path) -> int:
            stem = path.stem
            if is_video(path):
                if stem.endswith("_video"):
                    return 0
            else:
                if stem.endswith("_image"):
                    return 0
            if stem.endswith("_main"):
                return 1
            if stem.endswith("_media"):
                return 2
            return 3

        total_files = 0
        for folder in media_folders:
            if not folder.exists():
                continue
            total_files += sum(
                1 for file in folder.rglob("*")
                if file.is_file() and not file.name.startswith(".")
            )

        processed_files = 0
        last_emit = start
        if progress_cb:
            progress_cb(start, "Indexing Media...")

        for folder in media_folders:
            if not folder.exists(): continue
            is_chat_media = (folder.name == "chat_media")
            
            groups = {}
            for file in folder.rglob("*"):
                self._raise_if_cancelled()
                if file.is_file() and not file.name.startswith("."):
                    processed_files += 1
                    last_emit = self._maybe_emit_progress(
                        progress_cb, start, end, processed_files, total_files, "Indexing Media...", last_emit
                    )
                    stem_id = get_stem_id(file)
                    if stem_id not in groups:
                        groups[stem_id] = {"main": None, "overlay": None, "ts": None}
                    
                    is_overlay_file = file.stem.endswith(("_overlay", "_caption"))
                    
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

            batch = []
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
                    asset_id=stem_id,
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

        processed = 0
        last_emit = start
        if progress_cb:
            progress_cb(start, "Parsing Snap History...")

        for entry in snap_entries:
            self._raise_if_cancelled()
            processed += 1
            media_path = entry.get("media")
            if not media_path:
                last_emit = self._maybe_emit_progress(
                    progress_cb, start, end, processed, total, "Parsing Snap History...", last_emit
                )
                continue
            msg = Message(
                sender=entry["sender"],
                content="",
                timestamp=entry["ts"],
                msg_type=entry["type"],
                media_refs=[media_path],
                source="snap"
            )
            self.db.add_message(entry["conversation_id"], msg)
            last_emit = self._maybe_emit_progress(
                progress_cb, start, end, processed, total, "Parsing Snap History...", last_emit
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

        sorted_messages = []
        for entry in messages:
            self._raise_if_cancelled()
            try:
                sender = entry.get("From", "Unknown")
                content = entry.get("Content", "")
                ts_raw = entry.get("Created", "")
                media_type = entry.get("Media Type", "TEXT")
                
                clean_ts = ts_raw.replace(" UTC", "")
                ts = datetime.fromisoformat(clean_ts)
                
                sorted_messages.append({
                    "data": entry,
                    "ts": ts,
                    "sender": sender,
                    "content": content,
                    "type": media_type,
                    "media_ids": self._parse_media_ids(entry.get("Media IDs", ""))
                })
            except: continue
            
        sorted_messages.sort(key=lambda x: x["ts"])

        for item in sorted_messages:
            self._raise_if_cancelled()
            linked_media = self._find_media_for_message(item["ts"], item["media_ids"], item["type"])
            msg = Message(
                sender=item["sender"],
                content=item["content"] if item["content"] else "",
                timestamp=item["ts"],
                msg_type=item["type"],
                media_refs=linked_media
            )
            self.db.add_message(conversation_id, msg)
            if progress_state is not None:
                progress_state["processed"] += 1
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
