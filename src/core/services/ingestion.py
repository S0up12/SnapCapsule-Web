import json
import re
import shutil
import zipfile
from datetime import datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Callable, Optional, List, Dict, Any
from core.database.schema import DatabaseManager
from core.models import Message, MediaAsset
from core.utils.logger import get_logger
from core.utils.paths import get_app_data_dir
from bs4 import BeautifulSoup

logger = get_logger("IngestionService")

class IngestionService:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.chunk_size = 100
        self.current_root: Optional[Path] = None
        self.media_buckets: Dict[str, List[Dict]] = {} 
        self.media_id_map: Dict[str, str] = {}
        self.media_match_mode: str = "strict"

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

    def process_zip(self, zip_path: Path, extract_to: Path, progress_cb: Callable[[float, str], None]):
        try:
            zip_path = Path(zip_path)
            extract_to = Path(extract_to)
            progress_cb(0.0, "Checking disk space...")
            if not self._has_enough_space(zip_path, extract_to):
                logger.error("Insufficient disk space.")
                progress_cb(0.0, "Error: Not enough disk space!")
                return False

            progress_cb(0.1, "Extracting & Merging...")
            self._smart_extract(zip_path, extract_to, progress_cb, start=0.1, end=0.2)
            
            return self.process_folder(extract_to, progress_cb, skip_extract=True)
        except Exception as e:
            logger.error(f"ZIP ingestion failed: {e}")
            progress_cb(0.0, f"Error: {str(e)}")
            return False

    def _has_enough_space(self, zip_path: Path, dest_path: Path) -> bool:
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                total_uncompressed_size = sum(f.file_size for f in z.infolist())
            check_path = dest_path if dest_path.exists() else dest_path.parent
            if not check_path.exists(): check_path = get_app_data_dir()
            total, used, free = shutil.disk_usage(check_path)
            required = total_uncompressed_size + (500 * 1024 * 1024)
            return free >= required
        except Exception:
            return True 

    def _smart_extract(
        self,
        zip_path: Path,
        dest_path: Path,
        progress_cb: Optional[Callable[[float, str], None]] = None,
        start: float = 0.0,
        end: float = 1.0
    ):
        dest_path.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as z:
            members = z.infolist()
            total_members = len(members)
            processed = 0
            last_emit = start
            if progress_cb:
                progress_cb(start, "Extracting & Merging...")
            for member in members:
                if not self._is_safe_zip_member(member.filename):
                    logger.warning(f"Skipping unsafe zip member: {member.filename}")
                    processed += 1
                    last_emit = self._maybe_emit_progress(
                        progress_cb, start, end, processed, total_members, "Extracting & Merging...", last_emit
                    )
                    continue

                should_extract = True
                target = dest_path / member.filename
                if member.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    should_extract = False
                if should_extract:
                    is_media = target.suffix.lower() in ['.jpg', '.jpeg', '.png', '.mp4', '.mov', '.avi', '.webm', '.m4a']
                    if is_media and target.exists():
                        should_extract = False
                if should_extract:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        with z.open(member) as src, open(target, "wb") as dst:
                            shutil.copyfileobj(src, dst)
                    except Exception as exc:
                        logger.error(f"Failed to extract {member.filename}: {exc}")
                processed += 1
                last_emit = self._maybe_emit_progress(
                    progress_cb, start, end, processed, total_members, "Extracting & Merging...", last_emit
                )
            if progress_cb:
                progress_cb(end, "Extracting & Merging...")

    def process_folder(self, folder_path: Path, progress_cb: Callable[[float, str], None], skip_extract: bool = False):
        try:
            folder_path = Path(folder_path)
            if not skip_extract: progress_cb(0.1, "Verifying folder...")
            self.current_root = self._find_snap_root(folder_path)
            logger.info(f"Root detected at: {self.current_root}")
            self.db.set_config("root_path", str(self.current_root.absolute()))
            self.media_match_mode = (self.db.get_config("media_match_mode") or "strict").lower()

            progress_cb(0.2, "Staging Data...")
            self._create_staging_environment()

            progress_cb(0.25, "Indexing Media...")
            self._index_and_bucket_media(self.current_root, progress_cb, start=0.25, end=0.6)

            progress_cb(0.62, "Identifying User...")
            self._parse_account_info()

            progress_cb(0.65, "Parsing Chats...")
            self._parse_chats(progress_cb, start=0.65, end=0.85)
            self._parse_snap_history(progress_cb, start=0.85, end=0.95)

            progress_cb(1.0, "Complete!")
            return True
        except Exception as e:
            logger.error(f"Folder ingestion failed: {e}")
            return False

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
            if fname in special_files: continue
            src = None
            if json_dir.exists() and (json_dir / fname).exists(): src = json_dir / fname
            elif (self.current_root / fname).exists(): src = self.current_root / fname
            if src and src.exists():
                try: shutil.copy2(src, staged_dir / fname)
                except Exception as e: logger.error(f"Failed to stage {fname}: {e}")

    def _merge_chats_to_stage(self, staged_dir: Path):
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
                    if item.get("Date") not in existing_dates:
                        master_mems["Saved Media"].append(item)
                with open(staged_file, "w", encoding="utf-8") as f:
                    json.dump(master_mems, f, indent=4)
            except Exception as exc:
                logger.debug(f"Failed to merge memories history: {exc}")

    def _parse_html_directory(self, html_dir: Path) -> Dict:
        chats = {}
        for html_file in html_dir.glob("*.html"):
            try:
                with open(html_file, "r", encoding="utf-8") as f:
                    soup = BeautifulSoup(f, "html.parser")
                title = soup.find("title")
                friend_name = title.text.replace("Snapchat - ", "").strip() if title else html_file.stem
                messages = []
                rows = soup.find_all("tr")
                for row in rows:
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
            if not isinstance(messages, list):
                continue

            found_title = None
            for entry in messages:
                if t := entry.get("Conversation Title"):
                    found_title = t
                    break
            if found_title:
                self.db.update_conversation_title(conversation_id, found_title)

            for entry in messages:
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
                    if isinstance(content, list):
                        self._process_json_message_list(
                            key, content, progress_cb, progress_state, start=start, end=end
                        )
                    elif isinstance(content, dict):
                        for user, msgs in content.items():
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
            if t := entry.get("Conversation Title"):
                found_title = t
                break
        
        if found_title:
            self.db.update_conversation_title(conversation_id, found_title)

        sorted_messages = []
        for entry in messages:
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
